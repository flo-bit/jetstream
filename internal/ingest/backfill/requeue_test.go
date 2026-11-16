package backfill

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestRequeueForResync: only a repo whose download completed long enough
// ago is queued, exactly once, and the status counts move with it.
func TestRequeueForResync(t *testing.T) {
	t.Parallel()

	db := memstore.New()
	st := NewStore(db, nil)
	ctx := t.Context()
	require.NoError(t, st.SeedCounts(ctx))

	const old, fresh, queued, unknown = atmos.DID("did:plc:old"), atmos.DID("did:plc:fresh"), atmos.DID("did:plc:queued"), atmos.DID("did:plc:unknown")
	require.NoError(t, st.OnDiscoverForRetry(ctx, "pds.example.com", []atmossync.ListReposEntry{
		{DID: old, Active: true}, {DID: fresh, Active: true}, {DID: queued, Active: true},
	}))
	complete := func(did atmos.DID, at time.Time) {
		t.Helper()
		require.NoError(t, st.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
			rs.Backfill.Status = StatusComplete
			rs.Backfill.CompletedAt = at
			rs.Backfill.LastError = ""
			return nil, nil
		}))
	}
	complete(old, time.Now().Add(-2*time.Hour))
	complete(fresh, time.Now().Add(-time.Minute))

	before, err := loadCountsFrom(db)
	require.NoError(t, err)

	for did, want := range map[atmos.DID]bool{old: true, fresh: false, queued: false, unknown: false} {
		got, err := st.RequeueForResync(ctx, did, "commit chain broke", 30*time.Minute)
		require.NoError(t, err)
		require.Equal(t, want, got, "%s", did)
	}
	again, err := st.RequeueForResync(ctx, old, "commit chain broke", 30*time.Minute)
	require.NoError(t, err)
	require.False(t, again, "a repo already queued is not queued twice")

	rs, err := st.readRepoStatus(old)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	require.Equal(t, "commit chain broke", rs.Backfill.LastError)
	require.True(t, rs.Backfill.NextAttemptAt.IsZero(), "the next retry pass must pick it up")

	rs, err = st.readRepoStatus(fresh)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	_, err = st.readRepoStatus(unknown)
	require.NoError(t, err)

	after, err := loadCountsFrom(db)
	require.NoError(t, err)
	require.Equal(t, before.Complete-1, after.Complete)
	require.Equal(t, before.Failed+1, after.Failed)
	require.Equal(t, before.Total, after.Total)
}

// TestRetryRunner_StaleDownloadIsDiscarded: a repo copy older than what the
// live stream has processed must not be appended, because its tombstone
// would wipe records archived from the newer commits. It stays queued.
func TestRetryRunner_StaleDownloadIsDiscarded(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		liveRev  func(downloaded string) string
		wantDone bool
	}{
		{"live stream is ahead", func(string) string { return "zzzzzzzzzzzzz" }, false},
		{"live stream is at the downloaded rev", func(rev string) string { return rev }, true},
		{"live stream has no state", func(string) string { return "" }, true},
	} {
		st, w, segmentsDir := newRetryTestWriter(t)
		bs := newSeededStore(t, st, nil)
		ctx := context.Background()
		did := atmos.DID("did:plc:retrystale")
		fixture := buildRepoFixture(t, did)
		srv := newStubServer(t, map[atmos.DID]repoFixture{did: fixture})
		host := mustURLHost(t, srv.srv.URL)
		now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnFail(ctx, did, host, errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
		require.NoError(t, bs.RecordRetryFailure(ctx, did, host, errors.New("xrpc 503 unavailable"), now.Add(-time.Hour)))

		var downloaded string
		r, err := newRetryRunner(RetryConfig{
			Store:       st,
			Writer:      w,
			HTTPClient:  srv.srv.Client(),
			RelayURL:    srv.srv.URL,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			Interval:    time.Hour,
			Workers:     1,
			HostWorkers: 1,
			MaxDelay:    24 * time.Hour,
			ChainRev: func(context.Context, atmos.DID) (string, error) {
				return tc.liveRev(downloaded), nil
			},
			now: func() time.Time { return now },
		})
		require.NoError(t, err)
		// The fixture's rev, as the runner will see it.
		rp, commit, _, _, err := r.download(ctx, retryCandidate{DID: did, Host: host, PDS: ""})
		require.NoError(t, err, tc.name)
		releaseRepo(rp, nil)
		downloaded = commit.Rev

		require.NoError(t, r.runPass(ctx), tc.name)

		events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
		rs, err := bs.readRepoStatus(did)
		require.NoError(t, err)
		if tc.wantDone {
			require.Len(t, events, 2, tc.name)
			require.Equal(t, StatusComplete, rs.Backfill.Status, tc.name)
		} else {
			require.Empty(t, events, "%s: nothing may be appended", tc.name)
			require.Equal(t, StatusFailed, rs.Backfill.Status, tc.name)
			require.Contains(t, rs.Backfill.LastError, "older than the live stream", tc.name)
		}
	}
}
