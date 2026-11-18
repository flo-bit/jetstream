package backfill

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// damagedRepoFixture is a repo of the given records whose CAR lacks the
// blocks of the records in without, as a PDS that lost them serves it.
func damagedRepoFixture(t *testing.T, did atmos.DID, rkeys []string, without ...string) repoFixture {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	store := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: store, Tree: mst.NewTree(store)}
	for _, rkey := range rkeys {
		require.NoError(t, r.Create("app.bsky.feed.post", rkey, map[string]any{"text": rkey}))
	}
	var whole bytes.Buffer
	require.NoError(t, r.ExportCAR(&whole, key))

	lost := map[cbor.CID]bool{}
	require.NoError(t, r.Tree.Walk(func(path string, val cbor.CID) error {
		for _, rkey := range without {
			if path == "app.bsky.feed.post/"+rkey {
				lost[val] = true
			}
		}
		return nil
	}))
	require.Len(t, lost, len(without))
	header, blocks, err := car.ReadAll(bytes.NewReader(whole.Bytes()))
	require.NoError(t, err)
	kept := blocks[:0]
	for _, b := range blocks {
		if !lost[b.CID] {
			kept = append(kept, b)
		}
	}
	var damaged bytes.Buffer
	require.NoError(t, car.WriteAll(&damaged, header.Roots, kept))
	return repoFixture{did: did, car: damaged.Bytes()}
}

// A repo whose host has lost a record's block fails every download as
// incomplete. When the host itself says the record does not exist, the rest
// of the repo is archived; when the host does serve the record, the download
// was cut short and the repo stays queued.
func TestRetryRunner_RepoWithUnservedRecords(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		lost     []string
		unserved []string
		want     []string // archived rkeys; nil when the repo must stay failed
	}{
		{"host confirms the record is gone", []string{"b"}, []string{"b"}, []string{"a", "c"}},
		{"host confirms each of several", []string{"a", "c"}, []string{"a", "c"}, []string{"b"}},
		{"host still serves the record", []string{"b"}, nil, nil},
		{"host serves one of two", []string{"a", "b"}, []string{"a"}, nil},
		// Through a temporary file, as repos over 64 MB are loaded.
		{"spilled: host confirms the record is gone", []string{"b"}, []string{"b"}, []string{"a", "c"}},
		{"spilled: host still serves the record", []string{"b"}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, w, segmentsDir := newRetryTestWriter(t)
			bs := newSeededStore(t, st, nil)
			ctx := context.Background()
			did := atmos.DID("did:plc:retrydamaged")
			srv := newStubServer(t, map[atmos.DID]repoFixture{did: damagedRepoFixture(t, did, []string{"a", "b", "c"}, tc.lost...)})
			srv.unservedRecords = map[string]bool{}
			for _, rkey := range tc.unserved {
				srv.unservedRecords["app.bsky.feed.post/"+rkey] = true
			}
			host := mustURLHost(t, srv.srv.URL)
			now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

			require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
			require.NoError(t, bs.OnFail(ctx, did, host, errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
			require.NoError(t, bs.RecordRetryFailure(ctx, did, host, errors.New("xrpc 503 unavailable"), now.Add(-time.Hour)))

			spillDir := ""
			if strings.HasPrefix(tc.name, "spilled") {
				spillDir = t.TempDir() // SpillThreshold zero: every repo spills
			}
			drops := ingest.NewDropMetrics(prometheus.NewRegistry())
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
				DropMetrics: drops,
				SpillDir:    spillDir,
				now:         func() time.Time { return now },
			})
			require.NoError(t, err)
			require.NoError(t, r.runPass(ctx))

			rs, err := bs.readRepoStatus(did)
			require.NoError(t, err)
			if tc.want == nil {
				require.Equal(t, StatusFailed, rs.Backfill.Status)
				require.Contains(t, rs.Backfill.LastError, "incomplete CAR")
				requireNoSpillFiles(t, spillDir)
				return
			}
			require.Equal(t, StatusComplete, rs.Backfill.Status)
			events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
			require.Equal(t, segment.KindSync, events[0].Kind)
			var got []string
			for _, ev := range events[1:] {
				require.Equal(t, segment.KindCreateResync, ev.Kind)
				got = append(got, ev.Rkey)
			}
			require.Equal(t, tc.want, got)
			requireNoSpillFiles(t, spillDir)
			dropped := drops.Counter(ingest.DropSourceBackfill, ingest.DropReasonMissingBlock)
			require.InDelta(t, len(tc.lost), testutil.ToFloat64(dropped), 0)
		})
	}
}

func requireNoSpillFiles(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		return
	}
	left, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, left)
}
