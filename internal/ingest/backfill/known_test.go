package backfill

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestRepoBackfilled(t *testing.T) {
	t.Parallel()

	db := memstore.New()
	st := NewStore(db, nil)
	require.NoError(t, st.SeedCounts(t.Context()))

	const queued, done, unknown = atmos.DID("did:plc:queued"), atmos.DID("did:plc:done"), atmos.DID("did:plc:unknown")
	require.NoError(t, st.OnDiscoverForRetry(t.Context(), "pds.example.com", []atmossync.ListReposEntry{
		{DID: queued, Active: true}, {DID: done, Active: true},
	}))
	rs, err := readRepoStatusFrom(db, done)
	require.NoError(t, err)
	rs.Backfill.Status = StatusComplete
	require.NoError(t, st.putRepoStatus(done, rs))

	for did, want := range map[atmos.DID]bool{queued: false, done: true, unknown: false} {
		got, err := RepoBackfilled(t.Context(), db, did)
		require.NoError(t, err)
		require.Equal(t, want, got, "%s", did)
	}
}
