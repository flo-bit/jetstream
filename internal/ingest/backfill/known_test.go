package backfill

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestRepoResyncable(t *testing.T) {
	t.Parallel()

	db := memstore.New()
	st := NewStore(db, nil)
	require.NoError(t, st.SeedCounts(t.Context()))

	const queued, done, large, unknown = atmos.DID("did:plc:queued"), atmos.DID("did:plc:done"), atmos.DID("did:plc:large"), atmos.DID("did:plc:unknown")
	require.NoError(t, st.OnDiscoverForRetry(t.Context(), "pds.example.com", []atmossync.ListReposEntry{
		{DID: queued, Active: true}, {DID: done, Active: true}, {DID: large, Active: true},
	}))
	for _, did := range []atmos.DID{done, large} {
		rs, err := readRepoStatusFrom(db, did)
		require.NoError(t, err)
		rs.Backfill.Status = StatusComplete
		require.NoError(t, st.putRepoStatus(did, rs))
	}
	require.NoError(t, markLargeRepo(t.Context(), db, large))

	for did, want := range map[atmos.DID]bool{queued: false, done: true, large: false, unknown: false} {
		got, err := RepoResyncable(t.Context(), db, did)
		require.NoError(t, err)
		require.Equal(t, want, got, "%s", did)
	}
}
