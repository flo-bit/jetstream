package backfill

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
)

// largeRepoKeyPrefix marks repos that had to be loaded from disk.
const largeRepoKeyPrefix = "largerepo/"

func largeRepoKey(did atmos.DID) []byte { return []byte(largeRepoKeyPrefix + string(did)) }

func markLargeRepo(ctx context.Context, db metastore.Store, did atmos.DID) error {
	if err := db.Set(ctx, largeRepoKey(did), []byte{1}); err != nil {
		return fmt.Errorf("backfill: mark large repo %s: %w", did, err)
	}
	return nil
}

// RepoResyncable reports whether a chain break on did should be repaired by
// downloading the repo again: its backfill has completed, and it is not one
// of the repos too large to hold in memory, which a resync would load whole.
// Queued and failed repos report false because the retry pass still owes
// them a full download.
func RepoResyncable(ctx context.Context, db metastore.Store, did atmos.DID) (bool, error) {
	vals, err := db.GetMany(ctx, [][]byte{repoKey(did), largeRepoKey(did)})
	if err != nil {
		return false, fmt.Errorf("backfill: repo status %s: %w", did, err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] != nil {
		return false, nil
	}
	rs, err := decodeRepoStatus(vals[0])
	if err != nil {
		return false, err
	}
	return rs.Backfill.Status == StatusComplete, nil
}
