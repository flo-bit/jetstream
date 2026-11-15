package backfill

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
)

// RepoBackfilled reports whether did's repo has been downloaded. Queued and
// failed repos report false: the retry pass still owes them a full download,
// so repairing their chain with another one would fetch the repo twice.
func RepoBackfilled(_ context.Context, db metastore.Store, did atmos.DID) (bool, error) {
	rs, err := readRepoStatusFrom(db, did)
	if err != nil {
		return false, fmt.Errorf("backfill: repo status %s: %w", did, err)
	}
	return rs != nil && rs.Backfill.Status == StatusComplete, nil
}
