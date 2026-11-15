package backfill

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
)

// RepoKnown reports whether did has a backfill row in db, in any state:
// queued, downloaded or failed.
func RepoKnown(ctx context.Context, db metastore.Store, did atmos.DID) (bool, error) {
	vals, err := db.GetMany(ctx, [][]byte{repoKey(did)})
	if err != nil {
		return false, fmt.Errorf("backfill: repo known %s: %w", did, err)
	}
	return len(vals) == 1 && vals[0] != nil, nil
}
