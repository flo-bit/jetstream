package backfill

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/jcalabro/atmos"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestReadRepoList(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "repos.txt")
	require.NoError(t, os.WriteFile(path, []byte(`# comment

did:plc:aaaaaaaaaaaaaaaaaaaaaaaa pds.example.com
did:plc:bbbbbbbbbbbbbbbbbbbbbbbb	PDS.Example.com
did:plc:cccccccccccccccccccccccc
did:plc:dddddddddddddddddddddddd localhost
did:plc:aaaaaaaaaaaaaaaaaaaaaaaa other.example.com
not-a-did pds.example.com
`), 0o600))

	byHost, total, skipped, err := readRepoList(path)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Equal(t, 1, skipped)
	require.Equal(t, []atmos.DID{"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"}, byHost["pds.example.com"],
		"hosts are lowercased and a duplicate DID keeps its first line")
	require.Equal(t, []atmos.DID{"did:plc:cccccccccccccccccccccccc", "did:plc:dddddddddddddddddddddddd"}, byHost[""],
		"a missing or invalid host routes through the relay")
	require.NotContains(t, byHost, "other.example.com")
}

// TestSegmentHandler_FilteredCollectionDropsRecordKeepsSiblings: a record in
// a collection the ingest filter denies is dropped and counted once on both
// the full-repo and resync paths, and its siblings archive.
func TestSegmentHandler_FilteredCollectionDropsRecordKeepsSiblings(t *testing.T) {
	// Process-wide filter: names unique to this test, restored on exit.
	require.NoError(t, ingest.SetCollectionFilter("zz.handlerfilter.*", "zz.handlerfilter.kept.thing"))
	t.Cleanup(func() { require.NoError(t, ingest.SetCollectionFilter("", "")) })

	for _, resync := range []bool{false, true} {
		w := newTestIngest(t)
		dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
		h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		h.dropMetrics = dropMetrics

		r, commit := buildHostileKeyRepo(t, "did:plc:filtered",
			"zz.handlerfilter.dropped.thing/a", "zz.handlerfilter.kept.thing/a", "zz.other.thing/a")

		want := uint64(3) // two surviving records
		if resync {
			require.NoError(t, h.HandleRepoResync(t.Context(), "did:plc:filtered", r, commit))
			want = 4 // plus the KindSync tombstone
		} else {
			require.NoError(t, h.HandleRepo(t.Context(), "did:plc:filtered", r, commit))
		}
		require.Equal(t, want, w.NextSeq(), "resync=%v", resync)
		require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceBackfill, ingest.DropReasonFilteredCollection)), 0,
			"resync=%v: the drop is counted exactly once", resync)
	}
}
