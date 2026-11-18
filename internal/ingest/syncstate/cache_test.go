package syncstate

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// countingStore counts point reads, which on a shared backend are round
// trips to the catalog.
type countingStore struct {
	metastore.Store
	gets atomic.Int64
}

func (s *countingStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, key)
}

func (s *countingStore) takeGets() int64 { return s.gets.Swap(0) }

func commitChain(t *testing.T, s *StateStore, did atmos.DID, rev string) {
	t.Helper()
	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: rev, Data: fixedCID(t)}))
	s.PromoteChain(did, rev)
	require.NoError(t, flush(t, s))
}

func loadRev(t *testing.T, s *StateStore, did atmos.DID) string {
	t.Helper()
	got, err := s.LoadChain(t.Context(), did)
	require.NoError(t, err)
	if got == nil {
		return ""
	}
	return got.Rev
}

// A DID's chain state, once this StateStore commits it, loads without a
// metadata store read.
func TestStateStore_LoadChainServesCommitted(t *testing.T) {
	t.Parallel()
	cs := &countingStore{Store: newTestStore(t)}
	s := New(cs)
	did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")

	require.Empty(t, loadRev(t, s, did))
	require.Equal(t, int64(1), cs.takeGets())
	commitChain(t, s, did, "3lrev1")
	require.Equal(t, "3lrev1", loadRev(t, s, did))
	commitChain(t, s, did, "3lrev2")
	require.Equal(t, "3lrev2", loadRev(t, s, did))
	require.Zero(t, cs.takeGets())

	require.Equal(t, "3lrev2", loadRev(t, New(cs), did), "the metadata store agrees")
}

// A batch that commits a state a later promotion already superseded caches
// what it committed; the promotion shadows it until its own batch commits.
func TestStateStore_CommittedCacheUnderLatePromotion(t *testing.T) {
	t.Parallel()
	cs := &countingStore{Store: newTestStore(t)}
	s := New(cs)
	did := parseDID(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev1", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev1")
	b := cs.NewBatch()
	s.StageFlush(b)
	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev2", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev2")
	require.NoError(t, b.Commit(t.Context()))
	s.CommitStaged()

	require.Equal(t, "3lrev2", loadRev(t, s, did))
	require.NoError(t, flush(t, s))
	require.Equal(t, "3lrev2", loadRev(t, s, did))
	require.Zero(t, cs.takeGets())
}

// A batch that fails caches nothing: the next load still sees the promoted
// state, and after a successful retry, the committed one.
func TestStateStore_FailedCommitCachesNothing(t *testing.T) {
	t.Parallel()
	raw := newTestStore(t)
	fault := &metastore.KeyPrefixFault{Prefix: []byte("sync/"), Op: metastore.WriteOpBatchCommit, Ordinal: 2, Err: fmt.Errorf("injected")}
	s := New(metastore.WithFaults(raw, fault))
	did := parseDID(t, "did:plc:cccccccccccccccccccccccc")

	commitChain(t, s, did, "3lrev1")
	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev2", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev2")
	require.Error(t, flush(t, s))
	require.Equal(t, "3lrev2", loadRev(t, s, did), "the promoted state still wins")
	require.NoError(t, flush(t, s))
	require.Equal(t, "3lrev2", loadRev(t, s, did))
	require.Equal(t, "3lrev2", loadRev(t, New(raw), did))
}

// Delete drops the cached state with the durable one.
func TestStateStore_DeleteDropsCommitted(t *testing.T) {
	t.Parallel()
	cs := &countingStore{Store: newTestStore(t)}
	s := New(cs)
	did := parseDID(t, "did:plc:dddddddddddddddddddddddd")
	commitChain(t, s, did, "3lrev1")
	require.NoError(t, s.Delete(t.Context(), did))
	require.Empty(t, loadRev(t, s, did))
	require.Equal(t, int64(1), cs.takeGets())
}

// The cache is bounded and keeps what was used recently. What it evicted
// still loads, from the metadata store.
func TestStateStore_CommittedCacheBounded(t *testing.T) {
	t.Parallel()
	cs := &countingStore{Store: newTestStore(t)}
	s := newStateStore(cs, 8)
	dids := make([]atmos.DID, 20)
	for i := range dids {
		dids[i] = parseDID(t, fmt.Sprintf("did:plc:%024d", i))
		commitChain(t, s, dids[i], fmt.Sprintf("3lrev%02d", i))
		if i >= 1 {
			// dids[0] is used throughout, so it stays.
			require.Equal(t, "3lrev00", loadRev(t, s, dids[0]))
		}
	}
	require.Zero(t, cs.takeGets())
	require.LessOrEqual(t, len(s.committedChain.cur)+len(s.committedChain.prev), 8)
	for i, did := range dids {
		require.Equal(t, fmt.Sprintf("3lrev%02d", i), loadRev(t, s, did))
	}
	require.NotZero(t, cs.takeGets(), "the oldest were evicted")
	require.Equal(t, "3lrev19", loadRev(t, s, dids[19]))
	require.Zero(t, cs.takeGets())
}

// The verifier and the consumer pass DIDs that point into the upstream frame
// they were decoded from. A map key that kept such a string would keep the
// frame; in production that was over a gigabyte for a full committedChain.
func TestStateStore_KeysDoNotAliasCallerMemory(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	frame := []byte("did:plc:eeeeeeeeeeeeeeeeeeeeeeee and the rest of a firehose frame")
	did := atmos.DID(unsafe.String(&frame[0], 32))
	inFrame := func(k atmos.DID) bool {
		p := uintptr(unsafe.Pointer(unsafe.StringData(string(k))))
		start := uintptr(unsafe.Pointer(&frame[0]))
		return p >= start && p < start+uintptr(len(frame))
	}
	check := func(stage string) {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		for name, keys := range map[string]iter.Seq[atmos.DID]{
			"pendingChain":     maps.Keys(s.pendingChain),
			"pendingHosting":   maps.Keys(s.pendingHosting),
			"promotedChain":    maps.Keys(s.promotedChain),
			"promotedHosting":  maps.Keys(s.promotedHosting),
			"promotedIdent":    maps.Keys(s.promotedIdent),
			"promotedAccount":  maps.Keys(s.promotedAccount),
			"unsnappedChain":   maps.Keys(s.unsnappedChain),
			"unsnappedHosting": maps.Keys(s.unsnappedHosting),
			"unsnappedIdent":   maps.Keys(s.unsnappedIdent),
			"unsnappedAccount": maps.Keys(s.unsnappedAccount),
			"committedChain":   maps.Keys(s.committedChain.cur),
		} {
			for k := range keys {
				require.Falsef(t, inFrame(k), "%s: %s key points into the caller's buffer", stage, name)
			}
		}
	}

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev1", Data: fixedCID(t)}))
	require.NoError(t, s.SaveHosting(t.Context(), did, atmossync.HostingState{Seq: 5, Active: true}))
	check("saved")
	s.PromoteChain(did, "3lrev1")
	s.PromoteHosting(did, 5)
	s.RecordIdentitySeq(did, 6)
	s.RecordAccountSeq(did, 5)
	check("promoted")
	require.NoError(t, flush(t, s))
	check("committed")
	require.Equal(t, "3lrev1", loadRev(t, s, did))
	require.Len(t, s.committedChain.cur, 1)
}
