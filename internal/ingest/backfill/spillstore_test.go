package backfill

import (
	"fmt"
	"os"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

func TestSpillStore(t *testing.T) {
	t.Parallel()

	for _, threshold := range []int64{1 << 30, 4096, 0} {
		dir := t.TempDir()
		s := newSpillStore(dir, threshold)

		const n = 20000 // enough to grow the index table several times
		blocks := make(map[cbor.CID][]byte, n)
		for i := range n {
			data := []byte(fmt.Sprintf("block %d %s", i, string(make([]byte, i%97))))
			cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
			blocks[cid] = data
			require.NoError(t, s.PutBlock(cid, data))
		}
		empty := cbor.ComputeCID(cbor.CodecRaw, nil)
		require.NoError(t, s.PutBlock(empty, nil))
		// A CAR may carry the same block twice.
		for cid, data := range blocks {
			require.NoError(t, s.PutBlock(cid, data))
			break
		}

		require.Equal(t, threshold < 1<<30, s.Spilled(), "threshold=%d", threshold)
		for cid, want := range blocks {
			got, err := s.GetBlock(cid)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		got, err := s.GetBlock(empty)
		require.NoError(t, err)
		require.Empty(t, got)

		_, err = s.GetBlock(cbor.ComputeCID(cbor.CodecDagCBOR, []byte("absent")))
		require.ErrorIs(t, err, mst.ErrBlockNotFound)

		require.NoError(t, s.Close())
		left, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, left, "the temporary file is removed on close")
	}
}
