package backfill

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCappedReader(t *testing.T) {
	t.Parallel()

	got, err := io.ReadAll(&cappedReader{r: bytes.NewReader(make([]byte, 100)), remaining: 100, limit: 100})
	require.NoError(t, err, "a stream exactly at the limit is read in full")
	require.Len(t, got, 100)

	got, err = io.ReadAll(&cappedReader{r: bytes.NewReader(make([]byte, 101)), remaining: 100, limit: 100})
	require.ErrorIs(t, err, ErrRepoTooLarge)
	require.Len(t, got, 100)
}
