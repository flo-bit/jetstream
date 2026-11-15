package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectionPatterns(t *testing.T) {
	t.Parallel()

	p, err := parseCollectionPatterns(" app.bsky.* , chat.bsky.actor.declaration ,, ")
	require.NoError(t, err)
	require.True(t, p.match("app.bsky.feed.post"))
	require.True(t, p.match("chat.bsky.actor.declaration"))
	require.False(t, p.match("chat.bsky.convo.message"), "exact entries do not match siblings")
	require.False(t, p.match("app.bskyx.feed.post"), "a wildcard matches whole segments only")
	require.False(t, p.match("app.bsky"), "a wildcard does not match its own authority")

	_, err = parseCollectionPatterns("app.*.post")
	require.Error(t, err)
}

// The filter is process-wide, so this test uses collection names no other
// test touches and restores the empty filter when it finishes.
func TestSetCollectionFilter(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, SetCollectionFilter("", "")) })

	require.False(t, CollectionDenied("zz.filtertest.a.thing"), "no filter installed")

	require.NoError(t, SetCollectionFilter("zz.filtertest.a.*", "zz.filtertest.a.kept"))
	require.True(t, CollectionDenied("zz.filtertest.a.thing"))
	require.False(t, CollectionDenied("zz.filtertest.a.kept"), "keep wins over deny")
	require.False(t, CollectionDenied("zz.filtertest.b.thing"))
	require.False(t, CollectionDenied(""), "DID-level events carry no collection and always pass")

	require.Error(t, SetCollectionFilter("zz.*.bad", ""))
	require.True(t, CollectionDenied("zz.filtertest.a.thing"), "a rejected filter leaves the old one in place")

	require.NoError(t, SetCollectionFilter("", "zz.filtertest.a.kept"))
	require.False(t, CollectionDenied("zz.filtertest.a.thing"), "an empty deny list disables filtering")
}
