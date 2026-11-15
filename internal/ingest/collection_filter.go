package ingest

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Process-wide ingest collection filter. Record-level events
// (create/update/delete/resync rows) whose collection is denied are
// dropped before they reach the segment writer, on both the backfill
// and the live path. DID-level events carry no collection and always
// pass. Patterns are exact NSIDs or "<prefix>.*"; keep wins over deny.

type collectionPatterns struct {
	exact    map[string]struct{}
	prefixes []string
}

func parseCollectionPatterns(raw string) (collectionPatterns, error) {
	p := collectionPatterns{exact: map[string]struct{}{}}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		switch {
		case part == "":
		case strings.HasSuffix(part, ".*"):
			p.prefixes = append(p.prefixes, strings.TrimSuffix(part, "*"))
		case strings.Contains(part, "*"):
			return p, fmt.Errorf("ingest: collection pattern %q: only a trailing .* wildcard is supported", part)
		default:
			p.exact[part] = struct{}{}
		}
	}
	return p, nil
}

func (p collectionPatterns) match(collection string) bool {
	if _, ok := p.exact[collection]; ok {
		return true
	}
	for _, prefix := range p.prefixes {
		if strings.HasPrefix(collection, prefix) {
			return true
		}
	}
	return false
}

type collectionFilter struct {
	deny, keep collectionPatterns
}

var activeCollectionFilter atomic.Pointer[collectionFilter]

// SetCollectionFilter installs the process-wide filter. Empty deny
// disables filtering.
func SetCollectionFilter(deny, keep string) error {
	d, err := parseCollectionPatterns(deny)
	if err != nil {
		return err
	}
	k, err := parseCollectionPatterns(keep)
	if err != nil {
		return err
	}
	if len(d.exact) == 0 && len(d.prefixes) == 0 {
		activeCollectionFilter.Store(nil)
		return nil
	}
	activeCollectionFilter.Store(&collectionFilter{deny: d, keep: k})
	return nil
}

// CollectionDenied reports whether records of collection are dropped
// at ingest.
func CollectionDenied(collection string) bool {
	f := activeCollectionFilter.Load()
	if f == nil || collection == "" {
		return false
	}
	return f.deny.match(collection) && !f.keep.match(collection)
}
