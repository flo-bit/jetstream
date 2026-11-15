package backfill

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmossync "github.com/jcalabro/atmos/sync"
)

// repoListChunk bounds one lookup/discover write while reconciling a repo
// list file.
const repoListChunk = 1000

// repoListState remembers the last file version reconciled, so an unchanged
// file costs one stat per retry pass.
type repoListState struct {
	modTime time.Time
	size    int64
}

// reconcileRepoList queues every DID in cfg.RepoListFile that the store has
// never seen for the steady-state retry pass (the same failed-marker rows
// merge discovery writes). The file holds one "did [pds-host]" per line;
// blank lines and lines starting with '#' are ignored. A host that fails
// validation is dropped, which routes that repo through the relay redirect.
//
// The file is operator input, so a missing or malformed file is logged and
// skipped rather than failing the retry loop.
func (r *retryRunner) reconcileRepoList(ctx context.Context) error {
	path := r.cfg.RepoListFile
	if path == "" {
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		r.cfg.Logger.WarnContext(ctx, "repo list unreadable", "path", path, "err", err)
		return nil
	}
	if fi.ModTime().Equal(r.repoList.modTime) && fi.Size() == r.repoList.size {
		return nil
	}
	byHost, total, skipped, err := readRepoList(path)
	if err != nil {
		r.cfg.Logger.WarnContext(ctx, "repo list unreadable", "path", path, "err", err)
		return nil
	}

	queued := 0
	for host, dids := range byHost {
		for start := 0; start < len(dids); start += repoListChunk {
			chunk := dids[start:min(start+repoListChunk, len(dids))]
			entries, err := r.store.LookupBatch(ctx, chunk)
			if err != nil {
				return fmt.Errorf("backfill: repo list: %w", err)
			}
			var unknown []atmossync.ListReposEntry
			for i, entry := range entries {
				if entry.State == atmosbackfill.StateUnknown {
					unknown = append(unknown, atmossync.ListReposEntry{DID: chunk[i], Active: true})
				}
			}
			if len(unknown) == 0 {
				continue
			}
			if err := r.store.OnDiscoverForRetry(ctx, host, unknown); err != nil {
				return fmt.Errorf("backfill: repo list: %w", err)
			}
			queued += len(unknown)
		}
	}
	r.repoList = repoListState{modTime: fi.ModTime(), size: fi.Size()}
	r.cfg.Logger.InfoContext(ctx, "reconciled repo list",
		"path", path, "listed", total, "queued", queued, "skipped_lines", skipped)
	return nil
}

// readRepoList parses the repo list file into DIDs grouped by PDS host
// ("" when the line names none or names an invalid one). Duplicate DIDs
// keep their first line.
func readRepoList(path string) (byHost map[string][]atmos.DID, total, skipped int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer func() { _ = f.Close() }()

	byHost = make(map[string][]atmos.DID)
	seen := make(map[atmos.DID]struct{})
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		did, perr := atmos.ParseDID(fields[0])
		if perr != nil {
			skipped++
			continue
		}
		if _, dup := seen[did]; dup {
			continue
		}
		seen[did] = struct{}{}
		host := ""
		if len(fields) > 1 {
			host = strings.ToLower(fields[1])
			if atmosbackfill.ValidateHostname(host) != nil {
				host = ""
			}
		}
		byHost[host] = append(byHost[host], did)
		total++
	}
	if err := sc.Err(); err != nil {
		return nil, 0, 0, err
	}
	return byHost, total, skipped, nil
}
