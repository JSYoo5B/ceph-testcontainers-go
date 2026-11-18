//go:build all || integration

package cephfs_mirror_daemon_redistribution_test

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// redistributionCandidate requires a new service registered with the peer
// alongside the same surviving owner. The case starts only one new daemon.
func redistributionCandidate(prior, current map[string]bool, survivor string) string {
	if len(current) != 2 || !current[survivor] {
		return ""
	}
	for id := range current {
		if id != "0" && id != "" && !prior[id] && id != survivor {
			return id
		}
	}
	return ""
}

// judgeRedistribution requires matching peer membership on both sides of
// the distribution read. A complete 4/0 map differs from unreadable or stale
// evidence. Only keeping all directories on the survivor is the target
// symptom; moving all directories to the replacement is a different result.
func judgeRedistribution(raw []byte, before, after map[string]bool, survivor, replacement string, total int) (bool, error) {
	if survivor == replacement || len(before) != 2 || len(after) != 2 ||
		!before[survivor] || !before[replacement] || !after[survivor] || !after[replacement] {
		return false, fmt.Errorf("peer membership before %v and after %v must contain survivor %s and replacement %s", before, after, survivor, replacement)
	}
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := strictjson.Decode(raw, &distribution, strictjson.Options{RejectNull: true}); err != nil {
		return false, err
	}
	if len(distribution.Mapping) != 2 {
		return false, fmt.Errorf("expected two distribution entries, got %d", len(distribution.Mapping))
	}
	count := func(id string) (int, error) {
		value, ok := strings.CutSuffix(distribution.Mapping[id], " directories")
		n, err := strconv.Atoi(value)
		if !ok || err != nil || n < 0 || n > total {
			return 0, fmt.Errorf("invalid directory count %q for instance %s", distribution.Mapping[id], id)
		}
		return n, nil
	}
	kept, err := count(survivor)
	if err != nil {
		return false, err
	}
	moved, err := count(replacement)
	if err != nil {
		return false, err
	}
	if kept+moved != total {
		return false, fmt.Errorf("distribution accounts for %d directories, want %d", kept+moved, total)
	}
	if kept == 0 {
		return false, fmt.Errorf("all directories moved to the replacement without redistribution")
	}
	return moved != 0, nil
}
