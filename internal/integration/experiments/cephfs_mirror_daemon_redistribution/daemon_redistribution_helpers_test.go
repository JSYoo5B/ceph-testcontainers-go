//go:build all || integration

package cephfs_mirror_daemon_redistribution_test

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// redistributionObservation is the planned observation of both variants.
const redistributionObservation = "redistribution-after-join"

// judgeRedistribution decides one native reading taken after a replacement
// daemon joined the surviving one. The MGR spreading the directories over
// two instances, each with a positive count and every directory mapped, is
// the absence of the symptom. Every directory still with the survivor, the
// only instance with directories, while two services are registered with the
// shared peer, is the symptom. Its error starts with "condition not
// established" when fewer than two services are registered (the replacement
// did not join), and with "another fault" when a directory is not mapped to
// a current instance or the directories moved some other way: no verdict.
func judgeRedistribution(directories []string, survivor string, distribution, owners map[string]string, registered int) (symptom bool, detail string, err error) {
	positive := map[string]int{}
	for id, summary := range distribution {
		count, err := strconv.Atoi(strings.TrimSuffix(summary, " directories"))
		if id == "" || err != nil {
			return false, "", fmt.Errorf("another fault: the distribution has instance %q with %q", id, summary)
		}
		if count > 0 {
			positive[id] = count
		}
	}
	held := map[string]int{}
	for _, directory := range directories {
		owner := owners[directory]
		if owner == "" {
			return false, "", fmt.Errorf("another fault: directory %s is not mapped to a current instance (distribution %v)", directory, distribution)
		}
		held[owner]++
	}
	if registered < 2 {
		return false, "", fmt.Errorf("condition not established: %d services are registered with the shared peer, want the survivor and the replacement", registered)
	}
	instances := slices.Sorted(maps.Keys(held))
	switch {
	case len(distribution) == 2 && len(positive) == 2 && len(held) == 2:
		return false, fmt.Sprintf("the %d directories are spread over instances %s (%d) and %s (%d)", len(directories), instances[0], held[instances[0]], instances[1], held[instances[1]]), nil
	case len(held) == 1 && held[survivor] == len(directories) && len(positive) == 1 && positive[survivor] > 0:
		return true, fmt.Sprintf("all %d directories stay with the surviving instance %s while %d services are registered with the shared peer (distribution %v)", len(directories), survivor, registered, distribution), nil
	default:
		return false, "", fmt.Errorf("another fault: the directories are held by %v (survivor %s), distribution %v", held, survivor, distribution)
	}
}
