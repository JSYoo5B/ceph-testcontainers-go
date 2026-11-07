//go:build all || integration

package cephfs_test

import (
	"fmt"
	"strings"
	"time"
)

// Snapshots returns names oldest first. The scheduler uses the same UTC
// timestamp for its native name and Last, with an optional UTC name suffix.
func scheduleRetainsNewestTwo(names []string, creations []time.Time) bool {
	if len(names) != 2 || len(creations) < 2 {
		return false
	}
	for i, created := range creations[len(creations)-2:] {
		if strings.TrimSuffix(names[i], "_UTC") != "scheduled-"+created.UTC().Format("2006-01-02-15_04_05") {
			return false
		}
	}
	return true
}

func checkCephFSRemovalDispatch(states map[string]string, sent []string, target string) error {
	if _, ok := states[target]; !ok {
		return fmt.Errorf("no current policy observation for remove %s: %v", target, states)
	}
	for _, previous := range sent {
		if _, present := states[previous]; present {
			return fmt.Errorf("remove %s while previously removed %s remains in policy: %v", target, previous, states)
		}
	}
	for path, state := range states {
		if state != "mapped" && state != "stalled" {
			return fmt.Errorf("remove %s while %s has state %q", target, path, state)
		}
	}
	return nil
}

func cephFSReceiptFilePresent(code int, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch code {
	case 0:
		return true, nil
	case 1:
		return false, nil
	default:
		return false, fmt.Errorf("test -e exited %d", code)
	}
}

// The sampling interval can miss the edges of up:reconnect. A duration
// near the ordinary window must still fail even when the override reads back.
func cephFSShortReconnectWindow(held, shortened, ordinary time.Duration) bool {
	return shortened > 0 && shortened < ordinary && held >= shortened/2 && held < (ordinary+shortened)/2
}
