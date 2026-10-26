package multicluster

import (
	"errors"
	"strconv"
)

// NormalizeInitialMirrorDaemonCount applies the same count admission before runtime access and in normalization.
// Explicit zero construction does not change DaemonCount=0's legacy default.
func NormalizeInitialMirrorDaemonCount(count int, noInitial bool) (int, error) {
	if count < 0 {
		return 0, errors.New("mirror daemon count must not be negative")
	}
	if noInitial {
		if count != 0 {
			return 0, errors.New("mirror NoInitialDaemons requires DaemonCount=0")
		}
		return 0, nil
	}
	if count == 0 {
		return 1, nil
	}
	return count, nil
}

// InitialMirrorDaemonName returns the fixture name of the index-th (zero-based)
// initial mirror daemon: "a" through "z", then "node-27" and onward. RBD and
// CephFS publish the same sequence, which their daemon-name validators accept.
func InitialMirrorDaemonName(index int) string {
	if index < 26 {
		return string(rune('a' + index))
	}
	return "node-" + strconv.Itoa(index+1)
}
