package multicluster

import "errors"

// Use the same count admission before runtime access and in normalization.
// Explicit zero construction does not change DaemonCount=0's legacy default.
func normalizeInitialMirrorDaemonCount(count int, noInitial bool) (int, error) {
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
