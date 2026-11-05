//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestHealthWatchRestartedOSDStateNeedsExactlyOneUpAndInEntry(t *testing.T) {
	const id = 1
	up := ceph.OSDState{ID: id, Up: true, In: true}
	other := ceph.OSDState{ID: 0, Up: true, In: true}
	if err := checkRestartedOSDState([]ceph.OSDState{other, up}, id); err != nil {
		t.Fatalf("one up and in entry: %v", err)
	}
	for name, states := range map[string][]ceph.OSDState{
		"empty":     nil,
		"missing":   {other},
		"duplicate": {up, other, up},
		"down":      {other, {ID: id, In: true}},
		"out":       {other, {ID: id, Up: true}},
	} {
		if err := checkRestartedOSDState(states, id); err == nil {
			t.Errorf("%s: %+v passed", name, states)
		}
	}
}
