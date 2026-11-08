//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_test

import (
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

func TestNamespaceWatchRequiresPrimaryAndFreshStoppedSample(t *testing.T) {
	boundary := time.Now()
	for _, ns := range []string{"ns-a", "ns-c"} {
		pinned := rbdmirror.ImageStatus{Name: "volume", SourceNamespace: ns, SourcePrimary: true}
		for _, role := range []struct{ source, destination, ok bool }{{true, false, true}, {false, false, false}, {true, true, false}} {
			s := pinned
			s.SourcePrimary, s.DestinationPrimary = role.source, role.destination
			if err := checkScopedWatchImage(pinned, s); (err == nil) != role.ok {
				t.Fatalf("%s source=%v destination=%v: %v", ns, role.source, role.destination, err)
			}
		}
	}
	for _, tc := range []struct {
		name              string
		kind              ceph.WatchEventKind
		started, finished time.Duration
		ready, ok         bool
	}{
		{"buffered non-ready", ceph.WatchChanged, -2 * time.Second, -time.Second, false, false},
		{"straddles stop", ceph.WatchChanged, -time.Second, time.Second, false, false},
		{"new gap", ceph.WatchGap, time.Second, 2 * time.Second, false, false},
		{"still replaying", ceph.WatchChanged, time.Second, 2 * time.Second, true, false},
		{"stopped", ceph.WatchChanged, time.Second, 2 * time.Second, false, true},
		{"resumed stopped", ceph.WatchResumed, time.Second, 2 * time.Second, false, true},
	} {
		event := rbdmirror.ImageWatchEvent{Kind: tc.kind, Started: boundary.Add(tc.started), Finished: boundary.Add(tc.finished), Status: rbdmirror.ImageStatus{ReplayReady: tc.ready}}
		if got := scopedImageObservedStop(event, boundary); got != tc.ok {
			t.Fatalf("%s: got %t, want %t", tc.name, got, tc.ok)
		}
	}
}
