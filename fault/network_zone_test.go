package fault

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// A zone profile is refused before any helper starts when the request cannot
// name one router's egress: no cluster, a cluster without zones, another
// direction or no helper image.
func TestApplyZoneProfileRefusals(t *testing.T) {
	netem := newFakeNetem()
	cluster, _ := delayCluster(netem)
	inbound := testDelay
	inbound.Direction = Inbound
	noHelper := testDelay
	noHelper.HelperImage = ""
	for name, tc := range map[string]struct {
		cluster *ceph.Container
		delay   NetworkProfile
		want    error
	}{
		"no cluster":           {nil, testDelay, ceph.ErrInvalidRequest},
		"a cluster sans zones": {cluster.Container, testDelay, ceph.ErrInvalidRequest},
		"inbound":              {cluster.Container, inbound, ceph.ErrInvalidRequest},
		"no helper image":      {cluster.Container, noHelper, ceph.ErrImageCapability},
	} {
		lease, err := ApplyZoneProfile(t.Context(), tc.cluster, "a", tc.delay)
		if !errors.Is(err, tc.want) || lease != nil {
			t.Errorf("%s: got %v, lease %v, want %v", name, err, lease, tc.want)
		}
	}
	if len(netem.helpers) != 0 || len(netem.commands) != 0 {
		t.Fatalf("a refusal started a helper or ran %v", netem.commands)
	}
}
