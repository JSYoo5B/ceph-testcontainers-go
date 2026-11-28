package evidence

import (
	"errors"
	"slices"
	"testing"
)

func TestSnapshotOf(t *testing.T) {
	inv := Inventory{
		Members:  []Member{{Role: "mon", Name: "a"}, {Role: "osd", Name: "0"}, {Role: "mgr", Name: "x"}},
		Networks: []Network{{ID: "n", Name: "public"}}, Closed: true, PublicAddress: "192.0.2.1",
		Secrets: []string{"cluster-key"},
	}
	snapshot, err := SnapshotOf(inv, nil, []Member{{Role: "extra", Name: "mirror"}}, []string{"configured"}, 3)
	if err != nil || len(snapshot.Containers) != 3 || snapshot.Omitted != 1 || !snapshot.Closed || snapshot.PublicAddress != "192.0.2.1" ||
		len(snapshot.Networks) != 1 || !slices.Equal(snapshot.Secrets, []string{"configured", "cluster-key"}) {
		t.Fatalf("snapshot %+v, %v", snapshot, err)
	}
	// An inventory error keeps the state and secrets taken so far and
	// selects nothing, not even the additional containers.
	boom := errors.New("inventory failed")
	snapshot, err = SnapshotOf(inv, boom, []Member{{Role: "extra", Name: "mirror"}}, []string{"configured"}, MaxContainers)
	if !errors.Is(err, boom) || snapshot.Containers != nil || !snapshot.Closed || !slices.Equal(snapshot.Secrets, []string{"configured", "cluster-key"}) {
		t.Fatalf("snapshot %+v, %v", snapshot, err)
	}
}
