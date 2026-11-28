package evidence

import (
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// MaxContainers bounds the containers of one collection: the cluster's own
// together with the additional ones a caller named.
const MaxContainers = 128

// Snapshot is what one collection takes from the inventory of a cluster:
// the containers it reads, as descriptions of type T, the cluster's state
// and the secrets to redact.
type Snapshot[T Described] struct {
	// Containers are the cluster's members and the additional containers,
	// selected by Inventory.Select; Omitted counts those cut at the limit.
	Containers []T
	Omitted    int
	Networks   []Network
	// Control and ControlPlane are those of the Inventory.
	Control, ControlPlane testcontainers.Container
	HostNetwork, Closed   bool
	PublicAddress         string
	// Secrets are the configured values followed by the cluster's own.
	Secrets []string
}

// SnapshotOf takes the snapshot of one collection from inv: the members of
// inv and additional, at most limit, and the secrets values together with
// the cluster's. err is the error of taking inv: the snapshot then keeps
// what inv holds so far, enough to redact the error and report whether the
// cluster was closed, selects no container, and returns err.
func SnapshotOf[T Described](inv Inventory, err error, additional []T, values []string, limit int) (Snapshot[T], error) {
	snapshot := Snapshot[T]{
		Networks: inv.Networks, Control: inv.Control, ControlPlane: inv.ControlPlane,
		HostNetwork: inv.HostNetwork, Closed: inv.Closed, PublicAddress: inv.PublicAddress,
		Secrets: Secrets(inv, values),
	}
	if err != nil {
		return snapshot, err
	}
	snapshot.Containers, snapshot.Omitted = SelectAs(inv, additional, limit)
	return snapshot, nil
}

// Secrets returns values followed by the secrets of inv.
func Secrets(inv Inventory, values []string) []string {
	return append(slices.Clone(values), inv.Secrets...)
}
