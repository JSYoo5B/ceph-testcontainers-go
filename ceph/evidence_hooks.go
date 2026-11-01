package ceph

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// evidenceHooks gives the diagnostics and finalize packages the inventory of
// a cluster, which is not public API. Its methods are exported only so that
// internal/evidencehook can call them through an interface; the type itself
// is unexported and never appears in the public packages.
type evidenceHooks struct{}

func init() {
	hookreg.Evidence = evidenceHooks{}
}

// Inventory takes the inventory of c; see evidenceInventory.
func (evidenceHooks) Inventory(ctx context.Context, c *Container) (evidence.Inventory, error) {
	return c.evidenceInventory(ctx)
}

// Owner returns the ID that labels the cluster's containers.
func (evidenceHooks) Owner(c *Container) string { return c.startup.ownerID() }

// NativeFailure describes a failed native command as the module's commands do.
func (evidenceHooks) NativeFailure(program string, code int, stdout, stderr []byte) *NativeFailure {
	return nativeFailure(program, code, stdout, stderr)
}

// evidenceInventory takes the inventory of every tracked container, network
// and secret. It never blocks past ctx: each lock is polled. It works on a
// partially started or terminated cluster, and an error returns what was
// taken so far.
func (c *Container) evidenceInventory(ctx context.Context) (evidence.Inventory, error) {
	inv := evidence.Inventory{}
	if c == nil {
		return inv, errors.New("Ceph cluster is nil")
	}
	if err := diagnosticLock(ctx, c.mu.TryLock); err != nil {
		return inv, err
	}
	inv.Closed, inv.HostNetwork, inv.PublicAddress = c.closed, c.settings.hostNetwork, c.settings.publicAddress
	inv.Control = evidence.Handle(c.Container)
	add := func(role, name string, ctr testcontainers.Container) {
		inv.Members = append(inv.Members, evidence.Member{Role: role, Name: name, Container: evidence.Handle(ctr)})
	}
	topology := c.monitorTopology()
	topology.diagnosticDaemons(c.closed, add)
	c.osdFleet().diagnosticOSDs(add)
	topology.diagnosticServices(add)
	for _, state := range extensionsWith[serviceSecrets](c) {
		inv.Secrets = append(inv.Secrets, state.Secrets()...)
	}
	// Every client key the module generated, including replaced, attempted
	// and revoked ones.
	inv.Secrets = append(inv.Secrets, c.ledger.authRecords.secretValues()...)
	for _, nw := range c.nets.bridges() {
		inv.Networks = append(inv.Networks, evidence.Network{ID: nw.ID, Name: nw.Name})
	}
	c.mu.Unlock()
	control, err := c.controlPlane.diagnosticHandle(ctx)
	if err != nil {
		return inv, err
	}
	if control != nil {
		inv.Control = evidence.Handle(control)
		inv.ControlPlane = inv.Control
		add("control", "cli", inv.Control)
	}
	client, err := c.toolkit.diagnosticSnapshot(ctx)
	if err != nil {
		return inv, err
	}
	if client != nil {
		add("client", "toolkit", client)
	}
	keyring, err := c.bootstrap.diagnosticKeyring(ctx)
	if err != nil {
		return inv, err
	}
	inv.Secrets = append(inv.Secrets, evidence.KeyringSecrets(keyring)...)
	return inv, nil
}

// diagnosticLock polls try until it succeeds or ctx ends.
func diagnosticLock(ctx context.Context, try func() bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if try() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
