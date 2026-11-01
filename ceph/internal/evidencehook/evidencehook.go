// Package evidencehook gives the diagnostics and finalize packages the parts
// of a ceph.Container that are not public API: the inventory of the
// containers, networks and secrets that it tracks, the ID that labels its
// containers and the description of a failed native command. Package ceph
// registers them in internal/hookreg when it is initialized.
package evidencehook

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

type hooks interface {
	Inventory(ctx context.Context, c *ceph.Container) (evidence.Inventory, error)
	Owner(c *ceph.Container) string
	NativeFailure(program string, code int, stdout, stderr []byte) *ceph.NativeFailure
}

func get() hooks {
	h, ok := hookreg.Evidence.(hooks)
	if !ok {
		panic("evidencehook: package ceph did not register its hooks")
	}
	return h
}

// Inventory takes the inventory of c without blocking past ctx. It works on a
// partially started or terminated cluster; an error returns what was taken
// so far.
func Inventory(ctx context.Context, c *ceph.Container) (evidence.Inventory, error) {
	return get().Inventory(ctx, c)
}

// Owner returns the ID that labels the containers of c, empty for a cluster
// value that Run did not create.
func Owner(c *ceph.Container) string { return get().Owner(c) }

// NativeFailure describes a failed native command as the commands of package
// ceph do, with its errno and redacted reason.
func NativeFailure(program string, code int, stdout, stderr []byte) *ceph.NativeFailure {
	return get().NativeFailure(program, code, stdout, stderr)
}
