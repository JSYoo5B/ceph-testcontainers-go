package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// harnessHooks is the core side of the contract that internal/harnesshook
// types: the error construction and classification, the start of a shared
// cluster and the tenant Observer constructor. Its methods are exported only
// so that internal/harnesshook can call them through an interface; the type
// itself is unexported and never appears in a public package. It spells no
// internal/harnesshook type, because that package imports ceph.
type harnessHooks struct{}

func init() {
	hookreg.Harness = harnessHooks{}
}

// Invalid marks err as a refused request with ErrInvalidRequest, with the one
// construction that invalidRequest uses.
func (harnessHooks) Invalid(err error) error { return newInvalid(err) }

// InvalidRequest is invalidRequest.
func (harnessHooks) InvalidRequest(format string, args ...any) error {
	return invalidRequest(format, args...)
}

// Classify is classify.
func (harnessHooks) Classify(kind, err error) error { return classify(kind, err) }

// ClassifyDocker is classifyDocker.
func (harnessHooks) ClassifyDocker(ctx context.Context, err error) error {
	return classifyDocker(ctx, err)
}

// Start starts a cluster for a shared harness and returns it as a backend
// value, or nil.
func (harnessHooks) Start(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (any, error) {
	return startSharedContainer(ctx, image, opts)
}

// NewTenantObserver is newLeasedObserver.
func (harnessHooks) NewTenantObserver(target func() (*Container, error), live func() error) *Observer {
	return newLeasedObserver(target, live)
}

// Lend returns the gated cluster of a backend value, or nil for a value that
// is not a container backend.
func (harnessHooks) Lend(backend any) *Container {
	if b, ok := backend.(containerBackend); ok {
		return b.c
	}
	return nil
}
