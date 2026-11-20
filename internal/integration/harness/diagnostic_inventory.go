package harness

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// diagnosticContainers reads current owned handles with the cleanup's budget.
// Unavailable inventory is logged; it must not prevent fixture release.
func diagnosticContainers(t testing.TB, ctx context.Context, cluster *ceph.Container) []testcontainers.Container {
	t.Helper()
	ctrs := []testcontainers.Container{cluster.Container}
	ctrs = append(ctrs, diagnosticRead(t, ctx, "control", cluster.ControlContainer))
	for _, monitor := range diagnosticRead(t, ctx, "MONs", cluster.Monitors) {
		if monitor != nil {
			ctrs = append(ctrs, monitor.Container)
		}
	}
	for _, manager := range diagnosticRead(t, ctx, "MGRs", cluster.Managers) {
		if manager != nil {
			ctrs = append(ctrs, manager.Container)
		}
	}
	for _, osd := range diagnosticRead(t, ctx, "OSDs", cluster.OSDs) {
		if osd != nil {
			ctrs = append(ctrs, osd.Container)
		}
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		t.Logf("diagnostic service observer unavailable: %v", err)
	} else {
		ctrs = append(ctrs, diagnosticRead(t, ctx, "services", observer.ServiceContainers)...)
	}
	return ctrs
}

func diagnosticRead[T any](t testing.TB, ctx context.Context, name string, read func(context.Context) (T, error)) T {
	t.Helper()
	result, err := read(ctx)
	if err != nil {
		t.Logf("diagnostic %s inventory unavailable: %v", name, err)
	}
	return result
}
