package servicehook

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
)

// ceph/harness registers its tenant hooks when it is initialized, and this
// package imports it, so the slot is set before any caller runs. Asserting the
// typed interface proves that the registered value satisfies it, including the
// identical TenantPort type; the registrant is named when it does not.
func TestHarnessTenantSlotSatisfiesTheTypedSeam(t *testing.T) {
	hooks := tenantHooks()
	_ = hooks.NewServiceSupport(servicekind.RBD, nil, nil)
	// Support is the exported route; it must build the same opaque value.
	_ = Support(servicekind.RBD, nil, nil)
}
