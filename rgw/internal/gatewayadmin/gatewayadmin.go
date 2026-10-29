// Package gatewayadmin is the internal seam through which rgw/harness reaches
// the administration of a gateway that package rgw keeps private: the owner
// cluster, the ownership check, the bounded context and the radosgw-admin
// command. Package rgw sets Of when it is initialized. The seam is internal
// wiring of the module's own packages (the internal directory keeps it from
// any other importer) and adds no public name; it is not service
// registration.
package gatewayadmin

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Admin is what the RGW tenant adapter may do with one gateway.
type Admin interface {
	// Owner returns the cluster that owns the gateway.
	Owner() *ceph.Container
	// Validate fails when the cluster is terminated or no longer owns the
	// gateway.
	Validate() error
	// Context bounds one administration call by the cluster's startup timeout.
	Context(ctx context.Context) (context.Context, context.CancelFunc)
	// Command runs radosgw-admin in the gateway under the shell supervisor.
	Command(ctx context.Context, args ...string) ([]byte, error)
}

// Of is set once, during the initialization of package rgw. It returns nil for
// a value that is not a gateway.
var Of func(gateway any) Admin

// For returns the administration of gateway, or panics naming the missing
// registrant.
func For(gateway any) Admin {
	if Of == nil {
		panic("gatewayadmin: package rgw did not register its gateway administration")
	}
	return Of(gateway)
}
