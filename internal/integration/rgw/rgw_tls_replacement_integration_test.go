//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWTLSReplacement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if mode.Host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			testRGWTLSReplacement(t, opts...)
		})
	}
}
