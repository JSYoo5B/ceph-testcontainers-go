//go:build all || integration

package harness

import (
	"os"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

func NetemImage(t *testing.T) string {
	t.Helper()
	image := os.Getenv("CEPH_TEST_NETEM_IMAGE")
	if image == "" {
		image = fault.NetemHelperImage
	}
	t.Logf("netem helper: %s", image)
	return image
}
