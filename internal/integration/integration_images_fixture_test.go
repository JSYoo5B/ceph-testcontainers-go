//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestIntegrationImage(t *testing.T) {
	const essential, client = "sha256:" + "1111111111111111111111111111111111111111111111111111111111111111", "sha256:" + "2222222222222222222222222222222222222222222222222222222222222222"
	for name, test := range map[string]struct {
		env      map[string]string
		variable string
		want     string
		err      string
	}{
		"essential":         {map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": essential}, "CEPH_TEST_ESSENTIAL_IMAGE", essential, ""},
		"missing feature":   {map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": essential}, "CEPH_TEST_CEPHFS_IMAGE", "", "CEPH_TEST_CEPHFS_IMAGE is required"},
		"local default":     {map[string]string{}, "CEPH_TEST_CLIENT_IMAGE", ceph.DefaultImage, ""},
		"aggregate":         {map[string]string{"CEPH_TEST_IMAGE": essential}, "CEPH_TEST_CLIENT_IMAGE", essential, ""},
		"selected client":   {map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": essential, "CEPH_TEST_CLIENT_IMAGE": client}, "CEPH_TEST_CLIENT_IMAGE", client, ""},
		"undeclared role":   {map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles", "CEPH_TEST_ESSENTIAL_IMAGE": essential}, "CEPH_TEST_RGW_IMAGE", "", "CEPH_TEST_RGW_IMAGE is required"},
		"missing essential": {map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles"}, "CEPH_TEST_ESSENTIAL_IMAGE", "", "CEPH_TEST_ESSENTIAL_IMAGE is required"},
	} {
		image, err := harness.IntegrationImage(func(key string) string { return test.env[key] }, test.variable)
		if test.err != "" {
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Errorf("%s: image %q, error %v, want %q", name, image, err, test.err)
			}
			continue
		}
		if err != nil || image != test.want {
			t.Errorf("%s: image %q, error %v, want %q", name, image, err, test.want)
		}
	}
}
