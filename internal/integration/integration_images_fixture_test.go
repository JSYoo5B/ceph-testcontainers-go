//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestIntegrationImage(t *testing.T) {
	const control, osd = "sha256:" + "1111111111111111111111111111111111111111111111111111111111111111", "sha256:" + "2222222222222222222222222222222222222222222222222222222222222222"
	for name, test := range map[string]struct {
		env      map[string]string
		variable string
		want     string
		err      string
	}{
		"v2 core alias":                       {map[string]string{"SCENARIO_IMAGE_CONTRACT": "roles-v2", "CEPH_TEST_ESSENTIAL_IMAGE": control}, "CEPH_TEST_OSD_IMAGE", control, ""},
		"v2 missing feature never falls back": {map[string]string{"SCENARIO_IMAGE_CONTRACT": "roles-v2", "CEPH_TEST_ESSENTIAL_IMAGE": control}, "CEPH_TEST_CEPHFS_IMAGE", "", "CEPH_TEST_CEPHFS_IMAGE is required by roles-v2"},
		"local default":                       {map[string]string{}, "CEPH_TEST_OSD_IMAGE", ceph.DefaultImage, ""},
		"local control fallback":              {map[string]string{"CEPH_TEST_IMAGE": control}, "CEPH_TEST_MDS_IMAGE", control, ""},
		"local role image":                    {map[string]string{"CEPH_TEST_IMAGE": control, "CEPH_TEST_OSD_IMAGE": osd}, "CEPH_TEST_OSD_IMAGE", osd, ""},
		"declared control role":               {map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles", "SCENARIO_IMAGE_ROLES": "control", "CEPH_TEST_IMAGE": control}, "CEPH_TEST_IMAGE", control, ""},
		"undeclared role in CI":               {map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles", "SCENARIO_IMAGE_ROLES": "control", "CEPH_TEST_IMAGE": control}, "CEPH_TEST_RGW_IMAGE", "", `CEPH_TEST_RGW_IMAGE is not set: role-image CI prepared the roles "control"`},
		"missing control role in CI":          {map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles"}, "CEPH_TEST_IMAGE", "", "CEPH_TEST_IMAGE is not set"},
	} {
		image, err := integrationImage(func(key string) string { return test.env[key] }, test.variable)
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
