package multicluster

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func splitCluster() *ceph.Container {
	return testhelper.TestCluster(nil, testhelper.TestConfig{Images: &ceph.ImageSet{
		Essential: "essential:1", Client: "client:1", CephFS: "cephfs:1", RBDAux: "aux:1", RGW: "rgw:1"}})
}

func TestRequireRoleImageRefusesAnotherRoleOfASplitCluster(t *testing.T) {
	cluster := splitCluster()
	for _, test := range []struct{ image, role, other string }{
		{"essential:1", RoleRBDAux, "essential"},
		{"client:1", RoleRBDAux, "client"},
		{"essential:1", RoleCephFS, "essential"},
		{"aux:1", RoleCephFS, RoleRBDAux},
		{"client:1", RoleRGW, "client"},
	} {
		err := RequireRoleImage(test.image, test.role, cluster)
		if err == nil || !strings.Contains(err.Error(), test.other+" image") {
			t.Fatalf("%s image for role %s: %v", test.other, test.role, err)
		}
	}
}

func TestRequireRoleImageAcceptsTheRoleAggregatesAndCustomImages(t *testing.T) {
	split := splitCluster()
	aggregate := testhelper.TestCluster(nil, testhelper.TestConfig{})
	for _, test := range []struct {
		image, role string
		clusters    []*ceph.Container
	}{
		{"aux:1", RoleRBDAux, []*ceph.Container{split}},
		{"cephfs:1", RoleCephFS, []*ceph.Container{split}},
		{"rgw:1", RoleRGW, []*ceph.Container{split}},
		{"custom:patched", RoleRBDAux, []*ceph.Container{split}},
		{"unit-all", RoleRBDAux, []*ceph.Container{aggregate}},
		{"unit-all", RoleCephFS, []*ceph.Container{aggregate, nil}},
	} {
		if err := RequireRoleImage(test.image, test.role, test.clusters...); err != nil {
			t.Fatalf("%s for %s: %v", test.image, test.role, err)
		}
	}
	if err := RequireRoleImage("x", "mds", split); err == nil {
		t.Fatal("the retired daemon-named role was accepted")
	}
}
