package ceph

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestStaticFactsNilContainerKeepsDocumentedDefaults(t *testing.T) {
	var c *Container
	if c.AddressFamily() != AddressIPv4 || c.EssentialImage() != "" || c.Images() != (ImageSet{}) ||
		c.MessengerMode() != MessengerDefault || c.CephVersion() != "" {
		t.Fatalf("nil container facts changed: %v %q %v %v %q", c.AddressFamily(), c.EssentialImage(), c.Images(), c.MessengerMode(), c.CephVersion())
	}
	for name, fact := range map[string]func(){
		"NetworkName":               func() { c.NetworkName() },
		"UsesHostNetwork":           func() { c.UsesHostNetwork() },
		"PublicAddress":             func() { c.PublicAddress() },
		"ClusterNetworkName":        func() { c.ClusterNetworkName() },
		"HasSeparateClusterNetwork": func() { c.HasSeparateClusterNetwork() },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("nil container %s no longer panics", name)
				}
			}()
			fact()
		}()
	}
}

func TestStaticFactsNetworkNames(t *testing.T) {
	public := &testcontainers.DockerNetwork{Name: "public-net"}
	cluster := &testcontainers.DockerNetwork{Name: "cluster-net"}
	cases := []struct {
		name                           string
		c                              *Container
		network, clusterName, publicIP string
		separate, host                 bool
	}{
		{"no network", &Container{}, "", "", "", false, false},
		{"single bridge", &Container{network: public}, "public-net", "public-net", "", false, false},
		{"separate bridge", &Container{network: public, clusterNetwork: cluster}, "public-net", "cluster-net", "", true, false},
		{"bridge hides address", &Container{network: public, settings: options{publicAddress: "192.0.2.1"}}, "public-net", "public-net", "", false, false},
		{"host mode", &Container{network: public, settings: options{hostNetwork: true, publicAddress: "192.0.2.1"}}, "host", "host", "192.0.2.1", false, true},
		{"host mode with cluster bridge", &Container{network: public, clusterNetwork: cluster, settings: options{hostNetwork: true}}, "host", "cluster-net", "", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.c
			if c.NetworkName() != tc.network || c.ClusterNetworkName() != tc.clusterName || c.PublicAddress() != tc.publicIP ||
				c.HasSeparateClusterNetwork() != tc.separate || c.UsesHostNetwork() != tc.host {
				t.Fatalf("got %q %q %q %v %v", c.NetworkName(), c.ClusterNetworkName(), c.PublicAddress(), c.HasSeparateClusterNetwork(), c.UsesHostNetwork())
			}
		})
	}
}

func TestStaticFactsImagesCopyAndDefaults(t *testing.T) {
	defaults := &Container{settings: options{essentialImage: "ctl", cephfsOverride: "mds", rgwOverride: "rgw"}}
	if got, want := defaults.Images(), (ImageSet{Essential: "ctl", Client: "ctl", CephFS: "mds", RBDAux: "ctl", RGW: "rgw"}); got != want {
		t.Fatalf("default images = %+v, want %+v", got, want)
	}
	selected := &ImageSet{Essential: "e", Client: "c", CephFS: "f", RBDAux: "r", RGW: "g"}
	c := &Container{settings: options{essentialImage: "ctl", imageSet: selected}}
	got := c.Images()
	got.Essential = "changed"
	if *selected != c.Images() || c.Images().Essential != "e" {
		t.Fatalf("Images aliases the selected set: %+v", c.Images())
	}
}
