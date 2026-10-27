package ceph

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

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
		{"single bridge", &Container{nets: networkState{public: public}}, "public-net", "public-net", "", false, false},
		{"separate bridge", &Container{nets: networkState{public: public, cluster: cluster}}, "public-net", "cluster-net", "", true, false},
		{"bridge hides address", &Container{nets: networkState{public: public}, settings: options{publicAddress: "192.0.2.1"}}, "public-net", "public-net", "", false, false},
		{"host mode", &Container{nets: networkState{public: public}, settings: options{hostNetwork: true, publicAddress: "192.0.2.1"}}, "host", "host", "192.0.2.1", false, true},
		{"host mode with cluster bridge", &Container{nets: networkState{public: public, cluster: cluster}, settings: options{hostNetwork: true}}, "host", "cluster-net", "", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			facts, err := FactsOf(tc.c)
			if err != nil {
				t.Fatal(err)
			}
			if facts.NetworkName != tc.network || facts.ClusterNetworkName != tc.clusterName || facts.PublicAddress != tc.publicIP ||
				facts.HasSeparateClusterNetwork != tc.separate || facts.UsesHostNetwork != tc.host {
				t.Fatalf("got %q %q %q %v %v", facts.NetworkName, facts.ClusterNetworkName, facts.PublicAddress, facts.HasSeparateClusterNetwork, facts.UsesHostNetwork)
			}
		})
	}
}

func TestStaticFactsImagesCopyAndDefaults(t *testing.T) {
	defaults := &Container{settings: options{essentialImage: "ctl", cephfsOverride: "mds", rgwOverride: "rgw"}}
	defaultFacts, err := FactsOf(defaults)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := defaultFacts.Images, (ImageSet{Essential: "ctl", Client: "ctl", CephFS: "mds", RBDAux: "ctl", RGW: "rgw"}); got != want {
		t.Fatalf("default images = %+v, want %+v", got, want)
	}
	selected := &ImageSet{Essential: "e", Client: "c", CephFS: "f", RBDAux: "r", RGW: "g"}
	c := &Container{settings: options{essentialImage: "ctl", imageSet: selected}}
	facts, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	facts.Images.Essential = "changed"
	again, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	if *selected != again.Images || again.Images.Essential != "e" {
		t.Fatalf("Images aliases the selected set: %+v", again.Images)
	}
}
