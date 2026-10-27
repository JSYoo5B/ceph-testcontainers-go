package ceph

import (
	"reflect"
	"testing"
)

// TestContainerHasNoStaticFactAccessors fails when an exported method of
// *Container or *Tenant returns again a fact that ClusterFacts carries: the
// facts are read only through FactsOf.
func TestContainerHasNoStaticFactAccessors(t *testing.T) {
	removed := []string{
		"AddressFamily", "CephVersion", "ClusterNetworkName", "EssentialImage", "HasSeparateClusterNetwork",
		"Images", "MessengerMode", "NetworkName", "PublicAddress", "UsesHostNetwork",
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(&Container{}), reflect.TypeOf(&Tenant{})} {
		for _, name := range removed {
			if _, ok := typ.MethodByName(name); ok {
				t.Errorf("%s has the static fact accessor %s; read it through FactsOf", typ, name)
			}
		}
	}
}

// TestClusterFactsServeTheEssentialImageAfterTermination pins the facts that
// the removed EssentialImage and Images accessors served: both stay readable
// from a terminated cluster, and the essential image is the aggregate's
// Essential role.
func TestClusterFactsServeTheEssentialImageAfterTermination(t *testing.T) {
	c := factsFixture()
	c.closed = true
	facts, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Images.Essential != c.settings.essentialImage || facts.Images.Essential != "essential:1" {
		t.Fatalf("essential image %q", facts.Images.Essential)
	}
	want := ImageSet{Essential: "essential:1", Client: "essential:1", CephFS: "mds:1", RBDAux: "essential:1", RGW: "rgw:1"}
	if facts.Images != want {
		t.Fatalf("images %+v, want %+v", facts.Images, want)
	}
}

// TestStaticFactsEqualsFactsOfForAStartingCluster pins that the in-package read
// used during startup sees what FactsOf returns, including a cluster whose
// networks and version are not recorded yet.
func TestStaticFactsEqualsFactsOfForAStartingCluster(t *testing.T) {
	for name, c := range map[string]*Container{"recorded": factsFixture(), "empty": {}} {
		facts, err := FactsOf(c)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if facts != c.staticFacts() {
			t.Fatalf("%s: FactsOf %+v differs from the in-package read %+v", name, facts, c.staticFacts())
		}
	}
}
