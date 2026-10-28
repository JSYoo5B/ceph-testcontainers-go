package ceph

import (
	"errors"
	"reflect"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func factsFixture() *Container {
	return &Container{
		nets: networkState{public: &testcontainers.DockerNetwork{Name: "public-net"},
			cluster: &testcontainers.DockerNetwork{Name: "cluster-net"}},
		bootstrap: bootstrapRecord{cephVersion: "20.2.4"},
		settings: options{
			addressFamily:  AddressDualStack,
			essentialImage: "essential:1",
			cephfsOverride: "mds:1",
			rgwOverride:    "rgw:1",
			messengerMode:  MessengerV2Only,
		},
	}
}

func TestClusterFactsMatchRecordedSettings(t *testing.T) {
	c := factsFixture()
	f, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	want := ClusterFacts{
		AddressFamily: AddressDualStack,
		Images:        ImageSet{Essential: "essential:1", Client: "essential:1", CephFS: "mds:1", RBDAux: "essential:1", RGW: "rgw:1"},
		MessengerMode: MessengerV2Only, NetworkName: "public-net", ClusterNetworkName: "cluster-net",
		HasSeparateClusterNetwork: true, CephVersion: "20.2.4",
	}
	if f != want {
		t.Fatalf("snapshot %+v differs from the recorded settings %+v", f, want)
	}
	if f != c.staticFacts() {
		t.Fatal("FactsOf and the in-package read differ")
	}
	if f.NetworkName != "public-net" || f.ClusterNetworkName != "cluster-net" || !f.HasSeparateClusterNetwork || f.CephVersion != "20.2.4" {
		t.Fatalf("fixture values not captured: %+v", f)
	}
}

func TestClusterFactsNilClusterReturnsErrorAndZeroValue(t *testing.T) {
	f, err := FactsOf(nil)
	if err == nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil cluster error = %v, want an invalid request", err)
	}
	if f != (ClusterFacts{}) {
		t.Fatalf("nil cluster returned a non-zero snapshot: %+v", f)
	}
}

func TestClusterFactsCaptureAfterTerminationSucceeds(t *testing.T) {
	c := factsFixture()
	before, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	// terminate sets closed first and keeps the recorded settings, version and
	// network records.
	c.closed = true
	after, err := FactsOf(c)
	if err != nil {
		t.Fatalf("capture after termination failed: %v", err)
	}
	if after != before {
		t.Fatalf("termination changed the recorded facts: %+v != %+v", after, before)
	}
}

func TestClusterFactsClosedFlagIsNotEvidence(t *testing.T) {
	open, closed := factsFixture(), factsFixture()
	closed.closed = true
	a, errA := FactsOf(open)
	b, errB := FactsOf(closed)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("closed flag influenced capture: %+v %v / %+v %v", a, errA, b, errB)
	}
	// A cluster that is closed before teardown completed must not be reported
	// differently from one that is not: the snapshot carries no cleanup claim.
	typ := reflect.TypeOf(ClusterFacts{})
	for i := 0; i < typ.NumField(); i++ {
		switch typ.Field(i).Name {
		case "Closed", "Terminated", "Cleaned", "Removed":
			t.Fatalf("snapshot field %s implies cleanup state", typ.Field(i).Name)
		}
	}
}

func TestClusterFactsAreIndependentCopies(t *testing.T) {
	c := factsFixture()
	first, _ := FactsOf(c)
	second, _ := FactsOf(c)

	first.Images.Essential, first.NetworkName, first.CephVersion = "changed", "changed", "changed"
	first.Images.RGW = "changed"
	first.UsesHostNetwork = true
	if second.Images.Essential != "essential:1" || second.NetworkName != "public-net" || second.Images.RGW != "rgw:1" || second.UsesHostNetwork {
		t.Fatalf("snapshots share state: %+v", second)
	}
	if c.settings.essentialImage != "essential:1" || c.nets.public.Name != "public-net" || c.bootstrap.cephVersion != "20.2.4" || c.settings.hostNetwork {
		t.Fatal("mutating a snapshot changed the cluster")
	}

	// Aggregate image selections are held by pointer in the settings; the
	// snapshot must not alias that ImageSet.
	c = factsFixture()
	c.settings.imageSet = &ImageSet{Essential: "e", Client: "c", CephFS: "f", RBDAux: "r", RGW: "g", All: "a"}
	snap, _ := FactsOf(c)
	snap.Images.Essential = "changed"
	if c.settings.imageSet.Essential != "e" {
		t.Fatal("snapshot aliases the cluster image set")
	}

	// Later cluster changes do not update an obtained snapshot.
	c.bootstrap.cephVersion = "21.0.0"
	if snap.CephVersion != "20.2.4" {
		t.Fatalf("snapshot followed the cluster: %q", snap.CephVersion)
	}
}

// TestClusterFactsHoldNoControlPath fails when a snapshot field could reach
// the cluster or share mutable data: any pointer, interface, func, channel,
// slice, map, or unsafe pointer, at any depth. A future slice or map must be
// cloned and this guard updated together with the clone.
func TestClusterFactsHoldNoControlPath(t *testing.T) {
	var walk func(path string, typ reflect.Type)
	walk = func(path string, typ reflect.Type) {
		switch typ.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer, reflect.Slice, reflect.Map:
			t.Errorf("%s has reference kind %s", path, typ.Kind())
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				if !f.IsExported() {
					t.Errorf("%s.%s is unexported", path, f.Name)
				}
				walk(path+"."+f.Name, f.Type)
			}
		case reflect.Array:
			walk(path+"[]", typ.Elem())
		}
	}
	walk("ClusterFacts", reflect.TypeOf(ClusterFacts{}))
	if reflect.TypeOf(ClusterFacts{}).Implements(reflect.TypeOf((*interface{ Terminate() })(nil)).Elem()) {
		t.Fatal("snapshot has cluster-control methods")
	}
	if reflect.TypeOf(ClusterFacts{}).NumMethod() != 0 || reflect.TypeOf(&ClusterFacts{}).NumMethod() != 0 {
		t.Fatal("snapshot has methods")
	}
}
