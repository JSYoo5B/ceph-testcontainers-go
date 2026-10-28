package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// terminableFacts returns a cluster whose Terminate runs without Docker: both
// bridges are recorded as already removed, so teardown does not touch them,
// and no container is attached. Terminate still takes the topology lock, sets
// the closed flag and runs the real teardown path.
func terminableFacts() *Container {
	c := factsFixture()
	c.nets.publicRemoved, c.nets.clusterRemoved = true, true
	c.settings.hostNetwork = false
	c.settings.imageSet = &ImageSet{Essential: "e", Client: "c", CephFS: "f", RBDAux: "r", RGW: "g", All: "a"}
	return c
}

// TestClusterFactsTerminateKeepsRecordedFacts proves that the real Terminate
// leaves every recorded source of the snapshot as it was, and that a snapshot
// taken after it equals one taken before.
func TestClusterFactsTerminateKeepsRecordedFacts(t *testing.T) {
	c := terminableFacts()
	before, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	settings, public, cluster, version := c.settings, *c.nets.public, *c.nets.cluster, c.bootstrap.cephVersion
	imageSet := *c.settings.imageSet

	if err := c.Terminate(t.Context()); err != nil {
		t.Fatalf("fixture terminate: %v", err)
	}
	if !c.closed {
		t.Fatal("fixture did not run the real termination")
	}
	after, err := FactsOf(c)
	if err != nil {
		t.Fatalf("capture after termination: %v", err)
	}
	if after != before {
		t.Fatalf("termination changed the snapshot: %+v != %+v", after, before)
	}
	if !reflect.DeepEqual(c.settings, settings) || *c.nets.public != public || *c.nets.cluster != cluster ||
		c.bootstrap.cephVersion != version || *c.settings.imageSet != imageSet {
		t.Fatal("termination rewrote a recorded source of the static facts")
	}
}

// TestClusterFactsSnapshotSurvivesTermination checks that a snapshot obtained
// before termination is unchanged after it and does not follow later changes.
func TestClusterFactsSnapshotSurvivesTermination(t *testing.T) {
	c := terminableFacts()
	held, _ := FactsOf(c)
	want := held
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if held != want {
		t.Fatalf("held snapshot changed by termination: %+v", held)
	}
}

// TestClusterFactsConcurrentTerminateRace captures snapshots while Terminate
// runs. Under -race it fails if any recorded source is written during
// teardown; every snapshot must equal the pre-termination one.
func TestClusterFactsConcurrentTerminateRace(t *testing.T) {
	for range 20 {
		c := terminableFacts()
		want, _ := FactsOf(c)
		var wg sync.WaitGroup
		var bad atomic.Int32
		start := make(chan struct{})
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 200 {
					got, err := FactsOf(c)
					if err != nil || got != want {
						bad.Add(1)
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := c.Terminate(t.Context()); err != nil {
				bad.Add(1)
			}
		}()
		close(start)
		wg.Wait()
		if bad.Load() != 0 {
			t.Fatalf("%d captures failed or differed during termination", bad.Load())
		}
		if got, err := FactsOf(c); err != nil || got != want {
			t.Fatalf("capture after concurrent termination: %+v, %v", got, err)
		}
	}
}

// TestClusterFactsUnrecordedStaysUnavailable checks that a cluster whose
// startup stopped before the version or networks were recorded reports the
// empty value, not a default presented as an observation.
func TestClusterFactsUnrecordedStaysUnavailable(t *testing.T) {
	c := &Container{settings: options{addressFamily: AddressIPv4}}
	c.closed = true
	got, err := FactsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	if got.CephVersion != "" || got.NetworkName != "" || got.ClusterNetworkName != "" || got.HasSeparateClusterNetwork ||
		got.Images != (ImageSet{}) {
		t.Fatalf("unrecorded facts were filled in: %+v", got)
	}
}

// TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup guards the
// premise that FactsOf needs no lock: the fields it reads are assigned only
// while a cluster is being built, never by a method that can run beside
// Terminate. A write elsewhere must either move before publication or add a
// synchronized read to FactsOf and update this allowlist.
func TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup(t *testing.T) {
	allowed := map[string]bool{
		"Run":               true, // builds the cluster before returning it
		"recordCephVersion": true, // called only from Run before publication
		// The unit-test setter of the recorded version, a plain write for a cluster the
		// caller owns or holds the lock of.
		"UnitSetCephVersion": true,
		"UnitCluster":        true, // builds a fresh unit-test cluster
		"UnitState":          true, // unit-test pointer hooks (config, keyring, version, bridge flag)
		// Unit-test option setters, the only writers of c.settings after Run. They replace
		// the settings pointers UnitState used to publish; only UnitSetCephFSImage writes
		// a recorded fact (cephfsOverride), the others write fields FactsOf never reads.
		"UnitSetInitialOSDs":    true,
		"UnitSetPoolDefaults":   true,
		"UnitSetStartupTimeout": true,
		"UnitSetCephFSImage":    true,
		"scanRecordedWrites":    true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		found = append(found, scanRecordedWrites(t, file, allowed)...)
	}
	if len(found) != 0 {
		t.Fatalf("recorded fact sources are written outside startup: %v", found)
	}
}

// recordedFactFields are the Container fields FactsOf reads directly, and the
// options fields it reads through c.settings.
var recordedContainerFields = map[string]bool{"nets": true, "settings": true}

// recordedNetworkFields are the networkState fields FactsOf reads. The removal
// flags next to them are guarded by Container.mu and are not facts.
var recordedNetworkFields = map[string]bool{"public": true, "cluster": true, "publicSubnet": true, "clusterSubnet": true}
var recordedSettingsFields = map[string]bool{
	"addressFamily": true, "essentialImage": true, "imageSet": true, "cephfsOverride": true, "rgwOverride": true,
	"messengerMode": true, "hostNetwork": true, "publicAddress": true,
}

// scanRecordedWrites reports assignments, increments and address-of
// expressions that target a recorded source on a Container, outside the
// allowed functions. A write through the settings value of an options
// parameter (an Option) happens before the cluster exists and is not flagged.
func scanRecordedWrites(t *testing.T, file string, allowed map[string]bool) []string {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || allowed[fn.Name.Name] {
			continue
		}
		recv := networkStateReceiver(fn)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			var targets []ast.Expr
			switch s := n.(type) {
			case *ast.AssignStmt:
				targets = s.Lhs
			case *ast.IncDecStmt:
				targets = []ast.Expr{s.X}
			case *ast.UnaryExpr:
				if s.Op == token.AND {
					targets = []ast.Expr{s.X}
				}
			}
			if call, ok := n.(*ast.CallExpr); ok && recordsVersion(call) {
				out = append(out, fset.Position(call.Pos()).String())
			}
			for _, target := range targets {
				if recordedSourceTarget(target) || (recv != "" && networkStateTarget(target, recv)) {
					out = append(out, fset.Position(target.Pos()).String())
				}
			}
			return true
		})
	}
	return out
}

// recordsVersion reports whether call is X.bootstrap.recordVersion(...), the
// method that stores the recorded Ceph version on the bootstrap record.
func recordsVersion(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "recordVersion" {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "bootstrap" && isContainerReceiver(inner.X)
}

// networkStateReceiver returns the receiver name of a networkState method, or
// the empty string for any other function.
func networkStateReceiver(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok && id.Name == "networkState" {
		return fn.Recv.List[0].Names[0].Name
	}
	return ""
}

// networkStateTarget reports whether e writes a recorded bridge or subnet
// through the receiver of a networkState method.
func networkStateTarget(e ast.Expr, recv string) bool {
	for {
		switch v := e.(type) {
		case *ast.ParenExpr:
			e = v.X
		case *ast.IndexExpr:
			e = v.X
		case *ast.StarExpr:
			e = v.X
		case *ast.SelectorExpr:
			if id, ok := v.X.(*ast.Ident); ok && id.Name == recv && recordedNetworkFields[v.Sel.Name] {
				return true
			}
			e = v.X
		default:
			return false
		}
	}
}

// recordedSourceTarget reports whether e names a recorded Container source:
// X.bootstrap.cephVersion, X.nets (and anything under it), X.settings, X.settings.<fact
// field> (also nested, such as X.settings.imageSet.RGW), or a field of the
// pointed-to network (X.nets.public.Name).
func recordedSourceTarget(e ast.Expr) bool {
	for {
		switch v := e.(type) {
		case *ast.ParenExpr:
			e = v.X
		case *ast.IndexExpr:
			e = v.X
		case *ast.StarExpr:
			e = v.X
		case *ast.SelectorExpr:
			if recordedContainerFields[v.Sel.Name] && isContainerReceiver(v.X) {
				return true
			}
			// X.bootstrap.cephVersion, the recorded Ceph version.
			if x, ok := v.X.(*ast.SelectorExpr); ok && v.Sel.Name == "cephVersion" && x.Sel.Name == "bootstrap" && isContainerReceiver(x.X) {
				return true
			}
			if x, ok := v.X.(*ast.SelectorExpr); ok && x.Sel.Name == "settings" && isContainerReceiver(x.X) && recordedSettingsFields[v.Sel.Name] {
				return true
			}
			e = v.X
		default:
			return false
		}
	}
}

// isContainerReceiver accepts the identifiers the package uses for a cluster.
func isContainerReceiver(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && (id.Name == "c" || id.Name == "cluster" || id.Name == "owner")
}
