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
	c.networkRemoved, c.clusterNetworkRemoved = true, true
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
	settings, public, cluster, version := c.settings, *c.network, *c.clusterNetwork, c.cephVersion
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
	if !reflect.DeepEqual(c.settings, settings) || *c.network != public || *c.clusterNetwork != cluster ||
		c.cephVersion != version || *c.settings.imageSet != imageSet {
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
		got.Images != (ImageSet{}) || got.ControlImage != "" {
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
		"Run":                true, // builds the cluster before returning it
		"recordCephVersion":  true, // called only from Run before publication
		"TestCluster":        true, // builds a fresh unit-test cluster
		"TestState":          true, // unit-test pointer hooks
		"scanRecordedWrites": true,
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
var recordedContainerFields = map[string]bool{"cephVersion": true, "network": true, "clusterNetwork": true, "settings": true}
var recordedSettingsFields = map[string]bool{
	"addressFamily": true, "controlImage": true, "imageSet": true, "mdsImage": true, "rgwImage": true,
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
			for _, target := range targets {
				if recordedSourceTarget(target) {
					out = append(out, fset.Position(target.Pos()).String())
				}
			}
			return true
		})
	}
	return out
}

// recordedSourceTarget reports whether e names a recorded Container source:
// X.cephVersion, X.network, X.clusterNetwork, X.settings, X.settings.<fact
// field> (also nested, such as X.settings.imageSet.RGW), or a field of the
// pointed-to network (X.network.Name).
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
