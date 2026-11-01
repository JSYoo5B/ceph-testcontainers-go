package cephfs

import (
	"go/ast"
	"go/types"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// mdsFleetBindings are the fields of mdsFleet and filesystemDescriptor with the
// Filesystem field each one must be bound to, by address, in Filesystem.fleet.
// core is bound to the Filesystem's own foundation core, fsbase.Of(fs).
var mdsFleetBindings = map[string]string{
	"core": "", "config": "config", "mds": "mds", "first": "Container",
	"name": "FilesystemName", "metadataPool": "MetadataPool", "dataPool": "DataPool",
}

// TestMDSFleetIsBuiltOnlyByFilesystemFleet guards the shape written on
// mdsFleet: Filesystem.fleet is its only constructor, a single return of one
// literal that binds each field once to the address of the matching Filesystem
// field, and neither the fleet nor its descriptor declares a field that can hold
// a Filesystem. So the fleet acts for exactly one filesystem and cannot reach the
// rest of it.
func TestMDSFleetIsBuiltOnlyByFilesystemFleet(t *testing.T) {
	var problems []string
	constructors := 0
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || (ts.Name.Name != "mdsFleet" && ts.Name.Name != "filesystemDescriptor") {
						continue
					}
					ast.Inspect(ts.Type, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok && id.Name == "Filesystem" {
							problems = append(problems, file+": "+ts.Name.Name+" declares a field that holds a Filesystem")
						}
						return true
					})
				}
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				isConstructor := decl.Name.Name == "fleet" && rosterReceiverName(decl) == "Filesystem"
				literals := 0
				ast.Inspect(decl.Body, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok {
						return true
					}
					if id, ok := lit.Type.(*ast.Ident); ok && (id.Name == "mdsFleet" || id.Name == "filesystemDescriptor") {
						if !isConstructor {
							problems = append(problems, file+": "+decl.Name.Name+" builds a "+id.Name+" literal; use Filesystem.fleet")
						}
						literals++
					}
					return true
				})
				if !isConstructor {
					continue
				}
				constructors++
				if len(decl.Body.List) != 1 {
					problems = append(problems, "Filesystem.fleet must be a single return")
				}
				if literals != 2 {
					problems = append(problems, "Filesystem.fleet must build one mdsFleet and one filesystemDescriptor literal")
				}
				recv := decl.Recv.List[0].Names[0].Name
				bound := map[string]int{}
				ast.Inspect(decl.Body, func(n ast.Node) bool {
					kv, ok := n.(*ast.KeyValueExpr)
					if !ok {
						return true
					}
					key := kv.Key.(*ast.Ident).Name
					if _, isLit := kv.Value.(*ast.CompositeLit); isLit {
						return true
					}
					want, known := mdsFleetBindings[key]
					if !known {
						problems = append(problems, "Filesystem.fleet binds unknown field "+key)
						return true
					}
					wantExpr := "&" + recv + "." + want
					if key == "core" {
						wantExpr = "fsbase.Of(" + recv + ")"
					}
					if got := types.ExprString(kv.Value); got != wantExpr {
						problems = append(problems, "Filesystem.fleet binds "+key+" to "+got+", want "+wantExpr)
					}
					bound[key]++
					return true
				})
				for key := range mdsFleetBindings {
					if bound[key] != 1 {
						problems = append(problems, "Filesystem.fleet binds "+key+" "+strconv.Itoa(bound[key])+" times, want once")
					}
				}
			}
		}
	}
	if constructors != 1 {
		problems = append(problems, "want exactly one Filesystem.fleet")
	}
	if len(problems) != 0 {
		t.Fatalf("mdsFleet construction: %v", problems)
	}
}

// TestMDSStateIsReachedOnlyThroughTheFleet guards the ownership written on
// mdsState and the first-MDS slot: outside Filesystem.fleet, which takes their
// addresses, only methods of mdsFleet name them, and only through their own
// receiver. A Filesystem method that reads the roster or re-seats the embedded
// container directly fails this test.
func TestMDSStateIsReachedOnlyThroughTheFleet(t *testing.T) {
	var problems []string
	uses := map[string]int{}
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			recvType := rosterReceiverName(fn)
			recv := ""
			if fn.Recv != nil && len(fn.Recv.List[0].Names) == 1 {
				recv = fn.Recv.List[0].Names[0].Name
			}
			isConstructor := fn.Name.Name == "fleet" && recvType == "Filesystem"
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if node.Sel.Name != "mds" && node.Sel.Name != "first" {
						return true
					}
					if isConstructor {
						return true
					}
					if x, ok := node.X.(*ast.Ident); ok && recvType == "mdsFleet" && x.Name == recv {
						uses[node.Sel.Name]++
						return true
					}
					problems = append(problems, file+": "+fn.Name.Name+" reaches "+types.ExprString(node)+" outside the MDS fleet")
				case *ast.AssignStmt:
					for _, lhs := range node.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Container" && recvType == "Filesystem" {
							if x, ok := sel.X.(*ast.Ident); ok && x.Name == recv {
								problems = append(problems, file+": "+fn.Name.Name+" re-seats the embedded MDS container outside the fleet")
							}
						}
					}
				}
				return true
			})
		}
	}
	for _, field := range []string{"mds", "first"} {
		if uses[field] == 0 {
			problems = append(problems, "no fleet use of "+field+" found: the guard is vacuous")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("MDS state escapes the fleet: %s", strings.Join(problems, "; "))
	}
}

// TestFleetRegisteredFollowsTheFilesystem checks that the identity predicate
// the fleet uses agrees with the Filesystem it acts for: a replacement entry of
// the same name, or a closed cluster, refuses the fleet as well.
func TestFleetRegisteredFollowsTheFilesystem(t *testing.T) {
	fs, _ := subvolumeFixture()
	fleet := fs.fleet()
	if !fleet.core.Registered() {
		t.Fatal("the fleet of a live filesystem is not registered")
	}
	replacement := newFilesystem(fs.base().Cluster(), Config{Name: fs.base().Name()}, nil)
	filesystems(fs.base().Cluster()).byName[fs.base().Name()] = replacement
	if fleet.core.Registered() || !replacement.fleet().core.Registered() {
		t.Fatal("a replaced registry entry still admits the original fleet")
	}
}
