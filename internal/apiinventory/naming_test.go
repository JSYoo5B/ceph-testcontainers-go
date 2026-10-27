package apiinventory_test

import (
	"flag"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

var (
	updateNaming   = flag.Bool("update-api-naming", false, "regenerate the source-derived declaration and member catalogues; never writes a review receipt")
	namingPackages = flag.String("api-naming-packages", "", "with -update-api-naming: comma-separated packages to regenerate (default all)")
)

var (
	surfaceOnce sync.Once
	surfaceList []string
	surfaceData map[string]catalogue.Surface
	surfaceErr  error
	updateOnce  sync.Once
	updateErr   error
)

func config() catalogue.Config {
	return catalogue.Config{Root: root, Proposals: catalogue.Proposals}
}

func publicPackages(t *testing.T) []string {
	t.Helper()
	packages, err := catalogue.Discover(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(packages) == 0 {
		t.Fatal("no public package found")
	}
	return packages
}

// currentSurfaces regenerates once on request (-update-api-naming), which
// writes declarations.json and members.json only, and then derives the
// source surface that every check compares against.
func currentSurfaces(t *testing.T) ([]string, map[string]catalogue.Surface) {
	t.Helper()
	if *updateNaming {
		updateOnce.Do(func() {
			var named []string
			if *namingPackages != "" {
				named = strings.Split(*namingPackages, ",")
			}
			_, updateErr = catalogue.Regenerate(config(), named)
		})
		if updateErr != nil {
			t.Fatal(updateErr)
		}
	}
	surfaceOnce.Do(func() {
		surfaceList, surfaceErr = catalogue.Discover(root)
		if surfaceErr == nil {
			surfaceData, surfaceErr = catalogue.Generate(config(), surfaceList)
		}
	})
	if surfaceErr != nil {
		t.Fatal(surfaceErr)
	}
	return surfaceList, surfaceData
}

// Every public package has a catalogue directory with all three files, and no
// catalogue directory exists for a package that does not.
func TestPublicNamingLayout(t *testing.T) {
	currentSurfaces(t)
	if err := catalogue.CheckLayout(root); err != nil {
		t.Fatal(err)
	}
}

// This is a review baseline, not a claim that every baseline name was approved.
// Missing/stale rows, signature drift, duplicate names and colliding proposals
// fail the normal suite; refreshing requires an explicit opt-in flag.
func TestPublicNamingInventory(t *testing.T) {
	packages, surfaces := currentSurfaces(t)
	for _, pkg := range packages {
		t.Run(pkg, func(t *testing.T) {
			if err := catalogue.CheckDeclarations(root, pkg, surfaces[pkg]); err != nil {
				t.Fatalf("%v; run go test ./internal/apiinventory -run '^TestPublicNaming(Inventory|Members)$' -update-api-naming -api-naming-packages=%s", err, pkg)
			}
		})
	}
}

func TestPublicNamingMembers(t *testing.T) {
	packages, surfaces := currentSurfaces(t)
	for _, pkg := range packages {
		t.Run(pkg, func(t *testing.T) {
			if err := catalogue.CheckMembers(root, pkg, surfaces[pkg]); err != nil {
				t.Fatalf("%v; run go test ./internal/apiinventory -run '^TestPublicNaming(Inventory|Members)$' -update-api-naming -api-naming-packages=%s", err, pkg)
			}
		})
	}
}

func TestNamingMemberScope(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", `package synthetic
 type Record struct {
  A, B int
  Hidden struct { UUID string; private int }
  Rows []struct { Name string }
  Index map[string]struct { ID int }
  *external.Container
  privateEmbedding
  private struct { Exposed string }
 }
 type Endpoint interface { Read([]byte) (int, error); external.Closer }
 type private struct { Public string }
 `, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalogue.MembersInFile("synthetic", file)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"synthetic.Record": {
			"Record.A field int", "Record.B field int",
			"Record.Hidden field struct {\n\tUUID    string\n\tprivate int\n}", "Record.Hidden.UUID field string",
			"Record.Index field map[string]struct{ ID int }", "Record.Index[].ID field int",
			"Record.Rows field []struct{ Name string }", "Record.Rows[].Name field string",
			"Record embed *external.Container", "Record embed privateEmbedding",
		},
		"synthetic.Endpoint": {"Endpoint.Read method func([]byte) (int, error)", "Endpoint embed external.Closer"},
	}
	for _, entries := range want {
		slices.Sort(entries)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("member scope differs:\ngot: %#v\nwant: %#v", got, want)
	}
}

// Regenerating the catalogues must never silently approve new names. Each
// package's separately issued receipt pins the reviewed declaration and member
// snapshots of that package alone: a source change in package A makes only A's
// receipt stale.
func TestNamingReviewBindsCurrentSurface(t *testing.T) {
	packages, surfaces := currentSurfaces(t)
	for _, pkg := range packages {
		t.Run(pkg, func(t *testing.T) {
			if err := catalogue.CheckReceipt(root, pkg, surfaces[pkg]); err != nil {
				t.Fatal(err)
			}
		})
	}
}
