package ceph

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
)

// monStoreGenerationsFake is a small, explicit stand-in for the container side
// of a MON generation. It records every call in order, and models the Docker
// fake's view of which containers exist. Its answers are synthetic: it says
// nothing about how a daemon creates, starts, stops or removes a container.
type monStoreGenerationsFake struct {
	docker      *monStoreDockerFake
	events      []string
	launches    []monStoreLaunch
	images      []string
	next        int
	createErr   error
	partialID   string // returned with createErr when set
	readyErr    error
	removeErr   error
	removeLies  bool // Remove reports success and leaves the container
	removeCalls int
}

func (g *monStoreGenerationsFake) Create(_ context.Context, launch monStoreLaunch, image string) (string, error) {
	g.events = append(g.events, fmt.Sprintf("create gen%d %s %s", launch.generation, image, map[monStoreMode]string{monStoreFormat: "format", monStoreRequire: "require"}[launch.mode]))
	g.launches = append(g.launches, launch)
	g.images = append(g.images, image)
	if g.createErr != nil {
		if g.partialID != "" {
			g.docker.existing[g.partialID] = true
		}
		return g.partialID, g.createErr
	}
	g.next++
	id := fmt.Sprintf("ctr-%d", g.next)
	g.docker.existing[id] = true
	return id, nil
}

func (g *monStoreGenerationsFake) WaitReady(_ context.Context, id string) error {
	g.events = append(g.events, "ready "+id)
	return g.readyErr
}

func (g *monStoreGenerationsFake) Remove(_ context.Context, id string) error {
	g.removeCalls++
	g.events = append(g.events, "remove "+id)
	if g.removeErr != nil {
		return g.removeErr
	}
	if !g.removeLies {
		delete(g.docker.existing, id)
	}
	return nil
}

func monStoreGenerationFixture(t *testing.T) (*monStoreVolume, *monStoreDockerFake, *monStoreGenerationsFake) {
	t.Helper()
	_, r, d := newMonStoreFixture()
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, d, &monStoreGenerationsFake{docker: d}
}

// monStoreRunningFixture has a first generation, from image "old", that
// reached its ready check.
func monStoreRunningFixture(t *testing.T) (*monStoreVolume, *monStoreDockerFake, *monStoreGenerationsFake) {
	t.Helper()
	r, d, g := monStoreGenerationFixture(t)
	if _, err := r.startGeneration(t.Context(), g, "old"); err != nil {
		t.Fatal(err)
	}
	return r, d, g
}

func TestMonStoreFirstGenerationFormatsAndIsReadyWithoutAnIdentity(t *testing.T) {
	r, d, g := monStoreGenerationFixture(t)
	id, err := r.startGeneration(t.Context(), g, "old")
	if err != nil || id != "ctr-1" {
		t.Fatalf("%q %v", id, err)
	}
	if want := []string{"create gen1 old format", "ready ctr-1"}; !slices.Equal(g.events, want) {
		t.Fatalf("events %v, want %v", g.events, want)
	}
	if g.launches[0].volume != r.name || r.phase != monStoreBound || r.identity != nil || r.attachment == nil || r.attachment.pending || r.attachment.id != "ctr-1" {
		t.Fatalf("state after the first generation: phase %s identity %v attachment %+v", r.phase, r.identity, r.attachment)
	}
	if d.creates != 1 {
		t.Fatalf("volume creates %d", d.creates)
	}
}

func TestMonStoreReplaceRemovesTheOldContainerThenStartsTheNewImageOnTheSameVolume(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	creates, volume := d.creates, r.name
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err != nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	want := []string{"create gen1 old format", "ready ctr-1", "remove ctr-1", "create gen2 new require", "ready ctr-2"}
	if !slices.Equal(g.events, want) {
		t.Fatalf("events %v, want %v", g.events, want)
	}
	if g.launches[0].volume != g.launches[1].volume || g.launches[1].volume != volume || r.name != volume {
		t.Fatalf("the generations did not mount one volume: %+v", g.launches)
	}
	if d.creates != creates || d.removals != 0 {
		t.Fatalf("the replacement created %d or removed %d volumes", d.creates-creates, d.removals)
	}
	if r.phase != monStoreBound || r.generation != 2 || r.attachment == nil || r.attachment.id != "ctr-2" {
		t.Fatalf("state after the replacement: phase %s generation %d attachment %+v", r.phase, r.generation, r.attachment)
	}
	// And again, with the image going back: nothing depends on the direction.
	if _, err := r.replaceGeneration(t.Context(), g, "old"); err != nil || r.generation != 3 || g.launches[2].mode != monStoreRequire {
		t.Fatalf("third generation: %v %+v", err, g.launches)
	}
}

func TestMonStoreReplaceDoesNotStartWhileTheOldContainerMayStillUseTheVolume(t *testing.T) {
	cases := map[string]func(d *monStoreDockerFake, g *monStoreGenerationsFake){
		"remove fails": func(_ *monStoreDockerFake, g *monStoreGenerationsFake) {
			g.removeErr = errors.New("daemon unreachable")
		},
		"remove reports success":   func(_ *monStoreDockerFake, g *monStoreGenerationsFake) { g.removeLies = true },
		"another container mounts": func(d *monStoreDockerFake, _ *monStoreGenerationsFake) { d.users = []string{"stray"} },
		"list fails":               func(d *monStoreDockerFake, _ *monStoreGenerationsFake) { d.listErr = errors.New("daemon unreachable") },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			r, d, g := monStoreRunningFixture(t)
			apply(d, g)
			if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil {
				t.Fatal("the replacement went on")
			}
			if len(g.launches) != 1 || r.generation != 1 || r.attachment == nil {
				t.Fatalf("a second container was dispatched: launches %d generation %d attachment %+v", len(g.launches), r.generation, r.attachment)
			}
		})
	}
	// Control: with none of those the same call succeeds.
	r, _, g := monStoreRunningFixture(t)
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err != nil {
		t.Fatal(err)
	}
}

func TestMonStoreReplaceChecksTheNextGenerationBeforeItTouchesTheOldOne(t *testing.T) {
	cases := map[string]func(r *monStoreVolume, d *monStoreDockerFake){
		"volume missing":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.inspectErr = errdefs.ErrNotFound },
		"volume foreign":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.stored.Labels[monStoreOwnerLabel] = "foreign" },
		"volume changed":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.stored.CreatedAt = "replaced-volume" },
		"identity not verified": func(r *monStoreVolume, _ *monStoreDockerFake) { r.identity = &monStoreIdentity{} },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			r, d, g := monStoreRunningFixture(t)
			apply(r, d)
			if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil {
				t.Fatal("the replacement went on")
			}
			if g.removeCalls != 0 || len(g.launches) != 1 || r.attachment == nil || d.creates != 1 {
				t.Fatalf("the old generation was touched: removes %d launches %d creates %d", g.removeCalls, len(g.launches), d.creates)
			}
		})
	}
}

func TestMonStoreReplaceIsRefusedWithoutAReadyGeneration(t *testing.T) {
	r, _, g := monStoreGenerationFixture(t) // created, no generation yet
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.events) != 0 {
		t.Fatalf("a store without a generation was replaced: %v %v", err, g.events)
	}
	r, _, g = monStoreGenerationFixture(t)
	g.readyErr = errors.New("never ready")
	if _, err := r.startGeneration(t.Context(), g, "old"); err == nil || r.phase != monStoreFresh {
		t.Fatalf("a first generation that never got ready: %v, phase %s", err, r.phase)
	}
	g.events = nil
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.events) != 0 {
		t.Fatalf("a fresh store was replaced: %v %v", err, g.events)
	}
}

func TestMonStoreFailedNewGenerationKeepsTheVolumeAndTheRecordAndNeverFormats(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	g.readyErr = errors.New("new image never got ready")
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err == nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	if r.phase != monStoreBound || r.attachment == nil || r.attachment.id != "ctr-2" || d.removals != 0 {
		t.Fatalf("state after the failed start: phase %s attachment %+v removals %d", r.phase, r.attachment, d.removals)
	}
	// The failed container is removed and the next generation, from the old
	// image again, requires the same store.
	g.readyErr = nil
	if err := r.stopGeneration(t.Context(), g); err != nil || r.attachment != nil {
		t.Fatalf("stop the failed generation: %v", err)
	}
	if _, err := r.startGeneration(t.Context(), g, "old"); err != nil {
		t.Fatal(err)
	}
	for i, l := range g.launches[1:] {
		if l.mode != monStoreRequire || l.volume != r.name {
			t.Fatalf("launch %d: %+v", i+1, l)
		}
	}
}

func TestMonStoreUnknownCreateOutcomeIsNotReplayedAndAPartialContainerIsRecorded(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.createErr = errors.New("reply lost")
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || r.attachment == nil || !r.attachment.pending {
		t.Fatalf("the lost reply: %v, attachment %+v", err, r.attachment)
	}
	creates := len(g.launches)
	g.createErr = nil
	if _, err := r.startGeneration(t.Context(), g, "new"); err == nil || len(g.launches) != creates {
		t.Fatalf("an unknown outcome was replayed: %v", err)
	}
	if err := r.stopGeneration(t.Context(), g); err == nil {
		t.Fatal("a pending attachment was stopped without an ID")
	}
	// A partial container: the ID is recorded, so it can be removed.
	r, _, g = monStoreRunningFixture(t)
	g.createErr, g.partialID = errors.New("start failed"), "ctr-partial"
	if id, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || id != "ctr-partial" || r.attachment == nil || r.attachment.pending || r.attachment.id != "ctr-partial" {
		t.Fatalf("the partial container: %q %v %+v", id, err, r.attachment)
	}
	if err := r.stopGeneration(t.Context(), g); err != nil || r.attachment != nil {
		t.Fatalf("remove the partial container: %v", err)
	}
}

func TestMonStoreGenerationsRefuseABadImageAndANilContainerSide(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	if _, err := r.replaceGeneration(t.Context(), g, ""); err == nil || g.removeCalls != 0 {
		t.Fatalf("an empty image: %v", err)
	}
	var typedNil *monStoreGenerationsFake
	if _, err := r.replaceGeneration(t.Context(), typedNil, "new"); err == nil {
		t.Fatal("a typed nil container side was accepted")
	}
	if _, err := r.replaceGeneration(t.Context(), nil, "new"); err == nil {
		t.Fatal("a nil container side was accepted")
	}
	if err := r.stopGeneration(t.Context(), nil); err == nil {
		t.Fatal("stop accepted a nil container side")
	}
}

// A store bound with an identity (the verified path of the inspector slices)
// keeps its refusal: a generation needs the verification.
func TestMonStoreIdentityBoundStoreStillNeedsItsVerification(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	g := &monStoreGenerationsFake{docker: d}
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || g.removeCalls != 0 {
		t.Fatalf("an unverified identity-bound store was replaced: %v", err)
	}
	if err := r.verify(monStoreIdentityFixture()); err == nil {
		// verify needs no attachment; the live attachment keeps it unverified.
		t.Fatal("verify accepted a store with a live attachment")
	}
	// With the old container gone the store is still bound with its identity,
	// and a dispatch without a verification is refused; with one it is admitted.
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err == nil || r.attachment != nil {
		t.Fatalf("an unverified identity-bound store was dispatched: %v", err)
	}
	if err := r.verify(monStoreIdentityFixture()); err != nil {
		t.Fatal(err)
	}
	if launch, err := r.dispatch(t.Context()); err != nil || launch.mode != monStoreRequire {
		t.Fatalf("a verified store: %+v %v", launch, err)
	}
}

func TestMonStoreReadyNeedsAFreshStoreWithAKnownAttachment(t *testing.T) {
	_, r, _ := newMonStoreFixture()
	if err := r.ready(); err == nil {
		t.Fatal("a planned store was marked ready")
	}
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err == nil {
		t.Fatal("a created store was marked ready")
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err == nil || r.phase != monStoreFresh {
		t.Fatalf("a store with a pending attachment was marked ready: %v", err)
	}
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err != nil || r.phase != monStoreBound || r.identity != nil {
		t.Fatalf("ready: %v phase %s", err, r.phase)
	}
}

// ---- guards ----

var monStoreGenerationFile = "mon_store_generation.go"

// monStoreGenerationSymbols are the names the file adds. Production code
// outside it must not mention them until a slice adds a caller and updates this
// table in its own diff.
var monStoreGenerationSymbols = []string{"monStoreGenerations", "checkMonStoreGenerations", "startGeneration", "stopGeneration", "replaceGeneration"}

func monStoreGenerationProblems(files map[string]*ast.File) (problems []string) {
	for file, parsed := range files {
		if file == monStoreGenerationFile {
			continue
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && slices.Contains(monStoreGenerationSymbols, id.Name) {
				problems = append(problems, file+" mentions "+id.Name)
			}
			return true
		})
	}
	return problems
}

func TestMonStoreGenerationsHaveNoProductionCaller(t *testing.T) {
	files := parseProductionFiles(t)
	if files[monStoreGenerationFile] == nil {
		t.Fatalf("%s not found: the guard is vacuous", monStoreGenerationFile)
	}
	if problems := monStoreGenerationProblems(files); len(problems) != 0 {
		t.Fatalf("the generation path is used in production: %v", problems)
	}
	declared := map[string]bool{}
	ast.Inspect(files[monStoreGenerationFile], func(n ast.Node) bool {
		switch d := n.(type) {
		case *ast.FuncDecl:
			declared[d.Name.Name] = true
		case *ast.TypeSpec:
			declared[d.Name.Name] = true
		}
		return true
	})
	for _, symbol := range monStoreGenerationSymbols {
		if !declared[symbol] {
			t.Errorf("%s is in the table but the file does not declare it", symbol)
		}
	}
}

func TestMonStoreGenerationsGuardDetectsACaller(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", "package ceph\nfunc use(r *monStoreVolume) { _, _ = r.replaceGeneration(nil, nil, \"\") }\n", 0)
	if err != nil {
		t.Fatal(err)
	}
	if problems := monStoreGenerationProblems(map[string]*ast.File{"synthetic.go": parsed}); len(problems) != 1 {
		t.Fatalf("problems %v", problems)
	}
}

// TestMonStoreGenerationsAddNoLockAndKeepTheirOrder pins that the file imports
// no sync package, that the container side is called only from the three
// generation methods, and that replaceGeneration checks, then stops, then
// starts, in that order.
func TestMonStoreGenerationsAddNoLockAndKeepTheirOrder(t *testing.T) {
	parsed := parseProductionFiles(t)[monStoreGenerationFile]
	for _, imp := range parsed.Imports {
		if strings.HasPrefix(imp.Path.Value, `"sync`) {
			t.Errorf("the file imports %s", imp.Path.Value)
		}
	}
	portCalls := map[string]string{"Create": "startGeneration", "WaitReady": "startGeneration", "Remove": "stopGeneration"}
	positions := map[string]token.Pos{}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if want, isPort := portCalls[sel.Sel.Name]; isPort {
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == "g" && fn.Name.Name != want {
					t.Errorf("%s calls the container side's %s", fn.Name.Name, sel.Sel.Name)
				}
			}
			if fn.Name.Name == "replaceGeneration" {
				switch sel.Sel.Name {
				case "inspect", "stopGeneration", "startGeneration":
					positions[sel.Sel.Name] = call.Pos()
				}
			}
			return true
		})
	}
	if !(positions["inspect"].IsValid() && positions["inspect"] < positions["stopGeneration"] && positions["stopGeneration"] < positions["startGeneration"]) {
		t.Fatalf("replaceGeneration must check, stop, then start: %v", positions)
	}
}
