package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

func TestOSDStorageStateReleaseStagesForgetOnlyAfterSuccess(t *testing.T) {
	mem, memDocker, memKeeper := newOSDMemoryUnitFixture()
	vol, volDocker, volHelper := osdVolumeFixture()
	s := osdStorageState{memory: mem, volume: vol}

	// OSDs remain: nothing is released and nothing is forgotten.
	if err := s.releaseMemory(t.Context(), false); err != nil || s.memory != mem || memKeeper.terminations != 0 {
		t.Fatalf("memory released while OSDs remain: %v", err)
	}
	if err := s.releaseVolume(t.Context(), false); err != nil || s.volume != vol || volHelper.terminations != 0 {
		t.Fatalf("volume released while OSDs remain: %v", err)
	}

	// A failed release keeps the record, so the next stage can repeat it.
	busy := errors.New("referenced volume")
	memDocker.removeErr, volDocker.removeErr = busy, busy
	memoryErr, volumeErr := s.releaseMemory(t.Context(), true), s.releaseVolume(t.Context(), true)
	if !errors.Is(memoryErr, busy) || !errors.Is(volumeErr, busy) || s.memory != mem || s.volume != vol {
		t.Fatalf("failed release forgot a record: %v, %v", memoryErr, volumeErr)
	}

	// The retry stage runs only for a stage that failed and only when the OSDs
	// are gone; it returns the previous error untouched otherwise.
	removals := memDocker.removals
	if got := s.retryMemory(t.Context(), memoryErr, false); got != memoryErr || memDocker.removals != removals {
		t.Fatalf("retry ran while OSDs remain: %v", got)
	}
	if got := s.retryMemory(t.Context(), nil, true); got != nil || memDocker.removals != removals {
		t.Fatalf("retry ran for a stage that had not failed: %v", got)
	}
	volRemovals := volDocker.removals
	if got := s.retryVolume(t.Context(), volumeErr, false); got != volumeErr || volDocker.removals != volRemovals {
		t.Fatalf("volume retry ran while OSDs remain: %v", got)
	}
	if got := s.retryVolume(t.Context(), nil, true); got != nil || volDocker.removals != volRemovals {
		t.Fatalf("volume retry ran for a stage that had not failed: %v", got)
	}
	memDocker.removeErr, volDocker.removeErr = nil, nil
	if got := s.retryMemory(t.Context(), memoryErr, true); got != nil || s.memory != nil {
		t.Fatalf("memory retry = %v, memory %v", got, s.memory)
	}
	if got := s.retryVolume(t.Context(), volumeErr, true); got != nil || s.volume != nil {
		t.Fatalf("volume retry = %v, volume %v", got, s.volume)
	}
	// A forgotten record is not released again and the previous error stays.
	again := errors.New("earlier failure")
	if got := s.retryMemory(t.Context(), again, true); got != again {
		t.Fatalf("retry of a forgotten record = %v", got)
	}
	if got := s.releaseVolume(t.Context(), true); got != nil {
		t.Fatalf("release of a forgotten record = %v", got)
	}
}

func TestOSDStorageStateSeams(t *testing.T) {
	var s osdStorageState
	var gotImage string
	fake := &osdMemoryContainerFake{}
	s.runner = func(_ context.Context, image string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		gotImage = image
		return fake, nil
	}
	if ctr, err := s.runOSD(t.Context(), "osd-image"); err != nil || ctr != testcontainers.Container(fake) || gotImage != "osd-image" {
		t.Fatalf("runOSD ignored the seam: %v %q", err, gotImage)
	}
	helperImage := ""
	s.volumeRunner = func(_ context.Context, image string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		helperImage = image
		return fake, nil
	}
	if _, err := s.runVolumeHelper(t.Context(), "helper-image"); err != nil || helperImage != "helper-image" || gotImage != "osd-image" {
		t.Fatalf("runVolumeHelper used the OSD seam or none: %v %q", err, helperImage)
	}

	docker := &osdVolumeDockerFake{}
	s.volumeDocker = func(context.Context) (osdMemoryVolumeClient, error) { return docker, nil }
	v := s.beginVolume()
	if s.volumeStorage() != v || !s.hasVolume() || v.newClient == nil {
		t.Fatal("beginVolume did not publish a wired record")
	}
	if client, err := v.client(t.Context()); err != nil || client != osdMemoryVolumeClient(docker) {
		t.Fatalf("beginVolume did not wire the Docker seam: %v", err)
	}
	m := s.beginMemory(1 << 20)
	if s.memoryStorage() != m || m.size != 1<<20 {
		t.Fatal("beginMemory did not publish the record")
	}
	memory, data := s.launchVolumes()
	if memory != m.name || data != v.name || memory == "" || data == "" {
		t.Fatalf("launchVolumes = %q, %q", memory, data)
	}
	if memory, data := (&osdStorageState{}).launchVolumes(); memory != "" || data != "" {
		t.Fatalf("unpublished storage named volumes %q, %q", memory, data)
	}
}

func TestOSDStorageStateFreshnessAndDiscardCheckMemoryFirstAndStopAtTheFirstFailure(t *testing.T) {
	mem, _, memKeeper := newOSDMemoryUnitFixture()
	vol, _, volHelper := osdVolumeFixture()
	s := osdStorageState{memory: mem, volume: vol}
	if err := s.requireFresh(t.Context(), 3); err != nil || len(memKeeper.commands) == 0 || len(volHelper.commands) == 0 {
		t.Fatalf("fresh OSD refused: %v", err)
	}
	memKeeper.directories[osdMemoryKeeperPath+"/ceph-4"] = true
	volHelper.directories[osdVolumeHelperPath+"/ceph-4"] = true
	volCommands := len(volHelper.commands)
	if err := s.requireFresh(t.Context(), 4); err == nil || !strings.Contains(err.Error(), "memory") || len(volHelper.commands) != volCommands {
		t.Fatalf("memory failure did not stop the volume check: %v", err)
	}
	if err := s.discard(t.Context(), 4); err != nil || memKeeper.directories[osdMemoryKeeperPath+"/ceph-4"] || volHelper.directories[osdVolumeHelperPath+"/ceph-4"] {
		t.Fatalf("discard left a directory: %v", err)
	}
	fail := errors.New("memory directory removal failed")
	memKeeper.execErr = fail
	volHelper.directories[osdVolumeHelperPath+"/ceph-5"] = true
	if err := s.discard(t.Context(), 5); !errors.Is(err, fail) || !volHelper.directories[osdVolumeHelperPath+"/ceph-5"] {
		t.Fatalf("discard continued after the memory failure: %v", err)
	}
	var empty osdStorageState
	if err := empty.requireFresh(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if err := empty.discard(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
}

// storageRelease adapts the memory and volume fixtures to one scenario table.
type storageRelease struct {
	release        func(context.Context) error
	docker         *osdMemoryDockerFake
	helper         *osdMemoryContainerFake
	helperForgot   func() bool
	removed        func() bool
	foreignOwner   func()
	failClient     func(error)
	markRemoved    func()
	dropHelper     func()
	helperNameText string
	volumeNameText string
}

func memoryRelease() storageRelease {
	s, d, k := newOSDMemoryUnitFixture()
	return storageRelease{
		release: s.release, docker: d, helper: k,
		helperForgot: func() bool { return s.keeper == nil }, removed: func() bool { return s.volumeRemoved },
		foreignOwner: func() { d.stored.Labels[osdMemoryOwnerLabel] = "foreign" },
		failClient: func(err error) {
			s.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return nil, err }
		},
		markRemoved:    func() { s.volumeRemoved = true },
		dropHelper:     func() { s.keeper = nil },
		helperNameText: "OSD memory keeper", volumeNameText: "OSD memory volume",
	}
}

func volumeRelease() storageRelease {
	s, d, h := osdVolumeFixture()
	return storageRelease{
		release: s.release, docker: &d.osdMemoryDockerFake, helper: &h.osdMemoryContainerFake,
		helperForgot: func() bool { return s.helper == nil }, removed: func() bool { return s.removed },
		foreignOwner: func() { d.stored.Labels[osdVolumeOwnerLabel] = "foreign" },
		failClient: func(err error) {
			s.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return nil, err }
		},
		markRemoved:    func() { s.removed = true },
		dropHelper:     func() { s.helper = nil },
		helperNameText: "OSD volume helper", volumeNameText: "OSD volume",
	}
}

// TestOSDStorageReleaseSemanticsAreIdenticalForMemoryAndVolume is the proof
// that justifies the shared ownedStorageRelease: both storages run the same
// scenarios, a failing first attempt and then a clean retry, and must leave the
// same observable state, with the same error attribution and the same Docker
// calls.
func TestOSDStorageReleaseSemanticsAreIdenticalForMemoryAndVolume(t *testing.T) {
	lost := errors.New("synthetic native failure")
	scenarios := []struct {
		name  string
		apply func(*storageRelease)
		// clear removes the injected fault before the retry.
		clear func(*storageRelease)
	}{
		{name: "clean"},
		{name: "helper terminate fails", apply: func(r *storageRelease) { r.helper.terminateErr = lost }, clear: func(r *storageRelease) { r.helper.terminateErr = nil }},
		{name: "helper already gone", apply: func(r *storageRelease) { r.helper.terminateErr = errdefs.ErrNotFound }},
		{name: "no helper", apply: func(r *storageRelease) { r.dropHelper() }},
		{name: "volume already removed", apply: func(r *storageRelease) { r.markRemoved() }},
		{name: "volume missing", apply: func(r *storageRelease) { r.docker.inspectErr = errdefs.ErrNotFound }},
		{name: "inspect fails", apply: func(r *storageRelease) { r.docker.inspectErr = lost }, clear: func(r *storageRelease) { r.docker.inspectErr = nil }},
		{name: "foreign volume", apply: func(r *storageRelease) { r.foreignOwner() }},
		{name: "remove fails", apply: func(r *storageRelease) { r.docker.removeErr = lost }, clear: func(r *storageRelease) { r.docker.removeErr = nil }},
		{name: "remove not found", apply: func(r *storageRelease) { r.docker.removeErr = errdefs.ErrNotFound }},
		{name: "close fails", apply: func(r *storageRelease) { r.docker.closeErr = lost }, clear: func(r *storageRelease) { r.docker.closeErr = nil }},
		{name: "client fails", apply: func(r *storageRelease) { r.failClient(lost) }},
	}
	type outcome struct {
		first, retry                  string
		firstIsLost, retryIsLost      bool
		helperForgot, removed         bool
		inspections, removals, closes int
		terminations                  int
		forced                        bool
	}
	run := func(r storageRelease, scenario int) outcome {
		sc := scenarios[scenario]
		if sc.apply != nil {
			sc.apply(&r)
		}
		normalize := func(err error) string {
			if err == nil {
				return ""
			}
			text := strings.NewReplacer(r.helperNameText, "HELPER", r.volumeNameText, "VOLUME").Replace(err.Error())
			if strings.Contains(text, "differs") || strings.Contains(text, "ownership") {
				return "validation refused"
			}
			return text
		}
		var o outcome
		first := r.release(t.Context())
		o.first, o.firstIsLost = normalize(first), errors.Is(first, lost)
		o.helperForgot, o.removed = r.helperForgot(), r.removed()
		if sc.clear != nil {
			sc.clear(&r)
		}
		retry := r.release(t.Context())
		o.retry, o.retryIsLost = normalize(retry), errors.Is(retry, lost)
		o.helperForgot, o.removed = r.helperForgot(), r.removed()
		o.inspections, o.removals, o.closes = r.docker.inspections, r.docker.removals, r.docker.closes
		o.terminations, o.forced = r.helper.terminations, r.docker.removeOptions.Force
		return o
	}
	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			memory, volume := run(memoryRelease(), i), run(volumeRelease(), i)
			if memory != volume {
				t.Fatalf("release diverged\nmemory: %+v\nvolume: %+v", memory, volume)
			}
			if memory.forced {
				t.Fatal("a volume was force-removed")
			}
		})
	}
}

// osdStorageMethods pins the component: the calls each method makes, receiver
// included, and whether Container.mu must be held by the caller. No method
// takes a lock or calls into Container, so the component stays below mu.
var osdStorageMethods = map[string][]string{
	"memoryStorage": nil,
	"volumeStorage": nil,
	"hasVolume":     nil,
	"beginMemory":   {"newOSDMemoryStorage(size)"},
	"beginVolume":   {"newOSDVolumeStorage()"},
	"runOSD":        {"run(ctx, image, opts...)"},
	"runVolumeHelper": {
		"run(ctx, image, opts...)",
	},
	"launchVolumes": nil,
	"requireFresh":  {"s.memory.requireFreshOSD(ctx, id)", "s.volume.requireFreshOSD(ctx, id)"},
	"discard":       {"s.memory.discardOSD(ctx, id)", "s.volume.discardOSD(ctx, id)"},
	"retainedStore": {"s.volume.retainedStore(ctx, id)"},
	"releaseMemory": {"s.memory.release(ctx)"},
	"releaseVolume": {"s.volume.release(ctx)"},
	"retryMemory":   {"s.releaseMemory(ctx, osdsGone)"},
	"retryVolume":   {"s.releaseVolume(ctx, osdsGone)"},
}

// osdStorageCallers lists every function outside the component that calls one
// of its methods, and how it holds Container.mu: "takes" means it calls
// lockTopology itself before its first use; "holds" means its doc comment
// requires the caller to hold it (the callers of those take it, see
// osdStorageHoldersTake).
var osdStorageCallers = map[string]string{
	"registerOSD":        "holds",
	"ensureOSDMemory":    "holds",
	"ensureOSDVolume":    "holds",
	"newOSDLaunchRecipe": "holds",
	"removeOSD":          "takes",
	"detachOSD":          "takes",
	"reopenOSD":          "takes",
	"reopenPreflight":    "holds",
	"terminate":          "takes",
	"startOSD":           "seam",
}

// osdStorageHoldersTake names the functions that take Container.mu around the
// "holds" callers above.
var osdStorageHoldersTake = map[string][]string{
	"registerOSD":        {"startInitialOSDs", "AddOSDWithConfig"},
	"reopenPreflight":    {"reopenOSD"},
	"newOSDLaunchRecipe": {"registerOSD"},
}

func osdStorageUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "stores" }

// TestOSDStorageStateFieldsAreOnlyTouchedByItsMethods guards the ownership
// written on osdStorageState: outside this type, Container.stores is used only
// as the receiver of one of its methods, no function outside the type assigns
// one of its fields, and the lock-free FactsOf source does not mention it.
func TestOSDStorageStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "osdStorageState" {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := osdStorageMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && osdStorageUse(recv) {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && osdStorageUse(sel) {
					if allowed[sel] {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.stores outside an osdStorageState method call")
					}
				}
				return true
			})
			if file == "cluster_facts.go" && strings.Contains(fn.Name.Name, "Facts") {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && osdStorageUse(sel) {
						problems = append(problems, "FactsOf source reads Container.stores")
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.stores found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("OSD storage state escapes its methods: %v", problems)
	}
}

// TestOSDStorageStateMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned and each method makes exactly the
// calls listed. None takes a lock or calls into Container, and the fields are
// assigned only by the methods that publish or release a record.
func TestOSDStorageStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	writers := map[string]bool{"beginMemory": true, "beginVolume": true, "releaseMemory": true, "releaseVolume": true}
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "osdStorageState" {
				continue
			}
			name := fn.Name.Name
			want, known := osdStorageMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update osdStorageMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x))
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && (sel.Sel.Name == "memory" || sel.Sel.Name == "volume") && !writers[name] {
							problems = append(problems, name+" assigns "+sel.Sel.Name)
						}
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range osdStorageMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		t.Fatalf("osdStorageState breaks its lock contract: %v", problems)
	}
}

// TestOSDStorageStateSeamsHaveNoProductionWriter guards the claim that startOSD
// may read runner without Container.mu: nothing outside tests assigns a seam.
func TestOSDStorageStateSeamsHaveNoProductionWriter(t *testing.T) {
	seams := map[string]bool{"runner": true, "volumeDocker": true, "volumeRunner": true}
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range x.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && seams[sel.Sel.Name] {
						problems = append(problems, file+": assigns seam "+sel.Sel.Name)
					}
				}
			case *ast.KeyValueExpr:
				if id, ok := x.Key.(*ast.Ident); ok && seams[id.Name] {
					problems = append(problems, file+": sets seam "+id.Name)
				}
			}
			return true
		})
	}
	if len(problems) != 0 {
		t.Fatalf("a production path writes a test seam: %v", problems)
	}
}

// TestOSDStorageStateCallersHoldTheTopologyLock guards the callers: the set of
// functions that use the component is pinned, those marked "takes" call
// lockTopology before their first use, and the "holds" functions are reached
// only from functions that take it.
func TestOSDStorageStateCallersHoldTheTopologyLock(t *testing.T) {
	found := map[string]bool{}
	taking := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "osdStorageState" {
				continue
			}
			var lockAt, firstUse token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if isTopologyAcquire(call) && lockAt == token.NoPos {
					lockAt = call.Pos()
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && osdStorageUse(recv) {
						if firstUse == token.NoPos {
							firstUse = call.Pos()
						}
					}
				}
				return true
			})
			if lockAt != token.NoPos {
				taking[fn.Name.Name] = true
			}
			if firstUse == token.NoPos {
				continue
			}
			name := fn.Name.Name
			found[name] = true
			mode, known := osdStorageCallers[name]
			switch {
			case !known:
				problems = append(problems, name+" uses the OSD storage state but is not in osdStorageCallers")
			case mode == "takes" && (lockAt == token.NoPos || lockAt > firstUse):
				problems = append(problems, name+" must take lockTopology before using the OSD storage state")
			}
		}
	}
	for name := range osdStorageCallers {
		if !found[name] {
			problems = append(problems, name+" is listed but no longer uses the OSD storage state")
		}
	}
	for holder, takers := range osdStorageHoldersTake {
		ok := false
		for _, taker := range takers {
			ok = ok || taking[taker] || osdStorageCallers[taker] == "holds"
		}
		if !ok {
			problems = append(problems, holder+" is not reached from a function that takes lockTopology")
		}
	}
	if len(problems) != 0 {
		t.Fatalf("OSD storage callers break the lock contract: %v", problems)
	}
}

// TestTerminateReleasesOSDStorageInItsTwoStagesInOrder guards the Terminate
// order of the component: the topology lock first, the memory and volume
// release after the OSD containers and before the manager, monitors and
// control container, the labeled-container removal next, and only then the
// retry of a failed release, memory before volume in both stages.
func TestTerminateReleasesOSDStorageInItsTwoStagesInOrder(t *testing.T) {
	order := []string{"lockTopology", "releaseMemory", "releaseVolume", "removeLabeledContainers", "retryMemory", "retryVolume"}
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			var firstManager, controlLock token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						at[sel.Sel.Name] = append(at[sel.Sel.Name], call.Pos())
						if firstManager == token.NoPos && types.ExprString(call) == "first.Terminate(ctx, opts...)" {
							firstManager = call.Pos()
						}
						if controlLock == token.NoPos && types.ExprString(call) == "c.Container.Terminate(ctx, opts...)" {
							controlLock = call.Pos()
						}
					}
				}
				return true
			})
			var problems []string
			for _, name := range order {
				if len(at[name]) != 1 {
					problems = append(problems, name+" must be called exactly once in terminate")
				}
			}
			if len(problems) == 0 {
				for i := 1; i < len(order); i++ {
					if at[order[i-1]][0] >= at[order[i]][0] {
						problems = append(problems, order[i-1]+" must come before "+order[i])
					}
				}
				if firstManager == token.NoPos || at["releaseVolume"][0] >= firstManager || firstManager >= at["removeLabeledContainers"][0] {
					problems = append(problems, "the first release stage must precede the manager termination, which precedes the labeled-container removal")
				}
				if controlLock == token.NoPos || at["releaseVolume"][0] >= controlLock || controlLock >= at["removeLabeledContainers"][0] {
					problems = append(problems, "the first release stage must precede the monitor termination, which precedes the labeled-container removal")
				}
			}
			if len(problems) != 0 {
				t.Fatalf("terminate changed the OSD storage release order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
