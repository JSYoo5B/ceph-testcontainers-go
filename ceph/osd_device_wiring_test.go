package ceph

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// wiringCluster is a Docker-free cluster whose container runner records every
// creation request and answers with the given outcome.
type wiringCluster struct {
	*Container
	mon       *initialOSDMon
	creations []testcontainers.GenericContainerRequest
}

func newWiringCluster(t *testing.T, settings options, outcome func(id int) (testcontainers.Container, error)) *wiringCluster {
	t.Helper()
	settings.essentialImage = "synthetic-osd-image"
	c, mon, _ := newInitialOSDCluster(settings)
	c.bootstrap.config, c.bootstrap.keyring = []byte("[global]\nfsid = synthetic-cluster\n"), []byte("synthetic-admin-keyring")
	w := &wiringCluster{Container: c, mon: mon}
	c.fleet.stores.runner = func(_ context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		var req testcontainers.GenericContainerRequest
		for _, opt := range opts {
			if err := opt.Customize(&req); err != nil {
				t.Fatal(err)
			}
		}
		w.creations = append(w.creations, req)
		return outcome(len(w.creations) - 1)
	}
	return w
}

// wire runs the wiring step the way its one caller must: with the topology
// lock held.
func (w *wiringCluster) wire(t *testing.T, device osdBlockDevice, lease string) (osdLaunch, error) {
	t.Helper()
	if err := w.lockTopology(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer w.mu.Unlock()
	return w.osdFleet().registerBlockDeviceOSD(t.Context(), OSDConfig{Root: "default", Host: "synthetic-host"}, w.settings.essentialImage, device, lease)
}

func nopOutcome(int) (testcontainers.Container, error) { return &osdMemoryContainerFake{}, nil }

func TestRegisterBlockDeviceOSDHandsBackAMappedUnstartedLaunchWithItsClaim(t *testing.T) {
	w := newWiringCluster(t, options{}, nopOutcome)
	launch, err := w.wire(t, claimDevice("a", 1), "lease-a")
	if err != nil {
		t.Fatal(err)
	}
	info, held := w.fleet.deviceClaims.claimOf(launch.osd.ID)
	if !held || info.lease != "lease-a" || info.device != claimDevice("a", 1) || info.attempted || w.fleet.deviceClaims.count() != 1 {
		t.Fatalf("claim %+v held %v, count %d", info, held, w.fleet.deviceClaims.count())
	}
	if launch.recipe.started.Load() || len(w.creations) != 0 || w.fleet.osdHandles.get(launch.osd.ID) != launch.osd {
		t.Fatal("the wiring step started or lost the launch")
	}
	if err := w.osdFleet().startOSD(t.Context(), launch); err != nil {
		t.Fatal(err)
	}
	req := w.creations[0]
	host := &container.HostConfig{}
	req.HostConfigModifier(host)
	if len(w.creations) != 1 || len(host.Devices) != 1 || host.Devices[0].PathOnHost != "/dev/mapper/a" ||
		req.Env["CEPH_OSD_BLOCK_DEVICE_ID"] != "253:1" {
		t.Fatalf("creations %d, devices %v, env %v", len(w.creations), host.Devices, req.Env)
	}
	if info, _ := w.fleet.deviceClaims.claimOf(launch.osd.ID); !info.attempted || info.lease != "lease-a" {
		t.Fatalf("claim after start %+v", info)
	}
}

// A refusal before registration publishes nothing at all: no OSD in the MON, no
// handle, no claim.
func TestRegisterBlockDeviceOSDRefusesBadInputsBeforeRegistering(t *testing.T) {
	for name, tc := range map[string]struct {
		device osdBlockDevice
		lease  string
	}{
		"raw device":      {osdBlockDevice{hostPath: "/dev/sda", major: 8, sizeBytes: 1 << 30}, "lease-a"},
		"empty lease":     {claimDevice("a", 1), ""},
		"spaced lease":    {claimDevice("a", 1), "lease a"},
		"overlong lease":  {claimDevice("a", 1), strings.Repeat("x", 129)},
		"newline lease":   {claimDevice("a", 1), "lease-a\n"},
		"zero major":      {osdBlockDevice{hostPath: "/dev/mapper/a", sizeBytes: 128 << 20}, "lease-a"},
		"unaligned bytes": {osdBlockDevice{hostPath: "/dev/mapper/a", major: 253, sizeBytes: 128<<20 + 1}, "lease-a"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWiringCluster(t, options{}, nopOutcome)
			launch, err := w.wire(t, tc.device, tc.lease)
			if err == nil || launch.osd != nil || launch.recipe != nil {
				t.Fatalf("launch %+v, err %v", launch, err)
			}
			if w.fleet.osdHandles.count() != 0 || len(w.mon.uuids) != 0 || w.fleet.deviceClaims.count() != 0 {
				t.Fatalf("handles %d, MON registrations %d, claims %d", w.fleet.osdHandles.count(), len(w.mon.uuids), w.fleet.deviceClaims.count())
			}
		})
	}
	t.Run("cancelled context", func(t *testing.T) {
		w := newWiringCluster(t, options{}, nopOutcome)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := w.osdFleet().registerBlockDeviceOSD(ctx, OSDConfig{Root: "default"}, "img", claimDevice("a", 1), "lease-a")
		if !errors.Is(err, context.Canceled) || w.fleet.osdHandles.count() != 0 || w.fleet.deviceClaims.count() != 0 {
			t.Fatalf("err %v, handles %d, claims %d", err, w.fleet.osdHandles.count(), w.fleet.deviceClaims.count())
		}
	})
}

// requireSealed checks that the returned launch cannot start: no container is
// created and the ordinary "do not replay" error is reported.
func requireSealed(t *testing.T, w *wiringCluster, launch osdLaunch) {
	t.Helper()
	if launch.osd == nil || launch.recipe == nil || !launch.recipe.started.Load() {
		t.Fatalf("launch %+v is not a registered, sealed launch", launch)
	}
	before := len(w.creations)
	err := w.osdFleet().startOSD(t.Context(), launch)
	if err == nil || !strings.Contains(err.Error(), "do not replay") || len(w.creations) != before || launch.osd.Container != nil {
		t.Fatalf("sealed launch started: %v, creations %d", err, len(w.creations)-before)
	}
}

func TestRegisterBlockDeviceOSDSealsTheRecipeWhenTheAttachIsRefused(t *testing.T) {
	w := newWiringCluster(t, options{osdObjectStore: OSDMemStore}, nopOutcome)
	launch, err := w.wire(t, claimDevice("a", 1), "lease-a")
	if err == nil {
		t.Fatal("attach admitted for a MemStore OSD")
	}
	requireSealed(t, w, launch)
	if w.fleet.deviceClaims.count() != 0 || launch.recipe.blockDevice.Load() != nil {
		t.Fatalf("claims %d, device %v", w.fleet.deviceClaims.count(), launch.recipe.blockDevice.Load())
	}
	if w.fleet.osdHandles.get(launch.osd.ID) != launch.osd {
		t.Fatal("the registered OSD is not owned for Terminate")
	}
}

func TestRegisterBlockDeviceOSDRefusedClaimChangesNothingAndSealsTheRecipe(t *testing.T) {
	for name, second := range map[string]struct {
		device osdBlockDevice
		lease  string
	}{
		"same lease token":   {claimDevice("b", 2), "lease-a"},
		"same node":          {claimDevice("a", 3), "lease-b"},
		"same major:minor":   {claimDevice("b", 1), "lease-b"},
		"fully repeated ask": {claimDevice("a", 1), "lease-a"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWiringCluster(t, options{}, nopOutcome)
			first, err := w.wire(t, claimDevice("a", 1), "lease-a")
			if err != nil {
				t.Fatal(err)
			}
			held, _ := w.fleet.deviceClaims.claimOf(first.osd.ID)
			launch, err := w.wire(t, second.device, second.lease)
			if err == nil {
				t.Fatal("conflicting claim admitted")
			}
			requireSealed(t, w, launch)
			if again, _ := w.fleet.deviceClaims.claimOf(first.osd.ID); again != held || w.fleet.deviceClaims.count() != 1 {
				t.Fatalf("the first claim changed: %+v -> %+v, count %d", held, again, w.fleet.deviceClaims.count())
			}
			if _, claimed := w.fleet.deviceClaims.claimOf(launch.osd.ID); claimed {
				t.Fatal("a refused claim was published")
			}
			// The first launch is unaffected.
			if err := w.osdFleet().startOSD(t.Context(), first); err != nil {
				t.Fatalf("first launch: %v", err)
			}
		})
	}
}

// After a successful wiring the claim obeys the table's own rules and nothing
// else drops it, whatever the lease owner does.
func TestRegisterBlockDeviceOSDClaimFollowsTheTablesRulesThroughUncertainStartup(t *testing.T) {
	t.Run("unattempted launch is released by sealing", func(t *testing.T) {
		w := newWiringCluster(t, options{}, nopOutcome)
		launch, err := w.wire(t, claimDevice("a", 1), "lease-a")
		if err != nil {
			t.Fatal(err)
		}
		if !w.fleet.deviceClaims.release(launch.osd.ID, false) || w.fleet.deviceClaims.count() != 0 {
			t.Fatal("never-attempted claim kept")
		}
		requireSealed(t, w, launch)
	})
	for name, outcome := range map[string]func(int) (testcontainers.Container, error){
		"lost create reply": func(int) (testcontainers.Container, error) { return nil, errors.New("create reply lost") },
		"readiness failure": func(int) (testcontainers.Container, error) { return &osdMemoryContainerFake{}, errors.New("not ready") },
		"mkfs failure after create": func(int) (testcontainers.Container, error) {
			return &osdMemoryContainerFake{}, errors.New("mkfs exited 1")
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWiringCluster(t, options{}, outcome)
			launch, err := w.wire(t, claimDevice("a", 1), "lease-a")
			if err != nil {
				t.Fatal(err)
			}
			if err := w.osdFleet().startOSD(t.Context(), launch); err == nil {
				t.Fatal("start failure hidden")
			}
			// The caller's lease token is still on the claim, and neither a
			// release without container evidence nor a failed scan drops it.
			if w.fleet.deviceClaims.release(launch.osd.ID, false) || w.fleet.deviceClaims.count() != 1 {
				t.Fatal("an attempted launch lost its claim without a confirmed container removal")
			}
			err = w.fleet.deviceClaims.settle(false)
			if err == nil || !strings.Contains(err.Error(), "lease lease-a") || !strings.Contains(err.Error(), "keep the lease chain") {
				t.Fatalf("settle without a scan = %v", err)
			}
			if info, _ := w.fleet.deviceClaims.claimOf(launch.osd.ID); !info.attempted || info.lease != "lease-a" {
				t.Fatalf("claim %+v", info)
			}
			// The same launch can never be replayed to retry the device.
			if err := w.osdFleet().startOSD(t.Context(), launch); err == nil || !strings.Contains(err.Error(), "do not replay") || len(w.creations) != 1 {
				t.Fatalf("replay: %v, creations %d", err, len(w.creations))
			}
			// Only the table's confirmation drops it.
			if err := w.fleet.deviceClaims.settle(true); err != nil || w.fleet.deviceClaims.count() != 0 {
				t.Fatalf("settle after the labeled removal = %v, claims %d", err, w.fleet.deviceClaims.count())
			}
		})
	}
}

func TestRegisterBlockDeviceOSDTwoOSDsNeverShareADevice(t *testing.T) {
	w := newWiringCluster(t, options{}, nopOutcome)
	a, err := w.wire(t, claimDevice("a", 1), "lease-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.wire(t, claimDevice("b", 2), "lease-b")
	if err != nil {
		t.Fatal(err)
	}
	if a.osd.ID == b.osd.ID || w.fleet.deviceClaims.count() != 2 {
		t.Fatalf("osd.%d and osd.%d, claims %d", a.osd.ID, b.osd.ID, w.fleet.deviceClaims.count())
	}
}

// --- AST guards -------------------------------------------------------------

// osdDeviceWiringCallers is the caller table of registerBlockDeviceOSD. The
// single permitted production caller is addBlockDeviceOSD, which holds
// lockTopology through registerBlockDeviceOSD, startOSD and the up/in wait, as
// AddOSD does. The step has no option, no default path and no exported entry,
// so every other launch of a cluster keeps its default behavior. Tests take
// lockTopology around it themselves (wiringCluster.wire). Another caller must
// be added here with the lock it holds, and fails the guard until it is.
var osdDeviceWiringCallers = map[string]bool{"addBlockDeviceOSD": true}

// osdDeviceWiringSequence is the pinned order of the calls of the step.
var osdDeviceWiringSequence = []string{
	"device.validate",
	"osdDeviceLease.MatchString",
	"f.registerOSD",
	"launch.recipe.attachBlockDevice",
	"launch.recipe.seal",
	"f.deviceClaims.claim",
	"launch.recipe.seal",
}

// osdDeviceWiringProblems checks the production sources against the contract of
// the wiring step: the single caller table, the ownership of the two seams, the
// call order, the seal on each post-registration failure, the opaque lease and
// the one-shot argument, and no lock, goroutine or mutex.
func osdDeviceWiringProblems(files map[string]*ast.File) []string {
	var problems []string
	steps := 0
	for file, parsed := range files {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name != "registerBlockDeviceOSD" {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "registerBlockDeviceOSD" && !osdDeviceWiringCallers[fn.Name.Name] {
						problems = append(problems, file+": "+fn.Name.Name+" uses registerBlockDeviceOSD but is not in osdDeviceWiringCallers")
					}
					return true
				})
				continue
			}
			steps++
			problems = append(problems, osdDeviceWiringStepProblems(fn)...)
		}
		if file == "osd_device_wiring.go" {
			for _, imp := range parsed.Imports {
				if imp.Path.Value != `"context"` && imp.Path.Value != `"errors"` {
					problems = append(problems, "osd_device_wiring.go imports "+imp.Path.Value+": the step reaches no lock, Docker or device package")
				}
			}
			for _, decl := range parsed.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Name.IsExported() {
						problems = append(problems, "osd_device_wiring.go declares exported "+d.Name.Name)
					}
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						if ts, ok := spec.(*ast.TypeSpec); ok {
							problems = append(problems, "osd_device_wiring.go declares type "+ts.Name.Name)
						}
					}
				}
			}
		}
	}
	if steps != 1 {
		problems = append(problems, fmt.Sprintf("registerBlockDeviceOSD declared %d times, want once", steps))
	}
	return problems
}

func osdDeviceWiringStepProblems(fn *ast.FuncDecl) []string {
	var problems []string
	if fn.Recv == nil || receiverTypeName(fn) != "osdFleet" || fn.Name.IsExported() {
		problems = append(problems, "registerBlockDeviceOSD must be an unexported osdFleet method")
	}
	var got []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			problems = append(problems, "registerBlockDeviceOSD starts a goroutine")
		case *ast.DeferStmt:
			problems = append(problems, "registerBlockDeviceOSD defers: it holds no resource of its own")
		case *ast.SelectorExpr:
			switch x.Sel.Name {
			case "Lock", "Unlock", "TryLock", "RLock", "RUnlock", "lockTopology", "mu":
				problems = append(problems, "registerBlockDeviceOSD touches "+x.Sel.Name+": the caller holds Container.mu")
			}
		case *ast.CallExpr:
			text := types.ExprString(x.Fun)
			if strings.HasPrefix(text, "errors.") {
				return true
			}
			got = append(got, text)
			switch text {
			case "launch.recipe.attachBlockDevice":
				if len(x.Args) != 1 || types.ExprString(x.Args[0]) != "device" {
					problems = append(problems, "attachBlockDevice must receive the caller's device unchanged")
				}
			case "osdDeviceLease.MatchString":
				if len(x.Args) != 1 || types.ExprString(x.Args[0]) != "lease" {
					problems = append(problems, "the lease token must be checked as given")
				}
			case "f.deviceClaims.claim":
				want := []string{"launch.osd.ID", "lease", "device", "&launch.recipe.started"}
				var args []string
				for _, a := range x.Args {
					args = append(args, types.ExprString(a))
				}
				if !slices.Equal(args, want) {
					problems = append(problems, "claim receives "+strings.Join(args, ", ")+", want the opaque lease, the same device and the recipe's own one-shot: "+strings.Join(want, ", "))
				}
			}
		}
		return true
	})
	if !slices.Equal(got, osdDeviceWiringSequence) {
		problems = append(problems, "registerBlockDeviceOSD calls "+strings.Join(got, ", ")+", pinned "+strings.Join(osdDeviceWiringSequence, ", "))
	}
	// Each post-registration failure seals before it returns.
	sealed := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		init, ok := ifs.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			return true
		}
		rhs := types.ExprString(init.Rhs[0])
		if !strings.Contains(rhs, "deviceClaims.claim") && !strings.Contains(rhs, "attachBlockDevice") {
			return true
		}
		if len(ifs.Body.List) == 2 {
			if call, ok := ifs.Body.List[0].(*ast.ExprStmt); ok && types.ExprString(call.X) == "launch.recipe.seal()" {
				sealed++
			}
		}
		return true
	})
	if sealed != 2 {
		problems = append(problems, "registerBlockDeviceOSD must seal the recipe first in both the attach and the claim failure branch")
	}
	return problems
}

func TestRegisterBlockDeviceOSDHasOneProductionCallerAndKeepsItsContract(t *testing.T) {
	files := parseProductionFiles(t)
	if problems := osdDeviceWiringProblems(files); len(problems) != 0 {
		t.Fatalf("the OSD device wiring step breaks its contract: %v", problems)
	}
	if len(osdDeviceWiringCallers) != 1 || !osdDeviceWiringCallers["addBlockDeviceOSD"] {
		t.Fatalf("osdDeviceWiringCallers = %v, want only addBlockDeviceOSD", osdDeviceWiringCallers)
	}
}

// TestOSDDeviceWiringGuardRejectsBrokenVariants runs the guard on mutated copies
// of the production source: each mutant must be reported, so a weakened guard
// or a broken step cannot pass unnoticed.
func TestOSDDeviceWiringGuardRejectsBrokenVariants(t *testing.T) {
	source, err := os.ReadFile("osd_device_wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	original := string(source)
	mutants := map[string][2]string{
		"attach failure does not seal":  {"if err := launch.recipe.attachBlockDevice(device); err != nil {\n\t\tlaunch.recipe.seal()\n", "if err := launch.recipe.attachBlockDevice(device); err != nil {\n"},
		"claim failure does not seal":   {"&launch.recipe.started); err != nil {\n\t\tlaunch.recipe.seal()\n", "&launch.recipe.started); err != nil {\n"},
		"lease token is trimmed":        {"launch.osd.ID, lease, device", "launch.osd.ID, strings.TrimSpace(lease), device"},
		"claim uses a private one-shot": {"&launch.recipe.started); err", "new(atomic.Bool)); err"},
		"claim before attach":           {"if err := launch.recipe.attachBlockDevice(device); err != nil {\n\t\tlaunch.recipe.seal()\n\t\treturn launch, err\n\t}\n\tif err := f.deviceClaims.claim(launch.osd.ID, lease, device, &launch.recipe.started); err != nil {\n\t\tlaunch.recipe.seal()\n\t\treturn launch, err\n\t}\n", "if err := f.deviceClaims.claim(launch.osd.ID, lease, device, &launch.recipe.started); err != nil {\n\t\tlaunch.recipe.seal()\n\t\treturn launch, err\n\t}\n\tif err := launch.recipe.attachBlockDevice(device); err != nil {\n\t\tlaunch.recipe.seal()\n\t\treturn launch, err\n\t}\n"},
		"claim before registration":     {"launch, err := f.registerOSD(ctx, config, image, nil)", "_ = f.deviceClaims.claim(0, lease, device, nil)\n\tlaunch, err := f.registerOSD(ctx, config, image, nil)"},
		"token check dropped":           {"if !osdDeviceLease.MatchString(lease) {", "if false {"},
		"takes the topology lock":       {"launch, err := f.registerOSD(", "_ = f.owner.lockTopology(ctx)\n\tlaunch, err := f.registerOSD("},
		"takes the mutex":               {"launch, err := f.registerOSD(", "f.owner.mu.Lock()\n\tlaunch, err := f.registerOSD("},
		"starts a goroutine":            {"launch, err := f.registerOSD(", "go func() {}()\n\tlaunch, err := f.registerOSD("},
		"starts the launch itself":      {"\treturn launch, nil\n}", "\treturn launch, f.startOSD(ctx, launch)\n}"},
		"imports a device package":      {"import (\n\t\"context\"", "import (\n\t\"os/exec\"\n\t\"context\""},
	}
	if problems := osdDeviceWiringProblems(map[string]*ast.File{"osd_device_wiring.go": parseSourceString(t, original)}); len(problems) != 0 {
		t.Fatalf("control: the production source is rejected: %v", problems)
	}
	for name, m := range mutants {
		t.Run(name, func(t *testing.T) {
			if strings.Count(original, m[0]) != 1 {
				t.Fatalf("mutation anchor occurs %d times; update the mutant", strings.Count(original, m[0]))
			}
			mutated := strings.Replace(original, m[0], m[1], 1)
			parsed, err := parser.ParseFile(token.NewFileSet(), "osd_device_wiring.go", mutated, 0)
			if err != nil {
				t.Fatal(err)
			}
			if problems := osdDeviceWiringProblems(map[string]*ast.File{"osd_device_wiring.go": parsed}); len(problems) == 0 {
				t.Fatal("the guard accepted a broken step")
			}
		})
	}
	t.Run("production caller", func(t *testing.T) {
		caller := parseSourceString(t, "package ceph\nfunc (c *Container) launchDeviceOSD() { c.osdFleet().registerBlockDeviceOSD(nil, OSDConfig{}, \"\", osdBlockDevice{}, \"x\") }\n")
		if problems := osdDeviceWiringProblems(map[string]*ast.File{"x.go": caller}); len(problems) == 0 {
			t.Fatal("a production caller was accepted")
		}
	})
}

func parseSourceString(t *testing.T, source string) *ast.File {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
