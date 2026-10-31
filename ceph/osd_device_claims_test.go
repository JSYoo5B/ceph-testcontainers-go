package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func claimDevice(name string, minor uint32) osdBlockDevice {
	return osdBlockDevice{hostPath: "/dev/mapper/" + name, major: 253, minor: minor, sizeBytes: 128 << 20}
}

func mustClaim(t *testing.T, s *osdDeviceClaims, osd int, lease string, d osdBlockDevice) *atomic.Bool {
	t.Helper()
	creation := new(atomic.Bool)
	if err := s.claim(osd, lease, d, creation); err != nil {
		t.Fatalf("claim of osd.%d refused: %v", osd, err)
	}
	return creation
}

func TestOSDDeviceClaimAdmissionRefusesWithoutPublishing(t *testing.T) {
	spent := new(atomic.Bool)
	spent.Store(true)
	fresh := new(atomic.Bool)
	bad := claimDevice("x", 1)
	bad.hostPath = "/dev/sda"
	cases := map[string]struct {
		osd      int
		lease    string
		device   osdBlockDevice
		creation *atomic.Bool
	}{
		"negative osd":        {-1, "lease-a", claimDevice("a", 1), fresh},
		"empty lease":         {0, "", claimDevice("a", 1), fresh},
		"lease with space":    {0, "lease a", claimDevice("a", 1), fresh},
		"lease with slash":    {0, "dev/a", claimDevice("a", 1), fresh},
		"lease leading dot":   {0, ".a", claimDevice("a", 1), fresh},
		"overlong lease":      {0, strings.Repeat("a", 129), claimDevice("a", 1), fresh},
		"nil creation":        {0, "lease-a", claimDevice("a", 1), nil},
		"creation attempted":  {0, "lease-a", claimDevice("a", 1), spent},
		"invalid device path": {0, "lease-a", bad, fresh},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var s osdDeviceClaims
			if err := s.claim(c.osd, c.lease, c.device, c.creation); err == nil {
				t.Fatal("claim admitted")
			}
			if s.count() != 0 || s.records != nil {
				t.Fatalf("a refused claim published %d records", s.count())
			}
		})
	}
}

func TestOSDDeviceClaimUniquenessAcrossOSDs(t *testing.T) {
	var s osdDeviceClaims
	mustClaim(t, &s, 1, "lease-a", claimDevice("a", 1))
	before, _ := s.claimOf(1)
	for name, c := range map[string]struct {
		osd    int
		lease  string
		device osdBlockDevice
		want   string
	}{
		"same osd, other device":     {1, "lease-b", claimDevice("b", 2), "osd.1 already holds"},
		"identical request again":    {1, "lease-a", claimDevice("a", 1), "osd.1 already holds"},
		"same lease":                 {2, "lease-a", claimDevice("b", 2), "lease lease-a"},
		"same node name":             {2, "lease-b", func() osdBlockDevice { d := claimDevice("a", 2); return d }(), "device /dev/mapper/a"},
		"same number, other name":    {2, "lease-b", claimDevice("b", 1), "device number 253:1"},
		"same device, other size":    {2, "lease-b", func() osdBlockDevice { d := claimDevice("a", 1); d.sizeBytes *= 2; return d }(), "device /dev/mapper/a"},
		"same name, other lease/osd": {3, "lease-c", claimDevice("a", 9), "osd.1"},
	} {
		t.Run(name, func(t *testing.T) {
			err := s.claim(c.osd, c.lease, c.device, new(atomic.Bool))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want a conflict containing %q", err, c.want)
			}
			after, ok := s.claimOf(1)
			if !ok || after != before || s.count() != 1 {
				t.Fatalf("a refused claim changed the table: %+v, %d records", after, s.count())
			}
		})
	}
	mustClaim(t, &s, 2, "lease-b", claimDevice("b", 2))
	if got := s.osds(); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("osds = %v", got)
	}
}

func TestOSDDeviceClaimInfoIsACopy(t *testing.T) {
	var s osdDeviceClaims
	creation := mustClaim(t, &s, 4, "lease-a", claimDevice("a", 1))
	info, ok := s.claimOf(4)
	if !ok || info.attempted || info.lease != "lease-a" || info.device != claimDevice("a", 1) {
		t.Fatalf("claimOf = %+v, %v", info, ok)
	}
	info.lease, info.device.hostPath = "changed", "/dev/mapper/changed"
	if again, _ := s.claimOf(4); again.lease != "lease-a" || again.device.hostPath != "/dev/mapper/a" {
		t.Fatalf("mutating the copy changed the claim: %+v", again)
	}
	creation.Store(true)
	if again, _ := s.claimOf(4); !again.attempted {
		t.Fatal("the attempt witness is not read from the one-shot")
	}
	if _, ok := s.claimOf(5); ok {
		t.Fatal("claimOf invented a claim")
	}
}

func TestOSDDeviceClaimReleaseDependsOnCreationAndConfirmation(t *testing.T) {
	for _, c := range []struct {
		name               string
		attempted, confirm bool
		released           bool
	}{
		{"never attempted, unconfirmed", false, false, true},
		{"never attempted, confirmed", false, true, true},
		{"attempted, unconfirmed (lost create reply)", true, false, false},
		{"attempted, confirmed gone", true, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var s osdDeviceClaims
			creation := mustClaim(t, &s, 7, "lease-a", claimDevice("a", 1))
			creation.Store(c.attempted)
			if got := s.release(7, c.confirm); got != c.released {
				t.Fatalf("release = %v, want %v", got, c.released)
			}
			if _, held := s.claimOf(7); held == c.released {
				t.Fatalf("claim held = %v after release = %v", held, c.released)
			}
			if c.released && !creation.Load() {
				t.Fatal("a released claim left its launch able to start")
			}
		})
	}
	var s osdDeviceClaims
	if s.release(9, true) {
		t.Fatal("release of a missing claim reported success")
	}
	// A released device can be claimed again, by a new launch only.
	creation := mustClaim(t, &s, 7, "lease-a", claimDevice("a", 1))
	s.release(7, false)
	if err := s.claim(7, "lease-a", claimDevice("a", 1), creation); err == nil {
		t.Fatal("the sealed one-shot of a released claim was admitted again")
	}
	mustClaim(t, &s, 8, "lease-a", claimDevice("a", 1))
}

// The one-shot a claim holds is the recipe's own: releasing an unstarted claim
// stops startOSD from creating a container that nobody claimed.
func TestOSDDeviceClaimReleaseSealsTheRecipeAgainstALateStart(t *testing.T) {
	var s osdDeviceClaims
	recipe := &osdLaunchRecipe{id: 3}
	if err := s.claim(3, "lease-a", claimDevice("a", 1), &recipe.started); err != nil {
		t.Fatal(err)
	}
	if !s.release(3, false) {
		t.Fatal("unstarted claim kept")
	}
	creates := 0
	cluster := &Container{}
	cluster.fleet.stores.runner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		creates++
		return &osdMemoryContainerFake{}, nil
	}
	err := cluster.osdFleet().startOSD(t.Context(), osdLaunch{osd: &OSDContainer{ID: 3}, recipe: recipe})
	if err == nil || !strings.Contains(err.Error(), "do not replay") || creates != 0 {
		t.Fatalf("startOSD after release = %v, creations %d", err, creates)
	}

	// Control: a started launch keeps its claim without confirmation.
	recipe = &osdLaunchRecipe{id: 4}
	if err := s.claim(4, "lease-b", claimDevice("b", 2), &recipe.started); err != nil {
		t.Fatal(err)
	}
	if err := cluster.osdFleet().startOSD(t.Context(), osdLaunch{osd: &OSDContainer{ID: 4}, recipe: recipe}); err != nil || creates != 1 {
		t.Fatalf("control start = %v, creations %d", err, creates)
	}
	if s.release(4, false) {
		t.Fatal("a started launch lost its claim without a confirmed container")
	}
}

func TestOSDDeviceClaimsSettle(t *testing.T) {
	var empty osdDeviceClaims
	if err := empty.settle(false); err != nil {
		t.Fatalf("empty table: %v", err)
	}
	build := func() (*osdDeviceClaims, *atomic.Bool, *atomic.Bool) {
		var s osdDeviceClaims
		unstarted := mustClaim(t, &s, 1, "lease-a", claimDevice("a", 1))
		started := mustClaim(t, &s, 2, "lease-b", claimDevice("b", 2))
		started.Store(true)
		return &s, unstarted, started
	}
	s, unstarted, started := build()
	err := s.settle(false)
	if err == nil || !strings.Contains(err.Error(), "osd.2 lease lease-b device /dev/mapper/b") || strings.Contains(err.Error(), "osd.1") {
		t.Fatalf("settle without a scan = %v", err)
	}
	if s.count() != 1 || !unstarted.Load() || !started.Load() {
		t.Fatalf("settle without a scan left %d claims", s.count())
	}
	if err := s.settle(false); err == nil || s.count() != 1 {
		t.Fatalf("a repeated settle changed the outcome: %v, %d", err, s.count())
	}
	if err := s.settle(true); err != nil || s.count() != 0 {
		t.Fatalf("settle after the scan = %v, %d claims", err, s.count())
	}
	s, _, _ = build()
	if err := s.settle(true); err != nil || s.count() != 0 {
		t.Fatalf("settle after the scan, fresh = %v, %d claims", err, s.count())
	}
}

// publishAttempted publishes the claim of an OSD handle whose creation was
// attempted, with the given container (nil: the create reply was lost).
func publishAttempted(t *testing.T, c *Container, id int, ctr testcontainers.Container, lease string, device osdBlockDevice) {
	t.Helper()
	osd := &OSDContainer{ID: id}
	if ctr != nil {
		osd.Container = ctr
	}
	c.fleet.osdHandles.register(id, osd)
	mustClaim(t, &c.fleet.deviceClaims, id, lease, device).Store(true)
}

func TestTerminateReleasesDeviceClaimsByContainerEvidence(t *testing.T) {
	t.Run("terminated container releases in the OSD loop", func(t *testing.T) {
		docker := &fakeOwnedDocker{removeErr: map[string]error{"survivor": errors.New("device busy")},
			containers: map[string]map[string]string{"survivor": {clusterOwnerLabel: "c"}}}
		c := ownedCluster("c", docker)
		ctr := &osdMemoryContainerFake{}
		publishAttempted(t, c, 0, ctr, "lease-a", claimDevice("a", 1))
		// The scan fails, so only the OSD loop can have released the claim.
		if err := c.Terminate(t.Context()); err == nil || strings.Contains(err.Error(), "claims retained") {
			t.Fatalf("Terminate = %v", err)
		}
		if c.fleet.deviceClaims.count() != 0 || ctr.terminations != 1 {
			t.Fatalf("claims %d, terminations %d", c.fleet.deviceClaims.count(), ctr.terminations)
		}
	})
	t.Run("failed termination keeps the claim until the scan removes it", func(t *testing.T) {
		docker := &fakeOwnedDocker{containers: map[string]map[string]string{"osd": {clusterOwnerLabel: "c"}}}
		c := ownedCluster("c", docker)
		publishAttempted(t, c, 0, &osdMemoryContainerFake{terminateErr: errors.New("daemon busy")}, "lease-a", claimDevice("a", 1))
		if err := c.Terminate(t.Context()); err == nil || strings.Contains(err.Error(), "claims retained") {
			t.Fatalf("Terminate = %v", err)
		}
		if c.fleet.deviceClaims.count() != 0 || len(docker.removed) != 1 {
			t.Fatalf("claims %d, removed %v", c.fleet.deviceClaims.count(), docker.removed)
		}
	})
	t.Run("lost create reply is released only by the scan", func(t *testing.T) {
		docker := &fakeOwnedDocker{containers: map[string]map[string]string{"lost-osd": {clusterOwnerLabel: "c"}}}
		c := ownedCluster("c", docker)
		publishAttempted(t, c, 0, nil, "lease-a", claimDevice("a", 1))
		if err := c.Terminate(t.Context()); err != nil || c.fleet.deviceClaims.count() != 0 || len(docker.removed) != 1 {
			t.Fatalf("Terminate = %v, claims %d, removed %v", err, c.fleet.deviceClaims.count(), docker.removed)
		}
	})
	t.Run("lost create reply with a failed scan stays claimed and is reported", func(t *testing.T) {
		docker := &fakeOwnedDocker{removeErr: map[string]error{"lost-osd": errors.New("device busy")},
			containers: map[string]map[string]string{"lost-osd": {clusterOwnerLabel: "c"}}}
		c := ownedCluster("c", docker)
		publishAttempted(t, c, 0, nil, "lease-a", claimDevice("a", 1))
		err := c.Terminate(t.Context())
		if err == nil || !strings.Contains(err.Error(), "osd.0 lease lease-a") || c.fleet.deviceClaims.count() != 1 {
			t.Fatalf("Terminate = %v, claims %d", err, c.fleet.deviceClaims.count())
		}
	})
	t.Run("lost create reply of an unowned cluster is never released by a scan that did nothing", func(t *testing.T) {
		c := ownedCluster("", &fakeOwnedDocker{})
		publishAttempted(t, c, 0, nil, "lease-a", claimDevice("a", 1))
		err := c.Terminate(t.Context())
		if err == nil || !strings.Contains(err.Error(), "claims retained") || c.fleet.deviceClaims.count() != 1 {
			t.Fatalf("Terminate = %v, claims %d", err, c.fleet.deviceClaims.count())
		}
	})
	t.Run("a launch that never created a container keeps no claim", func(t *testing.T) {
		c := ownedCluster("", &fakeOwnedDocker{})
		c.fleet.osdHandles.register(0, &OSDContainer{ID: 0})
		creation := mustClaim(t, &c.fleet.deviceClaims, 0, "lease-a", claimDevice("a", 1))
		if err := c.Terminate(t.Context()); err != nil || c.fleet.deviceClaims.count() != 0 || !creation.Load() {
			t.Fatalf("Terminate = %v, claims %d", err, c.fleet.deviceClaims.count())
		}
	})
	t.Run("an empty table changes nothing", func(t *testing.T) {
		if err := ownedCluster("c", &fakeOwnedDocker{}).Terminate(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

// osdDeviceClaimMethods pins the component: the calls each method makes,
// receiver included, ignoring message construction. None takes a lock or calls
// into Container.
var osdDeviceClaimMethods = map[string][]string{
	"claim":   {"creation.Load()", "device.validate()", "osdDeviceLease.MatchString(lease)", "other.device.number()", "device.number()", "device.number()", "s.osds()"},
	"claimOf": {"r.creation.Load()"},
	"count":   nil,
	// exclusionOf is the read-only receipt of the chain-release gate
	// (osd_device_release_gate.go); its caller table is osdChainReleaseCallers.
	"exclusionOf": {"s.osds()", "other.device.number()", "device.number()"},
	"osds":        nil,
	"release":     {"r.creation.CompareAndSwap(false, true)", "delete(s.records, osd)"},
	"settle":      {"s.osds()", "s.release(id, scanRemoved)"},
}

// osdDeviceClaimIgnoredCall reports the calls the pin leaves out: error and
// message construction, the builtins that only measure or grow a local, and the
// iterator helper of osds.
func osdDeviceClaimIgnoredCall(call string) bool {
	for _, prefix := range []string{"errors.", "fmt.", "strings.Join(", "append(", "len(", "slices.Sorted(", "yield("} {
		if strings.HasPrefix(call, prefix) {
			return true
		}
	}
	return false
}

// osdDeviceClaimCallers is the production caller table (see stateCaller):
// the release hooks and the chain judge take the topology lock themselves, the
// wiring step and the terminate step hold it. claim has no other production
// caller.
var osdDeviceClaimCallers = map[string]stateCaller{
	"removeOSD": {"takes", []string{"release"}},
	"terminate": {"takes", []string{"settle"}},
	// registerBlockDeviceOSD's callers are also pinned by osdDeviceWiringCallers.
	"registerBlockDeviceOSD": {"holds", []string{"claim"}},
	// terminateOSDs is called only by terminate (TestTerminateStepsAreCalledOnlyByTerminate).
	"terminateOSDs": {"holds", []string{"release"}},
	// judgeDeviceChain's use is also pinned by osdChainReleaseCallers.
	"judgeDeviceChain": {"takes", []string{"exclusionOf"}},
}

func osdDeviceClaimUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "deviceClaims" }

func TestOSDDeviceClaimsFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.deviceClaims", use: osdDeviceClaimUse, own: isOSDDeviceClaimsReceiver, methods: osdDeviceClaimMethods, bound: osdFleetBinding}.check(t)
}

func isOSDDeviceClaimsReceiver(fn *ast.FuncDecl) bool {
	return receiverTypeName(fn) == "osdDeviceClaims"
}

func TestOSDDeviceClaimsMethodsMakeOnlyTheirPinnedCallsAndHaveNoMutex(t *testing.T) {
	methodAudit{label: "osdDeviceClaims", own: isOSDDeviceClaimsReceiver, methods: osdDeviceClaimMethods,
		writers: map[string][]string{"records": {"claim", "release"}},
		render: func(call *ast.CallExpr) string {
			if text := types.ExprString(call); !osdDeviceClaimIgnoredCall(text) {
				return text
			}
			return ""
		}}.check(t)
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		if file == "osd_device_claims.go" {
			for _, imp := range parsed.Imports {
				if imp.Path.Value == `"sync"` {
					problems = append(problems, "osd_device_claims.go imports sync")
				}
			}
			ast.Inspect(parsed, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Mutex" || sel.Sel.Name == "RWMutex" || sel.Sel.Name == "Lock" || sel.Sel.Name == "Unlock") {
					problems = append(problems, "osd_device_claims.go uses a mutex")
				}
				return true
			})
			for _, decl := range parsed.Decls {
				if gen, ok := decl.(*ast.GenDecl); ok {
					for _, spec := range gen.Specs {
						if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() {
							problems = append(problems, "osd_device_claims.go declares exported "+ts.Name.Name)
						}
					}
				}
			}
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && isOSDDeviceClaimsReceiver(fn) && fn.Name.IsExported() {
				problems = append(problems, fn.Name.Name+" is exported")
			}
		}
	}
	if len(problems) != 0 {
		t.Fatalf("osdDeviceClaims breaks its lock contract: %v", problems)
	}
}

// TestOSDDeviceClaimCallersAreTheReleaseHooksAndTheWiringStep is the
// caller-table guard: claim is called only by the lock-holding wiring step
// (osd_device_wiring.go), the release hooks only by the listed functions, and
// each of those takes lockTopology before its first use. A holder never takes
// the lock itself.
func TestOSDDeviceClaimCallersAreTheReleaseHooksAndTheWiringStep(t *testing.T) {
	callerAudit{label: "the OSD device claims", use: osdDeviceClaimUse, own: isOSDDeviceClaimsReceiver, callers: osdDeviceClaimCallers}.check(t)
}

// TestOSDDeviceClaimReleaseOrderAgainstContainerRemoval guards the release
// ordering: in terminate the per-OSD release sits after osd.Terminate and
// before the handle is forgotten, inside the loop that continues past a failed
// termination; settle comes after the labeled-container removal, whose error it
// reads; in removeOSD the release comes after the container terminated and the
// storage directory was discarded, immediately before the handle is forgotten.
func TestOSDDeviceClaimReleaseOrderAgainstContainerRemoval(t *testing.T) {
	positions := func(fn *ast.FuncDecl) map[string][]token.Pos {
		at := map[string][]token.Pos{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				at[types.ExprString(call)] = append(at[types.ExprString(call)], call.Pos())
			}
			return true
		})
		return at
	}
	check := func(label string, at map[string][]token.Pos, order []string) []string {
		var problems []string
		for _, name := range order {
			if len(at[name]) != 1 {
				problems = append(problems, label+": "+name+" must be called exactly once")
			}
		}
		if len(problems) == 0 {
			for i := 1; i < len(order); i++ {
				if at[order[i-1]][0] >= at[order[i]][0] {
					problems = append(problems, label+": "+order[i-1]+" must come before "+order[i])
				}
			}
		}
		return problems
	}
	var problems []string
	sawTerminate, sawRemove := false, false
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			switch {
			case isContainerTerminate(fn):
				sawTerminate = true
				at := positions(fn)
				problems = append(problems, check("terminate", at, []string{
					"c.lockTopology(ctx)",
					"osd.Terminate(ctx, opts...)",
					"c.fleet.deviceClaims.release(id, osd.Container != nil)",
					"c.fleet.osdHandles.forget(id)",
					"c.startup.removeLabeledContainers(ctx)",
					"c.fleet.deviceClaims.settle(scanErr == nil && c.startup.ownerID() != \"\")",
					"c.runDir.release(ctx)",
				})...)
			case fn.Name.Name == "removeOSD" && receiverTypeName(fn) == "Container":
				sawRemove = true
				at := positions(fn)
				problems = append(problems, check("removeOSD", at, []string{
					"c.lockTopology(ctx)",
					"osd.Terminate(ctx)",
					"c.fleet.stores.discard(ctx, id)",
					"c.fleet.deviceClaims.release(id, osd.Container != nil)",
					"c.fleet.osdHandles.forget(id)",
				})...)
			}
		}
	}
	if !sawTerminate || !sawRemove {
		problems = append(problems, "terminate or removeOSD not found")
	}
	if len(problems) != 0 {
		t.Fatalf("device claim release order changed: %v", problems)
	}
}
