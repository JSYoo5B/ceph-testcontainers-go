package ceph

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Audit of the stale-handle question that the owner's Exclusive decision of
// 2026-10-28 left to the extraction (issue 155, design note section "Exclusive
// stale-handle audit"): SharedCluster.Exclusive lends the core *Container, so a
// retained Container or derived handle must not keep an interval's authority.
// These controls enforce the closure of the two gaps the audit found, without
// Docker: the release retires the cluster's gate mode (gap 1) and a descriptor
// that a tenant obtained is admitted as a tenant's whatever the cluster's mode
// is later (gap 2). What stays open is named in the design note: the raw
// embedded Container field is not behind the gate.

// sharingModeWrites returns "callee(argument)" for every call in src that
// stores a cluster's sharing mode: setSharing, SetSharing, setSharingMode and the atomic
// Store of the sharing field.
func sharingModeWrites(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var writes []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch callee := types.ExprString(sel); {
		case sel.Sel.Name == "setSharing", sel.Sel.Name == "SetSharing", sel.Sel.Name == "setSharingMode", strings.HasSuffix(callee, "sharing.Store"):
			writes = append(writes, callee+"("+types.ExprString(call.Args[0])+")")
		}
		return true
	})
	return writes
}

// TestExclusiveAuditSharingModeOnlyMovesForward pins that no production code
// lowers the mode of a cluster. A cluster starts shared with tenants, becomes
// exclusive once, is retired by the release of that interval, and is never
// returned to tenants, so one *Container is never the cluster of a tenant
// interval after an exclusive interval and generation reuse of the same
// cluster handle cannot happen.
func TestExclusiveAuditSharingModeOnlyMovesForward(t *testing.T) {
	// The only writers: the two orchestration sites in ceph/harness, the
	// adapter and the backend method that forward, the core setter, and the
	// unit-test cluster builder.
	want := map[string][]string{
		"harness/shared.go":           {"backend.setSharing(sharedWithTenants)"},
		"harness/shared_exclusive.go": {"inst.backend.setSharing(sharedExclusively)", "inst.backend.setSharing(sharedRetired)"},
		"harness/core.go":             {"h.b.SetSharing(int32(mode))"},
		"shared_backend.go":           {"b.c.setSharingMode(sharingMode(mode))"},
		"shared_cluster_port.go":      {"c.sharing.Store(int32(mode))"},
		"unit_access.go":              {"c.sharing.Store(int32(sharedWithTenants))"},
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	more, err := filepath.Glob("harness/*.go")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, more...)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		got := sharingModeWrites(t, name, string(src))
		if !slices.Equal(got, want[name]) {
			t.Errorf("%s writes the sharing mode as %v, want %v: a new writer needs the stale-handle audit", name, got, want[name])
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("%s no longer exists but the audit lists it", name)
	}
}

// TestExclusiveAuditSharingModeScannerSeesADemotion is the negative control:
// the scanner reports a demotion, so the test above fails for one.
func TestExclusiveAuditSharingModeScannerSeesADemotion(t *testing.T) {
	src := `package ceph
func (s *SharedCluster) releaseExclusive(inst *sharedInstance) {
	inst.backend.setSharing(sharedWithTenants)
	c.sharing.Store(int32(notShared))
}`
	got := sharingModeWrites(t, "demote.go", src)
	want := []string{"inst.backend.setSharing(sharedWithTenants)", "c.sharing.Store(int32(notShared))"}
	if !slices.Equal(got, want) {
		t.Fatalf("scanner = %v, want %v", got, want)
	}
	if slices.Equal(got, []string{"inst.backend.setSharing(sharedExclusively)"}) {
		t.Fatal("a demoting release is indistinguishable from the real one")
	}
}

// releaseRetiresBeforeTerminating reports whether the function releaseExclusive
// of src marks the cluster retired before any call that terminates it.
func releaseRetiresBeforeTerminating(t *testing.T, src string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "release.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "releaseExclusive" {
			continue
		}
		retired, terminated := -1, -1
		for i, stmt := range fn.Body.List {
			ast.Inspect(stmt, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := types.ExprString(call.Fun); {
				case strings.HasSuffix(callee, "setSharing") && len(call.Args) == 1 && types.ExprString(call.Args[0]) == "sharedRetired":
					if retired < 0 {
						retired = i
					}
				case strings.HasSuffix(callee, "terminateDetached"), strings.HasSuffix(callee, "terminate"):
					if terminated < 0 {
						terminated = i
					}
				}
				return true
			})
		}
		return retired >= 0 && terminated >= 0 && retired < terminated
	}
	return false
}

// TestExclusiveAuditReleaseRetiresBeforeTerminating pins that the release of
// an exclusive interval lowers the gate before it ends the cluster, so a
// termination that fails leaves a retired cluster and not an exclusive one.
func TestExclusiveAuditReleaseRetiresBeforeTerminating(t *testing.T) {
	src, err := os.ReadFile("harness/shared_exclusive.go")
	if err != nil {
		t.Fatal(err)
	}
	if !releaseRetiresBeforeTerminating(t, string(src)) {
		t.Fatal("releaseExclusive does not mark the cluster retired before it terminates it")
	}
}

// TestExclusiveAuditReleaseOrderScannerSeesABadRelease is the negative
// control: a release that terminates first, or never retires, is reported.
func TestExclusiveAuditReleaseOrderScannerSeesABadRelease(t *testing.T) {
	for name, src := range map[string]string{
		"retires first": `package h
func (s *S) releaseExclusive(inst *I) {
	defer s.endExclusive()
	inst.backend.setSharing(sharedRetired)
	_ = s.terminateDetached(inst)
}`,
	} {
		if !releaseRetiresBeforeTerminating(t, src) {
			t.Errorf("%s: a correct release was reported as wrong", name)
		}
	}
	for name, src := range map[string]string{
		"terminates first": `package h
func (s *S) releaseExclusive(inst *I) {
	_ = s.terminateDetached(inst)
	inst.backend.setSharing(sharedRetired)
}`,
		"never retires": `package h
func (s *S) releaseExclusive(inst *I) {
	_ = s.terminateDetached(inst)
}`,
		"retires to exclusive": `package h
func (s *S) releaseExclusive(inst *I) {
	inst.backend.setSharing(sharedExclusively)
	_ = s.terminateDetached(inst)
}`,
		"no release": `package h
func (s *S) other() {}`,
	} {
		if releaseRetiresBeforeTerminating(t, src) {
			t.Errorf("%s: a wrong release was reported as correct", name)
		}
	}
}

// handleFamilies returns one admission probe per handle family that carries
// an owner, each on the same cluster, and the tenant-obtained descriptor and
// the leased Pool separately.
type handleFamily struct {
	name  string
	op    string
	admit func() error
}

func handleFamilies(cluster *Container) []handleFamily {
	own := &Pool{owner: cluster, ident: &poolIdentity{bound: true}}
	return []handleFamily{
		{"ClientConfig", "ClientConfig.UpdateCaps", func() error { _, err := (&ClientConfig{owner: cluster}).cluster("ClientConfig.UpdateCaps"); return err }},
		{"OSDContainer", "OSDContainer.SetIn", func() error { _, err := (&OSDContainer{owner: cluster}).cluster("OSDContainer.SetIn"); return err }},
		{"MonitorContainer", "MonitorContainer.Remove", func() error {
			_, err := (&MonitorContainer{owner: cluster}).cluster("MonitorContainer.Remove")
			return err
		}},
		{"ManagerContainer", "ManagerContainer.RotateKey", func() error {
			_, err := (&ManagerContainer{owner: cluster}).cluster("ManagerContainer.RotateKey")
			return err
		}},
		{"DetachedOSD", "DetachedOSD.Reopen", func() error { return (&DetachedOSD{cluster: cluster}).cluster.admitHandle("DetachedOSD.Reopen") }},
		{"Pool without a lease", "Pool.SetQuota", func() error { _, err := own.cluster("Pool.SetQuota"); return err }},
	}
}

// TestExclusiveAuditHandleAdmissionFollowsTheLiveMode shows how each handle
// family that carries an owner reads the sharing mode. The families a test
// gets from the lent *Container (OSD, monitor and manager handles, a
// detached OSD, a client or a pool the exclusive interval created) read the
// owner's current mode: admitted in the exclusive interval, refused once the
// release retired the cluster (gap 1). A client descriptor that a tenant
// obtained, and the tenant's leased Pool, are admitted as a tenant's in every
// mode, so the exclusive interval that later takes their cluster does not
// widen them (gap 2), and the retirement does not either.
func TestExclusiveAuditHandleAdmissionFollowsTheLiveMode(t *testing.T) {
	cluster := &Container{}
	cluster.sharing.Store(int32(sharedWithTenants))

	leased := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return nil }}}
	expired := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return errors.New("released") }}}
	tenantClient := &ClientConfig{owner: cluster, tenant: true}
	tenantAdmit := func() error { _, err := tenantClient.cluster("ClientConfig.UpdateCaps"); return err }
	families := handleFamilies(cluster)

	for _, f := range families {
		if err := f.admit(); !errors.Is(err, ErrSharedCluster) {
			t.Errorf("%s: %s under tenants returned %v, want ErrSharedCluster", f.name, f.op, err)
		}
	}
	if err := tenantAdmit(); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a tenant descriptor under tenants returned %v, want ErrSharedCluster", err)
	}
	for _, p := range []*Pool{leased, expired} {
		if _, err := p.cluster("Pool.SetQuota"); err == nil {
			t.Error("a tenant Pool was admitted for a cluster-wide operation under tenants")
		}
	}

	// The same cluster becomes exclusive, as the cluster that tenants used does
	// when an Exclusive takes it.
	cluster.sharing.Store(int32(sharedExclusively))
	for _, f := range families {
		if err := f.admit(); err != nil {
			t.Errorf("%s: %s under the exclusive mode returned %v; the interval's own handles must be admitted", f.name, f.op, err)
		}
	}
	if err := tenantAdmit(); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a descriptor that a tenant obtained gained the exclusive interval: %v", err)
	}
	if _, err := leased.cluster("Pool.SetQuota"); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a leased tenant Pool gained the exclusive interval: %v", err)
	}
	if _, err := expired.cluster("Pool.Status"); err == nil || errors.Is(err, ErrSharedCluster) {
		t.Errorf("an expired tenant Pool returned %v, want the lease error before admission", err)
	}

	// The release retires the cluster: nothing the interval lent is admitted.
	cluster.sharing.Store(int32(sharedRetired))
	for _, f := range families {
		if err := f.admit(); !errors.Is(err, ErrSharedCluster) {
			t.Errorf("%s: %s on a retired cluster returned %v, want ErrSharedCluster", f.name, f.op, err)
		}
	}
	if err := tenantAdmit(); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a tenant descriptor on a retired cluster returned %v, want ErrSharedCluster", err)
	}
	if _, err := leased.cluster("Pool.SetQuota"); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a leased tenant Pool on a retired cluster returned %v", err)
	}
}

// TestExclusiveAuditRetiredClusterAdmitsOnlyOpenOperations checks every
// classified operation: a retired cluster admits exactly those that only read
// state, and names the operation in the refusal.
func TestExclusiveAuditRetiredClusterAdmitsOnlyOpenOperations(t *testing.T) {
	retired := &Container{}
	retired.sharing.Store(int32(sharedRetired))
	check := func(kind, name string, policy sharedPolicy, err error) {
		t.Helper()
		if open := policy == sharedOpen; open != (err == nil) {
			t.Errorf("%s %s (policy %d) on a retired cluster: %v", kind, name, policy, err)
		}
		if err != nil {
			var operation *OperationError
			if !errors.Is(err, ErrSharedCluster) || !errors.As(err, &operation) || operation.Op != name {
				t.Errorf("%s %s refused with %v, want ErrSharedCluster naming the operation", kind, name, err)
			}
		}
	}
	for name, policy := range sharedMethodPolicies {
		check("method", name, policy, retired.sharedGate(name))
	}
	for name, policy := range sharedHandlePolicies {
		check("handle", name, policy, retired.admitHandle(name))
	}
	for name, policy := range sharedFaultPolicies {
		check("fault", name, policy, retired.admitFault(name))
	}
	for name, policy := range sharedOverridePolicies {
		check("override", name, policy, retired.admitOverride(name))
	}
}

// clientLiteralTenantFlags returns, for every ClientConfig literal in src, the
// text of its tenant field ("" when absent).
func clientLiteralTenantFlags(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var flags []string
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok || types.ExprString(lit.Type) != "ClientConfig" {
			return true
		}
		flag := ""
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok && types.ExprString(kv.Key) == "tenant" {
				flag = types.ExprString(kv.Value)
			}
		}
		flags = append(flags, flag)
		return true
	})
	return flags
}

// TestExclusiveAuditOnlyTheTenantCreationMarksADescriptor pins who sets the
// tenant mark: the creation that a tenant's release deletes (createSharedClient)
// sets it, Container.CreateClient, which only a test of its own cluster or an
// exclusive interval calls, does not, and the copies that ReplaceKey makes
// carry it on.
func TestExclusiveAuditOnlyTheTenantCreationMarksADescriptor(t *testing.T) {
	for name, want := range map[string][]string{
		"shared_cluster_port.go": {"true"},
		"auth.go":                {""},
		"client_key.go":          {"client.tenant", "client.tenant"},
	} {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := clientLiteralTenantFlags(t, name, string(src)); !slices.Equal(got, want) {
			t.Errorf("%s: tenant fields of the ClientConfig literals = %q, want %q", name, got, want)
		}
	}
	// Negative control: the scanner reports a dropped or misplaced mark.
	got := clientLiteralTenantFlags(t, "x.go", `package ceph
var a = &ClientConfig{owner: c, name: n}
var b = &ClientConfig{owner: c, tenant: true}
var c = &ClientConfig{tenant: client.tenant}`)
	if want := []string{"", "true", "client.tenant"}; !slices.Equal(got, want) {
		t.Fatalf("scanner = %q, want %q", got, want)
	}
}

// iotaConsts returns the names of the first const block of src that uses iota,
// in order.
func iotaConsts(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		var names []string
		uses := false
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for _, v := range value.Values {
				if types.ExprString(v) == "iota" {
					uses = true
				}
			}
			for _, n := range value.Names {
				names = append(names, n.Name)
			}
		}
		if uses {
			return names
		}
	}
	return nil
}

// TestExclusiveAuditModeValuesMatchTheHarnessContract pins that the gate's
// modes and the integers that ceph/harness passes through
// internal/harnesshook have the same names in the same order: the harness
// marks a cluster retired with the value it names, and the gate reads it by
// number.
func TestExclusiveAuditModeValuesMatchTheHarnessContract(t *testing.T) {
	core, err := os.ReadFile("shared_gate.go")
	if err != nil {
		t.Fatal(err)
	}
	hook, err := os.ReadFile("../internal/harnesshook/harnesshook.go")
	if err != nil {
		t.Fatal(err)
	}
	coreNames := iotaConsts(t, "shared_gate.go", string(core))
	hookNames := iotaConsts(t, "harnesshook.go", string(hook))
	want := []string{"notShared", "sharedWithTenants", "sharedExclusively", "sharedRetired"}
	if !slices.Equal(coreNames, want) {
		t.Errorf("gate modes = %v, want %v", coreNames, want)
	}
	if len(hookNames) != len(coreNames) {
		t.Fatalf("hook modes = %v, gate modes = %v", hookNames, coreNames)
	}
	for i, name := range hookNames {
		if !strings.EqualFold(name, coreNames[i]) {
			t.Errorf("mode %d is %s in the harness contract and %s in the gate", i, name, coreNames[i])
		}
	}
}

// TestExclusiveAuditLeasedPoolRefusesByLeaseBeforeTheGate pins the two distinct
// refusals: a Pool a tenant obtained checks its lease before the gate, so once
// the tenant is released it returns the lease's ErrInvalidRequest in every
// mode (retired included), while a handle admitted through the gate returns
// ErrSharedCluster on a retired cluster. A live lease is refused by the gate.
func TestExclusiveAuditLeasedPoolRefusesByLeaseBeforeTheGate(t *testing.T) {
	cluster := &Container{}
	released := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return invalidRequest("tenant t1 is released") }}}
	live := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return nil }}}
	for _, mode := range []sharingMode{sharedWithTenants, sharedExclusively, sharedRetired} {
		cluster.sharing.Store(int32(mode))
		_, err := released.cluster("Pool.SetQuota")
		if !errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrSharedCluster) {
			t.Errorf("mode %d: a released tenant's Pool returned %v, want the lease's ErrInvalidRequest", mode, err)
		}
		if _, err := live.cluster("Pool.SetQuota"); !errors.Is(err, ErrSharedCluster) {
			t.Errorf("mode %d: a live tenant Pool returned %v, want the gate's ErrSharedCluster", mode, err)
		}
	}
	cluster.sharing.Store(int32(sharedRetired))
	if _, err := (&Pool{owner: cluster, ident: &poolIdentity{bound: true}}).cluster("Pool.SetQuota"); !errors.Is(err, ErrSharedCluster) || errors.Is(err, ErrInvalidRequest) {
		t.Errorf("a gate-admitted Pool on a retired cluster returned %v, want only ErrSharedCluster", err)
	}
}
