package cephfs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type stoppedMDSEdit func(n *stoppedMDSControl, m map[string]any, survivor *stoppedMDSDaemon)

// stoppedMDSDecode runs the real stopped-MDS decoder over the fixture map after
// edit changed it, with the fixture's survivor daemon and pool identity.
func stoppedMDSDecode(t *testing.T, edit stoppedMDSEdit) (*cephFSMDSRemovalScope, error) {
	t.Helper()
	fs, native, target, _, survivor := stoppedMDSFixture(t)
	// The pool identity comes from the same dump, so it is read from the
	// settled map and only the decoded bytes carry the edit.
	pools, err := fs.id.readNativePools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(native, native.target(), survivor)
	}
	data := []byte(coldMDSTestJSON(native.native))
	if native.raw != "" {
		data = []byte(native.raw)
	}
	return fs.fleet().decodeStoppedMDSScope(t.Context(), target.identity, pools, data)
}

func stoppedMDSInfoRow(m map[string]any, key string) map[string]any {
	return m["info"].(map[string]any)[key].(map[string]any)
}

func TestStoppedMDSDecodeAcceptsTheSettledBaseline(t *testing.T) {
	scope, err := stoppedMDSDecode(t, nil)
	if err != nil || scope == nil || scope.active != 1 || scope.standby != 1 || !scope.joinable || scope.replay || !scope.protected || !slices.Equal(scope.dataPools, []int64{1}) || !slices.Equal(scope.poolNames, []string{"target-data"}) {
		t.Fatalf("baseline decode changed: %+v %v", scope, err)
	}
}

// TestStoppedMDSDecodeKeepsEveryGuardAndItsClassification pins, for the
// stopped-MDS global-map decoder, the exact refusal text of each guard group.
// It characterizes the decoder as it is: the decode-versus-survivor split must
// keep every guard on the same side of the final scope return, and an
// unsupported state such as up:oneshot-replay stays refused. The "no complete
// active survivor set" guard is not listed: the earlier policy and membership
// guards already refuse every map that could reach it, so no fixture isolates
// it, and it stays as defense in depth. It is a Docker-free control, not native
// evidence.
func TestStoppedMDSDecodeKeepsEveryGuardAndItsClassification(t *testing.T) {
	const (
		decode     = "decode stopped MDS global map"
		malformed  = "stopped MDS global daemon identity is present or malformed"
		standbyBad = "stopped MDS global standby identity is present or malformed"
		registered = "original stopped MDS name is still registered"
		entries    = "stopped MDS filesystem entries are incomplete or duplicated"
		assigned   = "global stopped MDS assigned membership is incomplete"
		contradict = "global stopped MDS info membership is contradictory"
		policy     = "original stopped MDS filesystem policy or settled ranks changed"
		ranks      = "stopped MDS active rank set is malformed"
		unsettled  = "stopped MDS target has unsettled or foreign assigned members"
		laggySb    = "owned MDS standby is laggy or has malformed laggy state"
		survivors  = "stopped MDS has an unconfirmed or foreign survivor"
		unowned    = "stopped MDS target includes unowned or missing survivors"
		notRunning = "original MDS survivor is not running normally"
	)
	setState := func(key, state string) stoppedMDSEdit {
		return func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, key)["state"] = state
		}
	}
	setPolicy := func(field string, value any) stoppedMDSEdit {
		return func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) { m[field] = value }
	}
	setFlag := func(flag string, value bool) stoppedMDSEdit {
		return func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			m["flags_state"].(map[string]any)[flag] = value
		}
	}
	standby := func(state string, rank int, extra map[string]any) stoppedMDSEdit {
		return func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			row := coldMDSTestRow("target-9", state, 19, rank, 7)
			for k, v := range extra {
				row[k] = v
			}
			n.native["standbys"] = []any{row}
		}
	}
	cases := []struct {
		name string
		edit stoppedMDSEdit
		want string
	}{
		{"decode-null", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) { n.raw = "null" }, decode},
		{"decode-trailing", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.raw = coldMDSTestJSON(n.native) + " {}"
		}, decode},
		{"decode-standbys-null", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) { n.native["standbys"] = nil }, decode},
		{"decode-filesystems-missing", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) { delete(n.native, "filesystems") }, decode},
		{"target-in-info", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			m["info"].(map[string]any)["gid_11"] = coldMDSTestRow("target-0", "up:standby-replay", 11, 0, 7)
		}, registered},
		{"target-in-standbys", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.native["standbys"] = []any{coldMDSTestRow("target-0", "up:standby", 11, -1, 7)}
		}, registered},
		{"info-state-oneshot-replay", setState("gid_12", "up:oneshot-replay"), malformed},
		{"info-state-down", setState("gid_12", "down:stopped"), malformed},
		{"info-state-unknown-name", setState("gid_12", "???"), malformed},
		{"info-state-empty", setState("gid_12", ""), malformed},
		{"info-state-standby", setState("gid_12", "up:standby"), malformed},
		{"info-state-boot", setState("gid_12", "up:boot"), malformed},
		{"info-key-mismatch", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			m["info"] = map[string]any{"gid_012": stoppedMDSInfoRow(m, "gid_12")}
		}, malformed},
		{"info-row-gid-zero", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["gid"] = 0
		}, malformed},
		{"info-row-negative-rank", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["rank"] = -1
		}, malformed},
		{"sibling-duplicate-name", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": coldMDSTestMap("target", 9, 10)})
		}, entries},
		{"filesystem-negative-id", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": -1, "mdsmap": coldMDSTestMap("sibling", 9, 10)})
		}, entries},
		{"up-without-info", setPolicy("up", map[string]any{"mds_0": uint64(999)}), assigned},
		{"up-bad-key", setPolicy("up", map[string]any{"mds_00": uint64(12)}), assigned},
		{"info-contradicts-up", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			m["info"].(map[string]any)["gid_14"] = coldMDSTestRow("target-4", "up:active", 14, 0, 7)
		}, contradict},
		{"policy-max-mds", setPolicy("max_mds", 2), policy},
		{"policy-standby-count", setPolicy("standby_count_wanted", 0), policy},
		{"policy-metadata-pool", setPolicy("metadata_pool", 2), policy},
		{"policy-data-pools", setPolicy("data_pools", []int64{2}), policy},
		{"policy-not-joinable", setFlag("joinable", false), policy},
		{"policy-standby-replay", setFlag("allow_standby_replay", true), policy},
		{"policy-not-protected", setFlag("refuse_standby_for_another_fs", false), policy},
		{"policy-failed-rank", setPolicy("failed", []int{0}), policy},
		{"policy-damaged-rank", setPolicy("damaged", []int{0}), policy},
		{"policy-stopped-rank", setPolicy("stopped", []int{0}), policy},
		{"policy-in-length", setPolicy("in", []int{}), policy},
		{"policy-in-null", setPolicy("in", nil), policy},
		{"policy-flags-null", setPolicy("flags_state", nil), policy},
		{"filesystem-id-drift", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.native["filesystems"].([]any)[0].(map[string]any)["id"] = 99
		}, policy},
		{"in-rank-out-of-range", setPolicy("in", []int{1}), ranks},
		{"info-join-foreign", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["join_fscid"] = 99
		}, unsettled},
		{"info-laggy", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["laggy_since"] = "2026-10-08T01:00:00Z"
		}, unsettled},
		{"info-laggy-null", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["laggy_since"] = nil
		}, unsettled},
		{"info-state-starting", setState("gid_12", "up:starting"), unsettled},
		{"standby-state-oneshot-replay", standby("up:oneshot-replay", -1, nil), standbyBad},
		{"standby-state-down", standby("down:stopped", -1, nil), standbyBad},
		{"standby-state-empty", standby("", -1, nil), standbyBad},
		{"standby-state-active", standby("up:active", -1, nil), standbyBad},
		{"standby-assigned-rank", standby("up:standby", 0, nil), standbyBad},
		{"standby-duplicate-gid", func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
			n.native["standbys"] = []any{coldMDSTestRow("target-9", "up:standby", 12, -1, 7)}
		}, standbyBad},
		{"standby-laggy", standby("up:standby", -1, map[string]any{"laggy_since": "2026-10-08T01:00:00Z"}), laggySb},
		{"survivor-unregistered", func(_ *stoppedMDSControl, m map[string]any, _ *stoppedMDSDaemon) {
			stoppedMDSInfoRow(m, "gid_12")["name"] = "foreign-0"
		}, survivors},
		{"survivor-extra-registered", standby("up:standby", -1, nil), unowned},
		{"survivor-not-running", func(_ *stoppedMDSControl, _ map[string]any, s *stoppedMDSDaemon) { s.state.Running = false }, notRunning},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := stoppedMDSDecode(t, tc.edit)
			if err == nil || err.Error() != tc.want || scope != nil {
				t.Fatalf("decoder guard changed: want %q, got %+v %v", tc.want, scope, err)
			}
		})
	}
}

// calleeName returns the called function or method name of call.
func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

// TestStoppedMDSDecoderIsAPureFunctionOfItsArguments guards the contract of
// stopped_mds_decode.go: the file declares only the decoder, its phase
// functions and their value types, imports no package that can reach a lock, a command or Docker, and
// names none of the owner state. So the decoder cannot start reading the
// roster, the cluster or a container without this test failing.
func TestStoppedMDSDecoderIsAPureFunctionOfItsArguments(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "stopped_mds_decode.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var funcs, types, imports []string
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv != nil {
				t.Errorf("method %s: the decoder file declares functions only", d.Name.Name)
			}
			funcs = append(funcs, d.Name.Name)
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					types = append(types, s.Name.Name)
				case *ast.ImportSpec:
					imports = append(imports, strings.Trim(s.Path.Value, `"`))
				}
			}
		}
	}
	slices.Sort(types)
	slices.Sort(imports)
	wantFuncs := []string{"decodeStoppedMDSMap", "admitStoppedMDSRow", "checkStoppedMDSMembership", "stoppedMDSScope", "collectStoppedMDSSurvivors", "collectStoppedMDSStandbys"}
	wantTypes := []string{"stoppedMDSFields", "stoppedMDSFilesystem", "stoppedMDSMap", "stoppedMDSMapExpectation", "stoppedMDSMapState", "stoppedMDSSeen"}
	if !slices.Equal(funcs, wantFuncs) || !slices.Equal(types, wantTypes) {
		t.Errorf("decoder file declarations changed: funcs %v types %v", funcs, types)
	}
	wantImports := []string{"errors", "github.com/JSYoo5B/ceph-testcontainers-go/internal/naming", "slices", "strconv", "strings"}
	if !slices.Equal(imports, wantImports) {
		t.Errorf("decoder file imports %v, pinned %v", imports, wantImports)
	}
	forbidden := []string{"Filesystem", "fs", "context", "ctx", "servicehook", "clusterutil", "testcontainers", "ceph", "sync", "roster", "members", "Inspect", "Lock", "Unlock", "LockMutex", "Container", "cluster", "setupMu"}
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && slices.Contains(forbidden, id.Name) {
			t.Errorf("decoder file names %q, which is owner state, a lock, a command or Docker", id.Name)
		}
		return true
	})
}

// TestStoppedMDSScopeReadHasOneDecodeThenOneSurvivorVerification pins the
// caller table of the split: the decoder and the survivor verification are
// called only by decodeStoppedMDSScope, decoding comes first and the verified
// scope is what it returns, and Docker inspection appears only in the pinned
// functions (the stopped target proof, the replacement observation and the
// survivor verification), never in the decoder.
func TestStoppedMDSScopeReadHasOneDecodeThenOneSurvivorVerification(t *testing.T) {
	callers := map[string][]string{}
	var scopeFn *ast.FuncDecl
	inspectIn := map[string]int{}
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "decodeStoppedMDSScope" {
				scopeFn = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch name := calleeName(call); name {
				case "decodeStoppedMDSMap", "verifyStoppedMDSSurvivors", "decodeStoppedMDSScope":
					callers[name] = append(callers[name], fn.Name.Name)
				case "Inspect":
					inspectIn[fn.Name.Name]++
				}
				return true
			})
		}
	}
	want := map[string][]string{
		"decodeStoppedMDSMap":       {"decodeStoppedMDSScope"},
		"verifyStoppedMDSSurvivors": {"decodeStoppedMDSScope"},
		"decodeStoppedMDSScope":     {"readStoppedMDSScope"},
	}
	if !reflect.DeepEqual(callers, want) {
		t.Errorf("caller table changed: %v, pinned %v", callers, want)
	}
	if !reflect.DeepEqual(inspectIn, map[string]int{"inspectStoppedMDS": 1, "observeMDSReplacement": 1, "verifyStoppedMDSSurvivors": 1}) {
		t.Errorf("Docker inspection moved: %v", inspectIn)
	}
	if scopeFn == nil {
		t.Fatal("decodeStoppedMDSScope is missing")
	}
	var order []string
	returned := false
	ast.Inspect(scopeFn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if name := calleeName(node); name == "decodeStoppedMDSMap" || name == "verifyStoppedMDSSurvivors" {
				order = append(order, name)
			}
		case *ast.ReturnStmt:
			if len(node.Results) == 1 {
				if call, ok := node.Results[0].(*ast.CallExpr); ok && calleeName(call) == "verifyStoppedMDSSurvivors" {
					returned = true
				}
			}
		}
		return true
	})
	if !slices.Equal(order, []string{"decodeStoppedMDSMap", "verifyStoppedMDSSurvivors"}) || !returned {
		t.Errorf("decode must come first and the verified scope must be returned: %v returned=%v", order, returned)
	}
}

// TestMDSRowStateListHasOneCopy guards the shared allowlist: the state names
// are written only in mds_native_state.go, and each strict decoder reaches them
// through the one function, once.
func TestMDSRowStateListHasOneCopy(t *testing.T) {
	callers := map[string][]string{}
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					if calleeName(node) == "cephFSMDSRowStateAccepted" {
						callers[fn.Name.Name] = append(callers[fn.Name.Name], file)
					}
				case *ast.BasicLit:
					if file != "mds_native_state.go" && strings.Contains(node.Value, `"up:creating"`) {
						t.Errorf("%s: %s writes a second copy of the daemon state list", file, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	want := map[string][]string{
		"decodeColdMDSMap":       {"cold_mds_decode.go"},
		"admitStoppedMDSRow":     {"stopped_mds_decode.go"},
		"admitReplacementMDSRow": {"mds_replacement_decode.go"},
	}
	if !reflect.DeepEqual(callers, want) {
		t.Errorf("state-list callers changed: %v, pinned %v", callers, want)
	}
}
