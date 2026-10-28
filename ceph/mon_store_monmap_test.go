package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
)

// The real fixture is a recorded "ceph mon dump" of a two-monitor Tentacle
// 20.2.4 cluster (members a and b). quorum_status has no recorded fixture: the
// tests build one from the same monmap with the field set that
// Monitor::_quorum_status writes, which is a synthetic input.
func monStoreMonDumpFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/daemon-addresses/20.2.4/mon-dump.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func monStoreDecode(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func monStoreEncode(t *testing.T, doc any) []byte {
	t.Helper()
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// monStoreStatusFixture builds a quorum_status document around the monmap of
// the mon dump fixture.
func monStoreStatusFixture(t *testing.T, ranks []int, names []string, leader string) []byte {
	t.Helper()
	monmap := monStoreDecode(t, monStoreMonDumpFixture(t))
	delete(monmap, "quorum")
	return monStoreEncode(t, map[string]any{
		"election_epoch": 8, "quorum": ranks, "quorum_names": names,
		"quorum_leader_name": leader, "quorum_age": 120,
		"features": map[string]any{"quorum_con": "4540138322906710015"}, "monmap": monmap,
	})
}

func monStoreBothInQuorum(t *testing.T) (dump, status []byte) {
	return monStoreMonDumpFixture(t), monStoreStatusFixture(t, []int{0, 1}, []string{"a", "b"}, "a")
}

func TestMonStoreMonmapDecodesTheRecordedTentacleFixture(t *testing.T) {
	dump, status := monStoreBothInQuorum(t)
	member, err := decodeMonStoreMember(dump, status, "b")
	if err != nil || member == nil {
		t.Fatalf("member b: %v %v", member, err)
	}
	wantAddrs := []string{"v2:172.20.0.4:3300/0", "v1:172.20.0.4:6789/0"}
	if member.name != "b" || member.fsid != "dfca4a09-05f4-4ef4-883f-9dd10c5fcdf7" || !slices.Equal(member.addresses, wantAddrs) || member.quorum != monStoreQuorumIn {
		t.Fatalf("member %+v", member)
	}
}

func TestMonStoreMonmapThreeStateQuorum(t *testing.T) {
	dump := monStoreDecode(t, monStoreMonDumpFixture(t))
	dump["quorum"] = []int{0}
	out := monStoreEncode(t, dump)
	status := monStoreStatusFixture(t, []int{0}, []string{"a"}, "a")
	for name, want := range map[string]monStoreQuorum{"a": monStoreQuorumIn, "b": monStoreQuorumOut} {
		member, err := decodeMonStoreMember(out, status, name)
		if err != nil || member == nil || member.quorum != want {
			t.Fatalf("%s: %+v %v, want quorum %d", name, member, err, want)
		}
	}
	if member, err := decodeMonStoreMember(out, status, "c"); member != nil || err != nil {
		t.Fatalf("a name outside the map: %+v %v", member, err)
	}
}

// monStoreMapMutations are mutations of a monmap object. Each is applied to
// both documents in turn, so a decoder that checks only one of them is caught.
func monStoreMapMutations() map[string]func(m map[string]any) {
	mon := func(m map[string]any, i int) map[string]any { return m["mons"].([]any)[i].(map[string]any) }
	addr := func(m map[string]any) map[string]any {
		return mon(m, 1)["public_addrs"].(map[string]any)["addrvec"].([]any)[0].(map[string]any)
	}
	return map[string]func(map[string]any){
		"epoch missing":        func(m map[string]any) { delete(m, "epoch") },
		"epoch null":           func(m map[string]any) { m["epoch"] = nil },
		"epoch string":         func(m map[string]any) { m["epoch"] = "2" },
		"epoch zero":           func(m map[string]any) { m["epoch"] = 0 },
		"epoch fraction":       func(m map[string]any) { m["epoch"] = 2.5 },
		"epoch negative":       func(m map[string]any) { m["epoch"] = -2 },
		"fsid missing":         func(m map[string]any) { delete(m, "fsid") },
		"fsid null":            func(m map[string]any) { m["fsid"] = nil },
		"fsid empty":           func(m map[string]any) { m["fsid"] = "" },
		"fsid number":          func(m map[string]any) { m["fsid"] = 7 },
		"fsid uppercase":       func(m map[string]any) { m["fsid"] = strings.ToUpper(m["fsid"].(string)) },
		"mons missing":         func(m map[string]any) { delete(m, "mons") },
		"mons null":            func(m map[string]any) { m["mons"] = nil },
		"mons empty":           func(m map[string]any) { m["mons"] = []any{} },
		"mons object":          func(m map[string]any) { m["mons"] = map[string]any{} },
		"mon name missing":     func(m map[string]any) { delete(mon(m, 1), "name") },
		"mon name null":        func(m map[string]any) { mon(m, 1)["name"] = nil },
		"mon name empty":       func(m map[string]any) { mon(m, 1)["name"] = "" },
		"mon name number":      func(m map[string]any) { mon(m, 1)["name"] = 1 },
		"mon name duplicate":   func(m map[string]any) { mon(m, 1)["name"] = "a" },
		"rank missing":         func(m map[string]any) { delete(mon(m, 1), "rank") },
		"rank null":            func(m map[string]any) { mon(m, 1)["rank"] = nil },
		"rank out of order":    func(m map[string]any) { mon(m, 1)["rank"] = 3 },
		"public_addrs missing": func(m map[string]any) { delete(mon(m, 1), "public_addrs") },
		"public_addrs null":    func(m map[string]any) { mon(m, 1)["public_addrs"] = nil },
		"addrvec missing":      func(m map[string]any) { delete(mon(m, 1)["public_addrs"].(map[string]any), "addrvec") },
		"addrvec null":         func(m map[string]any) { mon(m, 1)["public_addrs"].(map[string]any)["addrvec"] = nil },
		"addrvec empty":        func(m map[string]any) { mon(m, 1)["public_addrs"].(map[string]any)["addrvec"] = []any{} },
		"addrvec string":       func(m map[string]any) { mon(m, 1)["public_addrs"].(map[string]any)["addrvec"] = "x" },
		"addr type missing":    func(m map[string]any) { delete(addr(m), "type") },
		"addr type null":       func(m map[string]any) { addr(m)["type"] = nil },
		"addr type any":        func(m map[string]any) { addr(m)["type"] = "any" },
		"addr type number":     func(m map[string]any) { addr(m)["type"] = 2 },
		"addr missing":         func(m map[string]any) { delete(addr(m), "addr") },
		"addr null":            func(m map[string]any) { addr(m)["addr"] = nil },
		"addr empty":           func(m map[string]any) { addr(m)["addr"] = "" },
		"addr without port":    func(m map[string]any) { addr(m)["addr"] = "172.20.0.4" },
		"addr port zero":       func(m map[string]any) { addr(m)["addr"] = "172.20.0.4:0" },
		"addr unspecified":     func(m map[string]any) { addr(m)["addr"] = "0.0.0.0:3300" },
		"addr hostname":        func(m map[string]any) { addr(m)["addr"] = "mon-b:3300" },
		"nonce missing":        func(m map[string]any) { delete(addr(m), "nonce") },
		"nonce null":           func(m map[string]any) { addr(m)["nonce"] = nil },
		"nonce string":         func(m map[string]any) { addr(m)["nonce"] = "0" },
		"nonce beyond 32 bits": func(m map[string]any) { addr(m)["nonce"] = uint64(1) << 32 },
		"addr duplicated": func(m map[string]any) {
			vec := mon(m, 1)["public_addrs"].(map[string]any)
			vec["addrvec"] = append(vec["addrvec"].([]any), addr(m))
		},
	}
}

func TestMonStoreMonmapRefusesAMalformedMonmapInEitherDocument(t *testing.T) {
	dump, status := monStoreBothInQuorum(t)
	if _, err := decodeMonStoreMember(dump, status, "b"); err != nil {
		t.Fatalf("control: %v", err)
	}
	for name, mutate := range monStoreMapMutations() {
		t.Run("mon dump "+name, func(t *testing.T) {
			doc := monStoreDecode(t, dump)
			mutate(doc)
			if member, err := decodeMonStoreMember(monStoreEncode(t, doc), status, "b"); err == nil || member != nil {
				t.Fatalf("accepted %+v", member)
			}
			// The parser alone refuses it too: the cross-check between the
			// documents is not what catches it.
			if _, _, err := parseMonStoreMonDump(monStoreEncode(t, doc)); err == nil {
				t.Fatal("the mon dump parser accepted it")
			}
		})
		t.Run("quorum_status "+name, func(t *testing.T) {
			doc := monStoreDecode(t, status)
			mutate(doc["monmap"].(map[string]any))
			if member, err := decodeMonStoreMember(dump, monStoreEncode(t, doc), "b"); err == nil || member != nil {
				t.Fatalf("accepted %+v", member)
			}
			if _, _, err := parseMonStoreQuorumStatus(monStoreEncode(t, doc)); err == nil {
				t.Fatal("the quorum_status parser accepted it")
			}
		})
	}
}

func TestMonStoreMonmapRefusesAmbiguousQuorum(t *testing.T) {
	dump, status := monStoreBothInQuorum(t)
	dumpMut := map[string]func(map[string]any){
		"quorum missing":        func(m map[string]any) { delete(m, "quorum") },
		"quorum null":           func(m map[string]any) { m["quorum"] = nil },
		"quorum empty":          func(m map[string]any) { m["quorum"] = []any{} },
		"quorum object":         func(m map[string]any) { m["quorum"] = map[string]any{} },
		"quorum element null":   func(m map[string]any) { m["quorum"] = []any{0, nil} },
		"quorum element string": func(m map[string]any) { m["quorum"] = []any{"0", "1"} },
		"quorum element float":  func(m map[string]any) { m["quorum"] = []any{0, 1.5} },
		"quorum element -1":     func(m map[string]any) { m["quorum"] = []any{-1} },
		"quorum rank unknown":   func(m map[string]any) { m["quorum"] = []any{0, 5} },
		"quorum rank repeated":  func(m map[string]any) { m["quorum"] = []any{0, 0} },
	}
	for name, mutate := range dumpMut {
		t.Run("mon dump "+name, func(t *testing.T) {
			doc := monStoreDecode(t, dump)
			mutate(doc)
			if member, err := decodeMonStoreMember(monStoreEncode(t, doc), status, "b"); err == nil || member != nil {
				t.Fatalf("accepted %+v", member)
			}
			if _, _, err := parseMonStoreMonDump(monStoreEncode(t, doc)); err == nil {
				t.Fatal("the mon dump parser accepted it")
			}
		})
	}
	statusMut := map[string]func(map[string]any){
		"election_epoch missing":  func(m map[string]any) { delete(m, "election_epoch") },
		"election_epoch null":     func(m map[string]any) { m["election_epoch"] = nil },
		"quorum missing":          func(m map[string]any) { delete(m, "quorum") },
		"quorum null":             func(m map[string]any) { m["quorum"] = nil },
		"quorum empty":            func(m map[string]any) { m["quorum"] = []any{} },
		"quorum_names missing":    func(m map[string]any) { delete(m, "quorum_names") },
		"quorum_names null":       func(m map[string]any) { m["quorum_names"] = nil },
		"quorum_names empty":      func(m map[string]any) { m["quorum_names"] = []any{} },
		"quorum_names null item":  func(m map[string]any) { m["quorum_names"] = []any{"a", nil} },
		"quorum_names empty item": func(m map[string]any) { m["quorum_names"] = []any{"a", ""} },
		"quorum_names unknown":    func(m map[string]any) { m["quorum_names"] = []any{"a", "z"} },
		"quorum_names subset":     func(m map[string]any) { m["quorum_names"] = []any{"a"} },
		"leader missing":          func(m map[string]any) { delete(m, "quorum_leader_name") },
		"leader null":             func(m map[string]any) { m["quorum_leader_name"] = nil },
		"leader empty":            func(m map[string]any) { m["quorum_leader_name"] = "" },
		"leader outside quorum":   func(m map[string]any) { m["quorum_leader_name"] = "z" },
		"quorum_age missing":      func(m map[string]any) { delete(m, "quorum_age") },
		"quorum_age null":         func(m map[string]any) { m["quorum_age"] = nil },
		"monmap missing":          func(m map[string]any) { delete(m, "monmap") },
		"monmap null":             func(m map[string]any) { m["monmap"] = nil },
		"monmap string":           func(m map[string]any) { m["monmap"] = "x" },
	}
	for name, mutate := range statusMut {
		t.Run("quorum_status "+name, func(t *testing.T) {
			doc := monStoreDecode(t, status)
			mutate(doc)
			if member, err := decodeMonStoreMember(dump, monStoreEncode(t, doc), "b"); err == nil || member != nil {
				t.Fatalf("accepted %+v", member)
			}
			if _, _, err := parseMonStoreQuorumStatus(monStoreEncode(t, doc)); err == nil {
				t.Fatal("the quorum_status parser accepted it")
			}
		})
	}
	// Paired controls: the unmutated documents pass each parser alone.
	if _, _, err := parseMonStoreMonDump(dump); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseMonStoreQuorumStatus(status); err != nil {
		t.Fatal(err)
	}
}

func TestMonStoreMonmapRefusesDocumentsThatDisagree(t *testing.T) {
	dump, status := monStoreBothInQuorum(t)
	cases := map[string]func(dump, status map[string]any){
		"epoch": func(d, s map[string]any) { s["monmap"].(map[string]any)["epoch"] = 3 },
		"fsid": func(d, s map[string]any) {
			s["monmap"].(map[string]any)["fsid"] = "00000000-0000-4000-8000-000000000000"
		},
		"member name": func(d, s map[string]any) {
			s["monmap"].(map[string]any)["mons"].([]any)[1].(map[string]any)["name"] = "c"
		},
		"member count": func(d, s map[string]any) {
			m := s["monmap"].(map[string]any)
			m["mons"] = m["mons"].([]any)[:1]
		},
		"address": func(d, s map[string]any) {
			s["monmap"].(map[string]any)["mons"].([]any)[1].(map[string]any)["public_addrs"].(map[string]any)["addrvec"].([]any)[0].(map[string]any)["addr"] = "172.20.0.9:3300"
		},
		"quorum": func(d, s map[string]any) {
			s["quorum"], s["quorum_names"], s["quorum_leader_name"] = []any{0}, []any{"a"}, "a"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d, s := monStoreDecode(t, dump), monStoreDecode(t, status)
			mutate(d, s)
			if member, err := decodeMonStoreMember(monStoreEncode(t, d), monStoreEncode(t, s), "b"); err == nil || member != nil {
				t.Fatalf("accepted %+v", member)
			}
		})
	}
}

func TestMonStoreMonmapRefusesMalformedDocuments(t *testing.T) {
	dump, status := monStoreBothInQuorum(t)
	dupKey := strings.Replace(string(dump), `"epoch":2,`, `"epoch":2,"epoch":2,`, 1)
	dupNested := strings.Replace(string(dump), `"nonce":0}`, `"nonce":0,"nonce":0}`, 1)
	for name, bad := range map[string][]byte{
		"empty": nil, "blank": []byte(" \n"), "not json": []byte("dumped monmap epoch 2"), "array": []byte("[]"), "null": []byte("null"),
		"trailing data": append(slices.Clone(dump), []byte(" {}")...), "two documents": append(slices.Clone(dump), dump...),
		"duplicate key": []byte(dupKey), "nested duplicate key": []byte(dupNested), "truncated": dump[:len(dump)/2],
		"over the limit": append([]byte(" "), append(slices.Clone(dump), make([]byte, monStoreMonmapOutputLimit)...)...),
	} {
		t.Run("mon dump "+name, func(t *testing.T) {
			if m, err := decodeMonStoreMember(bad, status, "b"); err == nil || m != nil {
				t.Fatal("accepted")
			}
		})
		t.Run("quorum_status "+name, func(t *testing.T) {
			if m, err := decodeMonStoreMember(dump, bad, "b"); err == nil || m != nil {
				t.Fatal("accepted")
			}
		})
	}
	if !strings.Contains(dupKey, `"epoch":2,"epoch":2`) || !strings.Contains(dupNested, `"nonce":0,"nonce":0`) {
		t.Fatal("the duplicate-key fixtures were not built")
	}
}

type monStoreCLIFake struct {
	calls [][]string
	out   map[string][]byte
	err   map[string]error
}

func (f *monStoreCLIFake) run(_ context.Context, argv ...string) ([]byte, error) {
	f.calls = append(f.calls, slices.Clone(argv))
	key := argv[len(argv)-3]
	if f.err[key] != nil {
		return nil, f.err[key]
	}
	return f.out[key], nil
}

func monStoreCLIFixture(t *testing.T) *monStoreCLIFake {
	dump, status := monStoreBothInQuorum(t)
	return &monStoreCLIFake{out: map[string][]byte{"dump": dump, "quorum_status": status}, err: map[string]error{}}
}

func TestMonStoreMonmapCLIAsksForJSONOneCommandAtATime(t *testing.T) {
	f := monStoreCLIFixture(t)
	member, err := monStoreMonmapCLI{run: f.run}.Member(t.Context(), "a")
	if err != nil || member == nil || member.quorum != monStoreQuorumIn {
		t.Fatalf("%+v %v", member, err)
	}
	want := [][]string{
		{"ceph", "--connect-timeout", "5", "mon", "dump", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"},
	}
	if len(f.calls) != 2 || !slices.Equal(f.calls[0], want[0]) || !slices.Equal(f.calls[1], want[1]) {
		t.Fatalf("calls %v", f.calls)
	}
}

func TestMonStoreMonmapCLIPropagatesFailuresWithoutGuessing(t *testing.T) {
	boom := errors.New("exec failed")
	f := monStoreCLIFixture(t)
	f.err["dump"] = boom
	if m, err := (monStoreMonmapCLI{run: f.run}).Member(t.Context(), "a"); m != nil || !errors.Is(err, boom) || len(f.calls) != 1 {
		t.Fatalf("mon dump failure: %+v %v calls %d", m, err, len(f.calls))
	}
	f = monStoreCLIFixture(t)
	f.err["quorum_status"] = boom
	if m, err := (monStoreMonmapCLI{run: f.run}).Member(t.Context(), "a"); m != nil || !errors.Is(err, boom) || len(f.calls) != 2 {
		t.Fatalf("quorum_status failure: %+v %v", m, err)
	}
	f = monStoreCLIFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if m, err := (monStoreMonmapCLI{run: f.run}).Member(ctx, "a"); m != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %+v %v", m, err)
	}
	if m, err := (monStoreMonmapCLI{}).Member(t.Context(), "a"); m != nil || err == nil {
		t.Fatalf("no runner: %+v %v", m, err)
	}
	f = monStoreCLIFixture(t)
	f.out["dump"] = nil
	if m, err := (monStoreMonmapCLI{run: f.run}).Member(t.Context(), "a"); m != nil || err == nil {
		t.Fatalf("empty output: %+v %v", m, err)
	}
}

func TestMonStoreMonmapCLIFeedsObserveStopped(t *testing.T) {
	// Only an out-of-quorum member passes observeStopped; the decoder's quorum
	// fact reaches it unchanged.
	dump := monStoreDecode(t, monStoreMonDumpFixture(t))
	dump["quorum"] = []int{0}
	f := &monStoreCLIFake{out: map[string][]byte{
		"dump": monStoreEncode(t, dump), "quorum_status": monStoreStatusFixture(t, []int{0}, []string{"a"}, "a"),
	}, err: map[string]error{}}
	files := monStoreFilesFixture()
	ins := &monStoreInspectorFake{files: files, calls: &[]string{}}
	id, err := observeStopped(t.Context(), "b", "vol", ins, monStoreMonmapCLI{run: f.run})
	if err != nil || id.fsid != "dfca4a09-05f4-4ef4-883f-9dd10c5fcdf7" || len(id.addresses) != 2 {
		t.Fatalf("%+v %v", id, err)
	}
	if _, err := observeStopped(t.Context(), "a", "vol", ins, monStoreMonmapCLI{run: f.run}); err == nil || !strings.Contains(err.Error(), "in quorum") {
		t.Fatalf("a member in quorum: %v", err)
	}
}
