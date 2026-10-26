//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// These checks run the attachment input producers on synthetic documents only.
// No OSD, keeper, tmpfs, Ceph daemon or Docker is involved. The osd dump field
// names follow the pinned OSDMap::dump; the container inspection and ledger
// shapes are the ones this package defines for its own scenario. Nothing here
// shows native output.

const attachFSID = "6a1f8a52-0000-4000-8000-000000000001"

func attachDump(fsid, near, back, full string) []byte {
	return []byte(fmt.Sprintf(`{"epoch":12,"fsid":%q,"full_ratio":%s,"backfillfull_ratio":%s,"nearfull_ratio":%s,"pools":[]}`, fsid, full, back, near))
}

func attachInspect(status string, running bool, exit int, oom bool, started string) []byte {
	return []byte(fmt.Sprintf(`{"Id":"x","State":{"Status":%q,"Running":%t,"Paused":false,"Restarting":false,"OOMKilled":%t,"ExitCode":%d,"StartedAt":%q,"FinishedAt":"0001-01-01T00:00:00Z"}}`,
		status, running, oom, exit, started))
}

const (
	attachT1 = "2026-10-26T01:02:03.123456789Z"
	attachT2 = "2026-10-26T01:09:09.000000001Z"
)

var attachDigests = map[string]string{
	"a": strings.Repeat("a", 64), "b": strings.Repeat("b", 64), "c": strings.Repeat("c", 64),
}

func goodAttachRaw(t *testing.T) *attachRawInputs {
	t.Helper()
	base := goodAttachRun()
	mj := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	block := backingEntry(true, 50, 7, 64<<20, 0, "0:50", "/var/lib/ceph/osd", "tmpfs")
	scratch := backingEntry(false, 50, 2, 0, 0, "0:50", "/var/lib/ceph/osd", "tmpfs")
	policy := attachDump(attachFSID, "0.85", "0.9", "0.95")
	ledger := mj([]map[string]string{
		{"key": "a", "outcome": mutAcked, "sha256": attachDigests["a"]},
		{"key": "b", "outcome": mutUnknown, "sha256": attachDigests["b"]},
		{"key": "c", "outcome": mutFailed, "sha256": attachDigests["c"]},
	})
	readback := mj([]map[string]string{
		{"key": "a", "status": readbackPresent, "sha256": attachDigests["a"]},
		{"key": "b", "status": readbackAbsent},
		{"key": "c", "status": readbackAbsent},
	})
	// A real recovery check needs the release receipt of goodAttachRun; it has
	// the digest "seed".
	return &attachRawInputs{
		KeeperView: mj(goodStoreView()), OSDView: mj(goodStoreView()), Token: storeToken, CeilingBytes: storeBytes,
		BlockBacking: mj(backingReceipt(block, scratch)),
		FSID:         attachFSID, PolicyBefore: policy, PolicyAtEval: policy,
		Fill: mj(base.Fill), Release: mj(base.Release), SeedSHA: "seed", WriterJoined: true,
		KeeperAtAttach: attachInspect("running", true, 0, false, attachT1), KeeperAtEval: attachInspect("running", true, 0, false, attachT1),
		DaemonAtAttach: attachInspect("running", true, 0, false, attachT1), DaemonAtEval: attachInspect("running", true, 0, false, attachT1),
		Ledger: ledger, Readback: readback,
	}
}

func TestAttachPolicyOfRendersOrderedRatiosAndRefusesDefects(t *testing.T) {
	good := attachDump(attachFSID, "0.85", "0.9", "0.95")
	got, err := attachPolicyOf(good, attachFSID)
	if err != nil || got != "n=0.85 b=0.9 f=0.95" {
		t.Fatalf("got %q, %v", got, err)
	}
	other, _ := attachPolicyOf(attachDump(attachFSID, "0.85", "0.9", "0.96"), attachFSID)
	if other == got {
		t.Fatal("a changed ratio rendered the same policy")
	}
	bad := map[string][]byte{
		"empty":                    nil,
		"null document":            []byte("null"),
		"trailing":                 append(append([]byte{}, good...), []byte(`{}`)...),
		"wrong fsid":               attachDump("other", "0.85", "0.9", "0.95"),
		"missing full":             []byte(fmt.Sprintf(`{"fsid":%q,"backfillfull_ratio":0.9,"nearfull_ratio":0.85}`, attachFSID)),
		"null full":                attachDump(attachFSID, "0.85", "0.9", "null"),
		"string full":              attachDump(attachFSID, "0.85", "0.9", `"0.95"`),
		"wrong-case key":           []byte(strings.Replace(string(good), `"full_ratio"`, `"Full_Ratio"`, 1)),
		"missing fsid":             []byte(`{"full_ratio":0.95,"backfillfull_ratio":0.9,"nearfull_ratio":0.85}`),
		"null fsid":                []byte(`{"fsid":null,"full_ratio":0.95,"backfillfull_ratio":0.9,"nearfull_ratio":0.85}`),
		"unordered":                attachDump(attachFSID, "0.9", "0.85", "0.95"),
		"equal":                    attachDump(attachFSID, "0.9", "0.9", "0.95"),
		"zero nearfull":            attachDump(attachFSID, "0", "0.9", "0.95"),
		"above one":                attachDump(attachFSID, "0.85", "0.9", "1.01"),
		"negative":                 attachDump(attachFSID, "-0.1", "0.9", "0.95"),
		"overflow is not a number": attachDump(attachFSID, "0.85", "0.9", "1e999"),
	}
	for name, doc := range bad {
		if v, err := attachPolicyOf(doc, attachFSID); err == nil {
			t.Errorf("%s: accepted as %q", name, v)
		}
	}
	if _, err := attachPolicyOf(good, ""); err == nil {
		t.Error("empty expected fsid accepted")
	}
}

func TestAttachContainerStateParserRefusesMissingNullAndWrongTypes(t *testing.T) {
	good := string(attachInspect("running", true, 0, false, attachT1))
	if s, err := parseAttachContainerState([]byte(good)); err != nil || !s.Running || s.StartedAt != attachT1 {
		t.Fatalf("good state: %+v, %v", s, err)
	}
	for _, field := range []string{"Status", "Running", "Paused", "Restarting", "OOMKilled", "ExitCode", "StartedAt"} {
		var doc struct {
			State map[string]json.RawMessage
		}
		if err := json.Unmarshal([]byte(good), &doc); err != nil {
			t.Fatal(err)
		}
		for name, edit := range map[string]func(){
			"missing":    func() { delete(doc.State, field) },
			"null":       func() { doc.State[field] = json.RawMessage("null") },
			"wrong type": func() { doc.State[field] = json.RawMessage(`[1]`) },
		} {
			saved := doc.State[field]
			edit()
			b, _ := json.Marshal(doc)
			if _, err := parseAttachContainerState(b); err == nil {
				t.Errorf("%s %s accepted", field, name)
			}
			if saved == nil {
				delete(doc.State, field)
			} else {
				doc.State[field] = saved
			}
		}
	}
	for name, doc := range map[string]string{
		"no State":         `{"Id":"x"}`,
		"null State":       `{"State":null}`,
		"zero start":       string(attachInspect("created", false, 0, false, "0001-01-01T00:00:00Z")),
		"empty start":      string(attachInspect("running", true, 0, false, "")),
		"unparsable start": string(attachInspect("running", true, 0, false, "yesterday")),
		"empty":            "",
	} {
		if _, err := parseAttachContainerState([]byte(doc)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func mustState(t *testing.T, doc []byte) *attachContainerState {
	t.Helper()
	s, err := parseAttachContainerState(doc)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAttachKeeperIdentityKeepsRestartAndLivenessVisible(t *testing.T) {
	up := mustState(t, attachInspect("running", true, 0, false, attachT1))
	got, running, err := attachKeeperIdentity(up, up)
	if err != nil || got != [2]string{attachT1, attachT1} || !running {
		t.Fatalf("same keeper: %v %v %v", got, running, err)
	}
	restarted := mustState(t, attachInspect("running", true, 0, false, attachT2))
	if got, running, err = attachKeeperIdentity(up, restarted); err != nil || got[0] == got[1] || !running {
		t.Fatalf("restart must stay visible as a differing start: %v %v %v", got, running, err)
	}
	stopped := mustState(t, attachInspect("exited", false, 0, false, attachT1))
	if _, running, err = attachKeeperIdentity(up, stopped); err != nil || running {
		t.Fatalf("stopped keeper reported running: %v %v", running, err)
	}
	if _, _, err = attachKeeperIdentity(stopped, up); err == nil {
		t.Fatal("keeper not running at attachment accepted")
	}
	if _, _, err = attachKeeperIdentity(nil, up); err == nil {
		t.Fatal("nil attachment state accepted")
	}
	if _, _, err = attachKeeperIdentity(up, nil); err == nil {
		t.Fatal("nil evaluation state accepted")
	}
}

func TestAttachDaemonStateSeparatesAbortFromOtherExits(t *testing.T) {
	attached := mustState(t, attachInspect("running", true, 0, false, attachT1))
	for _, c := range []struct {
		name string
		now  []byte
		want string
	}{
		{"same instance running", attachInspect("running", true, 0, false, attachT1), daemonUp},
		{"aborted", attachInspect("exited", false, 134, false, attachT1), daemonAborted},
		{"sigkill", attachInspect("exited", false, 137, false, attachT1), daemonUnknown},
		{"oom killed", attachInspect("exited", false, 137, true, attachT1), daemonUnknown},
		{"oom flag on abort status", attachInspect("exited", false, 134, true, attachT1), daemonUnknown},
		{"running but flagged OOM-killed", attachInspect("running", true, 0, true, attachT1), daemonUnknown},
		{"clean exit", attachInspect("exited", false, 0, false, attachT1), daemonUnknown},
		{"other exit code", attachInspect("exited", false, 1, false, attachT1), daemonUnknown},
		{"restarted instance is not the attached daemon", attachInspect("running", true, 0, false, attachT2), daemonUnknown},
		{"abort status on another instance", attachInspect("exited", false, 134, false, attachT2), daemonUnknown},
		{"not exited yet", attachInspect("dead", false, 134, false, attachT1), daemonUnknown},
	} {
		got, err := attachDaemonState(attached, mustState(t, c.now))
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
	paused := mustState(t, attachInspect("running", true, 0, false, attachT1))
	paused.Paused = true
	if got, _ := attachDaemonState(attached, paused); got != daemonUnknown {
		t.Errorf("paused daemon is %q", got)
	}
	if _, err := attachDaemonState(mustState(t, attachInspect("exited", false, 134, false, attachT1)), attached); err == nil {
		t.Error("daemon not running at attachment accepted")
	}
	if _, err := attachDaemonState(nil, attached); err == nil {
		t.Error("nil attachment accepted")
	}
	if _, err := attachDaemonState(attached, nil); err == nil {
		t.Error("nil evaluation accepted")
	}
}

func TestAttachLedgerParserRefusesDefects(t *testing.T) {
	ok := func(k, o, s string) string {
		return fmt.Sprintf(`{"key":%q,"outcome":%q,"sha256":%q}`, k, o, s)
	}
	a := attachDigests["a"]
	if l, err := parseAttachLedger([]byte("[" + ok("a", mutAcked, a) + "," + ok("b", mutUnknown, a) + "]")); err != nil || len(l) != 2 || l[1].Outcome != mutUnknown {
		t.Fatalf("good ledger: %v %v", l, err)
	}
	for name, doc := range map[string]string{
		"empty output":        "",
		"null":                "null",
		"empty array":         "[]",
		"not an array":        "{}",
		"two lines":           "[]\n[]",
		"missing key":         `[{"outcome":"acked","sha256":"` + a + `"}]`,
		"null key":            `[{"key":null,"outcome":"acked","sha256":"` + a + `"}]`,
		"empty key":           "[" + ok("", mutAcked, a) + "]",
		"missing outcome":     `[{"key":"a","sha256":"` + a + `"}]`,
		"unknown outcome":     "[" + ok("a", "committed", a) + "]",
		"missing digest":      `[{"key":"a","outcome":"acked"}]`,
		"null digest":         `[{"key":"a","outcome":"acked","sha256":null}]`,
		"short digest":        "[" + ok("a", mutAcked, "abc") + "]",
		"uppercase digest":    "[" + ok("a", mutAcked, strings.ToUpper(a)) + "]",
		"wrong type digest":   `[{"key":"a","outcome":"acked","sha256":5}]`,
		"duplicate key":       "[" + ok("a", mutAcked, a) + "," + ok("a", mutUnknown, a) + "]",
		"unexpected field":    `[{"key":"a","outcome":"acked","sha256":"` + a + `","extra":1}]`,
		"unknown without sha": `[{"key":"a","outcome":"unknown"}]`,
	} {
		if l, err := parseAttachLedger([]byte(doc)); err == nil {
			t.Errorf("%s accepted: %v", name, l)
		}
	}
}

func TestAttachRecoveredJoinsReadbackToLedgerWithoutInventingEvidence(t *testing.T) {
	ledger := []attachMutation{
		{Key: "a", Outcome: mutAcked, SHA256: attachDigests["a"]},
		{Key: "b", Outcome: mutUnknown, SHA256: attachDigests["b"]},
	}
	rec := func(k, s, d string) string {
		if d == "" {
			return fmt.Sprintf(`{"key":%q,"status":%q}`, k, s)
		}
		return fmt.Sprintf(`{"key":%q,"status":%q,"sha256":%q}`, k, s, d)
	}
	m, err := attachRecoveredOf(ledger, []byte("["+rec("a", readbackPresent, attachDigests["a"])+","+rec("b", readbackAbsent, "")+"]"))
	if err != nil || len(m) != 1 || m["a"] != attachDigests["a"] {
		t.Fatalf("good readback: %v %v", m, err)
	}
	// A missing object is simply not in the map, so the judge reports it lost.
	m, err = attachRecoveredOf(ledger, []byte("["+rec("a", readbackAbsent, "")+"]"))
	if err != nil || len(m) != 0 {
		t.Fatalf("absent acknowledged key: %v %v", m, err)
	}
	// A read error on an unknown mutation is tolerated; on an acknowledged one it is not.
	if _, err = attachRecoveredOf(ledger, []byte("["+rec("a", readbackPresent, attachDigests["a"])+","+rec("b", readbackError, "")+"]")); err != nil {
		t.Fatalf("unknown mutation read error: %v", err)
	}
	for name, doc := range map[string]string{
		"empty":                      "",
		"null":                       "null",
		"acknowledged key not read":  "[" + rec("b", readbackAbsent, "") + "]",
		"empty readback":             "[]",
		"read error on acknowledged": "[" + rec("a", readbackError, "") + "]",
		"key outside ledger":         "[" + rec("a", readbackAbsent, "") + "," + rec("z", readbackAbsent, "") + "]",
		"duplicate record":           "[" + rec("a", readbackPresent, attachDigests["a"]) + "," + rec("a", readbackAbsent, "") + "]",
		"present without digest":     "[" + rec("a", readbackPresent, "") + "]",
		"present with bad digest":    "[" + rec("a", readbackPresent, "zz") + "]",
		"present with null digest":   `[{"key":"a","status":"present","sha256":null}]`,
		"absent with digest":         "[" + rec("a", readbackAbsent, attachDigests["a"]) + "]",
		"unknown status":             "[" + rec("a", "found", attachDigests["a"]) + "]",
		"missing status":             `[{"key":"a"}]`,
		"null key":                   `[{"key":null,"status":"absent"}]`,
		"unexpected field":           `[{"key":"a","status":"absent","extra":1}]`,
	} {
		if m, err := attachRecoveredOf(ledger, []byte(doc)); err == nil {
			t.Errorf("%s accepted: %v", name, m)
		}
	}
}

func TestAttachBuildRunFeedsTheJudge(t *testing.T) {
	judge := func(in *attachRawInputs) (string, error) {
		run, err := buildAttachRun(in)
		if err != nil {
			return "", err
		}
		return judgeScratchAttachment(run)
	}
	if got, err := judge(goodAttachRaw(t)); err != nil || got != attachRecovered {
		t.Fatalf("good run: %q, %v", got, err)
	}
	for _, c := range []struct {
		name   string
		mutate func(*attachRawInputs)
		want   string
	}{
		{"daemon aborted", func(r *attachRawInputs) {
			r.DaemonAtEval = attachInspect("exited", false, 134, false, attachT1)
			r.Readback = nil
		}, attachDaemonAborted},
		{"daemon killed", func(r *attachRawInputs) {
			r.DaemonAtEval = attachInspect("exited", false, 137, true, attachT1)
			r.Readback = nil
		}, attachDaemonIndefinite},
		{"readback ignored when the daemon is not up", func(r *attachRawInputs) {
			r.DaemonAtEval = attachInspect("exited", false, 134, false, attachT1)
			r.Readback = []byte("garbage")
		}, attachDaemonAborted},
	} {
		in := goodAttachRaw(t)
		c.mutate(in)
		if got, err := judge(in); err != nil || got != c.want {
			t.Errorf("%s: %q, %v; want %q", c.name, got, err, c.want)
		}
	}
	for name, mutate := range map[string]func(*attachRawInputs){
		"policy changed":       func(r *attachRawInputs) { r.PolicyAtEval = attachDump(attachFSID, "0.85", "0.9", "0.96") },
		"policy of other fsid": func(r *attachRawInputs) { r.PolicyAtEval = attachDump("other", "0.85", "0.9", "0.95") },
		"keeper restarted":     func(r *attachRawInputs) { r.KeeperAtEval = attachInspect("running", true, 0, false, attachT2) },
		"keeper stopped":       func(r *attachRawInputs) { r.KeeperAtEval = attachInspect("exited", false, 0, false, attachT1) },
		"keeper identity null": func(r *attachRawInputs) { r.KeeperAtEval = []byte(`{"State":null}`) },
		"acknowledged lost":    func(r *attachRawInputs) { r.Readback = []byte(`[{"key":"a","status":"absent"}]`) },
		"acknowledged changed": func(r *attachRawInputs) {
			r.Readback = []byte(`[{"key":"a","status":"present","sha256":"` + attachDigests["b"] + `"}]`)
		},
		"acknowledged unread":   func(r *attachRawInputs) { r.Readback = []byte(`[{"key":"b","status":"absent"}]`) },
		"readback missing":      func(r *attachRawInputs) { r.Readback = nil },
		"ledger empty":          func(r *attachRawInputs) { r.Ledger = []byte("[]") },
		"writer not joined":     func(r *attachRawInputs) { r.WriterJoined = false },
		"seed digest differs":   func(r *attachRawInputs) { r.SeedSHA = "other" },
		"fill receipt null":     func(r *attachRawInputs) { r.Fill = []byte("null") },
		"release receipt empty": func(r *attachRawInputs) { r.Release = nil },
		"backing receipt empty": func(r *attachRawInputs) { r.BlockBacking = nil },
		"view receipt empty":    func(r *attachRawInputs) { r.OSDView = nil },
		"daemon identity empty": func(r *attachRawInputs) { r.DaemonAtAttach = nil },
	} {
		in := goodAttachRaw(t)
		mutate(in)
		if got, err := judge(in); err == nil {
			t.Errorf("%s: judged %q instead of failing", name, got)
		}
	}
	if _, err := buildAttachRun(nil); err == nil {
		t.Error("nil inputs accepted")
	}
}
