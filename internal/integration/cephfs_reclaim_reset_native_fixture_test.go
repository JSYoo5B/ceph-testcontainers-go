//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// These controls run the native RESET recipe's embedded Python against a fake
// cephfs module and a fake ctypes library, and parse its output with the same
// readers as the native test. They check the scripts' declarations, receipts
// and output shapes against the Docker-free judge's contract. They do not load
// libcephfs, reach a cluster or reproduce RESET; the native effect is claimed
// only from the pull request's own CI run.

const reclaimFakeCephfs = `
import errno as _errno
class Error(Exception):
    def __init__(self, number):
        self.errno = number
class LibCephFS:
    def __init__(self, conffile=None): self.locks = {}
    def conf_set(self, name, value): pass
    def mount(self, filesystem_name=None): pass
    def unmount(self): pass
    def shutdown(self): pass
    def get_instance_id(self): return 7001
    def open(self, path, flags, mode): return 3 if path.endswith(b"a") else 4
    def close(self, fd): pass
    def flock(self, fd, operation, owner):
        import os
        number = int(os.environ["FAKE_A_ERRNO" if fd == 3 else "FAKE_U_ERRNO"])
        if number and operation != 8:
            raise Error(number)
`

const reclaimFakeLib = `
import builtins, ctypes, io, os, sys
here = os.path.dirname(os.path.abspath(__file__))
_state = {}
class _Fn:
    def __init__(self, name): self.name, self.restype, self.argtypes = name, None, None
    def __call__(self, *args):
        name = self.name
        if name == "ceph_conf_set" and args[1] == b"log_file":
            _state["log"] = args[2].decode()
            real_open(_state["log"], "w").close()
        if name == "ceph_create":
            args[0]._obj.value = 0x1000 + os.getpid()
            return int(os.environ.get("FAKE_CREATE_RC", "0"))
        if name == "ceph_version":
            for pointer, value in zip(args, (20, 2, 4)):
                pointer._obj.value = value
            return b"ceph version 20.2.4"
        if name == "ceph_get_instance_id":
            return int(os.environ.get("FAKE_INSTANCE", "0"))
        if name == "ceph_start_reclaim":
            if "log" in _state:
                with real_open(_state["log"], "a") as log:
                    for result in filter(None, os.environ.get("FAKE_REPLIES", "").split(",")):
                        log.write("2026-01-01T00:00:00.000+0000 7f 10 client.7001 handle_client_reclaim_reply client_reclaim_reply(%s e 0) from mds.0\n" % result)
            return int(os.environ.get("FAKE_RC", "-2"))
        if name == "ceph_read":
            data = b"reclaim-acknowledged-data\x00\xff"
            ctypes.memmove(args[2], data, len(data))
            return len(data)
        return 0
class _Lib:
    def __getattr__(self, name):
        if name in os.environ.get("FAKE_MISSING", "").split(","):
            raise AttributeError(name)
        return _Fn(name)
ctypes.CDLL = lambda path: _Lib()
real_open = builtins.open
def fake_open(path, *args, **kwargs):
    if path == "/proc/self/maps":
        return io.StringIO("7f00-7f01 r-xp 00000000 00:00 0 " + os.path.join(here, "libcephfs.so.2") + "\n")
    return real_open(path, *args, **kwargs)
builtins.open = fake_open
`

const reclaimFakeRunner = `import sys; sys.path.insert(0, sys.argv.pop(1)); import fakelib; exec(sys.argv.pop(1))`

type reclaimFake struct {
	t   *testing.T
	dir string
}

func newReclaimFake(t *testing.T) *reclaimFake {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"cephfs.py": reclaimFakeCephfs, "fakelib.py": reclaimFakeLib, "libcephfs.so.2": "fake"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &reclaimFake{t: t, dir: dir}
}

// run executes script like python3 -c, with the fakes on the path, and returns
// the combined output and the exit code.
func (f *reclaimFake) run(script string, env []string, args ...string) (string, int) {
	f.t.Helper()
	argv := append([]string{"-I", "-c", reclaimFakeRunner, f.dir, script}, args...)
	cmd := exec.Command("python3", argv...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return string(out), exit.ExitCode()
		}
		f.t.Fatal(err)
	}
	return string(out), 0
}

const reclaimFakeImage = "sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

func TestReclaimNativeABIProbeScript(t *testing.T) {
	f := newReclaimFake(t)
	probe := func(script string, env ...string) (harness.ResetABIProbe, error) {
		out, code := f.run(script, env, reclaimFakeImage)
		if code != 0 {
			t.Fatalf("probe exited %d: %s", code, out)
		}
		doc, err := harness.ReclaimJSONLine(out)
		if err != nil {
			t.Fatal(err)
		}
		return harness.ParseResetABIProbe(doc)
	}
	p, err := probe(harness.ReclaimABIProbe)
	if err != nil {
		t.Fatalf("good probe unreadable: %v", err)
	}
	if err := harness.CheckResetABIProbe(p, 20, 2, 4); err != nil {
		t.Fatalf("good probe rejected: %v", err)
	}
	if p.ImageConfigID != reclaimFakeImage || len(p.Calls) != len(harness.ResetABIContract) {
		t.Fatalf("probe: %+v", p)
	}
	if err := harness.CheckResetABIProbe(p, 19, 2, 5); err == nil {
		t.Fatal("a probe of release 20.2.4 passed as 19.2.5")
	}

	t.Run("an unresolved symbol is a blocker", func(t *testing.T) {
		if p, err := probe(harness.ReclaimABIProbe, "FAKE_MISSING=ceph_start_reclaim"); err == nil {
			t.Fatalf("accepted: %+v", p)
		}
	})
	t.Run("an initialized probe handle is not eligible", func(t *testing.T) {
		p, err := probe(harness.ReclaimABIProbe, "FAKE_INSTANCE=9")
		if err != nil || harness.CheckResetABIProbe(p, 20, 2, 4) == nil {
			t.Fatalf("%+v %v", p, err)
		}
	})
	t.Run("a failed create is not eligible", func(t *testing.T) {
		p, err := probe(harness.ReclaimABIProbe, "FAKE_CREATE_RC=-12")
		if err != nil || harness.CheckResetABIProbe(p, 20, 2, 4) == nil {
			t.Fatalf("%+v %v", p, err)
		}
	})
	t.Run("a wrongly declared void function is caught", func(t *testing.T) {
		broken := strings.Replace(harness.ReclaimABIProbe, `"ceph_finish_reclaim": ("None"`, `"ceph_finish_reclaim": ("c_int"`, 1)
		if broken == harness.ReclaimABIProbe {
			t.Fatal("control did not change the script")
		}
		p, err := probe(broken)
		if err != nil || harness.CheckResetABIProbe(p, 20, 2, 4) == nil {
			t.Fatalf("declaring ceph_finish_reclaim as int passed: %+v %v", p, err)
		}
	})
	t.Run("signed flags are caught", func(t *testing.T) {
		broken := strings.Replace(harness.ReclaimABIProbe, `["c_void_p", "c_char_p", "c_uint"]`, `["c_void_p", "c_char_p", "c_int"]`, 1)
		p, err := probe(broken)
		if broken == harness.ReclaimABIProbe || err != nil || harness.CheckResetABIProbe(p, 20, 2, 4) == nil {
			t.Fatalf("signed reclaim flags passed: %+v %v", p, err)
		}
	})
}

func TestReclaimNativeAttemptScript(t *testing.T) {
	f := newReclaimFake(t)
	log := filepath.Join(f.dir, "attempt.log")
	run := func(flags string, env ...string) harness.ReclaimAttemptResult {
		out, code := f.run(harness.ReclaimAttempt, env, "fs0", "target-uuid", "own-uuid", flags, log)
		if code != 0 {
			t.Fatalf("exit %d: %s", code, out)
		}
		r, err := harness.ParseReclaimAttempt(out)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := run("1", "FAKE_REPLIES=0")
	if r.RC == nil || *r.RC != -2 || r.ReleaseRC == nil || *r.ReleaseRC != 0 || r.Flags != harness.CephReclaimReset ||
		r.PriorReclaimCalls != 0 || !r.Initialized || r.Target != "target-uuid" || r.OwnUUID != "own-uuid" || !strings.Contains(r.Handle, ":") ||
		len(r.MDSReplies) != 1 || r.MDSReplies[0] != 0 {
		t.Fatalf("result: %+v", r)
	}
	if got := run("1", "FAKE_RC=0"); got.RC == nil || *got.RC != 0 || got.MDSReplies == nil || len(got.MDSReplies) != 0 {
		t.Fatalf("a return of 0 without a logged reply was not carried through: %+v", got)
	}
	// The state-preserving request: flags 0 and the MDS's -EINVAL reply.
	if got := run("0", "FAKE_RC=-22", "FAKE_REPLIES=-22"); got.Flags != 0 || got.RC == nil || *got.RC != -22 || len(got.MDSReplies) != 1 || got.MDSReplies[0] != -22 {
		t.Fatalf("state-preserving result: %+v", got)
	}
	if _, code := f.run(harness.ReclaimAttempt, []string{"FAKE_MISSING=ceph_start_reclaim"}, "fs0", "target-uuid", "own-uuid", "1", log); code == 0 {
		t.Fatal("a missing symbol did not stop the attempt")
	}
}

func TestReclaimNativeReaders(t *testing.T) {
	t.Run("the JSON line survives noise and the last one wins", func(t *testing.T) {
		doc, err := harness.ReclaimJSONLine("warning\nRECLAIM_JSON {\"a\":1}\nnoise\nRECLAIM_JSON {\"a\":2}\n")
		if err != nil || string(doc) != `{"a":2}` {
			t.Fatalf("%s %v", doc, err)
		}
		if _, err := harness.ReclaimJSONLine("no document\n"); err == nil {
			t.Fatal("accepted output without a document")
		}
	})
	missing := map[string]string{
		"unknown field":   `RECLAIM_JSON {"handle":"1:2","rc":-2,"extra":1}`,
		"not an object":   `RECLAIM_JSON []`,
		"rc is a string":  `RECLAIM_JSON {"handle":"1:2","rc":"-2"}`,
		"truncated":       `RECLAIM_JSON {"handle":`,
		"no document":     `nothing`,
		"flags is a bool": `RECLAIM_JSON {"flags":true}`,
	}
	for name, out := range missing {
		t.Run("attempt result rejects "+name, func(t *testing.T) {
			if r, err := harness.ParseReclaimAttempt(out); err == nil {
				t.Fatalf("accepted: %+v", r)
			}
		})
	}
	t.Run("a missing or null return value stays unrecorded", func(t *testing.T) {
		for _, out := range []string{`RECLAIM_JSON {"handle":"1:2"}`, `RECLAIM_JSON {"handle":"1:2","rc":null}`} {
			r, err := harness.ParseReclaimAttempt(out)
			if err != nil || r.RC != nil {
				t.Fatalf("%s: %+v %v", out, r, err)
			}
		}
	})
	locks := []struct {
		out  string
		a, u harness.ResetLock
		bad  bool
	}{
		{`RECLAIM_JSON {"client":1,"a_errno":11,"u_errno":11}`, harness.ResetLockHeld, harness.ResetLockHeld, false},
		{`RECLAIM_JSON {"client":1,"a_errno":0,"u_errno":11}`, harness.ResetLockFree, harness.ResetLockHeld, false},
		{`RECLAIM_JSON {"client":1,"a_errno":110,"u_errno":0}`, harness.ResetLockUnknown, harness.ResetLockFree, false},
		{`RECLAIM_JSON {"client":1,"a_errno":-1,"u_errno":11}`, harness.ResetLockUnknown, harness.ResetLockHeld, false},
		{`RECLAIM_JSON {"client":1,"u_errno":11}`, harness.ResetLockUnknown, harness.ResetLockHeld, false},
		{`RECLAIM_JSON {"client":1,"a_errno":null,"u_errno":11}`, harness.ResetLockUnknown, harness.ResetLockHeld, false},
		{`RECLAIM_JSON {"client":1,"a_errno":"11","u_errno":11}`, 0, 0, true},
		{`RECLAIM_JSON {"client":1,"a_errno":11,"u_errno":11,"x":1}`, 0, 0, true},
	}
	for _, c := range locks {
		a, u, err := harness.ParseReclaimLock(c.out)
		if c.bad != (err != nil) || (!c.bad && (a != c.a || u != c.u)) {
			t.Fatalf("%s: %d %d %v", c.out, a, u, err)
		}
	}
	// The caller path: StartCephFSSingleMDS collapses the whitespace of the
	// daemon's answer before the recipe reads it.
	normalize := func(answer string) string { return strings.Join(strings.Fields(answer), " ") }
	daemon := `{"version":"20.2.4","release":"tentacle","release_type":"stable"}`
	for name, answer := range map[string]string{
		"compact daemon answer":       daemon,
		"trailing newline":            daemon + "\n",
		"pretty printed":              "{\n    \"version\": \"20.2.4\",\n    \"release\": \"tentacle\",\n    \"release_type\": \"stable\"\n}\n",
		"raw CLI text, still allowed": "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)",
	} {
		if maj, min, pat, err := harness.ReclaimExpectedRelease(normalize(answer)); err != nil || maj != 20 || min != 2 || pat != 4 {
			t.Fatalf("%s: %d.%d.%d %v", name, maj, min, pat, err)
		}
	}
	if maj, min, pat, err := harness.ReclaimExpectedRelease(normalize(`{"version":"19.2.5","release":"squid","release_type":"stable"}` + "\n")); err != nil || maj != 19 || min != 2 || pat != 5 {
		t.Fatalf("squid: %d.%d.%d %v", maj, min, pat, err)
	}
	for name, answer := range map[string]string{
		"empty object":    `{}`,
		"null version":    `{"version":null}`,
		"numeric version": `{"version":20.2}`,
		"two parts":       `{"version":"20.2"}`,
		"four parts":      `{"version":"20.2.4.1"}`,
		"suffixed":        `{"version":"20.2.4-rc1"}`,
		"empty":           ``,
		"unrelated text":  `not a version`,
		"array":           `["20.2.4"]`,
	} {
		if maj, min, pat, err := harness.ReclaimExpectedRelease(normalize(answer)); err == nil {
			t.Fatalf("%s accepted as %d.%d.%d", name, maj, min, pat)
		}
	}
}

func TestReclaimNativeLockProbeScript(t *testing.T) {
	f := newReclaimFake(t)
	run := func(a, u string) (harness.ResetLock, harness.ResetLock) {
		out, code := f.run(harness.ReclaimLockProbe, []string{"FAKE_A_ERRNO=" + a, "FAKE_U_ERRNO=" + u}, "fs0", "/a", "/u")
		if code != 0 {
			t.Fatalf("exit %d: %s", code, out)
		}
		la, lu, err := harness.ParseReclaimLock(out)
		if err != nil {
			t.Fatal(err)
		}
		return la, lu
	}
	if a, u := run("11", "11"); a != harness.ResetLockHeld || u != harness.ResetLockHeld {
		t.Fatalf("both held: %d %d", a, u)
	}
	if a, u := run("0", "11"); a != harness.ResetLockFree || u != harness.ResetLockHeld {
		t.Fatalf("A free: %d %d", a, u)
	}
	if a, u := run("5", "0"); a != harness.ResetLockUnknown || u != harness.ResetLockFree {
		t.Fatalf("other errno: %d %d", a, u)
	}
}

func TestReclaimNativeReclaimerScript(t *testing.T) {
	f := newReclaimFake(t)
	var work string
	start := func(rc string) (string, int) {
		work = t.TempDir()
		for _, gate := range []string{"mount", "finish"} {
			if err := os.WriteFile(filepath.Join(work, gate), []byte("tok"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		out, code := f.run(string(harness.ReclaimReclaimer), []string{"FAKE_RC=" + rc, "FAKE_INSTANCE=7001"}, "fs0", work, "tok", "target-uuid", "own-uuid", "/a")
		return out, code
	}
	read := func(phase string) (harness.ReclaimReceipt, bool) {
		data, err := os.ReadFile(filepath.Join(work, "receipt-"+phase))
		if err != nil {
			return harness.ReclaimReceipt{}, false
		}
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		var r harness.ReclaimReceipt
		if err := decoder.Decode(&r); err != nil {
			t.Fatalf("receipt %s: %v", phase, err)
		}
		return r, true
	}
	if out, code := start("0"); code != 0 {
		t.Fatalf("successful RESET exited %d: %s", code, out)
	}
	one, ok1 := read("1")
	two, ok2 := read("2")
	three, ok3 := read("3")
	if !ok1 || !ok2 || !ok3 || one.RC == nil || *one.RC != 0 || one.Flags != harness.CephReclaimReset || one.Target != "target-uuid" || one.OwnUUID != "own-uuid" ||
		one.PriorReclaimCalls != 0 || !one.Initialized || one.Handle == "" || two.Client != 7001 || two.Size != 27 || !two.NewFd || !three.Closed {
		t.Fatalf("receipts: %+v %+v %+v", one, two, three)
	}
	if want := sha256.Sum256([]byte("reclaim-acknowledged-data\x00\xff")); two.SHA256 != hex.EncodeToString(want[:]) {
		t.Fatalf("digest %q", two.SHA256)
	}

	out, code := start("-2")
	if code != 3 {
		t.Fatalf("a failed RESET exited %d, want 3: %s", code, out)
	}
	if one, ok := read("1"); !ok || one.RC == nil || *one.RC != -2 {
		t.Fatalf("receipt 1 of the failed RESET: %+v %v", one, ok)
	}
	if _, ok := read("2"); ok {
		t.Fatal("a failed RESET went on to mount and read")
	}
}

func TestReclaimNativeActorSyntax(t *testing.T) {
	for name, script := range map[string]string{
		"seed": harness.ReclaimSeed, "holder": string(harness.ReclaimHolder), "reclaimer": string(harness.ReclaimReclaimer),
		"attempt": harness.ReclaimAttempt, "lock probe": harness.ReclaimLockProbe, "probe": harness.ReclaimABIProbe,
	} {
		cmd := exec.Command("python3", "-I", "-c", "import sys; compile(sys.argv[1], 'script', 'exec')", script)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s does not compile: %v: %s", name, err, out)
		}
	}
}
