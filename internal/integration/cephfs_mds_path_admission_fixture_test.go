//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The lookup records below are the values that TestCephFSClientMDSPathCut
// logged in Ceph short run 40711075458 (attempt 1, job 122896383801): the
// client's pending request {TID:4 Op:lookup MDS:0} and the MDS's historic
// record of client.4239 tid 4. The JSON around the logged values is rebuilt
// from the field names the helpers parse; the shape of a real mds_requests
// dump is not claimed by the fixture.
const (
	lookupHistoricDescription = "client_request(client.4239:4 lookup #0x1/pathcut 2026-10-28T15:00:57.547985+0000 caller_uid=0, caller_gid=0{})"
	lookupPendingJSON         = `{"requests":[{"tid":4,"op":"lookup","mds":0,"target_ino":"","got_unsafe":0}]}`
)

// The logged events, split at spaces: the log prints the list with %v, so the
// boundaries of multi-word events are not recoverable and no safety class is
// read from them.
var lookupHistoricEvents = strings.Fields("initiated header_read throttled all_read dispatched creating lookup batch head acquired locks acquired locks acquired locks replying finishing request cleaned up request done")

func historicOp(t *testing.T, client, tid uint64, description string, events ...string) mdsRequestOp {
	t.Helper()
	entries := make([]map[string]string, 0, len(events))
	for _, event := range events {
		entries = append(entries, map[string]string{"event": event})
	}
	dump, err := json.Marshal(map[string]any{"ops": []any{map[string]any{
		"description": description,
		"type_data": map[string]any{
			"reqid":  map[string]any{"entity": map[string]any{"type": "client", "num": client}, "tid": tid},
			"events": entries,
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ops, err := parseMDSRequestOps(dump)
	if err != nil || len(ops) != 1 {
		t.Fatalf("historic fixture: %v, %d ops", err, len(ops))
	}
	return ops[0]
}

func TestCephFSMDSPathAdmitsOnlyThePendingMkdir(t *testing.T) {
	pending := func(body string) []clientMDSRequest {
		t.Helper()
		requests, err := parseClientMDSRequests([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return requests
	}

	if request, err := admitPendingMkdir(pending(`{"requests":[{"tid":4,"op":"mkdir","mds":0,"target_ino":"","got_unsafe":0}]}`)); err != nil || request.TID != 4 || request.Op != "mkdir" {
		t.Fatalf("a pending mkdir was not admitted: %+v, %v", request, err)
	}
	// The logged failure: the one pending request is the lookup of the
	// parent, tid 4, and the mkdir has not been sent.
	if request, err := admitPendingMkdir(pending(lookupPendingJSON)); err == nil || !strings.Contains(err.Error(), "lookup") {
		t.Fatalf("a pending lookup was admitted as the mkdir: %+v, %v", request, err)
	}
	for name, body := range map[string]string{
		"nothing pending":        `{}`,
		"two requests":           `{"requests":[{"tid":4,"op":"lookup"},{"tid":5,"op":"mkdir"}]}`,
		"unrelated operation":    `{"requests":[{"tid":4,"op":"create"}]}`,
		"missing operation":      `{"requests":[{"tid":4}]}`,
		"empty operation":        `{"requests":[{"tid":4,"op":""}]}`,
		"missing tid":            `{"requests":[{"op":"mkdir"}]}`,
		"zero tid":               `{"requests":[{"tid":0,"op":"mkdir"}]}`,
		"operation case differs": `{"requests":[{"tid":4,"op":"MKDIR"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if request, err := admitPendingMkdir(pending(body)); err == nil {
				t.Fatalf("admitted %+v", request)
			}
		})
	}
}

func TestCephFSMDSPathRequiresTheMkdirToComplete(t *testing.T) {
	const client, tid = 4239, 4
	mkdir := "client_request(client.4239:4 mkdir owner_uid=0, owner_gid=0 #0x10000000000/created 2026-10-28T15:00:57.547985+0000 caller_uid=0, caller_gid=0{})"
	safe := []string{"initiated", "early_replied", "journal_committed", "replying"}

	// Synthetic positive control: the shape of a mkdir that the MDS
	// journaled and answered twice.
	if err := requireMDSMkdirCompleted(historicOp(t, client, tid, mkdir, safe...), true, client, tid, "created"); err != nil {
		t.Fatalf("a completed mkdir was rejected: %v", err)
	}
	// The logged record: a lookup of tid 4 that the MDS completed. It is
	// complete evidence of a lookup and none of a mkdir.
	lookup := historicOp(t, client, tid, lookupHistoricDescription, lookupHistoricEvents...)
	if got := lookup.events(); len(got) == 0 || got[len(got)-1] != "done" {
		t.Fatalf("logged lookup events = %v", got)
	}
	if err := requireMDSMkdirCompleted(lookup, true, client, tid, "created"); err == nil || !strings.Contains(err.Error(), "lookup") {
		t.Fatalf("a completed lookup counted as the mkdir: %v", err)
	}
	for name, test := range map[string]struct {
		op    mdsRequestOp
		found bool
		other string
	}{
		"no record":           {op: mdsRequestOp{}, found: false},
		"other client":        {op: historicOp(t, client, tid, strings.Replace(mkdir, "client.4239", "client.4240", 1), safe...), found: true},
		"other tid":           {op: historicOp(t, client, tid, strings.Replace(mkdir, ":4 ", ":5 ", 1), safe...), found: true},
		"other name":          {op: historicOp(t, client, tid, strings.Replace(mkdir, "/created ", "/other ", 1), safe...), found: true},
		"empty description":   {op: historicOp(t, client, tid, "", safe...), found: true},
		"no early reply":      {op: historicOp(t, client, tid, mkdir, "initiated", "journal_committed", "replying"), found: true},
		"unsafe":              {op: historicOp(t, client, tid, mkdir, "initiated", "early_replied"), found: true},
		"commit after reply":  {op: historicOp(t, client, tid, mkdir, "initiated", "early_replied", "replying", "journal_committed"), found: true},
		"no events":           {op: historicOp(t, client, tid, mkdir), found: true},
		"name as a substring": {op: historicOp(t, client, tid, strings.Replace(mkdir, "/created ", "/created2 ", 1), safe...), found: true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := requireMDSMkdirCompleted(test.op, test.found, client, tid, "created"); err == nil {
				t.Fatalf("accepted %q %v", test.op.Description, test.op.events())
			}
		})
	}
}

// fakeCephFSForPathCut is a cephfs module whose mount records, per call,
// which dentries a path resolves: an absolute path looks up every component,
// a relative one only the working directory's children. It reaches no
// cluster, so it shows what the actor asks for and nothing about libcephfs.
const fakeCephFSForPathCut = `
import os
class Error(Exception):
    pass
class Stat:
    def __init__(self, ino, size=0):
        self.st_ino, self.st_size = ino, size
class LibCephFS:
    def __init__(self, conffile=None):
        self.cwd, self.ops, self.data = "/", [], b""
    def conf_set(self, key, value): pass
    def mount(self, filesystem_name=None): self.ops.append(("mount",))
    def resolve(self, path):
        path = path.decode()
        return os.path.normpath(path if path.startswith("/") else self.cwd + "/" + path), path
    def lookups(self, path):
        full, given = self.resolve(path)
        if given.startswith("/"):
            return [c for c in full.split("/") if c]
        return [given]
    def mkdir(self, path, mode):
        self.ops.append(("mkdir", path.decode(), self.cwd, self.lookups(path)))
    def chdir(self, path):
        self.cwd = self.resolve(path)[0]
        self.ops.append(("chdir", path.decode(), self.lookups(path)))
    def open(self, path, flags, mode): return 3
    def write(self, fd, data, offset): self.data = data
    def fsync(self, fd, datasync): pass
    def fstat(self, fd): return Stat(1099511627776, len(self.data))
    def stat(self, path): return Stat(1099511627777)
    def read(self, fd, offset, size): return self.data
    def close(self, fd): pass
    def unmount(self): pass
    def shutdown(self):
        import json, sys
        with open(os.environ["FAKE_OPS"], "w") as out:
            json.dump(self.ops, out)
    def get_instance_id(self): return 4239
`

type fakeMkdir struct {
	Op      string
	Path    string
	Cwd     string
	Lookups []string
}

// runPathCutActor runs the actor script against the fake module with all
// gates open and returns the recorded mkdir of the new directory.
func runPathCutActor(t *testing.T, script string) (fakeMkdir, error) {
	t.Helper()
	dir := t.TempDir()
	module := filepath.Join(dir, "module")
	work := filepath.Join(dir, "work")
	for _, d := range []string{module, work} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(module, "cephfs.py"), []byte(fakeCephFSForPathCut), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, gate := range []string{"metadata", "data", "finish"} {
		if err := os.WriteFile(filepath.Join(work, gate), []byte("token"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ops := filepath.Join(dir, "ops.json")
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	args := []string{"-c", "import sys\nsys.path.insert(0, " + fmt.Sprintf("%q", module) + ")\n" + script,
		"fs", work, "token", "/pathcut", "/pathcut/file", "/pathcut/created", "/unused", "00", "01"}
	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Env = append(os.Environ(), "FAKE_OPS="+ops, "PYTHONDONTWRITEBYTECODE=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fakeMkdir{}, fmt.Errorf("actor: %v: %.512s", err, out)
	}
	raw, err := os.ReadFile(ops)
	if err != nil {
		t.Fatal(err)
	}
	var recorded [][]json.RawMessage
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	var found []fakeMkdir
	for _, op := range recorded {
		var name string
		if json.Unmarshal(op[0], &name) != nil || name != "mkdir" {
			continue
		}
		mkdir := fakeMkdir{Op: name}
		if json.Unmarshal(op[1], &mkdir.Path) != nil || json.Unmarshal(op[2], &mkdir.Cwd) != nil || json.Unmarshal(op[3], &mkdir.Lookups) != nil {
			t.Fatalf("recorded op %s", raw)
		}
		found = append(found, mkdir)
	}
	// The first mkdir makes the pathcut directory; the second is the one
	// the actor sends from its metadata thread.
	if len(found) != 2 {
		t.Fatalf("recorded mkdirs = %+v", found)
	}
	return found[1], nil
}

func TestCephFSMDSPathActorSendsTheMkdirWithoutAParentWalk(t *testing.T) {
	got, err := runPathCutActor(t, string(cephFSPathCutActor))
	if err != nil {
		t.Fatal(err)
	}
	if got.Path != "created" || got.Cwd != "/pathcut" || len(got.Lookups) != 1 || got.Lookups[0] != "created" {
		t.Fatalf("the actor's mkdir = %+v, want the relative name in the working directory with no lookup of /pathcut", got)
	}

	// Negative control: the earlier script, which sent the absolute path,
	// resolves the pathcut dentry before the mkdir and is rejected.
	earlier := strings.NewReplacer(
		"assert os.path.dirname(created) == directory\nfs.chdir(directory)\n", "",
		"fs.mkdir(os.path.basename(created), 0o755)", "fs.mkdir(created, 0o755)",
	).Replace(string(cephFSPathCutActor))
	if earlier == string(cephFSPathCutActor) {
		t.Fatal("the negative control changed nothing")
	}
	old, err := runPathCutActor(t, earlier)
	if err != nil {
		t.Fatal(err)
	}
	if len(old.Lookups) < 2 || old.Lookups[0] != "pathcut" {
		t.Fatalf("the earlier actor's mkdir = %+v, want a walk through pathcut", old)
	}
}
