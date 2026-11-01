package monstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

// ---- the listing script, executed against a temporary directory ----

// monStoreRunScript executes the embedded listing script with the host's
// python3 against dir. The listing is real script output; what it shows for a
// store in a container is a native question.
func monStoreRunScript(t *testing.T, script, dir string) ([]byte, string, error) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("the script tests need python3: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), python, "-I", "-c", script, dir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	return stdout.Bytes(), stderr.String(), err
}

func monStoreWrite(t *testing.T, dir, rel, content string, mode os.FileMode) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(full, mode); err != nil {
		t.Fatal(err)
	}
}

func monStoreTempStore(t *testing.T) string {
	dir := t.TempDir()
	monStoreWrite(t, dir, "keyring", "[mon.]\n\tkey = AQD=\n", 0o600)
	monStoreWrite(t, dir, "kv_backend", "rocksdb\n", 0o644)
	monStoreWrite(t, dir, "store.db/000004.sst", "sst bytes", 0o644)
	monStoreWrite(t, dir, "store.db/CURRENT", "MANIFEST-000005\n", 0o644)
	return dir
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestMonStoreInspectScriptListsAStoreTheIdentityComputationConsumes(t *testing.T) {
	dir := monStoreTempStore(t)
	out, stderr, err := monStoreRunScript(t, monStoreInspectScriptSource(), dir)
	if err != nil || stderr != "" {
		t.Fatalf("script: %v %q", err, stderr)
	}
	files, err := parseMonStoreListing(out)
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, out)
	}
	var paths []string
	for _, e := range files.entries {
		paths = append(paths, e.path)
		full := filepath.Join(dir, filepath.FromSlash(e.path))
		info, err := os.Lstat(full)
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		if e.inode != uint64(st.Ino) || e.size != info.Size() || e.mtimeNs != info.ModTime().UnixNano() || e.mode != uint32(info.Mode().Perm()) {
			t.Errorf("%s: listed %+v, lstat inode %d size %d mtime %d mode %o", e.path, e, st.Ino, info.Size(), info.ModTime().UnixNano(), info.Mode().Perm())
		}
	}
	want := []string{"keyring", "kv_backend", "store.db", "store.db/000004.sst", "store.db/CURRENT"}
	if !slices.Equal(paths, want) {
		t.Fatalf("paths %v, want %v", paths, want)
	}
	for _, e := range files.entries {
		switch e.path {
		case "keyring":
			if e.digest != sha256Hex("[mon.]\n\tkey = AQD=\n") || e.kind != monStoreKindFile {
				t.Errorf("keyring %+v", e)
			}
		case "store.db":
			if e.kind != monStoreKindDir || e.digest != "" {
				t.Errorf("store.db %+v", e)
			}
		}
	}
	if files.kvBackend == nil || *files.kvBackend != "rocksdb\n" {
		t.Fatalf("kv_backend %v", files.kvBackend)
	}
	keyring, shape, fingerprint, err := files.storeFacts()
	if err != nil || keyring != sha256Hex("[mon.]\n\tkey = AQD=\n") || shape != monStoreShapeRocksDB || fingerprint != monStoreFingerprint(files.entries) {
		t.Fatalf("storeFacts: %v %q %q", err, keyring, shape)
	}
	again, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), dir)
	if err != nil || !bytes.Equal(out, again) {
		t.Fatalf("two listings of the same store differ: %v", err)
	}
}

func TestMonStoreInspectScriptWritesNothing(t *testing.T) {
	dir := monStoreTempStore(t)
	snapshot := func() string {
		var b strings.Builder
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			st := info.Sys().(*syscall.Stat_t)
			fmt.Fprintf(&b, "%s %d %d %d %o\n", p, st.Ino, info.Size(), info.ModTime().UnixNano(), info.Mode())
			return nil
		})
		return b.String()
	}
	before := snapshot()
	if _, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), dir); err != nil {
		t.Fatal(err)
	}
	if after := snapshot(); after != before {
		t.Fatalf("the store changed:\n%s\n%s", before, after)
	}
}

func TestMonStoreInspectScriptNeverFollowsLinksAndRefusesThemThroughStoreFacts(t *testing.T) {
	outside := t.TempDir()
	monStoreWrite(t, outside, "secret/inner", "outside bytes", 0o644)
	monStoreWrite(t, outside, "outside-keyring", "other key", 0o600)
	cases := map[string]func(dir string) error{
		"symlink to an outside file": func(dir string) error {
			return os.Symlink(filepath.Join(outside, "outside-keyring"), filepath.Join(dir, "store.db", "link"))
		},
		"symlink to an outside directory": func(dir string) error {
			return os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "store.db", "dirlink"))
		},
		"relative escape": func(dir string) error {
			return os.Symlink("../../../../../etc", filepath.Join(dir, "store.db", "escape"))
		},
		"dangling symlink": func(dir string) error { return os.Symlink("missing", filepath.Join(dir, "store.db", "dangling")) },
		"keyring replaced by a symlink": func(dir string) error {
			_ = os.Remove(filepath.Join(dir, "keyring"))
			return os.Symlink(filepath.Join(outside, "outside-keyring"), filepath.Join(dir, "keyring"))
		},
		"kv_backend replaced by a symlink": func(dir string) error {
			_ = os.Remove(filepath.Join(dir, "kv_backend"))
			return os.Symlink(filepath.Join(outside, "outside-keyring"), filepath.Join(dir, "kv_backend"))
		},
		"store.db replaced by a dir symlink": func(dir string) error {
			_ = os.RemoveAll(filepath.Join(dir, "store.db"))
			return os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "store.db"))
		},
		"fifo": func(dir string) error { return syscall.Mkfifo(filepath.Join(dir, "store.db", "pipe"), 0o600) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := monStoreTempStore(t)
			if err := mutate(dir); err != nil {
				t.Fatal(err)
			}
			out, stderr, err := monStoreRunScript(t, monStoreInspectScriptSource(), dir)
			if err != nil || stderr != "" {
				t.Fatalf("script: %v %q", err, stderr)
			}
			files, err := parseMonStoreListing(out)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range files.entries {
				if strings.Contains(e.path, "inner") || (e.kind == monStoreKindSymlink && e.digest != "") {
					t.Fatalf("followed a link: %+v", e)
				}
			}
			if _, _, _, err := files.storeFacts(); err == nil {
				t.Fatalf("storeFacts accepted the store:\n%s", out)
			}
		})
	}
	// Paired control: the same layout without the link is accepted.
	out, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), monStoreTempStore(t))
	files, perr := parseMonStoreListing(out)
	if err != nil || perr != nil {
		t.Fatal(err, perr)
	}
	if _, _, _, err := files.storeFacts(); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestMonStoreInspectScriptRefusesWhatItCannotListCompletely(t *testing.T) {
	dir := monStoreTempStore(t)
	out, stderr, err := monStoreRunScript(t, fmt.Sprintf(monStoreInspectScript, 3), dir)
	if err == nil || len(out) != 0 || !strings.Contains(stderr, "bound") {
		t.Fatalf("entry bound: %v %q stdout %q", err, stderr, out)
	}
	if _, err := parseMonStoreListing(out); err == nil {
		t.Fatal("an empty listing parsed")
	}
	out, _, err = monStoreRunScript(t, fmt.Sprintf(monStoreInspectScript, 5), dir)
	if err != nil {
		t.Fatalf("a bound equal to the entry count must pass: %v %s", err, out)
	}
	if _, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing data directory listed")
	}
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, stderr, err := monStoreRunScript(t, monStoreInspectScriptSource(), link); err == nil || stderr == "" {
		t.Fatalf("a symlinked data directory listed: %v", err)
	}
	if _, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), filepath.Join(dir, "keyring")); err == nil {
		t.Fatal("a file as data directory listed")
	}
}

func TestMonStoreInspectScriptRefusesANameThatIsNotUTF8(t *testing.T) {
	dir := monStoreTempStore(t)
	name := filepath.Join(dir, "store.db", "bad\xff")
	if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
		t.Skipf("this file system refuses a non-UTF-8 name: %v", err)
	}
	if out, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), dir); err == nil || len(out) != 0 {
		t.Fatalf("listed a non-UTF-8 name: %v %q", err, out)
	}
}

// ---- the listing parser ----

func monStoreListingLines(t *testing.T) []string {
	out, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), monStoreTempStore(t))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
}

func monStoreJoin(lines []string) []byte { return []byte(strings.Join(lines, "\n") + "\n") }

func TestMonStoreListingParserRefusesAnythingButTheExactFormat(t *testing.T) {
	lines := monStoreListingLines(t)
	if _, err := parseMonStoreListing(monStoreJoin(lines)); err != nil {
		t.Fatalf("control: %v", err)
	}
	first, last := 1, len(lines)-1
	cases := map[string]func() []byte{
		"empty":            func() []byte { return nil },
		"no final newline": func() []byte { return []byte(strings.Join(lines, "\n")) },
		"blank line":       func() []byte { return monStoreJoin(slices.Insert(slices.Clone(lines), 2, "")) },
		"no trailer":       func() []byte { return monStoreJoin(lines[:last]) },
		"only the trailer": func() []byte { return monStoreJoin([]string{lines[0], lines[last]}) },
		"unknown format":   func() []byte { return monStoreJoin(slices.Insert(lines[1:], 0, `{"format":"v2"}`)) },
		"format missing":   func() []byte { return monStoreJoin(slices.Insert(lines[1:], 0, `{}`)) },
		"format line extra": func() []byte {
			return monStoreJoin(slices.Insert(lines[1:], 0, `{"format":"`+monStoreInspectListingFormat+`","x":1}`))
		},
		"entry after trailer": func() []byte { return monStoreJoin(append(slices.Clone(lines), lines[first])) },
		"trailer count low": func() []byte {
			return monStoreJoin(append(slices.Clone(lines[:last]), strings.Replace(lines[last], `"count":5`, `"count":4`, 1)))
		},
		"trailer count high": func() []byte {
			return monStoreJoin(append(slices.Clone(lines[:last]), strings.Replace(lines[last], `"count":5`, `"count":6`, 1)))
		},
		"trailer end false": func() []byte {
			return monStoreJoin(append(slices.Clone(lines[:last]), strings.Replace(lines[last], `"end":true`, `"end":false`, 1)))
		},
		"trailer kv missing": func() []byte { return monStoreJoin(append(slices.Clone(lines[:last]), `{"end":true,"count":5}`)) },
		"trailer kv number": func() []byte {
			return monStoreJoin(append(slices.Clone(lines[:last]), `{"end":true,"count":5,"kv_backend":1}`))
		},
		"trailer extra": func() []byte {
			return monStoreJoin(append(slices.Clone(lines[:last]), `{"end":true,"count":5,"kv_backend":null,"x":1}`))
		},
		"duplicate entry":     func() []byte { return monStoreJoin(slices.Insert(lines, 2, lines[first])) },
		"unordered entries":   func() []byte { return monStoreJoin(slices.Concat(lines[:1], []string{lines[2], lines[1]}, lines[3:])) },
		"entry not an object": func() []byte { return monStoreJoin(slices.Insert(lines[1:], 1, `[]`)) },
		"entry null":          func() []byte { return monStoreJoin(slices.Insert(lines[1:], 1, `null`)) },
		"duplicate key": func() []byte {
			return monStoreJoin(slices.Concat(lines[:1], []string{strings.Replace(lines[1], `{"path"`, `{"size":1,"path"`, 1)}, lines[2:]))
		},
		"trailing text": func() []byte { return monStoreJoin(slices.Concat(lines[:1], []string{lines[1] + " x"}, lines[2:])) },
		"invalid UTF-8": func() []byte { return append(monStoreJoin(lines[:last]), []byte("\xff\n"+lines[last]+"\n")...) },
		"replacement in path": func() []byte {
			return monStoreJoin(slices.Concat(lines[:1], []string{strings.Replace(lines[1], `"path":"keyring"`, `"path":"key�ring"`, 1)}, lines[2:]))
		},
		"control in path": func() []byte {
			return monStoreJoin(slices.Concat(lines[:1], []string{strings.Replace(lines[1], `"path":"keyring"`, `"path":"key\nring"`, 1)}, lines[2:]))
		},
		"mode beyond bits": func() []byte {
			return monStoreJoin(slices.Concat(lines[:1], []string{strings.Replace(lines[1], `"mode":384`, `"mode":33152`, 1)}, lines[2:]))
		},
		"unknown kind": func() []byte {
			return monStoreJoin(slices.Concat(lines[:1], []string{strings.Replace(lines[1], `"kind":"file"`, `"kind":"socket"`, 1)}, lines[2:]))
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			if f, err := parseMonStoreListing(build()); err == nil {
				t.Fatalf("accepted %d entries", len(f.entries))
			}
		})
	}
	for _, field := range []string{"path", "kind", "mode", "inode", "size", "mtime_ns", "digest"} {
		for variant, value := range map[string]any{"null": nil, "wrong type": []any{}, "missing": "\x00missing"} {
			t.Run(field+" "+variant, func(t *testing.T) {
				doc := monStoreDecode(t, []byte(lines[first]))
				if variant == "missing" {
					delete(doc, field)
				} else {
					doc[field] = value
				}
				got := slices.Clone(lines)
				got[first] = string(monStoreEncode(t, doc))
				if _, err := parseMonStoreListing(monStoreJoin(got)); err == nil {
					t.Fatalf("accepted %s %s", field, variant)
				}
			})
		}
	}
}

func TestMonStoreListingParserDistinguishesAnUnreadMarkerFromAnEmptyOne(t *testing.T) {
	lines := monStoreListingLines(t)
	last := len(lines) - 1
	build := func(trailer string) monStoreFiles {
		files, err := parseMonStoreListing(monStoreJoin(append(slices.Clone(lines[:last]), trailer)))
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	unread, empty := build(`{"end":true,"count":5,"kv_backend":null}`), build(`{"end":true,"count":5,"kv_backend":""}`)
	if unread.kvBackend != nil || empty.kvBackend == nil || *empty.kvBackend != "" {
		t.Fatalf("unread %v empty %v", unread.kvBackend, empty.kvBackend)
	}
	for name, files := range map[string]monStoreFiles{"null": unread, "empty": empty} {
		if _, _, _, err := files.storeFacts(); err == nil {
			t.Errorf("%s marker produced a store identity", name)
		}
	}
}

// ---- the request ----

const monStoreTestVolume = "tc-ceph-mon-volume-test"

func monStoreRequestFixture(t *testing.T) monStoreInspectRequest {
	req, err := newMonStoreInspectRequest("registry.example/ceph:20.2.4", "167:167", "owner-1", monStoreTestVolume, "token-1")
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestMonStoreInspectRequestIsReadOnlyIsolatedAndOwned(t *testing.T) {
	req := monStoreRequestFixture(t)
	if err := req.validate(); err != nil {
		t.Fatal(err)
	}
	if req.volume != monStoreTestVolume || req.target != "/store" || !req.readOnlyMount || req.networkMode != "none" ||
		!slices.Equal(req.capDrop, []string{"ALL"}) || !req.noNewPrivileges || !req.readOnlyRootfs || req.privileged || req.autoRemove ||
		req.labels[clusterutil.OwnerLabel] != "owner-1" || req.labels[monStoreInspectorLabel] != "token-1" || len(req.labels) != 2 ||
		req.user != "167:167" || req.cmd[len(req.cmd)-1] != "/store" || !slices.Equal(req.entrypoint, []string{"python3"}) {
		t.Fatalf("request %+v", req)
	}
	mutations := map[string]func(r *monStoreInspectRequest){
		"writable mount":    func(r *monStoreInspectRequest) { r.readOnlyMount = false },
		"network":           func(r *monStoreInspectRequest) { r.networkMode = "bridge" },
		"host network":      func(r *monStoreInspectRequest) { r.networkMode = "host" },
		"no network field":  func(r *monStoreInspectRequest) { r.networkMode = "" },
		"capabilities kept": func(r *monStoreInspectRequest) { r.capDrop = nil },
		"one capability":    func(r *monStoreInspectRequest) { r.capDrop = []string{"CHOWN"} },
		"escalation":        func(r *monStoreInspectRequest) { r.noNewPrivileges = false },
		"writable root":     func(r *monStoreInspectRequest) { r.readOnlyRootfs = false },
		"privileged":        func(r *monStoreInspectRequest) { r.privileged = true },
		"auto remove":       func(r *monStoreInspectRequest) { r.autoRemove = true },
		"volume removal":    func(r *monStoreInspectRequest) { r.keepVolumesOnRemove = false },
		"no owned label":    func(r *monStoreInspectRequest) { delete(r.labels, clusterutil.OwnerLabel) },
		"empty owned label": func(r *monStoreInspectRequest) { r.labels[clusterutil.OwnerLabel] = "" },
		"no token":          func(r *monStoreInspectRequest) { delete(r.labels, monStoreInspectorLabel) },
		"extra label":       func(r *monStoreInspectRequest) { r.labels["x"] = "y" },
		"other target":      func(r *monStoreInspectRequest) { r.target = "/var/lib/ceph" },
		"no volume":         func(r *monStoreInspectRequest) { r.volume = "" },
		"no image":          func(r *monStoreInspectRequest) { r.image = "" },
		"root user":         func(r *monStoreInspectRequest) { r.user = "" },
		"named user":        func(r *monStoreInspectRequest) { r.user = "ceph" },
		"other entrypoint":  func(r *monStoreInspectRequest) { r.entrypoint = []string{"sh"} },
		"other command":     func(r *monStoreInspectRequest) { r.cmd = []string{"-c", "rm -rf /store", "/store"} },
		"script edited":     func(r *monStoreInspectRequest) { r.cmd = slices.Clone(r.cmd); r.cmd[2] += "\nopen('/store/x','w')" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := monStoreRequestFixture(t)
			mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatal("validate accepted the request")
			}
		})
	}
	for name, args := range map[string][5]string{
		"no image": {"", "167:167", "o", "v", "t"}, "no user": {"i", "", "o", "v", "t"}, "named user": {"i", "ceph:ceph", "o", "v", "t"},
		"negative user": {"i", "-1:0", "o", "v", "t"}, "no owner": {"i", "1:1", "", "v", "t"}, "no volume": {"i", "1:1", "o", "", "t"}, "no token": {"i", "1:1", "o", "v", ""},
	} {
		if _, err := newMonStoreInspectRequest(args[0], args[1], args[2], args[3], args[4]); err == nil {
			t.Errorf("%s: built a request", name)
		}
	}
}

// ---- the driver, with a fake engine ----

type monStoreEngineFake struct {
	calls     []string
	req       monStoreInspectRequest
	createErr error
	createID  *string
	startErr  error
	waitErr   error
	waitCode  int64
	waitBlock bool
	logs      monStoreInspectLogs
	logsErr   error
	removeErr error
	// removeLive is true when the context of Remove was not ended.
	removeLive []bool
	limit      int
}

func (f *monStoreEngineFake) Create(_ context.Context, req monStoreInspectRequest) (string, error) {
	f.calls = append(f.calls, "create")
	f.req = req
	if f.createErr != nil {
		return "", f.createErr
	}
	if f.createID != nil {
		return *f.createID, nil
	}
	return "inspector-1", nil
}
func (f *monStoreEngineFake) Start(context.Context, string) error {
	f.calls = append(f.calls, "start")
	return f.startErr
}
func (f *monStoreEngineFake) Wait(ctx context.Context, _ string) (int64, error) {
	f.calls = append(f.calls, "wait")
	if f.waitBlock {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.waitCode, f.waitErr
}
func (f *monStoreEngineFake) Logs(_ context.Context, _ string, limit int) (monStoreInspectLogs, error) {
	f.calls = append(f.calls, "logs")
	f.limit = limit
	return f.logs, f.logsErr
}
func (f *monStoreEngineFake) Remove(ctx context.Context, id string) error {
	f.calls = append(f.calls, "remove:"+id)
	f.removeLive = append(f.removeLive, ctx.Err() == nil)
	return f.removeErr
}

func monStoreInspectorFixture(t *testing.T) (monStoreContainerInspector, *monStoreEngineFake) {
	out, _, err := monStoreRunScript(t, monStoreInspectScriptSource(), monStoreTempStore(t))
	if err != nil {
		t.Fatal(err)
	}
	f := &monStoreEngineFake{logs: monStoreInspectLogs{stdout: out}}
	return monStoreContainerInspector{engine: f, image: "registry.example/ceph:20.2.4", user: "167:167", owner: "owner-1", newToken: func() string { return "token-1" }}, f
}

func removals(calls []string) int {
	n := 0
	for _, c := range calls {
		if strings.HasPrefix(c, "remove:") {
			n++
		}
	}
	return n
}

func TestMonStoreContainerInspectorRunsOneContainerAndRemovesIt(t *testing.T) {
	ins, f := monStoreInspectorFixture(t)
	files, err := ins.Inspect(t.Context(), monStoreTestVolume)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"create", "start", "wait", "logs", "remove:inspector-1"}; !slices.Equal(f.calls, want) {
		t.Fatalf("calls %v", f.calls)
	}
	if err := f.req.validate(); err != nil || f.req.volume != monStoreTestVolume || f.limit != monStoreInspectOutputLimit {
		t.Fatalf("request %v limit %d", err, f.limit)
	}
	if _, _, _, err := files.storeFacts(); err != nil {
		t.Fatalf("the listing is not a bindable store: %v", err)
	}
	var _ monStoreInspector = ins
}

func TestMonStoreContainerInspectorRemovesTheContainerOnEveryOutcome(t *testing.T) {
	cases := map[string]func(f *monStoreEngineFake){
		"start fails":       func(f *monStoreEngineFake) { f.startErr = errors.New("start") },
		"wait fails":        func(f *monStoreEngineFake) { f.waitErr = errors.New("wait") },
		"non-zero exit":     func(f *monStoreEngineFake) { f.waitCode = 3 },
		"logs fail":         func(f *monStoreEngineFake) { f.logsErr = errors.New("logs") },
		"truncated output":  func(f *monStoreEngineFake) { f.logs.truncated = true },
		"oversized output":  func(f *monStoreEngineFake) { f.logs.stdout = make([]byte, monStoreInspectOutputLimit+1) },
		"standard error":    func(f *monStoreEngineFake) { f.logs.stderr = []byte("warning") },
		"empty output":      func(f *monStoreEngineFake) { f.logs.stdout = nil },
		"garbage output":    func(f *monStoreEngineFake) { f.logs.stdout = []byte("not a listing\n") },
		"truncated listing": func(f *monStoreEngineFake) { f.logs.stdout = f.logs.stdout[:len(f.logs.stdout)/2] },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ins, f := monStoreInspectorFixture(t)
			mutate(f)
			files, err := ins.Inspect(t.Context(), monStoreTestVolume)
			if err == nil || len(files.entries) != 0 || files.kvBackend != nil {
				t.Fatalf("accepted: %v", err)
			}
			if removals(f.calls) != 1 || !f.removeLive[0] {
				t.Fatalf("calls %v live %v", f.calls, f.removeLive)
			}
			for _, call := range []string{"create", "start", "wait"} {
				if n := strings.Count(strings.Join(f.calls, ","), call); n > 1 {
					t.Fatalf("%s was repeated: %v", call, f.calls)
				}
			}
		})
	}
}

func TestMonStoreContainerInspectorDoesNotRetryOrReadAfterAnUnknownOutcome(t *testing.T) {
	boom := errors.New("daemon connection lost")
	ins, f := monStoreInspectorFixture(t)
	f.createErr = boom
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); !errors.Is(err, boom) || !strings.Contains(err.Error(), "outcome unknown") || !slices.Equal(f.calls, []string{"create"}) {
		t.Fatalf("create: %v %v", err, f.calls)
	}
	ins, f = monStoreInspectorFixture(t)
	empty := ""
	f.createID = &empty
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); err == nil || !slices.Equal(f.calls, []string{"create"}) {
		t.Fatalf("create without an ID: %v %v", err, f.calls)
	}
	ins, f = monStoreInspectorFixture(t)
	f.startErr = boom
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); !errors.Is(err, boom) || !slices.Equal(f.calls, []string{"create", "start", "remove:inspector-1"}) {
		t.Fatalf("start: %v %v", err, f.calls)
	}
	ins, f = monStoreInspectorFixture(t)
	f.waitErr = boom
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); !errors.Is(err, boom) || !slices.Equal(f.calls, []string{"create", "start", "wait", "remove:inspector-1"}) {
		t.Fatalf("wait: %v %v", err, f.calls)
	}
}

func TestMonStoreContainerInspectorKeepsFirstFailureAndRemovalFailureSeparate(t *testing.T) {
	boom, stuck := errors.New("wait failed"), errors.New("removal refused")
	ins, f := monStoreInspectorFixture(t)
	f.removeErr = stuck
	files, err := ins.Inspect(t.Context(), monStoreTestVolume)
	if !errors.Is(err, stuck) || len(files.entries) != 0 {
		t.Fatalf("a listing was returned although the inspector stayed: %v", err)
	}
	ins, f = monStoreInspectorFixture(t)
	f.waitErr, f.removeErr = boom, stuck
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); !errors.Is(err, boom) || !errors.Is(err, stuck) {
		t.Fatalf("both failures must be kept: %v", err)
	}
}

func TestMonStoreContainerInspectorCleansUpWhenTheCallerGivesUp(t *testing.T) {
	ins, f := monStoreInspectorFixture(t)
	f.waitBlock = true
	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, err := ins.Inspect(ctx, monStoreTestVolume); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if removals(f.calls) != 1 || !f.removeLive[0] {
		t.Fatalf("the removal ran under the ended context: %v %v", f.calls, f.removeLive)
	}
	ins, f = monStoreInspectorFixture(t)
	f.waitBlock = true
	ins.timeout = 20 * time.Millisecond
	if _, err := ins.Inspect(t.Context(), monStoreTestVolume); !errors.Is(err, context.DeadlineExceeded) || removals(f.calls) != 1 || slices.Contains(f.calls, "logs") {
		t.Fatalf("timeout: %v %v", err, f.calls)
	}
}

func TestMonStoreContainerInspectorRefusesWithoutDispatchingAnything(t *testing.T) {
	ins, f := monStoreInspectorFixture(t)
	for name, mutate := range map[string]func(*monStoreContainerInspector){
		"no image": func(i *monStoreContainerInspector) { i.image = "" }, "named user": func(i *monStoreContainerInspector) { i.user = "ceph" },
		"no owner": func(i *monStoreContainerInspector) { i.owner = "" },
	} {
		bad := ins
		mutate(&bad)
		if _, err := bad.Inspect(t.Context(), monStoreTestVolume); err == nil || len(f.calls) != 0 {
			t.Fatalf("%s: %v %v", name, err, f.calls)
		}
	}
	if _, err := ins.Inspect(t.Context(), ""); err == nil || len(f.calls) != 0 {
		t.Fatalf("no volume: %v %v", err, f.calls)
	}
	var typedNil *monStoreEngineFake
	for name, engine := range map[string]monStoreInspectEngine{"nil": nil, "typed nil": typedNil} {
		bad := ins
		bad.engine = engine
		if _, err := bad.Inspect(t.Context(), monStoreTestVolume); err == nil {
			t.Fatalf("%s engine accepted", name)
		}
	}
}

// TestMonStoreInspectorAndMonmapAdaptersFeedVerifyStopped runs the real script
// output and the decoded monmap through the preflight of inspect.go.
func TestMonStoreInspectorAndMonmapAdaptersFeedVerifyStopped(t *testing.T) {
	ins, _ := monStoreInspectorFixture(t)
	dump := monStoreDecode(t, monStoreMonDumpFixture(t))
	dump["quorum"] = []int{0}
	cli := &monStoreCLIFake{out: map[string][]byte{
		"dump": monStoreEncode(t, dump), "quorum_status": monStoreStatusFixture(t, []int{0}, []string{"a"}, "a"),
	}, err: map[string]error{}}
	id, err := observeStopped(t.Context(), "b", monStoreTestVolume, ins, monStoreMonmapCLI{run: cli.run})
	if err != nil {
		t.Fatal(err)
	}
	if err := id.validate(); err != nil {
		t.Fatal(err)
	}
}

// monStoreMarkerPipeline runs the real script, the parser and storeFacts on a
// store whose kv_backend holds marker.
func monStoreMarkerPipeline(t *testing.T, marker string) (error, error) {
	t.Helper()
	dir := monStoreTempStore(t)
	monStoreWrite(t, dir, "kv_backend", marker, 0o644)
	out, _, scriptErr := monStoreRunScript(t, monStoreInspectScriptSource(), dir)
	if scriptErr != nil {
		return scriptErr, nil
	}
	files, err := parseMonStoreListing(out)
	if err != nil {
		return nil, err
	}
	_, _, _, err = files.storeFacts()
	return nil, err
}

// TestMonStoreInspectScriptNeverHidesAMarkerSuffix pins that the marker is read
// completely or refused: a suffix beyond a fixed read size must not vanish.
func TestMonStoreInspectScriptNeverHidesAMarkerSuffix(t *testing.T) {
	refused := map[string]string{
		"suffix after 58 newlines (76 bytes)": "rocksdb" + strings.Repeat("\n", 58) + "not-rocksdb",
		"suffix after the old 65-byte read":   "rocksdb" + strings.Repeat("\n", 58) + "x",
		"suffix just inside the bound":        "rocksdb" + strings.Repeat("\n", 4096-7-1) + "x",
		"marker beyond the bound":             "rocksdb" + strings.Repeat("\n", 4096),
		"marker not UTF-8":                    "rocksdb\xff",
	}
	for name, marker := range refused {
		t.Run(name, func(t *testing.T) {
			scriptErr, err := monStoreMarkerPipeline(t, marker)
			if scriptErr == nil && err == nil {
				t.Fatal("the pipeline reported a supported backend")
			}
		})
	}
	accepted := map[string]string{
		"plain":                     "rocksdb\n",
		"65-byte whitespace padded": "rocksdb" + strings.Repeat("\n", 58),
		"whole marker at the bound": "rocksdb" + strings.Repeat("\n", 4096-7),
	}
	for name, marker := range accepted {
		t.Run(name, func(t *testing.T) {
			if scriptErr, err := monStoreMarkerPipeline(t, marker); scriptErr != nil || err != nil {
				t.Fatalf("control refused: %v %v", scriptErr, err)
			}
		})
	}
}
