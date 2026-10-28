package ceph

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The require-store mode of mon.sh and mon-join.sh is exercised by running the
// real scripts against stubbed ceph-mon, monmaptool, ceph-authtool and ceph
// and a temporary directory standing in for the container filesystem. The
// stubs record their calls; the ceph-mon stub creates store.db on --mkfs, so a
// silent format is visible both as a call and as a changed directory tree.
//
// Ceph facts the mode rests on, at 7f793731f1b39eb4f465e960113d2363c311b964
// (src/ceph_mon.cc, src/mon/MonitorDBStore.h, src/mon/Monitor.cc):
//   - ceph-mon --mkfs exits 0 without formatting when the data directory holds
//     anything but kv_backend (check_mon_data_empty), and ceph-mon without
//     --mkfs exits when the directory is missing or empty; so only the
//     entrypoint script decides between format and reuse, and it formats
//     whenever store.db is absent.
//   - MonitorDBStore::open reads kv_backend and, when it is absent or empty,
//     assumes rocksdb and writes it; the store lives in <data>/store.db.
//   - Monitor::init loads <data>/keyring and, when missing, rebuilds it from
//     the store; Monitor::mkfs moves the mon. key there.

const requireStoreMode = "CEPH_MON_STORE_MODE"

type requireStoreFixture struct {
	script, root, calls, name, dataDir string
	env                                map[string]string
}

// newRequireStoreFixture prepares the named script ("mon" or "mon-join") with
// its container paths redirected into a temporary root. mutate may rewrite
// the script text; it is how the negative controls break the script.
func newRequireStoreFixture(t *testing.T, name string, mutate func(string) string) *requireStoreFixture {
	t.Helper()
	root := t.TempDir()
	aux := t.TempDir()
	f := &requireStoreFixture{root: root, name: name, calls: filepath.Join(aux, "calls"), script: filepath.Join(aux, "script.sh")}
	mon := "a"
	f.env = map[string]string{"CEPH_FSID": "synthetic-fsid", "CEPH_OSD_BLOCK_SIZE": "1073741824", "TC_CALLS": f.calls, "TC_HOSTNAME_I": "192.0.2.12"}
	if name == "mon-join" {
		mon = "b"
		f.env["CEPH_MON_ID"] = mon
	}
	f.dataDir = filepath.Join(root, "var/lib/ceph/mon/ceph-"+mon)
	bin := filepath.Join(aux, "bin")
	for _, dir := range []string{bin, filepath.Join(root, "etc/ceph"), filepath.Join(root, "tc"), filepath.Join(root, "tmp")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// A joining MON reads the live map and shared key through these files.
	for _, file := range []string{"etc/ceph/mon.keyring", "etc/ceph/ceph.conf"} {
		if err := os.WriteFile(filepath.Join(root, file), []byte("synthetic\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stub := "#!/bin/sh\nprintf '%s %s\\n' \"${0##*/}\" \"$*\" >> \"$TC_CALLS\"\n" +
		"case \"$*\" in --mkfs*) mkdir -p \"$TC_DATA/store.db\"; echo created > \"$TC_DATA/store.db/CURRENT\" ;; esac\n"
	f.env["TC_DATA"] = f.dataDir
	for _, command := range []string{"ceph-mon", "monmaptool", "ceph-authtool", "ceph"} {
		if err := os.WriteFile(filepath.Join(bin, command), []byte(stub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "hostname"), []byte("#!/bin/sh\nprintf '%s\\n' \"$TC_HOSTNAME_I\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	if mutate != nil {
		mutated := mutate(source)
		if mutated == source {
			t.Fatal("mutation did not change the script; the control is void")
		}
		source = mutated
	}
	for _, path := range []string{"/etc/ceph", "/var/lib/ceph", "/var/run/ceph", "/tmp/monmap", "/tc/monmap", userConfigPath} {
		source = strings.ReplaceAll(source, path, root+path)
	}
	if err := os.WriteFile(f.script, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	f.env["PATH"] = bin + string(os.PathListSeparator) + os.Getenv("PATH")
	return f
}

func (f *requireStoreFixture) put(t *testing.T, rel, content string) {
	t.Helper()
	path := filepath.Join(f.dataDir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// store writes a store shaped like the one mkfs leaves.
func (f *requireStoreFixture) store(t *testing.T) {
	t.Helper()
	f.put(t, "store.db/CURRENT", "MANIFEST-000004\n")
	f.put(t, "kv_backend", "rocksdb\n")
	f.put(t, "keyring", "[mon.]\n\tkey = synthetic\n")
}

type requireStoreResult struct {
	calls    []string
	output   string
	exitCode int
}

func (f *requireStoreFixture) run(t *testing.T, mode *string) requireStoreResult {
	t.Helper()
	if err := os.Remove(f.calls); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "/bin/sh", f.script)
	for k, v := range f.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if mode != nil {
		cmd.Env = append(cmd.Env, requireStoreMode+"="+*mode)
	}
	out, err := cmd.CombinedOutput()
	result := requireStoreResult{output: string(out)}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		result.exitCode = exit.ExitCode()
	default:
		t.Fatal(err)
	}
	data, readErr := os.ReadFile(f.calls)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	if len(data) > 0 {
		result.calls = strings.Split(strings.TrimSpace(string(data)), "\n")
	}
	return result
}

// tree records every entry under the fake container root, with link targets
// and file contents, so any write shows as a difference.
func (f *requireStoreFixture) tree(t *testing.T) map[string]string {
	t.Helper()
	tree := map[string]string{}
	err := filepath.Walk(f.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(f.root, path)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			tree[rel] = "link:" + target
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			tree[rel] = "file:" + string(data)
		default:
			tree[rel] = "dir"
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// dataTree is tree restricted to the MON data directory.
func (f *requireStoreFixture) dataTree(t *testing.T) map[string]string {
	t.Helper()
	rel, _ := filepath.Rel(f.root, f.dataDir)
	sub := map[string]string{}
	for path, v := range f.tree(t) {
		if path == rel || strings.HasPrefix(path, rel+string(filepath.Separator)) {
			sub[path] = v
		}
	}
	return sub
}

func requireMode() *string { v := "require"; return &v }

func hasCall(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

// Each scenario returns an error instead of failing the test, so the same
// scenario serves the real script (must return nil) and a deliberately broken
// script (must return an error: the negative control).
type requireStoreScenario func(t *testing.T, f *requireStoreFixture) error

func scenarioRequirePresentStoreReused(t *testing.T, f *requireStoreFixture) error {
	f.store(t)
	// Only the data directory is compared: the entrypoint still creates its
	// run directory, as it always did.
	before := f.dataTree(t)
	r := f.run(t, requireMode())
	if r.exitCode != 0 {
		return fmt.Errorf("present store refused: exit=%d output=%s", r.exitCode, r.output)
	}
	if want := []string{"ceph-mon -f -i " + strings.TrimPrefix(filepath.Base(f.dataDir), "ceph-")}; !reflect.DeepEqual(r.calls, want) {
		return fmt.Errorf("present store: calls=%v want %v", r.calls, want)
	}
	if !reflect.DeepEqual(before, f.dataTree(t)) {
		return errors.New("reusing a present store changed files")
	}
	return nil
}

func scenarioRequireOldStoreWithoutOptionalFiles(t *testing.T, f *requireStoreFixture) error {
	// Stores of old monitors have no kv_backend, and ceph-mon rebuilds a
	// missing keyring from the store, so neither may be demanded.
	f.put(t, "store.db/CURRENT", "x")
	r := f.run(t, requireMode())
	if r.exitCode != 0 || hasCall(r.calls, "ceph-mon --mkfs") || !hasCall(r.calls, "ceph-mon -f") {
		return fmt.Errorf("old-shaped store: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	return nil
}

func scenarioRequireMissingStoreRefusedBeforeWrite(t *testing.T, f *requireStoreFixture) error {
	// The data directory is also absent: refusing must not even create it.
	if err := os.RemoveAll(f.dataDir); err != nil {
		t.Fatal(err)
	}
	before := f.tree(t)
	r := f.run(t, requireMode())
	if r.exitCode != 3 || len(r.calls) != 0 || !strings.Contains(r.output, "tc-ceph: store:") {
		return fmt.Errorf("missing directory: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	if !reflect.DeepEqual(before, f.tree(t)) {
		return errors.New("refusal wrote to the container filesystem")
	}
	// An empty (freshly provisioned) volume is the case that matters.
	if err := os.MkdirAll(f.dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	before = f.tree(t)
	r = f.run(t, requireMode())
	if r.exitCode != 3 || len(r.calls) != 0 || !strings.Contains(r.output, "no store.db") {
		return fmt.Errorf("empty volume: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	if !reflect.DeepEqual(before, f.tree(t)) {
		return errors.New("refusal on an empty volume wrote to the container filesystem")
	}
	return nil
}

// requireNonStoreContent lists data directories that are not a store. Each
// has an opposite name in the real mkfs flow: the default mode would format
// over several of these, which is exactly what require mode must not do.
var requireNonStoreContent = map[string]func(t *testing.T, f *requireStoreFixture){
	"only-kv-backend": func(t *testing.T, f *requireStoreFixture) { f.put(t, "kv_backend", "rocksdb\n") },
	"store.db-file": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "store.db")
		f.put(t, "store.db", "not a directory")
	},
	"store.db-symlink": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "store.db")
		if err := os.Symlink(f.root, filepath.Join(f.dataDir, "store.db")); err != nil {
			t.Fatal(err)
		}
	},
	"store.db-empty": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "store.db")
		if err := os.Mkdir(filepath.Join(f.dataDir, "store.db"), 0o700); err != nil {
			t.Fatal(err)
		}
	},
	"foreign-files": func(t *testing.T, f *requireStoreFixture) { f.put(t, "unrelated.txt", "not a monitor") },
	"kv_backend-other": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		f.put(t, "kv_backend", "leveldb\n")
	},
	"kv_backend-empty": func(t *testing.T, f *requireStoreFixture) { f.store(t); f.put(t, "kv_backend", "") },
	"kv_backend-directory": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "kv_backend")
		if err := os.Mkdir(filepath.Join(f.dataDir, "kv_backend"), 0o700); err != nil {
			t.Fatal(err)
		}
	},
	"kv_backend-symlink": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "kv_backend")
		f.put(t, "../rocksdb-target", "rocksdb\n")
		if err := os.Symlink("../rocksdb-target", filepath.Join(f.dataDir, "kv_backend")); err != nil {
			t.Fatal(err)
		}
	},
	"keyring-directory": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "keyring")
		if err := os.Mkdir(filepath.Join(f.dataDir, "keyring"), 0o700); err != nil {
			t.Fatal(err)
		}
	},
	"keyring-symlink": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "keyring")
		f.put(t, "../keyring-target", "[mon.]\n\tkey = synthetic\n")
		if err := os.Symlink("../keyring-target", filepath.Join(f.dataDir, "keyring")); err != nil {
			t.Fatal(err)
		}
	},
	"keyring-dangling-symlink": func(t *testing.T, f *requireStoreFixture) {
		f.store(t)
		rm(t, f, "keyring")
		if err := os.Symlink("missing", filepath.Join(f.dataDir, "keyring")); err != nil {
			t.Fatal(err)
		}
	},
}

func rm(t *testing.T, f *requireStoreFixture, rel string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(f.dataDir, rel)); err != nil {
		t.Fatal(err)
	}
}

func scenarioRequireNonStoreRefused(name string) requireStoreScenario {
	return func(t *testing.T, f *requireStoreFixture) error {
		if err := os.MkdirAll(f.dataDir, 0o700); err != nil {
			t.Fatal(err)
		}
		requireNonStoreContent[name](t, f)
		before := f.tree(t)
		r := f.run(t, requireMode())
		if r.exitCode != 3 || len(r.calls) != 0 || !strings.Contains(r.output, "tc-ceph: store:") {
			return fmt.Errorf("%s: exit=%d calls=%v output=%s", name, r.exitCode, r.calls, r.output)
		}
		if !reflect.DeepEqual(before, f.tree(t)) {
			return fmt.Errorf("%s: refusal wrote to the container filesystem", name)
		}
		return nil
	}
}

func scenarioRequireDirectoryIsFileOrLink(t *testing.T, f *requireStoreFixture) error {
	for _, kind := range []string{"file", "symlink"} {
		if err := os.RemoveAll(f.dataDir); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(f.dataDir), 0o700); err != nil {
			t.Fatal(err)
		}
		if kind == "file" {
			if err := os.WriteFile(f.dataDir, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		} else {
			// Even a link to a valid store is not the mounted data directory.
			target := filepath.Join(f.root, "elsewhere")
			if err := os.MkdirAll(filepath.Join(target, "store.db"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(target, "store.db/CURRENT"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, f.dataDir); err != nil {
				t.Fatal(err)
			}
		}
		r := f.run(t, requireMode())
		if r.exitCode != 3 || len(r.calls) != 0 || !strings.Contains(r.output, "is not a directory") {
			return fmt.Errorf("data directory as %s: exit=%d calls=%v output=%s", kind, r.exitCode, r.calls, r.output)
		}
	}
	return nil
}

func scenarioRequireUnknownModeRefused(t *testing.T, f *requireStoreFixture) error {
	// A usable store is present: only the mode value is wrong, and the
	// script must refuse before any command or write, not default to reuse
	// or to formatting.
	f.store(t)
	before := f.tree(t)
	for _, mode := range []string{"", "Require", "require ", "reuse", "false", "1"} {
		r := f.run(t, &mode)
		if r.exitCode != 2 || len(r.calls) != 0 || !strings.Contains(r.output, "unsupported CEPH_MON_STORE_MODE") {
			return fmt.Errorf("mode %q: exit=%d calls=%v output=%s", mode, r.exitCode, r.calls, r.output)
		}
	}
	if !reflect.DeepEqual(before, f.tree(t)) {
		return errors.New("refused mode wrote to the container filesystem")
	}
	return nil
}

// scenarioDefaultModeUnchanged covers the absent mode: a missing store is
// formatted exactly as before, and a present one (also a non-store-shaped
// one that require mode refuses) is started without mkfs.
func scenarioDefaultModeUnchanged(t *testing.T, f *requireStoreFixture) error {
	r := f.run(t, nil)
	if r.exitCode != 0 || !hasCall(r.calls, "ceph-mon --mkfs") || r.calls[len(r.calls)-1] != "ceph-mon -f -i "+strings.TrimPrefix(filepath.Base(f.dataDir), "ceph-") {
		return fmt.Errorf("default fresh: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	if _, err := os.Stat(filepath.Join(f.dataDir, "store.db")); err != nil {
		return fmt.Errorf("default fresh: no store after mkfs: %v", err)
	}
	// Second run: the store now exists, so there is no second format.
	r = f.run(t, nil)
	if r.exitCode != 0 || hasCall(r.calls, "ceph-mon --mkfs") || !hasCall(r.calls, "ceph-mon -f") {
		return fmt.Errorf("default restart: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	// The default mode keeps its permissive reading of store.db: an empty
	// directory is accepted as present, which require mode refuses.
	rm(t, f, "")
	if err := os.MkdirAll(filepath.Join(f.dataDir, "store.db"), 0o700); err != nil {
		t.Fatal(err)
	}
	r = f.run(t, nil)
	if r.exitCode != 0 || hasCall(r.calls, "ceph-mon --mkfs") {
		return fmt.Errorf("default empty store.db: exit=%d calls=%v output=%s", r.exitCode, r.calls, r.output)
	}
	return nil
}

type requireStoreMutation struct {
	name string
	// scenario must fail against the mutated script.
	scenario requireStoreScenario
	mutate   func(string) string
}

func replaceOnce(old, new string) func(string) string {
	return func(s string) string {
		if strings.Count(s, old) != 1 {
			return s
		}
		return strings.Replace(s, old, new, 1)
	}
}

// requireStoreCall returns the call line of require_store in the script.
func requireStoreCall(name string) string {
	if name == "mon-join" {
		return "require_store \"/var/lib/ceph/mon/ceph-$id\"\n"
	}
	return "require_store /var/lib/ceph/mon/ceph-a\n"
}

func requireStoreMutations(name string) []requireStoreMutation {
	mkfs, dataPath := "exec ceph-mon -f -i a", "/var/lib/ceph/mon/ceph-a"
	if name == "mon-join" {
		mkfs, dataPath = "exec ceph-mon -f -i \"$id\"", "\"/var/lib/ceph/mon/ceph-$id\""
	}
	return []requireStoreMutation{
		{"mode ignored", scenarioRequireMissingStoreRefusedBeforeWrite, replaceOnce(requireStoreCall(name), ":\n")},
		{"mode ignored for non-store content", scenarioRequireNonStoreRefused("foreign-files"), replaceOnce(requireStoreCall(name), ":\n")},
		{"check after the directory is created", scenarioRequireMissingStoreRefusedBeforeWrite, replaceOnce(requireStoreCall(name), "mkdir -p "+dataPath+"\n"+requireStoreCall(name))},
		{"missing store.db tolerated", scenarioRequireMissingStoreRefusedBeforeWrite, replaceOnce(`[ -L "$store_dir/store.db" ] || [ ! -d "$store_dir/store.db" ]`, "false")},
		{"missing data directory tolerated", scenarioRequireDirectoryIsFileOrLink, replaceOnce(`[ -L "$store_dir" ] || [ ! -d "$store_dir" ]`, "false")},
		{"store.db symlink followed", scenarioRequireNonStoreRefused("store.db-symlink"), replaceOnce(`[ -L "$store_dir/store.db" ] || [ ! -d`, `[ ! -d`)},
		{"data directory symlink followed", scenarioRequireDirectoryIsFileOrLink, replaceOnce(`[ -L "$store_dir" ] || [ ! -d`, `[ ! -d`)},
		{"empty store.db accepted", scenarioRequireNonStoreRefused("store.db-empty"), replaceOnce(`[ -z "$(ls -A "$store_dir/store.db" 2>/dev/null)" ]`, "false")},
		{"kv_backend content ignored", scenarioRequireNonStoreRefused("kv_backend-other"), replaceOnce(` || [ "$(cat "$store_dir/kv_backend")" != rocksdb ]`, "")},
		{"kv_backend symlink followed", scenarioRequireNonStoreRefused("kv_backend-symlink"), replaceOnce(`[ -L "$store_dir/kv_backend" ] || [ ! -f`, `[ ! -f`)},
		{"keyring type ignored", scenarioRequireNonStoreRefused("keyring-directory"), replaceOnce(` || [ ! -f "$store_dir/keyring" ]`, "")},
		{"keyring symlink followed", scenarioRequireNonStoreRefused("keyring-symlink"), replaceOnce(`[ -L "$store_dir/keyring" ] || [ ! -f`, `[ ! -f`)},
		{"optional kv_backend demanded", scenarioRequireOldStoreWithoutOptionalFiles, replaceOnce(`if [ -e "$store_dir/kv_backend" ] || [ -L "$store_dir/kv_backend" ]; then`, `if true; then`)},
		{"unknown mode falls back to the default", scenarioRequireUnknownModeRefused, replaceOnce(`if [ "$CEPH_MON_STORE_MODE" != require ]; then`, `if false; then`)},
		{"empty mode treated as unset", scenarioRequireUnknownModeRefused, replaceOnce(`[ "${CEPH_MON_STORE_MODE+set}" = set ]`, `[ -n "${CEPH_MON_STORE_MODE:-}" ]`)},
		{"reuse formats anyway", scenarioRequirePresentStoreReused, replaceOnce(mkfs, "ceph-mon --mkfs -i x\n"+mkfs)},
		{"default mode requires a store", scenarioDefaultModeUnchanged, replaceOnce(`[ "${CEPH_MON_STORE_MODE+set}" = set ] || return 0`, `CEPH_MON_STORE_MODE=${CEPH_MON_STORE_MODE:-require}`)},
	}
}

var requireStoreScripts = []string{"mon", "mon-join"}

func TestMonRequireStoreScripts(t *testing.T) {
	scenarios := map[string]requireStoreScenario{
		"present store is reused without mkfs":              scenarioRequirePresentStoreReused,
		"old-shaped store without optional files is reused": scenarioRequireOldStoreWithoutOptionalFiles,
		"missing store is refused before any write":         scenarioRequireMissingStoreRefusedBeforeWrite,
		"data directory that is a file or link is refused":  scenarioRequireDirectoryIsFileOrLink,
		"unknown or empty mode is refused":                  scenarioRequireUnknownModeRefused,
		"default mode formats a missing store and restarts": scenarioDefaultModeUnchanged,
	}
	for name := range requireNonStoreContent {
		scenarios["non-store content refused: "+name] = scenarioRequireNonStoreRefused(name)
	}
	for _, script := range requireStoreScripts {
		for name, scenario := range scenarios {
			t.Run(script+"/"+name, func(t *testing.T) {
				if err := scenario(t, newRequireStoreFixture(t, script, nil)); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestMonRequireStoreNegativeControls breaks each script on purpose and
// requires the matching scenario to notice.
func TestMonRequireStoreNegativeControls(t *testing.T) {
	for _, script := range requireStoreScripts {
		for _, m := range requireStoreMutations(script) {
			t.Run(script+"/"+m.name, func(t *testing.T) {
				f := newRequireStoreFixture(t, script, m.mutate)
				if err := m.scenario(t, f); err == nil {
					t.Fatal("the scenario passed against a broken script; it does not guard the behavior")
				}
			})
		}
	}
}

// TestMonRequireStoreBlocksAreIdentical keeps the two scripts' copies of the
// shared function from drifting apart.
func TestMonRequireStoreBlocksAreIdentical(t *testing.T) {
	blocks := map[string]string{}
	for _, name := range requireStoreScripts {
		data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		start := strings.Index(text, "# BEGIN mon-store-mode")
		end := strings.Index(text, "# END mon-store-mode")
		if start < 0 || end < start || strings.Count(text, "# BEGIN mon-store-mode") != 1 {
			t.Fatalf("%s.sh: missing or repeated mon-store-mode block", name)
		}
		blocks[name] = text[start:end]
	}
	if blocks["mon"] != blocks["mon-join"] {
		t.Fatal("mon.sh and mon-join.sh carry different require_store blocks")
	}
}

// TestMonRequireStoreBlockDriftControl proves the identity check can fail.
func TestMonRequireStoreBlockDriftControl(t *testing.T) {
	data, err := scripts.ReadFile("internal/scripts/mon.sh")
	if err != nil {
		t.Fatal(err)
	}
	a := string(data)
	b := strings.Replace(a, "exit 3", "exit 4", 1)
	if a == b {
		t.Fatal("control did not change the block")
	}
	extract := func(s string) string {
		return s[strings.Index(s, "# BEGIN mon-store-mode"):strings.Index(s, "# END mon-store-mode")]
	}
	if extract(a) == extract(b) {
		t.Fatal("drift between the copies would go unnoticed")
	}
}
