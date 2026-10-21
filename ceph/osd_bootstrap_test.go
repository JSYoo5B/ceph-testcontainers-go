package ceph

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const bootstrapOSDUUID = "21c0da1e-8e35-49d2-ad3f-228831c409e2"
const bootstrapClusterFSID = "64db847c-58fe-44eb-a129-8e713ff2c305"

type osdBootstrapFixture struct {
	root, store, script, calls string
	env                        map[string]string
}

func newOSDBootstrapFixture(t *testing.T) *osdBootstrapFixture {
	t.Helper()
	root := t.TempDir()
	f := &osdBootstrapFixture{root: root, store: filepath.Join(root, "var/lib/ceph/osd/ceph-7"), script: filepath.Join(root, "osd.sh"), calls: filepath.Join(root, "calls")}
	f.env = map[string]string{"CEPH_OSD_ID": "7", "CEPH_OSD_UUID": bootstrapOSDUUID, "CEPH_OSD_HOST": "synthetic-osd-host", "TC_FSID": bootstrapClusterFSID, "TC_CALLS": f.calls, "TC_STORE": f.store, "TC_FAIL_MKFS": "0", "TC_FAIL_CONFIG": "0"}
	for _, dir := range []string{f.store, filepath.Join(root, "bin"), filepath.Join(root, "etc/ceph")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	f.write(t, "/etc/ceph/osd.keyring", "synthetic-bootstrap-key")
	data, err := scripts.ReadFile("internal/scripts/osd.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, prefix := range []string{"/var/lib/ceph", "/var/run/ceph", "/etc/ceph"} {
		source = strings.ReplaceAll(source, prefix, root+prefix)
	}
	if err := os.WriteFile(f.script, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	stub := `#!/bin/sh
printf '%s\n' "$*" >> "$TC_CALLS"
case "$*" in
 *--show-config-value*)
    [ "$TC_FAIL_CONFIG" = 0 ] || exit 25
    printf '%s\n' "$TC_FSID"
    ;;
 --mkfs*)
    [ "$TC_FAIL_MKFS" = 0 ] || exit 23
    printf '%s\n' "$CEPH_OSD_ID" > "$TC_STORE/whoami"
    printf '%s\n' "$CEPH_OSD_UUID" > "$TC_STORE/fsid"
    printf '%s\n' "$TC_FSID" > "$TC_STORE/ceph_fsid"
    printf 'ready\n' > "$TC_STORE/ready"
    ;;
esac
`
	if err := os.WriteFile(filepath.Join(root, "bin/ceph-osd"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *osdBootstrapFixture) write(t *testing.T, path, data string) {
	t.Helper()
	full := filepath.Join(f.root, strings.TrimPrefix(path, "/"))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}
func (f *osdBootstrapFixture) ready(t *testing.T) {
	t.Helper()
	for name, value := range map[string]string{"ready": "ready\n", "whoami": "7\n", "fsid": bootstrapOSDUUID + "\n", "ceph_fsid": bootstrapClusterFSID + "\n", "keyring": "synthetic-existing-key", "block": "synthetic-store-sentinel"} {
		f.write(t, "/var/lib/ceph/osd/ceph-7/"+name, value)
	}
}
func (f *osdBootstrapFixture) run(t *testing.T) ([]string, string, error) {
	t.Helper()
	if err := os.Remove(f.calls); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "/bin/sh", f.script)
	cmd.Env = []string{"PATH=" + filepath.Join(f.root, "bin") + string(os.PathListSeparator) + os.Getenv("PATH")}
	for k, v := range f.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	data, readErr := os.ReadFile(f.calls)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	var calls []string
	if len(data) > 0 {
		calls = strings.Split(strings.TrimSpace(string(data)), "\n")
	}
	return calls, string(out), err
}
func (f *osdBootstrapFixture) snapshot(t *testing.T) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.Walk(f.store, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(f.store, path)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			result[rel] = "link:" + target
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			result[rel] = fmt.Sprintf("%o:%s", info.Mode().Perm(), data)
		case info.IsDir():
			result[rel] = "directory"
		default:
			return fmt.Errorf("unsupported fixture entry %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestOSDBootstrapFreshAndMatchingRestart(t *testing.T) {
	f := newOSDBootstrapFixture(t)
	calls, out, err := f.run(t)
	if err != nil || len(calls) != 2 || !strings.HasPrefix(calls[0], "--mkfs ") || !strings.HasPrefix(calls[1], "-f ") {
		t.Fatalf("fresh: calls=%v err=%v output=%s", calls, err, out)
	}
	before := f.snapshot(t)
	calls, out, err = f.run(t)
	if err != nil || len(calls) != 2 || !strings.Contains(calls[0], "--show-config-value fsid") || !strings.HasPrefix(calls[1], "-f ") {
		t.Fatalf("restart: calls=%v err=%v output=%s", calls, err, out)
	}
	if !reflect.DeepEqual(before, f.snapshot(t)) {
		t.Fatal("matching restart changed store files")
	}
}

func TestOSDBootstrapRefusesNonEmptyUnreadyStoreBeforeWriting(t *testing.T) {
	for _, entry := range []string{"block", "keyring", ".hidden", "..hidden", "dangling", "ready-directory"} {
		t.Run(entry, func(t *testing.T) {
			f := newOSDBootstrapFixture(t)
			switch entry {
			case "dangling":
				if err := os.Symlink("missing", filepath.Join(f.store, entry)); err != nil {
					t.Fatal(err)
				}
			case "ready-directory":
				if err := os.Mkdir(filepath.Join(f.store, "ready"), 0o700); err != nil {
					t.Fatal(err)
				}
			default:
				f.write(t, "/var/lib/ceph/osd/ceph-7/"+entry, "synthetic-existing-content")
			}
			before := f.snapshot(t)
			calls, out, err := f.run(t)
			if err == nil || len(calls) != 0 || !strings.Contains(out, "non-empty OSD data directory") {
				t.Fatalf("unsafe initialization: calls=%v err=%v output=%s", calls, err, out)
			}
			if !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("refused bootstrap changed existing files")
			}
		})
	}
}

func TestOSDBootstrapRefusesMismatchedReadyIdentity(t *testing.T) {
	for _, field := range []string{"whoami", "fsid", "ceph_fsid", "missing-whoami", "missing-fsid", "missing-ceph_fsid", "missing-keyring", "empty-whoami", "empty-fsid", "empty-ceph_fsid", "empty-keyring", "symlink-whoami", "symlink-fsid", "symlink-ceph_fsid", "symlink-keyring", "symlink-ready", "config-error", "config-invalid"} {
		t.Run(field, func(t *testing.T) {
			f := newOSDBootstrapFixture(t)
			f.ready(t)
			switch field {
			case "missing-whoami", "missing-fsid", "missing-ceph_fsid", "missing-keyring":
				if err := os.Remove(filepath.Join(f.store, strings.TrimPrefix(field, "missing-"))); err != nil {
					t.Fatal(err)
				}
			case "empty-whoami", "empty-fsid", "empty-ceph_fsid", "empty-keyring":
				f.write(t, "/var/lib/ceph/osd/ceph-7/"+strings.TrimPrefix(field, "empty-"), "")
			case "symlink-whoami", "symlink-fsid", "symlink-ceph_fsid", "symlink-keyring", "symlink-ready":
				name := strings.TrimPrefix(field, "symlink-")
				path := filepath.Join(f.store, name)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				f.write(t, "/external-"+name, string(data))
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(f.root, "external-"+name), path); err != nil {
					t.Fatal(err)
				}
			case "config-error":
				f.env["TC_FAIL_CONFIG"] = "1"
			case "config-invalid":
				f.env["TC_FSID"] = "not-a-uuid"
			default:
				f.write(t, "/var/lib/ceph/osd/ceph-7/"+field, "foreign-identity")
			}
			before := f.snapshot(t)
			calls, out, err := f.run(t)
			if err == nil {
				t.Fatalf("foreign/incomplete store admitted: %v %s", calls, out)
			}
			for _, call := range calls {
				if !strings.Contains(call, "--show-config-value fsid") {
					t.Fatalf("mutating bootstrap call on refused store: %v", calls)
				}
			}
			if !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("identity refusal changed store bytes/permissions/links")
			}
		})
	}
}

func TestOSDBootstrapRefusesInvalidLaunchBeforeWriting(t *testing.T) {
	for _, test := range []struct{ key, value string }{{"CEPH_OSD_ID", "../foreign"}, {"CEPH_OSD_ID", ""}, {"CEPH_OSD_ID", "-7"}, {"CEPH_OSD_UUID", ""}, {"CEPH_OSD_UUID", "00000000-0000-0000-0000-000000000000"}, {"CEPH_OSD_UUID", "ffffffff-ffff-ffff-ffff-ffffffffffff-extra"}, {"CEPH_OSD_UUID", "bad-uuid"}, {"CEPH_OSD_UUID", bootstrapOSDUUID + "-"}, {"CEPH_OSD_UUID", "-" + bootstrapOSDUUID}} {
		t.Run(test.key+"/"+test.value, func(t *testing.T) {
			f := newOSDBootstrapFixture(t)
			f.env[test.key] = test.value
			before := f.snapshot(t)
			calls, out, err := f.run(t)
			if err == nil || len(calls) != 0 || !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatalf("invalid launch changed state: calls=%v err=%v output=%s", calls, err, out)
			}
		})
	}
}

func TestOSDBootstrapDoesNotReplayFailedInitialization(t *testing.T) {
	f := newOSDBootstrapFixture(t)
	f.env["TC_FAIL_MKFS"] = "1"
	calls, _, err := f.run(t)
	if err == nil || len(calls) != 1 || !strings.HasPrefix(calls[0], "--mkfs ") {
		t.Fatalf("failing initialization: %v %v", calls, err)
	}
	before := f.snapshot(t)
	f.env["TC_FAIL_MKFS"] = "0"
	calls, out, err := f.run(t)
	if err == nil || len(calls) != 0 || !strings.Contains(out, "non-empty OSD data directory") || !reflect.DeepEqual(before, f.snapshot(t)) {
		t.Fatalf("replayed partial initialization: %v %v %s", calls, err, out)
	}
}

func TestOSDBootstrapRefusesForeignDataPath(t *testing.T) {
	for _, kind := range []string{"symlink", "file"} {
		t.Run(kind, func(t *testing.T) {
			f := newOSDBootstrapFixture(t)
			if err := os.Remove(f.store); err != nil {
				t.Fatal(err)
			}
			foreign := filepath.Join(f.root, "foreign")
			if err := os.Mkdir(foreign, 0o700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(foreign, "keyring")
			if err := os.WriteFile(sentinel, []byte("foreign-key-sentinel"), 0o600); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				if err := os.Symlink(foreign, f.store); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(f.store, []byte("foreign-file-sentinel"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			calls, out, err := f.run(t)
			if err == nil || len(calls) != 0 {
				t.Fatalf("foreign path accepted: calls=%v err=%v output=%s", calls, err, out)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "foreign-key-sentinel" {
				t.Fatal("foreign path was changed")
			}
		})
	}
}
