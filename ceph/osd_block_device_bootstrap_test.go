package ceph

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// The block device launch arguments are exercised against the real osd.sh with
// a stubbed stat, a regular file in place of the device node and a synthetic
// sysfs size. No device, container or Ceph daemon is involved, so these cases
// show the script's decisions only; they say nothing about a kernel device.

const osdBlockFixtureBytes = 128 << 20

func newOSDBlockFixture(t *testing.T) *osdBootstrapFixture {
	t.Helper()
	f := newOSDBootstrapFixture(t)
	source, err := os.ReadFile(f.script)
	if err != nil {
		t.Fatal(err)
	}
	rewritten := strings.NewReplacer("/dev/tc-osd-block", f.root+"/dev/tc-osd-block", "/sys/dev/block", f.root+"/sys/dev/block").Replace(string(source))
	if err := os.WriteFile(f.script, []byte(rewritten), 0o600); err != nil {
		t.Fatal(err)
	}
	f.write(t, "/dev/tc-osd-block", "synthetic-node")
	f.write(t, "/sys/dev/block/253:4/size", strconv.Itoa(osdBlockFixtureBytes/512)+"\n")
	stat := `#!/bin/sh
printf '%s\n' "$*" >> "$TC_STAT_CALLS"
case "$*" in
 "-L -c %F "*) printf '%s\n' "$TC_STAT_KIND" ;;
 "-L -c %t %T "*) printf '%s\n' "$TC_STAT_RDEV" ;;
 *) exit 2 ;;
esac
`
	if err := os.WriteFile(filepath.Join(f.root, "bin/stat"), []byte(stat), 0o700); err != nil {
		t.Fatal(err)
	}
	f.env["TC_STAT_CALLS"] = filepath.Join(f.root, "stat-calls")
	f.env["TC_STAT_KIND"] = "block special file"
	f.env["TC_STAT_RDEV"] = "fd 4" // hexadecimal 0xfd:0x4 is 253:4
	f.env["CEPH_OSD_BLOCK_DEVICE"] = f.root + "/dev/tc-osd-block"
	f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "253:4"
	f.env["CEPH_OSD_BLOCK_DEVICE_BYTES"] = strconv.Itoa(osdBlockFixtureBytes)
	return f
}

func TestOSDBootstrapMapsAVerifiedBlockDeviceOnlyIntoMkfs(t *testing.T) {
	f := newOSDBlockFixture(t)
	calls, out, err := f.run(t)
	if err != nil || len(calls) != 2 {
		t.Fatalf("fresh block launch: calls=%v err=%v output=%s", calls, err, out)
	}
	want := "--bluestore_block_path=" + f.root + "/dev/tc-osd-block --no-bluestore_block_create --bluestore_block_size=0"
	if !strings.HasPrefix(calls[0], "--mkfs ") || !strings.HasSuffix(calls[0], " "+want) || strings.Contains(calls[1], "bluestore") {
		t.Fatalf("block arguments must reach mkfs and only mkfs: %v", calls)
	}
}

func TestOSDBootstrapWithoutABlockDeviceKeepsTheSparseFileLaunch(t *testing.T) {
	f := newOSDBlockFixture(t)
	for _, k := range []string{"CEPH_OSD_BLOCK_DEVICE", "CEPH_OSD_BLOCK_DEVICE_ID", "CEPH_OSD_BLOCK_DEVICE_BYTES"} {
		delete(f.env, k)
	}
	calls, out, err := f.run(t)
	if err != nil || len(calls) != 2 || strings.Contains(strings.Join(calls, "\n"), "bluestore") {
		t.Fatalf("ordinary launch changed: calls=%v err=%v output=%s", calls, err, out)
	}
}

func TestOSDBootstrapRefusesAnUnverifiedBlockDeviceBeforeWriting(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, *osdBootstrapFixture){
		"not-a-block-node": func(_ *testing.T, f *osdBootstrapFixture) { f.env["TC_STAT_KIND"] = "regular file" },
		"other-minor":      func(_ *testing.T, f *osdBootstrapFixture) { f.env["TC_STAT_RDEV"] = "fd 5" },
		"other-major":      func(_ *testing.T, f *osdBootstrapFixture) { f.env["TC_STAT_RDEV"] = "fc 4" },
		"unreadable-rdev":  func(_ *testing.T, f *osdBootstrapFixture) { f.env["TC_STAT_RDEV"] = "fd" },
		"extra-rdev-field": func(_ *testing.T, f *osdBootstrapFixture) { f.env["TC_STAT_RDEV"] = "fd 4 9" },
		"other-size": func(_ *testing.T, f *osdBootstrapFixture) {
			f.env["CEPH_OSD_BLOCK_DEVICE_BYTES"] = strconv.Itoa(osdBlockFixtureBytes + 512)
		},
		"missing-sys-size": func(t *testing.T, f *osdBootstrapFixture) { removeAll(t, filepath.Join(f.root, "sys/dev/block/253:4")) },
		"empty-sys-size":   func(t *testing.T, f *osdBootstrapFixture) { f.write(t, "/sys/dev/block/253:4/size", "\n") },
		"junk-sys-size":    func(t *testing.T, f *osdBootstrapFixture) { f.write(t, "/sys/dev/block/253:4/size", "12k\n") },
		"foreign-path":     func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE"] = "/dev/sda" },
		"missing-number":   func(_ *testing.T, f *osdBootstrapFixture) { delete(f.env, "CEPH_OSD_BLOCK_DEVICE_ID") },
		"no-colon":         func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "253" },
		"three-fields":     func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "253:4:1" },
		"zero-major":       func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "0:4" },
		"empty-minor":      func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "253:" },
		"non-numeric":      func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_ID"] = "253:x" },
		"missing-size":     func(_ *testing.T, f *osdBootstrapFixture) { delete(f.env, "CEPH_OSD_BLOCK_DEVICE_BYTES") },
		"zero-size":        func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_BYTES"] = "0" },
		"signed-size":      func(_ *testing.T, f *osdBootstrapFixture) { f.env["CEPH_OSD_BLOCK_DEVICE_BYTES"] = "+134217728" },
		"symlinked-node": func(t *testing.T, f *osdBootstrapFixture) {
			path := filepath.Join(f.root, "dev/tc-osd-block")
			removeAll(t, path)
			if err := os.Symlink(filepath.Join(f.root, "elsewhere"), path); err != nil {
				t.Fatal(err)
			}
		},
		"existing-ready-store": func(_ *testing.T, f *osdBootstrapFixture) {},
	} {
		t.Run(name, func(t *testing.T) {
			f := newOSDBlockFixture(t)
			if name == "existing-ready-store" {
				f.ready(t)
			}
			mutate(t, f)
			before := f.snapshot(t)
			calls, out, err := f.run(t)
			if err == nil || len(calls) != 0 || !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatalf("unverified device admitted or store changed: calls=%v err=%v output=%s", calls, err, out)
			}
		})
	}
}

func removeAll(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
}
