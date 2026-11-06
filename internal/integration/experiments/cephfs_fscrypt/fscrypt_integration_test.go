//go:build (all || integration) && ((cephfs_fscrypt && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fscrypt)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs

// The client containers mount CephFS with the host kernel or with ceph-fuse,
// so they need CAP_SYS_ADMIN, an unconfined AppArmor profile and, for the
// kernel client, a kernel that has the ceph filesystem type registered
// already; no required CI environment is proven to provide them. This is an
// optional batch: build it with the cephfs_fscrypt tag (go test -tags
// all,cephfs_fscrypt) or run it with tag_scenarios.py optional --batch
// cephfs_fscrypt.

package cephfs_fscrypt_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// fscryptCluster is the case's cluster: one filesystem with one active MDS
// and the fixture's admin client container.
type fscryptCluster struct {
	cluster *ceph.Container
	admin   testcontainers.Container
	fs      *cephfs.Filesystem
	mds     string
	version string
}

func startFscryptCluster(t *testing.T, ctx context.Context) *fscryptCluster {
	t.Helper()
	image, opts := harness.IntegrationImages(t)
	// The kernel client decodes only AES Cephx keys and tickets; Tentacle
	// 20.2.4 and later default to aes256k.
	opts = append(opts, ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES}))
	cluster, admin := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, opts...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("want one active MDS: %+v, %v", status, err)
	}
	return &fscryptCluster{cluster: cluster, admin: admin, fs: fs, mds: status.Active[0].Name, version: harness.StaticFacts(cluster).CephVersion}
}

// clientImage returns the image of path's client containers: the cluster's
// client image for the kernel client, which needs only python3, and for
// ceph-fuse the client image when it ships ceph-fuse, otherwise the
// official image of the cluster's release, which does.
func (c *fscryptCluster) clientImage(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	image := harness.StaticFacts(c.cluster).Images.Client
	if path == kernelPath {
		return image
	}
	if code, _, err := harness.SessionExec(ctx, c.admin, "test", "-x", "/usr/bin/ceph-fuse"); err != nil {
		t.Fatal(err)
	} else if code == 0 {
		return image
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(c.version) {
		t.Fatalf("client image %s has no ceph-fuse, and Ceph version %q names no official image", image, c.version)
	}
	return "quay.io/ceph/ceph:v" + c.version
}

// rados runs rados in the admin container and returns its standard output.
func (c *fscryptCluster) rados(t *testing.T, ctx context.Context, args ...string) []byte {
	t.Helper()
	result, err := c.cluster.RunNativeCommand(ctx, c.admin, ceph.NativeCommand{
		Argv: append([]string{"rados"}, args...), Timeout: 2 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "rados " + args[len(args)-2],
	})
	if err != nil || result.ExitCode != 0 || result.StdoutTruncated {
		t.Fatalf("rados %s: exit=%d err=%v truncated=%t stderr=%.1024s", strings.Join(args, " "), result.ExitCode, err, result.StdoutTruncated, result.Stderr)
	}
	return result.Stdout
}

// fscryptClient is one client container of the case with its own CephX
// identity and, once mounted, its own mount of the filesystem's root.
type fscryptClient struct {
	path     string
	name     string
	c        *fscryptCluster
	ctr      testcontainers.Container
	identity *ceph.ClientConfig
	point    string
	fuse     *fuseProcess
	mounted  bool
	// outstanding is a ceph-fuse process whose call had not returned when
	// the container was still running; it is stopped again after the
	// container is terminated.
	outstanding *fuseProcess
}

// startFscryptClient starts an idle client container for path with a fresh
// identity that may read, write and snapshot the whole filesystem, with
// every MDS permission for ceph-fuse. A
// privileged container gets CAP_SYS_ADMIN, apparmor=unconfined and, for
// ceph-fuse, /dev/fuse; the cluster's daemons and admin container keep
// their defaults. Cleanup unmounts what is still mounted, stops ceph-fuse
// and terminates the container. Each step has its own bound, so a step that
// runs out of time does not leave the next one without any, and a ceph-fuse
// call that has not returned by then is stopped again once the container is
// terminated.
func startFscryptClient(t *testing.T, ctx context.Context, c *fscryptCluster, path, name, image string, privileged bool) *fscryptClient {
	t.Helper()
	// ceph-fuse sets a policy through the ceph.fscrypt.auth and
	// ceph.fscrypt.file virtual xattrs, and the second one becomes a
	// synchronous setattr request. The 21.2.0 MDS answers a setattr request
	// that changes an fscrypt field with EACCES unless the session's MDS caps
	// are "allow *". The kernel client applies the policy under its
	// exclusive auth caps and flushes it with the caps, so "allow rws" is
	// enough for it.
	mds := "allow rws"
	if path == fusePath {
		mds = "allow *"
	}
	identity, err := c.cluster.CreateClient(ctx, "fscrypt-"+path+"-"+name, ceph.ClientCaps{
		Mon: "allow r", MDS: mds, OSD: "allow rw tag cephfs data=" + c.fs.FilesystemName,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := []testcontainers.ContainerCustomizer{identity.WithIdentity(), ceph.WithIdleEntrypoint()}
	if privileged {
		opts = append(opts, testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			if path == fusePath {
				host.Devices = append(host.Devices, container.DeviceMapping{PathOnHost: "/dev/fuse", PathInContainer: "/dev/fuse", CgroupPermissions: "rwm"})
			}
			host.CapAdd = append(host.CapAdd, "SYS_ADMIN")
			host.SecurityOpt = append(host.SecurityOpt, "apparmor=unconfined")
		}))
	}
	f := &fscryptClient{path: path, name: name, c: c, identity: identity, point: "/mnt/cephfs"}
	// Registered before the container's cleanup, so it runs after the
	// container was terminated.
	t.Cleanup(func() {
		if f.outstanding == nil {
			return
		}
		if err := f.outstanding.stop(time.Minute); err != nil {
			t.Errorf("ceph-fuse of %s after its container was terminated: %v", name, err)
		}
	})
	ctr, err := testcontainers.Run(ctx, image, opts...)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	f.ctr = ctr
	t.Cleanup(func() {
		if f.mounted {
			if err := f.emergencyUnmount(umountDetach); err != nil {
				t.Errorf("cleanup of client %s: %v", name, err)
			}
		}
		if f.fuse != nil {
			err := f.fuse.stop(time.Minute)
			if errors.Is(err, errFuseOutstanding) {
				f.outstanding = f.fuse
			}
			if err != nil {
				t.Errorf("ceph-fuse of %s: %v", name, err)
			}
		}
	})
	return f
}

// umountDetach is umount2's MNT_DETACH flag: a lazy unmount.
const umountDetach = "2"

// emergencyUnmount unmounts the container's mount point with umount2 and
// flags, within its own bound, in a cleanup after a failure, and checks in
// the container's mount table that nothing is mounted below the point any
// more.
func (f *fscryptClient) emergencyUnmount(flags string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var umount, listing probeExec
	umount.Code, umount.Output, umount.Err = harness.SessionExec(ctx, f.ctr, "python3", "-c", fscryptProbe, "umount", f.point, flags)
	listing.Code, listing.Output, listing.Err = harness.SessionExec(ctx, f.ctr, "python3", "-c", fscryptProbe, "mounts", f.point)
	return emergencyUnmountError(f.point, umount, listing)
}

// probe runs one fscryptProbe command in the client container, with input
// as its standard input when it is not nil, and decodes its JSON result.
func (f *fscryptClient) probe(t *testing.T, ctx context.Context, value any, input []byte, args ...string) {
	t.Helper()
	command := ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", fscryptProbe}, args...), Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "fscrypt probe " + args[0],
	}
	if input != nil {
		in := ceph.NewNativeInput()
		command.Input, command.MaxInput = in, len(input)
		go func() {
			_ = in.Write(ctx, input)
			_ = in.Close()
		}()
	}
	result, err := f.c.cluster.RunNativeCommand(ctx, f.ctr, command)
	if err != nil || result.ExitCode != 0 || json.Unmarshal([]byte(harness.LastJSONLine(string(result.Stdout))), value) != nil {
		t.Fatalf("fscrypt probe %s in %s client %s: exit=%d err=%v stdout=%.2048s stderr=%.2048s", args[0], f.path, f.name, result.ExitCode, err, result.Stdout, result.Stderr)
	}
}

func (f *fscryptClient) admit(t *testing.T, ctx context.Context) (fscryptAdmission, *fscryptRefusal) {
	t.Helper()
	var admission fscryptAdmission
	f.probe(t, ctx, &admission, nil, "admit")
	return admission, admission.refusal(f.path)
}

// mountPoints returns the mounts of the container below its mount point,
// as mount point and filesystem type.
func (f *fscryptClient) mountPoints(t *testing.T, ctx context.Context) [][]string {
	t.Helper()
	var listing struct {
		Mounts [][]string `json:"mounts"`
	}
	f.probe(t, ctx, &listing, nil, "mounts", f.point)
	return listing.Mounts
}

// mount mounts the filesystem's root on the container's mount point, after
// an admission that refused nothing, and waits, bounded, until the mount is
// in the container's mount table. A refused mount(2) whose errno names a
// missing privilege is returned as a refusal; any other failure fails the
// test.
func (f *fscryptClient) mount(t *testing.T, ctx context.Context, admission fscryptAdmission) *fscryptRefusal {
	t.Helper()
	fstype := "ceph"
	switch f.path {
	case kernelPath:
		var mounted struct {
			Errno    int    `json:"errno"`
			Monitors int    `json:"monitors"`
			KeyType  int    `json:"key_type"`
			Error    string `json:"error"`
		}
		f.probe(t, ctx, &mounted, nil, "kmount", f.identity.User(), f.c.fs.FilesystemName, "/", f.point, "")
		if mounted.KeyType != 1 {
			return &fscryptRefusal{Path: f.path, Requirement: fscryptNeedsAESKey, Detail: fmt.Sprintf("client %s has a Cephx key of type %d; the kernel client decodes only AES (1) keys", f.name, mounted.KeyType)}
		}
		if mounted.Errno != 0 {
			if refusal := mountRefusal(f.path, mounted.Errno, admission); refusal != nil {
				return refusal
			}
			t.Fatalf("kernel mount of %s by client %s: errno %d %s (%d MONs)", f.c.fs.FilesystemName, f.name, mounted.Errno, mounted.Error, mounted.Monitors)
		}
	case fusePath:
		fstype = "fuse.ceph-fuse"
		harness.ExecOutput(t, ctx, f.ctr, "mkdir", "-p", f.point)
		mountCtx, cancel := context.WithCancel(ctx)
		process := &fuseProcess{cancel: cancel, done: make(chan struct{})}
		f.fuse = process
		argv := []string{"ceph-fuse", "-f", "--id", f.identity.User(), "--client_fs", f.c.fs.FilesystemName, "--client_mount_timeout", "30", "-r", "/", f.point}
		go func() {
			defer close(process.done)
			process.result, process.err = f.c.cluster.RunNativeCommand(mountCtx, f.ctr, ceph.NativeCommand{
				Argv: argv, Timeout: 20 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 256 << 10, Label: "ceph-fuse",
			})
		}()
	}
	f.mounted = true
	seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, 500*time.Millisecond, func(context.Context) ([][]string, bool, error) {
		if f.fuse != nil {
			select {
			case <-f.fuse.done:
				return nil, false, fmt.Errorf("ceph-fuse ended: exit=%d err=%v stderr=%.2048s", f.fuse.result.ExitCode, f.fuse.err, f.fuse.result.Stderr)
			default:
			}
		}
		mounts := f.mountPoints(t, ctx)
		return mounts, slices.ContainsFunc(mounts, func(m []string) bool { return m[0] == f.point && m[1] == fstype }), nil
	})
	if err != nil {
		t.Fatalf("%s mount of client %s not in the mount table: %v: %v", f.path, f.name, seen.Value, err)
	}
	return nil
}

// umount unmounts the container's mount point and waits, bounded, until the
// container's mount table, the ceph-fuse process and the MDS have let go of
// the mount.
func (f *fscryptClient) umount(t *testing.T, ctx context.Context) {
	t.Helper()
	var unmounted struct {
		Errno int `json:"errno"`
	}
	f.probe(t, ctx, &unmounted, nil, "umount", f.point, "0")
	if unmounted.Errno != 0 {
		t.Fatalf("umount of client %s: errno %d", f.name, unmounted.Errno)
	}
	if f.fuse != nil {
		select {
		case <-f.fuse.done:
		case <-time.After(time.Minute):
			t.Fatalf("ceph-fuse of client %s did not return after umount", f.name)
		}
		if f.fuse.err != nil || f.fuse.result.ExitCode != 0 {
			t.Fatalf("ceph-fuse of client %s after umount: exit=%d err=%v", f.name, f.fuse.result.ExitCode, f.fuse.err)
		}
		f.fuse = nil
	}
	f.mounted = false
	if mounts := f.mountPoints(t, ctx); len(mounts) != 0 {
		t.Fatalf("client %s still has mounts %v", f.name, mounts)
	}
	f.sessionsGone(t, ctx)
}

// sessionsGone waits, bounded, until the MDS has no session of f's identity.
func (f *fscryptClient) sessionsGone(t *testing.T, ctx context.Context) {
	t.Helper()
	seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]cephfssession.Session, bool, error) {
		sessions, err := cephfssession.List(probeCtx, f.c.fs)
		var own []cephfssession.Session
		for _, session := range sessions {
			if session.EntityID == f.identity.User() {
				own = append(own, session)
			}
		}
		return own, err == nil && len(own) == 0, err
	})
	if err != nil {
		t.Fatalf("MDS sessions of client %s left: %+v: %v", f.name, seen.Value, err)
	}
}

type fscryptErrno struct {
	Errno int `json:"errno"`
}

type fscryptPolicy struct {
	Errno      int    `json:"errno"`
	Version    int    `json:"version"`
	Contents   int    `json:"contents"`
	Filenames  int    `json:"filenames"`
	Flags      int    `json:"flags"`
	Identifier string `json:"identifier"`
}

type fscryptKey struct {
	Errno      int    `json:"errno"`
	Identifier string `json:"identifier"`
}

type fscryptKeyStatus struct {
	Errno  int `json:"errno"`
	Status int `json:"status"`
	Flags  int `json:"flags"`
	Users  int `json:"users"`
}

// Key status values of FS_IOC_GET_ENCRYPTION_KEY_STATUS and the removal
// status flags of FS_IOC_REMOVE_ENCRYPTION_KEY.
const (
	keyAbsent              = 1
	keyPresent             = 2
	keyIncompletelyRemoved = 3
	removalFilesBusy       = 1
)

type fscryptEntry struct {
	Name     string `json:"name"`
	Mode     uint32 `json:"mode"`
	Size     int64  `json:"size"`
	Ino      uint64 `json:"ino"`
	MtimeNS  int64  `json:"mtime_ns"`
	Data     string `json:"data"`
	Target   string `json:"target"`
	Children *int   `json:"children"`
	Errno    int    `json:"errno"`
}

func (e fscryptEntry) plainName() string {
	name, _ := hex.DecodeString(e.Name)
	return string(name)
}

type fscryptListing struct {
	Errno   int            `json:"errno"`
	Entries []fscryptEntry `json:"entries"`
	Lookups map[string]int `json:"lookups"`
}

func (l fscryptListing) byName(name string) (fscryptEntry, bool) {
	for _, entry := range l.Entries {
		if entry.plainName() == name {
			return entry, true
		}
	}
	return fscryptEntry{}, false
}

func (l fscryptListing) byIno(ino uint64) (fscryptEntry, bool) {
	for _, entry := range l.Entries {
		if entry.Ino == ino {
			return entry, true
		}
	}
	return fscryptEntry{}, false
}

func (f *fscryptClient) inspect(t *testing.T, ctx context.Context, dir string, plain ...string) fscryptListing {
	t.Helper()
	var listing fscryptListing
	f.probe(t, ctx, &listing, nil, append([]string{"inspect", f.point + dir}, plain...)...)
	if listing.Errno != 0 {
		t.Fatalf("listing %s through client %s: errno %d", dir, f.name, listing.Errno)
	}
	return listing
}

func (f *fscryptClient) addKey(t *testing.T, ctx context.Context, key []byte) fscryptKey {
	t.Helper()
	var added fscryptKey
	f.probe(t, ctx, &added, key, "addkey", f.path, f.point)
	return added
}

func (f *fscryptClient) keyStatus(t *testing.T, ctx context.Context, identifier string) fscryptKeyStatus {
	t.Helper()
	var status fscryptKeyStatus
	f.probe(t, ctx, &status, nil, "keystatus", f.path, f.point, identifier)
	if status.Errno != 0 {
		t.Fatalf("key status through client %s: errno %d", f.name, status.Errno)
	}
	return status
}

// fscryptNames are the plaintext names and bytes the authorized client
// seeds in the control and the encrypted directory.
type fscryptNames struct {
	file, second, link, target, sub string
	data                            []byte
}

func newFscryptNames(t *testing.T) fscryptNames {
	t.Helper()
	tag := randomHex(t, 6)
	data := make([]byte, 4096+17)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return fscryptNames{
		file: "plaintext-file-" + tag + ".txt", second: "plaintext-second-" + tag + ".txt", link: "plaintext-link-" + tag,
		target: "plaintext-target-" + tag + "/elsewhere", sub: "plaintext-dir-" + tag, data: data,
	}
}

func (n fscryptNames) all() []string { return []string{n.file, n.second, n.link, n.sub} }

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

type fscryptSeed struct {
	Ino    uint64 `json:"ino"`
	DirIno uint64 `json:"dir_ino"`
	Size   int64  `json:"size"`
}

func (f *fscryptClient) seed(t *testing.T, ctx context.Context, dir string, n fscryptNames) fscryptSeed {
	t.Helper()
	var seeded fscryptSeed
	f.probe(t, ctx, &seeded, nil, "seed", f.point+dir, n.file, hex.EncodeToString(n.data), n.second, n.link, n.target, n.sub)
	if seeded.Size != int64(len(n.data)) {
		t.Fatalf("seeded %s through client %s: %+v", dir, f.name, seeded)
	}
	return seeded
}

// requirePlain fails unless listing is the keyed view of the seed: the
// plaintext names, the exact bytes and the link target.
func requirePlain(t *testing.T, listing fscryptListing, n fscryptNames, view string) {
	t.Helper()
	file, okFile := listing.byName(n.file)
	second, okSecond := listing.byName(n.second)
	link, okLink := listing.byName(n.link)
	_, okSub := listing.byName(n.sub)
	reversed := slices.Clone(n.data)
	slices.Reverse(reversed)
	if len(listing.Entries) != 4 || !okFile || !okSecond || !okLink || !okSub ||
		file.Data != hex.EncodeToString(n.data) || second.Data != hex.EncodeToString(reversed) || link.Target != hex.EncodeToString([]byte(n.target)) {
		t.Fatalf("%s: want the plaintext names, bytes and link target; got %d entries %s", view, len(listing.Entries), describe(listing))
	}
}

// requireLocked fails unless listing is the view of the seed without its
// key: four entries, none with a plaintext name or the plaintext link
// target, every regular file refused with ENOKEY, and every plaintext name
// unknown to a direct lookup. It returns the entries by inode.
func requireLocked(t *testing.T, listing fscryptListing, n fscryptNames, view string) {
	t.Helper()
	if len(listing.Entries) != 4 {
		t.Fatalf("%s: want four entries, got %s", view, describe(listing))
	}
	for _, entry := range listing.Entries {
		if slices.Contains(n.all(), entry.plainName()) || strings.Contains(entry.plainName(), "plaintext") {
			t.Fatalf("%s: plaintext name listed: %s", view, describe(listing))
		}
		switch entry.Mode & 0o170000 {
		case 0o100000:
			if entry.Errno != errnoENOKEY || entry.Data != "" {
				t.Fatalf("%s: regular file %q opened with errno %d, want ENOKEY: %s", view, entry.plainName(), entry.Errno, describe(listing))
			}
		case 0o120000:
			if target, _ := hex.DecodeString(entry.Target); strings.Contains(string(target), "plaintext") {
				t.Fatalf("%s: plaintext link target: %s", view, describe(listing))
			}
		}
	}
	for name, errno := range listing.Lookups {
		if errno != errnoENOENT {
			t.Fatalf("%s: lookup of plaintext %q: errno %d, want ENOENT", view, name, errno)
		}
	}
}

// describe renders a listing without file contents.
func describe(listing fscryptListing) string {
	var parts []string
	for _, entry := range listing.Entries {
		part := fmt.Sprintf("%q mode=%o size=%d ino=%d errno=%d data=%dB", entry.plainName(), entry.Mode, entry.Size, entry.Ino, entry.Errno, len(entry.Data)/2)
		if entry.Target != "" {
			target, _ := hex.DecodeString(entry.Target)
			part += fmt.Sprintf(" target=%q", target)
		}
		if entry.Children != nil {
			part += fmt.Sprintf(" children=%d", *entry.Children)
		}
		parts = append(parts, part)
	}
	return fmt.Sprintf("[%s] lookups=%v", strings.Join(parts, "; "), listing.Lookups)
}

// exposed compares the keyed and the locked views by inode and reports the
// metadata that the locked view shows as it is.
func exposed(t *testing.T, keyed, locked fscryptListing, view string) string {
	t.Helper()
	var same []string
	for _, entry := range keyed.Entries {
		other, ok := locked.byIno(entry.Ino)
		if !ok {
			t.Fatalf("%s: inode %d of %q not listed", view, entry.Ino, entry.plainName())
		}
		var fields []string
		if other.Mode == entry.Mode {
			fields = append(fields, "mode")
		}
		if other.MtimeNS == entry.MtimeNS {
			fields = append(fields, "mtime")
		}
		if other.Size == entry.Size {
			fields = append(fields, "size")
		} else {
			fields = append(fields, fmt.Sprintf("size %d for %d", other.Size, entry.Size))
		}
		same = append(same, fmt.Sprintf("%s(%s)", map[uint32]string{0o100000: "file", 0o120000: "link", 0o040000: "dir"}[entry.Mode&0o170000], strings.Join(fields, ",")))
	}
	slices.Sort(same)
	return strings.Join(same, " ")
}

// testFscryptPath runs the case for one client path on c: admission, the
// policy query that decides whether the client implements fscrypt, the
// encrypted directory with a real key next to a control directory, the
// views of a second, independent client without the key and with a wrong
// key, the stored bytes and names in RADOS, the removal of the key with a
// retained open file, a remount and one snapshot.
func testFscryptPath(t *testing.T, ctx context.Context, c *fscryptCluster, path string) {
	image := c.clientImage(t, ctx, path)

	// An unprivileged container of the same image is refused before any
	// mount.
	plain := startFscryptClient(t, ctx, c, path, "unprivileged", image, false)
	if _, refused := plain.admit(t, ctx); refused == nil {
		t.Fatalf("an unprivileged %s client container was admitted", path)
	} else {
		t.Logf("unprivileged control: %v", refused)
	}
	if err := testcontainers.TerminateContainer(plain.ctr); err != nil {
		t.Fatal(err)
	}

	a := startFscryptClient(t, ctx, c, path, "a", image, true)
	admission, refused := a.admit(t, ctx)
	if refused != nil {
		t.Fatal(refused)
	}
	if refused := a.mount(t, ctx, admission); refused != nil {
		t.Fatal(refused)
	}
	base := "/fscrypt-" + path
	harness.ExecOutput(t, ctx, a.ctr, "mkdir", "-p", a.point+base+"/control", a.point+base+"/enc", a.point+base+"/nokey")

	// Whether the client implements fscrypt: the policy query on an empty,
	// unencrypted directory.
	var query fscryptPolicy
	a.probe(t, ctx, &query, nil, "getpolicy", path, a.point+base+"/enc")
	refusal, classified := policyRefusal(path, query.Errno)
	if !classified {
		t.Fatalf("policy query on a new directory through the %s client: %+v", path, query)
	}
	if refusal != nil {
		if path == kernelPath {
			t.Fatal(refusal)
		}
		// The client build lacks the fscrypt ioctls: the recorded outcome.
		a.umount(t, ctx)
		t.Logf("FSCRYPT_CLIENT_REFUSED %s image=%s kernel=%s: %v; the mount, its MDS session and the client container are removed", c.version, image, admission.Kernel, refusal)
		return
	}

	// A v2 policy naming a key that was never added is refused.
	var noKey fscryptErrno
	a.probe(t, ctx, &noKey, nil, "setpolicy", path, a.point+base+"/nokey", randomHex(t, 16))

	// The real key and policy.
	key := make([]byte, 64)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	added := a.addKey(t, ctx, key)
	if added.Errno != 0 || len(added.Identifier) != 32 {
		t.Fatalf("add the key through client a: %+v", added)
	}
	var set fscryptErrno
	a.probe(t, ctx, &set, nil, "setpolicy", path, a.point+base+"/enc", added.Identifier)
	if set.Errno != 0 {
		t.Fatalf("set the v2 policy through client a: errno %d", set.Errno)
	}
	var policy fscryptPolicy
	a.probe(t, ctx, &policy, nil, "getpolicy", path, a.point+base+"/enc")
	if policy.Errno != 0 || policy.Version != 2 || policy.Contents != 1 || policy.Filenames != 4 || policy.Flags != 3 || policy.Identifier != added.Identifier {
		t.Fatalf("policy read back through client a: %+v, want v2 AES-256-XTS/AES-256-CTS pad 32 with %s", policy, added.Identifier)
	}

	names := newFscryptNames(t)
	encSeed := a.seed(t, ctx, base+"/enc", names)
	controlSeed := a.seed(t, ctx, base+"/control", names)
	keyed := a.inspect(t, ctx, base+"/enc")
	requirePlain(t, keyed, names, "client a with the key")

	// A second, independent client without the key.
	b := startFscryptClient(t, ctx, c, path, "b", image, true)
	admissionB, refused := b.admit(t, ctx)
	if refused != nil {
		t.Fatal(refused)
	}
	if refused := b.mount(t, ctx, admissionB); refused != nil {
		t.Fatal(refused)
	}
	if status := b.keyStatus(t, ctx, added.Identifier); status.Status != keyAbsent {
		t.Fatalf("client b sees client a's key: %+v", status)
	}
	var bPolicy fscryptPolicy
	b.probe(t, ctx, &bPolicy, nil, "getpolicy", path, b.point+base+"/enc")
	if bPolicy.Errno != 0 || bPolicy.Identifier != added.Identifier {
		t.Fatalf("policy read through client b: %+v", bPolicy)
	}
	locked := b.inspect(t, ctx, base+"/enc", names.all()...)
	requireLocked(t, locked, names, "client b without the key")
	exposedMetadata := exposed(t, keyed, locked, "client b without the key")
	requirePlain(t, b.inspect(t, ctx, base+"/control"), names, "client b, control directory")

	// A wrong key in client b.
	wrong := make([]byte, 64)
	if _, err := rand.Read(wrong); err != nil {
		t.Fatal(err)
	}
	wrongAdded := b.addKey(t, ctx, wrong)
	if wrongAdded.Errno != 0 || wrongAdded.Identifier == added.Identifier {
		t.Fatalf("add a wrong key through client b: %+v", wrongAdded)
	}
	requireLocked(t, b.inspect(t, ctx, base+"/enc", names.all()...), names, "client b with a wrong key")

	// The stored bytes and names. The journal flush puts the dentries in
	// the directory objects.
	if _, err := c.cluster.Ceph(ctx, "tell", "mds."+c.mds, "flush", "journal"); err != nil {
		t.Fatal(err)
	}
	object := func(ino uint64) string { return fmt.Sprintf("%x.%08x", ino, 0) }
	stored := c.rados(t, ctx, "-p", c.fs.DataPool, "get", object(encSeed.Ino), "-")
	control := c.rados(t, ctx, "-p", c.fs.DataPool, "get", object(controlSeed.Ino), "-")
	if !bytes.Equal(control, names.data) {
		t.Fatalf("control file object %s: %d bytes, equal %t", object(controlSeed.Ino), len(control), bytes.Equal(control, names.data))
	}
	if len(stored) < len(names.data) || bytes.Contains(stored, names.data[:32]) || bytes.Contains(stored, names.data[len(names.data)-32:]) {
		t.Fatalf("encrypted file object %s: %d bytes, holds plaintext", object(encSeed.Ino), len(stored))
	}
	encKeys := string(c.rados(t, ctx, "-p", c.fs.MetadataPool, "listomapkeys", object(encSeed.DirIno)))
	controlKeys := string(c.rados(t, ctx, "-p", c.fs.MetadataPool, "listomapkeys", object(controlSeed.DirIno)))
	if !strings.Contains(controlKeys, names.file+"_head") || strings.Contains(encKeys, "plaintext") || len(strings.Fields(encKeys)) != 4 {
		t.Fatalf("directory objects: control keys %q, encrypted keys %q", controlKeys, encKeys)
	}

	// The key is removed while client a keeps the file open. Once it is
	// gone, the plaintext path no longer opens: ENOENT where the name no
	// longer resolves, ENOKEY where it does but the contents stay locked.
	var retained struct {
		Before        string `json:"before"`
		RemoveErrno   int    `json:"remove_errno"`
		RemoveFlags   *int   `json:"remove_flags"`
		BusyStatus    int    `json:"busy_status"`
		Retained      string `json:"retained"`
		RetainedErrno int    `json:"retained_errno"`
		OtherErrno    int    `json:"other_errno"`
		FinalErrno    int    `json:"final_errno"`
		FinalFlags    *int   `json:"final_flags"`
		FinalStatus   int    `json:"final_status"`
		ReopenErrno   int    `json:"reopen_errno"`
	}
	a.probe(t, ctx, &retained, nil, "retain", path, a.point, added.Identifier, a.point+base+"/enc/"+names.file, a.point+base+"/enc/"+names.second)
	retainedDescription := fmt.Sprintf("remove errno=%d flags=%v status=%d; retained fd read equal=%t errno=%d; other file open errno=%d; after close remove errno=%d flags=%v status=%d; reopen errno=%d",
		retained.RemoveErrno, deref(retained.RemoveFlags), retained.BusyStatus, retained.Retained == hex.EncodeToString(names.data), retained.RetainedErrno,
		retained.OtherErrno, retained.FinalErrno, deref(retained.FinalFlags), retained.FinalStatus, retained.ReopenErrno)
	if retained.Before != hex.EncodeToString(names.data) || retained.RemoveErrno != 0 || retained.FinalStatus != keyAbsent ||
		(retained.ReopenErrno != errnoENOENT && retained.ReopenErrno != errnoENOKEY) {
		t.Fatalf("key removal with a retained open file: %s", retainedDescription)
	}
	requireLocked(t, a.inspect(t, ctx, base+"/enc", names.all()...), names, "client a after the key removal")

	// A remount of client a starts locked; the same key unlocks it again.
	a.umount(t, ctx)
	if refused := a.mount(t, ctx, admission); refused != nil {
		t.Fatal(refused)
	}
	requireLocked(t, a.inspect(t, ctx, base+"/enc", names.all()...), names, "client a after the remount")
	readded := a.addKey(t, ctx, key)
	if readded.Errno != 0 || readded.Identifier != added.Identifier {
		t.Fatalf("add the key again after the remount: %+v, want %s", readded, added.Identifier)
	}
	requirePlain(t, a.inspect(t, ctx, base+"/enc"), names, "client a after the remount with the key")

	// One snapshot of the encrypted directory.
	const snapName = "fscrypt-snapshot"
	var snapped fscryptErrno
	a.probe(t, ctx, &snapped, nil, "mkdir", a.point+base+"/enc/.snap/"+snapName)
	if snapped.Errno != 0 {
		t.Fatalf("snapshot of the encrypted directory through client a: errno %d", snapped.Errno)
	}
	keyedSnaps := a.inspect(t, ctx, base+"/enc/.snap")
	if _, ok := keyedSnaps.byName(snapName); !ok || len(keyedSnaps.Entries) != 1 {
		t.Fatalf("client a with the key lists the snapshots %s", describe(keyedSnaps))
	}
	requirePlain(t, a.inspect(t, ctx, base+"/enc/.snap/"+snapName), names, "client a, snapshot with the key")
	lockedSnaps := b.inspect(t, ctx, base+"/enc/.snap", snapName)
	if len(lockedSnaps.Entries) != 1 {
		t.Fatalf("client b without the key lists the snapshots %s", describe(lockedSnaps))
	}
	snapVisible := lockedSnaps.Entries[0].plainName() == snapName
	requireLocked(t, b.inspect(t, ctx, base+"/enc/.snap/"+lockedSnaps.Entries[0].plainName(), names.all()...), names, "client b, snapshot without the key")
	var unsnapped fscryptErrno
	a.probe(t, ctx, &unsnapped, nil, "rmdir", a.point+base+"/enc/.snap/"+snapName)
	if unsnapped.Errno != 0 {
		t.Fatalf("remove the snapshot through client a: errno %d", unsnapped.Errno)
	}

	// Cleanup of the keys and mounts this case added. A key whose inodes the
	// client still caches stays incompletely removed until the unmount.
	var cleanup []string
	for _, removal := range []struct {
		client     *fscryptClient
		identifier string
	}{{a, added.Identifier}, {b, wrongAdded.Identifier}} {
		var removed fscryptErrno
		removal.client.probe(t, ctx, &removed, nil, "removekey", path, removal.client.point, removal.identifier)
		status := removal.client.keyStatus(t, ctx, removal.identifier)
		if removed.Errno != 0 || (status.Status != keyAbsent && status.Status != keyIncompletelyRemoved) {
			t.Fatalf("remove key %s through client %s: errno %d, then %+v", removal.identifier, removal.client.name, removed.Errno, status)
		}
		cleanup = append(cleanup, fmt.Sprintf("client %s key status %d", removal.client.name, status.Status))
		removal.client.umount(t, ctx)
	}
	for _, client := range []*fscryptClient{a, b} {
		if err := testcontainers.TerminateContainer(client.ctr); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("FSCRYPT_RECEIPT %s path=%s image=%s kernel=%s: a policy naming a key never added: errno %d; key %s added through client a, v2 AES-256-XTS/AES-256-CTS pad 32 set on an empty directory and read back through both clients; client a reads the plaintext names, %d bytes and the link target; client b without the key and with a wrong key (%s) lists 4 encoded names, ENOKEY on open, ENOENT for every plaintext name, exposed metadata %s; the control directory is plaintext through b; RADOS data object %s holds %d bytes without the plaintext and the control object holds it exactly; the encrypted directory object has 4 omap keys without a plaintext name, the control object has %s_head; key removal with a retained fd: %s; a remount is locked until the same key is added again (same identifier); snapshot %q of the encrypted directory reads the exact bytes with the key, its name is %s to client b, whose view of it is locked; keys removed (%s), mounts, MDS sessions and client containers gone",
		c.version, path, image, admission.Kernel, noKey.Errno, added.Identifier, len(names.data), wrongAdded.Identifier, exposedMetadata,
		object(encSeed.Ino), len(stored), names.file, retainedDescription, snapName, map[bool]string{true: "plaintext", false: "encoded"}[snapVisible], strings.Join(cleanup, ", "))
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// One filesystem, two independent client containers per client path, a
// control directory and a directory with a real fscrypt v2 policy and key.
// The kernel path runs on the engine's kernel and fails, never loading a
// module, when the kernel has not registered the ceph filesystem type. The
// ceph-fuse path records a typed refusal when the client build answers the
// policy query without the fscrypt ioctls.
func TestCephFSFscrypt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	c := startFscryptCluster(t, ctx)
	for _, path := range []string{kernelPath, fusePath} {
		t.Run(path, func(t *testing.T) {
			testFscryptPath(t, ctx, c, path)
		})
	}
}
