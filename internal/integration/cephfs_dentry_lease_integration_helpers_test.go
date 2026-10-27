//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// cephFSLeaseHolder is an actor that keeps one mount and only looks names up.
// It mounts and publishes receipt 1 with its client ID. At the "lookup" gate
// it stats the directory and publishes receipt 2 with the directory's inode.
// At "renamed" it looks up the old name, which must be gone, and the new
// name, and publishes receipt 3 with the errno of the old name and the inode
// of the new one. At "removed" it looks the new name up again and publishes
// receipt 4 with that errno. It unmounts after "finish" and publishes receipt
// 5. Arguments after the common ones: the directory, its new name.
const cephFSLeaseHolder cephFSActorScript = cephFSActorHelpers + `
old, new = sys.argv[4].encode(), sys.argv[5].encode()
def errno_of(path):
    try:
        fs.stat(path)
        return 0
    except cephfs.Error as error:
        return getattr(error, "errno", None) or -1
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
publish(1, client=fs.get_instance_id())
gate("lookup")
publish(2, inode=fs.stat(old).st_ino)
gate("renamed")
publish(3, errno=errno_of(old), inode=fs.stat(new).st_ino)
gate("removed")
publish(4, errno=errno_of(new))
gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(5, closed=True, **closing)
`

// cephFSLeaseRenamer is an actor that creates the directory and publishes
// receipt 1 with its client ID and the directory's inode. At the "rename"
// gate it renames the directory and publishes receipt 2 with how long the
// rename took; at "remove" it removes the renamed directory and publishes
// receipt 3 with how long that took. Its mount stays open throughout, so the
// MDS shares the parent directory between two clients. It unmounts after
// "finish" and publishes receipt 4. Arguments after the common ones: the
// directory, its new name.
const cephFSLeaseRenamer cephFSActorScript = cephFSActorHelpers + `
old, new = sys.argv[4].encode(), sys.argv[5].encode()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
fs.mkdir(old, 0o755)
publish(1, client=fs.get_instance_id(), inode=fs.stat(old).st_ino)
gate("rename")
started = time.monotonic()
fs.rename(old, new)
publish(2, elapsed_ms=int((time.monotonic() - started) * 1000))
gate("remove")
started = time.monotonic()
fs.rmdir(new)
publish(3, elapsed_ms=int((time.monotonic() - started) * 1000))
gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// mdsDentry is one dentry of the MDS's "dump cache", with the client lease
// state of its dentry lock.
type mdsDentry struct {
	Path string `json:"path"`
	Lock struct {
		State    string `json:"state"`
		IsLeased bool   `json:"is_leased"`
	} `json:"lock"`
}

// cephFSDentries returns the dentries that mds has cached under the root with
// one of names, keyed by name.
func cephFSDentries(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string, names ...string) map[string]mdsDentry {
	t.Helper()
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dump", "cache")
	if err != nil {
		t.Fatal("dump cache:", err)
	}
	var inodes []struct {
		Dirfrags []struct {
			Dentries []mdsDentry `json:"dentries"`
		} `json:"dirfrags"`
	}
	if err := json.Unmarshal(out, &inodes); err != nil {
		t.Fatalf("dump cache: %v: %.512s", err, out)
	}
	found := make(map[string]mdsDentry)
	for _, inode := range inodes {
		for _, frag := range inode.Dirfrags {
			for _, dentry := range frag.Dentries {
				if slices.Contains(names, dentry.Path) {
					found[dentry.Path] = dentry
				}
			}
		}
	}
	return found
}

// cephFSWaitForSessionLeases waits up to a minute until client holds want
// leases on mds, and fails with the last count otherwise. A lease that a
// client has released reaches the MDS with the client's next message.
func cephFSWaitForSessionLeases(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string, client uint64, want int) {
	t.Helper()
	seen, err := observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (string, bool, error) {
		out, err := cluster.Ceph(probeCtx, "tell", "mds."+mds, "session", "ls", "--format", "json")
		if err != nil {
			return "", false, err
		}
		var sessions []struct {
			ID        uint64 `json:"id"`
			NumLeases int    `json:"num_leases"`
		}
		if err := json.Unmarshal(out, &sessions); err != nil {
			return "", false, err
		}
		for _, session := range sessions {
			if session.ID == client {
				return strconv.Itoa(session.NumLeases), session.NumLeases == want, nil
			}
		}
		return "", false, errors.New("no session of client." + strconv.FormatUint(client, 10))
	})
	if err != nil {
		t.Fatalf("client.%d num_leases %s, want %d: %v", client, seen.Value, want, err)
	}
}

// testCephFSDentryLeaseRecall lets one client look a directory up after a
// second client created it. The first client mounted before the creation,
// which took the root's shared file capability (Fs) from it, and the second
// keeps its mount, so the MDS answers the lookup with a dentry lease instead
// of giving Fs back. (A client that mounts after the creation, alone with
// Fs on the root, gets no lease.) The second client then renames the directory and removes it.
// Each time the MDS recalls the first client's lease before it changes the
// dentry: the client's num_leases drops, the dentry lock is no longer
// leased, the operation finishes at once, and the first client sees the new
// namespace without being evicted.
func testCephFSDentryLeaseRecall(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	cluster, client := newServiceClusterRun(t, cephfsRun, image, append(imageOpts, ceph.WithMessengerMode(ceph.MessengerV2Secure))...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("want one active MDS: %+v, %v", status, err)
	}
	mds := status.Active[0].Name
	const dir, moved = "leased", "leased-moved"

	holder := startCephFSActor(t, ctx, cluster, client, cephFSLeaseHolder, fs.FilesystemName, "/tmp/lease-holder", "/"+dir, "/"+moved)
	holderID := holder.receipt(t, ctx, 1).Client
	session := cephFSRetainedSession(t, ctx, fs, holderID, 0)
	renamer := startCephFSActor(t, ctx, cluster, toolkitContainer(t, cluster), cephFSLeaseRenamer, fs.FilesystemName, "/tmp/lease-renamer", "/"+dir, "/"+moved)
	created := renamer.receipt(t, ctx, 1)
	if created.Client == holderID {
		t.Fatalf("holder and renamer share client.%d", holderID)
	}
	rootCaps, _ := cephFSInodeCaps(t, ctx, cluster, mds, 1)
	holderRoot := ""
	for _, caps := range rootCaps {
		if caps.ClientID == holderID {
			holderRoot = caps.Issued
		}
	}
	if holderRoot == "" || capsOf(holderRoot, 'F') != "" {
		t.Fatalf("holder client.%d root capabilities %q, want some without Fs: %+v", holderID, holderRoot, rootCaps)
	}
	holder.open(t, ctx, "lookup")
	if looked := holder.receipt(t, ctx, 2); looked.Inode != created.Inode {
		t.Fatalf("holder looked up inode %d, renamer created %d", looked.Inode, created.Inode)
	}
	cephFSWaitForSessionLeases(t, ctx, cluster, mds, holderID, 1)
	if dentry, ok := cephFSDentries(t, ctx, cluster, mds, dir)[dir]; !ok || !dentry.Lock.IsLeased {
		t.Fatalf("dentry %s is not leased after the lookup: %+v (found %t)", dir, dentry, ok)
	}

	renamer.open(t, ctx, "rename")
	renamed := renamer.receipt(t, ctx, 2)
	// The holder answers the recall at once; one that did not would hold
	// the rename until its lease or session timed out.
	if renamed.ElapsedMS > 10_000 {
		t.Fatalf("rename took %d ms: the holder did not give up its lease", renamed.ElapsedMS)
	}
	cephFSWaitForSessionLeases(t, ctx, cluster, mds, holderID, 0)
	if dentry, ok := cephFSDentries(t, ctx, cluster, mds, dir)[dir]; ok && dentry.Lock.IsLeased {
		t.Fatalf("the old dentry %s is still leased: %+v", dir, dentry)
	}
	holder.open(t, ctx, "renamed")
	after := holder.receipt(t, ctx, 3)
	if after.Errno != 2 || after.Inode != created.Inode {
		t.Fatalf("holder after the rename: %+v, want ENOENT for %s and inode %d at %s", after, dir, created.Inode, moved)
	}
	// Looking the new name up leases its dentry to the holder.
	cephFSWaitForSessionLeases(t, ctx, cluster, mds, holderID, 1)

	renamer.open(t, ctx, "remove")
	removed := renamer.receipt(t, ctx, 3)
	if removed.ElapsedMS > 10_000 {
		t.Fatalf("rmdir took %d ms: the holder did not give up its lease", removed.ElapsedMS)
	}
	cephFSWaitForSessionLeases(t, ctx, cluster, mds, holderID, 0)
	holder.open(t, ctx, "removed")
	if gone := holder.receipt(t, ctx, 4); gone.Errno != 2 {
		t.Fatalf("holder after the rmdir: %+v, want ENOENT", gone)
	}

	if retained := cephFSRetainedSession(t, ctx, fs, holderID, 0); retained.Address != session.Address || retained.PID != session.PID {
		t.Fatalf("holder session %+v, before %+v", retained, session)
	}
	entries, err := observeCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("holder %s was blocklisted: %+v", session.Address, entries)
	}
	for actor, phase := range map[*cephFSActorRun]int{renamer: 4, holder: 5} {
		actor.open(t, ctx, "finish")
		if closed := actor.receipt(t, ctx, phase); !closed.Closed || closed.CloseErrno != 0 {
			t.Fatalf("closing receipt = %+v", closed)
		}
		actor.wait(t, ctx)
	}
	t.Logf("CEPHFS_LEASE dir inode=%d holder client.%d (session %s, root caps %s without Fs) leased /%s; renamer client.%d renamed it in %d ms and removed /%s in %d ms, each recalling the holder's lease (num_leases 1 -> 0); the holder saw ENOENT for the old names and kept its session",
		created.Inode, holderID, session.Address, holderRoot, dir, created.Client, renamed.ElapsedMS, moved, removed.ElapsedMS)
}
