//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_clients))))

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// realmLook is one bounded look at the retained client's cache.
type realmLook struct {
	released bool
	derived  clientSnapContext
	err      error
	data     []byte
}

// pollRealmGate polls the client's dump_cache with the given gate until it
// releases, under one bounded phase.
func pollRealmGate(t *testing.T, ctx context.Context, m *harness.CephFSSingleMDS, asok string, gate func([]byte) (bool, clientSnapContext, error)) (realmLook, int, time.Duration, error) {
	t.Helper()
	started := time.Now()
	final, looks, err := boundedPoll(ctx, realPollClock, 90*time.Second, time.Second, func(attemptCtx context.Context) (realmLook, bool, error) {
		data, err := queryClientDumpCache(attemptCtx, m.Cluster, m.Client, asok)
		if err != nil {
			return realmLook{}, false, err
		}
		l := realmLook{data: data}
		l.released, l.derived, l.err = gate(data)
		return l, l.released, nil
	})
	return final, looks, time.Since(started), err
}

func testCephFSLiveSnapRealmRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	toolkit := harness.ToolkitContainer(t, m.Cluster)
	fsName := m.FS.FilesystemName
	if _, err := m.Cluster.Ceph(ctx, "fs", "set", fsName, "allow_new_snaps", "true"); err != nil {
		t.Fatal("allow_new_snaps:", err)
	}

	base := bytes.Repeat([]byte("before-snapshot\x00\xff"), 512)
	updated := bytes.Repeat([]byte("after-removal!!\x01\xfe"), 512)
	const (
		workdir   = "/tmp/realm-actor"
		directory = "/realm"
		leaf      = "file"
		snapName  = "live-realm-removal"
	)
	asok := workdir + "/client.asok"
	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSRealmActor, fsName, workdir,
		directory, directory+"/"+leaf, hex.EncodeToString(base), hex.EncodeToString(updated), asok)
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.DirInode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}

	// The snapshot is removed on every exit; an absent one counts as removed.
	t.Cleanup(func() {
		cleanCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if _, err := runCephFSRealmAdmin(cleanCtx, m.Cluster, toolkit, "rmsnap", fsName, directory, snapName, leaf); err != nil {
			t.Errorf("remove snapshot: %v", err)
		}
	})

	made, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "snap", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if made.Client == 0 || made.Client == baseline.Client || made.DirInode != baseline.DirInode {
		t.Fatalf("snapshot administrator = %+v for actor %+v", made, baseline)
	}
	var snapLookErr error
	snapID, _, err := boundedPoll(ctx, realPollClock, time.Minute, time.Second, func(attemptCtx context.Context) (uint64, bool, error) {
		raw, err := m.Cluster.Ceph(attemptCtx, "tell", "mds."+m.MDS.Name, "dump", "snaps")
		if err != nil {
			snapLookErr = err
			return 0, false, nil
		}
		id, err := mdsSnapshotID(raw, baseline.DirInode, snapName)
		snapLookErr = err
		return id, err == nil, nil
	})
	if err != nil {
		t.Fatalf("snapshot ID of %q: %v (last: %v)", snapName, err, snapLookErr)
	}

	// Addition: the retained client must first show the snapshot, so that its
	// later disappearance is a change of the same cache and not an absence.
	added, looks, took, err := pollRealmGate(t, ctx, m, asok, func(data []byte) (bool, clientSnapContext, error) {
		return snapContextGate(data, baseline.Inode, snapID)
	})
	if err != nil || !added.released {
		t.Fatalf("CEPHFS_LIVE_REALM_REMOVAL addition not observed after %d looks over %s (%v): %s; derived=%+v; snapshot id=%x: %s",
			looks, took.Round(time.Second), err, classifySnapRealm(added.err), added.derived, snapID, snapRealmShape(added.data, baseline.Inode))
	}
	if err := snapRemovalPriorOK(added.derived, snapID); err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_LIVE_REALM_REMOVAL addition observed after %d looks, %s: derived seq=%x snaps=%x chain=%x; %s",
		looks, took.Round(time.Millisecond), added.derived.Seq, added.derived.Snaps, added.derived.Chain, snapRealmShape(added.data, baseline.Inode))

	// Removal by the independent administrator while the actor stays live.
	removed, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "rmsnap", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if !removed.Removed || removed.Client == 0 || removed.Client == baseline.Client {
		t.Fatalf("removal administrator = %+v for actor %+v", removed, baseline)
	}

	// The MDS pushes the destroy to clients with caps in the realm
	// (Client::handle_snap with CEPH_SNAP_OP_DESTROY); nothing orders that
	// before the administrator's reply, so the client's cache is polled.
	gone, looks, took, err := pollRealmGate(t, ctx, m, asok, func(data []byte) (bool, clientSnapContext, error) {
		return snapRemovalGate(data, baseline.Inode, snapID, added.derived.Seq)
	})
	if err != nil || !gone.released {
		t.Fatalf("CEPHFS_LIVE_REALM_REMOVAL removal not observed after %d looks over %s (%v): %s; derived=%+v; snapshot id=%x, sequence before=%x; the overwrite was not attempted: %s",
			looks, took.Round(time.Second), err, classifySnapRealm(gone.err), gone.derived, snapID, added.derived.Seq, snapRealmShape(gone.data, baseline.Inode))
	}
	t.Logf("CEPHFS_LIVE_REALM_REMOVAL removal observed after %d looks, %s: derived seq=%x snaps=%x chain=%x; %s",
		looks, took.Round(time.Millisecond), gone.derived.Seq, gone.derived.Snaps, gone.derived.Chain, snapRealmShape(gone.data, baseline.Inode))

	actor.Open(t, ctx, "overwrite")
	over := actor.Receipt(t, ctx, 2)
	if over.Errno != 0 || over.Error != "" || over.PID != baseline.PID || over.Inode != baseline.Inode ||
		over.Size != int64(len(updated)) || over.Full != hex.EncodeToString(updated) {
		t.Fatalf("overwrite receipt: errno=%d error=%q pid=%d inode=%d size=%d", over.Errno, over.Error, over.PID, over.Inode, over.Size)
	}
	read, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "read", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if read.Head == nil || read.Snap == nil || read.Head.Errno != 0 || read.Head.Data != hex.EncodeToString(updated) ||
		read.Head.Size != int64(len(updated)) || read.Snap.Errno != int(syscall.ENOENT) || read.Snap.Data != "" {
		t.Fatalf("independent read: head=%+v removed snapshot=%+v", read.Head, read.Snap)
	}
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt: %+v", closed)
	}
	actor.Wait(t, ctx)
	t.Logf("CEPHFS_LIVE_REALM_REMOVAL %s client.%d inode=%d dir=0x%x snapshot id=%x: addition observed at seq=%x, removal observed at seq=%x on the live actor; live fd overwrite of %d bytes after the removal gate; HEAD read as the new bytes by client.%d and the removed snapshot read as ENOENT",
		m.Version, baseline.Client, baseline.Inode, baseline.DirInode, snapID, added.derived.Seq, gone.derived.Seq, len(updated), read.Client)
}
