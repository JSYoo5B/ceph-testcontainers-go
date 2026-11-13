//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_mirror_blockdiff_empty)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs

package cephfs_mirror_blockdiff_empty_versions_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// blockdiffScript changes the files of the mirrored directory, takes a
// snapshot of it and reports the snapshot, or only reports a snapshot
// ("read"). The directory holds f, the trigger file, and g, a file that
// always has data.
//
//   - baseline: both files get 1 MiB of data.
//   - control: f is truncated to nothing and extended to 4096 bytes without
//     data, so its first object is empty after a version with data; the
//     first 64 KiB of g are rewritten.
//   - trigger: one byte is written to f and flushed, which makes the object
//     store keep the empty object as the version of the previous snapshot;
//     then f is truncated and extended as before, so the head is empty too.
//     g is not touched, so f is the only changed file of the snapshot.
const blockdiffScript = `import cephfs,hashlib,json,os,sys
filesystem,phase,snapshot=sys.argv[1:4]
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','30')
fs.mount(filesystem_name=filesystem.encode())
root,mib='/probe',1<<20
def write(path,data,flags=os.O_WRONLY):
    fd=fs.open(path,flags,0o640)
    try:
        assert fs.write(fd,data,0)==len(data)
        fs.fsync(fd,False)
    finally:
        fs.close(fd)
def hole(path):
    fs.truncate(path,0)
    fs.truncate(path,4096)
def read(directory):
    files={}
    for name in ('f','g'):
        path=directory+'/'+name
        fd=fs.open(path,os.O_RDONLY)
        try:
            digest,offset=hashlib.sha256(),0
            while True:
                data=fs.read(fd,offset,mib)
                if not data:
                    break
                digest.update(data)
                offset+=len(data)
        finally:
            fs.close(fd)
        status=fs.stat(path)
        assert status.st_size==offset,'size %d, read %d bytes: %s'%(status.st_size,offset,path)
        files[name]={'ino':status.st_ino,'size':offset,'sha256':digest.hexdigest()}
    return files
try:
    path=root+'/.snap/'+snapshot
    if phase=='baseline':
        fs.mkdir(root,0o755)
        write(root+'/f',b'f'*mib,os.O_CREAT|os.O_WRONLY)
        write(root+'/g',b'g'*mib,os.O_CREAT|os.O_WRONLY)
    elif phase=='control':
        hole(root+'/f')
        write(root+'/g',b'2'*65536)
    elif phase=='trigger':
        write(root+'/f',b'x')
        hole(root+'/f')
    else:
        assert phase=='read','unknown phase: '+phase
    if phase!='read':
        fs.sync_fs()
        fs.mkdir(path,0o755)
    try:
        info=fs.snap_info(path)
    except cephfs.ObjectNotFound:
        print(json.dumps({'present':False,'name':snapshot}))
    else:
        print(json.dumps({'present':True,'id':info['id'],'name':snapshot,'files':read(path)}))
finally:
    fs.shutdown()
`

const (
	blockdiffScriptPath = "/tmp/blockdiff-empty-versions.py"
	blockdiffDirectory  = "/probe"
	// blockdiffThreshold is the mirror daemon's setting for the file size
	// up to which it copies a changed file whole instead of asking for its
	// changed blocks. Images that have it default it far above the files of
	// this case.
	blockdiffThreshold = "cephfs_mirror_blockdiff_min_file_size"
	// blockdiffSyncWait bounds the wait for the trigger snapshot.
	blockdiffSyncWait = 4 * time.Minute
)

// image is the Ceph image the case runs on, for both clusters, their client
// containers and the mirror daemon: Squid 19.2.5, which reproduces the
// defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions_test.image=<image>"
//
// Tentacle 20.2.4 and Umbrella 21.2.0 do not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:d0fcbea14a2a0bc6f5e6dfbf4973849a80cda16dda19336c66fa02d54b8cf38e"

// TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions reports whether the
// source MDS aborts when an incremental mirror synchronization asks for the
// changed blocks of a file whose two compared object versions are both
// empty.
//
// Given two clusters with one filesystem each and a mirror of one directory
// whose baseline snapshot arrived, and the control (a snapshot in which the
// trigger file's first object is empty after a version with data, and a
// second file has data in both versions, is synchronized through blockdiff
// requests by an MDS that stays live, with the source's sizes and content),
// when a further snapshot in which only the trigger file changed, with an
// empty object version on either side, is synchronized, then the defect is
// REPRODUCED if the MDS process ends with the interval assertion in the
// function that aggregates object versions, and NOT_REPRODUCED if the same
// MDS process served a further blockdiff request and the snapshot arrived
// with the source's sizes and content.
//
// A failure of the given or when steps fails the test, and so does a live
// MDS that was never asked for the trigger file's blocks; the result does
// not.
func TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	t.Logf("image: %s", image)
	opts := []testcontainers.ContainerCustomizer{harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	clusters := make([]*ceph.Container, 2)
	clients := make([]testcontainers.Container, 2)
	filesystems := make([]*cephfs.Filesystem, 2)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, image, opts...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(), ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		filesystem, err := cephfs.Start(ctx, cluster, cephfs.Config{})
		if err != nil {
			t.Fatal(err)
		}
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		if err := client.CopyToContainer(ctx, []byte(blockdiffScript), blockdiffScriptPath, 0o600); err != nil {
			t.Fatal(err)
		}
		clusters[i], clients[i], filesystems[i] = cluster, client, filesystem
	}
	source, destination, sourceClient, destinationClient := clusters[0], clusters[1], clients[0], clients[1]
	sourceFS, destinationFS := filesystems[0], filesystems[1]
	t.Logf("client: %s", harness.MultiClusterExecOutput(t, ctx, sourceClient, "ceph", "--version"))
	if len(sourceFS.MDSs()) != 1 {
		t.Fatalf("the source filesystem has %d MDS daemons, want one", len(sourceFS.MDSs()))
	}
	mds := sourceFS.MDSs()[0]

	// An image whose mirror daemon copies small files whole is told to ask
	// for the changed blocks of a file of any size. An image without the
	// setting always asks.
	switch _, err := source.Ceph(ctx, "config", "help", blockdiffThreshold); {
	case err == nil:
		if _, err := source.Ceph(ctx, "config", "set", "client", blockdiffThreshold, "0"); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s set to 0 for the mirror daemon", blockdiffThreshold)
	case errors.Is(err, fs.ErrNotExist):
		t.Logf("the image has no %s", blockdiffThreshold)
	default:
		t.Fatal(err)
	}

	script := func(client testcontainers.Container, filesystem, phase, snapshot string) blockdiffSnapshot {
		t.Helper()
		out := harness.MultiClusterExecOutput(t, ctx, client, "python3", blockdiffScriptPath, filesystem, phase, snapshot)
		var got blockdiffSnapshot
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("%s %s: %v: %s", phase, snapshot, err, out)
		}
		return got
	}
	take := func(phase, snapshot string) blockdiffSnapshot {
		t.Helper()
		got := script(sourceClient, sourceFS.FilesystemName, phase, snapshot)
		if !got.Present || got.ID == 0 || len(got.Files) != 2 {
			t.Fatalf("source snapshot %s: %+v", snapshot, got)
		}
		return got
	}
	arrived := func(snapshot string) blockdiffSnapshot {
		t.Helper()
		return script(destinationClient, destinationFS.FilesystemName, "read", snapshot)
	}
	// versions returns the sizes of the version of the file's first object
	// that holds the snapshot previous, and of its head.
	versions := func(file blockdiffFile, previous uint64) (uint64, uint64, error) {
		t.Helper()
		object := fmt.Sprintf("%x.00000000", file.Inode)
		listing := harness.MultiClusterExecOutput(t, ctx, sourceClient, "rados", "-p", sourceFS.DataPool, "listsnaps", object, "--format", "json")
		return versionSizes(listing, previous)
	}
	// requests returns how many blockdiff requests the source MDS served.
	requests := func() (uint64, error) {
		out, err := source.Ceph(ctx, "tell", "mds."+mds.ID, "perf", "dump", "mds_server")
		if err != nil {
			return 0, err
		}
		var dump struct {
			Server struct {
				Blockdiff *struct {
					Count uint64 `json:"avgcount"`
				} `json:"req_blockdiff_latency"`
			} `json:"mds_server"`
		}
		if err := json.Unmarshal(out, &dump); err != nil || dump.Server.Blockdiff == nil {
			return 0, fmt.Errorf("the MDS reports no blockdiff request counter: %s: %v", out, err)
		}
		return dump.Server.Blockdiff.Count, nil
	}
	// generation returns what identifies the source MDS process: the start
	// time of its container, which must be running, and the daemon's GID.
	generation := func() (string, error) {
		state, err := mds.State(ctx)
		if err != nil {
			return "", err
		}
		process := fmt.Sprintf("container status %s, exit code %d, started %s", state.Status, state.ExitCode, state.StartedAt)
		if !state.Running {
			return process, errors.New("the MDS container is not running")
		}
		status, err := sourceFS.MDSStatus(ctx)
		if err != nil {
			return process, err
		}
		if len(status.Active) != 1 || status.Active[0].Name != mds.ID {
			return process, fmt.Errorf("the active MDS daemons are %+v, want only %s", status.Active, mds.ID)
		}
		return fmt.Sprintf("%s, GID %d", process, status.Active[0].GID), nil
	}

	// Given: a mirror whose baseline snapshot arrived.
	baseline := take("baseline", "s1")
	mirror, err := cephfsmirror.Run(ctx, image, cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{blockdiffDirectory},
	}))
	if mirror != nil {
		t.Cleanup(func() {
			if t.Failed() {
				logCtx, logCancel := context.WithTimeout(context.Background(), 20*time.Second)
				for _, daemon := range mirror.Daemons() {
					harness.MultiClusterLogContainer(t, logCtx, daemon.Container)
				}
				harness.MultiClusterLogContainer(t, logCtx, mds.Container)
				logCancel()
			}
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate the CephFS mirror: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(mirror.Daemons()) != 1 {
		t.Fatalf("the mirror has %d daemons, want one", len(mirror.Daemons()))
	}
	daemon := mirror.Daemons()[0]
	synced := func(snapshot blockdiffSnapshot) {
		t.Helper()
		if status, err := mirror.WaitSnapshotSynced(ctx, blockdiffDirectory, cephfsmirror.Snapshot{ID: snapshot.ID, Name: snapshot.Name}); err != nil {
			t.Fatalf("condition not established: snapshot %s was not synchronized: %v: %+v", snapshot.Name, err, status)
		}
		if got := arrived(snapshot.Name); !snapshot.sameContent(got) {
			t.Fatalf("condition not established: the destination has %+v for snapshot %s, want the sizes and content of %+v", got, snapshot.Name, snapshot)
		}
	}
	synced(baseline)

	// The control: an empty head after a version with data, and a file with
	// data in both versions, go through blockdiff requests that the MDS
	// survives.
	first, err := generation()
	if err != nil {
		t.Fatalf("condition not established: %v (%s)", err, first)
	}
	asked, err := requests()
	if err != nil {
		t.Fatalf("condition not established: %v", err)
	}
	control := take("control", "s2")
	synced(control)
	for name, wantHead := range map[string]bool{"f": false, "g": true} {
		clone, head, err := versions(control.Files[name], baseline.ID)
		if err != nil || clone == 0 || (head > 0) != wantHead {
			t.Fatalf("condition not established: the first object of %s has %d bytes in snapshot %s and %d in its head (data in the head wanted: %t): %v", name, clone, baseline.Name, head, wantHead, err)
		}
	}
	afterControl, err := requests()
	if err != nil {
		t.Fatalf("condition not established: %v", err)
	}
	if afterControl <= asked {
		t.Fatalf("condition not established: the mirror synchronized the control snapshot without a blockdiff request (%d served before, %d after)", asked, afterControl)
	}
	if now, err := generation(); err != nil || now != first {
		t.Fatalf("control failed: the source MDS process changed during the control snapshot: %s, then %s: %v", first, now, err)
	}
	t.Logf("control: snapshot %s synchronized with %d blockdiff requests; source MDS %s", control.Name, afterControl-asked, first)

	// When: with the mirror daemon stopped, only the trigger file changes
	// so that its first object is empty in the control snapshot and in the
	// new one; then the daemon synchronizes the new snapshot.
	stop := 10 * time.Second
	if err := daemon.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	trigger := take("trigger", "s3")
	if trigger.Files["g"] != control.Files["g"] || trigger.Files["f"].Inode != control.Files["f"].Inode {
		t.Fatalf("trigger not established: the trigger snapshot has %+v after %+v", trigger.Files, control.Files)
	}
	// The MDS truncates the object after it answers the client.
	layout := time.Now().Add(time.Minute)
	for {
		clone, head, err := versions(trigger.Files["f"], control.ID)
		if err == nil && clone == 0 && head == 0 {
			break
		}
		if time.Now().After(layout) {
			t.Fatalf("trigger not established: the first object of f has %d bytes in snapshot %s and %d in its head, want both empty: %v", clone, control.Name, head, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	before, err := requests()
	if err != nil {
		t.Fatalf("trigger not established: %v", err)
	}
	if now, err := generation(); err != nil || now != first {
		t.Fatalf("trigger not established: the source MDS process changed before the trigger: %s, then %s: %v", first, now, err)
	}
	if err := daemon.Start(ctx); err != nil {
		t.Fatal(err)
	}
	got := blockdiffOutcome{RequestsBefore: before, Source: trigger}
	deadline := time.Now().Add(blockdiffSyncWait)
	for {
		state, err := mds.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			break
		}
		attempt, attemptCancel := context.WithTimeout(ctx, 20*time.Second)
		status, err := mirror.DirectoryStatus(attempt, blockdiffDirectory)
		attemptCancel()
		got.Status = fmt.Sprintf("state %q, current %s, last synchronized %s, error %v", status.State, snapshotName(status.CurrentSnapshot), snapshotName(status.LastSyncedSnapshot), err)
		if last := status.LastSyncedSnapshot; last != nil && last.ID == trigger.ID && last.Name == trigger.Name {
			got.Synced = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}

	// Then: the MDS process is the same one and served the request, or it
	// ended.
	now, err := generation()
	got.Process, got.SameProcess = now, err == nil && now == first
	if err != nil {
		got.Process = fmt.Sprintf("%s (%v)", now, err)
	}
	if got.SameProcess {
		got.RequestsAfter, err = requests()
		got.RequestsRead = err == nil
		if err != nil {
			t.Logf("blockdiff request counter after the trigger: %v", err)
		}
	}
	got.Log = blockdiffLog(t, mds)
	got.Destination = arrived(trigger.Name)
	t.Logf("CEPHFS_MIRROR_BLOCKDIFF_EMPTY_VERSIONS_RECEIPT source MDS before: %s; after: %s; blockdiff requests %d before, %d after (read: %t); mirror: %s; destination snapshot %s present: %t",
		first, got.Process, got.RequestsBefore, got.RequestsAfter, got.RequestsRead, got.Status, trigger.Name, got.Destination.Present)
	symptoms, err := judgeBlockdiff(got)
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_mirror_blockdiff_empty_versions result=%s image=%s symptoms=%q", result, image, symptoms)
}

func snapshotName(snapshot *cephfsmirror.Snapshot) string {
	if snapshot == nil {
		return "none"
	}
	return fmt.Sprintf("%s(%d)", snapshot.Name, snapshot.ID)
}

// blockdiffLog returns the distinct lines of the MDS container's log that
// contain the interval assertion or name the function that aggregates object
// versions.
func blockdiffLog(t *testing.T, mds *cephfs.MDS) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	logs, err := mds.Logs(ctx)
	if err != nil {
		t.Logf("MDS log unavailable: %v", err)
		return nil
	}
	defer func() {
		if err := logs.Close(); err != nil {
			t.Logf("close MDS log: %v", err)
		}
	}()
	var found []string
	scanner := bufio.NewScanner(logs)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if (strings.Contains(line, blockdiffAssertion) || strings.Contains(line, blockdiffFrame)) && !slices.Contains(found, line) {
			found = append(found, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Logf("read MDS log: %v", err)
	}
	return found
}
