//go:build all || (integration && multicluster)

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephFSMirrorSessionBoundary retains one actual session reply while restarting
// its container. Exec returns the exact original bytes and reader framing. The
// subsequent replay and confirmation commands run against the restarted daemon.
// No fixture lifecycle method is called while DirectoryStatus holds its lock.
type cephFSMirrorSessionBoundary struct {
	testcontainers.Container
	source                        *ceph.Container
	filesystem, directory, peerID string
	fired, completed              bool
	oldAddress, newAddress        string
	beforeMapping, afterMapping   []byte
	failure                       error
	replayObserved                bool
	replayCode                    int
	replayRaw                     []byte
	replayErr                     error
}

func (b *cephFSMirrorSessionBoundary) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := b.Container.Exec(ctx, args, opts...)
	if b.fired && len(args) == 9 && slices.Equal(args[:7], []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status"}) {
		b.replayObserved, b.replayCode, b.replayErr = true, code, err
		if err != nil {
			return code, reader, err
		}
		b.replayRaw, b.replayErr = io.ReadAll(reader)
		return code, bytes.NewReader(bytes.Clone(b.replayRaw)), b.replayErr
	}
	if b.fired || err != nil || code != 0 || len(args) != 7 || !slices.Equal(args[:6], []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status"}) {
		return code, reader, err
	}
	b.fired = true
	raw, err := io.ReadAll(reader)
	if err != nil {
		b.failure = err
		return code, nil, err
	}
	b.failure = b.restartBetweenReads(ctx, args, raw)
	if b.failure != nil {
		return code, nil, b.failure
	}
	return code, bytes.NewReader(bytes.Clone(raw)), nil
}

func (b *cephFSMirrorSessionBoundary) restartBetweenReads(ctx context.Context, args []string, raw []byte) error {
	old, err := cephFSBoundarySession(raw, b.peerID)
	if err != nil {
		return err
	}
	b.oldAddress = old.Address
	b.beforeMapping, err = b.source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", b.filesystem, b.directory)
	if err != nil {
		return err
	}
	grace := time.Second
	if err := b.Container.Stop(ctx, &grace); err != nil {
		return fmt.Errorf("stop native mirror at session-read boundary: %w", err)
	}
	if err := b.Container.Start(ctx); err != nil {
		return fmt.Errorf("start native mirror at session-read boundary: %w", err)
	}
	var lastErr error
	for {
		code, reader, err := b.Container.Exec(ctx, args, tcexec.Multiplexed())
		if err == nil {
			var out []byte
			out, err = io.ReadAll(reader)
			if err == nil && code == 0 {
				var current cephFSBoundarySessionIdentity
				current, err = cephFSBoundarySession(out, b.peerID)
				if err == nil && current.Address != old.Address && current.Peer == old.Peer {
					b.newAddress = current.Address
					b.afterMapping, err = b.source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", b.filesystem, b.directory)
					b.completed = err == nil
					return err
				}
			}
			lastErr = fmt.Errorf("new session: exit=%d data=%s error=%v", code, out, err)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("new native mirror session not observed: %w (last=%v)", ctx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type cephFSBoundarySessionIdentity struct {
	Address string
	Peer    struct {
		Client     string `json:"client_name"`
		Site       string `json:"cluster_name"`
		Filesystem string `json:"fs_name"`
	}
}

// cephFSBoundaryOutput decodes a copy for logging and identity checks without
// changing the exact plain or framed bytes returned to the original caller.
func cephFSBoundaryOutput(raw []byte) ([]byte, error) {
	if json.Valid(raw) {
		return bytes.Clone(raw), nil
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

func cephFSBoundarySession(raw []byte, peerID string) (cephFSBoundarySessionIdentity, error) {
	var result cephFSBoundarySessionIdentity
	data, err := cephFSBoundaryOutput(raw)
	if err != nil {
		return result, err
	}
	var native struct {
		Address string          `json:"rados_inst"`
		State   json.RawMessage `json:"state"`
		Peers   map[string]struct {
			Remote json.RawMessage `json:"remote"`
		} `json:"peers"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return result, err
	}
	if len(native.State) != 0 {
		return result, fmt.Errorf("native mirror session is unavailable: %s", native.State)
	}
	result.Address = native.Address
	if err := json.Unmarshal(native.Peers[peerID].Remote, &result.Peer); err != nil {
		return result, err
	}
	if result.Address == "" || result.Peer.Client == "" || result.Peer.Site == "" || result.Peer.Filesystem == "" {
		return result, fmt.Errorf("incomplete native mirror session identity: %s", data)
	}
	return result, nil
}

func checkCephFSMirrorSessionBoundary(t *testing.T, ctx context.Context, mirror *cephfsmirror.Container, source *ceph.Container, expected cephfsmirror.DirectoryStatus) {
	t.Helper()
	daemons := mirror.Daemons()
	if len(daemons) != 1 {
		t.Fatalf("session boundary requires one owned mirror daemon, got %d", len(daemons))
	}
	daemon := daemons[0]
	original := daemon.Container
	boundary := &cephFSMirrorSessionBoundary{Container: original, source: source, filesystem: expected.SourceFilesystem, directory: "/federation", peerID: expected.PeerID}
	// This scenario has not started its watch yet. Restore even on a failed
	// assertion so fixture cleanup always uses the original container handle.
	daemon.Container = boundary
	defer func() { daemon.Container = original }()
	status, err := mirror.DirectoryStatus(ctx, "/federation")
	daemon.Container = original
	if !boundary.fired || !boundary.completed || boundary.failure != nil {
		t.Fatalf("native session boundary was not completed: fired=%t completed=%t error=%v observation=%v", boundary.fired, boundary.completed, boundary.failure, err)
	}
	var before, after struct {
		State    string `json:"state"`
		Instance string `json:"instance_id"`
	}
	if json.Unmarshal(boundary.beforeMapping, &before) != nil || json.Unmarshal(boundary.afterMapping, &after) != nil {
		t.Fatalf("cannot decode actual boundary mappings: before=%s after=%s", boundary.beforeMapping, boundary.afterMapping)
	}
	staleMapping := before.State == "mapped" && before.Instance != "" && before == after
	t.Logf("CEPHFS_SESSION_READ_BOUNDARY old_session=%s new_session=%s old_mapping=%+v new_mapping=%+v unchanged_mapping_with_new_session=%t observed_ready=%t observation_error=%v", boundary.oldAddress, boundary.newAddress, before, after, staleMapping, status.Ready, err)
	replay, replayDecodeErr := cephFSBoundaryOutput(boundary.replayRaw)
	t.Logf("CEPHFS_SESSION_BOUNDARY_REPLAY queried=%t exit=%d read_error=%v framing_error=%v native_peer_status=%s", boundary.replayObserved, boundary.replayCode, boundary.replayErr, replayDecodeErr, replay)
	if err == nil || status.Ready || status.LastSyncedSnapshot != nil || status.CurrentSnapshot != nil || status.SnapshotsSynced != 0 {
		t.Fatalf("native mid-read restart published mixed replay evidence: %+v error=%v", status, err)
	}
	if _, err := mirror.WaitDirectoryReady(ctx, "/federation"); err != nil {
		t.Fatalf("normal observation did not recover after native read-boundary restart: %v", err)
	}
}
