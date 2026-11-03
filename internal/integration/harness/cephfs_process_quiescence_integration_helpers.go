//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Parse the complete result, including unrelated/new identities. Never infer
// old GID retirement by searching only its old address or current inventory.
func CephFSOriginalProcessWatchers(ctx context.Context, source *ceph.Container, pool string) (map[string]string, error) {
	client, err := source.ClientContainer(ctx)
	if err != nil {
		return nil, err
	}
	data, err := CephFSOriginalProcessExec(ctx, client, "rados", "--pool", pool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	addresses := make(map[string]bool)
	if strings.TrimSpace(string(data)) == "" {
		return result, nil
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "watcher=") || !strings.HasPrefix(fields[1], "client.") || !strings.HasPrefix(fields[2], "cookie=") {
			return nil, errors.New("complete native watcher output malformed")
		}
		address, gid := strings.TrimPrefix(fields[0], "watcher="), strings.TrimPrefix(fields[1], "client.")
		number, err := strconv.ParseUint(gid, 10, 64)
		_, cookieErr := strconv.ParseUint(strings.TrimPrefix(fields[2], "cookie="), 10, 64)
		_, duplicate := result[gid]
		if address == "" || err != nil || number == 0 || strconv.FormatUint(number, 10) != gid || cookieErr != nil || duplicate || addresses[address] {
			return nil, errors.New("complete native watcher identity is malformed or duplicated")
		}
		result[gid], addresses[address] = address, true
	}
	return result, nil
}

type CephFSOriginalProcessPolicy struct {
	Source, Destination     *ceph.Container
	SourceFS, DestinationFS *cephfs.Filesystem
	Expected                cephfsmirror.DirectoryStatus
	Directory, Client, Site string
	FSIDs                   [2]string
	Pools                   [2]int64
}

func (p *CephFSOriginalProcessPolicy) Capture(ctx context.Context) error {
	for i, cluster := range []*ceph.Container{p.Source, p.Destination} {
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil {
			return err
		}
		p.FSIDs[i] = strings.TrimSpace(string(data))
		filesystem := []*cephfs.Filesystem{p.SourceFS, p.DestinationFS}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", filesystem.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var fs struct {
			MDSMap *struct {
				Pool *int64 `json:"metadata_pool"`
			} `json:"mdsmap"`
		}
		if json.Unmarshal(data, &fs) != nil || fs.MDSMap == nil || fs.MDSMap.Pool == nil || *fs.MDSMap.Pool <= 0 {
			return errors.New("independent native metadata pool identity unavailable")
		}
		p.Pools[i] = *fs.MDSMap.Pool
	}
	if p.FSIDs[0] == "" || p.FSIDs[1] == "" || p.FSIDs[0] == p.FSIDs[1] {
		return errors.New("independent native cluster FSIDs unavailable or equal")
	}
	return nil
}

func (p CephFSOriginalProcessPolicy) Check(ctx context.Context, kind string) error {
	var sourcePeers map[string]json.RawMessage
	for i, cluster := range []*ceph.Container{p.Source, p.Destination} {
		data, err := cluster.Ceph(ctx, "fsid")
		if err := CephFSOriginalFSIDCheck([]string{"source", "destination"}[i], p.FSIDs[i], data, err); err != nil {
			return err
		}
		filesystem := []*cephfs.Filesystem{p.SourceFS, p.DestinationFS}[i]
		id := []int{p.Expected.SourceFilesystemID, p.Expected.DestinationFilesystemID}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", filesystem.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var fs struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &fs) != nil || fs.ID == nil || *fs.ID != id || fs.MDSMap == nil || fs.MDSMap.Name == nil || *fs.MDSMap.Name != filesystem.FilesystemName || fs.MDSMap.Pool == nil || *fs.MDSMap.Pool != p.Pools[i] {
			return errors.New("independent native original filesystem/metadata pool changed")
		}
		data, err = cluster.Ceph(ctx, "osd", "lspools", "--format", "json")
		if err != nil {
			return err
		}
		var pools []struct {
			ID   int64  `json:"poolnum"`
			Name string `json:"poolname"`
		}
		if json.Unmarshal(data, &pools) != nil {
			return errors.New("independent native pool catalog malformed")
		}
		matches := 0
		for _, pool := range pools {
			if pool.Name == filesystem.MetadataPool {
				if pool.ID != p.Pools[i] {
					return errors.New("independent native original metadata pool name was rebound")
				}
				matches++
			}
		}
		if matches != 1 {
			return errors.New("independent native original metadata pool name unavailable")
		}
		if i == 0 {
			if fs.MirrorInfo == nil || fs.MirrorInfo.Peers == nil {
				return errors.New("independent source MON mirroring peer policy unavailable")
			}
			sourcePeers = fs.MirrorInfo.Peers
		}
	}
	wantPeer := kind != "peer"
	if err := p.checkPeers(sourcePeers, true, wantPeer); err != nil {
		return err
	}
	data, err := p.Source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", p.SourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil || peers == nil {
		return errors.New("independent MGR peer policy malformed")
	}
	if err := p.checkPeers(peers, false, wantPeer); err != nil {
		return err
	}
	data, err = p.Source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", p.SourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var directories []string
	if json.Unmarshal(data, &directories) != nil || directories == nil {
		return errors.New("independent MGR directory policy malformed")
	}
	wantDirectory := kind != "directory"
	if len(directories) != map[bool]int{true: 1, false: 0}[wantDirectory] || (wantDirectory && directories[0] != p.Directory) {
		return errors.New("independent original directory policy differs")
	}
	return ctx.Err()
}

func (p CephFSOriginalProcessPolicy) checkPeers(peers map[string]json.RawMessage, remote, present bool) error {
	if peers == nil || len(peers) != map[bool]int{true: 1, false: 0}[present] {
		return errors.New("independent original peer policy presence differs")
	}
	if !present {
		return nil
	}
	data, found := peers[p.Expected.PeerID]
	if !found {
		return errors.New("independent original peer UUID substituted")
	}
	if remote {
		var entry struct {
			Remote json.RawMessage `json:"remote"`
		}
		if json.Unmarshal(data, &entry) != nil || len(entry.Remote) == 0 {
			return errors.New("independent original remote peer tuple unavailable")
		}
		data = entry.Remote
	}
	var tuple map[string]string
	if json.Unmarshal(data, &tuple) != nil {
		return errors.New("independent original peer tuple malformed")
	}
	key := "site_name"
	if remote {
		key = "cluster_name"
	}
	if tuple["client_name"] != p.Client || tuple[key] != p.Site || tuple["fs_name"] != p.DestinationFS.FilesystemName {
		return errors.New("independent original destination peer tuple substituted")
	}
	return nil
}

func CephFSOriginalProcessExec(ctx context.Context, client testcontainers.Container, args ...string) ([]byte, error) {
	code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("independent native command returned no output reader")
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		return nil, errors.Join(err, fmt.Errorf("independent native command exited %d", code))
	}
	return data, ctx.Err()
}

func CephFSWaitOriginalProcessBytes(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, snapshot, role, stage string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last error
	for {
		var data []byte
		if data, last = CephFSOriginalProcessExec(waitCtx, client, "python3", script, filesystem, "verify", directory, snapshot); last == nil {
			CephFSLogOriginalProcessBytes(t, data, filesystem, directory, snapshot, role, stage)
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("independent mirrored checkpoint bytes: %v", errors.Join(last, waitCtx.Err()))
		case <-time.After(time.Second):
		}
	}
}

func CephFSLogOriginalProcessBytes(t *testing.T, data []byte, filesystem, directory, snapshot, role, stage string) {
	t.Helper()
	var proof struct {
		Snapshot string `json:"snapshot"`
		Bytes    int    `json:"bytes"`
		SHA256   string `json:"sha256"`
	}
	// Validate the successful native JSON instead of merely preserving opaque
	// output. The source/destination payload and hash must match the frozen
	// expected bytes independently of the Python verifier's exit status.
	expected := []byte(strings.Repeat(snapshot+":"+directory+":", 1024))
	for value := 0; value < 256; value++ {
		expected = append(expected, byte(value))
	}
	digest := sha256.Sum256(expected)
	if err := json.Unmarshal(data, &proof); err != nil || proof.Snapshot != snapshot || proof.Bytes <= 0 || proof.Bytes != len(expected) || proof.SHA256 != fmt.Sprintf("%x", digest) || (role != "source" && role != "destination") {
		t.Fatalf("invalid positive original snapshot byte/hash proof: role=%s stage=%s output=%s error=%v", role, stage, data, err)
	}
	log, err := json.Marshal(struct {
		Case       string `json:"case"`
		Role       string `json:"role"`
		Stage      string `json:"stage"`
		Filesystem string `json:"filesystem"`
		Directory  string `json:"directory"`
		Snapshot   string `json:"snapshot"`
		Bytes      int    `json:"bytes"`
		SHA256     string `json:"sha256"`
	}{t.Name(), role, stage, filesystem, directory, proof.Snapshot, proof.Bytes, proof.SHA256})
	if err != nil {
		t.Fatal("encode positive original snapshot byte/hash proof", err)
	}
	t.Logf("CEPHFS_ORIGINAL_PROCESS_BYTES %s", log)
}

func CephFSAssertOriginalProcessSnapshotAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, snapshot, stage string, duration time.Duration) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(duration)
	checks := 0
	for {
		data := MultiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "absent", directory, snapshot)
		var proof struct {
			Snapshot string `json:"snapshot"`
			Absent   bool   `json:"absent"`
		}
		if err := json.Unmarshal(data, &proof); err != nil || proof.Snapshot != snapshot || !proof.Absent {
			t.Fatalf("invalid independent removed-authority snapshot absence proof: stage=%s output=%s error=%v", stage, data, err)
		}
		checks++
		if time.Now().After(deadline) && checks >= 2 {
			log, err := json.Marshal(struct {
				Case       string `json:"case"`
				Role       string `json:"role"`
				Stage      string `json:"stage"`
				Filesystem string `json:"filesystem"`
				Directory  string `json:"directory"`
				Snapshot   string `json:"snapshot"`
				Absent     bool   `json:"absent"`
				Count      int    `json:"count"`
				DurationMS int64  `json:"duration_ms"`
			}{t.Name(), "destination", stage, filesystem, directory, snapshot, true, checks, time.Since(started).Milliseconds()})
			if err != nil {
				t.Fatal("encode aggregate independent snapshot absence proof", err)
			}
			t.Logf("CEPHFS_ORIGINAL_PROCESS_ABSENCE %s", log)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

const CephFSOriginalProcessProbeScript = `import cephfs
import hashlib
import json
import os
import sys

filesystem, phase, directory, snapshot = sys.argv[1:]
assert directory == '/daemon-a'
assert snapshot and snapshot not in ('.', '..') and '/' not in snapshot and '\x00' not in snapshot
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())
root = directory.encode()
path = root + b'/.snap/' + snapshot.encode()
expected = (snapshot + ':' + directory + ':').encode() * 1024 + bytes(range(256))
try:
    if phase == 'checkpoint':
        try:
            fs.mkdir(root, 0o755)
        except cephfs.ObjectExists:
            pass
        fd = fs.open(root + b'/payload', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
        try:
            assert fs.write(fd, expected, 0) == len(expected)
            fs.fsync(fd, False)
        finally:
            fs.close(fd)
        fs.sync_fs()
        fs.mkdir(path, 0o755)
    elif phase == 'snapshot-checkpoint':
        info = fs.snap_info(path)
        assert type(info['id']) is int and 1 <= info['id'] <= 18446744073709551613
        print(json.dumps({'id': info['id'], 'name': snapshot}, sort_keys=True))
    elif phase == 'verify':
        fd = fs.open(path + b'/payload', os.O_RDONLY)
        try:
            actual = fs.read(fd, 0, len(expected) + 1)
            assert actual == expected, 'original snapshot bytes changed'
            print(json.dumps({'snapshot': snapshot, 'bytes': len(actual), 'sha256': hashlib.sha256(actual).hexdigest()}, sort_keys=True))
        finally:
            fs.close(fd)
    elif phase == 'absent':
        try:
            fs.stat(path)
        except cephfs.ObjectNotFound:
            print(json.dumps({'snapshot': snapshot, 'absent': True}, sort_keys=True))
        else:
            raise AssertionError('removed authority copied a new snapshot')
    else:
        raise AssertionError('unknown phase')
finally:
    fs.shutdown()
`
