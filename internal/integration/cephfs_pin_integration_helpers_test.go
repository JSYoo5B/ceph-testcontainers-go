//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func cephFSPinGroupLease(t *testing.T, ctx context.Context, fs *ceph.CephFSContainer, group *ceph.CephFSSubvolumeGroup, setting ceph.CephFSPinSetting) *ceph.CephFSPinOverride {
	t.Helper()
	change, err := fs.TemporarySubvolumeGroupPin(ctx, group, setting)
	cephFSPinTrackRestore(t, change)
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func cephFSPinVolumeLease(t *testing.T, ctx context.Context, fs *ceph.CephFSContainer, volume *ceph.CephFSSubvolume, setting ceph.CephFSPinSetting) *ceph.CephFSPinOverride {
	t.Helper()
	change, err := fs.TemporarySubvolumePin(ctx, volume, setting)
	cephFSPinTrackRestore(t, change)
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func cephFSPinTrackRestore(t *testing.T, change *ceph.CephFSPinOverride) {
	t.Helper()
	if change == nil {
		return
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := change.Restore(ctx); err != nil {
			t.Errorf("restore temporary directory pin: %v", err)
		}
	})
}

func cephFSPinWaitConfig(t *testing.T, parent context.Context, cluster *ceph.Container, fs *ceph.CephFSContainer, name, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	var last string
	for {
		status, err := fs.MDSStatus(ctx)
		ready := err == nil && len(status.Active) == 2
		if err == nil {
			for _, active := range status.Active {
				data, err := cluster.Ceph(ctx, "config", "show", "mds."+active.Name, name)
				got := strings.TrimSpace(string(data))
				if name == "mds_export_ephemeral_random_max" {
					number, parseErr := strconv.ParseFloat(got, 64)
					ready = ready && err == nil && parseErr == nil && number == 1
				} else {
					ready = ready && err == nil && got == value
				}
				last = fmt.Sprintf("daemon=%s value=%s error=%v", active.Name, got, err)
			}
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ephemeral feature configuration never reached native MDSs: %s", last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSPinWaitAuthority(t *testing.T, parent context.Context, cluster *ceph.Container, fs *ceph.CephFSContainer, bases map[string]bool, exportRank int, kind ceph.CephFSPinType) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last string
	for {
		status, err := fs.MDSStatus(ctx)
		ranks := make(map[int][]string)
		fragmentOwners := make(map[string]int)
		if err == nil {
			for _, active := range status.Active {
				if !active.Owned || active.State != "up:active" {
					continue
				}
				data, err := cluster.Ceph(ctx, "tell", "mds."+active.Name, "get", "subtrees", "--format", "json")
				last = fmt.Sprintf("daemon=%s error=%v output=%s", active.Name, err, data)
				if err != nil {
					continue
				}
				type subtree struct {
					IsAuth      bool `json:"is_auth"`
					AuthFirst   int  `json:"auth_first"`
					ExportPin   int  `json:"export_pin"`
					Target      int  `json:"export_pin_target"`
					Distributed bool `json:"distributed_ephemeral_pin"`
					Random      bool `json:"random_ephemeral_pin"`
					Dir         struct {
						Path     string `json:"path"`
						Fragment string `json:"dirfrag"`
					} `json:"dir"`
				}
				var subtrees []subtree
				if err := json.Unmarshal(data, &subtrees); err != nil {
					var wrapped struct {
						Subtrees []subtree `json:"subtrees"`
					}
					if json.Unmarshal(data, &wrapped) != nil {
						continue
					}
					subtrees = wrapped.Subtrees
				}
				for _, subtree := range subtrees {
					if !subtree.IsAuth || subtree.AuthFirst != active.Rank || !bases[subtree.Dir.Path] {
						continue
					}
					evidence := subtree.Dir.Path
					switch kind {
					case ceph.CephFSPinExport:
						if subtree.ExportPin != exportRank || active.Rank != exportRank {
							continue
						}
					case ceph.CephFSPinDistributed:
						if !subtree.Distributed || subtree.Target != active.Rank || subtree.Dir.Fragment == "" {
							continue
						}
						// Only count distinct authoritative native fragments;
						// a transient duplicate must not satisfy both ranks.
						evidence += "#" + subtree.Dir.Fragment
						if previous, exists := fragmentOwners[evidence]; exists && previous != active.Rank {
							delete(ranks, previous)
							continue
						}
						fragmentOwners[evidence] = active.Rank
					case ceph.CephFSPinRandom:
						if !subtree.Random || subtree.Target != active.Rank {
							continue
						}
					}
					ranks[active.Rank] = append(ranks[active.Rank], evidence)
				}
			}
		}
		if (exportRank < 0 && len(ranks[0]) > 0 && len(ranks[1]) > 0) || (exportRank >= 0 && len(ranks[exportRank]) > 0) {
			t.Logf("native subtree authority: exportRank=%d rank0=%v rank1=%v", exportRank, ranks[0], ranks[1])
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pin policy never distributed owned native subtrees across both active ranks: ranks=%v; %s", ranks, last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSPinIO(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, mode string, paths []string) {
	t.Helper()
	encoded, err := json.Marshal(paths)
	if err != nil {
		t.Fatal(err)
	}
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", cephFSPinIOScript, filesystem, mode, string(encoded)}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("fresh libcephfs pin fixture I/O failed: mode=%s exit=%d error=%v output=%s", mode, code, err, output)
	}
	t.Logf("native pin client bytes: %s", strings.TrimSpace(string(output)))
}

const cephFSPinIOScript = `import cephfs, hashlib, json, os, sys, threading
filesystem, mode, encoded = sys.argv[1:]
paths = json.loads(encoded)
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
    for path in paths:
        expected = (path.encode() + bytes(range(256))) * 16
        flags = os.O_RDONLY if mode == 'read' else os.O_CREAT | os.O_TRUNC | os.O_RDWR
        fd = fs.open(path + '/fixture', flags, 0o600)
        try:
            if mode == 'write':
                assert fs.write(fd, expected, 0) == len(expected), 'short native write'
                fs.fsync(fd, False)
            actual = fs.read(fd, 0, len(expected) + 1)
            assert actual == expected, 'native file bytes changed under pin policy'
        finally:
            fs.close(fd)
    print(json.dumps({'filesystem':filesystem, 'mode':mode, 'files':len(paths), 'last_sha256':hashlib.sha256(actual).hexdigest()}))
finally:
    fs.shutdown()
    deadline.cancel()
`
