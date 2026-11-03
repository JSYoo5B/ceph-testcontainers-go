//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
)

func CephFSDirectoryAdditionAssertStatus(t *testing.T, status cephfsmirror.DirectoryAdditionStatus, expected cephfsmirror.DirectoryStatus, directory string, present, registered bool) {
	t.Helper()
	if status.Directory != directory || status.PeerID != expected.PeerID || status.SourceFilesystem != expected.SourceFilesystem || status.DestinationFilesystem != expected.DestinationFilesystem || status.SourceFilesystemID != expected.SourceFilesystemID || status.DestinationFilesystemID != expected.DestinationFilesystemID || status.PolicyPresent != present || status.Registered != registered {
		t.Fatalf("retained directory addition changed original identity or state: %+v", status)
	}
}

func CephFSDirectoryAdditionReconcile(t *testing.T, ctx context.Context, mirror *cephfsmirror.Mirror, directory string, receipt *cephfsmirror.DirectoryAddition) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		same, err := mirror.BeginDirectoryAddition(waitCtx, directory)
		if same != receipt {
			t.Fatal("addition convergence replaced retained original intent", err)
		}
		last = err
		if err == nil {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("reconcile original directory addition convergence", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func CephFSDirectoryAdditionWaitZeroWatchers(t *testing.T, ctx context.Context, source *ceph.Container, pool string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		watchers, err := CephFSOriginalProcessWatchers(waitCtx, source, pool)
		last = err
		if err == nil && len(watchers) == 0 {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("zero inventory retained a native receiving watcher: %v", errors.Join(last, waitCtx.Err()))
		case <-time.After(time.Second):
		}
	}
}

func CephFSDirectoryAdditionWaitScope(t *testing.T, ctx context.Context, p CephFSOriginalProcessPolicy, paths []string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		last = CephFSDirectoryAdditionCheckScope(waitCtx, p, paths)
		if last == nil {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("original directory addition policy convergence", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Independent native oracle preserves the original FSIDs, FS/metadata pool
// name+IDs, MON+MGR peer UUID/tuple, and strict full expected directory policy.
// It does not consult receipt state or production decoders.
func CephFSDirectoryAdditionCheckScope(ctx context.Context, p CephFSOriginalProcessPolicy, wantPaths []string) error {
	var sourcePeers map[string]json.RawMessage
	for i, cluster := range []*ceph.Container{p.Source, p.Destination} {
		// A failed read is no identity change; report it as what it is.
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil {
			return fmt.Errorf("read independent original addition cluster FSID: %w", err)
		}
		if strings.TrimSpace(string(data)) != p.FSIDs[i] {
			return errors.New("independent original addition cluster FSID changed")
		}
		fs := []*cephfs.Filesystem{p.SourceFS, p.DestinationFS}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var value struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &value) != nil || value.ID == nil || *value.ID != []int{p.Expected.SourceFilesystemID, p.Expected.DestinationFilesystemID}[i] || value.MDSMap == nil || value.MDSMap.Name == nil || *value.MDSMap.Name != fs.FilesystemName || value.MDSMap.Pool == nil || *value.MDSMap.Pool != p.Pools[i] {
			return errors.New("independent original addition filesystem/metadata pool changed")
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
			return errors.New("independent addition pool catalog malformed")
		}
		matches := 0
		for _, pool := range pools {
			if pool.Name == fs.MetadataPool {
				if pool.ID != p.Pools[i] {
					return errors.New("independent original addition metadata pool name rebound")
				}
				matches++
			}
		}
		if matches != 1 {
			return errors.New("independent original addition metadata pool unavailable")
		}
		if i == 0 {
			if value.MirrorInfo == nil || value.MirrorInfo.Peers == nil {
				return errors.New("independent addition MON peer policy unavailable")
			}
			sourcePeers = value.MirrorInfo.Peers
		}
	}
	if err := p.checkPeers(sourcePeers, true, true); err != nil {
		return err
	}
	data, err := p.Source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", p.SourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil {
		return errors.New("independent addition MGR peer policy malformed")
	}
	if err := p.checkPeers(peers, false, true); err != nil {
		return err
	}
	data, err = p.Source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", p.SourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var paths []string
	if json.Unmarshal(data, &paths) != nil || paths == nil {
		return errors.New("independent addition policy array malformed")
	}
	seen := make(map[string]bool)
	for _, directory := range paths {
		if !strings.HasPrefix(directory, "/") || path.Clean(directory) != directory || strings.ContainsAny(directory, "\x00\r\n") || seen[directory] {
			return errors.New("independent addition native path identity ambiguous")
		}
		seen[directory] = true
	}
	paths = slices.Clone(paths)
	wantPaths = slices.Clone(wantPaths)
	slices.Sort(paths)
	slices.Sort(wantPaths)
	if !slices.Equal(paths, wantPaths) {
		return fmt.Errorf("independent addition native paths=%v want=%v", paths, wantPaths)
	}
	return ctx.Err()
}
