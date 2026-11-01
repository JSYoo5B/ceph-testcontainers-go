package fsbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

// VolumeCommands runs a filesystem's native commands: volume, subvolume,
// snapshot, pin, session and schedule reads and writes, each addressed to the
// filesystem's authoritative name. It holds only the owning cluster and that
// name, both fixed when the descriptor is published, so it needs no lock of
// its own; admission, ownership checks and the cluster lock stay with the
// operation that calls it.
type VolumeCommands struct {
	cluster *ceph.Container
	name    string
}

// Cluster returns the cluster the commands run on.
func (v VolumeCommands) Cluster() *ceph.Container { return v.cluster }

// Name returns the filesystem name the commands address.
func (v VolumeCommands) Name() string { return v.name }

// GroupInfo is the native metadata of one subvolume group, in the field order
// of subvolume.GroupMetadata, which converts it.
type GroupInfo struct {
	Name       string
	Path       string
	DataPool   string
	QuotaBytes int64
	BytesUsed  int64
	CreatedAt  string
}

// SubvolumeInfo is the native metadata of one subvolume, in the field order
// of subvolume.Metadata, which converts it.
type SubvolumeInfo struct {
	Name          string
	GroupName     string
	Path          string
	DataPool      string
	PoolNamespace string
	QuotaBytes    int64
	BytesUsed     int64
	CreatedAt     string
	State         string
	Type          string
}

// EnsureVolumesModule enables only the MGR volumes module, then waits for its
// CLI to recognize the existing filesystem; fs volume create would invoke an
// orchestrator and create resources outside the fixture's ownership.
func (v VolumeCommands) EnsureVolumesModule(ctx context.Context) error {
	data, err := v.cluster.Ceph(ctx, "mgr", "module", "ls", "--format", "json")
	if err != nil {
		return fmt.Errorf("inspect MGR volumes module: %w", err)
	}
	var modules struct {
		Enabled  []string        `json:"enabled_modules"`
		AlwaysOn json.RawMessage `json:"always_on_modules"`
	}
	if err := json.Unmarshal(data, &modules); err != nil {
		return fmt.Errorf("decode MGR modules: %w", err)
	}
	if modules.Enabled == nil && len(modules.AlwaysOn) == 0 {
		return errors.New("decode MGR modules: missing enabled/always-on module listing")
	}
	enabled := slices.Contains(modules.Enabled, "volumes")
	var always []string
	if json.Unmarshal(modules.AlwaysOn, &always) == nil {
		enabled = enabled || slices.Contains(always, "volumes")
	}
	if !enabled {
		if _, err := v.cluster.Ceph(ctx, "mgr", "module", "enable", "volumes"); err != nil {
			return fmt.Errorf("enable MGR volumes module: %w", err)
		}
	}
	return clusterutil.Poll(ctx, func() (bool, error) {
		data, err := v.cluster.Ceph(ctx, "fs", "volume", "ls", "--format", "json")
		if err != nil {
			return false, err
		}
		names, err := DecodeVolumeNames(data)
		return slices.Contains(names, v.name), err
	})
}

// DecodeVolumeNames decodes a volumes-module listing of named entries into
// sorted names, refusing an empty or duplicate name.
func DecodeVolumeNames(data []byte) ([]string, error) {
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, fmt.Errorf("decode CephFS resource listing: expected JSON array (error: %v)", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Name == "" || slices.Contains(names, entry.Name) {
			return nil, errors.New("decode CephFS resource listing: empty or duplicate name")
		}
		names = append(names, entry.Name)
	}
	slices.Sort(names)
	return names, nil
}

// SubvolumeNames lists the subvolumes of group; empty selects the default group.
func (v VolumeCommands) SubvolumeNames(ctx context.Context, group string) ([]string, error) {
	args := AppendGroup([]string{"fs", "subvolume", "ls", v.name}, group)
	data, err := v.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	return DecodeVolumeNames(data)
}

// SubvolumeGroupNames lists the filesystem's subvolume groups.
func (v VolumeCommands) SubvolumeGroupNames(ctx context.Context) ([]string, error) {
	data, err := v.cluster.Ceph(ctx, "fs", "subvolumegroup", "ls", v.name, "--format", "json")
	if err != nil {
		return nil, err
	}
	return DecodeVolumeNames(data)
}

// AppendGroup appends the --group_name argument of a non-default group.
func AppendGroup(args []string, group string) []string {
	if group != "" {
		args = append(args, "--group_name", group)
	}
	return args
}

type rawVolumeInfo struct {
	Path          string          `json:"path"`
	DataPool      string          `json:"data_pool"`
	PoolNamespace string          `json:"pool_namespace"`
	Quota         json.RawMessage `json:"bytes_quota"`
	BytesUsed     int64           `json:"bytes_used"`
	CreatedAt     string          `json:"created_at"`
	State         string          `json:"state"`
	Type          string          `json:"type"`
}

func decodeVolumeQuota(raw json.RawMessage) (int64, error) {
	if string(raw) == `"infinite"` {
		return 0, nil
	}
	var quota int64
	if err := json.Unmarshal(raw, &quota); err != nil || quota < 0 || string(raw) == "null" {
		return 0, errors.New("decode CephFS quota: expected a non-negative integer or infinite")
	}
	return quota, nil
}

func validateVolumeInfo(raw rawVolumeInfo) error {
	if raw.Path == "" || !path.IsAbs(raw.Path) || path.Clean(raw.Path) != raw.Path || raw.Path == "/" || raw.CreatedAt == "" || raw.DataPool == "" || raw.BytesUsed < 0 {
		return errors.New("decode CephFS volume metadata: missing or invalid path, birth time, pool or used bytes")
	}
	return nil
}

// SubvolumeGroupInfo reads one group's native metadata and path.
func (v VolumeCommands) SubvolumeGroupInfo(ctx context.Context, name string) (*GroupInfo, error) {
	data, err := v.cluster.Ceph(ctx, "fs", "subvolumegroup", "info", v.name, name, "--format", "json")
	if err != nil {
		return nil, err
	}
	var raw rawVolumeInfo
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode subvolume group info: %w", err)
	}
	data, err = v.cluster.Ceph(ctx, "fs", "subvolumegroup", "getpath", v.name, name)
	if err != nil {
		return nil, err
	}
	raw.Path = strings.TrimSpace(string(data))
	if err := validateVolumeInfo(raw); err != nil {
		return nil, err
	}
	quota, err := decodeVolumeQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	return &GroupInfo{Name: name, Path: raw.Path, DataPool: raw.DataPool, QuotaBytes: quota, BytesUsed: raw.BytesUsed, CreatedAt: raw.CreatedAt}, nil
}

// SubvolumeInfo reads one subvolume's native metadata. A snapshot-retained
// entry carries only its name, group, state and type.
func (v VolumeCommands) SubvolumeInfo(ctx context.Context, name, group string) (*SubvolumeInfo, error) {
	args := AppendGroup([]string{"fs", "subvolume", "info", v.name, name}, group)
	data, err := v.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	var raw rawVolumeInfo
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode subvolume info: %w", err)
	}
	if raw.State == "snapshot-retained" {
		return &SubvolumeInfo{Name: name, GroupName: group, State: raw.State, Type: raw.Type}, nil
	}
	if err := validateVolumeInfo(raw); err != nil {
		return nil, err
	}
	quota, err := decodeVolumeQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	return &SubvolumeInfo{Name: name, GroupName: group, Path: raw.Path, DataPool: raw.DataPool, PoolNamespace: raw.PoolNamespace, QuotaBytes: quota, BytesUsed: raw.BytesUsed, CreatedAt: raw.CreatedAt, State: raw.State, Type: raw.Type}, nil
}
