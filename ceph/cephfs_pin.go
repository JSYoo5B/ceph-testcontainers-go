package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"time"
)

// CephFSPinType selects one native directory policy. Policies can coexist;
// ancestry and Ceph's native precedence determine actual subtree placement.
type CephFSPinType string

const (
	CephFSPinExport      CephFSPinType = "export"
	CephFSPinDistributed CephFSPinType = "distributed"
	CephFSPinRandom      CephFSPinType = "random"
)

// CephFSPinSetting changes one policy field. Export accepts an integer owned
// active rank, or -1 to remove the local pin. Distributed accepts 0 or 1.
// Random accepts a probability in [0,1], additionally capped by the MDS's
// mds_export_ephemeral_random_max. Ephemeral feature configuration is caller-
// controlled; setting a policy does not prove its runtime effect.
type CephFSPinSetting struct {
	Type  CephFSPinType
	Value float64
}

// CephFSPinPolicy reports the local virtual xattrs on the directory targeted by
// the native volumes pin command. For subvolumes, Path is the base directory,
// one level above the UUID data directory returned by SubvolumeInfo. ExportRank
// -1, Distributed false and RandomProbability 0 are native virtual defaults.
// These local values do not report inherited policy or actual MDS authority.
// RandomProbability has the precision of native virtual-xattr text.
type CephFSPinPolicy struct {
	Path              string
	Inode             uint64
	ExportRank        int
	Distributed       bool
	RandomProbability float64
}

// CephFSPinOverride owns a temporary change to one confirmed owned resource's
// policy field. Copies share restoration state. Retain a non-nil result even
// on error: cancellation or a lost reply can leave native state changed.
// Explicit Restore returns that field to its prior canonical virtual value;
// other fields, directories and data are preserved. It refuses a different
// current field or native resource identity. Same-value outside edits and
// changes below native text precision cannot be distinguished; do not race
// external pin edits or delete/recreate the directory while the handle is used.
type CephFSPinOverride struct {
	filesystem *CephFSContainer
	target     cephFSPinTarget
	setting    CephFSPinSetting
	state      *cephFSPinOverrideState
}

type cephFSPinTarget struct {
	identity *cephFSVolumeIdentity
	group    bool
}

type cephFSPinOverrideState struct {
	path              string
	inode             uint64
	previous, applied float64
	restored          bool
}

// SubvolumePinPolicy reads all three local fields on an owned subvolume's base
// directory through libcephfs inside the control container. No host mount or
// Go native dependency is required; the image must supply Python libcephfs.
func (fs *CephFSContainer) SubvolumePinPolicy(ctx context.Context, volume *CephFSSubvolume) (*CephFSPinPolicy, error) {
	if volume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	return fs.readOwnedPinPolicy(ctx, cephFSPinTarget{identity: volume.identity})
}

// SubvolumeGroupPinPolicy reads an owned group's local directory policies.
func (fs *CephFSContainer) SubvolumeGroupPinPolicy(ctx context.Context, group *CephFSSubvolumeGroup) (*CephFSPinPolicy, error) {
	if group == nil {
		return nil, errors.New("subvolume group is unavailable")
	}
	return fs.readOwnedPinPolicy(ctx, cephFSPinTarget{identity: group.identity, group: true})
}

// TemporarySubvolumePin changes one local field using Ceph's volumes command.
// The native subvolume data path is left intact. Overlapping handles for the
// same target/field are rejected; independent fields may be changed separately.
func (fs *CephFSContainer) TemporarySubvolumePin(ctx context.Context, volume *CephFSSubvolume, setting CephFSPinSetting) (*CephFSPinOverride, error) {
	if volume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	return fs.temporaryPin(ctx, cephFSPinTarget{identity: volume.identity}, setting)
}

// TemporarySubvolumeGroupPin changes one owned group's local policy field.
func (fs *CephFSContainer) TemporarySubvolumeGroupPin(ctx context.Context, group *CephFSSubvolumeGroup, setting CephFSPinSetting) (*CephFSPinOverride, error) {
	if group == nil {
		return nil, errors.New("subvolume group is unavailable")
	}
	return fs.temporaryPin(ctx, cephFSPinTarget{identity: group.identity, group: true}, setting)
}

func (fs *CephFSContainer) readOwnedPinPolicy(ctx context.Context, target cephFSPinTarget) (*CephFSPinPolicy, error) {
	if err := fs.validateVolumeHandle(target.identity); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	base, err := fs.checkPinTarget(ctx, fsID, target)
	if err != nil {
		return nil, err
	}
	policy, err := fs.readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	_, err = fs.checkPinTarget(ctx, fsID, target)
	return policy, err
}

func (fs *CephFSContainer) temporaryPin(ctx context.Context, target cephFSPinTarget, setting CephFSPinSetting) (*CephFSPinOverride, error) {
	if err := validateCephFSPinSetting(setting); err != nil {
		return nil, err
	}
	if err := fs.validateVolumeHandle(target.identity); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	base, err := fs.checkPinTarget(ctx, fsID, target)
	if err != nil {
		return nil, err
	}
	key := base + "\x00" + string(setting.Type)
	if fs.pinOverrides[key] != nil {
		return nil, errors.New("restore the existing directory pin override for this field first")
	}
	prior, err := fs.readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	if err := fs.validatePinRank(ctx, fsID, setting); err != nil {
		return nil, err
	}
	change := &CephFSPinOverride{filesystem: fs, target: target, setting: setting, state: &cephFSPinOverrideState{
		path: base, inode: prior.Inode, previous: cephFSPinValue(prior, setting.Type), applied: canonicalCephFSPinValue(setting),
	}}
	if fs.pinOverrides == nil {
		fs.pinOverrides = make(map[string]*CephFSPinOverride)
	}
	fs.pinOverrides[key] = change
	if change.state.previous == change.state.applied {
		return change, nil
	}
	_, setErr := fs.pinCommand(ctx, target, setting.Type, setting.Value)
	readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	current, readErr := fs.readAndCheckPinTarget(readCtx, fsID, target, base, prior.Inode)
	if readErr == nil && cephFSPinValue(current, setting.Type) != change.state.applied {
		readErr = errors.New("directory pin readback differs from requested policy; inspect native state before restoration")
	}
	return change, errors.Join(setErr, readErr)
}

// Restore confirms the same owned directory/inode and returns only this field
// to its previous canonical native value. It is idempotent across handle copies;
// after an uncertain reply, retry with a fresh context to reconcile readback.
func (change *CephFSPinOverride) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.filesystem.cluster == nil || change.state == nil {
		return errors.New("CephFS pin override is unavailable")
	}
	fs := change.filesystem
	fs.cluster.cephfsSetupMu.Lock()
	restored := change.state.restored
	fs.cluster.cephfsSetupMu.Unlock()
	if restored {
		return nil
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if change.state.restored {
		return nil
	}
	key := change.state.path + "\x00" + string(change.setting.Type)
	if tracked := fs.pinOverrides[key]; tracked == nil || tracked.state != change.state {
		return errors.New("directory pin override is not tracked by this filesystem")
	}
	current, err := fs.readAndCheckPinTarget(ctx, fsID, change.target, change.state.path, change.state.inode)
	if err != nil {
		return err
	}
	value := cephFSPinValue(current, change.setting.Type)
	if value == change.state.previous {
		change.state.restored = true
		delete(fs.pinOverrides, key)
		return nil
	}
	if value != change.state.applied {
		return errors.New("directory pin policy changed outside this override; refusing restoration")
	}
	if _, err := fs.pinCommand(ctx, change.target, change.setting.Type, change.state.previous); err != nil {
		return err
	}
	current, err = fs.readAndCheckPinTarget(ctx, fsID, change.target, change.state.path, change.state.inode)
	if err != nil {
		return err
	}
	if cephFSPinValue(current, change.setting.Type) != change.state.previous {
		return errors.New("directory pin restoration readback differs from prior policy")
	}
	change.state.restored = true
	delete(fs.pinOverrides, key)
	return nil
}

func validateCephFSPinSetting(setting CephFSPinSetting) error {
	if math.IsNaN(setting.Value) || math.IsInf(setting.Value, 0) {
		return errors.New("directory pin value must be finite")
	}
	switch setting.Type {
	case CephFSPinExport:
		if setting.Value < -1 || setting.Value > math.MaxInt32 || setting.Value != math.Trunc(setting.Value) {
			return errors.New("export pin must be an integer rank or -1")
		}
	case CephFSPinDistributed:
		if setting.Value != 0 && setting.Value != 1 {
			return errors.New("distributed pin must be 0 or 1")
		}
	case CephFSPinRandom:
		if setting.Value < 0 || setting.Value > 1 {
			return errors.New("random pin probability must be between 0 and 1")
		}
	default:
		return errors.New("directory pin type must be export, distributed or random")
	}
	return nil
}

func canonicalCephFSPinValue(setting CephFSPinSetting) float64 {
	if setting.Type != CephFSPinRandom {
		return setting.Value
	}
	// Native MDS get-vxattr prints the probability with stream precision.
	value, _ := strconv.ParseFloat(strconv.FormatFloat(setting.Value, 'g', 6, 64), 64)
	return value
}

func cephFSPinValue(policy *CephFSPinPolicy, kind CephFSPinType) float64 {
	switch kind {
	case CephFSPinExport:
		return float64(policy.ExportRank)
	case CephFSPinDistributed:
		if policy.Distributed {
			return 1
		}
		return 0
	default:
		return policy.RandomProbability
	}
}

func (fs *CephFSContainer) checkPinTarget(ctx context.Context, fsID int64, target cephFSPinTarget) (string, error) {
	fs.cluster.mu.Lock()
	closed := fs.cluster.closed
	fs.cluster.mu.Unlock()
	if closed {
		return "", errors.New("CephFS cluster is terminated")
	}
	if err := fs.validateVolumeHandle(target.identity); err != nil || target.identity.removed {
		return "", errors.New("directory pin target must remain a confirmed owned resource")
	}
	state, err := fs.readNativePools(ctx)
	if err != nil || state.id != fsID {
		return "", errors.Join(err, errors.New("filesystem identity changed during directory pin operation"))
	}
	identity := target.identity
	if target.group {
		info, err := fs.subvolumeGroupInfo(ctx, identity.name)
		if err != nil {
			return "", err
		}
		return identity.path, fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt)
	}
	info, err := fs.subvolumeInfo(ctx, identity.name, identity.group)
	if err != nil {
		return "", err
	}
	if err := fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt); err != nil {
		return "", err
	}
	base := path.Dir(identity.path)
	if path.Base(base) != identity.name {
		return "", errors.New("owned subvolume data path does not identify the native pin base directory")
	}
	return base, nil
}

func (fs *CephFSContainer) validatePinRank(ctx context.Context, fsID int64, setting CephFSPinSetting) error {
	if setting.Type != CephFSPinExport || setting.Value < 0 {
		return nil
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		return err
	}
	if status.FilesystemID != fsID || int(setting.Value) >= status.MaxMDS {
		return errors.New("export pin rank is outside the current filesystem topology")
	}
	for _, active := range status.Active {
		if active.Rank == int(setting.Value) && active.Owned && active.State == "up:active" {
			return nil
		}
	}
	return errors.New("export pin requires a currently active MDS rank owned by this filesystem")
}

func (fs *CephFSContainer) pinCommand(ctx context.Context, target cephFSPinTarget, kind CephFSPinType, value float64) ([]byte, error) {
	identity := target.identity
	args := []string{"fs", "subvolume", "pin", fs.config.Name, identity.name, string(kind), strconv.FormatFloat(value, 'g', -1, 64)}
	if target.group {
		args[1] = "subvolumegroup"
	} else {
		args = appendSubvolumeGroup(args, identity.group)
	}
	return fs.cluster.Ceph(ctx, args...)
}

func (fs *CephFSContainer) readAndCheckPinTarget(ctx context.Context, fsID int64, target cephFSPinTarget, base string, inode uint64) (*CephFSPinPolicy, error) {
	currentBase, err := fs.checkPinTarget(ctx, fsID, target)
	if err != nil {
		return nil, err
	}
	if currentBase != base {
		return nil, errors.New("directory pin base path changed")
	}
	policy, err := fs.readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	if policy.Inode != inode {
		return nil, errors.New("directory pin inode was replaced; refusing mutation")
	}
	_, err = fs.checkPinTarget(ctx, fsID, target)
	return policy, err
}

func (fs *CephFSContainer) readPinPolicy(ctx context.Context, base string) (*CephFSPinPolicy, error) {
	data, err := command(ctx, fs.cluster.cliContainer(), "python3", "-c", cephFSPinReadScript, fs.config.Name, base)
	if err != nil {
		return nil, fmt.Errorf("read native CephFS directory pins: %w", err)
	}
	var raw struct {
		Path                        string `json:"path"`
		Inode                       uint64 `json:"inode"`
		Export, Distributed, Random *string
	}
	if json.Unmarshal(data, &raw) != nil || raw.Path != base || raw.Inode == 0 || raw.Export == nil || raw.Distributed == nil || raw.Random == nil {
		return nil, errors.New("decode native directory pin policy: incomplete path/inode/xattrs")
	}
	export, exportErr := strconv.Atoi(*raw.Export)
	distributed, distributedErr := strconv.Atoi(*raw.Distributed)
	random, randomErr := strconv.ParseFloat(*raw.Random, 64)
	if exportErr != nil || distributedErr != nil || randomErr != nil || export < -1 || (distributed != 0 && distributed != 1) || math.IsNaN(random) || math.IsInf(random, 0) || random < 0 || random > 1 {
		return nil, errors.New("decode native directory pin policy: invalid virtual values")
	}
	return &CephFSPinPolicy{Path: raw.Path, Inode: raw.Inode, ExportRank: export, Distributed: distributed == 1, RandomProbability: random}, nil
}

const cephFSPinReadScript = `import cephfs, json, os, stat, sys, threading
filesystem, base = sys.argv[1:]
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
    status = fs.statx(base, cephfs.CEPH_STATX_INO | cephfs.CEPH_STATX_MODE, cephfs.AT_SYMLINK_NOFOLLOW)
    if not stat.S_ISDIR(status['mode']): raise ValueError('pin target is not a directory')
    result = {'path':base, 'inode':status['ino']}
    for field, xattr in [('export', 'ceph.dir.pin'), ('distributed', 'ceph.dir.pin.distributed'), ('random', 'ceph.dir.pin.random')]:
        result[field] = fs.getxattr(base, xattr).decode('utf-8')
    print(json.dumps(result))
finally:
    fs.shutdown()
    deadline.cancel()
`
