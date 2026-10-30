package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// PinType selects one native directory policy. Policies can coexist;
// ancestry and Ceph's native precedence determine actual subtree placement.
type PinType string

const (
	PinExport      PinType = "export"
	PinDistributed PinType = "distributed"
	PinRandom      PinType = "random"
)

// PinSetting changes one policy field. Export accepts an integer owned
// active rank, or -1 to remove the local pin. Distributed accepts 0 or 1.
// Random accepts a probability in [0,1], additionally capped by the MDS's
// mds_export_ephemeral_random_max. Ephemeral feature configuration is caller-
// controlled; setting a policy does not prove its runtime effect.
type PinSetting struct {
	Type  PinType
	Value float64
}

// PinPolicy reports the local virtual xattrs on the directory targeted by
// the native volumes pin command. For subvolumes, Path is the base directory,
// one level above the UUID data directory returned by SubvolumeInfo. ExportRank
// -1, Distributed false and RandomProbability 0 are native virtual defaults.
// These local values do not report inherited policy or actual MDS authority.
// RandomProbability has the precision of native virtual-xattr text.
type PinPolicy struct {
	Path              string
	Inode             uint64
	ExportRank        int
	Distributed       bool
	RandomProbability float64
}

// PinOverrideHandle manages restoration of one confirmed owned resource's
// policy field, retaining prior state and possibly uncertain application. Copies share restoration state. Retain a non-nil result even
// on error: cancellation or a lost reply can leave native state changed.
// Explicit Restore returns that field to its prior canonical virtual value;
// other fields, directories and data are preserved. It refuses a different
// current field or native resource identity. Same-value outside edits and
// changes below native text precision cannot be distinguished; do not race
// external pin edits or delete/recreate the directory while the handle is used.
type PinOverrideHandle struct {
	filesystem *Filesystem
	target     cephFSPinTarget
	setting    PinSetting
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

// PinPolicy reads all three local fields on an owned subvolume's base
// directory through libcephfs inside the control container. No host mount or
// Go native dependency is required; the image must supply Python libcephfs.
func (volume *Subvolume) PinPolicy(ctx context.Context) (*PinPolicy, error) {
	fs, err := volume.filesystem()
	if err != nil {
		return nil, err
	}
	if volume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	return fs.readOwnedPinPolicy(ctx, cephFSPinTarget{identity: volume.identity})
}

// PinPolicy reads an owned group's local directory policies.
func (group *SubvolumeGroup) PinPolicy(ctx context.Context) (*PinPolicy, error) {
	fs, err := group.filesystem()
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, errors.New("subvolume group is unavailable")
	}
	return fs.readOwnedPinPolicy(ctx, cephFSPinTarget{identity: group.identity, group: true})
}

// OverridePin changes one local field using Ceph's volumes command.
// The native subvolume data path is left intact. Overlapping handles for the
// same target/field are rejected; independent fields may be changed separately.
func (volume *Subvolume) OverridePin(ctx context.Context, setting PinSetting) (*PinOverrideHandle, error) {
	fs, err := volume.filesystem()
	if err != nil {
		return nil, err
	}
	if volume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	return fs.temporaryPin(ctx, cephFSPinTarget{identity: volume.identity}, setting)
}

// OverridePin changes one owned group's local policy field.
func (group *SubvolumeGroup) OverridePin(ctx context.Context, setting PinSetting) (*PinOverrideHandle, error) {
	fs, err := group.filesystem()
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, errors.New("subvolume group is unavailable")
	}
	return fs.temporaryPin(ctx, cephFSPinTarget{identity: group.identity, group: true}, setting)
}

func (fs *Filesystem) readOwnedPinPolicy(ctx context.Context, target cephFSPinTarget) (*PinPolicy, error) {
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
	policy, err := fs.volumes().readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	_, err = fs.checkPinTarget(ctx, fsID, target)
	return policy, err
}

func (fs *Filesystem) temporaryPin(ctx context.Context, target cephFSPinTarget, setting PinSetting) (*PinOverrideHandle, error) {
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
	if fs.pins.tracked(key) != nil {
		return nil, errors.New("restore the existing directory pin override for this field first")
	}
	prior, err := fs.volumes().readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	if setting.Type == PinExport && setting.Value >= 0 {
		status, err := fs.MDSStatus(ctx)
		if err != nil {
			return nil, err
		}
		if status.FilesystemID != fsID || int(setting.Value) >= status.MaxMDS {
			return nil, errors.New("export pin rank is outside the current filesystem topology")
		}
		owned := false
		for _, active := range status.Active {
			owned = owned || active.Rank == int(setting.Value) && active.Owned && active.State == "up:active"
		}
		if !owned {
			return nil, errors.New("export pin requires a currently active MDS rank owned by this filesystem")
		}
	}
	change := &PinOverrideHandle{filesystem: fs, target: target, setting: setting, state: &cephFSPinOverrideState{
		path: base, inode: prior.Inode, previous: cephFSPinValue(prior, setting.Type), applied: canonicalCephFSPinValue(setting),
	}}
	fs.pins.track(key, change)
	if change.state.previous == change.state.applied {
		return change, nil
	}
	_, setErr := fs.volumes().pinCommand(ctx, target, setting.Type, setting.Value)
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
func (change *PinOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.filesystem.id.cluster == nil || change.state == nil {
		return errors.New("CephFS pin override is unavailable")
	}
	fs := change.filesystem
	if err := clusterutil.LockMutex(ctx, &filesystems(fs.id.cluster).setupMu); err != nil {
		return err
	}
	restored := change.state.restored
	filesystems(fs.id.cluster).setupMu.Unlock()
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
	if tracked := fs.pins.tracked(key); tracked == nil || tracked.state != change.state {
		return errors.New("directory pin override is not tracked by this filesystem")
	}
	current, err := fs.readAndCheckPinTarget(ctx, fsID, change.target, change.state.path, change.state.inode)
	if err != nil {
		return err
	}
	value := cephFSPinValue(current, change.setting.Type)
	if value == change.state.previous {
		change.state.restored = true
		fs.pins.forget(key)
		return nil
	}
	if value != change.state.applied {
		return servicehook.Invalid(errors.New("directory pin policy changed outside this override; refusing restoration"))
	}
	if _, err := fs.volumes().pinCommand(ctx, change.target, change.setting.Type, change.state.previous); err != nil {
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
	fs.pins.forget(key)
	return nil
}

func validateCephFSPinSetting(setting PinSetting) error {
	if math.IsNaN(setting.Value) || math.IsInf(setting.Value, 0) {
		return servicehook.Invalid(errors.New("directory pin value must be finite"))
	}
	switch setting.Type {
	case PinExport:
		if setting.Value < -1 || setting.Value > math.MaxInt32 || setting.Value != math.Trunc(setting.Value) {
			return servicehook.Invalid(errors.New("export pin must be an integer rank or -1"))
		}
	case PinDistributed:
		if setting.Value != 0 && setting.Value != 1 {
			return servicehook.Invalid(errors.New("distributed pin must be 0 or 1"))
		}
	case PinRandom:
		if setting.Value < 0 || setting.Value > 1 {
			return servicehook.Invalid(errors.New("random pin probability must be between 0 and 1"))
		}
	default:
		return servicehook.Invalid(errors.New("directory pin type must be export, distributed or random"))
	}
	return nil
}

func canonicalCephFSPinValue(setting PinSetting) float64 {
	if setting.Type != PinRandom {
		return setting.Value
	}
	// Native MDS get-vxattr prints the probability with stream precision.
	value, _ := strconv.ParseFloat(strconv.FormatFloat(setting.Value, 'g', 6, 64), 64)
	return value
}

func cephFSPinValue(policy *PinPolicy, kind PinType) float64 {
	switch kind {
	case PinExport:
		return float64(policy.ExportRank)
	case PinDistributed:
		if policy.Distributed {
			return 1
		}
		return 0
	default:
		return policy.RandomProbability
	}
}

func (fs *Filesystem) checkPinTarget(ctx context.Context, fsID int64, target cephFSPinTarget) (string, error) {
	if err := servicehook.LockTopology(ctx, fs.id.cluster); err != nil {
		return "", err
	}
	closed := servicehook.Closed(fs.id.cluster)
	servicehook.Unlock(fs.id.cluster)
	if closed {
		return "", servicehook.Invalid(errors.New("CephFS cluster is terminated"))
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
		info, err := fs.volumes().subvolumeGroupInfo(ctx, identity.name)
		if err != nil {
			return "", err
		}
		return identity.path, fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt)
	}
	info, err := fs.volumes().subvolumeInfo(ctx, identity.name, identity.group)
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

func (v volumeCommands) pinCommand(ctx context.Context, target cephFSPinTarget, kind PinType, value float64) ([]byte, error) {
	identity := target.identity
	args := []string{"fs", "subvolume", "pin", v.name, identity.name, string(kind), strconv.FormatFloat(value, 'g', -1, 64)}
	if target.group {
		args[1] = "subvolumegroup"
	} else {
		args = appendSubvolumeGroup(args, identity.group)
	}
	return v.cluster.Ceph(ctx, args...)
}

func (fs *Filesystem) readAndCheckPinTarget(ctx context.Context, fsID int64, target cephFSPinTarget, base string, inode uint64) (*PinPolicy, error) {
	currentBase, err := fs.checkPinTarget(ctx, fsID, target)
	if err != nil {
		return nil, err
	}
	if currentBase != base {
		return nil, errors.New("directory pin base path changed")
	}
	policy, err := fs.volumes().readPinPolicy(ctx, base)
	if err != nil {
		return nil, err
	}
	if policy.Inode != inode {
		return nil, errors.New("directory pin inode was replaced; refusing mutation")
	}
	_, err = fs.checkPinTarget(ctx, fsID, target)
	return policy, err
}

func (v volumeCommands) readPinPolicy(ctx context.Context, base string) (*PinPolicy, error) {
	control, err := v.cluster.ClientContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select CephFS pin CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "python3", "-c", cephFSPinReadScript, v.name, base)
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
	return &PinPolicy{Path: raw.Path, Inode: raw.Inode, ExportRank: export, Distributed: distributed == 1, RandomProbability: random}, nil
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

// pinOverrides is the set of directory pin overrides of one filesystem that
// are not restored yet, keyed by directory and pin field: one override per
// field, so an override never captures another's applied value as its
// previous one. The setup gate taken by every subvolume operation serializes
// its use, so it has no lock of its own.
type pinOverrides struct {
	byField map[string]*PinOverrideHandle
}

// tracked returns the override of key, or nil.
func (p *pinOverrides) tracked(key string) *PinOverrideHandle { return p.byField[key] }

// track records change as the override of key.
func (p *pinOverrides) track(key string, change *PinOverrideHandle) {
	if p.byField == nil {
		p.byField = make(map[string]*PinOverrideHandle)
	}
	p.byField[key] = change
}

// forget drops the override of key once it is restored.
func (p *pinOverrides) forget(key string) { delete(p.byField, key) }
