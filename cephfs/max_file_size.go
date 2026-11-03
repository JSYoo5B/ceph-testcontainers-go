package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// MinMaxFileSize is the MDSMonitor's lower bound for max_file_size, in bytes.
const MinMaxFileSize = 64 << 10

// MaxFileSizeOverrideHandle restores the max_file_size that
// OverrideMaxFileSize changed. Copies share restoration state. Retain a
// non-nil result even on error: a lost reply can leave the value changed.
type MaxFileSizeOverrideHandle struct {
	filesystem *Filesystem
	state      *maxFileSizeState
}

// maxFileSizeState is the restoration state of one override. The setup gate
// guards it.
type maxFileSizeState struct {
	fsID              int64
	previous, applied int64
	restored          bool
}

// OverrideMaxFileSize sets the MDSMap max_file_size of the filesystem to
// bytes, so that a client write or truncate that would make a file larger
// than bytes fails with EFBIG while smaller files stay usable. bytes must be
// at least MinMaxFileSize. The limit is filesystem-wide; it is not a quota,
// does not count used capacity and does not shrink existing files. Clients
// check the limit from the MDSMap they hold, so mount the client under test
// after the call returns. The previous value is captured, the applied one
// read back and the FSMap ID of the filesystem bound to the handle. Only one
// handle per filesystem may be active.
//
//purpose:condition
func (fs *Filesystem) OverrideMaxFileSize(ctx context.Context, bytes int64) (*MaxFileSizeOverrideHandle, error) {
	if bytes < MinMaxFileSize {
		return nil, servicehook.Invalid(fmt.Errorf("CephFS max_file_size must be at least %d bytes", MinMaxFileSize))
	}
	ctx, fsID, done, err := fs.beginMaxFileSizeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	registry := filesystems(fs.base().Cluster())
	if active := registry.maxFileSizes[fs]; active != nil && !active.restored {
		return nil, errors.New("restore the existing CephFS max_file_size override first")
	}
	v := fs.base().Volumes()
	previous, err := readMaxFileSize(ctx, v, fsID)
	if err != nil {
		return nil, err
	}
	state := &maxFileSizeState{fsID: fsID, previous: previous, applied: bytes}
	registry.maxFileSizes[fs] = state
	return &MaxFileSizeOverrideHandle{filesystem: fs, state: state}, writeMaxFileSize(ctx, v, fsID, previous, bytes)
}

// Restore returns max_file_size to the value captured before the change. It
// is idempotent across handle copies. A value that is neither the applied nor
// the captured one is an outside edit and is refused rather than
// overwritten; a filesystem recreated under the same name is refused too.
//
//purpose:condition,lifetime
func (change *MaxFileSizeOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.state == nil {
		return errors.New("CephFS max_file_size override is unavailable")
	}
	fs, state := change.filesystem, change.state
	core := fs.base()
	// A confirmed restore stays complete after the filesystem or cluster is gone.
	if core.Cluster() != nil {
		var restored bool
		if err := core.WithSetupGate(ctx, func() { restored = state.restored }); err != nil {
			return err
		}
		if restored {
			return nil
		}
	}
	ctx, fsID, done, err := fs.beginMaxFileSizeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if state.restored {
		return nil
	}
	registry := filesystems(core.Cluster())
	if registry.maxFileSizes[fs] != state {
		return errors.New("CephFS max_file_size override is not tracked by this filesystem")
	}
	if fsID != state.fsID {
		return errors.New("CephFS filesystem identity changed since the max_file_size override")
	}
	v := core.Volumes()
	current, err := readMaxFileSize(ctx, v, fsID)
	if err != nil {
		return err
	}
	if current != state.previous && current != state.applied {
		return fmt.Errorf("CephFS max_file_size changed outside this override: %d", current)
	}
	if err := writeMaxFileSize(ctx, v, fsID, current, state.previous); err != nil {
		return err
	}
	state.restored = true
	delete(registry.maxFileSizes, fs)
	return nil
}

// beginMaxFileSizeOperation admits an operation on the original registered
// filesystem and returns its FSMap ID.
func (fs *Filesystem) beginMaxFileSizeOperation(ctx context.Context) (context.Context, int64, func(), error) {
	core := fs.base()
	var fsID int64
	ctx, done, err := core.Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "CephFS max_file_size requires the original registered filesystem and confirmed native identity",
		Check: func(context.Context) (bool, error) {
			if !core.Registered() || !core.DescriptorIntact() || core.Native() == nil {
				return false, nil
			}
			fsID = core.Native().ID
			return true, nil
		},
	})
	return ctx, fsID, done, err
}

func readMaxFileSize(ctx context.Context, v fsbase.VolumeCommands, fsID int64) (int64, error) {
	data, err := v.Cluster().Ceph(ctx, "fs", "get", v.Name(), "--format", "json")
	if err != nil {
		return 0, fmt.Errorf("read CephFS %q max_file_size: %w", v.Name(), err)
	}
	return parseMaxFileSize(data, fsID)
}

func parseMaxFileSize(data []byte, fsID int64) (int64, error) {
	var native struct {
		ID     int64 `json:"id"`
		MDSMap struct {
			MaxFileSize *int64 `json:"max_file_size"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.MDSMap.MaxFileSize == nil {
		return 0, errors.New("decode CephFS max_file_size")
	}
	if native.ID != fsID {
		return 0, fmt.Errorf("CephFS filesystem ID is %d, want %d", native.ID, fsID)
	}
	return *native.MDSMap.MaxFileSize, nil
}

// writeMaxFileSize changes a differing value and confirms the result with a
// readback that survives cancellation of the caller's context.
func writeMaxFileSize(ctx context.Context, v fsbase.VolumeCommands, fsID, current, target int64) error {
	if current != target {
		if _, err := v.Cluster().Ceph(ctx, "fs", "set", v.Name(), "max_file_size", strconv.FormatInt(target, 10)); err != nil {
			return fmt.Errorf("set CephFS %q max_file_size: %w", v.Name(), err)
		}
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stored, err := readMaxFileSize(readCtx, v, fsID)
	if err != nil {
		return err
	}
	if stored != target {
		return fmt.Errorf("CephFS max_file_size readback is %d, want %d", stored, target)
	}
	return nil
}
