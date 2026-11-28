package cephfs

import (
	"context"
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
	fsbase.OverrideRecord
	previous, applied int64
}

// maxFileSizeSubject names the max_file_size override in its refusals.
const maxFileSizeSubject = "max_file_size override"

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
	ctx, fsID, done, err := fs.base().AdmitOwned(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	registry := filesystems(fs.base().Cluster())
	if active := registry.maxFileSizes[fs]; active != nil && !active.Restored {
		return nil, fsbase.OverrideActive(maxFileSizeSubject)
	}
	v := fs.base().Volumes()
	previous, err := readMaxFileSize(ctx, v, fsID)
	if err != nil {
		return nil, err
	}
	state := &maxFileSizeState{OverrideRecord: fsbase.OverrideRecord{FSID: fsID}, previous: previous, applied: bytes}
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
		return fsbase.OverrideUnavailable(maxFileSizeSubject)
	}
	fs, state := change.filesystem, change.state
	core := fs.base()
	return core.RestoreOverride(ctx, fsbase.Override{
		Subject: maxFileSizeSubject,
		Record:  &state.OverrideRecord,
		Tracked: func() bool { return filesystems(core.Cluster()).maxFileSizes[fs] == state },
		Restore: func(ctx context.Context, fsID int64) error {
			v := core.Volumes()
			current, err := readMaxFileSize(ctx, v, fsID)
			if err != nil {
				return err
			}
			if current != state.previous && current != state.applied {
				return fmt.Errorf("CephFS max_file_size changed outside this override: %d", current)
			}
			return writeMaxFileSize(ctx, v, fsID, current, state.previous)
		},
		Forget: func() { delete(filesystems(core.Cluster()).maxFileSizes, fs) },
	})
}

// nativeMaxFileSize is the max_file_size field of an fs get mdsmap.
type nativeMaxFileSize struct {
	MaxFileSize *int64 `json:"max_file_size"`
}

func readMaxFileSize(ctx context.Context, v fsbase.VolumeCommands, fsID int64) (int64, error) {
	var native nativeMaxFileSize
	if err := v.ReadMDSMap(ctx, fsID, "max_file_size", &native); err != nil {
		return 0, err
	}
	return checkMaxFileSize(native)
}

func parseMaxFileSize(data []byte, fsID int64) (int64, error) {
	var native nativeMaxFileSize
	if err := fsbase.DecodeMDSMap(data, fsID, "max_file_size", &native); err != nil {
		return 0, err
	}
	return checkMaxFileSize(native)
}

func checkMaxFileSize(native nativeMaxFileSize) (int64, error) {
	if native.MaxFileSize == nil {
		return 0, errors.New("decode CephFS max_file_size")
	}
	return *native.MaxFileSize, nil
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
