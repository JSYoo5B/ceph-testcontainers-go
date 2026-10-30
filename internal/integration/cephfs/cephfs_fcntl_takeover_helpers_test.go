//go:build all || integration

package cephfs_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

type fcntlTakeoverFD struct {
	StartTicks string `json:"start_ticks"`
	FD         *int   `json:"fd"`
	MountID    uint64 `json:"mount_id"`
	Device     uint64 `json:"device"`
	Written    int    `json:"written,omitempty"`
	Data       string `json:"data,omitempty"`
}

func fcntlTakeoverFDView(r harness.CephFSActorReceipt) (fcntlTakeoverFD, error) {
	var fd fcntlTakeoverFD
	if err := json.Unmarshal(r.Ops, &fd); err != nil {
		return fd, err
	}
	start, err := strconv.ParseUint(fd.StartTicks, 10, 64)
	if err != nil || start == 0 || r.PID <= 0 || r.Inode == 0 || fd.FD == nil || *fd.FD < 0 || fd.MountID == 0 || fd.Device == 0 {
		return fd, errors.New("incomplete fcntl process/fd/mount identity")
	}
	return fd, nil
}

func sameFcntlTakeoverFD(first, next harness.CephFSActorReceipt) error {
	a, err := fcntlTakeoverFDView(first)
	if err != nil {
		return err
	}
	b, err := fcntlTakeoverFDView(next)
	if err != nil {
		return err
	}
	if first.Token == "" || next.Token != first.Token || first.PID != next.PID || first.Inode != next.Inode ||
		a.StartTicks != b.StartTicks || *a.FD != *b.FD || a.MountID != b.MountID || a.Device != b.Device {
		return errors.New("fcntl actor changed process, fd, inode or mount")
	}
	return nil
}

func fcntlTakeoverConflict(before, after harness.CephFSActorReceipt) error {
	if !fcntlConflictResult(before.Errno) || !fcntlConflictResult(after.Errno) {
		return fmt.Errorf("overlapping lock was not refused before/after takeover: %d/%d", before.Errno, after.Errno)
	}
	return nil
}

func fcntlConflictResult(errno int) bool { return errno == 11 || errno == 13 }

// The remote FUSE client may report l_pid=0. Keep that field unchanged, but
// do not identify the holder from it: the holder-only unlock is the owner oracle.
func sameFcntlTakeoverLock(before, after *harness.FcntlLockView) error {
	if before == nil || after == nil || before.Type != 1 || after.Type != 1 ||
		before.Start != 0 || after.Start != 0 || before.Len != 4096 || after.Len != 4096 || before.PID != after.PID {
		return fmt.Errorf("F_GETLK did not retain the overlapping write lock: %+v -> %+v", before, after)
	}
	return nil
}
