package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
)

// RefusalOverrideHandle restores the refuse_client_session flag that
// OverrideRefusal set. Copies share restoration state. Retain a non-nil
// result even on error: a lost reply can leave the flag set.
type RefusalOverrideHandle struct {
	filesystem *fsbase.Core
	state      *refusalState
}

type refusalState struct {
	fsbase.OverrideRecord
}

// refusalSubject names the new-session refusal in its refusals.
const refusalSubject = "new-session refusal"

type nativeRefusal struct {
	Flags struct {
		RefuseClientSession *bool `json:"refuse_client_session"`
	} `json:"flags_state"`
}

// OverrideRefusal sets the MDSMap flag refuse_client_session of fs, so that
// every MDS rank of the filesystem rejects a client that opens a new session:
// the client's mount fails with EPERM. Sessions that are already open keep
// working. The handle owns only the change from admitting to refusing and is
// bound to the filesystem's FSMap ID; a filesystem that already refuses new
// sessions is refused before any change. Only one handle per filesystem may
// be active.
//
// While the flag is set, every new session is refused, including the
// fixture's own short-lived libcephfs probes and a session the MGR volumes
// module opens, so restore it before such fixture operations. An open
// session that must reconnect to a restarted or replaced MDS while the flag
// is set is refused like a new one: the MDS drops it and blocklists the
// client, whose I/O does not come back after Restore. A cluster whose MDSMap
// does not report the flag returns ceph.ErrUnsupported before any change.
//
//purpose:condition
func OverrideRefusal(ctx context.Context, fs *cephfs.Filesystem) (*RefusalOverrideHandle, error) {
	return overrideRefusal(ctx, coreOf(fs))
}

func overrideRefusal(ctx context.Context, fs *fsbase.Core) (*RefusalOverrideHandle, error) {
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	overrides := overridesOf(fs)
	if active := overrides.refusals[fs]; active != nil && !active.Restored {
		return nil, fsbase.OverrideActive(refusalSubject)
	}
	refused, err := readRefusal(ctx, fs.Volumes(), fsID)
	if err != nil {
		return nil, err
	}
	if refused {
		return nil, fmt.Errorf("CephFS %q already refuses new client sessions; the override would own no change", fs.Name())
	}
	state := &refusalState{OverrideRecord: fsbase.OverrideRecord{FSID: fsID}}
	overrides.refusals[fs] = state
	change := &RefusalOverrideHandle{filesystem: fs, state: state}
	return change, writeRefusal(ctx, fs.Volumes(), fsID, true)
}

// Restore clears refuse_client_session again, so new sessions are admitted.
// It is idempotent across handle copies. A flag that is already clear is
// accepted without a command. A filesystem recreated under the same name is
// refused, and its flag is left alone.
//
//purpose:condition,lifetime
func (change *RefusalOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.state == nil {
		return fsbase.OverrideUnavailable(refusalSubject)
	}
	fs, state := change.filesystem, change.state
	return fs.RestoreOverride(ctx, fsbase.Override{
		Subject: refusalSubject,
		Record:  &state.OverrideRecord,
		Tracked: func() bool { return overridesOf(fs).refusals[fs] == state },
		Restore: func(ctx context.Context, fsID int64) error {
			refused, err := readRefusal(ctx, fs.Volumes(), fsID)
			if err != nil || !refused {
				return err
			}
			return writeRefusal(ctx, fs.Volumes(), fsID, false)
		},
		Forget: func() { delete(overridesOf(fs).refusals, fs) },
	})
}

func readRefusal(ctx context.Context, v fsbase.VolumeCommands, fsID int64) (bool, error) {
	var native nativeRefusal
	err := v.ReadMDSMap(ctx, fsID, "refuse_client_session", &native)
	refused := false
	if err == nil {
		refused, err = checkRefusal(native)
	}
	if errors.Is(err, errNoRefusalFlag) {
		return false, unsupported(v.Cluster(), "fs set refuse_client_session", err)
	}
	return refused, err
}

var errNoRefusalFlag = errors.New("the MDSMap reports no refuse_client_session flag")

func parseRefusal(data []byte, fsID int64) (bool, error) {
	var native nativeRefusal
	if err := fsbase.DecodeMDSMap(data, fsID, "refuse_client_session", &native); err != nil {
		return false, err
	}
	return checkRefusal(native)
}

func checkRefusal(native nativeRefusal) (bool, error) {
	if native.Flags.RefuseClientSession == nil {
		return false, errNoRefusalFlag
	}
	return *native.Flags.RefuseClientSession, nil
}

// unsupported reports that the cluster's release lacks capability.
func unsupported(cluster *ceph.Container, capability string, cause error) error {
	err := &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: "cluster", Capability: capability, Err: cause}
	if facts, factsErr := ceph.FactsOf(cluster); factsErr == nil {
		err.Version = facts.CephVersion
	}
	return err
}

// writeRefusal sets the flag and confirms it with a readback that survives
// cancellation of the caller's context.
func writeRefusal(ctx context.Context, v fsbase.VolumeCommands, fsID int64, refuse bool) error {
	if _, err := v.Cluster().Ceph(ctx, "fs", "set", v.Name(), "refuse_client_session", fmt.Sprint(refuse)); err != nil {
		return fmt.Errorf("set CephFS %q refuse_client_session: %w", v.Name(), err)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readbackTimeout)
	defer cancel()
	stored, err := readRefusal(readCtx, v, fsID)
	if err != nil {
		return err
	}
	if stored != refuse {
		return fmt.Errorf("CephFS refuse_client_session readback is %t, want %t", stored, refuse)
	}
	return nil
}
