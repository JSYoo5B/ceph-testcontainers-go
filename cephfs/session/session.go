package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// minTimeout is the MDSMonitor's lower bound for both values.
const minTimeout = 30 * time.Second

// Limits are the filesystem's MDSMap client session limits, session_timeout
// and session_autoclose.
// A silent client whose capabilities nobody wants stays open until Autoclose.
// Once another client asks for its capabilities, the MDS evicts it as soon as
// Timeout passes. With Ceph's default mds_session_blocklist_on_timeout the
// evicted address is blocklisted. These are filesystem settings, not central
// configuration, so override.SetConfig cannot change them. Native values are
// whole seconds.
type Limits struct {
	Timeout, Autoclose time.Duration
}

// Session is one client session reported by an active MDS rank. A client
// with sessions on several ranks appears once per rank. Address is the
// session's IP:port/nonce, the same form BlocklistEntries reports, so an
// evicted session can be matched to its blocklist entry. EntityID, Hostname,
// Root and PID come from the metadata the client sent when it opened the
// session.
type Session struct {
	Rank     int
	ID       uint64
	Address  string
	State    string
	EntityID string
	Hostname string
	Root     string
	PID      string
	Caps     int
}

// TimeoutsOverrideHandle manages restoration of both session limits,
// retaining prior state and ownership even when application is uncertain.
// Copies share restoration state. Retain a non-nil result even on error: a
// lost reply can leave either value changed.
type TimeoutsOverrideHandle struct {
	filesystem *fsbase.Core
	state      *timeoutsState
}

type timeoutsState struct {
	fsID              int64
	previous, applied Limits
	restored          bool
}

type nativeLimits struct {
	ID     int64 `json:"id"`
	MDSMap struct {
		SessionTimeout   *int64 `json:"session_timeout"`
		SessionAutoclose *int64 `json:"session_autoclose"`
	} `json:"mdsmap"`
}

type nativeSession struct {
	ID     uint64 `json:"id"`
	State  string `json:"state"`
	Caps   int    `json:"num_caps"`
	Entity struct {
		Addr struct {
			Addr  string `json:"addr"`
			Nonce uint64 `json:"nonce"`
		} `json:"addr"`
	} `json:"entity"`
	Metadata struct {
		EntityID string `json:"entity_id"`
		Hostname string `json:"hostname"`
		Root     string `json:"root"`
		PID      string `json:"pid"`
	} `json:"client_metadata"`
}

// coreOf returns the foundation core of fs; nil for a nil fs.
func coreOf(fs *cephfs.Filesystem) *fsbase.Core {
	if fs == nil {
		return nil
	}
	return fsbase.Of(fs)
}

// Timeouts reads both session limits of fs from its current MDSMap.
//
//purpose:observation
func Timeouts(ctx context.Context, fs *cephfs.Filesystem) (Limits, error) {
	return timeouts(ctx, coreOf(fs))
}

func timeouts(ctx context.Context, fs *fsbase.Core) (Limits, error) {
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return Limits{}, err
	}
	defer done()
	return readLimits(ctx, fs.Volumes(), fsID)
}

// OverrideTimeouts sets both session limits of fs so a test can observe
// evicted clients in seconds instead of Ceph's default 60 and 300 seconds.
// Each value must be a whole number of seconds and at least 30 seconds, the
// native minimum. Only one handle per filesystem may be active. The native CLI
// changes one value per command, so apply and restore are not atomic.
//
//purpose:condition
func OverrideTimeouts(ctx context.Context, fs *cephfs.Filesystem, requested Limits) (*TimeoutsOverrideHandle, error) {
	return overrideTimeouts(ctx, coreOf(fs), requested)
}

func overrideTimeouts(ctx context.Context, fs *fsbase.Core, requested Limits) (*TimeoutsOverrideHandle, error) {
	if err := validateLimits(requested); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	overrides := overridesOf(fs)
	if active := overrides.byCore[fs]; active != nil && !active.restored {
		return nil, errors.New("restore the existing CephFS session timeouts override first")
	}
	previous, err := readLimits(ctx, fs.Volumes(), fsID)
	if err != nil {
		return nil, err
	}
	state := &timeoutsState{fsID: fsID, previous: previous, applied: requested}
	overrides.byCore[fs] = state
	change := &TimeoutsOverrideHandle{filesystem: fs, state: state}
	return change, writeLimits(ctx, fs.Volumes(), fsID, previous, requested)
}

// Restore returns both limits to the values captured before the change. It is
// idempotent across handle copies. A value that is neither the applied nor the
// captured one is an outside edit and is refused rather than overwritten.
//
//purpose:condition,lifetime
func (change *TimeoutsOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.state == nil {
		return errors.New("CephFS session timeouts override is unavailable")
	}
	fs := change.filesystem
	state := change.state
	// A confirmed restore stays complete after the filesystem or cluster is gone.
	if fs.Cluster() != nil {
		var restored bool
		if err := fs.WithSetupGate(ctx, func() { restored = state.restored }); err != nil {
			return err
		}
		if restored {
			return nil
		}
	}
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if state.restored {
		return nil
	}
	overrides := overridesOf(fs)
	if overrides.byCore[fs] != state {
		return errors.New("CephFS session timeouts override is not tracked by this filesystem")
	}
	if fsID != state.fsID {
		return errors.New("CephFS filesystem identity changed since the session timeouts override")
	}
	current, err := readLimits(ctx, fs.Volumes(), fsID)
	if err != nil {
		return err
	}
	if !ownedSessionValue(current.Timeout, state.previous.Timeout, state.applied.Timeout) ||
		!ownedSessionValue(current.Autoclose, state.previous.Autoclose, state.applied.Autoclose) {
		return fmt.Errorf("CephFS session timeouts changed outside this override: %+v", current)
	}
	if err := writeLimits(ctx, fs.Volumes(), fsID, current, state.previous); err != nil {
		return err
	}
	state.restored = true
	delete(overrides.byCore, fs)
	return nil
}

// ownedSessionValue accepts either side of an interrupted apply or restore.
func ownedSessionValue(value, previous, applied time.Duration) bool {
	return value == previous || value == applied
}

// List lists the client sessions of fs from every active rank, including the
// fixture's own short-lived libcephfs probes. Filter by Hostname or EntityID
// to find a particular client. It reads native state only.
//
//purpose:observation
func List(ctx context.Context, fs *cephfs.Filesystem) ([]Session, error) {
	return list(ctx, coreOf(fs))
}

func list(ctx context.Context, fs *fsbase.Core) ([]Session, error) {
	ctx, _, done, err := begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	ranks, err := fs.ActiveRanks(ctx)
	if err != nil {
		return nil, err
	}
	var sessions []Session
	for _, mds := range ranks.Active {
		data, err := fs.Cluster().Ceph(ctx, "tell", "mds."+fs.Name()+":"+strconv.Itoa(mds.Rank), "session", "ls", "--format", "json")
		if err != nil {
			return nil, fmt.Errorf("list sessions of CephFS %q rank %d: %w", fs.Name(), mds.Rank, err)
		}
		ranked, err := parseSessions(data, mds.Rank)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, ranked...)
	}
	return sessions, nil
}

func parseSessions(data []byte, rank int) ([]Session, error) {
	var native []nativeSession
	if err := json.Unmarshal(data, &native); err != nil || native == nil {
		return nil, fmt.Errorf("decode CephFS sessions of rank %d", rank)
	}
	sessions := make([]Session, 0, len(native))
	for _, session := range native {
		if session.ID == 0 || session.State == "" || session.Entity.Addr.Addr == "" {
			return nil, fmt.Errorf("CephFS session of rank %d lacks its client identity", rank)
		}
		sessions = append(sessions, Session{
			Rank: rank, ID: session.ID, State: session.State, Caps: session.Caps,
			Address:  session.Entity.Addr.Addr + "/" + strconv.FormatUint(session.Entity.Addr.Nonce, 10),
			EntityID: session.Metadata.EntityID, Hostname: session.Metadata.Hostname,
			Root: session.Metadata.Root, PID: session.Metadata.PID,
		})
	}
	return sessions, nil
}

func validateLimits(requested Limits) error {
	for _, value := range []struct {
		name  string
		value time.Duration
	}{{"Timeout", requested.Timeout}, {"Autoclose", requested.Autoclose}} {
		if value.value%time.Second != 0 {
			return servicehook.Invalid(fmt.Errorf("CephFS session %s must be a whole number of seconds", value.name))
		}
		if value.value < minTimeout || value.value/time.Second > math.MaxInt32 {
			return servicehook.Invalid(fmt.Errorf("CephFS session %s must be between 30s and %ds", value.name, math.MaxInt32))
		}
	}
	return nil
}

// begin serializes with other CephFS setup and confirms that fs is still the
// cluster's active owned filesystem. It does not recheck cancellation after
// the setup gate.
func begin(ctx context.Context, fs *fsbase.Core) (context.Context, int64, func(), error) {
	var fsID int64
	ctx, done, err := fs.Admit(ctx, fsbase.AdmissionSpec{
		Refusal: "CephFS filesystem must be an active filesystem created by this cluster",
		Check: func(context.Context) (bool, error) {
			if !fs.Registered() || !fs.DescriptorIntact() || fs.Native() == nil {
				return false, nil
			}
			fsID = fs.Native().ID
			return true, nil
		},
	})
	if err != nil {
		return ctx, 0, func() {}, err
	}
	return ctx, fsID, done, nil
}

func readLimits(ctx context.Context, v fsbase.VolumeCommands, fsID int64) (Limits, error) {
	data, err := v.Cluster().Ceph(ctx, "fs", "get", v.Name(), "--format", "json")
	if err != nil {
		return Limits{}, fmt.Errorf("read CephFS %q session timeouts: %w", v.Name(), err)
	}
	return parseLimits(data, fsID)
}

func parseLimits(data []byte, fsID int64) (Limits, error) {
	var limits nativeLimits
	if err := json.Unmarshal(data, &limits); err != nil || limits.MDSMap.SessionTimeout == nil || limits.MDSMap.SessionAutoclose == nil {
		return Limits{}, errors.New("decode CephFS session timeouts")
	}
	if limits.ID != fsID {
		return Limits{}, fmt.Errorf("CephFS filesystem ID is %d, want %d", limits.ID, fsID)
	}
	return Limits{
		Timeout:   time.Duration(*limits.MDSMap.SessionTimeout) * time.Second,
		Autoclose: time.Duration(*limits.MDSMap.SessionAutoclose) * time.Second,
	}, nil
}

// writeLimits changes only differing values and confirms the result with a
// readback that survives cancellation of the caller's context.
func writeLimits(ctx context.Context, v fsbase.VolumeCommands, fsID int64, current, target Limits) error {
	for _, field := range []struct {
		name          string
		current, want time.Duration
	}{{"session_timeout", current.Timeout, target.Timeout}, {"session_autoclose", current.Autoclose, target.Autoclose}} {
		if field.current == field.want {
			continue
		}
		if _, err := v.Cluster().Ceph(ctx, "fs", "set", v.Name(), field.name, strconv.FormatInt(int64(field.want/time.Second), 10)); err != nil {
			return fmt.Errorf("set CephFS %q %s: %w", v.Name(), field.name, err)
		}
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stored, err := readLimits(readCtx, v, fsID)
	if err != nil {
		return err
	}
	if stored != target {
		return fmt.Errorf("CephFS session timeouts readback is %+v, want %+v", stored, target)
	}
	return nil
}

// overrideKind is the cluster extension that holds the session timeouts
// override of every filesystem of the cluster.
const overrideKind = "cephfs/session.timeouts"

// overrides is the session timeouts override of each filesystem of one
// cluster that is not restored yet, keyed by the filesystem's core: one per
// filesystem. The setup gate of the cluster's filesystem registry, which every
// session operation holds through begin, guards it, so it has no lock of its
// own. A removed filesystem's entry stays but is inert: no operation admits
// that core again, and a filesystem created later under the same name gets a
// new core. It lives as long as its cluster.
type overrides struct {
	byCore map[*fsbase.Core]*timeoutsState
}

// overridesOf returns the overrides of the cluster of core. The caller holds
// the setup gate.
func overridesOf(core *fsbase.Core) *overrides {
	return servicehook.Extension(core.Cluster(), overrideKind, func() any {
		return &overrides{byCore: make(map[*fsbase.Core]*timeoutsState)}
	}).(*overrides)
}
