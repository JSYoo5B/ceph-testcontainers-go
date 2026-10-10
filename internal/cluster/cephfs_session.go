package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// minCephFSSessionTimeout is the MDSMonitor's lower bound for both values.
const minCephFSSessionTimeout = 30 * time.Second

// CephFSSessionTimeouts are the filesystem's MDSMap client session limits.
// A session that has not renewed its capabilities for Timeout becomes stale;
// one silent for Autoclose is evicted, and with Ceph's default
// mds_session_blocklist_on_timeout its address is blocklisted. These are
// filesystem settings, not central configuration, so TemporaryConfig cannot
// change them. Native values are whole seconds.
type CephFSSessionTimeouts struct {
	Timeout, Autoclose time.Duration
}

// CephFSSession is one client session reported by an active MDS rank. A client
// with sessions on several ranks appears once per rank. Address is the
// session's IP:port/nonce, the same form BlocklistEntries reports, so an
// evicted session can be matched to its blocklist entry. EntityID, Hostname,
// Root and PID come from the metadata the client sent when it opened the
// session.
type CephFSSession struct {
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

// CephFSSessionTimeoutsOverride owns one temporary change to both session
// limits of a filesystem. Copies share restoration state. Retain a non-nil
// result even on error: a lost reply can leave either value changed.
type CephFSSessionTimeoutsOverride struct {
	filesystem *CephFSContainer
	state      *cephFSSessionTimeoutsState
}

type cephFSSessionTimeoutsState struct {
	fsID              int64
	previous, applied CephFSSessionTimeouts
	restored          bool
}

type cephFSSessionLimits struct {
	ID     int64 `json:"id"`
	MDSMap struct {
		SessionTimeout   *int64 `json:"session_timeout"`
		SessionAutoclose *int64 `json:"session_autoclose"`
	} `json:"mdsmap"`
}

type cephFSNativeSession struct {
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

// SessionTimeouts reads both limits from the filesystem's current MDSMap.
func (fs *CephFSContainer) SessionTimeouts(ctx context.Context) (CephFSSessionTimeouts, error) {
	ctx, fsID, done, err := fs.beginSessionOperation(ctx)
	if err != nil {
		return CephFSSessionTimeouts{}, err
	}
	defer done()
	return fs.readSessionTimeouts(ctx, fsID)
}

// TemporarySessionTimeouts sets both limits so a test can observe stale and
// evicted clients in seconds instead of Ceph's default 60 and 300 seconds.
// Each value must be a whole number of seconds and at least 30 seconds, the
// native minimum. Only one handle per filesystem may be active. The native CLI
// changes one value per command, so apply and restore are not atomic.
func (fs *CephFSContainer) TemporarySessionTimeouts(ctx context.Context, requested CephFSSessionTimeouts) (*CephFSSessionTimeoutsOverride, error) {
	if err := validateCephFSSessionTimeouts(requested); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSessionOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if active := fs.sessionTimeouts; active != nil && !active.restored {
		return nil, errors.New("restore the existing CephFS session timeouts override first")
	}
	previous, err := fs.readSessionTimeouts(ctx, fsID)
	if err != nil {
		return nil, err
	}
	state := &cephFSSessionTimeoutsState{fsID: fsID, previous: previous, applied: requested}
	fs.sessionTimeouts = state
	change := &CephFSSessionTimeoutsOverride{filesystem: fs, state: state}
	return change, fs.writeSessionTimeouts(ctx, fsID, previous, requested)
}

// Restore returns both limits to the values captured before the change. It is
// idempotent across handle copies. A value that is neither the applied nor the
// captured one is an outside edit and is refused rather than overwritten.
func (change *CephFSSessionTimeoutsOverride) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.state == nil {
		return errors.New("CephFS session timeouts override is unavailable")
	}
	fs := change.filesystem
	state := change.state
	// A confirmed restore stays complete after the filesystem or cluster is gone.
	if fs.cluster != nil {
		if err := lockTopologyMutex(ctx, &fs.cluster.cephfsSetupMu); err != nil {
			return err
		}
		restored := state.restored
		fs.cluster.cephfsSetupMu.Unlock()
		if restored {
			return nil
		}
	}
	ctx, fsID, done, err := fs.beginSessionOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if state.restored {
		return nil
	}
	if fs.sessionTimeouts != state {
		return errors.New("CephFS session timeouts override is not tracked by this filesystem")
	}
	if fsID != state.fsID {
		return errors.New("CephFS filesystem identity changed since the session timeouts override")
	}
	current, err := fs.readSessionTimeouts(ctx, fsID)
	if err != nil {
		return err
	}
	if !ownedSessionValue(current.Timeout, state.previous.Timeout, state.applied.Timeout) ||
		!ownedSessionValue(current.Autoclose, state.previous.Autoclose, state.applied.Autoclose) {
		return fmt.Errorf("CephFS session timeouts changed outside this override: %+v", current)
	}
	if err := fs.writeSessionTimeouts(ctx, fsID, current, state.previous); err != nil {
		return err
	}
	state.restored = true
	fs.sessionTimeouts = nil
	return nil
}

// ownedSessionValue accepts either side of an interrupted apply or restore.
func ownedSessionValue(value, previous, applied time.Duration) bool {
	return value == previous || value == applied
}

// Sessions lists client sessions from every active rank of the filesystem,
// including the fixture's own short-lived libcephfs probes. Filter by Hostname
// or EntityID to find a particular client. It reads native state only.
func (fs *CephFSContainer) Sessions(ctx context.Context) ([]CephFSSession, error) {
	ctx, _, done, err := fs.beginSessionOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		return nil, err
	}
	var sessions []CephFSSession
	for _, mds := range status.Active {
		data, err := fs.cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":"+strconv.Itoa(mds.Rank), "session", "ls", "--format", "json")
		if err != nil {
			return nil, fmt.Errorf("list sessions of CephFS %q rank %d: %w", fs.FilesystemName, mds.Rank, err)
		}
		ranked, err := parseCephFSSessions(data, mds.Rank)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, ranked...)
	}
	return sessions, nil
}

func parseCephFSSessions(data []byte, rank int) ([]CephFSSession, error) {
	var native []cephFSNativeSession
	if err := json.Unmarshal(data, &native); err != nil || native == nil {
		return nil, fmt.Errorf("decode CephFS sessions of rank %d", rank)
	}
	sessions := make([]CephFSSession, 0, len(native))
	for _, session := range native {
		if session.ID == 0 || session.State == "" || session.Entity.Addr.Addr == "" {
			return nil, fmt.Errorf("CephFS session of rank %d lacks its client identity", rank)
		}
		sessions = append(sessions, CephFSSession{
			Rank: rank, ID: session.ID, State: session.State, Caps: session.Caps,
			Address:  session.Entity.Addr.Addr + "/" + strconv.FormatUint(session.Entity.Addr.Nonce, 10),
			EntityID: session.Metadata.EntityID, Hostname: session.Metadata.Hostname,
			Root: session.Metadata.Root, PID: session.Metadata.PID,
		})
	}
	return sessions, nil
}

func validateCephFSSessionTimeouts(requested CephFSSessionTimeouts) error {
	for _, value := range []struct {
		name  string
		value time.Duration
	}{{"Timeout", requested.Timeout}, {"Autoclose", requested.Autoclose}} {
		if value.value%time.Second != 0 {
			return fmt.Errorf("CephFS session %s must be a whole number of seconds", value.name)
		}
		if value.value < minCephFSSessionTimeout || value.value/time.Second > math.MaxInt32 {
			return fmt.Errorf("CephFS session %s must be between 30s and %ds", value.name, math.MaxInt32)
		}
	}
	return nil
}

// beginSessionOperation serializes with other CephFS setup and confirms that
// this handle is still the cluster's active owned filesystem.
func (fs *CephFSContainer) beginSessionOperation(ctx context.Context) (context.Context, int64, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, 0, func() {}, err
	}
	if fs == nil || fs.cluster == nil {
		return ctx, 0, func() {}, errors.New("CephFS filesystem is unavailable")
	}
	c := fs.cluster
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return ctx, 0, func() {}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		c.cephfsSetupMu.Unlock()
		return ctx, 0, func() {}, err
	}
	valid := !c.closed && c.filesystems[fs.config.Name] == fs && fs.FilesystemName == fs.config.Name && fs.nativeIdentity != nil
	var fsID int64
	if valid {
		fsID = fs.nativeIdentity.id
	}
	timeout := c.settings.startupTimeout
	c.mu.Unlock()
	if !valid {
		c.cephfsSetupMu.Unlock()
		return ctx, 0, func() {}, errors.New("CephFS filesystem must be an active filesystem created by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, fsID, func() { cancel(); c.cephfsSetupMu.Unlock() }, nil
}

func (fs *CephFSContainer) readSessionTimeouts(ctx context.Context, fsID int64) (CephFSSessionTimeouts, error) {
	data, err := fs.cluster.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
	if err != nil {
		return CephFSSessionTimeouts{}, fmt.Errorf("read CephFS %q session timeouts: %w", fs.FilesystemName, err)
	}
	return parseCephFSSessionTimeouts(data, fsID)
}

func parseCephFSSessionTimeouts(data []byte, fsID int64) (CephFSSessionTimeouts, error) {
	var limits cephFSSessionLimits
	if err := json.Unmarshal(data, &limits); err != nil || limits.MDSMap.SessionTimeout == nil || limits.MDSMap.SessionAutoclose == nil {
		return CephFSSessionTimeouts{}, errors.New("decode CephFS session timeouts")
	}
	if limits.ID != fsID {
		return CephFSSessionTimeouts{}, fmt.Errorf("CephFS filesystem ID is %d, want %d", limits.ID, fsID)
	}
	return CephFSSessionTimeouts{
		Timeout:   time.Duration(*limits.MDSMap.SessionTimeout) * time.Second,
		Autoclose: time.Duration(*limits.MDSMap.SessionAutoclose) * time.Second,
	}, nil
}

// writeSessionTimeouts changes only differing values and confirms the result
// with a readback that survives cancellation of the caller's context.
func (fs *CephFSContainer) writeSessionTimeouts(ctx context.Context, fsID int64, current, target CephFSSessionTimeouts) error {
	for _, field := range []struct {
		name          string
		current, want time.Duration
	}{{"session_timeout", current.Timeout, target.Timeout}, {"session_autoclose", current.Autoclose, target.Autoclose}} {
		if field.current == field.want {
			continue
		}
		if _, err := fs.cluster.Ceph(ctx, "fs", "set", fs.FilesystemName, field.name, strconv.FormatInt(int64(field.want/time.Second), 10)); err != nil {
			return fmt.Errorf("set CephFS %q %s: %w", fs.FilesystemName, field.name, err)
		}
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stored, err := fs.readSessionTimeouts(readCtx, fsID)
	if err != nil {
		return err
	}
	if stored != target {
		return fmt.Errorf("CephFS session timeouts readback is %+v, want %+v", stored, target)
	}
	return nil
}
