package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// CephFSQuiesceConfig bounds reaching the I/O pause and its lifetime. Both
// durations must be at least one second. Expiration is a native safety timer;
// observing Status does not extend it. This fixture does not pause processes.
type CephFSQuiesceConfig struct {
	Timeout, Expiration time.Duration
}

// CephFSQuiesceState describes one native set. Version changes on native state
// transitions or edits. Members are native file: paths, not mutable handle fields.
type CephFSQuiesceState struct {
	ID                  string
	Version             uint64
	State               string
	Timeout, Expiration float64
	Members             []string
}

// CephFSQuiesce owns a newly created, unique native quiesce set. Copies share
// state. Release confirms the captured QUIESCED version, so an outside edit or
// expiration cannot masquerade as a consistent checkpoint. An uncertain create
// may return a non-nil handle plus an error; retain it or let its native timer
// expire before terminating the cluster. Never externally edit this set ID.
type CephFSQuiesce struct {
	filesystem *CephFSContainer
	state      *cephFSQuiesceIdentity
}

type cephFSQuiesceIdentity struct {
	id                                    string
	filesystemID                          int64
	members                               []string
	version                               uint64
	confirmed, releaseAttempted, released bool
	await                                 time.Duration
}

func (q *CephFSQuiesce) ID() string {
	if q == nil || q.state == nil {
		return ""
	}
	return q.state.id
}

// QuiesceSubvolumes pauses I/O to confirmed owned subvolumes as one native set.
// It uses --if-version=0 to create exclusively, validates every current resource
// identity, and awaits QUIESCED with finite native timeout and expiration.
// Existing sets and application writers remain caller-owned. Take checkpoints
// with client/admin APIs while paused, then explicitly Release. Multiple native
// sets can overlap paths; releasing this set does not cancel other sets.
func (fs *CephFSContainer) QuiesceSubvolumes(ctx context.Context, volumes []*CephFSSubvolume, config CephFSQuiesceConfig) (*CephFSQuiesce, error) {
	if config.Timeout < time.Second || config.Expiration < time.Second || len(volumes) == 0 {
		return nil, errors.New("quiesce requires subvolumes and timeout/expiration of at least one second")
	}
	for _, volume := range volumes {
		if fs == nil || volume == nil || volume.identity == nil || volume.identity.filesystem != fs {
			return nil, errors.New("quiesce members must be confirmed subvolumes owned by this filesystem")
		}
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	members := make([]string, 0, len(volumes))
	names := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		identity := volume.identity
		if !identity.ready || identity.removed {
			return nil, errors.New("quiesce member is not live and confirmed")
		}
		info, err := fs.subvolumeInfo(ctx, identity.name, identity.group)
		if err != nil {
			return nil, err
		}
		if err := fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt); err != nil {
			return nil, err
		}
		member := "file:" + identity.path
		if slices.Contains(members, member) {
			return nil, errors.New("quiesce members must be distinct")
		}
		members = append(members, member)
		name := identity.name
		if identity.group != "" {
			name = identity.group + "/" + name
		}
		names = append(names, name)
	}
	slices.Sort(members)
	q := &CephFSQuiesce{filesystem: fs, state: &cephFSQuiesceIdentity{
		id: "tc-" + uuid.NewString(), filesystemID: fsID, members: members, await: config.Timeout,
	}}
	args := []string{"fs", "quiesce", fs.config.Name, "--set-id", q.state.id, "--if-version", "0",
		"--timeout", quiesceSeconds(config.Timeout), "--expiration", quiesceSeconds(config.Expiration),
		"--await-for", quiesceSeconds(config.Timeout)}
	args = append(args, names...)
	_, setErr := fs.cluster.Ceph(ctx, args...)
	readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	state, readErr := q.read(readCtx)
	if readErr == nil {
		q.state.version, q.state.confirmed = state.Version, true
		if state.Timeout != quiesceNativeSeconds(config.Timeout) || state.Expiration != quiesceNativeSeconds(config.Expiration) {
			readErr = errors.New("native quiesce timeout/expiration differs from request")
			q.state.confirmed = false
		} else if state.State != "QUIESCED" {
			readErr = fmt.Errorf("native quiesce set is %s, not QUIESCED", state.State)
		}
	}
	return q, errors.Join(setErr, readErr)
}

func quiesceSeconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'f', 6, 64)
}

func quiesceNativeSeconds(duration time.Duration) float64 {
	seconds, _ := strconv.ParseFloat(quiesceSeconds(duration), 64)
	return seconds
}

// Status queries this set without renewing the native expiration timer. It
// verifies filesystem and exact nonexcluded members; it does not adopt new
// versions as permission to release an externally edited set.
func (q *CephFSQuiesce) Status(ctx context.Context) (*CephFSQuiesceState, error) {
	if q == nil || q.filesystem == nil || q.state == nil {
		return nil, errors.New("quiesce handle is unavailable")
	}
	ctx, fsID, done, err := q.filesystem.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if fsID != q.state.filesystemID {
		return nil, errors.New("quiesce filesystem was replaced")
	}
	return q.read(ctx)
}

// Release resumes this set's I/O only at its captured QUIESCED version, using
// native optimistic concurrency. Expired/timed-out/canceled sets are reported as
// errors, so a test cannot treat an interrupted checkpoint as consistent. Lost
// replies can be retried; confirmed RELEASED copies become no-ops. Native set
// records are retained by the QDB and disposed of with the cluster.
func (q *CephFSQuiesce) Release(ctx context.Context) error {
	if q == nil || q.filesystem == nil || q.state == nil {
		return errors.New("quiesce handle is unavailable")
	}
	fs := q.filesystem
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if fsID != q.state.filesystemID {
		return errors.New("quiesce filesystem was replaced")
	}
	if q.state.released {
		return nil
	}
	if !q.state.confirmed {
		return errors.New("quiesce creation was not confirmed; inspect native state or await expiration")
	}
	current, err := q.read(ctx)
	if err != nil {
		return err
	}
	if q.state.releaseAttempted && current.State == "RELEASED" {
		q.state.released = true
		return nil
	}
	if current.State != "QUIESCED" || current.Version != q.state.version {
		return fmt.Errorf("quiesce state/version changed (%s/%d); checkpoint release refused", current.State, current.Version)
	}
	q.state.releaseAttempted = true
	_, setErr := fs.cluster.Ceph(ctx, "fs", "quiesce", fs.config.Name, "--set-id", q.state.id,
		"--release", "--if-version", strconv.FormatUint(q.state.version, 10), "--await-for", quiesceSeconds(q.state.await))
	readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	current, readErr := q.read(readCtx)
	if readErr == nil {
		if current.State == "RELEASED" {
			q.state.released = true
		} else {
			readErr = fmt.Errorf("native release is %s", current.State)
		}
	}
	return errors.Join(setErr, readErr)
}

func (q *CephFSQuiesce) read(ctx context.Context) (*CephFSQuiesceState, error) {
	data, err := q.filesystem.cluster.Ceph(ctx, "fs", "quiesce", q.filesystem.config.Name, "--set-id", q.state.id, "--query", "--format", "json")
	if err != nil {
		return nil, err
	}
	state, err := decodeCephFSQuiesce(data, q.state.id)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(state.Members, q.state.members) {
		return nil, errors.New("native quiesce members changed; refusing ownership")
	}
	return state, nil
}

func decodeCephFSQuiesce(data []byte, id string) (*CephFSQuiesceState, error) {
	var database struct {
		Sets map[string]struct {
			Version    uint64  `json:"version"`
			Timeout    float64 `json:"timeout"`
			Expiration float64 `json:"expiration"`
			State      struct {
				Name string `json:"name"`
			} `json:"state"`
			Members map[string]struct {
				Excluded bool `json:"excluded"`
			} `json:"members"`
		} `json:"sets"`
	}
	if err := json.Unmarshal(data, &database); err != nil {
		return nil, errors.New("decode native quiesce database")
	}
	set, exists := database.Sets[id]
	if !exists || len(database.Sets) != 1 || set.Version == 0 || len(set.Members) == 0 || set.Timeout < 0 || set.Expiration < 0 {
		return nil, errors.New("native quiesce set is absent or incomplete")
	}
	if !slices.Contains([]string{"QUIESCING", "QUIESCED", "RELEASING", "RELEASED", "EXPIRED", "TIMEDOUT", "CANCELED", "FAILED"}, set.State.Name) {
		return nil, errors.New("unsupported native quiesce state")
	}
	members := make([]string, 0, len(set.Members))
	for path, member := range set.Members {
		if member.Excluded || len(path) < 7 || path[:6] != "file:/" {
			return nil, errors.New("native quiesce has excluded or unsupported members")
		}
		members = append(members, path)
	}
	slices.Sort(members)
	return &CephFSQuiesceState{ID: id, Version: set.Version, State: set.State.Name, Timeout: set.Timeout, Expiration: set.Expiration, Members: members}, nil
}
