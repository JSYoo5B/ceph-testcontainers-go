package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// mdsReadOnlyCommand is the MDS command that sets the read-only state.
const mdsReadOnlyCommand = "force_readonly"

// mdsReadOnlyHealthCheck is the health check an MDS reports in its beacon
// while it is read-only.
const mdsReadOnlyHealthCheck = "MDS_READ_ONLY"

// mdsReadOnlyInvalid is a refusal of the MDS read-only fault: an invalid
// request.
func mdsReadOnlyInvalid(format string, args ...any) error {
	return servicehook.Invalid(fmt.Errorf(format, args...))
}

// mdsReadOnlyPollInterval is the wait between two reads of the native
// evidence. The MDS reports its health in its beacon, every 4 seconds
// (mds_beacon_interval).
const mdsReadOnlyPollInterval = 500 * time.Millisecond

// MDSReadOnlyFault is the record of one MDS process that MDS.ForceReadOnly
// made read-only. It is a one-way fault, not an override: Ceph has no
// command that makes a read-only MDS writable again, so the handle has no
// Restore. The fault ends only when the process is replaced, by failing the
// daemon to a standby (MDS.Stop, then ceph mds fail <gid>) or by restarting
// it (MDS.Stop and MDS.Start), and WaitReplaced confirms that a new GID
// holds the rank. Copies share the record. Retain a non-nil result even on
// error: the command may have reached the daemon.
type MDSReadOnlyFault struct {
	fleet mdsFleet
	state *mdsReadOnlyState
}

// mdsReadOnlyState is the identity one fault captured before the command:
// the filesystem's native identity and the target process. It does not
// change after ForceReadOnly records it.
type mdsReadOnlyState struct {
	fsID, metadataPool, defaultPool int64
	target                          MDSStatus
}

// mdsReadOnlyKey names one faulted process of one filesystem.
type mdsReadOnlyKey struct {
	filesystem *fsbase.Core
	gid        uint64
}

// ForceReadOnly makes the current process of this owned MDS read-only with
// the MDS command force_readonly, so that a client meets a metadata server
// that still answers but refuses metadata mutations with EROFS. The daemon
// must hold the only active rank of its filesystem (max_mds 1) as
// up:active. Its rank and GID are read from the FSMap and must be confirmed
// by the process that answers ceph tell mds.<gid> status. A standby, a
// daemon whose handle no longer matches what its filesystem created, a
// filesystem with more ranks, a terminated cluster and a removed filesystem
// are ceph.ErrInvalidRequest before any change, and so is a daemon whose
// health already reports MDS_READ_ONLY: that state is not this fault's. A
// second call for a process this fixture already faulted returns the
// existing fault with ceph.ErrInvalidRequest. A daemon whose own command
// list lacks force_readonly is ceph.ErrUnsupported, with the daemon's
// release in Version.
//
// The command goes to the captured GID, never to the name, so a process
// that replaced the daemon in the meantime is not hit. After it, the call
// waits until the daemon reports MDS_READ_ONLY in its health, which arrives
// with its next beacon, and the same GID still holds the rank. An error
// after the command was sent returns the fault together with the error.
//
// What the fault proves: the target process has set its in-memory
// read-only state, reports it to the MONs, and refuses new metadata
// mutations; a fresh client still reads existing data. What it does not
// prove: that the metadata pool failed (no OSD or pool is touched, so the
// cause a real read-only MDS has is absent), that a client which already
// held write capabilities or buffered data has lost them, how any single
// read or data write of a retained client ends, or anything about other
// ranks.
//
//purpose:condition
func (daemon *MDS) ForceReadOnly(ctx context.Context) (*MDSReadOnlyFault, error) {
	if daemon == nil || daemon.identity == nil || daemon.identity.fleet.core.Cluster() == nil {
		return nil, mdsReadOnlyInvalid("CephFS MDS read-only fault: the MDS has no owned creation identity")
	}
	f := daemon.identity.fleet
	ctx, native, done, err := f.beginMDSReadOnlyOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := f.checkMDSReadOnlyOwner(ctx, daemon); err != nil {
		return nil, err
	}
	state := &mdsReadOnlyState{fsID: native.ID, metadataPool: native.MetadataPool, defaultPool: native.DefaultPool}
	target, err := f.readMDSReadOnlyTarget(ctx, state, daemon.identity.name)
	if err != nil {
		return nil, err
	}
	state.target = target
	registry := filesystems(f.core.Cluster())
	key := mdsReadOnlyKey{filesystem: f.core, gid: target.GID}
	if state := registry.readOnlyFaults[key]; state != nil {
		return &MDSReadOnlyFault{fleet: f, state: state}, mdsReadOnlyInvalid("mds.%s (GID %d) is already read-only through this fixture; only a process replacement ends it", target.Name, target.GID)
	}
	if err := checkMDSProcess(ctx, f.core.Volumes(), target); err != nil {
		return nil, err
	}
	if err := checkMDSReadOnlyCommand(ctx, f.core.Volumes(), target); err != nil {
		return nil, err
	}
	reported, err := mdsReportsReadOnly(ctx, f.core.Volumes(), target)
	if err != nil {
		return nil, err
	}
	if reported {
		return nil, mdsReadOnlyInvalid("mds.%s (GID %d) already reports %s; the fault would own no change", target.Name, target.GID, mdsReadOnlyHealthCheck)
	}
	registry.readOnlyFaults[key] = state
	fault := &MDSReadOnlyFault{fleet: f, state: state}
	gid := strconv.FormatUint(target.GID, 10)
	if _, err := f.core.Cluster().Ceph(ctx, "tell", "mds."+gid, mdsReadOnlyCommand); err != nil {
		return fault, fmt.Errorf("force mds.%s (GID %d) read-only: %w", target.Name, target.GID, err)
	}
	return fault, f.confirmMDSReadOnly(ctx, state)
}

// WaitReplaced waits until the process this fault made read-only no longer
// exists and another GID of an owned daemon holds its rank as up:active,
// and returns that daemon as MDSStatus reports it. It never replaces the
// process itself: stop the daemon and fail its GID so that a standby takes
// over, or restart it, before or while it waits. The wait ends with the
// context or the cluster's startup timeout, and its error then says that
// the faulted GID still holds the rank.
//
// Replaced means, in one FSMap, that the faulted GID appears nowhere and
// the rank is held by another owned GID; that ceph tell mds.<gid> status of
// the new GID answers as that rank of this filesystem; and that the health
// read after it does not report MDS_READ_ONLY for the new holder. A new
// process starts without the read-only state, but if the cause of a real
// read-only MDS remained it would set it again: that case keeps the wait
// going. It does not prove that a client which held a session with the
// faulted process can write again; mount a new client to check. A rank
// taken by a daemon this fixture does not own, a filesystem that was
// removed or recreated under the same name and a terminated cluster are
// ceph.ErrInvalidRequest at once. WaitReplaced takes the topology lock only
// to snapshot the owned daemons, as MDSStatus does, so MDS.Stop, MDS.Start
// and other fixture calls may run while it waits.
//
//purpose:observation
func (fault *MDSReadOnlyFault) WaitReplaced(ctx context.Context) (MDSStatus, error) {
	if fault == nil || fault.state == nil || fault.fleet.core.Cluster() == nil {
		return MDSStatus{}, mdsReadOnlyInvalid("CephFS MDS read-only fault is unavailable")
	}
	f := fault.fleet
	state := fault.state
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(f.core.Cluster()))
	defer cancel()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return MDSStatus{}, fmt.Errorf("wait for a replacement of read-only mds.%s (GID %d) on rank %d; force_readonly has no inverse, so fail or restart the daemon: %w",
				state.target.Name, state.target.GID, state.target.Rank, errors.Join(err, last))
		}
		holder, replaced, err := f.readMDSReadOnlyReplacement(ctx, state)
		switch {
		case errors.Is(err, ceph.ErrInvalidRequest):
			return MDSStatus{}, err
		case err != nil:
			last = err
		case replaced:
			return holder, nil
		default:
			last = nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(mdsReadOnlyPollInterval):
		}
	}
}

// String reports the faulted process and how the fault ends.
func (fault *MDSReadOnlyFault) String() string {
	if fault == nil || fault.state == nil {
		return "unavailable CephFS MDS read-only fault"
	}
	s := fault.state
	return fmt.Sprintf("read-only mds.%s (GID %d, rank %d) of CephFS FSMap ID %d; force_readonly has no inverse: it ends only when another process holds the rank",
		s.target.Name, s.target.GID, s.target.Rank, s.fsID)
}

// beginMDSReadOnlyOperation admits a fault on the original registered
// filesystem and returns its native identity. A terminated cluster or a
// removed filesystem is ceph.ErrInvalidRequest.
func (f mdsFleet) beginMDSReadOnlyOperation(ctx context.Context) (context.Context, fsbase.NativeIdentity, func(), error) {
	core := f.core
	var native fsbase.NativeIdentity
	ctx, done, err := core.Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "CephFS MDS read-only fault requires the original registered filesystem and confirmed native identity",
		Check: func(context.Context) (bool, error) {
			if !core.Registered() || !core.DescriptorIntact() || core.Native() == nil {
				return false, mdsReadOnlyInvalid("CephFS MDS read-only fault requires an open cluster and the original registered filesystem with its confirmed native identity")
			}
			native = fsbase.NativeIdentity{ID: core.Native().ID, MetadataPool: core.Native().MetadataPool, DefaultPool: core.Native().DefaultPool}
			return true, nil
		},
	})
	return ctx, native, done, err
}

// readMDSReadOnlyTopology snapshots the owned daemons, reads one FSMap and
// decodes the filesystem's MDS status with it. A terminated cluster and a
// filesystem that was removed or recreated under the same name are
// ceph.ErrInvalidRequest; the FSMap is returned for the GID search.
func (f mdsFleet) readMDSReadOnlyTopology(ctx context.Context, state *mdsReadOnlyState) (*FilesystemStatus, fsbase.FSMap, error) {
	owner, err := f.ownerSample(ctx)
	if err != nil {
		return nil, fsbase.FSMap{}, err
	}
	name := f.core.Name()
	switch {
	case !owner.open:
		return nil, fsbase.FSMap{}, mdsReadOnlyInvalid("CephFS MDS read-only fault: the cluster is terminated")
	case !owner.registered || owner.native == nil:
		return nil, fsbase.FSMap{}, mdsReadOnlyInvalid("CephFS MDS read-only fault: cephfs %q was removed or this handle no longer owns it", name)
	case owner.native.ID != state.fsID || owner.native.MetadataPool != state.metadataPool || owner.native.DefaultPool != state.defaultPool:
		return nil, fsbase.FSMap{}, mdsReadOnlyInvalid("CephFS MDS read-only fault: cephfs %q is not the filesystem of FSMap ID %d the fault captured", name, state.fsID)
	}
	data, err := f.readFSMap(ctx)
	if err != nil {
		return nil, fsbase.FSMap{}, fmt.Errorf("read cephfs MDS status: %w", err)
	}
	topology, err := decodeCephFSMDSTopology(data, name, owner.daemons)
	if errors.Is(err, errFilesystemAbsent) {
		if err := checkWatchedFSMap(data, name); err != nil {
			return nil, fsbase.FSMap{}, err
		}
		return nil, fsbase.FSMap{}, mdsReadOnlyInvalid("CephFS MDS read-only fault: cephfs %q (FSMap ID %d) is absent from the FSMap", name, state.fsID)
	}
	if err != nil {
		return nil, fsbase.FSMap{}, err
	}
	if topology.status.FilesystemID != state.fsID || topology.metadataPool != state.metadataPool || topology.defaultPool != state.defaultPool {
		if err := checkWatchedFSMap(data, name); err != nil {
			return nil, fsbase.FSMap{}, err
		}
		return nil, fsbase.FSMap{}, mdsReadOnlyInvalid("CephFS MDS read-only fault: cephfs %q now has FSMap ID %d, not the captured %d: recreated under the same name", name, topology.status.FilesystemID, state.fsID)
	}
	var fsmap fsbase.FSMap
	if err := json.Unmarshal(data, &fsmap); err != nil {
		return nil, fsbase.FSMap{}, fmt.Errorf("decode cephfs FSMap: %w", err)
	}
	return topology.status, fsmap, nil
}

// checkMDSReadOnlyOwner refuses, under the topology lock, a daemon handle
// that is not an original owned, unremoved MDS of this filesystem, as the
// other MDS handle operations do.
func (f mdsFleet) checkMDSReadOnlyOwner(ctx context.Context, daemon *MDS) error {
	c := f.core.Cluster()
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return err
	}
	defer servicehook.Unlock(c)
	if err := f.checkStoppedMDSOwner(daemon, daemon.identity); err != nil || daemon.identity.removed {
		return mdsReadOnlyInvalid("CephFS MDS read-only fault: mds.%s is not an owned MDS of cephfs %q: %v", daemon.ID, f.core.Name(), err)
	}
	return nil
}

// readMDSReadOnlyTarget returns the FSMap entry of the daemon named name,
// which must hold the only active rank of a single-rank filesystem.
func (f mdsFleet) readMDSReadOnlyTarget(ctx context.Context, state *mdsReadOnlyState, name string) (MDSStatus, error) {
	status, _, err := f.readMDSReadOnlyTopology(ctx, state)
	if err != nil {
		return MDSStatus{}, err
	}
	if status.MaxMDS != 1 || len(status.Active) != 1 {
		return MDSStatus{}, mdsReadOnlyInvalid("CephFS MDS read-only fault needs a filesystem with one active rank; cephfs %q has max_mds %d and %d active", f.core.Name(), status.MaxMDS, len(status.Active))
	}
	holder := status.Active[0]
	if holder.Name != name || !holder.Owned {
		return MDSStatus{}, mdsReadOnlyInvalid("mds.%s is not the active MDS of cephfs %q: rank %d is held by mds.%s", name, f.core.Name(), holder.Rank, holder.Name)
	}
	return holder, nil
}

// confirmMDSReadOnly waits until the target's health reports MDS_READ_ONLY
// and an FSMap read after it still shows the target GID on its rank.
func (f mdsFleet) confirmMDSReadOnly(ctx context.Context, state *mdsReadOnlyState) error {
	target := state.target
	for {
		reported, err := mdsReportsReadOnly(ctx, f.core.Volumes(), target)
		if err != nil {
			return err
		}
		if reported {
			status, _, err := f.readMDSReadOnlyTopology(ctx, state)
			if err != nil {
				return err
			}
			if !slices.ContainsFunc(status.Active, func(active MDSStatus) bool {
				return active.Rank == target.Rank && active.GID == target.GID && active.Name == target.Name
			}) {
				return fmt.Errorf("mds.%s (GID %d) left rank %d before its read-only state was confirmed", target.Name, target.GID, target.Rank)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("mds.%s (GID %d) did not report %s: %w", target.Name, target.GID, mdsReadOnlyHealthCheck, ctx.Err())
		case <-time.After(mdsReadOnlyPollInterval):
		}
	}
}

// readMDSReadOnlyReplacement is one read of WaitReplaced. replaced holds
// when the faulted GID is absent from the FSMap, another owned GID holds the
// rank, that GID answers as the rank's daemon and its health does not report
// MDS_READ_ONLY.
func (f mdsFleet) readMDSReadOnlyReplacement(ctx context.Context, state *mdsReadOnlyState) (MDSStatus, bool, error) {
	status, fsmap, err := f.readMDSReadOnlyTopology(ctx, state)
	if err != nil {
		return MDSStatus{}, false, err
	}
	if mdsGIDRegistered(fsmap, state.target.GID) {
		return MDSStatus{}, false, nil
	}
	index := slices.IndexFunc(status.Active, func(active MDSStatus) bool { return active.Rank == state.target.Rank })
	if index < 0 {
		return MDSStatus{}, false, nil // the rank is in replay, reconnect or rejoin
	}
	holder := status.Active[index]
	if !holder.Owned {
		return MDSStatus{}, false, mdsReadOnlyInvalid("rank %d of cephfs %q is held by mds.%s, which this fixture does not own", holder.Rank, f.core.Name(), holder.Name)
	}
	if err := checkMDSProcess(ctx, f.core.Volumes(), holder); err != nil {
		return MDSStatus{}, false, err
	}
	reported, err := mdsReportsReadOnly(ctx, f.core.Volumes(), holder)
	if err != nil || reported {
		return MDSStatus{}, false, err
	}
	return holder, true, nil
}

// mdsGIDRegistered reports whether any filesystem or the standby list of
// fsmap names gid.
func mdsGIDRegistered(fsmap fsbase.FSMap, gid uint64) bool {
	for _, info := range fsmap.Standbys {
		if info.GID == gid {
			return true
		}
	}
	for _, filesystem := range fsmap.Filesystems {
		for _, info := range filesystem.MDSMap.Info {
			if info.GID == gid {
				return true
			}
		}
	}
	return false
}

// mdsProcessStatus is the part of ceph tell mds.<gid> status that binds a
// reply to one process: its GID, rank, filesystem, state and release.
type mdsProcessStatus struct {
	GID     *uint64 `json:"id"`
	Rank    *int    `json:"whoami"`
	State   string  `json:"state"`
	FSName  string  `json:"fs_name"`
	Sysinfo struct {
		Version string `json:"ceph_version_short"`
	} `json:"sysinfo"`
}

// readMDSProcess reads ceph tell mds.<gid> status of daemon.
func readMDSProcess(ctx context.Context, v fsbase.VolumeCommands, daemon MDSStatus) (mdsProcessStatus, error) {
	data, err := v.Cluster().Ceph(ctx, "tell", "mds."+strconv.FormatUint(daemon.GID, 10), "status")
	if err != nil {
		return mdsProcessStatus{}, fmt.Errorf("read the status of mds.%s (GID %d): %w", daemon.Name, daemon.GID, err)
	}
	var status mdsProcessStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return mdsProcessStatus{}, fmt.Errorf("decode the status of mds.%s (GID %d): %w", daemon.Name, daemon.GID, err)
	}
	return status, nil
}

// checkMDSProcess confirms that the process answering daemon's GID is the
// up:active daemon of daemon's rank of this filesystem.
func checkMDSProcess(ctx context.Context, v fsbase.VolumeCommands, daemon MDSStatus) error {
	status, err := readMDSProcess(ctx, v, daemon)
	if err != nil {
		return err
	}
	if status.GID == nil || *status.GID != daemon.GID || status.Rank == nil || *status.Rank != daemon.Rank || status.FSName != v.Name() || status.State != "up:active" {
		return fmt.Errorf("the process of GID %d does not answer as up:active rank %d of cephfs %q", daemon.GID, daemon.Rank, v.Name())
	}
	return nil
}

// checkMDSReadOnlyCommand refuses a target whose own command list lacks
// force_readonly, with the release the daemon reports.
func checkMDSReadOnlyCommand(ctx context.Context, v fsbase.VolumeCommands, target MDSStatus) error {
	data, err := v.Cluster().Ceph(ctx, "tell", "mds."+strconv.FormatUint(target.GID, 10), "get_command_descriptions")
	if err != nil {
		return fmt.Errorf("read the commands of mds.%s (GID %d): %w", target.Name, target.GID, err)
	}
	offered, err := mdsOffersCommand(data, mdsReadOnlyCommand)
	if err != nil || offered {
		return err
	}
	unsupported := &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: "mds." + target.Name, Capability: "mds " + mdsReadOnlyCommand,
		Err: fmt.Errorf("mds.%s does not list %s among its commands", target.Name, mdsReadOnlyCommand)}
	if status, err := readMDSProcess(ctx, v, target); err == nil {
		unsupported.Version = status.Sysinfo.Version
	}
	return unsupported
}

// mdsOffersCommand reports whether a get_command_descriptions reply lists a
// command whose whole signature is the single literal word.
func mdsOffersCommand(data []byte, word string) (bool, error) {
	var commands map[string]struct {
		Signature []json.RawMessage `json:"sig"`
	}
	if err := json.Unmarshal(data, &commands); err != nil || len(commands) == 0 {
		return false, errors.New("decode the MDS command descriptions")
	}
	for _, command := range commands {
		if len(command.Signature) != 1 {
			continue
		}
		var literal string
		if json.Unmarshal(command.Signature[0], &literal) == nil && literal == word {
			return true, nil
		}
	}
	return false, nil
}

// mdsReportsReadOnly reports whether the cluster health lists daemon's
// name and rank under MDS_READ_ONLY.
func mdsReportsReadOnly(ctx context.Context, v fsbase.VolumeCommands, daemon MDSStatus) (bool, error) {
	data, err := v.Cluster().Ceph(ctx, "health", "detail", "--format", "json")
	if err != nil {
		return false, fmt.Errorf("read the MDS read-only health: %w", err)
	}
	return parseMDSReadOnlyHealth(data, daemon)
}

// parseMDSReadOnlyHealth finds "mds.<name>(mds.<rank>):" at the start of a
// detail message of MDS_READ_ONLY.
func parseMDSReadOnlyHealth(data []byte, daemon MDSStatus) (bool, error) {
	var health struct {
		Checks *map[string]struct {
			Detail []struct {
				Message string `json:"message"`
			} `json:"detail"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(data, &health); err != nil || health.Checks == nil {
		return false, errors.New("decode the cluster health detail")
	}
	check, ok := (*health.Checks)[mdsReadOnlyHealthCheck]
	if !ok {
		return false, nil
	}
	prefix := fmt.Sprintf("mds.%s(mds.%d):", daemon.Name, daemon.Rank)
	return slices.ContainsFunc(check.Detail, func(detail struct {
		Message string `json:"message"`
	}) bool {
		return strings.HasPrefix(detail.Message, prefix)
	}), nil
}
