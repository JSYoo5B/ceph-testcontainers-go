package cephfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/mdsmap"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// Creation and attempt state is shared by copied descriptive handles. Native
// GIDs are deliberately not attributed to a CID by this cleanup operation.
type cephFSMDSIdentity struct {
	// fleet is the MDS fleet of the filesystem that created the daemon.
	fleet                                mdsFleet
	native                               *fsbase.NativeIdentity
	descriptor                           *MDS
	container                            testcontainers.Container
	name, cid, fsid                      string
	confirmed, removalAttempted, removed bool
	scope                                *mdsmap.Scope
	replacement                          *cephFSMDSReplacement
	createdByReplacement                 *cephFSMDSReplacement
}

func cephFSMDSFullCID(cid string) bool {
	if len(cid) != 64 {
		return false
	}
	_, err := hex.DecodeString(cid)
	return err == nil && cid == string(bytes.ToLower([]byte(cid)))
}

// RemoveStopped retires this original owned, stopped MDS container after its
// immutable daemon name has left every native filesystem and global standby
// list. Healthy original owned daemons must still serve every desired active
// rank. It neither fails a native MDS nor deletes its auth entity or changes
// desired capacity. Use Filesystem.ScaleMDS afterward to create missing
// replacement members.
//
// Only successful original startups are eligible. A partial startup remains
// owned for cluster Terminate. An uncertain Docker removal retains the original
// handle for explicit retry; initial arbitrary absence is not success. Direct
// failure injection, Start/Stop and raw policy changes must not run concurrently.
//
//purpose:condition,lifetime
func (daemon *MDS) RemoveStopped(ctx context.Context) error {
	if daemon == nil || daemon.identity == nil || daemon.identity.fleet.core == nil {
		return errMDSUnavailable
	}
	return daemon.identity.fleet.removeStoppedMDS(ctx, daemon)
}

// removeStoppedMDS implements MDS.RemoveStopped for the filesystem that
// created daemon.
func (f mdsFleet) removeStoppedMDS(parent context.Context, daemon *MDS) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if f.core.Cluster() == nil || daemon == nil || daemon.identity == nil {
		return errMDSUnavailable
	}
	c := f.core.Cluster()
	ctx, cancel := context.WithTimeout(parent, servicehook.StartupTimeout(c))
	defer cancel()
	release, err := lockSetupThenTopology(ctx, c)
	if err != nil {
		return err
	}
	defer release()
	defer servicehook.Unlock(c)
	i := daemon.identity
	scope, err := f.admitStoppedMDSRemoval(ctx, daemon)
	if err != nil {
		return err
	}
	missing, err := f.reattestStoppedMDS(ctx, daemon, scope)
	if err != nil {
		return err
	}
	if !i.removed && !missing {
		i.scope, i.removalAttempted = scope, true
		if err := i.container.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
			return clusterutil.OperationError(ctx, "terminate original stopped MDS", err)
		}
	}
	if err := f.confirmStoppedMDSRemoved(ctx, daemon, scope); err != nil {
		return err
	}
	if i.removed {
		return nil
	}
	f.unpublishMDS(i.descriptor)
	i.removed = true
	return nil
}

// admitStoppedMDSRemoval is the admission of removeStoppedMDS. Owner held:
// the caller holds setupMu and the cluster mutex. It checks the owner and the
// completed-replacement authority before any Docker or native read, then the
// control, the native cluster, the container and the scope, which must equal
// the scope admitted by an earlier attempt.
func (f mdsFleet) admitStoppedMDSRemoval(ctx context.Context, daemon *MDS) (*mdsmap.Scope, error) {
	i := daemon.identity
	if err := f.checkOwnedMDS(daemon, i); err != nil {
		return nil, err
	}
	if err := completedMDSReplacementAuthority(i); err != nil {
		return nil, err
	}
	// Admission to control/config precedes any Docker or native reads.
	control, err := f.core.Cluster().ControlContainer(ctx)
	if err != nil || control == nil {
		return nil, clusterutil.OperationError(ctx, "select stopped MDS control", err)
	}
	if err := checkStoppedMDSCluster(ctx, f.core.Volumes(), i); err != nil {
		return nil, err
	}
	if _, err := inspectStoppedMDS(ctx, i, false); err != nil {
		return nil, err
	}
	scope, err := f.readStoppedMDSScope(ctx, i)
	if err != nil {
		return nil, err
	}
	if i.removalAttempted && !i.removed && !reflect.DeepEqual(i.scope, scope) {
		return nil, errors.New("admitted stopped MDS policy or attachments changed")
	}
	return scope, nil
}

// reattestStoppedMDS reattests the original native cluster, scope, owner and
// container state immediately before a Docker attempt, and reports whether
// the container is already missing. Owner held.
func (f mdsFleet) reattestStoppedMDS(ctx context.Context, daemon *MDS, scope *mdsmap.Scope) (bool, error) {
	i := daemon.identity
	if err := checkStoppedMDSCluster(ctx, f.core.Volumes(), i); err != nil {
		return false, err
	}
	after, err := f.readStoppedMDSScope(ctx, i)
	if err != nil || !reflect.DeepEqual(scope, after) {
		return false, clusterutil.OperationError(ctx, "stopped MDS scope changed before removal", err)
	}
	if err := f.checkOwnedMDS(daemon, i); err != nil {
		return false, err
	}
	missing, err := inspectStoppedMDS(ctx, i, false)
	if err != nil {
		return false, err
	}
	return missing, ctx.Err()
}

// confirmStoppedMDSRemoved follows a removal with exact full-CID absence, then
// fresh native authority: the native cluster, an unchanged scope and the
// owner. Context or error loss leaves the retained pending owner for retry.
// Owner held.
func (f mdsFleet) confirmStoppedMDSRemoved(ctx context.Context, daemon *MDS, scope *mdsmap.Scope) error {
	i := daemon.identity
	missing, err := inspectStoppedMDS(ctx, i, true)
	if err != nil || !missing {
		return clusterutil.OperationError(ctx, "confirm original stopped MDS removal", err)
	}
	if err := checkStoppedMDSCluster(ctx, f.core.Volumes(), i); err != nil {
		return err
	}
	after, err := f.readStoppedMDSScope(ctx, i)
	if err != nil || !reflect.DeepEqual(scope, after) {
		return clusterutil.OperationError(ctx, "stopped MDS scope changed after removal", err)
	}
	if err := f.checkOwnedMDS(daemon, i); err != nil {
		return err
	}
	return ctx.Err()
}

// checkOwnedMDS refuses an MDS handle that is no longer one of this
// filesystem's owned daemons, whatever its process state: the filesystem
// descriptor is still the registered one, the creation record is consistent,
// the public handle is unedited, and the registry still holds the daemon and
// an owned MDS as the embedded container. Every MDS handle operation, and the
// survivor checks of a stopped-MDS removal, use it. Owner held: the caller
// holds the cluster mutex. Exported fields cannot redirect either target or
// survivor reads.
func (f mdsFleet) checkOwnedMDS(handle *MDS, i *cephFSMDSIdentity) error {
	if !f.descriptorIntact() || !i.consistent(f) || !i.handleUnedited(handle, f) {
		return errors.New("owned MDS ownership changed or its startup is unconfirmed")
	}
	if !i.handleUnedited(i.descriptor, f) {
		return servicehook.Invalid(errors.New("owned MDS descriptor was edited"))
	}
	if !f.embeddedOwned() {
		return servicehook.Invalid(errors.New("filesystem compatibility container is not an original owned MDS"))
	}
	if !f.registryHolds(i) {
		return errors.New("owned MDS registry ownership changed")
	}
	return nil
}

// descriptorIntact reports whether the filesystem is still registered and its
// exported descriptor names its configured filesystem and pools. Owner held.
func (f mdsFleet) descriptorIntact() bool {
	return f.core.Registered() && *f.descriptor.name == f.core.Name() && *f.descriptor.metadataPool == f.config.MetadataPool.Name && *f.descriptor.dataPool == f.config.DataPool.Name
}

// consistent reports whether i is a confirmed creation record of f: it was
// created by f's filesystem with its native identity, its container still
// has the recorded full CID, and its descriptor still points back at it.
func (i *cephFSMDSIdentity) consistent(f mdsFleet) bool {
	return i.fleet.core == f.core && i.confirmed && i.native != nil && i.native == f.core.Native() && i.containerIntact() && i.descriptor != nil && i.descriptor.identity == i
}

// containerIntact reports whether i still has a container with the recorded
// CID.
func (i *cephFSMDSIdentity) containerIntact() bool {
	return i.container != nil && i.container.GetContainerID() == i.cid
}

// handleUnedited reports whether the exported fields of handle still name
// i's daemon, f's filesystem and i's container.
func (i *cephFSMDSIdentity) handleUnedited(handle *MDS, f mdsFleet) bool {
	return handle.ID == i.name && handle.FilesystemName == f.core.Name() && handle.Container == i.container
}

// embeddedOwned reports whether the filesystem's embedded container is the
// container of an unremoved owned MDS. Owner held.
func (f mdsFleet) embeddedOwned() bool {
	for _, owned := range f.mds.roster.members() {
		if owned != nil && owned.identity != nil && !owned.identity.removed && owned.identity.container == *f.first {
			return true
		}
	}
	return false
}

// registryHolds reports whether the roster and the service registry still
// hold i's daemon exactly once while it is not removed, and not at all once
// it is. Owner held.
func (f mdsFleet) registryHolds(i *cephFSMDSIdentity) bool {
	count := 0
	for _, owned := range f.mds.roster.members() {
		if owned == i.descriptor {
			count++
		}
	}
	service := servicehook.Service(f.core.Cluster(), "mds."+i.name)
	if i.removed {
		return count == 0 && service == nil
	}
	return count == 1 && service == i.container
}

// containerExited reports whether info is the positively exited container
// cid: not running, restarting, paused or dead, and without a process.
func containerExited(info *container.InspectResponse, cid string) bool {
	return info != nil && info.ID == cid && info.State != nil && !info.State.Running && !info.State.Restarting && !info.State.Paused && !info.State.Dead && info.State.Pid == 0 && info.State.Status == "exited"
}

// containerRunning reports whether info is the normally running container
// cid: running with a process, not paused, restarting or dead, and without a
// Docker error.
func containerRunning(info *container.InspectResponse, cid string) bool {
	return info != nil && info.ID == cid && info.State != nil && info.State.Running && !info.State.Paused && !info.State.Restarting && !info.State.Dead && info.State.Pid > 0 && info.State.Error == "" && info.State.Status == "running"
}

// Owner held. Reuse the confirmed bootstrap parser, fresh native quorum and
// address validation, with context-aware admission to its existing cache gate.
func checkStoppedMDSCluster(ctx context.Context, v fsbase.VolumeCommands, i *cephFSMDSIdentity) error {
	c := v.Cluster()
	fsid, confirmed, err := servicehook.ClusterIdentity(ctx, c)
	if err != nil {
		return err
	}
	if !confirmed || fsid != i.fsid {
		return errors.New("original stopped MDS cluster bootstrap changed")
	}
	data, err := c.Ceph(ctx, "quorum_status", "--format", "json")
	if err != nil {
		return clusterutil.OperationError(ctx, "read stopped MDS cluster quorum", err)
	}
	var quorum ceph.QuorumStatus
	if mdsmap.DecodeJSON(data, &quorum) != nil || quorum.MonMap.FSID != i.fsid {
		return errors.New("native stopped MDS cluster identity changed")
	}
	if _, err := servicehook.MonitorAddresses(quorum); err != nil {
		return errors.New("stopped MDS cluster has no valid majority quorum")
	}
	return ctx.Err()
}

func inspectStoppedMDS(ctx context.Context, i *cephFSMDSIdentity, requireMissing bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if i.container.GetContainerID() != i.cid {
		return false, errors.New("original MDS container CID changed")
	}
	info, err := i.container.Inspect(ctx)
	if err != nil {
		if servicehook.OnlyMissingHostResource(err) && (i.removalAttempted || i.removed) {
			return true, ctx.Err()
		}
		return false, clusterutil.OperationError(ctx, "inspect original stopped MDS", err)
	}
	if !containerExited(info, i.cid) || requireMissing {
		return false, errors.New("original MDS must be positively stopped or confirmed removed")
	}
	return false, ctx.Err()
}

// Strict global absence and logical owned-survivor membership come from one
// FSMap. This never attributes a native GID to a Docker process or adopts one.
func (f mdsFleet) readStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity) (*mdsmap.Scope, error) {
	state, err := f.core.ReadNativePools(ctx)
	if err != nil {
		return nil, clusterutil.OperationError(ctx, "read original stopped MDS filesystem pools", err)
	}
	data, err := f.core.Cluster().Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, clusterutil.OperationError(ctx, "read stopped MDS global map", err)
	}
	return f.decodeStoppedMDSScope(ctx, target, state, data)
}

func (f mdsFleet) decodeStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity, pools *fsbase.NativePools, data []byte) (*mdsmap.Scope, error) {
	state, err := mdsmap.DecodeStopped(data, mdsmap.StoppedExpectation{
		TargetName:    target.name,
		Filesystem:    f.core.Name(),
		Native:        mdsMapNative(target.native),
		Pools:         mdsMapPools(pools),
		ActiveMDS:     f.config.ActiveMDS,
		StandbyMDS:    f.config.StandbyMDS,
		StandbyReplay: f.config.StandbyReplay,
	})
	if err != nil {
		return nil, err
	}
	return f.verifyStoppedMDSSurvivors(ctx, target, state)
}

// verifyStoppedMDSSurvivors checks that every other owned daemon is a
// confirmed, normally running member of the decoded registered set. It is the
// only part of the scope read that inspects Docker, and it runs after the
// decoded map was accepted. Owner held.
func (f mdsFleet) verifyStoppedMDSSurvivors(ctx context.Context, target *cephFSMDSIdentity, state *mdsmap.StoppedState) (*mdsmap.Scope, error) {
	scope, registered := state.Scope, state.Registered
	remaining := 0
	seenNames, seenCIDs := map[string]bool{}, map[string]bool{}
	for _, daemon := range f.mds.roster.members() {
		if daemon == target.descriptor {
			continue
		}
		if daemon == nil || daemon.identity == nil {
			return nil, errors.New("original MDS survivor creation identity is unavailable")
		}
		i := daemon.identity
		if f.checkOwnedMDS(daemon, i) != nil || completedMDSReplacementAuthority(i) != nil || i.removed || i.fsid != target.fsid || !registered[i.name] || seenNames[i.name] || seenCIDs[i.cid] {
			return nil, errors.New("stopped MDS has an unconfirmed or foreign survivor")
		}
		info, err := i.container.Inspect(ctx)
		if err != nil {
			return nil, clusterutil.OperationError(ctx, "inspect original MDS survivor", err)
		}
		if i.container.GetContainerID() != i.cid || !containerRunning(info, i.cid) {
			return nil, errors.New("original MDS survivor is not running normally")
		}
		seenNames[i.name], seenCIDs[i.cid] = true, true
		remaining++
	}
	if remaining == 0 || remaining != len(registered) {
		return nil, errors.New("stopped MDS target includes unowned or missing survivors")
	}
	return scope, ctx.Err()
}
