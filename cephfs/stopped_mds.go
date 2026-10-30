package cephfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// Creation and attempt state is shared by copied descriptive handles. Native
// GIDs are deliberately not attributed to a CID by this cleanup operation.
type cephFSMDSIdentity struct {
	filesystem                           *Filesystem
	native                               *cephFSNativeIdentity
	descriptor                           *MDS
	container                            testcontainers.Container
	name, cid, fsid                      string
	confirmed, removalAttempted, removed bool
	scope                                *cephFSMDSRemovalScope
	replacement                          *cephFSMDSReplacement
	createdByReplacement                 *cephFSMDSReplacement
}

type cephFSMDSRemovalScope struct {
	dataPools                   []int64
	poolNames                   []string
	active, standby             int
	joinable, replay, protected bool
}

type stoppedMDSRow struct {
	coldMDSRow
	Laggy json.RawMessage `json:"laggy_since"`
}

// Called under c.mu at real nonnil service publication. Failed/canceled capture
// never loses the cleanup handle or changes the original starter's error.
func (fs *Filesystem) captureMDSIdentity(ctx context.Context, daemon *MDS, ctr testcontainers.Container, startErr error) *cephFSMDSIdentity {
	i := &cephFSMDSIdentity{filesystem: fs, native: fs.nativeIdentity, descriptor: daemon, container: ctr, name: daemon.ID, cid: ctr.GetContainerID()}
	if startErr != nil || i.native == nil || !cephFSMDSFullCID(i.cid) {
		return i
	}
	fsid, confirmed, err := servicehook.ClusterIdentity(ctx, fs.cluster)
	if err == nil && confirmed && ctx.Err() == nil {
		i.fsid, i.confirmed = fsid, true
	}
	return i
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
func (daemon *MDS) RemoveStopped(ctx context.Context) error {
	if daemon == nil || daemon.identity == nil || daemon.identity.filesystem == nil {
		return errors.New("original owned MDS creation identity is unavailable")
	}
	return daemon.identity.filesystem.removeStoppedMDS(ctx, daemon)
}

// removeStoppedMDS implements MDS.RemoveStopped for the filesystem that
// created daemon.
func (fs *Filesystem) removeStoppedMDS(parent context.Context, daemon *MDS) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if fs == nil || fs.cluster == nil || daemon == nil || daemon.identity == nil {
		return errors.New("original owned MDS creation identity is unavailable")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, servicehook.StartupTimeout(c))
	defer cancel()
	release, err := lockSetupThenTopology(ctx, c)
	if err != nil {
		return err
	}
	defer release()
	defer servicehook.Unlock(c)
	i := daemon.identity
	if err := fs.checkStoppedMDSOwner(daemon, i); err != nil {
		return err
	}
	if err := completedMDSReplacementAuthority(i); err != nil {
		return err
	}
	// Admission to control/config precedes any Docker or native reads.
	control, err := c.ControlContainer(ctx)
	if err != nil || control == nil {
		return clusterutil.OperationError(ctx, "select stopped MDS control", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return err
	}
	if _, err := inspectStoppedMDS(ctx, i, false); err != nil {
		return err
	}
	scope, err := fs.readStoppedMDSScope(ctx, i)
	if err != nil {
		return err
	}
	if i.removalAttempted && !i.removed && !reflect.DeepEqual(i.scope, scope) {
		return errors.New("admitted stopped MDS policy or attachments changed")
	}
	// Reattest original native/owner/state immediately before a Docker attempt.
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return err
	}
	after, err := fs.readStoppedMDSScope(ctx, i)
	if err != nil || !reflect.DeepEqual(scope, after) {
		return clusterutil.OperationError(ctx, "stopped MDS scope changed before removal", err)
	}
	if err := fs.checkStoppedMDSOwner(daemon, i); err != nil {
		return err
	}
	missing, err := inspectStoppedMDS(ctx, i, false)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !i.removed && !missing {
		i.scope, i.removalAttempted = scope, true
		if err := i.container.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
			return clusterutil.OperationError(ctx, "terminate original stopped MDS", err)
		}
	}
	// A success reply is followed by exact full-CID absence, then fresh native
	// authority. Context/error loss leaves the retained pending owner for retry.
	missing, err = inspectStoppedMDS(ctx, i, true)
	if err != nil || !missing {
		return clusterutil.OperationError(ctx, "confirm original stopped MDS removal", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return err
	}
	after, err = fs.readStoppedMDSScope(ctx, i)
	if err != nil || !reflect.DeepEqual(scope, after) {
		return clusterutil.OperationError(ctx, "stopped MDS scope changed after removal", err)
	}
	if err := fs.checkStoppedMDSOwner(daemon, i); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.removed {
		return nil
	}
	servicehook.DeleteService(c, "mds."+i.name)
	fs.roster.remove(i.descriptor)
	if fs.Container == i.container {
		fs.Container = fs.roster.at(0).identity.container
	}
	i.removed = true
	return nil
}

// Owner held; exported fields cannot redirect either target or survivor reads.
func (fs *Filesystem) checkStoppedMDSOwner(handle *MDS, i *cephFSMDSIdentity) error {
	c := fs.cluster
	if !fs.registered() || fs.FilesystemName != fs.name || fs.MetadataPool != fs.config.MetadataPool.Name || fs.DataPool != fs.config.DataPool.Name || i.filesystem != fs || !i.confirmed || i.native == nil || i.native != fs.nativeIdentity || i.container == nil || i.container.GetContainerID() != i.cid || i.descriptor == nil || i.descriptor.identity != i || handle.ID != i.name || handle.FilesystemName != fs.name || handle.Container != i.container {
		return errors.New("original stopped MDS ownership changed or startup is unconfirmed")
	}
	if i.descriptor.ID != i.name || i.descriptor.FilesystemName != fs.name || i.descriptor.Container != i.container {
		return servicehook.Invalid(errors.New("original stopped MDS descriptor was edited"))
	}
	count, embedded := 0, false
	for _, owned := range fs.roster.members() {
		if owned == i.descriptor {
			count++
		}
		if owned != nil && owned.identity != nil && !owned.identity.removed && owned.identity.container == fs.Container {
			embedded = true
		}
	}
	if !embedded {
		return servicehook.Invalid(errors.New("filesystem compatibility container is not an original owned MDS"))
	}
	service := servicehook.Service(c, "mds."+i.name)
	if (!i.removed && (count != 1 || service != i.container)) || (i.removed && (count != 0 || service != nil)) {
		return errors.New("original stopped MDS registry ownership changed")
	}
	return nil
}

// Owner held. Reuse the confirmed bootstrap parser, fresh native quorum and
// address validation, with context-aware admission to its existing cache gate.
func (fs *Filesystem) checkStoppedMDSCluster(ctx context.Context, i *cephFSMDSIdentity) error {
	c := fs.cluster
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
	if coldMDSJSON(data, &quorum) != nil || quorum.MonMap.FSID != i.fsid {
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
	if info == nil || info.ID != i.cid || info.State == nil || info.State.Running || info.State.Restarting || info.State.Paused || info.State.Dead || info.State.Pid != 0 || info.State.Status != "exited" || requireMissing {
		return false, errors.New("original MDS must be positively stopped or confirmed removed")
	}
	return false, ctx.Err()
}

// Strict global absence and logical owned-survivor membership come from one
// FSMap. This never attributes a native GID to a Docker process or adopts one.
func (fs *Filesystem) readStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity) (*cephFSMDSRemovalScope, error) {
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, clusterutil.OperationError(ctx, "read original stopped MDS filesystem pools", err)
	}
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, clusterutil.OperationError(ctx, "read stopped MDS global map", err)
	}
	return fs.decodeStoppedMDSScope(ctx, target, state, data)
}

func (fs *Filesystem) decodeStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity, pools *cephFSNativePools, data []byte) (*cephFSMDSRemovalScope, error) {
	state, err := decodeStoppedMDSMap(data, stoppedMDSMapExpectation{
		targetName:    target.name,
		filesystem:    fs.name,
		native:        target.native,
		pools:         pools,
		activeMDS:     fs.config.ActiveMDS,
		standbyMDS:    fs.config.StandbyMDS,
		standbyReplay: fs.config.StandbyReplay,
	})
	if err != nil {
		return nil, err
	}
	return fs.verifyStoppedMDSSurvivors(ctx, target, state)
}

// verifyStoppedMDSSurvivors checks that every other owned daemon is a
// confirmed, normally running member of the decoded registered set. It is the
// only part of the scope read that inspects Docker, and it runs after the
// decoded map was accepted. Owner held.
func (fs *Filesystem) verifyStoppedMDSSurvivors(ctx context.Context, target *cephFSMDSIdentity, state *stoppedMDSMapState) (*cephFSMDSRemovalScope, error) {
	scope, registered := state.scope, state.registered
	remaining := 0
	seenNames, seenCIDs := map[string]bool{}, map[string]bool{}
	for _, daemon := range fs.roster.members() {
		if daemon == target.descriptor {
			continue
		}
		if daemon == nil || daemon.identity == nil {
			return nil, errors.New("original MDS survivor creation identity is unavailable")
		}
		i := daemon.identity
		if fs.checkStoppedMDSOwner(daemon, i) != nil || completedMDSReplacementAuthority(i) != nil || i.removed || i.fsid != target.fsid || !registered[i.name] || seenNames[i.name] || seenCIDs[i.cid] {
			return nil, errors.New("stopped MDS has an unconfirmed or foreign survivor")
		}
		info, err := i.container.Inspect(ctx)
		if err != nil {
			return nil, clusterutil.OperationError(ctx, "inspect original MDS survivor", err)
		}
		if i.container.GetContainerID() != i.cid || info == nil || info.ID != i.cid || info.State == nil || !info.State.Running || info.State.Paused || info.State.Restarting || info.State.Dead || info.State.Pid <= 0 || info.State.Error != "" || info.State.Status != "running" {
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
