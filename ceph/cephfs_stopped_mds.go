package ceph

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// Creation and attempt state is shared by copied descriptive handles. Native
// GIDs are deliberately not attributed to a CID by this cleanup operation.
type cephFSMDSIdentity struct {
	filesystem                           *CephFSContainer
	native                               *cephFSNativeIdentity
	descriptor                           *MDSContainer
	container                            testcontainers.Container
	name, cid, fsid                      string
	confirmed, removalAttempted, removed bool
	scope                                *cephFSMDSRemovalScope
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
func (fs *CephFSContainer) captureMDSIdentity(ctx context.Context, daemon *MDSContainer, ctr testcontainers.Container, startErr error) *cephFSMDSIdentity {
	i := &cephFSMDSIdentity{filesystem: fs, native: fs.nativeIdentity, descriptor: daemon, container: ctr, name: daemon.ID, cid: ctr.GetContainerID()}
	if startErr != nil || i.native == nil || !cephFSMDSFullCID(i.cid) {
		return i
	}
	c := fs.cluster
	if lockTopologyReadMutex(ctx, &c.configMu) != nil {
		return i
	}
	fsid, err := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
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

// RemoveStoppedMDS retires one original owned, stopped container after its
// immutable daemon name has left every native filesystem and global standby
// list. Healthy original owned daemons must still serve every desired active
// rank. It neither fails a native MDS nor deletes its auth entity or changes
// desired capacity. Use ScaleMDS afterward to create missing replacement members.
//
// Only successful original startups are eligible. A partial startup remains
// owned for cluster Terminate. An uncertain Docker removal retains the original
// handle for explicit retry; initial arbitrary absence is not success. Direct
// failure injection, Start/Stop and raw policy changes must not run concurrently.
func (fs *CephFSContainer) RemoveStoppedMDS(parent context.Context, daemon *MDSContainer) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if fs == nil || fs.cluster == nil || daemon == nil || daemon.identity == nil {
		return errors.New("original owned MDS creation identity is unavailable")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, c.settings.startupTimeout)
	defer cancel()
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return err
	}
	defer c.cephfsSetupMu.Unlock()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	i := daemon.identity
	if err := fs.checkStoppedMDSOwner(daemon, i); err != nil {
		return err
	}
	// Admission to control/config precedes any Docker or native reads.
	control, err := c.ControlContainerContext(ctx)
	if err != nil || control == nil {
		return clientOperationError(ctx, "select stopped MDS control", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return err
	}
	missing, err := inspectStoppedMDS(ctx, i, false)
	if err != nil {
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
		return clientOperationError(ctx, "stopped MDS scope changed before removal", err)
	}
	if err := fs.checkStoppedMDSOwner(daemon, i); err != nil {
		return err
	}
	missing, err = inspectStoppedMDS(ctx, i, false)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !i.removed && !missing {
		i.scope, i.removalAttempted = scope, true
		if err := i.container.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
			return clientOperationError(ctx, "terminate original stopped MDS", err)
		}
	}
	// A success reply is followed by exact full-CID absence, then fresh native
	// authority. Context/error loss leaves the retained pending owner for retry.
	missing, err = inspectStoppedMDS(ctx, i, true)
	if err != nil || !missing {
		return clientOperationError(ctx, "confirm original stopped MDS removal", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return err
	}
	after, err = fs.readStoppedMDSScope(ctx, i)
	if err != nil || !reflect.DeepEqual(scope, after) {
		return clientOperationError(ctx, "stopped MDS scope changed after removal", err)
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
	delete(c.services, "mds."+i.name)
	fs.mdss = slices.DeleteFunc(fs.mdss, func(owned *MDSContainer) bool { return owned == i.descriptor })
	if fs.Container == i.container {
		fs.Container = fs.mdss[0].identity.container
	}
	i.removed = true
	return nil
}

// Owner held; exported fields cannot redirect either target or survivor reads.
func (fs *CephFSContainer) checkStoppedMDSOwner(handle *MDSContainer, i *cephFSMDSIdentity) error {
	c := fs.cluster
	if c.closed || fs.FilesystemName != fs.config.Name || fs.MetadataPool != fs.config.MetadataPool.Name || fs.DataPool != fs.config.DataPool.Name || c.filesystems[fs.config.Name] != fs || i.filesystem != fs || !i.confirmed || i.native == nil || i.native != fs.nativeIdentity || i.container == nil || i.container.GetContainerID() != i.cid || i.descriptor == nil || i.descriptor.identity != i || handle.ID != i.name || handle.FilesystemName != fs.config.Name || handle.Container != i.container {
		return errors.New("original stopped MDS ownership changed or startup is unconfirmed")
	}
	if i.descriptor.ID != i.name || i.descriptor.FilesystemName != fs.config.Name || i.descriptor.Container != i.container {
		return errors.New("original stopped MDS descriptor was edited")
	}
	count, embedded := 0, false
	for _, owned := range fs.mdss {
		if owned == i.descriptor {
			count++
		}
		if owned != nil && owned.identity != nil && !owned.identity.removed && owned.identity.container == fs.Container {
			embedded = true
		}
	}
	if !embedded {
		return errors.New("filesystem compatibility container is not an original owned MDS")
	}
	service := c.services["mds."+i.name]
	if (!i.removed && (count != 1 || service != i.container)) || (i.removed && (count != 0 || service != nil)) {
		return errors.New("original stopped MDS registry ownership changed")
	}
	return nil
}

// Owner held. Reuse the confirmed bootstrap parser, fresh native quorum and
// address validation, with context-aware admission to its existing cache gate.
func (fs *CephFSContainer) checkStoppedMDSCluster(ctx context.Context, i *cephFSMDSIdentity) error {
	c := fs.cluster
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return err
	}
	fsid, err := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if err != nil || !confirmed || fsid != i.fsid {
		return errors.New("original stopped MDS cluster bootstrap changed")
	}
	data, err := c.Ceph(ctx, "quorum_status", "--format", "json")
	if err != nil {
		return clientOperationError(ctx, "read stopped MDS cluster quorum", err)
	}
	var quorum QuorumStatus
	if coldMDSJSON(data, &quorum) != nil || quorum.MonMap.FSID != i.fsid {
		return errors.New("native stopped MDS cluster identity changed")
	}
	if _, err := monitorBootstrapAddresses(quorum); err != nil {
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
		if onlyMissingHostResource(err) && (i.removalAttempted || i.removed) {
			return true, ctx.Err()
		}
		return false, clientOperationError(ctx, "inspect original stopped MDS", err)
	}
	if info == nil || info.ID != i.cid || info.State == nil || info.State.Running || info.State.Restarting || info.State.Paused || info.State.Dead || info.State.Pid != 0 || info.State.Status != "exited" || requireMissing {
		return false, errors.New("original MDS must be positively stopped or confirmed removed")
	}
	return false, ctx.Err()
}

// Strict global absence and logical owned-survivor membership come from one
// FSMap. This never attributes a native GID to a Docker process or adopts one.
func (fs *CephFSContainer) readStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity) (*cephFSMDSRemovalScope, error) {
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, clientOperationError(ctx, "read original stopped MDS filesystem pools", err)
	}
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, clientOperationError(ctx, "read stopped MDS global map", err)
	}
	return fs.decodeStoppedMDSScope(ctx, target, state, data)
}

func (fs *CephFSContainer) decodeStoppedMDSScope(ctx context.Context, target *cephFSMDSIdentity, pools *cephFSNativePools, data []byte) (*cephFSMDSRemovalScope, error) {
	var native struct {
		Standbys    *[]stoppedMDSRow `json:"standbys"`
		Filesystems *[]struct {
			ID  *int64 `json:"id"`
			Map *struct {
				Name     *string                   `json:"fs_name"`
				Max      *int                      `json:"max_mds"`
				Wanted   *int                      `json:"standby_count_wanted"`
				Metadata *int64                    `json:"metadata_pool"`
				Data     *[]int64                  `json:"data_pools"`
				Info     *map[string]stoppedMDSRow `json:"info"`
				Up       *map[string]uint64        `json:"up"`
				In       *[]int                    `json:"in"`
				Failed   *[]int                    `json:"failed"`
				Damaged  *[]int                    `json:"damaged"`
				Stopped  *[]int                    `json:"stopped"`
				Flags    *struct {
					Joinable *bool `json:"joinable"`
					Replay   *bool `json:"allow_standby_replay"`
					Refuse   *bool `json:"refuse_standby_for_another_fs"`
				} `json:"flags_state"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if coldMDSJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return nil, errors.New("decode stopped MDS global map")
	}
	rows := map[string]stoppedMDSRow{}
	gids, fsNames, fsIDs := map[uint64]bool{}, map[string]bool{}, map[int64]bool{}
	validRow := func(row stoppedMDSRow) bool {
		if row.Name == nil || !poolResourceName.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.State == nil || !slices.Contains([]string{"up:boot", "up:standby", "up:standby-replay", "up:creating", "up:starting", "up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active", "up:stopping"}, *row.State) || row.Rank == nil || row.Join == nil || *row.Join < -1 || gids[*row.GID] || rows[*row.Name].Name != nil || *row.Name == target.name {
			return false
		}
		gids[*row.GID], rows[*row.Name] = true, row
		return true
	}
	var scope *cephFSMDSRemovalScope
	active := map[int]string{}
	registered := map[string]bool{}
	for _, entry := range *native.Filesystems {
		m := entry.Map
		if entry.ID == nil || *entry.ID < 0 || m == nil || m.Name == nil || *m.Name == "" || fsIDs[*entry.ID] || fsNames[*m.Name] || m.Info == nil || m.Up == nil {
			return nil, errors.New("stopped MDS filesystem entries are incomplete or duplicated")
		}
		fsIDs[*entry.ID], fsNames[*m.Name] = true, true
		for key, row := range *m.Info {
			if row.Name != nil && *row.Name == target.name {
				return nil, errors.New("original stopped MDS name is still registered")
			}
			if !validRow(row) || key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:standby" || *row.State == "up:boot" {
				return nil, errors.New("stopped MDS global daemon identity is present or malformed")
			}
		}
		// Every globally assigned GID must have a corresponding complete info
		// row. A missing sibling row cannot prove the target name absent.
		for key, gid := range *m.Up {
			text, ok := strings.CutPrefix(key, "mds_")
			rank, err := strconv.Atoi(text)
			row, found := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
			if !ok || err != nil || rank < 0 || text != strconv.Itoa(rank) || gid == 0 || !found || *row.GID != gid || *row.Rank != rank || *row.State == "up:standby-replay" {
				return nil, errors.New("global stopped MDS assigned membership is incomplete")
			}
		}
		for _, row := range *m.Info {
			if *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
				return nil, errors.New("global stopped MDS info membership is contradictory")
			}
		}
		if *m.Name != fs.config.Name {
			continue
		}
		if *entry.ID != target.native.id || m.Metadata == nil || *m.Metadata != target.native.metadataPool || m.Data == nil || !slices.Equal(*m.Data, pools.dataPools) || m.Max == nil || *m.Max != fs.config.ActiveMDS || m.Wanted == nil || *m.Wanted != fs.config.StandbyMDS || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay != fs.config.StandbyReplay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 || len(*m.In) != *m.Max || len(*m.Up) != *m.Max {
			return nil, errors.New("original stopped MDS filesystem policy or settled ranks changed")
		}
		scope = &cephFSMDSRemovalScope{dataPools: slices.Clone(*m.Data), active: *m.Max, standby: *m.Wanted, joinable: *m.Flags.Joinable, replay: *m.Flags.Replay, protected: *m.Flags.Refuse}
		seenIn := map[int]bool{}
		for _, rank := range *m.In {
			if rank < 0 || rank >= *m.Max || seenIn[rank] {
				return nil, errors.New("stopped MDS active rank set is malformed")
			}
			seenIn[rank] = true
		}
		for _, row := range *m.Info {
			if *row.Join != *entry.ID || (*row.State != "up:active" && *row.State != "up:standby-replay") || len(row.Laggy) != 0 || registered[*row.Name] {
				return nil, errors.New("stopped MDS target has unsettled or foreign assigned members")
			}
			registered[*row.Name] = true
			if *row.State == "up:active" {
				if *row.Rank >= *m.Max || active[*row.Rank] != "" || (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
					return nil, errors.New("stopped MDS active rank membership is contradictory")
				}
				active[*row.Rank] = *row.Name
			} else if !scope.replay || *row.Rank >= *m.Max {
				return nil, errors.New("stopped MDS replay membership is contradictory")
			}
		}
	}
	if scope == nil || scope.active < 1 || len(active) != scope.active {
		return nil, errors.New("stopped MDS filesystem has no complete active survivor set")
	}
	for _, row := range *native.Standbys {
		if row.Name != nil && *row.Name == target.name {
			return nil, errors.New("original stopped MDS name is still registered")
		}
		if !validRow(row) || *row.Rank != -1 || *row.State != "up:standby" {
			return nil, errors.New("stopped MDS global standby identity is present or malformed")
		}
		if *row.Join == target.native.id {
			if len(row.Laggy) != 0 {
				return nil, errors.New("owned MDS standby is laggy or has malformed laggy state")
			}
			registered[*row.Name] = true
		}
	}
	for _, id := range scope.dataPools {
		pool, err := pools.poolByID(id)
		if err != nil {
			return nil, errors.New("stopped MDS attached data pool is unavailable")
		}
		scope.poolNames = append(scope.poolNames, pool.Name)
	}
	remaining := 0
	seenNames, seenCIDs := map[string]bool{}, map[string]bool{}
	for _, daemon := range fs.mdss {
		if daemon == target.descriptor {
			continue
		}
		if daemon == nil || daemon.identity == nil {
			return nil, errors.New("original MDS survivor creation identity is unavailable")
		}
		i := daemon.identity
		if fs.checkStoppedMDSOwner(daemon, i) != nil || i.removed || i.fsid != target.fsid || !registered[i.name] || seenNames[i.name] || seenCIDs[i.cid] {
			return nil, errors.New("stopped MDS has an unconfirmed or foreign survivor")
		}
		info, err := i.container.Inspect(ctx)
		if err != nil {
			return nil, clientOperationError(ctx, "inspect original MDS survivor", err)
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
