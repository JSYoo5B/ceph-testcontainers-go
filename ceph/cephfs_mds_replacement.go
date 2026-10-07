package ceph

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// A receipt belongs to one original stopped descriptor. Creation confirmation
// and operation completion are different: every attempted error is cleanup-only.
type cephFSMDSReplacement struct {
	name      string
	daemon    *MDSContainer
	identity  *cephFSMDSIdentity
	scope     *cephFSMDSRemovalScope
	completed bool
}

// AddMDSReplacement starts a new indexed MDS for this filesystem's sole stopped
// original member, preserving its pools, data and desired one-active/zero-standby
// policy. The old name must already be globally absent and rank zero failed;
// native failure is caller-controlled. No native fail, auth deletion or old CID
// removal is performed. Once the new rank is ready, RemoveStoppedMDS can retire
// the original old container using the new healthy survivor.
//
// Any failure after the auth/start attempt leaves this request cleanup-only,
// including its use as typed scaling/retirement authority.
// A nonnil result remains owned even on error. Retrying the original or an exact
// copied handle never implicitly starts another daemon or completes a partial
// request. A completed retry reattests and returns only the exact new handle.
// Direct Start/Stop, native policy/failure edits and exported-field edits must
// not run concurrently. Native name/GID and original Docker CID are independent
// evidence; this method does not establish process-to-GID attribution.
func (fs *CephFSContainer) AddMDSReplacement(ctx context.Context, stopped *MDSContainer) (*MDSContainer, error) {
	if fs == nil || fs.cluster == nil {
		return nil, errors.New("original MDS replacement filesystem is unavailable")
	}
	return fs.addMDSReplacement(ctx, stopped, fs.cluster.startService)
}

func (fs *CephFSContainer) addMDSReplacement(parent context.Context, stopped *MDSContainer, start func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)) (*MDSContainer, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if stopped == nil || stopped.identity == nil {
		return nil, errors.New("original owned MDS creation identity is unavailable")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, c.settings.startupTimeout)
	defer cancel()
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return nil, err
	}
	defer c.cephfsSetupMu.Unlock()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	i := stopped.identity
	if previous := i.replacement; previous != nil && !previous.completed {
		err := fs.checkStoppedMDSOwner(stopped, i)
		if err == nil {
			err = fs.checkMDSReplacementPartial(i, previous)
		}
		c.mu.Unlock()
		if err != nil {
			return previous.daemon, err
		}
		return previous.daemon, errors.New("attempted MDS replacement is cleanup-only")
	}
	if err := fs.checkMDSReplacementOwner(stopped, i, i.replacement); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if previous := i.replacement; previous != nil {
		c.mu.Unlock()
		ready, err := fs.observeMDSReplacement(ctx, stopped, previous)
		if err != nil || !ready {
			return previous.daemon, clientOperationError(ctx, "reattest completed MDS replacement", err)
		}
		return previous.daemon, nil
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil || control == nil {
		c.mu.Unlock()
		return nil, clientOperationError(ctx, "select MDS replacement control", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if missing, err := inspectStoppedMDS(ctx, i, false); err != nil || missing {
		c.mu.Unlock()
		return nil, clientOperationError(ctx, "inspect original MDS before replacement", err)
	}
	r := &cephFSMDSReplacement{name: cephFSMDSID(fs.FilesystemName, fs.nextMDSIndex)}
	r.scope, _, err = fs.readMDSReplacementScope(ctx, i, r)
	if err == nil {
		err = fs.checkMDSReplacementAuth(ctx, control, r.name)
	}
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	admit := func(ctx context.Context, name string) (testcontainers.Container, error) {
		// The real starter owns c.mu here. Return selected control so the first
		// auth invocation cannot wait on another admission gate after receipt.
		if name != r.name || i.replacement != nil {
			return nil, errors.New("original MDS replacement reservation changed")
		}
		if err := fs.checkMDSReplacementOwner(stopped, i, nil); err != nil {
			return nil, err
		}
		selected, err := c.ControlContainerContext(ctx)
		if err != nil || selected == nil {
			return nil, clientOperationError(ctx, "select admitted MDS replacement control", err)
		}
		if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
			return nil, err
		}
		if missing, err := inspectStoppedMDS(ctx, i, false); err != nil || missing {
			return nil, clientOperationError(ctx, "reattest original stopped MDS", err)
		}
		scope, _, err := fs.readMDSReplacementScope(ctx, i, r)
		if err != nil || !reflect.DeepEqual(scope, r.scope) {
			return nil, clientOperationError(ctx, "MDS replacement scope changed before startup", err)
		}
		if err := fs.checkMDSReplacementAuth(ctx, selected, name); err != nil {
			return nil, err
		}
		data, err := command(ctx, selected, "ceph", "--connect-timeout", "5", "fsid")
		if err != nil {
			return nil, clientOperationError(ctx, "read selected MDS replacement cluster", err)
		}
		if _, err := coldMDSFSID(ctx, data, &cephFSColdMDS{fsid: i.fsid}); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		i.replacement = r // immediately before reserved name and auth invocation
		return selected, nil
	}
	published := func(daemon *MDSContainer) {
		r.daemon, r.identity = daemon, daemon.identity
		daemon.identity.createdByReplacement = r
	}
	newDaemon, err := fs.startMDSWithAdmission(ctx, start, admit, published)
	if err != nil {
		return newDaemon, clientOperationError(ctx, "start MDS replacement", err)
	}
	if newDaemon == nil || r.daemon != newDaemon || r.identity == nil {
		return newDaemon, errors.New("MDS replacement service returned no confirmed owned handle")
	}
	for {
		ready, err := fs.observeMDSReplacement(ctx, stopped, r)
		if err != nil {
			return newDaemon, err
		}
		if ready {
			// Recheck before publishing completion; creation success alone is
			// never enough to convert an attempted request to completed.
			ready, err = fs.observeMDSReplacement(ctx, stopped, r)
			if err != nil || !ready {
				return newDaemon, clientOperationError(ctx, "confirm MDS replacement", err)
			}
			if err := c.lockTopology(ctx); err != nil {
				return newDaemon, err
			}
			err = fs.checkMDSReplacementOwner(stopped, i, r)
			if err == nil {
				err = ctx.Err()
			}
			if err == nil {
				r.completed = true
			}
			c.mu.Unlock()
			return newDaemon, err
		}
		select {
		case <-ctx.Done():
			return newDaemon, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Creation confirmation is retained for exact cleanup ownership. It does not
// authorize typed retirement/scaling of an incomplete replacement operation.
// The core original-owner checker deliberately stays usable by observation.
func completedMDSReplacementAuthority(i *cephFSMDSIdentity) error {
	if i != nil && (i.createdByReplacement != nil && !i.createdByReplacement.completed || i.replacement != nil && !i.replacement.completed) {
		return errors.New("attempted MDS replacement is cleanup-only")
	}
	return nil
}

// Owner held. Untouched ordinary/cold publications have no replacement receipt.
func (fs *CephFSContainer) completedMDSReplacementCohort() error {
	for _, daemon := range fs.mdss {
		if daemon != nil {
			if err := completedMDSReplacementAuthority(daemon.identity); err != nil {
				return err
			}
		}
	}
	return nil
}

// Owner held; reuse Q's creation/registry/embed guard, not its survivor gate.
func (fs *CephFSContainer) checkMDSReplacementOwner(stopped *MDSContainer, i *cephFSMDSIdentity, r *cephFSMDSReplacement) error {
	if err := fs.checkStoppedMDSOwner(stopped, i); err != nil {
		return err
	}
	if fs.config.ActiveMDS != 1 || fs.config.StandbyMDS != 0 || fs.config.StandbyReplay || i.removalAttempted && (!i.removed || r == nil || !r.completed) {
		return errors.New("MDS replacement requires one active, zero standby and original stopped ownership")
	}
	if r == nil {
		if i.removed || len(fs.mdss) != 1 || fs.mdss[0] != i.descriptor {
			return errors.New("MDS replacement requires the sole original member")
		}
		return nil
	}
	if i.replacement != r {
		return errors.New("original MDS replacement receipt changed")
	}
	if r.daemon == nil {
		return nil // useful nil partial retained only for cleanup-only error
	}
	if r.identity == nil || r.daemon.identity != r.identity || r.identity.createdByReplacement != r || r.daemon.ID != r.name || r.identity.name != r.name || r.identity.fsid != i.fsid || r.identity.native != i.native || r.identity.removed || fs.checkStoppedMDSOwner(r.daemon, r.identity) != nil {
		return errors.New("original new MDS replacement ownership changed")
	}
	wanted := 2
	if i.removed {
		wanted = 1
	}
	if len(fs.mdss) != wanted {
		return errors.New("MDS replacement owned cohort changed")
	}
	return nil
}

// A failed receipt validates retained handles, never startup confirmation.
func (fs *CephFSContainer) checkMDSReplacementPartial(old *cephFSMDSIdentity, r *cephFSMDSReplacement) error {
	if r.daemon == nil {
		if len(fs.mdss) != 1 || fs.mdss[0] != old.descriptor {
			return errors.New("cleanup-only MDS replacement cohort changed")
		}
		return nil
	}
	i := r.identity
	if i == nil || r.daemon.identity != i || i.createdByReplacement != r || i.descriptor != r.daemon || i.filesystem != fs || i.native != old.native || i.name != r.name || r.daemon.ID != r.name || r.daemon.FilesystemName != fs.config.Name || r.daemon.Container != i.container || i.container == nil || i.container.GetContainerID() != i.cid || fs.cluster.services["mds."+r.name] != i.container || len(fs.mdss) != 2 || fs.mdss[0] != old.descriptor || fs.mdss[1] != r.daemon {
		return errors.New("cleanup-only original MDS replacement handle changed")
	}
	return nil
}

func (fs *CephFSContainer) checkMDSReplacementAuth(ctx context.Context, control testcontainers.Container, name string) error {
	data, err := command(ctx, control, "ceph", "--connect-timeout", "5", "auth", "ls", "--format", "json")
	if err != nil {
		return clientOperationError(ctx, "check replacement MDS identity names", err)
	}
	var auth struct {
		Entities *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if coldMDSJSON(data, &auth) != nil || auth.Entities == nil {
		return errors.New("replacement MDS identity names are incomplete")
	}
	seen := map[string]bool{}
	for _, entry := range *auth.Entities {
		if entry.Entity == nil || *entry.Entity == "" || strings.TrimSpace(*entry.Entity) != *entry.Entity || seen[*entry.Entity] {
			return errors.New("replacement MDS identity names are malformed or duplicated")
		}
		seen[*entry.Entity] = true
		if *entry.Entity == "mds."+name {
			return errors.New("reserved replacement MDS identity already exists")
		}
	}
	return ctx.Err()
}

func (fs *CephFSContainer) observeMDSReplacement(ctx context.Context, stopped *MDSContainer, r *cephFSMDSReplacement) (bool, error) {
	c := fs.cluster
	if err := c.lockTopology(ctx); err != nil {
		return false, err
	}
	defer c.mu.Unlock()
	i := stopped.identity
	if err := fs.checkMDSReplacementOwner(stopped, i, r); err != nil {
		return false, err
	}
	if r.daemon == nil || r.identity == nil || !r.identity.confirmed {
		return false, errors.New("replacement service creation is unconfirmed")
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return false, err
	}
	missing, err := inspectStoppedMDS(ctx, i, i.removed)
	if err != nil || missing != i.removed || i.removed && (!i.removalAttempted || i.scope == nil) {
		return false, clientOperationError(ctx, "reattest original replacement target CID", err)
	}
	info, err := r.identity.container.Inspect(ctx)
	if err != nil {
		return false, clientOperationError(ctx, "inspect original new MDS replacement", err)
	}
	if r.identity.container.GetContainerID() != r.identity.cid || info == nil || info.ID != r.identity.cid || info.State == nil || !info.State.Running || info.State.Restarting || info.State.Paused || info.State.Dead || info.State.Pid <= 0 || info.State.Error != "" || info.State.Status != "running" {
		return false, errors.New("original new MDS replacement is not running normally")
	}
	scope, ready, err := fs.readMDSReplacementScope(ctx, i, r)
	if err != nil || !reflect.DeepEqual(scope, r.scope) {
		return false, clientOperationError(ctx, "original MDS replacement scope changed", err)
	}
	if i.removed && !reflect.DeepEqual(i.scope, r.scope) {
		return false, errors.New("completed original MDS removal scope differs from replacement")
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		return false, err
	}
	if err := fs.checkMDSReplacementOwner(stopped, i, r); err != nil {
		return false, err
	}
	return ready, ctx.Err()
}

type replacementMDSMap struct {
	Standbys    *[]stoppedMDSRow `json:"standbys"`
	Filesystems *[]struct {
		ID  *int64 `json:"id"`
		Map *struct {
			Name                         *string  `json:"fs_name"`
			Max                          *int     `json:"max_mds"`
			Wanted                       *int     `json:"standby_count_wanted"`
			Metadata                     *int64   `json:"metadata_pool"`
			Data                         *[]int64 `json:"data_pools"`
			In, Failed, Damaged, Stopped *[]int
			Up                           *map[string]uint64        `json:"up"`
			Info                         *map[string]stoppedMDSRow `json:"info"`
			Flags                        *struct {
				Joinable *bool `json:"joinable"`
				Replay   *bool `json:"allow_standby_replay"`
				Refuse   *bool `json:"refuse_standby_for_another_fs"`
			} `json:"flags_state"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

func (fs *CephFSContainer) readMDSReplacementScope(ctx context.Context, old *cephFSMDSIdentity, r *cephFSMDSReplacement) (*cephFSMDSRemovalScope, bool, error) {
	pools, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, false, clientOperationError(ctx, "read original MDS replacement pools", err)
	}
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, false, clientOperationError(ctx, "read original MDS replacement map", err)
	}
	var native replacementMDSMap
	if coldMDSJSON(data, &native) != nil || native.Filesystems == nil || native.Standbys == nil {
		return nil, false, errors.New("MDS replacement map is incomplete or malformed")
	}
	names, gids, fsNames, fsIDs := map[string]bool{}, map[uint64]bool{}, map[string]bool{}, map[int64]bool{}
	validRow := func(row stoppedMDSRow) error {
		if row.Name != nil && *row.Name == old.name {
			return errors.New("original stopped MDS name is still registered")
		}
		if row.Name == nil || !poolResourceName.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil || row.Join == nil || *row.Join < -1 || row.State == nil || !slices.Contains([]string{"up:boot", "up:standby", "up:standby-replay", "up:creating", "up:starting", "up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active", "up:stopping"}, *row.State) || names[*row.Name] || gids[*row.GID] {
			return errors.New("MDS replacement global daemon map is contradictory")
		}
		names[*row.Name], gids[*row.GID] = true, true
		return nil
	}
	var scope *cephFSMDSRemovalScope
	ready := false
	for _, entry := range *native.Filesystems {
		m := entry.Map
		if entry.ID == nil || *entry.ID < 0 || fsIDs[*entry.ID] || m == nil || m.Name == nil || *m.Name == "" || fsNames[*m.Name] || m.Info == nil || m.Up == nil {
			return nil, false, errors.New("MDS replacement global filesystem map is incomplete")
		}
		fsIDs[*entry.ID], fsNames[*m.Name] = true, true
		for key, row := range *m.Info {
			if err := validRow(row); err != nil {
				return nil, false, err
			}
			if key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" || *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
				return nil, false, errors.New("MDS replacement global rank membership is malformed")
			}
			if *m.Name != fs.config.Name && *row.Name == r.name {
				return nil, false, errors.New("reserved MDS replacement name belongs to another filesystem")
			}
		}
		for key, gid := range *m.Up {
			row, ok := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
			if !ok || gid == 0 || row.Rank == nil || key != "mds_"+strconv.Itoa(*row.Rank) || row.State == nil || *row.State == "up:standby-replay" {
				return nil, false, errors.New("MDS replacement global up map has no matching daemon")
			}
		}
		if *m.Name != fs.config.Name {
			continue
		}
		if *entry.ID != old.native.id || m.Metadata == nil || *m.Metadata != old.native.metadataPool || m.Data == nil || !slices.Equal(*m.Data, pools.dataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || !slices.Equal(*m.In, []int{0}) || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Damaged)+len(*m.Stopped) != 0 {
			return nil, false, errors.New("original MDS replacement filesystem policy changed")
		}
		scope = &cephFSMDSRemovalScope{active: 1, standby: 0, joinable: true, protected: true, dataPools: slices.Clone(pools.dataPools)}
		seenPools := map[int64]bool{}
		for _, id := range pools.dataPools {
			if id < 0 || seenPools[id] {
				return nil, false, errors.New("MDS replacement data attachments are invalid or duplicated")
			}
			seenPools[id] = true
			pool, err := pools.poolByID(id)
			if err != nil {
				return nil, false, errors.New("MDS replacement attached pool is unavailable")
			}
			scope.poolNames = append(scope.poolNames, pool.Name)
		}
		if r.daemon == nil {
			if len(*m.Info) != 0 || len(*m.Up) != 0 || !slices.Equal(*m.Failed, []int{0}) {
				return nil, false, errors.New("original MDS replacement requires failed rank zero without a worker")
			}
		} else if len(*m.Info) == 0 && len(*m.Up) == 0 && slices.Equal(*m.Failed, []int{0}) {
			// Original newly-created service has not yet registered.
		} else {
			if len(*m.Info) != 1 || len(*m.Up) != 1 || len(*m.Failed) != 0 {
				return nil, false, errors.New("MDS replacement rank is not the exact new member")
			}
			for _, row := range *m.Info {
				if *row.Name != r.name || *row.Rank != 0 || *row.Join != old.native.id || len(row.Laggy) != 0 || !slices.Contains([]string{"up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active"}, *row.State) {
					return nil, false, errors.New("MDS replacement native rank identity or recovery state changed")
				}
				ready = *row.State == "up:active"
			}
		}
	}
	if scope == nil {
		return nil, false, errors.New("original MDS replacement filesystem is absent")
	}
	for _, row := range *native.Standbys {
		if err := validRow(row); err != nil {
			return nil, false, err
		}
		if *row.Rank != -1 || *row.State != "up:standby" {
			return nil, false, errors.New("MDS replacement global standby map is malformed")
		}
		if *row.Name == r.name {
			if r.daemon == nil || *row.Join != old.native.id || len(row.Laggy) != 0 {
				return nil, false, errors.New("reserved MDS replacement name is already registered")
			}
			ready = false
		} else if *row.Join == -1 || *row.Join == old.native.id || !fsIDs[*row.Join] {
			return nil, false, errors.New("foreign standby can replace the failed original MDS rank")
		}
	}
	return scope, ready, ctx.Err()
}
