package cephfs

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// A receipt belongs to one original stopped descriptor. Creation confirmation
// and operation completion are different: every attempted error is cleanup-only.
type cephFSMDSReplacement struct {
	name      string
	daemon    *MDS
	identity  *cephFSMDSIdentity
	scope     *cephFSMDSRemovalScope
	completed bool
}

// AddReplacement starts a new indexed MDS for the filesystem of this stopped
// MDS, its sole original member, preserving its pools, data and desired one-active/zero-standby
// policy. The old name must already be globally absent and rank zero failed;
// native failure is caller-controlled. No native fail, auth deletion or old CID
// removal is performed. Once the new rank is ready, MDS.RemoveStopped can retire
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
func (stopped *MDS) AddReplacement(ctx context.Context) (*MDS, error) {
	if stopped == nil || stopped.identity == nil || stopped.identity.filesystem == nil {
		return nil, errors.New("original owned MDS creation identity is unavailable")
	}
	fs := stopped.identity.filesystem
	if fs.cluster == nil {
		return nil, errors.New("original MDS replacement filesystem is unavailable")
	}
	return fs.addMDSReplacement(ctx, stopped, fs.startService)
}

func (fs *Filesystem) addMDSReplacement(parent context.Context, stopped *MDS, start func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)) (*MDS, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if stopped == nil || stopped.identity == nil {
		return nil, errors.New("original owned MDS creation identity is unavailable")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, servicehook.StartupTimeout(c))
	defer cancel()
	release, err := lockSetupThenTopology(ctx, c)
	if err != nil {
		return nil, err
	}
	defer release()
	i := stopped.identity
	if previous := i.replacement; previous != nil && !previous.completed {
		err := fs.checkStoppedMDSOwner(stopped, i)
		if err == nil {
			err = fs.checkMDSReplacementPartial(i, previous)
		}
		servicehook.Unlock(c)
		if err != nil {
			return previous.daemon, err
		}
		return previous.daemon, errors.New("attempted MDS replacement is cleanup-only")
	}
	if err := fs.checkMDSReplacementOwner(stopped, i, i.replacement); err != nil {
		servicehook.Unlock(c)
		return nil, err
	}
	if previous := i.replacement; previous != nil {
		servicehook.Unlock(c)
		ready, err := fs.observeMDSReplacement(ctx, stopped, previous)
		if err != nil || !ready {
			return previous.daemon, clusterutil.OperationError(ctx, "reattest completed MDS replacement", err)
		}
		return previous.daemon, nil
	}
	control, err := c.ControlContainer(ctx)
	if err != nil || control == nil {
		servicehook.Unlock(c)
		return nil, clusterutil.OperationError(ctx, "select MDS replacement control", err)
	}
	if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
		servicehook.Unlock(c)
		return nil, err
	}
	if missing, err := inspectStoppedMDS(ctx, i, false); err != nil || missing {
		servicehook.Unlock(c)
		return nil, clusterutil.OperationError(ctx, "inspect original MDS before replacement", err)
	}
	r := &cephFSMDSReplacement{name: fs.roster.nextID(fs.FilesystemName)}
	r.scope, _, err = fs.readMDSReplacementScope(ctx, i, r)
	if err == nil {
		err = fs.checkMDSReplacementAuth(ctx, control, r.name)
	}
	servicehook.Unlock(c)
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
		selected, err := c.ControlContainer(ctx)
		if err != nil || selected == nil {
			return nil, clusterutil.OperationError(ctx, "select admitted MDS replacement control", err)
		}
		if err := fs.checkStoppedMDSCluster(ctx, i); err != nil {
			return nil, err
		}
		if missing, err := inspectStoppedMDS(ctx, i, false); err != nil || missing {
			return nil, clusterutil.OperationError(ctx, "reattest original stopped MDS", err)
		}
		scope, _, err := fs.readMDSReplacementScope(ctx, i, r)
		if err != nil || !reflect.DeepEqual(scope, r.scope) {
			return nil, clusterutil.OperationError(ctx, "MDS replacement scope changed before startup", err)
		}
		if err := fs.checkMDSReplacementAuth(ctx, selected, name); err != nil {
			return nil, err
		}
		data, err := servicehook.Command(ctx, selected, "ceph", "--connect-timeout", "5", "fsid")
		if err != nil {
			return nil, clusterutil.OperationError(ctx, "read selected MDS replacement cluster", err)
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
	published := func(daemon *MDS) {
		r.daemon, r.identity = daemon, daemon.identity
		daemon.identity.createdByReplacement = r
	}
	newDaemon, err := fs.startMDSWithAdmission(ctx, start, admit, published)
	if err != nil {
		return newDaemon, clusterutil.OperationError(ctx, "start MDS replacement", err)
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
				return newDaemon, clusterutil.OperationError(ctx, "confirm MDS replacement", err)
			}
			if err := servicehook.LockTopology(ctx, c); err != nil {
				return newDaemon, err
			}
			err = fs.checkMDSReplacementOwner(stopped, i, r)
			if err == nil {
				err = ctx.Err()
			}
			if err == nil {
				r.completed = true
			}
			servicehook.Unlock(c)
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
func (fs *Filesystem) completedMDSReplacementCohort() error {
	for _, daemon := range fs.roster.members() {
		if daemon != nil {
			if err := completedMDSReplacementAuthority(daemon.identity); err != nil {
				return err
			}
		}
	}
	return nil
}

// Owner held; reuse Q's creation/registry/embed guard, not its survivor gate.
func (fs *Filesystem) checkMDSReplacementOwner(stopped *MDS, i *cephFSMDSIdentity, r *cephFSMDSReplacement) error {
	if err := fs.checkStoppedMDSOwner(stopped, i); err != nil {
		return err
	}
	if fs.config.ActiveMDS != 1 || fs.config.StandbyMDS != 0 || fs.config.StandbyReplay || i.removalAttempted && (!i.removed || r == nil || !r.completed) {
		return servicehook.Invalid(errors.New("MDS replacement requires one active, zero standby and original stopped ownership"))
	}
	if r == nil {
		if i.removed || fs.roster.count() != 1 || fs.roster.at(0) != i.descriptor {
			return servicehook.Invalid(errors.New("MDS replacement requires the sole original member"))
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
	if fs.roster.count() != wanted {
		return errors.New("MDS replacement owned cohort changed")
	}
	return nil
}

// A failed receipt validates retained handles, never startup confirmation.
func (fs *Filesystem) checkMDSReplacementPartial(old *cephFSMDSIdentity, r *cephFSMDSReplacement) error {
	if r.daemon == nil {
		if fs.roster.count() != 1 || fs.roster.at(0) != old.descriptor {
			return errors.New("cleanup-only MDS replacement cohort changed")
		}
		return nil
	}
	i := r.identity
	if i == nil || r.daemon.identity != i || i.createdByReplacement != r || i.descriptor != r.daemon || i.filesystem != fs || i.native != old.native || i.name != r.name || r.daemon.ID != r.name || r.daemon.FilesystemName != fs.config.Name || r.daemon.Container != i.container || i.container == nil || i.container.GetContainerID() != i.cid || servicehook.Service(fs.cluster, "mds."+r.name) != i.container || fs.roster.count() != 2 || fs.roster.at(0) != old.descriptor || fs.roster.at(1) != r.daemon {
		return errors.New("cleanup-only original MDS replacement handle changed")
	}
	return nil
}

func (fs *Filesystem) checkMDSReplacementAuth(ctx context.Context, control testcontainers.Container, name string) error {
	data, err := servicehook.Command(ctx, control, "ceph", "--connect-timeout", "5", "auth", "ls", "--format", "json")
	if err != nil {
		return clusterutil.OperationError(ctx, "check replacement MDS identity names", err)
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
			return servicehook.Invalid(errors.New("reserved replacement MDS identity already exists"))
		}
	}
	return ctx.Err()
}

func (fs *Filesystem) observeMDSReplacement(ctx context.Context, stopped *MDS, r *cephFSMDSReplacement) (bool, error) {
	c := fs.cluster
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return false, err
	}
	defer servicehook.Unlock(c)
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
		return false, clusterutil.OperationError(ctx, "reattest original replacement target CID", err)
	}
	info, err := r.identity.container.Inspect(ctx)
	if err != nil {
		return false, clusterutil.OperationError(ctx, "inspect original new MDS replacement", err)
	}
	if r.identity.container.GetContainerID() != r.identity.cid || info == nil || info.ID != r.identity.cid || info.State == nil || !info.State.Running || info.State.Restarting || info.State.Paused || info.State.Dead || info.State.Pid <= 0 || info.State.Error != "" || info.State.Status != "running" {
		return false, errors.New("original new MDS replacement is not running normally")
	}
	scope, ready, err := fs.readMDSReplacementScope(ctx, i, r)
	if err != nil || !reflect.DeepEqual(scope, r.scope) {
		return false, clusterutil.OperationError(ctx, "original MDS replacement scope changed", err)
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

// readMDSReplacementScope reads the native pools and the global map (two
// commands) and returns the decoded scope. The decoding itself is
// decodeMDSReplacementMap, a function of its arguments. The cluster mutex is
// held by its callers, as before the split.
func (fs *Filesystem) readMDSReplacementScope(ctx context.Context, old *cephFSMDSIdentity, r *cephFSMDSReplacement) (*cephFSMDSRemovalScope, bool, error) {
	pools, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, false, clusterutil.OperationError(ctx, "read original MDS replacement pools", err)
	}
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, false, clusterutil.OperationError(ctx, "read original MDS replacement map", err)
	}
	state, err := decodeMDSReplacementMap(data, replacementMDSMapExpectation{
		oldName: old.name, filesystem: fs.config.Name, reservedName: r.name, workerExists: r.daemon != nil, native: old.native, pools: pools,
	})
	if err != nil {
		return nil, false, err
	}
	return state.scope, state.ready, ctx.Err()
}
