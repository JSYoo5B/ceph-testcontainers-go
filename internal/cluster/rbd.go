package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// RBDNamespace identifies an image namespace created by this cluster. Names and
// native pool identity are immutable. Namespace removal never deletes images,
// snapshots or trash; callers must remove those through their RBD client first.
// The cluster disposes of namespaces with all its data when it terminates.
type RBDNamespace struct {
	owner    *Container
	poolName string
	name     string
	poolID   int64
	state    *rbdNamespaceState
}

// Copies of the public descriptor share lifecycle state, so a stale value copy
// cannot remove a later same-named namespace after the original was removed.
type rbdNamespaceState struct {
	created bool
	removed bool
}

// Name returns the namespace name used by librbd and the RBD CLI.
func (ns *RBDNamespace) Name() string { return ns.name }

// PoolName returns the replicated metadata pool containing the namespace.
func (ns *RBDNamespace) PoolName() string { return ns.poolName }

// initRBDPool initializes an existing replicated pool for RBD metadata. Pools
// registered to other applications are rejected; no force option is used. An
// existing RBD initialization is safe to repeat. EC pools may supply image data
// but cannot hold RBD metadata. Use CreatePool before this method, then wait for
// clean PGs before client I/O. Partial initialization never deletes pool data.
func (c *Container) initRBDPool(ctx context.Context, poolName string) error {
	if err := validateRBDResourceName("pool", poolName); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.rbdControlReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if _, err := c.inspectRBDPool(ctx, poolName, false); err != nil {
		return err
	}
	if _, err := command(ctx, c.cliContainer(), "rbd", "pool", "init", "--pool", poolName); err != nil {
		return fmt.Errorf("initialize RBD pool %q: %w", poolName, err)
	}
	// Some Ceph CLI versions return success even when librbd pool_init fails.
	// Verify both the MON application tag and the native initialization object.
	if _, err := c.inspectRBDPool(ctx, poolName, true); err != nil {
		return fmt.Errorf("verify RBD pool initialization: %w", err)
	}
	return nil
}

// createRBDNamespace creates a fresh image namespace in an initialized,
// replicated RBD pool. Existing names are rejected without changing their data.
// A non-nil descriptor returned with an error identifies an attempted creation,
// but cannot authorize removal unless creation was confirmed. Inspect native
// state or dispose of the cluster after an uncertain creation failure.
func (c *Container) createRBDNamespace(ctx context.Context, poolName, name string) (*RBDNamespace, error) {
	if err := validateRBDResourceName("pool", poolName); err != nil {
		return nil, err
	}
	if err := validateRBDResourceName("namespace", name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.rbdControlReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	poolID, err := c.inspectRBDPool(ctx, poolName, true)
	if err != nil {
		return nil, err
	}
	names, err := c.listRBDNamespaces(ctx, poolName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, fmt.Errorf("RBD namespace %q in pool %q already exists", name, poolName)
	}
	ns := &RBDNamespace{owner: c, poolName: poolName, name: name, poolID: poolID, state: &rbdNamespaceState{}}
	if _, err := command(ctx, c.cliContainer(), "rbd", "namespace", "create", "--pool", poolName, "--namespace", name); err != nil {
		return ns, fmt.Errorf("create RBD namespace %q: %w", name, err)
	}
	ns.state.created = true
	return ns, nil
}

// namespaceList returns the native namespace names in sorted order,
// including names created outside this fixture. The unnamed default namespace
// is omitted by Ceph. Listing does not grant ownership for removal.
func (c *Container) namespaceList(ctx context.Context, poolName string) ([]string, error) {
	if err := validateRBDResourceName("pool", poolName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.rbdControlReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if _, err := c.inspectRBDPool(ctx, poolName, true); err != nil {
		return nil, err
	}
	return c.listRBDNamespaces(ctx, poolName)
}

// removeRBDNamespace removes a confirmed namespace owned by this cluster. Ceph
// refuses nonempty namespaces, including images in trash. No image
// purge or force option is used. Successful removal is idempotent; failed CLI
// removal can be retried with the same descriptor and a new context. Replacing
// its underlying pool is rejected. Do not delete and recreate the same named
// namespace through another client while its descriptor is in use: native RBD
// namespaces have no separate generation identity to distinguish replacements.
func (c *Container) removeRBDNamespace(ctx context.Context, ns *RBDNamespace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.rbdControlReady(); err != nil {
		return err
	}
	if ns == nil || ns.owner != c || ns.state == nil || !ns.state.created {
		return errors.New("RBD namespace must be a confirmed creation owned by this cluster")
	}
	if ns.state.removed {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	poolID, err := c.inspectRBDPool(ctx, ns.poolName, true)
	if err != nil {
		return err
	}
	if poolID != ns.poolID {
		return fmt.Errorf("RBD pool %q was replaced; namespace removal refused", ns.poolName)
	}
	names, err := c.listRBDNamespaces(ctx, ns.poolName)
	if err != nil {
		return err
	}
	if !slices.Contains(names, ns.name) {
		ns.state.removed = true
		return nil // Reconcile an applied removal after an uncertain CLI error.
	}
	if _, err := command(ctx, c.cliContainer(), "rbd", "namespace", "remove", "--pool", ns.poolName, "--namespace", ns.name); err != nil {
		return fmt.Errorf("remove RBD namespace %q: %w", ns.name, err)
	}
	ns.state.removed = true
	return nil
}

func validateRBDResourceName(kind, name string) error {
	if len(name) > 128 || !poolResourceName.MatchString(name) {
		return fmt.Errorf("RBD %s name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash", kind)
	}
	return nil
}

func (c *Container) rbdControlReady() error {
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if c.cliContainer() == nil {
		return errors.New("ceph control container is unavailable")
	}
	return nil
}

// inspectRBDPool runs while c.mu serializes this fixture's policy mutations.
func (c *Container) inspectRBDPool(ctx context.Context, poolName string, initialized bool) (int64, error) {
	data, err := c.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return 0, fmt.Errorf("inspect RBD metadata pool: %w", err)
	}
	var dump struct {
		Pools []struct {
			ID           *int64                     `json:"pool"`
			Name         string                     `json:"pool_name"`
			Type         int                        `json:"type"`
			Applications map[string]json.RawMessage `json:"application_metadata"`
		} `json:"pools"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Pools == nil {
		return 0, errors.New("decode RBD metadata pool map: invalid OSDMap")
	}
	for _, pool := range dump.Pools {
		if pool.Name != poolName {
			continue
		}
		if pool.ID == nil || *pool.ID < 0 || pool.Type != 1 {
			return 0, fmt.Errorf("RBD metadata pool %q must be replicated with a valid native pool ID", poolName)
		}
		for app := range pool.Applications {
			if app != "rbd" {
				return 0, fmt.Errorf("pool %q is registered to application %q; RBD initialization refused", poolName, app)
			}
		}
		if initialized {
			if _, ok := pool.Applications["rbd"]; !ok {
				return 0, fmt.Errorf("pool %q is not initialized for RBD", poolName)
			}
			if _, err := command(ctx, c.cliContainer(), "rados", "--pool", poolName, "stat", "rbd_trash"); err != nil {
				return 0, fmt.Errorf("verify RBD initialization object in pool %q: %w", poolName, err)
			}
		}
		return *pool.ID, nil
	}
	return 0, fmt.Errorf("RBD metadata pool %q does not exist", poolName)
}

func (c *Container) listRBDNamespaces(ctx context.Context, poolName string) ([]string, error) {
	data, err := command(ctx, c.cliContainer(), "rbd", "namespace", "list", "--pool", poolName, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("list RBD namespaces in pool %q: %w", poolName, err)
	}
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, errors.New("decode RBD namespace listing: invalid namespace array")
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Name == "" || slices.Contains(names, entry.Name) {
			return nil, errors.New("decode RBD namespace listing: empty or duplicate name")
		}
		names = append(names, entry.Name)
	}
	slices.Sort(names)
	return names, nil
}
