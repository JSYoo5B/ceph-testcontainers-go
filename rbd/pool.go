package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Namespace identifies an image namespace created by this cluster. Names and
// native pool identity are immutable. Namespace removal never deletes images,
// snapshots or trash; callers must remove those through their RBD client first.
// The cluster disposes of namespaces with all its data when it terminates.
type Namespace struct {
	owner    *ceph.Container
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
func (ns *Namespace) Name() string { return ns.name }

// PoolName returns the replicated metadata pool containing the namespace.
func (ns *Namespace) PoolName() string { return ns.poolName }

// initPool initializes an existing replicated pool for RBD metadata. Pools
// registered to other applications are rejected; no force option is used. An
// existing RBD initialization is safe to repeat. EC pools may supply image data
// but cannot hold RBD metadata. Use CreatePool before this method, then wait for
// clean PGs before client I/O. Partial initialization never deletes pool data.
func initPool(ctx context.Context, c *ceph.Container, poolName string) error {
	if err := validateResourceName("pool", poolName); err != nil {
		return err
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := controlReady(ctx, c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	if _, err := inspectPool(ctx, c, poolName, false); err != nil {
		return err
	}
	if _, err := servicehook.ClientCommand(ctx, c, "rbd", "pool", "init", "--pool", poolName); err != nil {
		return fmt.Errorf("initialize RBD pool %q: %w", poolName, err)
	}
	// Some Ceph CLI versions return success even when librbd pool_init fails.
	// Verify both the MON application tag and the native initialization object.
	if _, err := inspectPool(ctx, c, poolName, true); err != nil {
		return fmt.Errorf("verify RBD pool initialization: %w", err)
	}
	return nil
}

// createNamespace creates a fresh image namespace in an initialized,
// replicated RBD pool. Existing names are rejected without changing their data.
// A non-nil descriptor returned with an error identifies an attempted creation,
// but cannot authorize removal unless creation was confirmed. Inspect native
// state or dispose of the cluster after an uncertain creation failure.
func createNamespace(ctx context.Context, c *ceph.Container, poolName, name string) (*Namespace, error) {
	if err := validateResourceName("pool", poolName); err != nil {
		return nil, err
	}
	if err := validateResourceName("namespace", name); err != nil {
		return nil, err
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := controlReady(ctx, c); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	poolID, err := inspectPool(ctx, c, poolName, true)
	if err != nil {
		return nil, err
	}
	names, err := nativeNamespaces(ctx, c, poolName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, invalid(fmt.Errorf("RBD namespace %q in pool %q already exists", name, poolName))
	}
	ns := &Namespace{owner: c, poolName: poolName, name: name, poolID: poolID, state: &rbdNamespaceState{}}
	if _, err := servicehook.ClientCommand(ctx, c, "rbd", "namespace", "create", "--pool", poolName, "--namespace", name); err != nil {
		return ns, fmt.Errorf("create RBD namespace %q: %w", name, err)
	}
	ns.state.created = true
	return ns, nil
}

// listNamespaces returns the native namespace names in sorted order,
// including names created outside this fixture. The unnamed default namespace
// is omitted by Ceph. Listing does not grant ownership for removal.
func listNamespaces(ctx context.Context, c *ceph.Container, poolName string) ([]string, error) {
	if err := validateResourceName("pool", poolName); err != nil {
		return nil, err
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := controlReady(ctx, c); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	if _, err := inspectPool(ctx, c, poolName, true); err != nil {
		return nil, err
	}
	return nativeNamespaces(ctx, c, poolName)
}

// removeNamespace removes a confirmed namespace owned by this cluster. Ceph
// refuses nonempty namespaces, including images in trash. No image
// purge or force option is used. Successful removal is idempotent; failed CLI
// removal can be retried with the same descriptor and a new context. Replacing
// its underlying pool is rejected. Do not delete and recreate the same named
// namespace through another client while its descriptor is in use: native RBD
// namespaces have no separate generation identity to distinguish replacements.
func removeNamespace(ctx context.Context, c *ceph.Container, ns *Namespace) error {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := controlReady(ctx, c); err != nil {
		return err
	}
	if ns == nil || ns.owner != c || ns.state == nil || !ns.state.created {
		return errors.New("RBD namespace must be a confirmed creation owned by this cluster")
	}
	if ns.state.removed {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	poolID, err := inspectPool(ctx, c, ns.poolName, true)
	if err != nil {
		return err
	}
	if poolID != ns.poolID {
		return invalid(fmt.Errorf("RBD pool %q was replaced; namespace removal refused", ns.poolName))
	}
	names, err := nativeNamespaces(ctx, c, ns.poolName)
	if err != nil {
		return err
	}
	if !slices.Contains(names, ns.name) {
		ns.state.removed = true
		return nil // Reconcile an applied removal after an uncertain CLI error.
	}
	if _, err := servicehook.ClientCommand(ctx, c, "rbd", "namespace", "remove", "--pool", ns.poolName, "--namespace", ns.name); err != nil {
		return fmt.Errorf("remove RBD namespace %q: %w", ns.name, err)
	}
	ns.state.removed = true
	return nil
}

func validateResourceName(kind, name string) error {
	if len(name) > 128 || !naming.PoolResource.MatchString(name) {
		return invalid(fmt.Errorf("RBD %s name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash", kind))
	}
	return nil
}

func controlReady(ctx context.Context, c *ceph.Container) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Callers already hold the topology lock. Actual toolkit startup happens
	// through ClientCommand under that lock, after read-only pool validation.
	if c.UsesRoleImages() && c.Images().Client == "" {
		return invalid(errors.New("client image is required for this operation"))
	}
	if servicehook.Closed(c) {
		return invalid(errors.New("ceph cluster is terminated"))
	}
	if servicehook.CLI(c) == nil {
		return invalid(errors.New("ceph control container is unavailable"))
	}
	return nil
}

// inspectPool runs while c.mu serializes this fixture's policy mutations.
func inspectPool(ctx context.Context, c *ceph.Container, poolName string, initialized bool) (int64, error) {
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
				return 0, invalid(fmt.Errorf("pool %q is registered to application %q; RBD initialization refused", poolName, app))
			}
		}
		if initialized {
			if _, ok := pool.Applications["rbd"]; !ok {
				return 0, fmt.Errorf("pool %q is not initialized for RBD", poolName)
			}
			if _, err := servicehook.ClientCommand(ctx, c, "rados", "--pool", poolName, "stat", "rbd_trash"); err != nil {
				return 0, fmt.Errorf("verify RBD initialization object in pool %q: %w", poolName, err)
			}
		}
		return *pool.ID, nil
	}
	return 0, fmt.Errorf("RBD metadata pool %q does not exist", poolName)
}

func nativeNamespaces(ctx context.Context, c *ceph.Container, poolName string) ([]string, error) {
	data, err := servicehook.ClientCommand(ctx, c, "rbd", "namespace", "list", "--pool", poolName, "--format", "json")
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

// InitPool initializes an existing replicated pool for RBD metadata. Pools
// registered to other applications are rejected; no force option is used. An
// existing RBD initialization is safe to repeat. EC pools may supply image data
// but cannot hold RBD metadata. Create the pool first, then wait for clean PGs
// before client I/O. Partial initialization never deletes pool data.
func InitPool(ctx context.Context, c *ceph.Container, pool string) error {
	if c == nil {
		return errNilCluster
	}
	return initPool(ctx, c, pool)
}

// CreateNamespace creates a fresh image namespace in an initialized,
// replicated RBD pool. Existing names are rejected without changing their data.
// A non-nil descriptor returned with an error identifies an attempted creation,
// but cannot authorize removal unless creation was confirmed. Inspect native
// state or dispose of the cluster after an uncertain creation failure.
func CreateNamespace(ctx context.Context, c *ceph.Container, pool, name string) (*Namespace, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return createNamespace(ctx, c, pool, name)
}

// ListNamespaces returns the pool's native namespace names in sorted order,
// including names created outside this fixture. The unnamed default namespace
// is omitted by Ceph. Listing does not grant ownership for removal.
func ListNamespaces(ctx context.Context, c *ceph.Container, pool string) ([]string, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return listNamespaces(ctx, c, pool)
}

// RemoveNamespace removes a confirmed namespace owned by this cluster. Ceph
// refuses nonempty namespaces, including images in trash. No image purge or
// force option is used. Successful removal is idempotent; failed CLI removal
// can be retried with the same descriptor and a new context. Replacing its
// underlying pool is rejected. Do not delete and recreate the same named
// namespace through another client while its descriptor is in use: native RBD
// namespaces have no separate generation identity to distinguish replacements.
func RemoveNamespace(ctx context.Context, c *ceph.Container, ns *Namespace) error {
	if c == nil {
		return errNilCluster
	}
	return removeNamespace(ctx, c, ns)
}

var errNilCluster = invalid(errors.New("ceph cluster is unavailable"))
