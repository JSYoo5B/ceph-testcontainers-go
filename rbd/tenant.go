package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// TenantNamespace is the RBD namespace of a shared-cluster tenant: the
// namespace named after the tenant's ID in an RBD pool of the shared
// cluster, and a client whose capabilities reach only that namespace. The
// tenant's release removes the namespace with its images, snapshots, groups
// and trash, and deletes the client.
type TenantNamespace struct {
	tenant *ceph.Tenant
	pool   string
	client *ceph.ClientConfig
}

// tenantWatchWait bounds how long a release waits for an image to lose the
// watchers of a client that ended without closing it, which the OSDs drop
// after osd_client_watch_timeout, 30 seconds by default.
const tenantWatchWait = 60

// Lease creates the RBD namespace of tenant t in pool, an initialized RBD
// pool of the shared cluster, and a client limited to it: client.ID-rbd-POOL
// with mon 'allow r' and osd and mgr 'profile rbd pool=POOL namespace=ID'.
// The client cannot reach other namespaces, create namespaces or add
// blocklist entries. Create the pool with WithPools in
// ceph.WithSharedRunOptions. A tenant leases each pool once; its release
// removes everything in the namespace.
func Lease(ctx context.Context, t *ceph.Tenant, pool string) (*TenantNamespace, error) {
	if t == nil {
		return nil, invalid(errors.New("tenant is required"))
	}
	if err := validateResourceName("pool", pool); err != nil {
		return nil, err
	}
	c := servicehook.TenantCluster(t)
	if c == nil {
		return nil, invalid(fmt.Errorf("tenant %s is released", t.ID()))
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	data, err := servicehook.ClientCommand(ctx, c, "ceph", "--connect-timeout", "5", "osd", "pool", "application", "get", pool, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("inspect RBD pool %q: %w", pool, err)
	}
	var applications map[string]json.RawMessage
	if err := json.Unmarshal(data, &applications); err != nil {
		return nil, errors.New("decode the applications of an RBD pool")
	}
	if _, ok := applications["rbd"]; !ok {
		return nil, invalid(fmt.Errorf("pool %q is not an RBD pool; create it with rbd.WithPools in ceph.WithSharedRunOptions", pool))
	}
	namespace := t.ID()
	// Attached first, so that the release also removes a namespace whose
	// creation failed after it applied.
	if err := servicehook.AttachTenantScope(t, "rbd namespace "+pool+"/"+namespace, func(ctx context.Context) ([]string, error) {
		return releaseTenantNamespace(ctx, c, pool, namespace)
	}); err != nil {
		return nil, err
	}
	if _, err := servicehook.ClientCommand(ctx, c, "rbd", "namespace", "create", "--pool", pool, "--namespace", namespace); err != nil {
		return nil, fmt.Errorf("create RBD namespace %q: %w", namespace, err)
	}
	scope := "profile rbd pool=" + pool + " namespace=" + namespace
	client, err := servicehook.TenantClient(ctx, t, "rbd-"+pool, ceph.ClientCaps{Mon: "allow r", OSD: scope, MGR: scope})
	if err != nil {
		return nil, fmt.Errorf("create the RBD client of tenant %s: %w", t.ID(), err)
	}
	return &TenantNamespace{tenant: t, pool: pool, client: client}, nil
}

// Pool returns the RBD pool that holds the namespace.
func (n *TenantNamespace) Pool() string { return n.pool }

// Namespace returns the RBD namespace, the tenant's ID.
func (n *TenantNamespace) Namespace() string { return n.tenant.ID() }

// Client returns the namespace's client. Pass --id with its User, and
// --pool and --namespace, to the rbd CLI, or set them on a librbd I/O
// context.
func (n *TenantNamespace) Client() *ceph.ClientConfig { return n.client }

// WithClient attaches a container with the namespace's client, as
// ceph.Tenant.WithClientIdentity does, so the tenant's RunNativeCommand
// accepts it. The caller terminates it before the test ends.
func (n *TenantNamespace) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption {
	return n.tenant.WithClientIdentity(n.client, opts...)
}

// tenantReleaseScript removes an RBD namespace and everything in it: groups,
// trash, user snapshots (unprotecting them) and images, retrying images that
// a clone or a lingering watcher still holds until a deadline, then the
// namespace and any raw object left in it. It never breaks a lock with a
// blocklist entry, which would change the shared cluster's blocklist. It
// reports what remains.
const tenantReleaseScript = `
import json, rados, rbd, sys, time
pool, namespace, wait = sys.argv[1], sys.argv[2], float(sys.argv[3])
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.conf_set("rbd_blocklist_on_break_lock", "false")
cluster.connect(timeout=30)
remaining = []
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        r = rbd.RBD()
        if r.namespace_exists(ioctx, namespace):
            ioctx.set_namespace(namespace)
            deadline = time.monotonic() + wait
            while True:
                busy = []
                for group in r.group_list(ioctx):
                    r.group_remove(ioctx, group)
                for entry in list(r.trash_list(ioctx)):
                    try:
                        r.trash_remove(ioctx, entry["id"], True)
                    except rbd.Error as e:
                        busy.append("trashed image %s: %s" % (entry["name"], type(e).__name__))
                for name in r.list(ioctx):
                    try:
                        with rbd.Image(ioctx, name) as image:
                            for snap in list(image.list_snaps()):
                                if snap.get("namespace", rbd.RBD_SNAP_NAMESPACE_TYPE_USER) != rbd.RBD_SNAP_NAMESPACE_TYPE_USER:
                                    continue
                                if image.is_protected_snap(snap["name"]):
                                    image.unprotect_snap(snap["name"])
                                image.remove_snap(snap["name"])
                        r.remove(ioctx, name)
                    except rbd.Error as e:
                        busy.append("image %s: %s" % (name, type(e).__name__))
                if not busy and not r.list(ioctx) and not list(r.trash_list(ioctx)):
                    break
                if time.monotonic() > deadline:
                    remaining.extend(busy)
                    break
                time.sleep(1)
            ioctx.set_namespace("")
            if not remaining:
                r.namespace_remove(ioctx, namespace)
        if not remaining:
            ioctx.set_namespace(namespace)
            for obj in list(ioctx.list_objects()):
                ioctx.set_locator_key(obj.locator_key or "")
                ioctx.remove_object(obj.key)
            ioctx.set_locator_key("")
        ioctx.set_namespace("")
        exists = r.namespace_exists(ioctx, namespace)
        ioctx.set_namespace(namespace)
        objects = len(list(ioctx.list_objects()))
    finally:
        ioctx.close()
    print(json.dumps({"remaining": remaining, "exists": exists, "objects": objects}))
finally:
    cluster.shutdown()
`

func releaseTenantNamespace(ctx context.Context, c *ceph.Container, pool, namespace string) ([]string, error) {
	data, err := servicehook.ClientCommand(ctx, c, "python3", "-c", tenantReleaseScript, pool, namespace, fmt.Sprint(tenantWatchWait))
	if err != nil {
		return nil, fmt.Errorf("remove RBD namespace %q: %w", namespace, err)
	}
	var result struct {
		Remaining []string `json:"remaining"`
		Exists    *bool    `json:"exists"`
		Objects   *int     `json:"objects"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.Exists == nil || result.Objects == nil {
		return nil, errors.New("decode the removal of an RBD namespace")
	}
	remains := result.Remaining
	if *result.Exists {
		remains = append(remains, "the namespace still exists")
	}
	if *result.Objects > 0 {
		remains = append(remains, fmt.Sprintf("%d RADOS objects remain in the namespace", *result.Objects))
	}
	return remains, nil
}

// tenantProbeScript lists the RBD namespaces of every pool with the rbd
// application. librbd already lists a pool that never had a namespace (no
// rbd_namespace object) as empty, so every error fails the probe: an
// observation that could not be made never passes for an empty one.
const tenantProbeScript = `
import json, rados, rbd
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect(timeout=30)
try:
    found = {}
    for pool in cluster.list_pools():
        ioctx = cluster.open_ioctx(pool)
        try:
            if "rbd" in ioctx.application_list():
                found[pool] = list(rbd.RBD().namespace_list(ioctx))
        finally:
            ioctx.close()
    print(json.dumps(found))
finally:
    cluster.shutdown()
`

// probeTenantNamespaces lists the RBD namespaces of a shared cluster, each
// owned by the tenant of its name.
func probeTenantNamespaces(ctx context.Context, c *ceph.Container) (map[string]string, error) {
	data, err := servicehook.ClientCommand(ctx, c, "python3", "-c", tenantProbeScript)
	if err != nil {
		return nil, fmt.Errorf("list RBD namespaces: %w", err)
	}
	var found map[string][]string
	if err := json.Unmarshal(data, &found); err != nil || found == nil {
		return nil, errors.New("decode the RBD namespaces")
	}
	resources := map[string]string{}
	pools := make([]string, 0, len(found))
	for pool := range found {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	for _, pool := range pools {
		for _, namespace := range found[pool] {
			resources["rbd namespace "+pool+"/"+namespace] = namespace
		}
	}
	return resources, nil
}

func init() {
	servicehook.RegisterSharedService(bootstrapKind, nil, probeTenantNamespaces)
}
