package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/harnesshook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
	"github.com/testcontainers/testcontainers-go"
)

// TenantGroup is the CephFS subvolume group of a shared-cluster tenant: the
// group named after the tenant's ID, whose directory layout puts file data
// in the RADOS namespace fsns-ID of the filesystem's default data pool, and
// a client limited to that directory and that namespace. Subvolumes created
// in the group inherit the namespace. The tenant's release evicts the
// client's sessions, removes the group with its subvolumes, snapshots and
// files, and waits until the namespace holds no data.
type TenantGroup struct {
	tenant   servicehook.TenantPort
	fs       string
	dataPool string
	client   *ceph.ClientConfig
}

// tenantPurgeWait bounds how long a release waits for the MDS and the
// volumes module to purge the data of removed files and subvolumes.
const tenantPurgeWait = 90 * time.Second

// Lease creates the subvolume group of tenant t in filesystem fs of the
// shared cluster, and client.ID-cephfs-FS with mon 'allow r', mds 'allow rw
// fsname=FS path=/volumes/ID' and osd 'allow rw pool=DATA
// namespace=fsns-ID', where DATA is the filesystem's default data pool. The
// client can mount only the group's directory, cannot set layouts, quotas
// or snapshots, and cannot read other namespaces of the pool. An empty fs
// selects the cluster's only filesystem. Create filesystems with
// WithFilesystems in ceph.WithSharedRunOptions. A tenant leases each
// filesystem once.
func Lease(ctx context.Context, t *ceph.Tenant, fs string) (*TenantGroup, error) {
	if t == nil {
		return nil, harnesshook.Invalid(errors.New("tenant is required"))
	}
	if fs != "" && !naming.ClientID.MatchString(fs) {
		return nil, harnesshook.Invalid(errors.New("filesystem name must use letters, digits, dots, underscores or hyphens"))
	}
	port, err := servicehook.Tenant(t)
	if err != nil {
		return nil, harnesshook.Invalid(fmt.Errorf("tenant %s is released", t.ID()))
	}
	if err := port.RequireService(servicekind.CephFS); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, port.StartupTimeout())
	defer cancel()
	found, err := tenantScript(ctx, port.Command, "find", fs, t.ID())
	if err != nil {
		return nil, err
	}
	if found.Error != "" {
		return nil, harnesshook.Invalid(errors.New(found.Error))
	}
	g := &TenantGroup{tenant: port, fs: found.Filesystem, dataPool: found.DataPool}
	// Attached first, so that the release also removes a group whose
	// creation failed after it applied.
	if err := port.AttachScope("cephfs group "+g.fs+"/"+t.ID(), g.release); err != nil {
		return nil, err
	}
	created, err := tenantScript(ctx, port.Command, "create", g.fs, t.ID(), g.dataPool)
	if err != nil {
		return nil, err
	}
	if created.Error != "" {
		return nil, fmt.Errorf("create the subvolume group of tenant %s: %s", t.ID(), created.Error)
	}
	client, err := port.CreateClient(ctx, "cephfs-"+g.fs, ceph.ClientCaps{
		Mon: "allow r",
		MDS: "allow rw fsname=" + g.fs + " path=" + g.Path(),
		OSD: "allow rw pool=" + g.dataPool + " namespace=" + g.PoolNamespace(),
	})
	if err != nil {
		return nil, fmt.Errorf("create the CephFS client of tenant %s: %w", t.ID(), err)
	}
	g.client = client
	return g, nil
}

// FilesystemName returns the filesystem that holds the group.
func (g *TenantGroup) FilesystemName() string { return g.fs }

// Group returns the subvolume group's name, the tenant's ID.
func (g *TenantGroup) Group() string { return g.tenant.ID() }

// Path returns the group's directory, /volumes/ID: the root the client
// mounts.
func (g *TenantGroup) Path() string { return "/volumes/" + g.tenant.ID() }

// DataPool returns the data pool that holds the group's file data.
func (g *TenantGroup) DataPool() string { return g.dataPool }

// PoolNamespace returns the RADOS namespace of the group's file data,
// fsns-ID.
func (g *TenantGroup) PoolNamespace() string { return "fsns-" + g.tenant.ID() }

// Client returns the group's client. Mount Path, or a subvolume's path,
// with it: libcephfs mount_root, ceph-fuse -r or the kernel client's path.
func (g *TenantGroup) Client() *ceph.ClientConfig { return g.client }

// WithClient attaches a container with the group's client, as
// ceph.Tenant.WithClientIdentity does, so the tenant's RunNativeCommand
// accepts it. Unmount before the container ends: the release evicts
// sessions that remain.
func (g *TenantGroup) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption {
	return g.tenant.WithClientIdentity(g.client, opts...)
}

// CreateSubvolume creates subvolume name in the group, with a quota of
// sizeBytes when it is positive, and returns its path. It inherits the
// group's data namespace, so the group's client can use it; the release
// removes it.
func (g *TenantGroup) CreateSubvolume(ctx context.Context, name string, sizeBytes int64) (string, error) {
	if !naming.ClientID.MatchString(name) {
		return "", harnesshook.Invalid(errors.New("subvolume name must use letters, digits, dots, underscores or hyphens"))
	}
	if sizeBytes < 0 {
		return "", harnesshook.Invalid(errors.New("subvolume size must not be negative"))
	}
	if g.tenant.Live() != nil {
		return "", harnesshook.Invalid(fmt.Errorf("tenant %s is released", g.tenant.ID()))
	}
	ctx, cancel := context.WithTimeout(ctx, g.tenant.StartupTimeout())
	defer cancel()
	result, err := tenantScript(ctx, g.tenant.Command, "subvolume", g.fs, g.tenant.ID(), name, strconv.FormatInt(sizeBytes, 10))
	if err != nil {
		return "", err
	}
	if result.Error != "" {
		return "", fmt.Errorf("create subvolume %q: %s", name, result.Error)
	}
	return result.Path, nil
}

func (g *TenantGroup) release(ctx context.Context) ([]string, error) {
	result, err := tenantScript(ctx, g.tenant.Command, "release", g.fs, g.tenant.ID(), g.dataPool,
		"client."+g.tenant.ID()+"-cephfs-"+g.fs, strconv.Itoa(int(tenantPurgeWait/time.Second)))
	if err != nil {
		return nil, err
	}
	if result.Error != "" {
		return result.Remaining, errors.New(result.Error)
	}
	return result.Remaining, nil
}

// commandRunner runs a CLI command in the cluster of a tenant.
type commandRunner = func(ctx context.Context, args ...string) ([]byte, error)

// clusterCommand runs commands in c, for the hooks that run before a cluster
// is shared and so have no tenant.
func clusterCommand(c *ceph.Container) commandRunner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		return servicehook.ClientCommand(ctx, c, args...)
	}
}

type tenantScriptResult struct {
	Error      string   `json:"error"`
	Filesystem string   `json:"filesystem"`
	DataPool   string   `json:"data_pool"`
	Path       string   `json:"path"`
	Remaining  []string `json:"remaining"`
	// Groups maps each filesystem to its subvolume groups.
	Groups map[string][]string `json:"groups"`
	// Ready reports whether the volumes module answered.
	Ready bool `json:"ready"`
}

func tenantScript(ctx context.Context, run commandRunner, args ...string) (tenantScriptResult, error) {
	var result tenantScriptResult
	data, err := run(ctx, append([]string{"python3", "-c", tenantGroupScript}, args...)...)
	if err != nil {
		return result, fmt.Errorf("cephfs tenant %s: %w", args[0], err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("decode the result of cephfs tenant %s", args[0])
	}
	return result, nil
}

// tenantGroupScript does one tenant operation with the admin identity over
// one connection; a libcephfs mount of the admin, unmounted before it
// returns, sets layouts and removes files. Its modes:
//
//	find FS ID: the filesystem (the only one when FS is empty) and its
//	  default data pool;
//	create FS ID DATA: the subvolume group ID with pool_namespace fsns-ID;
//	subvolume FS ID NAME SIZE: a subvolume of the group and its path;
//	release FS ID DATA ENTITY WAIT: evict ENTITY's sessions and remove the
//	  blocklist entries that the evictions added, remove the subvolumes and
//	  their snapshots, the group's other files and the group, wait at most
//	  WAIT seconds for fsns-ID of DATA to empty, and report what remains;
//	probe: the subvolume groups of every filesystem, and whether the
//	  volumes module answered.
const tenantGroupScript = `
import errno, json, os, stat, sys, time
import cephfs, rados

mode, args = sys.argv[1], sys.argv[2:]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect(timeout=30)
fs = None
result = {}

def split(ret, first, second):
    # Bindings differ in the order of the output buffer (bytes) and the
    # status string.
    out, outs = (first, second) if isinstance(first, bytes) else (second, first)
    return ret, out, outs if isinstance(outs, str) else outs.decode("utf-8", "replace")

def mon(**cmd):
    return split(*cluster.mon_command(json.dumps(cmd), b"", timeout=30))

def mgr(**cmd):
    return split(*cluster.mgr_command(json.dumps(cmd), b"", timeout=60))

def filesystems():
    ret, out, outs = mon(prefix="fs ls", format="json")
    if ret != 0:
        raise Exception("fs ls: " + outs)
    return json.loads(out)

def mount(name):
    global fs
    fs = cephfs.LibCephFS(rados_inst=cluster)
    fs.conf_set("client_mount_timeout", "30")
    fs.mount(filesystem_name=name.encode())

def mds(name, **cmd):
    return split(*fs.mds_command(name, json.dumps(cmd), b""))

def remove_tree(path):
    names = []
    handle = fs.opendir(path)
    try:
        while True:
            entry = fs.readdir(handle)
            if entry is None:
                break
            if entry.d_name not in (b".", b".."):
                names.append(entry.d_name.decode())
    finally:
        fs.closedir(handle)
    for name in names:
        child = path + "/" + name
        if stat.S_ISDIR(fs.lstat(child).st_mode):
            remove_tree(child)
            fs.rmdir(child)
        else:
            fs.unlink(child)

try:
    if mode == "find":
        name, tenant = args
        found = [f for f in filesystems() if not name or f["name"] == name]
        if len(found) != 1:
            result["error"] = ("filesystem %s does not exist" % name) if name else "the cluster has %d filesystems; name one" % len(found)
        elif not found[0]["data_pools"]:
            result["error"] = "filesystem %s has no data pool" % found[0]["name"]
        else:
            result["filesystem"], result["data_pool"] = found[0]["name"], found[0]["data_pools"][0]
    elif mode == "create":
        name, tenant, data = args
        ret, out, outs = mgr(prefix="fs subvolumegroup create", vol_name=name, group_name=tenant, pool_layout=data)
        if ret != 0:
            result["error"] = outs
        else:
            ret, out, outs = mgr(prefix="fs subvolumegroup getpath", vol_name=name, group_name=tenant)
            path = out.decode().strip()
            if ret != 0 or path != "/volumes/" + tenant:
                result["error"] = "unexpected group path %r: %s" % (path, outs)
            else:
                mount(name)
                fs.setxattr(path, "ceph.dir.layout.pool_namespace", ("fsns-" + tenant).encode(), 0)
                if fs.getxattr(path, "ceph.dir.layout.pool_namespace").decode() != "fsns-" + tenant:
                    result["error"] = "the group layout did not keep its pool namespace"
                result["path"] = path
    elif mode == "subvolume":
        name, tenant, sub, size = args
        cmd = {"prefix": "fs subvolume create", "vol_name": name, "sub_name": sub, "group_name": tenant}
        if int(size) > 0:
            cmd["size"] = int(size)
        ret, out, outs = mgr(**cmd)
        if ret != 0:
            result["error"] = outs
        else:
            ret, out, outs = mgr(prefix="fs subvolume getpath", vol_name=name, sub_name=sub, group_name=tenant)
            if ret != 0:
                result["error"] = outs
            result["path"] = out.decode().strip()
    elif mode == "release":
        name, tenant, data, entity, wait = args
        remaining = []
        group = "/volumes/" + tenant
        ret, out, outs = mon(prefix="fs get", fs_name=name, format="json")
        if ret != 0:
            raise Exception("fs get: " + outs)
        info = json.loads(out)["mdsmap"]["info"]
        actives = sorted(i["name"] for i in info.values() if i["state"] == "up:active")
        mount(name)
        for daemon in actives:
            ret, out, outs = mds(daemon, prefix="session ls", format="json")
            if ret != 0:
                raise Exception("session ls on mds.%s: %s" % (daemon, outs))
            for session in json.loads(out):
                if "client." + session.get("client_metadata", {}).get("entity_id", "") != entity:
                    continue
                ret, _, outs = mds(daemon, prefix="client evict", filters=["id=%d" % session["id"]])
                if ret != 0:
                    raise Exception("evict session %d: %s" % (session["id"], outs))
                addr = session.get("entity", {}).get("addr")
                if addr:
                    address = "%s/%d" % (addr["addr"], addr["nonce"])
                else:
                    # inst is "client.ID v1:IP:PORT/NONCE".
                    address = session["inst"].split(" ", 1)[1].split(":", 1)[1]
                ret, _, outs = mon(prefix="osd blocklist", blocklistop="rm", addr=address)
                if ret != 0:
                    raise Exception("remove the blocklist entry of an evicted session: " + outs)
        ret, out, outs = mgr(prefix="fs subvolume ls", vol_name=name, group_name=tenant, format="json")
        if ret == -errno.ENOENT:
            subvolumes = []
        elif ret != 0:
            raise Exception("fs subvolume ls: " + outs)
        else:
            subvolumes = [s["name"] for s in json.loads(out)]
        for sub in subvolumes:
            ret, out, outs = mgr(prefix="fs subvolume snapshot ls", vol_name=name, sub_name=sub, group_name=tenant, format="json")
            if ret != 0:
                raise Exception("fs subvolume snapshot ls: " + outs)
            for snap in json.loads(out):
                ret, _, outs = mgr(prefix="fs subvolume snapshot rm", vol_name=name, sub_name=sub, snap_name=snap["name"], group_name=tenant, force=True)
                if ret != 0:
                    remaining.append("snapshot %s of subvolume %s: %s" % (snap["name"], sub, outs))
            ret, _, outs = mgr(prefix="fs subvolume rm", vol_name=name, sub_name=sub, group_name=tenant, force=True)
            if ret != 0:
                remaining.append("subvolume %s: %s" % (sub, outs))
        try:
            fs.stat(group)
            present = True
        except cephfs.ObjectNotFound:
            present = False
        if present and not remaining:
            remove_tree(group)
            ret, _, outs = mgr(prefix="fs subvolumegroup rm", vol_name=name, group_name=tenant, force=True)
            if ret != 0:
                remaining.append("the group: " + outs)
        try:
            fs.stat(group)
            remaining.append("the group directory remains")
        except cephfs.ObjectNotFound:
            pass
        ioctx = cluster.open_ioctx(data)
        try:
            ioctx.set_namespace("fsns-" + tenant)
            deadline = time.monotonic() + float(wait)
            while True:
                count = len(list(ioctx.list_objects()))
                if count == 0 or time.monotonic() > deadline:
                    break
                time.sleep(1)
        finally:
            ioctx.close()
        if count:
            remaining.append("%d RADOS objects remain in namespace fsns-%s of pool %s" % (count, tenant, data))
        result["remaining"] = remaining
    elif mode == "probe":
        groups = {}
        result["ready"] = True
        for f in filesystems():
            ret, out, outs = mgr(prefix="fs subvolumegroup ls", vol_name=f["name"], format="json")
            if ret != 0:
                result["ready"] = False
                result["error"] = "fs subvolumegroup ls %s: %s" % (f["name"], outs)
                break
            groups[f["name"]] = [g["name"] for g in json.loads(out)]
        result["groups"] = groups
    print(json.dumps(result))
finally:
    if fs is not None:
        try:
            fs.unmount()
        finally:
            fs.shutdown()
    cluster.shutdown()
`

// tenantVolumesWait bounds the wait for the volumes module to answer before
// a shared cluster takes its baseline.
const tenantVolumesWait = 2 * time.Minute

// prepareShared waits until the volumes module answers for every
// filesystem, so that the baseline lists their subvolume groups.
func prepareShared(ctx context.Context, c *ceph.Container) error {
	ctx, cancel := context.WithTimeout(ctx, tenantVolumesWait)
	defer cancel()
	for {
		result, err := tenantScript(ctx, clusterCommand(c), "probe")
		if err != nil {
			return err
		}
		if result.Ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the volumes module did not answer: %s: %w", result.Error, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// probeShared lists the subvolume groups of every filesystem, each owned by
// the tenant of its name.
func probeShared(ctx context.Context, c *ceph.Container) (map[string]string, error) {
	result, err := tenantScript(ctx, clusterCommand(c), "probe")
	if err != nil {
		return nil, err
	}
	if !result.Ready {
		return nil, errors.New(result.Error)
	}
	resources := map[string]string{}
	for fs, groups := range result.Groups {
		for _, group := range groups {
			resources["cephfs group "+fs+"/"+group] = group
		}
	}
	return resources, nil
}

func init() {
	servicehook.RegisterSharedService(servicekind.CephFS, prepareShared, probeShared)
}
