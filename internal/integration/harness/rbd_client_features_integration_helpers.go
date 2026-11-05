//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RBDClientFeatures is the fixture state of the native librbd client
// scenarios: a replicated pool with an isolated namespace, a principal
// limited to it, and a client container running as that principal. Client
// operations stay in the consumer's native librbd; each phase is a Python
// script that ends with one JSON proof line.
type RBDClientFeatures struct {
	Cluster   *ceph.Container
	Admin     testcontainers.Container
	Client    testcontainers.Container
	Identity  *ceph.ClientConfig
	Pool      string
	Namespace string
	FSID      string
	PoolID    int64
}

// NewRBDClientFeatures starts the cluster, pool, namespaces, principal and
// client container, and seeds the foreign and default namespace sentinels
// that Finish verifies.
func NewRBDClientFeatures(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) *RBDClientFeatures {
	t.Helper()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}, opts...)
	cluster, admin := NewServiceCluster(t, opts...)
	const pool, namespace = "tc-rbd-client-features", "consumer"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := rbd.CreateNamespace(ctx, cluster, pool, namespace); err != nil {
		t.Fatal(err)
	}
	if _, err := rbd.CreateNamespace(ctx, cluster, pool, "foreign"); err != nil {
		t.Fatal(err)
	}
	if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := ObserveCluster(cluster).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := PoolHandle(t, ctx, cluster, pool).Status(ctx)
	if err != nil || state.ID <= 0 || state.Type != "replicated" || state.Size != 1 || state.MinSize != 1 {
		t.Fatalf("native RBD metadata pool: %+v error=%v", state, err)
	}
	identity, err := cluster.CreateClient(ctx, "rbd-features", ceph.ClientCaps{
		Mon: "profile rbd",
		MGR: "profile rbd pool=" + pool + " namespace=" + namespace,
		OSD: "profile rbd pool=" + pool + " namespace=" + namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	image, _ := IntegrationImages(t)
	if custom := os.Getenv("CEPH_TEST_RBD_CLIENT_IMAGE"); custom != "" {
		image = custom
	}
	client, err := testcontainers.Run(ctx, NativeClientImage(t, image), identity.WithIdentity(),
		ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados, rbd"})))
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native RBD client image=%s; encryption-rekey requires cryptsetup in this image", image)
	// Namespace-wide trash purge is safe here because the namespace is a
	// fresh fixture. Expired foreign/default trash must survive the purge.
	ExecCommand(t, ctx, admin, "python3", "-c", rbdClientFeatureSentinelScript, pool, "seed")
	return &RBDClientFeatures{Cluster: cluster, Admin: admin, Client: client, Identity: identity, Pool: pool, Namespace: namespace, FSID: status.FSID, PoolID: state.ID}
}

// Phase runs one phase of script as a subtest and requires its JSON proof to
// name the phase, the cluster and the pool. after, when given, runs inside
// the subtest once the phase passed. A false result means the phase failed:
// it may leave owned partial images or trash, so the caller stops there and
// lets the cluster cleanup remove them instead of cascading failures.
func (f *RBDClientFeatures) Phase(t *testing.T, ctx context.Context, script, phase string, after ...func(t *testing.T)) bool {
	return t.Run(phase, func(t *testing.T) {
		args := []string{"python3", "-c", `import subprocess, sys
phase = sys.argv[2]
try:
    result = subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=180, check=False, stderr=subprocess.PIPE)
except subprocess.TimeoutExpired:
    print('native phase timeout: phase=' + phase + ' exit=124', file=sys.stderr)
    sys.exit(124)
if result.returncode != 0:
    print('native phase failed: phase=' + phase + ' exit=' + str(result.returncode), file=sys.stderr)
    if result.stderr:
        # Expected lock/key denials also log on native stderr. Only failed
        # phases forward a bounded tail; successful stdout stays exact JSON.
        sys.stderr.write(result.stderr[-8192:].decode('utf-8', errors='replace'))
sys.exit(result.returncode)`,
			script, phase, f.Pool, f.Namespace, f.Identity.Name(), f.Identity.KeyringPath(), f.FSID, fmt.Sprint(f.PoolID)}
		code, reader, err := f.Client.Exec(ctx, args, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		output, err := io.ReadAll(reader)
		if err != nil || code != 0 {
			t.Fatalf("native %s exit=%d error=%v:\n%s", phase, code, err, output)
		}
		var result struct {
			Phase  string `json:"phase"`
			FSID   string `json:"fsid"`
			PoolID int64  `json:"pool_id"`
		}
		if err := json.Unmarshal(output, &result); err != nil || result.Phase != phase || result.FSID != f.FSID || result.PoolID != f.PoolID {
			t.Fatalf("unexpected native proof: %s error=%v", output, err)
		}
		t.Logf("native proof: %s", output)
		for _, step := range after {
			step(t)
		}
	})
}

// Backup archives source with the fixture's own credentials and restores it
// as target, both in the client namespace.
func (f *RBDClientFeatures) Backup(t *testing.T, ctx context.Context, source, target string) {
	t.Helper()
	var archive bytes.Buffer
	if err := rbd.ExportBackup(ctx, f.Admin, f.Pool+"/"+f.Namespace+"/"+source, &archive); err != nil {
		t.Fatal(err)
	}
	if err := rbd.RestoreBackup(ctx, f.Admin, f.Pool+"/"+f.Namespace+"/"+target, &archive); err != nil {
		t.Fatal(err)
	}
}

// Finish verifies that the other namespaces' sentinels and the pool identity
// are unchanged.
func (f *RBDClientFeatures) Finish(t *testing.T, ctx context.Context) {
	t.Helper()
	ExecCommand(t, ctx, f.Admin, "python3", "-c", rbdClientFeatureSentinelScript, f.Pool, "verify-and-remove")
	after, err := PoolHandle(t, ctx, f.Cluster, f.Pool).Status(ctx)
	if err != nil || after.ID != f.PoolID {
		t.Fatalf("native pool identity changed: before=%d after=%+v error=%v", f.PoolID, after, err)
	}
}

const rbdClientFeatureSentinelScript = `import rados, rbd, sys
pool, phase = sys.argv[1:]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "10", "rados_osd_op_timeout": "10"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        api = rbd.RBD()
        for namespace in ["", "foreign"]:
            io.set_namespace(namespace)
            expected = ("untouched-namespace:" + namespace).encode() * 4096
            if phase == "seed":
                api.create(io, "sentinel", 8 << 20, old_format=False, features=rbd.RBD_FEATURE_LAYERING)
                with rbd.Image(io, "sentinel") as image:
                    assert image.write(expected, 0) == len(expected)
                    image.flush()
                    original_id = image.id()
                api.trash_move(io, "sentinel", delay=0)
                assert api.trash_get(io, original_id)["name"] == "sentinel"
            else:
                entries = list(api.trash_list(io))
                assert len(entries) == 1 and entries[0]["name"] == "sentinel", "purge crossed namespaces"
                original_id = entries[0]["id"]
                api.trash_restore(io, original_id, "sentinel")
                with rbd.Image(io, "sentinel") as image:
                    assert image.id() == original_id
                    assert image.read(0, len(expected)) == expected, "foreign sentinel bytes changed"
                api.remove(io, "sentinel")
print("foreign/default expired trash and exact bytes preserved")
`

// RBDClientFeaturePrelude starts every phase script: arguments, the shared
// payloads and the session, image and key-denial helpers. A phase chain that
// follows it begins with "if phase == ..." and ends with
// RBDClientFeatureEpilogue.
const RBDClientFeaturePrelude = `import contextlib, datetime, errno, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, time
import rados, rbd
# The native binding converts UTC-labelled expiry through local time.mktime.
# Pin this consumer's local timezone before any deferred-trash operation.
os.environ["TZ"] = "UTC"
time.tzset()
phase, pool, namespace, entity, keyring, expected_fsid, expected_pool_id = sys.argv[1:]
expected_pool_id = int(expected_pool_id)
api = rbd.RBD()
features = rbd.RBD_FEATURE_LAYERING | rbd.RBD_FEATURE_EXCLUSIVE_LOCK
payload = bytes((i * 37 + 17) % 251 + 1 for i in range(2 << 20))
patch = b"changed-child-and-migration-data" * 4096
proof = {"phase": phase, "fsid": expected_fsid, "pool_id": expected_pool_id}

def require_passphrase_denied(image, fmt, passphrase, label, stage):
    # Ceph v20.2.4 test_mock_LoadRequest.cc::WrongPassphrase requires -EPERM.
    # LUKS Header passes crypt_volume_key_get's errno through unchanged; the
    # Python binding maps it to PermissionError with positive errno.EPERM.
    # Invalid format/header, timeout, I/O and unsupported algorithms are not
    # evidence that a passphrase was denied. Never print key/error text.
    try:
        image.encryption_load(fmt, passphrase)
    except rbd.Error as error:
        error_type, actual = type(error).__name__, getattr(error, "errno", None)
        if not isinstance(error, rbd.PermissionError) or actual != errno.EPERM:
            raise AssertionError("unexpected native key rejection: type=%s errno=%s" % (error_type, actual)) from None
        proof.setdefault("key_denials", []).append({"format": label, "stage": stage, "type": error_type, "errno": actual})
        return
    raise AssertionError("native passphrase unexpectedly loaded: " + stage)

@contextlib.contextmanager
def session():
    with rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity,
                     conf={"keyring": keyring, "rados_mon_op_timeout": "10", "rados_osd_op_timeout": "15", "rbd_cache": "false"}) as conn:
        assert conn.get_fsid() == expected_fsid, "connected to another cluster"
        with conn.open_ioctx(pool) as io:
            assert io.get_pool_id() == expected_pool_id, "native pool was replaced"
            io.set_namespace(namespace)
            yield conn, io

def create(io, name, data=payload, size=8 << 20):
    api.create(io, name, size, old_format=False, features=features, order=20)
    with rbd.Image(io, name) as image:
        if data:
            assert image.write(data, 0) == len(data)
            image.flush()
        return image.id()

def read(io, name, expected, image_id=None, offset=0):
    with rbd.Image(io, name) as image:
        if image_id is not None:
            assert image.id() == image_id, "same-named image was replaced"
        assert image.read(offset, len(expected)) == expected, "native image bytes changed"

def directory_group_id(io, name):
    # cls_rbd keeps each group's id in rbd_group_directory under name_<group>
    # as an encoded string: a little-endian 32-bit length and the text.
    key = "name_" + name
    with rados.ReadOpCtx() as op:
        values, _ = io.get_omap_vals_by_keys(op, (key,))
        io.operate_read_op(op, "rbd_group_directory")
        value = dict(values)[key]
    assert int.from_bytes(value[:4], "little") == len(value) - 4, "malformed group directory entry"
    return value[4:].decode()

def group_id(io, group, name):
    # Squid's binding has no Group.id(). Where it exists, the directory entry
    # must agree, so the fallback stays proven on the default release.
    native = directory_group_id(io, name)
    if hasattr(group, "id"):
        assert group.id() == native, "group directory id differs from Group.id()"
    return native

def group_snap_info(io, group, group_name, snap_name, images):
    # Squid's binding has no get_snap_info: read the state from list_snaps
    # and member snapshots from each image's group snapshot namespace. Where
    # get_snap_info exists, both views must name the same image snapshots.
    state = next(entry["state"] for entry in group.list_snaps() if entry["name"] == snap_name)
    members = []
    for image_name in images:
        with rbd.Image(io, image_name) as image:
            for entry in image.list_snaps():
                owner = entry.get("group") or {}
                if entry["namespace"] == rbd.RBD_SNAP_NAMESPACE_TYPE_GROUP and owner.get("name") == group_name:
                    members.append({"image_name": image_name, "pool_id": io.get_pool_id(), "snap_id": entry["id"]})
    if not hasattr(group, "get_snap_info"):
        return {"state": state, "image_snaps": members, "id": None}
    info = group.get_snap_info(snap_name)
    native = sorted((entry["image_name"], entry["snap_id"]) for entry in info["image_snaps"])
    assert native == sorted((entry["image_name"], entry["snap_id"]) for entry in members), "image group snapshots differ from get_snap_info"
    assert info["state"] == state, "group snapshot state differs from list_snaps"
    return info

def absent(io, name):
    try:
        with rbd.Image(io, name):
            pass
    except rbd.ImageNotFound:
        return
    raise AssertionError("image must be absent: " + name)

def verify_restored_encryption(name, own_pass, other_pass, expected, raw_size):
    # A restored encrypted image is the raw LUKS header and ciphertext with no
    # parent: it opens only with its own secret, and its raw bytes hide the
    # plaintext.
    fmt = rbd.RBD_ENCRYPTION_FORMAT_LUKS2
    with session() as (_, io):
        with rbd.Image(io, name) as raw:
            restored_id = raw.id()
            assert restored_id != "" and raw.size() == raw_size
            assert raw.read(0, 6) == b"LUKS\xba\xbe", "restore lost the LUKS header"
            try:
                raw.parent_id()
            except rbd.ImageNotFound:
                pass
            else:
                raise AssertionError("restored image has a parent")
        with rbd.Image(io, name) as image:
            require_passphrase_denied(image, fmt, other_pass, "luks2", "other-secret-on-restore")
        with rbd.Image(io, name) as image:
            image.encryption_load(fmt, own_pass)
            header_size = raw_size - image.size()
            assert 0 < header_size < raw_size - len(expected)
            assert image.read(0, len(expected)) == expected, "restored plaintext differs"
        with rbd.Image(io, name) as raw:
            assert raw.read(header_size, 4096) != expected[:4096], "restored raw image contains plaintext"
    return restored_id

# A phase that hands images to a fixture step after it names them here.
retained = []

`

// RBDClientFeatureEpilogue rejects an unknown phase, requires the namespace
// to hold only the images the phase named in retained, and prints the proof.
const RBDClientFeatureEpilogue = `
else:
    raise AssertionError("unknown probe phase: " + phase)

with session() as (_, io):
    assert sorted(api.list(io)) == sorted(retained) and list(api.trash_list(io)) == [], "client probe left images/trash behind"
print(json.dumps(proof, sort_keys=True))
`
