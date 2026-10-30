//go:build all || integration

package rbd_test

const rbdNamespaceAdminProbe = `import rados, rbd, sys
pool, phase = sys.argv[1:]
blue = bytes(range(256)) * 256
red = b"independent-red-namespace\n" * 4096
patch = b"namespace-scoped-client-write"
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "10", "rados_osd_op_timeout": "10"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        api = rbd.RBD()
        for namespace, expected in [("blue", blue), ("red", red)]:
            io.set_namespace(namespace)
            if phase == "seed":
                api.create(io, "shared", 8 << 20, old_format=False, features=rbd.RBD_FEATURE_LAYERING)
            with rbd.Image(io, "shared") as image:
                if phase == "seed":
                    assert image.write(expected, 0) == len(expected)
                    image.flush()
                elif namespace == "blue":
                    expected = patch + expected[len(patch):]
                assert image.read(0, len(expected)) == expected, "namespace bytes were mixed"
            if phase == "verify-and-remove":
                api.remove(io, "shared")
print("independent namespace image bytes verified in fresh native session")
`

const rbdNamespaceScopedProbe = `import rados, rbd, subprocess, sys
pool, entity, keyring = sys.argv[1:]
patch = b"namespace-scoped-client-write"
with rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity,
                 conf={"keyring": keyring, "rados_mon_op_timeout": "5", "rados_osd_op_timeout": "5"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        io.set_namespace("blue")
        with rbd.Image(io, "shared") as image:
            assert image.read(4096, 256) == bytes(range(256))
            assert image.write(patch, 0) == len(patch)
            image.flush()
            assert image.read(0, len(patch)) == patch
        io.set_namespace("red")
        try:
            image = rbd.Image(io, "shared")
        except rbd.PermissionError:
            pass
        else:
            image.close()
            raise AssertionError("namespace-scoped client opened foreign image")
        try:
            io.write_full("forbidden", b"denied")
        except rados.PermissionError:
            pass
        else:
            raise AssertionError("namespace-scoped client wrote foreign namespace")
    for command in (["rbd", "ls", "--pool", pool, "--namespace", "red"],
                    ["rbd", "rm", "--no-progress", pool + "/red/shared"],
                    ["rbd", "create", "--size", "8M", pool + "/denied-default"],
                    ["rbd", "namespace", "create", "--pool", pool, "--namespace", "denied-namespace"],
                    ["ceph", "osd", "blocklist", "add", "192.0.2.1:0/1", "60"]):
        result = subprocess.run([command[0], "--id", entity.removeprefix("client."),
                                 "--keyring", keyring, *command[1:]],
                                capture_output=True, text=True, timeout=10)
        output = result.stdout + result.stderr
        assert result.returncode != 0 and any(denial in output for denial in
            ("Operation not permitted", "EACCES", "EPERM", "access denied", "Permission denied")), output
print("scoped native RBD write/read succeeded and cross-namespace operations were denied")
`

// A separate owned client dies with a native image open and its exclusive lock
// held. The standalone test observes its watch before stopping it and waits
// for that watch to disappear before explicitly removing the owned image.
const rbdNamespaceKilledHolder = `import rados, rbd, sys, time
pool, user = sys.argv[1:]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", rados_id=user)
cluster.connect(timeout=10)
io = cluster.open_ioctx(pool)
io.set_namespace("blue")
api = rbd.RBD()
api.create(io, "held", 8 << 20, old_format=False,
           features=rbd.RBD_FEATURE_LAYERING | rbd.RBD_FEATURE_EXCLUSIVE_LOCK)
image = rbd.Image(io, "held")
assert image.write(b"held" * 1024, 0) == 4096
image.flush()
print("RBD_NAMESPACE_HELD", flush=True)
time.sleep(3600)
`

const rbdNamespaceReadOnlyProbe = `import errno, hashlib, json, rados, rbd, sys
pool, entity, keyring, fsid, pool_id, expected_image_id = sys.argv[1:]
patch = b"namespace-scoped-client-write"
original = bytes(range(256)) * 256
expected = patch + original[len(patch):]
def denied(operation):
    try:
        operation()
    except (rbd.PermissionError, rados.PermissionError) as error:
        assert getattr(error, "errno", None) in (errno.EPERM, errno.EACCES), "unexpected native denial"
        return
    raise AssertionError("read-only principal mutated an image or accessed foreign namespace")
with rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity,
                 conf={"keyring": keyring, "rados_mon_op_timeout": "5", "rados_osd_op_timeout": "5"}) as cluster:
    assert cluster.get_fsid() == fsid and cluster.pool_lookup(pool) == int(pool_id), "native cluster/pool identity differs"
    with cluster.open_ioctx(pool) as io:
        io.set_namespace("blue")
        api = rbd.RBD()
        # A default writable open registers an image watch, which requires
        # native write permission. The RO profile denies that open itself.
        # Use the RO open to verify reads; its local EROFS write guard is not
        # evidence of server authorization, so do not call image.write here.
        def open_writable():
            with rbd.Image(io, "shared"):
                pass
        denied(open_writable)
        with rbd.Image(io, "shared", read_only=True) as image:
            image_id = image.id()
            assert image_id == expected_image_id, "native image identity differs from owned source"
            assert image.read(0, len(expected)) == expected
            assert image.id() == image_id and image.read(0, len(expected)) == expected
        denied(lambda: api.create(io, "reader-denied-create", 8 << 20, old_format=False,
                                  features=rbd.RBD_FEATURE_LAYERING))
        assert "reader-denied-create" not in api.list(io), "denied create left an image"
        denied(lambda: io.write_full("reader-denied-object", b"denied"))
        io.set_namespace("red")
        def open_foreign():
            with rbd.Image(io, "shared", read_only=True):
                pass
        denied(open_foreign)
        denied(lambda: io.write_full("reader-denied-foreign", b"denied"))
        print(json.dumps({"principal":entity,"fsid":cluster.get_fsid(),"pool_id":cluster.pool_lookup(pool),
                          "namespace":"blue","image_id":image_id,"read_bytes":len(expected),
                          "sha256":hashlib.sha256(expected).hexdigest(),"readonly_open_exact_bytes":True,
                          "writable_open_create_rados_write_foreign_denied":True}))
`
