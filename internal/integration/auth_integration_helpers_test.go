//go:build all || (integration && auth)

package integration_test

// Seed real foreign/default namespace data and independently observe it before
// checking restricted identities. An empty listing is not an isolation proof.
const authNativeIsolationControl = `import rados, subprocess, sys
phase = sys.argv[1]
expected = [("tc-auth", "red", "red-only"), ("tc-auth", "", "default-only"),
            ("tc-auth-other", "blue", "other-pool-only")]
with rados.Rados(conffile="/etc/ceph/ceph.conf", name="client.admin",
                 conf={"rados_mon_op_timeout": "5", "rados_osd_op_timeout": "5"}) as cluster:
    for pool, namespace, sentinel in expected:
        with cluster.open_ioctx(pool) as io:
            io.set_namespace(namespace)
            payload = (pool + "/" + namespace + "/native foreign sentinel").encode()
            if phase == "seed":
                io.write_full(sentinel, payload)
                io.write_full("shared", payload)
            assert io.read(sentinel, len(payload)) == payload
            assert io.read("shared", len(payload)) == payload
            assert sentinel in [obj.key for obj in io.list_objects()]
    with cluster.open_ioctx("tc-auth") as io:
        io.set_namespace(rados.LIBRADOS_ALL_NSPACES)
        objects = {(obj.nspace, obj.key) for obj in io.list_objects()}
        assert {("red", "red-only"), ("", "default-only")} <= objects
for options, sentinels in ((["-N", "red"], ("red-only",)),
                           (["-N", ""], ("default-only",)),
                           (["--all"], ("red-only", "default-only"))):
    result = subprocess.run(["rados", "-p", "tc-auth", *options, "ls"],
                            capture_output=True, text=True, timeout=10, check=True)
    assert all(sentinel in result.stdout for sentinel in sentinels), result.stdout
print("admin confirmed nonempty foreign/default/all namespace exposure and exact sentinel bytes")
`

const authNativeProbe = `import rados, subprocess, sys
entity, phase, keyring = sys.argv[1:]
payload = bytes(range(256)) * 256
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity)
cluster.conf_set("keyring", keyring)
cluster.conf_set("rados_mon_op_timeout", "5")
cluster.conf_set("rados_osd_op_timeout", "5")
if phase == "bad-auth":
    try:
        cluster.connect(timeout=10)
    except rados.Error:
        print("fresh authentication rejected")
    else:
        raise AssertionError("invalid or revoked credentials authenticated")
    finally:
        cluster.shutdown()
    sys.exit(0)
cluster.connect(timeout=10)
def denied(operation):
    try:
        operation()
    except rados.PermissionError:
        return
    raise AssertionError("unauthorized object operation succeeded")
def denied_cli(command):
    result = subprocess.run([command[0], "--name", entity, "--keyring", keyring,
                             *command[1:]], capture_output=True, text=True, timeout=10)
    output = result.stdout + result.stderr
    assert result.returncode != 0 and any(denial in output for denial in
        ("Operation not permitted", "EACCES", "EPERM", "access denied", "Permission denied")), output
try:
    with cluster.open_ioctx("tc-auth") as io:
        io.set_namespace("blue")
        if phase == "write":
            io.write_full("shared", payload)
        assert io.read("shared", len(payload)) == payload
        assert "shared" in [obj.key for obj in io.list_objects()]
        if phase == "readonly":
            denied(lambda: io.write_full("denied-write", payload))
        else:
            io.write_full("allowed-write", payload[::-1])
            assert io.read("allowed-write", len(payload)) == payload[::-1]
        io.set_namespace("red")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-namespace", payload))
        # The Python ObjectIterator converts negative next() results into
        # StopIteration, including permission errors. Use the CLI for an
        # explicit refusal, with admin-confirmed nonempty exposure controls.
        denied_cli(["rados", "-p", "tc-auth", "-N", "red", "ls"])
        io.set_namespace("")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-default", payload))
        denied_cli(["rados", "-p", "tc-auth", "-N", "", "ls"])
        denied_cli(["rados", "-p", "tc-auth", "--all", "ls"])
    with cluster.open_ioctx("tc-auth-other") as io:
        io.set_namespace("blue")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-pool", payload))
    for command in (["config", "set", "global", "mon_max_pg_per_osd", "500"],
                    ["osd", "set", "noout"],
                    ["osd", "blocklist", "add", "192.0.2.1:0/1", "60"]):
        denied_cli(["ceph", "--connect-timeout", "5", *command])
    print("fresh scoped native object I/O and access denials passed")
finally:
    cluster.shutdown()
`
