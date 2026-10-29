//go:build all || (integration && auth)

package integration_test

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
        denied(lambda: list(io.list_objects()))
        io.set_namespace("")
        denied(lambda: io.write_full("denied-default", payload))
        # LIBRADOS_ALL_NSPACES is the native one-byte all-namespace sentinel.
        io.set_namespace("\x01")
        denied(lambda: list(io.list_objects()))
    with cluster.open_ioctx("tc-auth-other") as io:
        io.set_namespace("blue")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-pool", payload))
    for command in (["config", "set", "global", "mon_max_pg_per_osd", "500"],
                    ["osd", "set", "noout"],
                    ["osd", "blocklist", "add", "192.0.2.1:0/1", "60"]):
        result = subprocess.run(["ceph", "--name", entity, "--keyring", keyring,
                                 "--connect-timeout", "5", *command],
                                capture_output=True, text=True, timeout=10)
        output = result.stdout + result.stderr
        assert result.returncode != 0 and any(denial in output for denial in
            ("Operation not permitted", "EACCES", "EPERM", "access denied", "Permission denied")), output
    print("fresh scoped native object I/O and access denials passed")
finally:
    cluster.shutdown()
`
