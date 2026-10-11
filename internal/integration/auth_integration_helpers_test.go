//go:build all || (integration && auth)

package integration_test

const authNativeProbe = `import rados, sys
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
        if phase == "readonly":
            denied(lambda: io.write_full("denied-write", payload))
        else:
            io.write_full("allowed-write", payload[::-1])
            assert io.read("allowed-write", len(payload)) == payload[::-1]
        io.set_namespace("red")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-namespace", payload))
    with cluster.open_ioctx("tc-auth-other") as io:
        io.set_namespace("blue")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-pool", payload))
    print("fresh scoped native object I/O and access denials passed")
finally:
    cluster.shutdown()
`
