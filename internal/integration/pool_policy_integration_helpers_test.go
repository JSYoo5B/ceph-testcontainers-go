//go:build all || integration

package integration_test

const poolPolicyProbe = `import rados,sys
phase=sys.argv[1]
payload=bytes(range(256))*256
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.conf_set("rados_osd_op_timeout","3")
c.connect(timeout=10)
try:
    with c.open_ioctx("tc-policy") as io:
        if phase=="seed": io.write_full("shared",payload)
        assert io.read("shared",len(payload))==payload
        if phase=="full":
            try: io.write_full("quota-denied",payload)
            except rados.Error: pass
            else: raise AssertionError("full pool accepted write")
        elif phase=="resumed":
            io.write_full("resumed",payload[::-1])
            assert io.read("resumed",len(payload))==payload[::-1]
    with c.open_ioctx("tc-unaffected") as io:
        io.write_full("unaffected",payload)
        assert io.read("unaffected",len(payload))==payload
    print("pool policy native phase passed:",phase)
finally: c.shutdown()
`
