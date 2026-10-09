//go:build all || integration

package integration_test

const erasurePoolClientScript = `import rados, rbd, sys
metadata_pool, data_pool, phase = sys.argv[1:]
original = bytes(range(256)) * 512
patch = b"unaligned-erasure-code-overwrite" * 7
patch_offset = 4093
expected = original[:patch_offset] + patch + original[patch_offset + len(patch):]
image_offset = (4 << 20) - 8191
object_name, image_name, snapshot = "tc-ec-object", "tc-ec-image", "baseline"
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "30", "rados_osd_op_timeout": "30"}) as cluster:
    with cluster.open_ioctx(data_pool) as ioctx:
        if phase == "seed":
            ioctx.write_full(object_name, original)
            ioctx.write(object_name, patch, patch_offset)
        assert ioctx.read(object_name, len(expected) + 1) == expected, "EC RADOS byte mismatch"
        if phase == "verify":
            ioctx.remove_object(object_name)
            try:
                ioctx.stat(object_name)
            except rados.ObjectNotFound:
                pass
            else:
                raise AssertionError("deleted EC RADOS object remains")
    with cluster.open_ioctx(metadata_pool) as ioctx:
        api = rbd.RBD()
        if phase == "seed":
            api.create(ioctx, image_name, 8 << 20, old_format=False,
                       features=rbd.RBD_FEATURE_LAYERING, data_pool=data_pool)
        with rbd.Image(ioctx, image_name) as image:
            assert image.data_pool_id() == cluster.pool_lookup(data_pool), "RBD data did not select EC pool"
            if phase == "seed":
                assert image.write(original, image_offset) == len(original)
                image.flush()
                image.create_snap(snapshot)
                assert image.write(patch, image_offset + patch_offset) == len(patch)
                image.flush()
            assert image.read(image_offset, len(expected)) == expected, "EC RBD head byte mismatch"
            image.set_snap(snapshot)
            assert image.read(image_offset, len(original)) == original, "EC RBD snapshot byte mismatch"
            image.set_snap(None)
            if phase == "verify":
                image.remove_snap(snapshot)
        if phase == "verify":
            api.remove(ioctx, image_name)
            assert image_name not in api.list(ioctx), "deleted EC RBD image remains"
`
