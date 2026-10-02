#!/bin/sh
set -eu

test -s /usr/share/doc/ceph/COPYING
test -s /usr/share/ceph-testcontainers/runtime-packages.txt

# Docker exec needs real utility executables, not just shell builtins.
python3 - <<'PY'
import shutil
names = "sh mkdir cp cat rm test sleep hostname awk ceph ceph-authtool monmaptool ceph-mon ceph-mgr ceph-osd ceph-mds rados rbd rbd-mirror cephfs-mirror radosgw radosgw-admin python3".split()
missing = [name for name in names if not shutil.which(name)]
assert not missing, missing
import cephfs, rados, rbd, ceph_argparse, ceph_daemon
print("Runtime executables and Ceph Python imports OK")
PY

hostname -i | awk '{print $1}'
for binary in ceph ceph-mon ceph-mgr ceph-osd ceph-mds rados rbd rbd-mirror cephfs-mirror radosgw radosgw-admin; do
    "$binary" --version
done
ceph-authtool --create-keyring /tmp/tc-smoke.keyring --gen-key -n client.admin
ceph-authtool /tmp/tc-smoke.keyring --print-key > /dev/null
monmaptool --create --fsid 9df1f6ea-6047-4d0c-bb3d-408fbef094b4 \
    --addv a '[v2:127.0.0.1:3300,v1:127.0.0.1:6789]' /tmp/tc-smoke.monmap
monmaptool --print /tmp/tc-smoke.monmap
