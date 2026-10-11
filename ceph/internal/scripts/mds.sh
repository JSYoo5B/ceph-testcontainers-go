#!/bin/sh
set -eu
id=${CEPH_MDS_ID:-a}
filesystem=${CEPH_FILESYSTEM:-tc-cephfs}
mkdir -p "/var/lib/ceph/mds/ceph-$id" /var/run/ceph
cp /etc/ceph/mds.keyring "/var/lib/ceph/mds/ceph-$id/keyring"
exec ceph-mds -f -i "$id" --mds-join-fs "$filesystem" --mds-cache-memory-limit 134217728
