#!/bin/sh
set -eu
id=${CEPH_MGR_ID:-a}
mkdir -p "/var/lib/ceph/mgr/ceph-$id" /var/run/ceph
cp /etc/ceph/mgr.keyring "/var/lib/ceph/mgr/ceph-$id/keyring"
exec ceph-mgr -f -i "$id"
