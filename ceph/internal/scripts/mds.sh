#!/bin/sh
set -eu
mkdir -p /var/lib/ceph/mds/ceph-a /var/run/ceph
cp /etc/ceph/mds.keyring /var/lib/ceph/mds/ceph-a/keyring
exec ceph-mds -f -i a --mds-cache-memory-limit 134217728
