#!/bin/sh
set -eu
mkdir -p /var/lib/ceph/mgr/ceph-a /var/run/ceph
cp /etc/ceph/mgr.keyring /var/lib/ceph/mgr/ceph-a/keyring
exec ceph-mgr -f -i a
