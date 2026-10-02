#!/bin/sh
set -eu
mkdir -p /var/run/ceph
# Test-only admin credentials are already copied by WithClient. A small thread
# pool keeps the opt-in gateway practical on a Docker Desktop test VM.
exec radosgw -f -n client.admin --keyring /etc/ceph/ceph.client.admin.keyring \
    --rgw-frontends 'beast port=7480' --rgw-thread-pool-size 4
