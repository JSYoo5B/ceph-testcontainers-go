#!/bin/sh
set -eu
mkdir -p /var/run/ceph
# Test-only admin credentials are already copied by WithClient. A small thread
# pool keeps the opt-in gateway practical on a Docker Desktop test VM.
frontend="beast port=${CEPH_RGW_PORT:-7480}"
if [ -n "${CEPH_RGW_ENDPOINT:-}" ]; then
    frontend="beast endpoint=${CEPH_RGW_ENDPOINT}"
fi
exec radosgw -f -n client.admin --keyring /etc/ceph/ceph.client.admin.keyring \
    --rgw-frontends "$frontend" --rgw-thread-pool-size 4
