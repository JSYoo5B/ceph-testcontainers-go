#!/bin/sh
set -eu
mkdir -p /var/run/ceph
# Test-only admin credentials are already copied by WithClient. A small thread
# pool keeps the opt-in gateway practical on a Docker Desktop test VM.
frontend="beast port=${CEPH_RGW_PORT:-7480}"
if [ -n "${CEPH_RGW_ENDPOINT:-}" ]; then
    frontend="beast endpoint=${CEPH_RGW_ENDPOINT}"
fi
if [ -n "${CEPH_RGW_TLS_PORT:-}" ]; then
    if [ -n "${CEPH_RGW_TLS_ENDPOINT:-}" ]; then
        frontend="$frontend ssl_endpoint=${CEPH_RGW_TLS_ENDPOINT}"
    else
        frontend="$frontend ssl_port=${CEPH_RGW_TLS_PORT}"
    fi
    frontend="$frontend ssl_certificate=/tc/rgw-tls.pem"
fi
# A disposable gateway need not drain for the default 120 seconds on SIGTERM;
# Docker would otherwise kill it after its own stop timeout.
set -- radosgw -f -n client.admin --keyring /etc/ceph/ceph.client.admin.keyring \
    --rgw-frontends "$frontend" --rgw-thread-pool-size 4 --rgw-exit-timeout-secs 1
if [ -n "${CEPH_RGW_REALM:-}" ]; then
    set -- "$@" --rgw-realm "$CEPH_RGW_REALM" --rgw-sync-obj-etag-verify true \
        --osd-pool-default-pg-num 1 --osd-pool-default-pgp-num 0
fi
if [ -n "${CEPH_RGW_ZONEGROUP:-}" ]; then
    set -- "$@" --rgw-zonegroup "$CEPH_RGW_ZONEGROUP"
fi
if [ -n "${CEPH_RGW_ZONE:-}" ]; then
    set -- "$@" --rgw-zone "$CEPH_RGW_ZONE"
fi
exec "$@"
