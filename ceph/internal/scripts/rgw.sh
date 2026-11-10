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
# Multisite setup restarts gateways after the final period commit. A killed
# process keeps its sync shard leases until they expire, so the native 120
# second lease would stall replication for two minutes after each restart.
# The multisite fixture owns the replacement of that period: a test whose
# gateway stops renewing leases for a while (a source that does not answer)
# needs a period that outlasts it.
case "${CEPH_RGW_SYNC_LEASE_PERIOD:-}" in
    "") lease=30 ;;
    *[!0-9]* | 0*) echo "invalid RGW sync lease period" >&2; exit 1 ;;
    *) lease=$CEPH_RGW_SYNC_LEASE_PERIOD ;;
esac
if [ -n "${CEPH_RGW_REALM:-}" ]; then
    set -- "$@" --rgw-realm "$CEPH_RGW_REALM" --rgw-sync-obj-etag-verify true \
        --rgw-sync-lease-period "$lease" \
        --osd-pool-default-pg-num 1 --osd-pool-default-pgp-num 0
fi
# The multisite fixture owns this startup condition. It changes future idle
# poll scheduling, not sync lease ownership, notifications or storage I/O.
case "${CEPH_RGW_INCREMENTAL_SYNC_TIMING:-}" in
    "") ;;
    1) set -- "$@" --rgw-meta-sync-poll-interval 1 --rgw-data-sync-poll-interval 1 ;;
    *) echo "invalid RGW incremental sync timing profile" >&2; exit 1 ;;
esac
if [ -n "${CEPH_RGW_ZONEGROUP:-}" ]; then
    set -- "$@" --rgw-zonegroup "$CEPH_RGW_ZONEGROUP"
fi
if [ -n "${CEPH_RGW_ZONE:-}" ]; then
    set -- "$@" --rgw-zone "$CEPH_RGW_ZONE"
fi
exec "$@"
