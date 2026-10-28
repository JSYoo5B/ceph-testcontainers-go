#!/bin/sh
set -eu
id=${CEPH_MON_ID}
# BEGIN mon-store-mode (identical in mon.sh and mon-join.sh)
# require_store implements CEPH_MON_STORE_MODE=require for the data directory
# $1: an existing store must be reused, and a missing or non-store directory is
# a hard failure instead of the mkfs below. It runs before any write. Unset
# keeps the historical behavior; any other value, also an empty one, is
# refused so that a misspelled mode cannot fall back to formatting a store.
# The shape checks mirror what ceph-mon itself needs: store.db is the RocksDB
# directory, kv_backend (absent in stores of old monitors, which are read as
# RocksDB) names rocksdb, and keyring (recreated by ceph-mon from the store
# when absent) is a plain file. They detect foreign content, not a damaged
# store; ceph-mon still opens and validates the store.
require_store() {
    [ "${CEPH_MON_STORE_MODE+set}" = set ] || return 0
    if [ "$CEPH_MON_STORE_MODE" != require ]; then
        echo "tc-ceph: environment: unsupported CEPH_MON_STORE_MODE; only require is defined" >&2
        exit 2
    fi
    store_dir=$1
    store_refuse() {
        echo "tc-ceph: store: $store_dir $1; require mode never formats a new store" >&2
        exit 3
    }
    if [ -L "$store_dir" ] || [ ! -d "$store_dir" ]; then
        store_refuse "is not a directory"
    fi
    if [ -L "$store_dir/store.db" ] || [ ! -d "$store_dir/store.db" ]; then
        store_refuse "has no store.db directory"
    fi
    if [ -z "$(ls -A "$store_dir/store.db" 2>/dev/null)" ]; then
        store_refuse "has an empty store.db directory"
    fi
    if [ -e "$store_dir/kv_backend" ] || [ -L "$store_dir/kv_backend" ]; then
        if [ -L "$store_dir/kv_backend" ] || [ ! -f "$store_dir/kv_backend" ] || [ "$(cat "$store_dir/kv_backend")" != rocksdb ]; then
            store_refuse "has a kv_backend that is not a plain rocksdb marker"
        fi
    fi
    if [ -e "$store_dir/keyring" ] || [ -L "$store_dir/keyring" ]; then
        if [ -L "$store_dir/keyring" ] || [ ! -f "$store_dir/keyring" ]; then
            store_refuse "has a keyring that is not a plain file"
        fi
    fi
}
# END mon-store-mode
require_store "/var/lib/ceph/mon/ceph-$id"
# messenger_address prints this container's address of the address family,
# $1 or CEPH_ADDRESS_FAMILY: IPv4, global IPv6, or IPv4 as an IPv4-mapped
# IPv6 address in the hexadecimal form that Ceph parses.
messenger_address() {
    family=${1:-${CEPH_ADDRESS_FAMILY:-ipv4}}
    for address in $(hostname -i); do
        case "$family:$address" in
        ipv6:fe80:* | ipv6:FE80:*) ;;
        ipv6:*:*)
            echo "$address"
            return
            ;;
        ipv4-mapped:*:* | ipv4:*:*) ;;
        ipv4-mapped:*)
            old_ifs=$IFS
            IFS=.
            # shellcheck disable=SC2086
            set -- $address
            IFS=$old_ifs
            printf '::ffff:%x:%x\n' $(($1 * 256 + $2)) $(($3 * 256 + $4))
            return
            ;;
        ipv4:*)
            echo "$address"
            return
            ;;
        esac
    done
    echo "tc-ceph: environment: this container has no $family address" >&2
    exit 2
}
v2=${CEPH_MON_PORT_V2:-3300}
v1=${CEPH_MON_PORT_V1:-6789}
# entries prints the address vector entries of host $1: v2, and v1 unless the
# Messenger mode has no v1 listener.
entries() {
    if [ "${CEPH_MSGR2_ONLY:-false}" = true ] || [ "${CEPH_MSGR2_SECURE_ONLY:-false}" = true ]; then
        echo "v2:$1:$v2"
    else
        echo "v2:$1:$v2,v1:$1:$v1"
    fi
}
# Assign each address on its own so that a missing address stops the script.
case "${CEPH_ADDRESS_FAMILY:-ipv4}" in
dual-stack)
    # IPv6 first, as mon.sh renders the first MON; see there.
    ip=$(messenger_address ipv4)
    ip6=$(messenger_address ipv6)
    addrs="[$(entries "[$ip6]"),$(entries "$ip")]"
    ;;
ipv6 | ipv4-mapped)
    ip=${CEPH_PUBLIC_ADDRESS:-$(messenger_address)}
    addrs="[$(entries "[$ip]")]"
    ;;
*)
    ip=${CEPH_PUBLIC_ADDRESS:-$(messenger_address)}
    addrs="[$(entries "$ip")]"
    ;;
esac
mkdir -p "/var/lib/ceph/mon/ceph-$id" /var/run/ceph
if [ ! -d "/var/lib/ceph/mon/ceph-$id/store.db" ]; then
    # The map and shared mon. key come from the live quorum. Give this member
    # its complete address vector before mkfs so host-mode ports stay exact.
    monmaptool --addv "$id" "$addrs" /tc/monmap
    ceph-mon --mkfs -i "$id" --monmap /tc/monmap --keyring /etc/ceph/mon.keyring
fi
exec ceph-mon -f -i "$id"
