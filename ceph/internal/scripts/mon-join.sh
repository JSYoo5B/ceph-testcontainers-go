#!/bin/sh
set -eu
id=${CEPH_MON_ID}
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
