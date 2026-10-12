#!/bin/sh
set -eu
id=${CEPH_MON_ID}
# messenger_address prints this container's address of the address family:
# IPv4, global IPv6, or IPv4 as an IPv4-mapped IPv6 address in the
# hexadecimal form that Ceph parses.
messenger_address() {
    for address in $(hostname -i); do
        case "${CEPH_ADDRESS_FAMILY:-ipv4}:$address" in
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
    echo "tc-ceph: environment: this container has no ${CEPH_ADDRESS_FAMILY:-ipv4} address" >&2
    exit 2
}
ip=${CEPH_PUBLIC_ADDRESS:-$(messenger_address)}
case "${CEPH_ADDRESS_FAMILY:-ipv4}" in
ipv6 | ipv4-mapped) ip="[$ip]" ;;
esac
v2=${CEPH_MON_PORT_V2:-3300}
v1=${CEPH_MON_PORT_V1:-6789}
addrs="[v2:$ip:$v2,v1:$ip:$v1]"
if [ "${CEPH_MSGR2_SECURE_ONLY:-false}" = true ]; then
    addrs="[v2:$ip:$v2]"
fi
mkdir -p "/var/lib/ceph/mon/ceph-$id" /var/run/ceph
if [ ! -d "/var/lib/ceph/mon/ceph-$id/store.db" ]; then
    # The map and shared mon. key come from the live quorum. Give this member
    # its complete address vector before mkfs so host-mode ports stay exact.
    monmaptool --addv "$id" "$addrs" /tc/monmap
    ceph-mon --mkfs -i "$id" --monmap /tc/monmap --keyring /etc/ceph/mon.keyring
fi
exec ceph-mon -f -i "$id"
