#!/bin/sh
set -eu

mkdir -p /etc/ceph /var/lib/ceph/mon/ceph-a /var/run/ceph
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
    echo "tc-ceph: this container has no ${CEPH_ADDRESS_FAMILY:-ipv4} address" >&2
    exit 2
}
mon_ip=${CEPH_PUBLIC_ADDRESS:-$(messenger_address)}
mon_host=$mon_ip
bind_ipv4=true
bind_ipv6=false
case "${CEPH_ADDRESS_FAMILY:-ipv4}" in
ipv6 | ipv4-mapped)
    mon_host="[$mon_ip]"
    bind_ipv4=false
    bind_ipv6=true
    ;;
esac
mon_v2=${CEPH_MON_PORT_V2:-3300}
mon_v1=${CEPH_MON_PORT_V1:-6789}
mon_addrs="[v2:${mon_host}:${mon_v2},v1:${mon_host}:${mon_v1}]"
messenger_config=
if [ "${CEPH_MSGR2_SECURE_ONLY:-false}" = true ]; then
    mon_addrs="[v2:${mon_host}:${mon_v2}]"
    messenger_config='ms cluster mode = secure
ms service mode = secure
ms client mode = secure
ms mon cluster mode = secure
ms mon service mode = secure
ms mon client mode = secure
ms bind msgr1 = false
ms bind msgr2 = true
'
fi
# Cephx key type, MonMap ciphers and ticket lifetimes from WithAuth. Squid and
# Tentacle before 20.2.4 have no key types other than AES and no MonMap
# ciphers; accept only an AES-only request there, and fail before creating
# anything otherwise.
fail() {
    echo "tc-ceph: $*" >&2
    exit 2
}
key_type_args=
monmap_cipher_args=
auth_config=
mon_auth_config=
if [ -n "${CEPH_KEY_TYPE:-}" ]; then
    if ceph-authtool --help 2>&1 | grep -q -- '--key-type' && monmaptool --help 2>&1 | grep -q -- '--auth-service-cipher'; then
        key_type_args="--key-type ${CEPH_KEY_TYPE}"
        monmap_cipher_args="--auth-service-cipher ${CEPH_AUTH_SERVICE_CIPHER} --auth-allowed-ciphers ${CEPH_AUTH_ALLOWED_CIPHERS} --auth-preferred-cipher ${CEPH_AUTH_PREFERRED_CIPHER}"
        if [ "${CEPH_AUTH_PREFERRED_CIPHER}" = aes ]; then
            # The MONs generate keys of the preferred type for get-or-create.
            mon_auth_config='mon auth allow insecure key = true
'
        fi
    else
        for cipher in ${CEPH_KEY_TYPE} ${CEPH_AUTH_SERVICE_CIPHER} $(echo "${CEPH_AUTH_ALLOWED_CIPHERS}" | tr ',' ' ') ${CEPH_AUTH_PREFERRED_CIPHER}; do
            [ "$cipher" = aes ] || fail "Cephx cipher $cipher needs ceph-authtool --key-type and monmaptool cipher options, as in Ceph 20.2.4 and later; this image supports only aes"
        done
    fi
fi
if [ -n "${CEPH_AUTH_MON_TICKET_TTL:-}" ]; then
    auth_config="${auth_config}auth mon ticket ttl = ${CEPH_AUTH_MON_TICKET_TTL}
"
fi
if [ -n "${CEPH_AUTH_SERVICE_TICKET_TTL:-}" ]; then
    auth_config="${auth_config}auth service ticket ttl = ${CEPH_AUTH_SERVICE_TICKET_TTL}
"
fi
# MemStore saves its contents to the OSD data directory only when the store
# is unmounted, which Ceph's fast shutdown skips.
memstore_config=
if [ "${CEPH_OSD_OBJECTSTORE:-bluestore}" = memstore ]; then
    memstore_config='osd fast shutdown = false
'
fi
public_config=
network_config=
if [ -n "${CEPH_PUBLIC_NETWORK:-}" ]; then
    network_config="public network = ${CEPH_PUBLIC_NETWORK}
cluster network = ${CEPH_CLUSTER_NETWORK}"
fi
if [ -n "${CEPH_PUBLIC_ADDRESS:-}" ]; then
    public_config="public addr = ${mon_ip}
cluster addr = ${mon_ip}"
fi
# WithConfigFile renders one header per section and one normalized key per
# line. Insert those entries into the matching generated section and drop the
# generated value for the same key, because Ceph keeps the first duplicate.
merge_user_config() {
    awk '
    function key_of(line) {
        sub(/=.*/, "", line)
        gsub(/[-_ \t]+/, " ", line)
        sub(/^ /, "", line)
        sub(/ $/, "", line)
        gsub(/ /, "_", line)
        return line
    }
    FNR == NR {
        if ($0 ~ /^\[/) {
            section = $0
            order[++sections] = section
            next
        }
        user[section, key_of($0)] = 1
        entries[section] = entries[section] $0 "\n"
        next
    }
    /^\[/ {
        section = $0
        print
        if (section in entries) {
            printf "%s", entries[section]
            merged[section] = 1
        }
        next
    }
    index($0, "=") && (section, key_of($0)) in user { next }
    { print }
    END {
        for (i = 1; i <= sections; i++) {
            if (!(order[i] in merged)) {
                print order[i]
                printf "%s", entries[order[i]]
            }
        }
    }
    ' "$1" "$2"
}

if [ ! -d /var/lib/ceph/mon/ceph-a/store.db ]; then
    cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = ${CEPH_FSID}
mon host = ${mon_addrs}
mon initial members = a
${public_config}
${network_config}
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
auth allow insecure global id reclaim = false
${auth_config}log to file = false
log to stderr = true
err to stderr = true
mon cluster log to file = false
mon cluster log to stderr = true
# Crash reports stay where CollectCrashMetadata reads them, also after a
# daemon exits; the MON configuration database cannot move them.
crash dir = /var/lib/ceph/crash
osd pool default size = ${CEPH_POOL_SIZE:-2}
osd pool default min size = ${CEPH_POOL_MIN_SIZE:-1}
osd pool default pg num = 8
osd pool default pgp num = 0
osd pool default pg autoscale mode = off
mon allow pool size one = true
ms bind ipv4 = ${bind_ipv4}
ms bind ipv6 = ${bind_ipv6}
# Report daemon and PG statistics every second instead of five, so readiness
# waits observe new PG states sooner in this small disposable cluster.
mgr stats period = 1
${messenger_config}[mon]
${mon_auth_config}[osd]
osd objectstore = ${CEPH_OSD_OBJECTSTORE:-bluestore}
memstore device bytes = ${CEPH_OSD_BLOCK_SIZE}
${memstore_config}bluestore block create = true
bluestore block size = ${CEPH_OSD_BLOCK_SIZE}
bluestore block preallocate file = false
bluestore cache autotune = false
bluestore cache size = 67108864
osd crush chooseleaf type = 0
osd max object name len = 256
osd max object namespace len = 64
EOF
    if [ -s /tc/ceph-user.conf ]; then
        merge_user_config /tc/ceph-user.conf /etc/ceph/ceph.conf > /etc/ceph/ceph.conf.merged
        mv /etc/ceph/ceph.conf.merged /etc/ceph/ceph.conf
    fi
    # shellcheck disable=SC2086
    ceph-authtool --create-keyring /etc/ceph/mon.keyring --gen-key -n mon. $key_type_args --cap mon 'allow *'
    # shellcheck disable=SC2086
    ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin $key_type_args \
        --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *' --cap mds 'allow *'
    ceph-authtool /etc/ceph/mon.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring
    # shellcheck disable=SC2086
    monmaptool --create --fsid "$CEPH_FSID" --addv a "$mon_addrs" $monmap_cipher_args /tmp/monmap
    ceph-mon --mkfs -i a --monmap /tmp/monmap --keyring /etc/ceph/mon.keyring
fi
# Use the complete address vector from monmap. An IP-only public_bind_addr
# expands to the default MON ports and overrides the allocated host ports.
exec ceph-mon -f -i a
