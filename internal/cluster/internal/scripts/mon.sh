#!/bin/sh
set -eu

mkdir -p /etc/ceph /var/lib/ceph/mon/ceph-a /var/run/ceph
mon_ip=${CEPH_PUBLIC_ADDRESS:-$(hostname -i | awk '{print $1}')}
mon_v2=${CEPH_MON_PORT_V2:-3300}
mon_v1=${CEPH_MON_PORT_V1:-6789}
mon_addrs="[v2:${mon_ip}:${mon_v2},v1:${mon_ip}:${mon_v1}]"
messenger_config=
if [ "${CEPH_MSGR2_SECURE_ONLY:-false}" = true ]; then
    mon_addrs="[v2:${mon_ip}:${mon_v2}]"
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
log to file = false
log to stderr = true
err to stderr = true
mon cluster log to file = false
mon cluster log to stderr = true
osd pool default size = ${CEPH_POOL_SIZE:-2}
osd pool default min size = ${CEPH_POOL_MIN_SIZE:-1}
osd pool default pg num = 8
osd pool default pgp num = 0
osd pool default pg autoscale mode = off
mon allow pool size one = true
ms bind ipv6 = false
# Report daemon and PG statistics every second instead of five, so readiness
# waits observe new PG states sooner in this small disposable cluster.
mgr stats period = 1
${messenger_config}[osd]
osd objectstore = bluestore
bluestore block create = true
bluestore block size = ${CEPH_OSD_BLOCK_SIZE}
bluestore block preallocate file = false
bluestore cache autotune = false
bluestore cache size = 67108864
osd memory target = 536870912
osd crush chooseleaf type = 0
osd max object name len = 256
osd max object namespace len = 64
EOF
    ceph-authtool --create-keyring /etc/ceph/mon.keyring --gen-key -n mon. --cap mon 'allow *'
    ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin \
        --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *' --cap mds 'allow *'
    ceph-authtool /etc/ceph/mon.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring
    monmaptool --create --fsid "$CEPH_FSID" --addv a "$mon_addrs" /tmp/monmap
    ceph-mon --mkfs -i a --monmap /tmp/monmap --keyring /etc/ceph/mon.keyring
fi
# Use the complete address vector from monmap. An IP-only public_bind_addr
# expands to the default MON ports and overrides the allocated host ports.
exec ceph-mon -f -i a
