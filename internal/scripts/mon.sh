#!/bin/sh
set -eu

mkdir -p /etc/ceph /var/lib/ceph/mon/ceph-a /var/run/ceph
if [ ! -d /var/lib/ceph/mon/ceph-a/store.db ]; then
    mon_ip=$(hostname -i | awk '{print $1}')
    cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = ${CEPH_FSID}
mon host = [v2:${mon_ip}:3300,v1:${mon_ip}:6789]
mon initial members = a
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
auth allow insecure global id reclaim = false
log to file = false
log to stderr = true
err to stderr = true
mon cluster log to file = false
mon cluster log to stderr = true
osd pool default size = 2
osd pool default min size = 1
osd pool default pg num = 8
osd pool default pgp num = 8
mon allow pool size one = true
ms bind ipv6 = false
[osd]
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
    monmaptool --create --fsid "$CEPH_FSID" --addv a "[v2:${mon_ip}:3300,v1:${mon_ip}:6789]" /tmp/monmap
    ceph-mon --mkfs -i a --monmap /tmp/monmap --keyring /etc/ceph/mon.keyring
fi
exec ceph-mon -f -i a --public-bind-addr "$(hostname -i | awk '{print $1}')"
