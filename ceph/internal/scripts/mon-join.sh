#!/bin/sh
set -eu
id=${CEPH_MON_ID}
ip=${CEPH_PUBLIC_ADDRESS:-$(hostname -i | awk '{print $1}')}
v2=${CEPH_MON_PORT_V2:-3300}
v1=${CEPH_MON_PORT_V1:-6789}
mkdir -p "/var/lib/ceph/mon/ceph-$id" /var/run/ceph
if [ ! -d "/var/lib/ceph/mon/ceph-$id/store.db" ]; then
    # The map and shared mon. key come from the live quorum. Give this member
    # its complete address vector before mkfs so host-mode ports stay exact.
    monmaptool --addv "$id" "[v2:$ip:$v2,v1:$ip:$v1]" /tc/monmap
    ceph-mon --mkfs -i "$id" --monmap /tc/monmap --keyring /etc/ceph/mon.keyring
fi
exec ceph-mon -f -i "$id"
