#!/bin/sh
set -eu
osd_dir=/var/lib/ceph/osd/ceph-${CEPH_OSD_ID}
mkdir -p "$osd_dir" /var/run/ceph
if [ ! -f "$osd_dir/ready" ]; then
    cp /etc/ceph/osd.keyring "$osd_dir/keyring"
    ceph-osd --mkfs -i "$CEPH_OSD_ID" --osd-uuid "$CEPH_OSD_UUID"
fi
exec ceph-osd -f -i "$CEPH_OSD_ID" --crush-location "root=default host=${CEPH_OSD_HOST}"
