#!/bin/sh
set -eu

fail() {
    echo "OSD bootstrap refused: $*" >&2
    exit 1
}

# Validate launch identity before constructing a path or writing a keyring.
case "${CEPH_OSD_ID:-}" in
    ''|*[!0-9]*) fail 'invalid numeric OSD ID' ;;
esac
valid_uuid() (
    [ "${#1}" -eq 36 ] || return 1
    case "$1" in
        ''|*[!0-9a-fA-F-]*|00000000-0000-0000-0000-000000000000) return 1 ;;
    esac
    IFS=-
    set -- $1
    [ "$#" -eq 5 ] && [ "${#1}" -eq 8 ] && [ "${#2}" -eq 4 ] &&
        [ "${#3}" -eq 4 ] && [ "${#4}" -eq 4 ] && [ "${#5}" -eq 12 ]
)
valid_uuid "${CEPH_OSD_UUID:-}" || fail 'invalid OSD UUID'
osd_dir=/var/lib/ceph/osd/ceph-${CEPH_OSD_ID}
[ ! -L "$osd_dir" ] || fail 'OSD data directory is a symbolic link'
if [ -e "$osd_dir" ] && [ ! -d "$osd_dir" ]; then
    fail 'OSD data path is not a directory'
fi
if [ -f "$osd_dir/ready" ] && [ ! -L "$osd_dir/ready" ]; then
    # This admits only an existing instance's matching local store. It does not
    # establish a retained-store lease or validate current MON registration.
    # Read configuration locally: no network lookup or mutation is needed.
    expected_fsid=$(ceph-osd -i "$CEPH_OSD_ID" --show-config-value fsid) || fail 'cannot read configured cluster FSID'
    valid_uuid "$expected_fsid" || fail 'invalid configured cluster FSID'
    for field in whoami fsid ceph_fsid keyring; do
        [ -f "$osd_dir/$field" ] && [ ! -L "$osd_dir/$field" ] && [ -s "$osd_dir/$field" ] ||
            fail "missing or invalid stored $field"
    done
    [ "$(cat "$osd_dir/whoami")" = "$CEPH_OSD_ID" ] || fail 'stored numeric OSD ID differs'
    [ "$(cat "$osd_dir/fsid")" = "$CEPH_OSD_UUID" ] || fail 'stored OSD UUID differs'
    [ "$(cat "$osd_dir/ceph_fsid")" = "$expected_fsid" ] || fail 'stored cluster FSID differs'
else
    # A missing ready marker must never turn an existing/partial store into a
    # fresh initialization. Include hidden entries and dangling symbolic links.
    for entry in "$osd_dir"/* "$osd_dir"/.[!.]* "$osd_dir"/..?*; do
        if [ -e "$entry" ] || [ -L "$entry" ]; then
            fail 'non-empty OSD data directory without a regular ready marker'
        fi
    done
    mkdir -p "$osd_dir"
    cp /etc/ceph/osd.keyring "$osd_dir/keyring"
    ceph-osd --mkfs -i "$CEPH_OSD_ID" --osd-uuid "$CEPH_OSD_UUID"
fi
mkdir -p /var/run/ceph
location="root=${CEPH_OSD_ROOT:-default} host=${CEPH_OSD_HOST}"
if [ -n "${CEPH_OSD_RACK:-}" ]; then
    location="${location} rack=${CEPH_OSD_RACK}"
fi
exec ceph-osd -f -i "$CEPH_OSD_ID" --crush-location "$location"
