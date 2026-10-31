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

# An optional launch-time block device. The launcher passes the device number
# and size its caller verified on the host; compare them with what this
# container was actually given before anything is written, so a node that
# Docker resolved to another device is refused. This does not establish the
# host lease. The same check runs again when the container starts again, and
# then admits only the store that mkfs made on this device.
block_args=
if [ -n "${CEPH_OSD_BLOCK_DEVICE:-}" ]; then
    dev=$CEPH_OSD_BLOCK_DEVICE
    [ "$dev" = /dev/tc-osd-block ] || fail 'unexpected block device path'
    [ ! -L "$dev" ] || fail 'block device path is a symbolic link'
    case "${CEPH_OSD_BLOCK_DEVICE_ID:-}" in
        *:*) ;;
        *) fail 'invalid block device number' ;;
    esac
    dev_major=$CEPH_OSD_BLOCK_DEVICE_ID
    dev_minor=${dev_major#*:}
    dev_major=${dev_major%%:*}
    case "$dev_major:$dev_minor" in
        ''|:*|*:|*[!0-9:]*|0*:*|*:*:*) fail 'invalid block device number' ;;
    esac
    case "${CEPH_OSD_BLOCK_DEVICE_BYTES:-}" in
        ''|*[!0-9]*|0*) fail 'invalid block device size' ;;
    esac
    [ "$(stat -L -c %F "$dev")" = 'block special file' ] || fail 'block device is not a block special file'
    set -- $(stat -L -c '%t %T' "$dev")
    [ "$#" -eq 2 ] || fail 'cannot read block device number'
    [ "$(printf '%d:%d' "0x$1" "0x$2")" = "$CEPH_OSD_BLOCK_DEVICE_ID" ] || fail 'block device number differs'
    sectors=$(cat "/sys/dev/block/$CEPH_OSD_BLOCK_DEVICE_ID/size") || fail 'cannot read block device size'
    case "$sectors" in
        ''|*[!0-9]*) fail 'invalid block device size' ;;
    esac
    [ $((sectors * 512)) -eq "$CEPH_OSD_BLOCK_DEVICE_BYTES" ] || fail 'block device size differs'
    # Unknown options are ignored by ceph-osd, so spell these exactly. A size
    # of 0 keeps BlueStore from creating or resizing a file at the path.
    block_args="--bluestore_block_path=$dev --no-bluestore_block_create --bluestore_block_size=0"
fi
osd_dir=/var/lib/ceph/osd/ceph-${CEPH_OSD_ID}
[ ! -L "$osd_dir" ] || fail 'OSD data directory is a symbolic link'
if [ -e "$osd_dir" ] && [ ! -d "$osd_dir" ]; then
    fail 'OSD data path is not a directory'
fi
if [ -f "$osd_dir/ready" ] && [ ! -L "$osd_dir/ready" ]; then
    if [ -n "$block_args" ]; then
        # The container starting again after its own mkfs: BlueStore linked
        # block to the device path it was given, which was verified above.
        [ -L "$osd_dir/block" ] && [ "$(readlink "$osd_dir/block")" = "$dev" ] ||
            fail 'a block device is admitted only for a fresh OSD store or the store made on it'
    fi
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
    # shellcheck disable=SC2086
    ceph-osd --mkfs -i "$CEPH_OSD_ID" --osd-uuid "$CEPH_OSD_UUID" $block_args
fi
mkdir -p /var/run/ceph
location="root=${CEPH_OSD_ROOT:-default} host=${CEPH_OSD_HOST}"
if [ -n "${CEPH_OSD_RACK:-}" ]; then
    location="${location} rack=${CEPH_OSD_RACK}"
fi
exec ceph-osd -f -i "$CEPH_OSD_ID" --crush-location "$location"
