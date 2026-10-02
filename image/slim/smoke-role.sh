#!/bin/sh
set -eu

role=${1:?usage: smoke-role.sh control|osd|rgw|mds|all}
case "$role" in
    control|osd|rgw|mds|all) ;;
    *) echo "Unsupported Ceph image role: $role" >&2; exit 2 ;;
esac

test -s /usr/share/doc/ceph/COPYING
test -s /usr/share/ceph-testcontainers/runtime-packages.txt
test -s /usr/share/ceph-testcontainers/image-manifest.json

python3 - "$role" <<'PY'
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


role = sys.argv[1]
manifest_dir = Path('/usr/share/ceph-testcontainers')
manifest = json.loads((manifest_dir / 'image-manifest.json').read_text())
require(manifest.get('role') == role, 'Image manifest role does not match requested role ' + role)
source = manifest.get('source_image')
require(isinstance(source, str) and source not in ('', 'unspecified'), 'Image manifest lacks its source image')
package_lines = (manifest_dir / 'runtime-packages.txt').read_text().splitlines()
require(package_lines[0] == '# source-image: ' + source, 'Package manifest source differs from image manifest')
require(package_lines[1:] == manifest.get('package_versions'), 'Package version manifest differs from image manifest')
require(bool(package_lines[1:]), 'Package manifest is empty')
require(bool(manifest.get('root_packages')), 'Image manifest has no root packages')
require(bool(manifest.get('groups')), 'Image manifest has no runtime groups')

probes = manifest.get('metadata_probes')
required_probes = {
    '/etc/ceph', '/var/lib/ceph', '/run/ceph', '/var/log/ceph', '/tmp',
    '/bin', '/lib64', '/usr/share/doc/ceph/COPYING',
}
require(isinstance(probes, dict) and required_probes.issubset(probes),
        'Image manifest lacks required ownership/mode probes')
for path, expected in sorted(probes.items()):
    info = os.lstat(path)
    actual = {'uid': info.st_uid, 'gid': info.st_gid, 'mode': '%04o' % stat.S_IMODE(info.st_mode)}
    if stat.S_ISLNK(info.st_mode):
        actual.update({'type': 'symlink', 'link_target': os.readlink(path)})
    elif stat.S_ISDIR(info.st_mode):
        actual['type'] = 'directory'
    elif stat.S_ISREG(info.st_mode):
        actual['type'] = 'regular'
    else:
        raise RuntimeError('Unsupported metadata probe type: ' + path)
    require(actual == expected, 'Runtime metadata differs from source for ' + path +
            ': expected ' + repr(expected) + ', actual ' + repr(actual))
print('Linux ownership, modes and symlink targets preserved for ' + str(len(probes)) + ' probes')

aliases = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64', 'amd64': 'amd64'}
actual_arch = aliases.get(platform.machine(), platform.machine())
manifest_arch = manifest.get('architecture')
require(aliases.get(manifest_arch, manifest_arch) == actual_arch, 'Image manifest architecture differs from runtime')
if 'oci_architecture' in manifest:
    require(manifest['oci_architecture'] == actual_arch, 'OCI architecture differs from runtime')

daemons = {
    'control': ('ceph-mon', 'ceph-mgr', 'rbd-mirror', 'cephfs-mirror'),
    'osd': ('ceph-osd',),
    'rgw': ('radosgw',),
    'mds': ('ceph-mds',),
    'all': ('ceph-mon', 'ceph-mgr', 'ceph-osd', 'radosgw', 'ceph-mds', 'rbd-mirror', 'cephfs-mirror'),
}
utilities = 'sh mkdir cp cat rm test sleep hostname awk python3'.split()
# The first role split deliberately retains the complete shared client closure.
clients = ('ceph', 'rados', 'rbd', 'radosgw-admin')
control_tools = ('ceph-authtool', 'monmaptool') if role in ('control', 'all') else ()
executables = utilities + list(clients + daemons[role] + control_tools)
missing = [name for name in executables if shutil.which(name) is None]
require(not missing, 'Missing runtime executables: ' + ', '.join(missing))
for daemon in set(daemons['all']) - set(daemons[role]):
    require(shutil.which(daemon) is None and not os.path.lexists('/usr/bin/' + daemon),
            'Unexpected daemon in ' + role + ' image: ' + daemon)

import cephfs, rados, rbd, ceph_argparse, ceph_daemon

version_pattern = re.compile(r'^ceph version (\S+) \(([^)]+)\)', re.MULTILINE)
recorded_version = manifest.get('ceph_version', '')
recorded = version_pattern.match(recorded_version)
require(recorded is not None, 'Image manifest has an unsupported Ceph version format')
for binary in clients + daemons[role] + control_tools:
    result = subprocess.run([binary, '--version'], check=True, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    actual = version_pattern.search(result.stdout)
    require(actual is not None, binary + ' did not report a Ceph version: ' + result.stdout)
    require(actual.groups() == recorded.groups(), binary + ' Ceph release/commit differs from image manifest')
    if binary == 'ceph':
        require(result.stdout.strip() == recorded_version.strip(), 'Ceph CLI version differs from recorded source')
    print(binary + ': ' + result.stdout.strip())

if role in ('osd', 'all'):
    for suffix in ('rados-classes', 'ceph/compressor', 'ceph/erasure-code'):
        files = [path for base in ('/usr/lib64', '/usr/lib')
                 for path in (Path(base) / suffix).glob('*.so*') if path.is_file()]
        require(bool(files), 'Missing OSD runtime plugins/object classes: ' + suffix)
        print(suffix + ': ' + str(len(files)) + ' shared objects available')
    crypto = [path for base in ('/usr/lib64', '/usr/lib')
              for path in (Path(base) / 'ceph/crypto').glob('*.so*') if path.is_file()]
    print('ceph/crypto: ' + str(len(crypto)) + ' shared objects available (build dependent)')

print('Role ' + role + ': manifests, architecture, executable boundaries and Python imports OK')
print('Source image: ' + source)
PY

hostname -i | awk '{print $1}'
case "$role" in
    control|all)
        ceph-authtool --create-keyring /tmp/tc-smoke.keyring --gen-key -n client.admin
        ceph-authtool /tmp/tc-smoke.keyring --print-key > /dev/null
        monmaptool --create --fsid 9df1f6ea-6047-4d0c-bb3d-408fbef094b4 \
            --addv a '[v2:127.0.0.1:3300,v1:127.0.0.1:6789]' /tmp/tc-smoke.monmap
        monmaptool --print /tmp/tc-smoke.monmap
        ;;
esac
