#!/usr/bin/env python3
"""Authenticated inputs, actual Ceph CMake/gtests and guarded native RGW staging.

--prepare-only is host-safe and does not extract/configure/build or call Docker/Git.
--build requires an Ubuntu Noble Linux ARM64 native environment. No native result
is claimed until the corresponding command completed and its evidence is saved.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent


def relative_input(root, value):
    """Require a regular recipe-relative input, contained even through symlinks."""
    path = PurePosixPath(value)
    if not path.parts or path.is_absolute() or '..' in path.parts or '\\' in value:
        raise RuntimeError('Unsafe relative input path: ' + str(value))
    result = root / value
    if not result.resolve().is_relative_to(root.resolve()):
        raise RuntimeError('Input path escapes its owner: ' + str(value))
    return result


class Recipe:
    """Explicit, bounded recipe. Additional distro/version profiles need native gates."""
    def __init__(self, path):
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            raise RuntimeError('Recipe must be a regular file')
        self.path = path.resolve()
        self.sha256 = digest(self.path)
        self.data = json.loads(self.path.read_text())
        supported = {'schema': 1, 'profile': 'ceph-20.2.4-noble-arm64',
                     'version': '20.2.4', 'source_version': '20.2.4-1noble',
                     'architecture': 'arm64', 'distribution': 'ubuntu',
                     'distribution_version': '24.04', 'native_build_parallelism': 1,
                     'gtest_target': 'unittest_rgw_bucket_sync_pipe_rules',
                     'minimum_gtest_cases': 9, 'private_version_prefix': '20.2.4+tc.'}
        if any(self.data.get(name) != value for name, value in supported.items()):
            raise RuntimeError('Only the gated Ceph 20.2.4 Noble native ARM64 profile is supported')
        if self.data.get('native_machines') != ['aarch64', 'arm64']:
            raise RuntimeError('Unsupported native machine profile')
        if not isinstance(self.data.get('source_date_epoch'), int) or self.data['source_date_epoch'] < 0:
            raise RuntimeError('Nonnegative source_date_epoch required')
        self.cmake = self.data.get('cmake_definitions')
        if not isinstance(self.cmake, dict) or not self.cmake or any(
                not isinstance(k, str) or not re.fullmatch(r'[A-Z0-9_]+', k) or
                not isinstance(v, str) or '\n' in v or '\0' in v for k, v in self.cmake.items()):
            raise RuntimeError('Invalid explicit CMake definitions')
        for name in ['CMAKE_EXE_LINKER_FLAGS', 'CMAKE_SHARED_LINKER_FLAGS']:
            if self.cmake.get(name) != '-fuse-ld=bfd':
                raise RuntimeError('Supported profile requires GNU bfd: ' + name)
        self.targets = self.data.get('targets')
        if self.targets != ['unittest_rgw_bucket_sync_pipe_rules', 'radosgw', 'radosgw-admin', 'rgw']:
            raise RuntimeError('Exact native regression and RGW targets required')
        for name in ['inputs', 'patch_manifest', 'source_manifest']:
            record = self.data[name]
            path = relative_input(self.path.parent, record['path'])
            check_file(path, record)
            setattr(self, name + '_path', path)
        self.metadata = relative_input(self.path.parent, self.data['metadata_directory'])
        self.inputs = json.loads(self.inputs_path.read_text())
        if self.inputs.get('schema') != 1 or self.inputs.get('version') != self.data['version'] or \
                self.inputs.get('source', {}).get('source_version') != self.data['source_version']:
            raise RuntimeError('Recipe and authenticated input version disagree')
        packages = self.inputs.get('runtime_packages', [])
        if len(packages) != 22 or len({v['Package'] for v in packages}) != 22 or any(
                v['Version'] != self.data['source_version'] or v['Architecture'] not in
                (self.data['architecture'], 'all') for v in packages):
            raise RuntimeError('Exact 22-package source-version/architecture closure required')
        source_manifest = json.loads(self.source_manifest_path.read_text())
        if source_manifest.get('source_commit') != self.inputs.get('source_commit_reference'):
            raise RuntimeError('Source manifest and authenticated source commit disagree')
        for record in source_manifest['files']:
            relative_input(self.path.parent, record['path'])
        for name, expected in self.inputs['metadata'].items():
            check_file(self.metadata / basename(name), expected)

    def verify_unchanged(self):
        check_file(self.path, {'sha256': self.sha256})
        for name in ['inputs', 'patch_manifest', 'source_manifest']:
            check_file(getattr(self, name + '_path'), self.data[name])


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    os.replace(temporary, path)


def check_file(path, expected):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('Missing or nonregular input: ' + str(path))
    if 'size' in expected and path.stat().st_size != int(expected['size']):
        raise RuntimeError('Size mismatch: ' + str(path))
    if digest(path) != expected['sha256']:
        raise RuntimeError('SHA256 mismatch: ' + str(path))


def fields(text):
    result = {}; current = None
    for line in text.splitlines():
        if line.startswith((' ', '\t')):
            if current is None:
                raise RuntimeError('Control continuation without field')
            result[current] += '\n' + line[1:]
        elif ':' in line:
            current, value = line.split(':', 1)
            if current in result:
                raise RuntimeError('Duplicate control field ' + current)
            result[current] = value.strip()
    return result


def paragraphs(text):
    return [fields(value) for value in text.split('\n\n') if value.strip()]


def basename(value):
    if not value or '/' in value or '\\' in value or value in ('.', '..'):
        raise RuntimeError('Unsafe basename: ' + str(value))
    return value


def execute(arguments, log, cwd=None, env=None, timeout=None):
    print('Executing ' + str(arguments[0]) + '; log=' + str(log), flush=True)
    started = time.time()
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('w') as output:
        output.write(json.dumps({'argv': list(map(str, arguments)), 'cwd': str(cwd),
                                 'started': started}) + '\n')
        output.flush()
        try:
            result = subprocess.run(list(map(str, arguments)), cwd=cwd, env=env,
                                    stdout=output, stderr=subprocess.STDOUT,
                                    timeout=timeout, check=False)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError('Command timed out; see ' + str(log)) from error
    with log.open('a') as output:
        output.write(json.dumps({'exit_code': result.returncode,
                                 'elapsed_seconds': time.time() - started}) + '\n')
    if result.returncode:
        raise RuntimeError('Command failed (%s); see %s' % (result.returncode, log))


def capture(arguments):
    return subprocess.run(list(map(str, arguments)), check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def verify_metadata(inputs, output, metadata):
    for name, expected in inputs['metadata'].items():
        check_file(metadata / basename(name), expected)
    for command in ['gpg', 'gpgv']:
        if shutil.which(command) is None:
            raise RuntimeError(command + ' is required for authenticating pinned metadata')
    # No default keyring writes, keyserver, private keys, or gpg-agent.
    with tempfile.TemporaryDirectory(prefix='ceph-key-') as directory:
        keyring = Path(directory) / 'release.gpg'
        execute(['gpg', '--homedir', directory, '--batch', '--yes', '--dearmor',
                 '--output', keyring, metadata / 'release.asc'], output / 'logs/key-dearmor.log')
        execute(['gpgv', '--homedir', directory, '--keyring', keyring, '--status-fd', '1',
                 metadata / 'InRelease'], output / 'logs/release-signature.log')
    status = (output / 'logs/release-signature.log').read_text()
    if '[GNUPG:] VALIDSIG ' + inputs['release_fingerprint'] + ' ' not in status:
        raise RuntimeError('Release signature did not match pinned Ceph release fingerprint')
    release = fields((metadata / 'InRelease').read_text().split('\n\n', 1)[1]
                     .split('\n-----BEGIN PGP SIGNATURE-----', 1)[0])
    hashes = {row.split()[2]: (row.split()[0], int(row.split()[1]))
              for row in release['SHA256'].splitlines() if row.strip()}
    for local, name in [('Sources.gz', 'main/source/Sources.gz'),
                        ('Packages.arm64', 'main/binary-arm64/Packages')]:
        expected = inputs['metadata'][local]
        if hashes.get(name) != (expected['sha256'], expected['size']):
            raise RuntimeError('Signed Release/index hash mismatch: ' + name)
    sources = paragraphs(gzip.decompress((metadata / 'Sources.gz').read_bytes()).decode())
    source = [record for record in sources if record.get('Package') == 'ceph' and
              record.get('Version') == inputs['source']['source_version']]
    if len(source) != 1:
        raise RuntimeError('Missing or ambiguous pinned Ceph source in signed Sources')
    row = [value.split() for value in source[0]['Checksums-Sha256'].splitlines()
           if value.strip() and value.split()[-1] == inputs['source']['filename']]
    if row != [[inputs['source']['sha256'], str(inputs['source']['size']),
                inputs['source']['filename']]]:
        raise RuntimeError('Source archive identity differs from authenticated Sources')
    archive_url = inputs['repo_url'] + source[0]['Directory'] + '/' + inputs['source']['filename']
    if archive_url != inputs['source']['url']:
        raise RuntimeError('Source URL differs from authenticated Sources directory')
    index = paragraphs((metadata / 'Packages.arm64').read_text())
    for package in inputs['runtime_packages']:
        actual = [record for record in index if record.get('Package') == package['Package'] and
                  record.get('Architecture') == package['Architecture'] and
                  record.get('Version') == package['Version']]
        if len(actual) != 1 or any(actual[0].get(name) != value for name, value in package.items()):
            raise RuntimeError('Binary archive identity differs from authenticated Packages: ' + package['Package'])
    return {'release_date': release['Date'], 'fingerprint': inputs['release_fingerprint'],
            'key_bootstrap': inputs['key_bootstrap'], 'archive_identity': inputs['source'],
            'metadata_sha256': inputs['metadata']}


def patch_inputs(manifest):
    specification = json.loads(manifest.read_text())
    if specification.get('schema') != 1 or not specification.get('patches'):
        raise RuntimeError('Nonempty ordered schema=1 patch list required')
    seen = set(); patches = []
    for record in specification['patches']:
        name = basename(record['filename'])
        if name in seen or record.get('strip') != 1:
            raise RuntimeError('Duplicate patch or unsupported strip count: ' + name)
        seen.add(name)
        path = manifest.parent / name
        check_file(path, record)
        patches.append((path, record))
    return patches


def download(url, destination, expected, offline):
    if not url.startswith('https://download.ceph.com/'):
        raise RuntimeError('Only official HTTPS Ceph archive URLs are accepted')
    if destination.exists() or destination.is_symlink():
        check_file(destination, expected)
        print('Verified cached ' + destination.name, flush=True)
        return
    if offline:
        raise RuntimeError('Offline input missing: ' + str(destination))
    if not url.startswith('https://download.ceph.com/'):
        raise RuntimeError('Only official HTTPS Ceph archive URLs are accepted')
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + '.part')
    if partial.exists() or partial.is_symlink():
        raise RuntimeError('Unclaimed partial download exists: ' + str(partial))
    print('Downloading ' + destination.name + ' (' + str(expected['size']) + ' bytes)', flush=True)
    try:
        with urllib.request.urlopen(url, timeout=90) as response, partial.open('xb') as output:
            if not response.geturl().startswith('https://download.ceph.com/'):
                raise RuntimeError('Unexpected archive redirect origin')
            total = 0
            for block in iter(lambda: response.read(1024 * 1024), b''):
                total += len(block)
                if total > int(expected['size']):
                    raise RuntimeError('Download exceeds authenticated archive size')
                output.write(block)
        check_file(partial, expected)
        os.replace(partial, destination)
    finally:
        if partial.exists():
            partial.unlink()


def check_linux(recipe):
    if platform.system() != 'Linux' or platform.machine() not in recipe.data['native_machines']:
        raise RuntimeError('Native build requires Linux ARM64, not host emulation or macOS')
    release = fields(Path('/etc/os-release').read_text().replace('=', ': '))
    if release.get('ID', '').strip('"') != recipe.data['distribution'] or release.get('VERSION_ID', '').strip('"') != recipe.data['distribution_version']:
        raise RuntimeError('Native build requires Ubuntu Noble 24.04')
    if capture(['dpkg', '--print-architecture']).strip() != recipe.data['architecture']:
        raise RuntimeError('Native dpkg architecture must be arm64')


def extract_verified(archive, destination):
    if destination.exists():
        raise RuntimeError('Source extraction directory must not exist: ' + str(destination))
    temporary = destination.with_name(destination.name + '.extracting')
    if temporary.exists():
        raise RuntimeError('Existing incomplete source extraction: ' + str(temporary))
    temporary.mkdir()
    with tarfile.open(archive, 'r:gz') as stream:
        members = stream.getmembers()
        top = set()
        for member in members:
            name = PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts or not name.parts:
                raise RuntimeError('Unsafe archive path: ' + member.name)
            top.add(name.parts[0])
            if member.isdev() or member.isfifo():
                raise RuntimeError('Unexpected special source archive member: ' + member.name)
            if member.issym():
                target = (temporary / member.name).parent / member.linkname
                if not target.resolve().is_relative_to(temporary.resolve()):
                    raise RuntimeError('Source archive symlink escapes extraction root')
            if member.islnk():
                target = PurePosixPath(member.linkname)
                if target.is_absolute() or '..' in target.parts:
                    raise RuntimeError('Source archive hardlink escapes extraction root')
        if len(top) != 1:
            raise RuntimeError('Source archive must contain one top-level directory')
        # Python Noble 3.12 supports data filter; never extract as unrestricted root.
        stream.extractall(temporary, filter='data')
    os.replace(temporary / top.pop(), destination)
    temporary.rmdir()


def is_elf(path):
    with path.open('rb') as stream:
        return stream.read(4) == b'\x7fELF'


def stage_native_artifacts(build, artifacts):
    artifacts.mkdir()
    (artifacts / 'bin').mkdir(); (artifacts / 'lib').mkdir()
    recorded = []
    for name in ['radosgw', 'radosgw-admin']:
        source = build / 'bin' / name
        if not source.is_file() or source.is_symlink():
            raise RuntimeError('Expected native executable missing: ' + str(source))
        destination = artifacts / 'bin' / name
        shutil.copy2(source, destination)
        recorded.append({'path': 'bin/' + name, 'sha256': digest(destination)})
    # Collect only ELF shared objects under build/lib. This includes same-build
    # private Ceph libraries and any non-system bundled shared dependencies.
    libraries = sorted((build / 'lib').glob('*.so*'))
    for source in libraries:
        if source.is_symlink():
            link = os.readlink(source)
            if '/' in link or link in ('.', '..'):
                raise RuntimeError('Unexpected shared-library symlink: ' + str(source))
            (artifacts / 'lib' / source.name).symlink_to(link)
        elif source.is_file() and is_elf(source):
            shutil.copy2(source, artifacts / 'lib' / source.name)
            recorded.append({'path': 'lib/' + source.name, 'sha256': digest(source)})
    for prefix in ['libceph-common.so', 'librados.so', 'librgw.so']:
        if not any((artifacts / 'lib').glob(prefix + '*')):
            raise RuntimeError('Missing same-build native private closure library: ' + prefix)
    native_env = dict(os.environ)
    native_env.pop('LD_LIBRARY_PATH', None)
    native_env.pop('LD_PRELOAD', None)
    for item in recorded:
        path = artifacts / item['path']
        # Relocate *every* ELF's loader paths before checking closure. Build-tree
        # RUNPATH must never make an incomplete staged bundle appear complete.
        rpath = '$ORIGIN/../lib' if item['path'].startswith('bin/') else '$ORIGIN'
        subprocess.run(['patchelf', '--force-rpath', '--set-rpath', rpath, str(path)], check=True)
        item['sha256_before_rpath'] = item['sha256']
        item['sha256'] = digest(path)
    for item in recorded:
        path = artifacts / item['path']
        header = capture(['readelf', '-h', path])
        if not re.search(r'Machine:\s+AArch64', header):
            raise RuntimeError('Native artifact is not AArch64: ' + item['path'])
        dynamic = capture(['readelf', '-d', path])
        (artifacts / (path.name + '.readelf.txt')).write_text(header + dynamic)
        result = subprocess.run(['ldd', '-r', str(path)], env=native_env, check=False,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (artifacts / (path.name + '.ldd.txt')).write_text(result.stdout)
        if result.returncode or 'not found' in result.stdout or 'undefined symbol:' in result.stdout:
            raise RuntimeError('Native loader closure unresolved: ' + item['path'])
        for match in re.finditer(r'(?:=>\s+|^\s*)(/[^\s()]+)', result.stdout, re.MULTILINE):
            resolved = Path(match.group(1)).resolve(strict=True)
            if not resolved.is_relative_to(artifacts.resolve()) and not resolved.is_relative_to(Path('/usr/lib').resolve()):
                raise RuntimeError('Native loader resolves outside staged/system closure: ' + str(resolved))
    save_json(artifacts / 'manifest.json', {'files': recorded, 'loader_checked_with': 'Relocated DT_RPATH, no LD_LIBRARY_PATH or LD_PRELOAD',
        'status': 'built-native-closure; not yet repackaged or runtime-image/client proven',
        'private_abi_boundary': 'Same-build libceph-common+librados isolated per patched RGW process; do not globally replace official libraries.'})


def build_native(args, inputs, output, patches, report):
    recipe = args.recipe_data
    recipe.verify_unchanged()
    check_linux(recipe)
    for command in ['cmake', 'ninja', 'patch', 'readelf', 'ldd', 'ld.bfd', 'g++', 'patchelf']:
        if shutil.which(command) is None:
            raise RuntimeError('Build prerequisite missing: ' + command)
    source = output / 'source'; build = output / 'build'
    prepared = Path(report['source_archive_path'])
    if source.exists() or build.exists():
        raise RuntimeError('Native build requires fresh source/build dirs; preserve failed logs and choose new output')
    extract_verified(prepared, source)
    if (source / '.git').exists():
        raise RuntimeError('Expected archive source without Git checkout')
    version = (source / 'src/.git_version').read_text().splitlines()
    if not version or version[0] != inputs['source_commit_reference']:
        raise RuntimeError('Archive .git_version differs from pinned official source commit reference')
    original = json.loads(recipe.source_manifest_path.read_text())
    # SHA256 proves only the targeted original files; release archive identity is
    # independently authenticated above. Do not claim complete git tree identity.
    source_files = original.get('files', [])
    if isinstance(source_files, dict):
        source_files = [dict(path=name, **record) for name, record in source_files.items()]
    for record in source_files:
        check_file(relative_input(source, record['path']), record)
    report['archive_git_version'] = version
    report['source_manifest_sha256'] = digest(recipe.source_manifest_path)
    for index, (path, record) in enumerate(patches):
        execute(['patch', '--batch', '--forward', '--fuzz=0', '--dry-run', '-p1', '-i', path],
                output / ('logs/patch-%02d-dry-run.log' % index), cwd=source)
        execute(['patch', '--batch', '--forward', '--fuzz=0', '-p1', '-i', path],
                output / ('logs/patch-%02d-apply.log' % index), cwd=source)
    patched_files = {}
    for path, _ in patches:
        for line in path.read_text().splitlines():
            if line.startswith('+++ '):
                value = line[4:].split()[0]
                if value == '/dev/null':
                    continue
                parts = PurePosixPath(value).parts
                if len(parts) < 2 or '..' in parts or PurePosixPath(value).is_absolute():
                    raise RuntimeError('Unsafe patch output path: ' + value)
                relative = '/'.join(parts[1:])
                patched_files[relative] = digest(source / relative)
    report['patched_source_files_sha256'] = patched_files
    env = dict(os.environ, LC_ALL='C.UTF-8', TZ='UTC', CMAKE_BUILD_PARALLEL_LEVEL='1',
               MAKEFLAGS='-j1', NINJAFLAGS='-j1', PYTHONHASHSEED='0')
    inventory = capture(['dpkg-query', '-W', '-f=${Package}\t${Version}\t${Architecture}\n'])
    (output / 'build-package-inventory.tsv').write_text(inventory)
    (output / 'compiler-version.txt').write_text(capture(['g++', '--version']))
    command = ['cmake', '-S', source, '-B', build, '-G', 'Ninja']
    command += ['-D' + name + '=' + value for name, value in sorted(recipe.cmake.items())]
    execute(command, output / 'logs/configure.log', env=env)
    # Mark native results only after real commands completed. Never call ninja all.
    report['native_status'] = 'configured'; save_json(output / 'report.json', report)
    execute(['cmake', '--build', build, '--parallel', '1', '--target'] + recipe.targets,
            output / 'logs/build.log', env=env, timeout=args.build_timeout)
    report['native_status'] = 'built'; save_json(output / 'report.json', report)
    tests = capture(['ctest', '--test-dir', build, '-N', '-R', '^unittest_rgw_bucket_sync_pipe_rules$'])
    (output / 'logs/ctest-discovery.log').write_text(tests)
    if 'Total Tests: 1' not in tests:
        raise RuntimeError('Exactly one actual native Ceph selector test target must be discovered')
    execute(['ctest', '--test-dir', build, '-R', '^unittest_rgw_bucket_sync_pipe_rules$',
             '--output-on-failure', '--timeout', '180'], output / 'logs/native-gtest.log', env=env)
    execute([build / 'bin/unittest_rgw_bucket_sync_pipe_rules',
             '--gtest_output=xml:' + str(output / 'native-gtest.xml')],
            output / 'logs/native-gtest-xml.log', env=env, timeout=180)
    xml = ET.parse(output / 'native-gtest.xml').getroot()
    if int(xml.attrib.get('tests', '0')) < recipe.data['minimum_gtest_cases'] or any(int(xml.attrib.get(name, '0')) for name in ['failures', 'errors', 'disabled']):
        raise RuntimeError('Native gtest XML does not prove the required nine regression cases passed')
    report['native_gtest_tests'] = int(xml.attrib['tests'])
    report['native_gtest_xml_sha256'] = digest(output / 'native-gtest.xml')
    report['native_status'] = 'native-unit-pass'; save_json(output / 'report.json', report)
    for name in ['radosgw', 'radosgw-admin']:
        execute([build / 'bin' / name, '--version'], output / ('logs/' + name + '-version.log'), env=env, timeout=30)
    stage_native_artifacts(build, output / 'native-artifacts')
    report['native_artifacts_manifest_sha256'] = digest(output / 'native-artifacts/manifest.json')
    report['runtime_status'] = 'native-artifacts-staged; image/multisite-client proof NOT RUN'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--plan', action='store_true', help='Print frozen input/target plan; no network or subprocess')
    mode.add_argument('--prepare-only', action='store_true', help='Authenticate and fetch source+22 runtime archives only; host-safe')
    mode.add_argument('--resume-build', action='store_true', help='Guarded same-snapshot resume preserving previous failure/logs; canonical GNU bfd')
    mode.add_argument('--build', action='store_true', help='Authenticate/fetch then actual Noble ARM64 CMake, j1 build, native gtest and ELF stage')
    parser.add_argument('--recipe', required=True, type=Path, help='Explicit supported recipe JSON')
    parser.add_argument('--output', type=Path, help='Required except --plan; use an ignored artifact/cache directory')
    parser.add_argument('--source-archive', type=Path, help='Adopt already downloaded exact signed source archive; never redownload/copy it')
    parser.add_argument('--deb-directory', type=Path, help='Adopt complete already downloaded official runtime archives; never redownload/copy')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--build-timeout', type=int, default=24 * 3600)
    args = parser.parse_args(argv)
    recipe = args.recipe_data = Recipe(args.recipe)
    inputs = recipe.inputs
    args.patch_manifest = recipe.patch_manifest_path
    patches = patch_inputs(args.patch_manifest.resolve())
    if args.plan:
        print(json.dumps({'source': inputs['source'], 'source_identity_boundary': inputs['source_identity_boundary'],
            'deb_count': len(inputs['runtime_packages']), 'deb_bytes': sum(int(p['Size']) for p in inputs['runtime_packages']),
            'patches': [record for _, record in patches], 'cmake': recipe.cmake, 'targets': recipe.targets,
            'native_status': 'UNVERIFIED; this command executes no native operation'}, indent=2, sort_keys=True))
        return 0
    if args.output is None:
        parser.error('--output is required except with --plan')
    output = args.output.resolve()
    if args.resume_build:
        import native_resume
        return native_resume.resume(args, inputs, output, patches)
    output.mkdir(parents=True, exist_ok=True)
    marker = output / '.ceph-native-selector-run'
    if marker.is_symlink():
        raise RuntimeError('Output ownership marker must be regular')
    if not marker.exists():
        if any(output.iterdir()):
            raise RuntimeError('Output must be empty or owned by this runner')
        marker.write_text('schema=1\n')
    elif marker.read_text() != 'schema=1\n':
        raise RuntimeError('Unrecognized output ownership marker')
    # A fresh preparation must not overwrite an actual configured build's proof.
    if (output / 'source').exists() or (output / 'build').exists():
        raise RuntimeError('Existing native source/build requires --resume-build or a fresh output')
    if (output / 'report.json').exists():
        previous = json.loads((output / 'report.json').read_text())
        if previous.get('native_status') != 'NOT RUN':
            raise RuntimeError('Preserve prior native proof; use --resume-build or a fresh output')
    report = {'schema': 1, 'native_status': 'NOT RUN', 'runtime_status': 'NOT RUN',
              'inputs_manifest_sha256': digest(recipe.inputs_path),
              'recipe_sha256': recipe.sha256, 'profile': recipe.data['profile'],
              'runner_sha256': digest(HERE / 'native_build.py'),
              'patches_manifest_sha256': digest(args.patch_manifest),
              'ordered_patches': [record for _, record in patches], 'targets': recipe.targets,
              'cmake_definitions': recipe.cmake, 'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    try:
        report['authentication'] = verify_metadata(inputs, output, recipe.metadata)
        save_json(output / 'report.json', report)
        archive = args.source_archive.resolve() if args.source_archive else output / 'archives' / inputs['source']['filename']
        if args.source_archive:
            check_file(archive, inputs['source'])
            print('Adopted verified source archive ' + str(archive), flush=True)
        else:
            download(inputs['source']['url'], archive, inputs['source'], args.offline)
        report['source_archive_path'] = str(archive)
        deb_directory = args.deb_directory.resolve() if args.deb_directory else output / 'official-debs'
        for package in inputs['runtime_packages']:
            name = basename(PurePosixPath(package['Filename']).name)
            expected = {'sha256': package['SHA256'], 'size': int(package['Size'])}
            if args.deb_directory:
                check_file(deb_directory / name, expected)
            else:
                download(inputs['repo_url'] + package['Filename'], deb_directory / name, expected, args.offline)
        report['runtime_archive_directory'] = str(deb_directory)
        report['download_status'] = 'all archives authenticated and exact SHA256 verified'
        save_json(output / 'report.json', report)
        if args.build:
            build_native(args, inputs, output, patches, report)
        save_json(output / 'report.json', report)
        print('Completed requested phase; native=' + report['native_status'] + '; report=' + str(output / 'report.json'))
        return 0
    except Exception as error:
        report['failure'] = str(error); save_json(output / 'report.json', report)
        print('FAILED: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
