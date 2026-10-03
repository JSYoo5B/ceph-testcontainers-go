#!/usr/bin/env python3
"""Package actual native-unit-passed RGW artifacts for image/slim --deb-directory.

Ubuntu Noble ARM64 only. No Docker/Git. Isolate same-build private Ceph ELF
closure per patched RGW process; retain official libraries for other daemons.
This packager never claims installed-image or multisite/client proof.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

import native_build as runner

PRIVATE_PACKAGE = 'ceph-tc-native-rgw'
PRIVATE_RELATIVE = Path('usr/lib') / PRIVATE_PACKAGE


def clean_env(recipe):
    result = dict(os.environ, LC_ALL='C.UTF-8', TZ='UTC', SOURCE_DATE_EPOCH=str(recipe.data['source_date_epoch']))
    result.pop('LD_LIBRARY_PATH', None)
    result.pop('LD_PRELOAD', None)
    return result


def query_owner(path):
    # Distro packages may record /lib instead of usrmerged /usr/lib paths.
    candidates = [path, path.resolve()]
    for candidate in list(candidates):
        value = str(candidate)
        if value.startswith('/usr/'):
            candidates.append(Path(value[4:]))
    for candidate in dict.fromkeys(candidates):
        result = subprocess.run(['dpkg-query', '-S', str(candidate)], check=False,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode:
            continue
        matches = []
        for line in result.stdout.splitlines():
            owner, separator, supplied = line.partition(': ')
            if separator and supplied == str(candidate) and not owner.startswith('diversion'):
                matches.extend(owner.split(', '))
        if len(matches) == 1:
            values = runner.capture(['dpkg-query', '-W', '-f=${Package}\t${Version}\t${Architecture}\t${source:Package}\t${db:Status-Status}\n', matches[0]]).strip().split('\t')
            if len(values) == 5 and values[4] == 'installed':
                return {'package': values[0], 'version': values[1], 'architecture': values[2],
                        'source': values[3] or values[0]}
    raise RuntimeError('Cannot establish unique installed package ownership of ELF dependency: ' + str(path))


def verify_loader_closure(combined, logs, recipe):
    private = combined / PRIVATE_RELATIVE
    elf_files = sorted(path for path in combined.rglob('*') if path.is_file() and
                       not path.is_symlink() and runner.is_elf(path))
    if not elf_files:
        raise RuntimeError('No real native ELF artifacts staged')
    dependencies = {}; resolved_records = []
    for path in elf_files:
        relative = path.relative_to(combined).as_posix()
        header = runner.capture(['readelf', '-h', path])
        if not re.search(r'Machine:\s+AArch64', header):
            raise RuntimeError('Non-AArch64 ELF: ' + relative)
        dynamic = runner.capture(['readelf', '-d', path])
        wanted_rpath = '$ORIGIN/../lib/' + PRIVATE_PACKAGE if path.parent == combined / 'usr/bin' else '$ORIGIN'
        if '(RUNPATH)' in dynamic or '(RPATH)' not in dynamic or '[' + wanted_rpath + ']' not in dynamic:
            raise RuntimeError('Native ELF has unexpected loader search path: ' + relative)
        result = subprocess.run(['ldd', '-r', str(path)], check=False, env=clean_env(recipe),
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log_name = relative.replace('/', '_')
        (logs / (log_name + '.readelf.txt')).write_text(header + dynamic)
        (logs / (log_name + '.ldd.txt')).write_text(result.stdout)
        if result.returncode or 'not found' in result.stdout or 'undefined symbol:' in result.stdout:
            raise RuntimeError('Native ELF dependency unresolved: ' + relative)
        for match in re.finditer(r'(?:=>\s+|^\s*)(/[^\s()]+)', result.stdout, re.MULTILINE):
            resolved = Path(match.group(1)).resolve(strict=True)
            if resolved.is_relative_to(private.resolve()):
                origin = 'same-build-private'
            elif resolved.is_relative_to(Path('/usr/lib').resolve()):
                owner = query_owner(resolved)
                if owner['source'] == 'ceph':
                    raise RuntimeError('Patched RGW would load an external official Ceph private/public ELF: ' + str(resolved))
                dependencies[owner['package'] + ':' + owner['architecture']] = owner
                origin = owner
            else:
                raise RuntimeError('Patched RGW would load build-tree/unpackaged ELF: ' + str(resolved))
            resolved_records.append({'elf': relative, 'resolved': str(resolved), 'origin': origin})
    return sorted(dependencies.values(), key=lambda record: record['package']), resolved_records


def regenerate_md5sums(root):
    entries = []
    for path in sorted(root.rglob('*')):
        if 'DEBIAN' in path.relative_to(root).parts or path.is_symlink() or not path.is_file():
            continue
        digest = hashlib.md5()
        with path.open('rb') as stream:
            for data in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(data)
        entries.append(digest.hexdigest() + '  ' + path.relative_to(root).as_posix())
    (root / 'DEBIAN/md5sums').write_text('\n'.join(entries) + '\n')


def append_control_fields(control_path, package_version, patch_manifest_sha256):
    text = control_path.read_text().rstrip('\n') + '\n'
    if len(runner.paragraphs(text)) != 1:
        raise RuntimeError('Binary control must contain exactly one paragraph')
    if re.search(r'^X-Ceph-TC-', text, flags=re.MULTILINE):
        raise RuntimeError('Official package already contains local fixture provenance fields')
    dependency = PRIVATE_PACKAGE + ' (= ' + package_version + ')'
    match = re.search(r'^Depends:([^\n]*(?:\n[ \t][^\n]*)*)', text, flags=re.MULTILINE)
    if match:
        # Unfold the entire native control field before appending, so an original
        # trailing comma/continuation cannot corrupt Debian Depends grammar.
        original = ' '.join(runner.fields(text)['Depends'].split())
        if not original or original.endswith(','):
            raise RuntimeError('Malformed original Depends field')
        text = text[:match.start()] + 'Depends: ' + original + ', ' + dependency + text[match.end():]
    else:
        text += 'Depends: ' + dependency + '\n'
    text = text.rstrip('\n') + '\n'
    text += ('X-Ceph-TC-Native-Patch-Manifest-SHA256: ' + patch_manifest_sha256 +
             '\nX-Ceph-TC-Local-Payload: true\n')
    control_path.write_text(text)


def private_control(private_version, dependencies, recipe):
    depends = ', '.join(record['package'] + ' (>= ' + record['version'] + ')' for record in dependencies)
    return ('Package: ' + PRIVATE_PACKAGE + '\nVersion: ' + private_version +
               '\nArchitecture: ' + recipe.data['architecture'] + '\nSource: ceph (' + recipe.data['source_version'] + ')\nSection: libs\nPriority: optional\n' +
               'Maintainer: JaeSang Yoo <jsyoo5b@gmail.com>\n' +
               'Depends: ' + depends + '\n' +
               'Description: Local native Ceph RGW fixture private ELF closure\n' +
               ' Same-build private libs for patched radosgw and radosgw-admin only.\n' +
               ' Original Ceph libraries remain installed for all other daemons.\n' +
               ' Native image/client regression evidence is a separate required gate.\n')


def package(args):
    recipe = args.recipe_data
    recipe.verify_unchanged()
    runner.check_linux(recipe)
    for command in ['dpkg-deb', 'dpkg-query', 'patchelf', 'readelf', 'ldd']:
        if shutil.which(command) is None:
            raise RuntimeError('Packaging prerequisite missing: ' + command)
    run = args.run_dir.resolve(strict=True)
    report = json.loads((run / 'report.json').read_text())
    if report.get('native_status') != 'native-unit-pass' or report.get('native_gtest_tests', 0) < recipe.data['minimum_gtest_cases']:
        raise RuntimeError('Actual native CMake build and nine passed selector gtests are required before packaging')
    if report.get('runtime_status') != 'native-artifacts-staged; image/multisite-client proof NOT RUN':
        raise RuntimeError('Complete relocated native artifact stage required')
    if report['patches_manifest_sha256'] != runner.digest(args.patch_manifest):
        raise RuntimeError('Patch manifest differs from the actual built snapshot')
    if report['inputs_manifest_sha256'] != runner.digest(recipe.inputs_path):
        raise RuntimeError('Authenticated build inputs differ from the built snapshot')
    if report.get('recipe_sha256') != recipe.sha256:
        raise RuntimeError('Recipe differs from the actual built snapshot')
    runner.patch_inputs(args.patch_manifest.resolve())
    runner.check_file(run / 'native-gtest.xml', {'sha256': report['native_gtest_xml_sha256']})
    runner.check_file(run / 'native-artifacts/manifest.json', {'sha256': report['native_artifacts_manifest_sha256']})
    xml = runner.ET.parse(run / 'native-gtest.xml').getroot()
    if int(xml.attrib.get('tests', '0')) != report['native_gtest_tests'] or any(int(xml.attrib.get(name, '0')) for name in ['failures', 'errors', 'disabled']):
        raise RuntimeError('Native XML evidence differs from passed build report')
    for name, expected in report.get('patched_source_files_sha256', {}).items():
        relative = PurePosixPath(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise RuntimeError('Unsafe patched source provenance path')
        runner.check_file(run / 'source' / name, {'sha256': expected})
    inputs = recipe.inputs
    official = args.deb_directory.resolve() if args.deb_directory else Path(report['runtime_archive_directory'])
    artifact = run / 'native-artifacts'
    native = json.loads((artifact / 'manifest.json').read_text())
    for item in native['files']:
        relative = PurePosixPath(item['path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise RuntimeError('Unsafe artifact path')
        runner.check_file(artifact / item['path'], item)
    output = args.output.resolve()
    if output.exists():
        if not output.is_dir() or any(output.iterdir()):
            raise RuntimeError('Packaging output must be empty')
    else:
        output.mkdir(parents=True)
    (output / 'logs').mkdir(); (output / 'debs').mkdir(); (output / 'staging').mkdir()
    for path in (artifact / 'lib').iterdir():
        if path.is_symlink():
            target = os.readlink(path)
            if '/' in target or target in ('.', '..') or not path.resolve(strict=True).is_relative_to((artifact / 'lib').resolve()):
                raise RuntimeError('Staged native library symlink escapes private closure')
    combined = output / 'staging/combined'
    (combined / 'usr/bin').mkdir(parents=True)
    shutil.copytree(artifact / 'lib', combined / PRIVATE_RELATIVE, symlinks=True)
    for name in ['radosgw', 'radosgw-admin']:
        path = combined / 'usr/bin' / name
        shutil.copy2(artifact / 'bin' / name, path)
        subprocess.run(['patchelf', '--force-rpath', '--set-rpath',
                        '$ORIGIN/../lib/' + PRIVATE_PACKAGE, str(path)], check=True)
    dependencies, resolved = verify_loader_closure(combined, output / 'logs', recipe)
    patch_sha = runner.digest(args.patch_manifest)
    private_version = recipe.data['private_version_prefix'] + patch_sha[:16]
    provenance = {'schema': 1, 'status': 'locally patched native ELF/packages; runtime-image/multisite client proof NOT RUN',
                  'build_report': report, 'artifact_stage': native, 'private_package_version': private_version,
                  'dependency_versions_at_packaging': dependencies,
                  'resolved_elf_dependencies': resolved,
                  'official_ceph_package_version_preserved': recipe.data['source_version'],
                  'version_note': 'Ceph native --version retains original release commit. Ordered patch SHA and modified .deb SHA identify this custom build.'}
    for name in ['radosgw', 'radosgw-admin']:
        runner.execute([combined / 'usr/bin' / name, '--version'], output / ('logs/' + name + '-relocated-version.log'),
                       env=clean_env(recipe), timeout=30)
    private_root = output / 'staging' / PRIVATE_PACKAGE
    shutil.copytree(combined / PRIVATE_RELATIVE, private_root / PRIVATE_RELATIVE, symlinks=True)
    doc = private_root / 'usr/share/doc' / PRIVATE_PACKAGE
    doc.mkdir(parents=True)
    copying = run / 'source/COPYING'
    if not copying.is_file():
        raise RuntimeError('Native source COPYING is required for private package license/provenance')
    shutil.copy2(copying, doc / 'copyright')
    runner.save_json(doc / 'provenance.json', provenance)
    shutil.copy2(args.patch_manifest, doc / 'patches.json')
    for path, record in runner.patch_inputs(args.patch_manifest.resolve()):
        shutil.copy2(path, doc / path.name)
    (private_root / 'DEBIAN').mkdir()
    depends = ', '.join(record['package'] + ' (>= ' + record['version'] + ')' for record in dependencies)
    control = private_control(private_version, dependencies, recipe)
    (private_root / 'DEBIAN/control').write_text(control)
    regenerate_md5sums(private_root)
    private_deb = output / 'debs' / (PRIVATE_PACKAGE + '_' + private_version + '_' + recipe.data['architecture'] + '.deb')
    runner.execute(['dpkg-deb', '--root-owner-group', '--build', private_root, private_deb],
                   output / 'logs/build-private-package.log', env=clean_env(recipe))
    records = []
    for item in inputs['runtime_packages']:
        filename = runner.basename(PurePosixPath(item['Filename']).name)
        source = official / filename
        runner.check_file(source, {'sha256': item['SHA256'], 'size': int(item['Size'])})
        info = runner.fields(runner.capture(['dpkg-deb', '--field', source]))
        if any(info.get(name) != item[name] for name in ['Package', 'Version', 'Architecture']):
            raise RuntimeError('Official .deb control identity differs from signed Packages: ' + filename)
        target = output / 'debs' / filename
        if item['Package'] in ('radosgw', 'ceph-common'):
            root = output / 'staging' / item['Package']
            runner.execute(['dpkg-deb', '--raw-extract', source, root], output / ('logs/extract-' + item['Package'] + '.log'))
            name = 'radosgw' if item['Package'] == 'radosgw' else 'radosgw-admin'
            original = root / 'usr/bin' / name
            if original.is_symlink() or not original.is_file():
                raise RuntimeError('Unexpected original executable package layout: ' + str(original))
            shutil.copy2(combined / 'usr/bin' / name, original)
            append_control_fields(root / 'DEBIAN/control', private_version, patch_sha)
            regenerate_md5sums(root)
            runner.execute(['dpkg-deb', '--root-owner-group', '--build', root, target],
                           output / ('logs/build-' + item['Package'] + '.log'), env=clean_env(recipe))
        else:
            shutil.copy2(source, target)
        records.append({'package': item['Package'], 'filename': filename, 'original_official_sha256': item['SHA256'],
                        'sha256': runner.digest(target), 'modified': item['Package'] in ('radosgw', 'ceph-common')})
    records.append({'package': PRIVATE_PACKAGE, 'filename': private_deb.name, 'sha256': runner.digest(private_deb),
                    'modified': True})
    provenance['output_packages'] = records
    runner.save_json(output / 'package-report.json', provenance)
    print('Staged 23 local .debs for --deb-directory: ' + str(output / 'debs'))
    print('Install/slim image smoke + unchanged multisite/client tests remain NOT RUN.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--deb-directory', type=Path, help='Override already verified original runtime archives cache')
    parser.add_argument('--recipe', required=True, type=Path, help='The exact recipe used for the native build')
    args = parser.parse_args()
    try:
        args.recipe_data = runner.Recipe(args.recipe)
        args.patch_manifest = args.recipe_data.patch_manifest_path
        package(args)
        return 0
    except Exception as error:
        print('FAILED: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
