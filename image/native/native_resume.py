#!/usr/bin/env python3
"""Guarded same-snapshot resume of actual Ceph native build; preserves failures."""
import copy
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import time
import uuid
import xml.etree.ElementTree as ET

import native_build as runner

LINKER_KEYS = frozenset(('CMAKE_EXE_LINKER_FLAGS', 'CMAKE_SHARED_LINKER_FLAGS'))
INVENTORY_COMMAND = ['dpkg-query', '-W', '-f=${Package}\t${Version}\t${Architecture}\n']


def cache_fields(path):
    result = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith(('#', '//')) or '=' not in line:
            continue
        name, value = line.split('=', 1)
        name = name.split(':', 1)[0]
        if name in result:
            raise RuntimeError('Duplicate CMake cache entry: ' + name)
        result[name] = value
    return result


def verify_flags(cache, definitions, allow_previous_linker):
    for name, value in definitions.items():
        expected = {value}
        if allow_previous_linker and name in LINKER_KEYS:
            expected.add('-fuse-ld=lld')
        if cache.get(name) not in expected:
            raise RuntimeError('CMake cache differs from admitted snapshot: ' + name +
                               '=' + str(cache.get(name)) + '; expected ' + str(sorted(expected)))


def admission(args, inputs, output, patches):
    recipe = args.recipe_data
    recipe.verify_unchanged()
    runner.check_linux(recipe)
    if (output / '.ceph-native-selector-run').is_symlink():
        raise RuntimeError('Output ownership marker must be regular')
    if (output / '.ceph-native-selector-run').read_text() != 'schema=1\n':
        raise RuntimeError('Missing or unrecognized native runner ownership marker')
    original_report = output / 'report.json'
    previous = json.loads(original_report.read_text())
    if previous.get('schema') != 1 or not (output / 'source').is_dir() or not (output / 'build').is_dir():
        raise RuntimeError('Existing configured owned source/build/report required')
    if previous.get('native_status') not in ('configured', 'built', 'native-unit-pass', 'resume-admitted'):
        raise RuntimeError('Prior attempt did not reach an admissible native configured state')
    expected_hashes = {
        'recipe_sha256': recipe.sha256,
        'inputs_manifest_sha256': runner.digest(recipe.inputs_path),
        'patches_manifest_sha256': runner.digest(args.patch_manifest),
        'source_manifest_sha256': runner.digest(recipe.source_manifest_path),
    }
    for name, value in expected_hashes.items():
        if previous.get(name) != value:
            raise RuntimeError('Build snapshot manifest changed; cannot resume: ' + name)
    if previous.get('ordered_patches') != [record for _, record in patches] or previous.get('targets') != recipe.targets:
        raise RuntimeError('Ordered patches/native targets differ from configured snapshot')
    previous_flags = previous.get('cmake_definitions', {})
    if set(previous_flags) != set(recipe.cmake):
        raise RuntimeError('CMake definition set changed across resume')
    for name, value in recipe.cmake.items():
        allowed = {value, '-fuse-ld=lld'} if name in LINKER_KEYS else {value}
        if previous_flags[name] not in allowed:
            raise RuntimeError('Only the explicit lld-to-bfd linker change is admitted: ' + name)
    source = output / 'source'
    version = (source / 'src/.git_version').read_text().splitlines()
    if not version or version != previous.get('archive_git_version') or version[0] != inputs['source_commit_reference']:
        raise RuntimeError('Source native release identity changed')
    patched = previous.get('patched_source_files_sha256')
    if not isinstance(patched, dict) or not patched:
        raise RuntimeError('Prior patch output identity proof is missing')
    for name, value in patched.items():
        path = PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts:
            raise RuntimeError('Unsafe patched-source provenance path')
        runner.check_file(source / name, {'sha256': value})
    baseline = json.loads(recipe.source_manifest_path.read_text())
    for record in baseline['files']:
        if record['path'] not in patched:
            runner.check_file(runner.relative_input(source, record['path']), record)
    if runner.capture(INVENTORY_COMMAND) != (output / 'build-package-inventory.tsv').read_text():
        raise RuntimeError('Installed package inventory changed since original configure')
    if runner.capture(['g++', '--version']) != (output / 'compiler-version.txt').read_text():
        raise RuntimeError('Compiler version changed since original configure')
    cache = cache_fields(output / 'build/CMakeCache.txt')
    verify_flags(cache, recipe.cmake, allow_previous_linker=True)
    archive = args.source_archive.resolve() if args.source_archive else Path(previous['source_archive_path'])
    runner.check_file(archive, inputs['source'])
    directory = args.deb_directory.resolve() if args.deb_directory else Path(previous['runtime_archive_directory'])
    for item in inputs['runtime_packages']:
        runner.check_file(directory / runner.basename(PurePosixPath(item['Filename']).name),
                          {'sha256': item['SHA256'], 'size': int(item['Size'])})
    return previous, archive, directory, cache


def resume(args, inputs, output, patches):
    recipe = args.recipe_data
    previous, archive, directory, previous_cache = admission(args, inputs, output, patches)
    for command in ['cmake', 'ninja', 'patch', 'readelf', 'ldd', 'ld.bfd', 'g++', 'patchelf']:
        if shutil.which(command) is None:
            raise RuntimeError('Native resume prerequisite missing: ' + command)
    attempt = output / 'attempts' / (time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '-' + uuid.uuid4().hex[:8])
    attempt.mkdir(parents=True)
    old_report = attempt / 'previous-report.json'
    shutil.copy2(output / 'report.json', old_report)
    if (output / 'native-gtest.xml').exists():
        shutil.copy2(output / 'native-gtest.xml', attempt / 'previous-native-gtest.xml')
    history = list(previous.get('attempt_history', []))
    history.append({'report_path': str(old_report), 'report_sha256': runner.digest(old_report),
                    'native_status': previous['native_status'], 'failure': previous.get('failure'),
                    'cmake_definitions': previous['cmake_definitions'],
                    'original_logs_preserved': [str(output / 'logs/configure.log'), str(output / 'logs/build.log')],
                    'cache_linker_before_resume': {name: previous_cache[name] for name in LINKER_KEYS}})
    report = copy.deepcopy(previous)
    report.pop('failure', None)
    report.update({'attempt_history': history, 'current_attempt_directory': str(attempt),
                   'cmake_definitions': copy.deepcopy(recipe.cmake),
                   'native_status': 'resume-admitted', 'runtime_status': 'NOT RUN',
                   'runner_sha256': runner.digest(runner.HERE / 'native_build.py'),
                   'resume_helper_sha256': runner.digest(runner.HERE / 'native_resume.py'),
                   'resumed_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                   'source_archive_path': str(archive), 'runtime_archive_directory': str(directory)})
    runner.save_json(output / 'report.json', report)
    source = output / 'source'; build = output / 'build'
    env = dict(os.environ, LC_ALL='C.UTF-8', TZ='UTC', CMAKE_BUILD_PARALLEL_LEVEL='1',
               MAKEFLAGS='-j1', NINJAFLAGS='-j1', PYTHONHASHSEED='0')
    try:
        report['authentication'] = runner.verify_metadata(inputs, attempt, recipe.metadata)
        command = ['cmake', '-S', source, '-B', build, '-G', 'Ninja']
        command += ['-D' + name + '=' + value for name, value in sorted(recipe.cmake.items())]
        runner.execute(command, attempt / 'logs/configure.log', env=env)
        verify_flags(cache_fields(build / 'CMakeCache.txt'), recipe.cmake, allow_previous_linker=False)
        report['native_status'] = 'configured'; runner.save_json(output / 'report.json', report)
        # This actual native build verifies all dependencies/targets. It is a
        # no-op if root's same-snapshot manual bfd build already completed.
        runner.execute(['cmake', '--build', build, '--parallel', '1', '--target'] + recipe.targets,
                       attempt / 'logs/build.log', env=env, timeout=args.build_timeout)
        report['native_status'] = 'built'; runner.save_json(output / 'report.json', report)
        tests = runner.capture(['ctest', '--test-dir', build, '-N', '-R', '^unittest_rgw_bucket_sync_pipe_rules$'])
        (attempt / 'logs/ctest-discovery.log').write_text(tests)
        if 'Total Tests: 1' not in tests:
            raise RuntimeError('Exactly one actual native selector target must be discovered')
        runner.execute(['ctest', '--test-dir', build, '-R', '^unittest_rgw_bucket_sync_pipe_rules$',
                        '--output-on-failure', '--timeout', '180'], attempt / 'logs/native-gtest.log', env=env)
        xml_path = attempt / 'native-gtest.xml'
        runner.execute([build / 'bin/unittest_rgw_bucket_sync_pipe_rules', '--gtest_output=xml:' + str(xml_path)],
                       attempt / 'logs/native-gtest-xml.log', env=env, timeout=180)
        xml = ET.parse(xml_path).getroot()
        if int(xml.attrib.get('tests', '0')) < recipe.data['minimum_gtest_cases'] or any(int(xml.attrib.get(name, '0')) for name in ['failures', 'errors', 'disabled']):
            raise RuntimeError('Native gtest XML does not prove nine passed regression cases')
        shutil.copy2(xml_path, output / 'native-gtest.xml')
        report['native_gtest_tests'] = int(xml.attrib['tests'])
        report['native_gtest_xml_sha256'] = runner.digest(xml_path)
        report['native_status'] = 'native-unit-pass'; runner.save_json(output / 'report.json', report)
        for name in ['radosgw', 'radosgw-admin']:
            runner.execute([build / 'bin' / name, '--version'], attempt / ('logs/' + name + '-version.log'), env=env, timeout=30)
        if (output / 'native-artifacts').exists():
            # Preserve any previous/partial stage; never merge stale artifacts.
            os.replace(output / 'native-artifacts', attempt / 'previous-native-artifacts')
        runner.stage_native_artifacts(build, output / 'native-artifacts')
        report['native_artifacts_manifest_sha256'] = runner.digest(output / 'native-artifacts/manifest.json')
        report['runtime_status'] = 'native-artifacts-staged; image/multisite-client proof NOT RUN'
        runner.save_json(output / 'report.json', report)
        print('Native resumed phase complete; prior failure/history preserved; image/client proof NOT RUN.')
        return 0
    except Exception as error:
        report['failure'] = str(error)
        runner.save_json(output / 'report.json', report)
        print('Native resume FAILED: ' + str(error), flush=True)
        return 1
