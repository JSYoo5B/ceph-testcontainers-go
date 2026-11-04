#!/usr/bin/env python3
"""Execute a frozen periodic sample sequentially using existing CI receipts.

No fixture is shared across batches. Failed preparation and native execution
remain different outcomes. A dirty cleanup stops admission of further cases.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

import check_runtime_cleanup as cleanup
import in_test_images
import periodic_selection as selection
import registry_pull
import run_image_matrix as images
import runner_storage
import tag_scenarios as tags

ROOT = selection.ROOT
CLEANUP_RESERVE = 120
MIN_FREE_BYTES = 12 * (1 << 30)


def stop(child):
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()


def command(argv, log, seconds, env=None):
    """Bound a phase and its entire owned process group, including interruption."""
    started = time.monotonic()
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('w') as out:
        child = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=out, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        try:
            code = child.wait(timeout=max(1, seconds))
            timed_out = False
        except subprocess.TimeoutExpired:
            stop(child)
            code, timed_out = None, True
        except BaseException:
            stop(child)
            raise
    return {'exit_code': code, 'timed_out': timed_out,
            'elapsed_seconds': round(time.monotonic() - started, 3), 'passed': code == 0 and not timed_out}


def read_command(argv, seconds=60):
    return subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=seconds, check=False)


def cpu_eligible(baseline, architecture, run=read_command):
    required = baseline.get(architecture)
    if not required:
        return True
    if required != 'x86-64-v3':
        return False
    result = run(['/lib64/ld-linux-x86-64.so.2', '--help'])
    return result.returncode == 0 and bool(re.search(r'x86-64-v3 \(supported, searched\)', result.stdout))


def prepare_images(plan, directory, deadline=None):
    directory.mkdir(parents=True, exist_ok=False)
    def remaining(limit):
        seconds = limit if deadline is None else min(limit, deadline - time.monotonic() - CLEANUP_RESERVE)
        if seconds <= 0:
            raise RuntimeError('image preparation exhausted the execution budget')
        return seconds
    def read(argv):
        return read_command(argv, remaining(60))
    info = read(['docker', 'info', '--format', '{{json .}}'])
    if info.returncode:
        raise RuntimeError('Docker engine info unavailable')
    platform = images.native_platform(json.loads(info.stdout))
    if platform != plan['platform']:
        raise RuntimeError('environment_ineligible: engine is not the planned native platform')
    if not cpu_eligible(plan['images'].get('cpu_baseline', {}), platform.split('/')[1], read):
        raise RuntimeError('environment_ineligible: image CPU baseline is unavailable')
    if plan['images']['eligibility'] != 'available':
        raise RuntimeError('artifact_unavailable: catalogue has no usable image')
    if plan.get('test_images_error'):
        raise RuntimeError('artifact_resolution_failure: helper image identities were not resolved')
    if 'reference' in plan['images']:
        references = {'all': plan['images']['reference']}
    else:
        references = dict(plan['images']['references'])
    helper_names = {}
    for number, (reference, pinned) in enumerate(plan.get('test_images', {}).items()):
        name = 'helper-' + str(number)
        references[name] = pinned
        helper_names[reference] = name
    prepared, pulls = {}, {}
    for role, reference in references.items():
        def attempt(number, reference=reference, role=role):
            path = directory / (role + '-pull-' + str(number) + '.log')
            result = command(['docker', 'pull', '--platform', platform, reference], path, remaining(300))
            return result['exit_code'] or (1 if result['timed_out'] else 0), path.read_text()
        prepared[role] = registry_pull.resolve(role, reference, platform,
                lambda ref: read(['docker', 'image', 'inspect', ref]), attempt, pulls)
    # Give source literals local aliases to their frozen manifests. Refuse to
    # overwrite an existing conflicting alias, including on shared trial hosts.
    for reference, role in helper_names.items():
        inspected = read(['docker', 'image', 'inspect', reference])
        if inspected.returncode == 0:
            if json.loads(inspected.stdout)[0]['Id'] != prepared[role]['image_id']:
                raise RuntimeError('environment_ineligible: a helper alias has another image identity')
        elif registry_pull.ABSENT.search(inspected.stderr or '') and '@' not in reference:
            if read(['docker', 'image', 'tag', prepared[role]['image_id'], reference]).returncode:
                raise RuntimeError('cannot create the owned helper image alias')
        else:
            raise RuntimeError('helper image alias inspection failed')
    environment = {name: prepared.get(role, prepared.get('all'))['image_id'] for role, name in images.ROLE_ENV.items()}
    environment.update(SCENARIO_IMAGE_LAYOUT='roles', SCENARIO_IMAGE_ROLES=','.join(images.ROLE_ENV))
    result = {'schema': 1, 'release': plan['release'], 'platform': platform, 'images': prepared,
              'pulls': pulls, 'prepared_environment': environment, 'preparation_passed': True}
    selection.write(directory / 'summary.json', result)
    # Record the actual native build string as well as the registry identities.
    name = 'ctg-periodic-version-' + uuid.uuid4().hex
    try:
        check = command(['docker', 'run', '--rm', '--name', name, '--network', 'none',
                         environment['CEPH_TEST_ESSENTIAL_IMAGE'], 'ceph', '--version'], directory / 'version.log', remaining(30))
        version = (directory / 'version.log').read_text().strip()
        if not check['passed'] or not re.search(r'^ceph version ' + re.escape(plan['release']) + r'(?:\s|$)', version):
            raise RuntimeError('artifact_version_mismatch: native build is not the selected release')
        result['native_build'] = version
        selection.write(directory / 'summary.json', result)
    finally:
        # Exact name owned by this invocation; never prune images or other sessions.
        read_command(['docker', 'rm', '--force', name])
    return result


def native(plan, item, directory, compiled):
    catalog = tags.load_catalog()
    selection.validate_plan(plan, catalog)
    if 'test_images' not in plan:
        raise ValueError('execution requires a plan with frozen helper images')
    inventory = tags.discover_optional(catalog) if item['profile']['category'] == 'optional' else tags.discover(catalog)
    p = item['profile']
    report = tags.run_profile(catalog, inventory, p['category'], p['batch'], p['package'], directory,
                             binary=compiled, selected_tests_text=','.join(item['tests']),
                             timeout_seconds=item['native_seconds'])
    return 0 if report['passed'] else 1


def persist(path, receipt):
    receipt.pop('receipt_sha256', None)
    receipt['receipt_sha256'] = selection.digest(receipt)
    selection.write(path, receipt)


def execute(plan_path, directory, trial=False, runner=command):
    plan_path, directory = Path(plan_path).resolve(), Path(directory).resolve()
    plan = json.loads(plan_path.read_text())
    catalog = tags.load_catalog()
    selection.validate_plan(plan, catalog)
    if not trial and 'test_images' not in plan:
        raise ValueError('execution requires a plan with frozen helper images')
    revision = read_command(['git', 'rev-parse', 'HEAD'])
    if not trial and (revision.returncode or revision.stdout.strip() != plan['source_sha']
                      or read_command(['git', 'diff', '--quiet', 'HEAD']).returncode):
        raise ValueError('execution checkout differs from the frozen source commit')
    directory.mkdir(parents=True, exist_ok=False)
    receipt = {'schema': selection.SCHEMA, 'source_sha': plan['source_sha'],
               'source_provenance': 'working_tree_trial' if trial else 'verified_commit',
               'source_manifest': images.source_manifest(ROOT),
               'execution': os.environ.get('GITHUB_RUN_ID', '0') + '-' + os.environ.get('GITHUB_RUN_ATTEMPT', '1'),
               'mode': plan['mode'], 'cell': plan['cell'], 'reason': plan['reason'],
               'selected_at': plan['selected_at'], 'plan_sha256': plan['plan_sha256'],
               'release': plan['release'], 'images': plan['images'], 'plan': plan, 'entries': [],
               'advances': False, 'complete': False, 'classification': 'not_started'}
    result_path = directory / 'periodic-run.json'
    persist(result_path, receipt)
    started = time.monotonic()
    deadline = started + plan['budget_seconds']
    environment = tags.go_environment()
    for key in images.ROLE_ENV.values():
        environment.pop(key, None)
    environment.pop('CEPH_TEST_IMAGE', None)
    # Execute independent fixtures sequentially inside this one allocation.
    environment['CEPH_TEST_PARALLEL'] = '1'
    try:
        print('Periodic cell ' + plan['cell'] + '; source ' + plan['source_sha'], flush=True)
        # Fresh hosted engines usually pass. Only insufficient admission permits
        # the existing standard hosted cleanup; shared hosts are never modified.
        def headroom(minimum):
            observed = runner_storage.storage_snapshot()
            selection.write(directory / 'storage-snapshot.json', observed)
            return (observed['layout']['verified'] and shutil.disk_usage(ROOT).free >= minimum
                    and all(fs['available_bytes'] >= minimum and not fs['below_mon_warn']
                            for fs in observed['filesystems']))
        if not headroom(MIN_FREE_BYTES):
            if os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted':
                runner([sys.executable, '.github/scripts/runner_storage.py', 'free', '--directory',
                        str(directory / 'runner-storage')], directory / 'storage.log', 180)
            if not headroom(MIN_FREE_BYTES):
                raise RuntimeError('environment_ineligible: insufficient runner disk headroom')
        prep_started = time.monotonic()
        modules = runner([sys.executable, '.github/scripts/tag_scenarios.py', 'modules', '--directory',
                          str(directory / 'modules')], directory / 'modules.log', 330, environment)
        if not modules['passed']:
            raise RuntimeError('module_preparation_failure')
        prepared = prepare_images(plan, directory / 'images', deadline)
        if not headroom(6 * (1 << 30)):
            raise RuntimeError('environment_ineligible: insufficient post-image headroom')
        receipt['prepared_images'] = prepared
        receipt['preparation_seconds'] = round(time.monotonic() - prep_started, 3)
        environment.update(prepared['prepared_environment'])
        # Compile receipts remain per profile; reuse only the same verified
        # binary for another selection/network of that profile in this run.
        binaries = {}
        for item in plan['entries']:
            record = {'id': item['id'], 'profile': item['profile'], 'tests': item['tests'], 'network': item['network'],
                      'native_started': False, 'classification': 'not_started', 'phases': {}}
            receipt['entries'].append(record)
            persist(result_path, receipt)
            if deadline - time.monotonic() < item['native_seconds'] + CLEANUP_RESERVE + 240:
                record['classification'] = 'budget_not_started'
                break
            if not headroom(6 * (1 << 30)):
                record['classification'] = 'environment_ineligible'
                receipt['advances'] = True
                break
            p = item['profile']
            work = directory / item['id']
            work.mkdir()
            compiled = binaries.get(p['id'], work / 'compile')
            snapshot = False
            try:
                if p['id'] not in binaries:
                    record['phases']['compile'] = runner([sys.executable, '.github/scripts/tag_scenarios.py', 'compile',
                        '--category', p['category'], '--batch', p['batch'], '--package', p['package'],
                        '--directory', str(compiled)], work / 'compile.log', 210, environment)
                    if not record['phases']['compile']['passed']:
                        record['classification'] = 'compile_failure'
                        receipt['advances'] = True
                        continue
                    binaries[p['id']] = compiled
                snapshot_report = cleanup.snapshot(work / 'cleanup')
                if not snapshot_report['passed']:
                    raise RuntimeError('Docker baseline unavailable')
                snapshot = True
                args = [sys.executable, '.github/scripts/in_test_images.py', '--compile', str(compiled),
                        '--tests', ','.join(item['tests']), '--release', plan['release'],
                        '--roles', p['image_roles'], '--directory', str(work / 'test-images')]
                if 'reference' in plan['images']:
                    args += ['--discovery']
                record['phases']['test_images'] = runner(args, work / 'test-images.log', 300, environment)
                if not record['phases']['test_images']['passed']:
                    record['classification'] = 'image_preparation_failure'
                    report_path = work / 'test-images/summary.json'
                    if report_path.exists():
                        image_report = json.loads(report_path.read_text())
                        record['image_preparation'] = image_report
                        if image_report.get('classification') == 'contract_inapplicable':
                            record['classification'] = 'contract_inapplicable'
                    receipt['advances'] = True
                    raise RuntimeError('test image preparation failed')
                for reference, name in helper_names_from(plan).items():
                    actual = read_command(['docker', 'image', 'inspect', reference])
                    if actual.returncode or json.loads(actual.stdout)[0]['Id'] != prepared['images'][name]['image_id']:
                        raise RuntimeError('helper image alias changed after plan preparation')
                if deadline - time.monotonic() < item['native_seconds'] + CLEANUP_RESERVE:
                    record['classification'] = 'budget_not_started'
                    break
                environment['CEPH_TEST_NETWORK'] = item['network']
                record['native_admitted'] = True
                persist(result_path, receipt)  # durable checkpoint BEFORE starting native work
                record['phases']['native'] = runner([sys.executable, '.github/scripts/periodic_execute.py', 'native',
                        '--plan', str(plan_path), '--entry', item['id'], '--directory', str(work / 'native'),
                        '--compile', str(compiled)], work / 'execution.log', item['native_seconds'] + 30, environment)
                record['native_phase_finished'] = True
                native_path = work / 'native/report.json'
                report = json.loads(native_path.read_text()) if native_path.exists() else {}
                record['native'] = report
                record['classification'] = (report.get('classification', 'incomplete')
                                            if record['phases']['native']['passed'] else
                                            'timeout' if record['phases']['native']['timed_out'] else
                                            report.get('classification', 'native_failure'))
                if record['classification'] == 'passed' and (not record['phases']['native']['passed']
                                                             or report.get('passed') is not True):
                    record['classification'] = 'native_failure'
            except InterruptedError:
                raise
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                if record['classification'] == 'not_started':
                    record['classification'] = 'preparation_failure'
                record['error'] = type(error).__name__
                receipt['advances'] = True
            finally:
                marker = work / 'native/started.json'
                if marker.exists():
                    started_record = json.loads(marker.read_text())
                    if started_record.get('tests') == sorted(item['tests']):
                        record['native_started'] = True
                        receipt['advances'] = True
                if record.get('native_phase_finished') and not record['native_started']:
                    record['classification'] = 'preparation_failure'
                    receipt['advances'] = True
                if snapshot:
                    # A cleanup failure makes later native results unsafe.
                    checked = cleanup.check(work / 'cleanup', grace_seconds=30)
                    record['cleanup'] = checked
                persist(result_path, receipt)
            print(item['id'] + ': ' + record['classification'], flush=True)
            if snapshot and not record['cleanup']['passed']:
                receipt['classification'] = 'cleanup_failure'
                break
        receipt['complete'] = (len(receipt['entries']) == len(plan['entries']) and
                all(r['classification'] == 'passed' and r['native_started'] and r.get('cleanup', {}).get('passed')
                    for r in receipt['entries']))
        if receipt['classification'] != 'cleanup_failure':
            receipt['classification'] = 'passed' if receipt['complete'] else 'incomplete_or_failed'
    except InterruptedError:
        receipt['classification'] = 'interrupted'
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt['classification'] = 'preparation_failure'
        receipt['error'] = str(error)[:250]
        kind = str(error).split(':', 1)[0]
        if kind in ('environment_ineligible', 'artifact_unavailable', 'artifact_version_mismatch', 'artifact_resolution_failure'):
            receipt['classification'] = kind
        receipt['advances'] = True
    finally:
        receipt['elapsed_seconds'] = round(time.monotonic() - started, 3)
        receipt['unstarted_entries'] = [e['id'] for e in plan['entries'] if e['id'] not in
                {r['id'] for r in receipt['entries'] if r['native_started']}]
        persist(result_path, receipt)
        summary = os.environ.get('GITHUB_STEP_SUMMARY')
        if summary:
            with open(summary, 'a') as out:
                out.write('### Periodic sample outcome\n\n`' + receipt['classification'] + '`; source `'
                          + plan['source_sha'] + '`; release `' + plan['release'] + '`.\n\n'
                          + str(len(receipt['unstarted_entries'])) + ' planned entries never started native work. '
                          'Failed/interrupted attempts remain incomplete; later passes do not erase them.\n')
    return 0 if receipt['complete'] else 1


def helper_names_from(plan):
    return {reference: 'helper-' + str(number) for number, reference in enumerate(plan.get('test_images', {}))}


def interrupted(signum, frame):
    raise InterruptedError('periodic execution interrupted')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('execute', 'native'))
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--entry')
    parser.add_argument('--compile', type=Path)
    parser.add_argument('--trial', action='store_true', help='Record a local working-tree trial rather than a verified Git checkout')
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    if args.command == 'execute':
        return execute(args.plan, args.directory, args.trial)
    plan = json.loads(args.plan.read_text())
    item = next(e for e in plan['entries'] if e['id'] == args.entry)
    return native(plan, item, args.directory, args.compile)


if __name__ == '__main__':
    raise SystemExit(main())
