#!/usr/bin/env python3
"""Source-owned, budgeted periodic samples; never a supported-release guarantee."""
import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time

import run_image_matrix as images
import tag_scenarios as tags

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 'ceph-periodic-sample/v1'
DAILY_CRON = '41 4 * * *'
BUDGET_SECONDS = 110 * 60
RUNNER_MINUTES = {'plan': 15, 'sample_including_upload': 120, 'history': 15}
NATIVE_SECONDS = 20 * 60
GROUP_SECONDS = 45 * 60
CORE = ('TestClusterLifecycle', 'TestRBDLifecycle', 'TestCephFSFilesystem',
        'TestRGWS3', 'TestClientIdentities', 'TestCephFSOSDRecovery')
SHA = re.compile(r'[a-f0-9]{40}\Z')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def patch_inventory():
    data = json.loads((ROOT / '.github/ceph-discovery-images.json').read_text())
    if data.get('schema') != 1 or data.get('scope') != 'patch_discovery_not_support':
        raise ValueError('invalid discovery image inventory')
    for release, row in data['releases'].items():
        if not re.fullmatch(r'\d+\.\d+\.\d+', release):
            raise ValueError('invalid discovery release')
        if row['eligibility'] == 'available':
            if (not re.fullmatch(re.escape(images.REGISTRY) + r'@sha256:[a-f0-9]{64}', row['reference'])
                    or set(row['platforms']) != {'linux/amd64', 'linux/arm64'}
                    or any(images.DIGEST.fullmatch(d) is None for d in row['platforms'].values())):
                raise ValueError('invalid discovery image identity')
        elif row['eligibility'] != 'artifact_unavailable':
            raise ValueError('unknown image eligibility')
    return data['releases']


def image_identity(release, discovery):
    inventory = patch_inventory()
    if discovery:
        return inventory[release]
    return {'eligibility': 'available', 'layout': 'roles',
            'references': images.image_references('official', 'roles', release),
            'cpu_baseline': inventory.get(release, {}).get('cpu_baseline', {})}


def entry(profile, tests, network, core=False):
    tests = sorted(tests)
    identity = {'profile': profile['id'], 'tags': profile['tags'], 'tests': tests, 'network': network}
    return {'id': profile['id'] + '-' + digest(identity)[:10], 'profile': profile,
            'tests': tests, 'network': network, 'core': core,
            'native_seconds': min(600 if core else NATIVE_SECONDS, tags.duration_seconds(profile['timeout']))}


def scope(catalog):
    """Every required non-environment profile/mode remains in the extension inventory.

    Core tests are small samples; they never replace complete batch coverage.
    Group composition is source-owned, not changed by today's measured timings.
    """
    plan = tags.discover(catalog)
    core, found = [], set()
    for profile in plan['include']:
        tests = tags.selected_tests(catalog, profile['package'], profile['tags'])
        chosen = set(tests) & set(CORE)
        if chosen:
            if found & chosen:
                raise ValueError('core test has multiple owners')
            found |= chosen
            network = (profile.get('networks') or [''])[0] if chosen & set(profile.get('network_tests', [])) else ''
            core.append(entry(profile, chosen, network, True))
    if found != set(CORE):
        raise ValueError('core test inventory changed: ' + str(sorted(set(CORE) - found)))
    hints = json.loads((ROOT / '.github/periodic-costs.json').read_text())['native_seconds']
    groups, current, estimated, category = [], [], 0, None
    for cell in tags.network_matrix([p for p in plan['include'] if p['requires_ceph']], 'all'):
        base = next(p for p in plan['include'] if p['id'] == cell['id'].removesuffix('-' + cell['network'])
                    or p['id'] == cell['id'])
        tests = cell['tests'].split(',') if cell['tests'] else tags.selected_tests(catalog, base['package'], base['tags'])
        item = entry(base, tests, cell['network'])
        cost = max(60, hints.get(base['id'], 600)) + 90
        if current and (estimated + cost > GROUP_SECONDS or category != base['category']):
            groups.append({'id': category + '-' + digest([e['id'] for e in current])[:12],
                           'entries': current, 'estimated_seconds': estimated})
            current, estimated = [], 0
        category = base['category']
        current.append(item)
        estimated += cost
    if current:
        groups.append({'id': category + '-' + digest([e['id'] for e in current])[:12],
                       'entries': current, 'estimated_seconds': estimated})
    all_ids = [e['id'] for g in groups for e in g['entries']]
    if len(all_ids) != len(set(all_ids)):
        raise ValueError('duplicate extension cell')
    return core, groups


def validate_receipt(receipt):
    if (receipt.get('schema') != SCHEMA or not SHA.fullmatch(receipt.get('source_sha', ''))
            or receipt.get('receipt_sha256') != digest({k: v for k, v in receipt.items() if k != 'receipt_sha256'})
            or not isinstance(receipt.get('entries'), list)
            or receipt.get('mode') not in ('rotate', 'discovery', 'targeted')
            or not re.fullmatch(r'\d+-\d+', receipt.get('execution', ''))):
        raise ValueError('invalid periodic sample receipt')
    dt.datetime.fromisoformat(receipt['selected_at'])
    if not isinstance(receipt.get('advances'), bool) or not isinstance(receipt.get('complete'), bool):
        raise ValueError('receipt lacks attempt/completion state')
    return receipt


def receipts(directory):
    return [validate_receipt(json.loads(p.read_text())) for p in sorted((Path(directory) / 'periodic').glob('*.json'))]


def choose(candidates, history, mode):
    attempts = {}
    relevant = sorted((r for r in history if r['mode'] == mode and r['advances']),
                      key=lambda r: (r['selected_at'], r['execution']))
    for row in relevant:
        attempts[row['cell']] = row['selected_at']
    # One new-patch priority turn, followed by oldest attempt. Constant arrival
    # of new patches cannot consume consecutive discovery slots forever.
    new = [c for c in candidates if c['id'] not in attempts]
    if mode == 'discovery' and new and (not relevant or relevant[-1]['reason'] != 'new_patch'):
        return max(new, key=lambda c: tuple(map(int, c['release'].split('.')))), 'new_patch'
    return min(enumerate(candidates), key=lambda pair: (attempts.get(pair[1]['id'], ''), pair[0]))[1], 'oldest_attempt'


def make_plan(catalog, history, source_sha, mode, now, release='', profiles='', tests='', network=''):
    if not SHA.fullmatch(source_sha):
        raise ValueError('source must be an exact commit SHA')
    core, groups = scope(catalog)
    if mode == 'auto':
        mode = 'discovery' if now.weekday() in (1, 4) else 'rotate'
    if mode not in ('rotate', 'discovery', 'targeted'):
        raise ValueError('unknown bounded execution mode')
    candidates = []
    if mode == 'rotate':
        for group in groups:
            for version in images.RELEASES:
                candidates.append({'id': version + '/' + group['id'], 'release': version, 'entries': core + group['entries']})
    elif mode == 'discovery':
        for version in patch_inventory():
            # The same compact operational core runs on historical patches;
            # one source-directed characterization supplements its boundary.
            entries = list(core)
            if version in ('19.2.2', '19.2.3'):
                optional = tags.discover_optional(catalog)
                profile = next(p for p in optional['include'] if p['batch'] == 'discovery_schedule')
                entries.append(entry(profile, ['TestDiscoveryTrashScheduleBoundary'], ''))
            candidates.append({'id': version + '/patch-core', 'release': version, 'entries': entries})
    else:
        if release not in patch_inventory() or not profiles:
            raise ValueError('targeted requires a catalogued release and explicit profile IDs')
        required = tags.discover(catalog)['include'] + tags.discover_optional(catalog)['include']
        selected = profiles.split(',')
        if len(set(selected)) != len(selected) or set(selected) - {p['id'] for p in required}:
            raise ValueError('unknown or duplicate targeted profile')
        entries = []
        for profile in required:
            if profile['id'] not in selected:
                continue
            full = tags.selected_tests(catalog, profile['package'], profile['tags'])
            chosen = [t for t in tests.split(',') if t in full] if tests else full
            if not chosen or (network and network not in profile.get('networks', [])):
                raise ValueError('targeted tests or network do not belong to the profile')
            if network and not set(chosen) <= set(profile.get('network_tests', [])):
                raise ValueError('an explicit network requires mode-declaring tests')
            default = (profile.get('networks') or [''])[0] if set(chosen) & set(profile.get('network_tests', [])) else ''
            entries.append(entry(profile, chosen, network or default))
        if tests and set(tests.split(',')) != {t for e in entries for t in e['tests']}:
            raise ValueError('targeted test selection contains unknown tests')
        candidates = [{'id': release + '/targeted-' + digest(entries)[:12], 'release': release, 'entries': entries}]
    selected, reason = choose(candidates, history, mode)
    identity = image_identity(selected['release'], mode == 'discovery' or selected['release'] not in images.RELEASES)
    # Duplicate core/extension entries retain full extension intent; identical
    # selections need only one execution, marked as both via coverage inventory.
    entries = list({e['id']: e for e in selected['entries']}.values())
    previous = [r for r in history if r['cell'] == selected['id'] and r['complete']]
    plan = {'schema': SCHEMA, 'mode': mode, 'cell': selected['id'], 'reason': reason,
            'source_sha': source_sha, 'catalogue_sha256': digest(catalog), 'selected_at': now.isoformat(),
            'release': selected['release'], 'images': identity, 'platform': 'linux/amd64',
            'entries': entries, 'budget_seconds': BUDGET_SECONDS,
            'maximum_runner_minutes': sum(RUNNER_MINUTES.values()),
            'last_complete': max((r['selected_at'] for r in previous), default=None),
            'scope': 'sampled_compatibility_not_support', 'candidate_cells': len(candidates),
            'omitted_cells': len(candidates) - 1, 'extension_groups': len(groups),
            'expected_revisit_weeks': (math.ceil(len(candidates) / (5 if mode == 'rotate' else 2))
                                       if mode != 'targeted' else None)}
    plan['plan_sha256'] = digest(plan)
    return plan


def validate_plan(plan, catalog):
    if (plan.get('schema') != SCHEMA or not SHA.fullmatch(plan.get('source_sha', ''))
            or plan.get('plan_sha256') != digest({k: v for k, v in plan.items() if k != 'plan_sha256'})
            or plan.get('catalogue_sha256') != digest(catalog)
            or plan.get('maximum_runner_minutes') != sum(RUNNER_MINUTES.values())
            or plan.get('budget_seconds') != BUDGET_SECONDS or not plan.get('entries')):
        raise ValueError('invalid, stale or unbounded periodic plan')
    expected_images = image_identity(plan['release'], 'reference' in plan['images'])
    if plan['images'] != expected_images:
        raise ValueError('planned images differ from the source pins')
    profiles = {p['id']: p for p in tags.discover(catalog)['include'] + tags.discover_optional(catalog)['include']}
    if len({e['id'] for e in plan['entries']}) != len(plan['entries']):
        raise ValueError('duplicate planned entry')
    for item in plan['entries']:
        profile = profiles.get(item['profile']['id'])
        if profile != item['profile'] or not item['tests']:
            raise ValueError('planned profile differs from source inventory')
        full = tags.selected_tests(catalog, profile['package'], profile['tags'])
        if (not set(item['tests']) <= set(full) or len(set(item['tests'])) != len(item['tests'])
                or (profile['checker'] != 'none' and set(item['tests']) != set(full))
                or item != entry(profile, item['tests'], item['network'], item['core'])):
            raise ValueError('planned selection violates the source contract')
        if item['network'] and item['network'] not in profile.get('networks', []):
            raise ValueError('planned network is not declared')
        modes = set(profile.get('network_tests', []))
        if (set(item['tests']) & modes and not item['network']
                or item['network'] and item['network'] != (profile.get('networks') or [''])[0]
                and not set(item['tests']) <= modes):
            raise ValueError('planned selection changes the declared network scope')
    for reference, pinned in plan.get('test_images', {}).items():
        if (not reference or reference.startswith('-') or any(c.isspace() for c in reference)
                or not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', pinned)):
            raise ValueError('invalid planned helper image pin')


def freeze_test_images(plan):
    """Resolve Ryuk and source-proven additional images before native work.

    A failed analysis/resolution stays an attributable preparation failure in
    the plan, rather than disappearing or selecting a different cohort.
    """
    import in_test_images
    import periodic_campaign
    deadline = time.monotonic() + 180
    def run(argv, **kwargs):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('image identity preparation deadline')
        kwargs['timeout'] = min(kwargs.get('timeout', 60), remaining)
        return subprocess.run(argv, **kwargs)
    plan['test_images'] = {}
    try:
        listed = run(['go', 'list', '-m', '-f', '{{.Dir}}', in_test_images.TESTCONTAINERS],
                     capture_output=True, text=True)
        if listed.returncode:
            raise ValueError('cannot locate the source-pinned reaper')
        references = {in_test_images.reaper_reference(listed.stdout)}
        for item in plan['entries']:
            p = item['profile']
            result = run(in_test_images.analysis_command(p['package'], p['tags'], item['tests']),
                         capture_output=True, text=True, timeout=60)
            if result.returncode:
                raise ValueError('test image analysis failed')
            uses = in_test_images.validate_analysis(json.loads(result.stdout), p['package'], item['tests'])
            planned, _ = in_test_images.plan_images(uses, plan['release'], images.parse_roles(p['image_roles']),
                                                   True, 'reference' in plan['images'])
            references.update(image['reference'] for image in planned)
        for reference in sorted(references):
            manifest = periodic_campaign.resolve(reference, run)
            if '@' in reference and reference.rsplit('@', 1)[1] != manifest:
                raise ValueError('source-pinned helper resolved to another digest')
            plan['test_images'][reference] = reference.split('@')[0] + '@' + manifest
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        plan['test_images_error'] = 'helper_image_identity_unavailable'
    plan.pop('plan_sha256', None)
    plan['plan_sha256'] = digest(plan)
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('auto', 'rotate', 'discovery', 'targeted'), default='auto')
    parser.add_argument('--history', type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--release', default='')
    parser.add_argument('--profiles', default='')
    parser.add_argument('--tests', default='')
    parser.add_argument('--network', choices=('', 'default', 'host', 'bridge'), default='')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--freeze-images', action='store_true', help='Freeze source-proven helper images as well as Ceph pins')
    args = parser.parse_args()
    plan = make_plan(tags.load_catalog(), receipts(args.history), args.source, args.mode,
                     dt.datetime.now(dt.timezone.utc), args.release, args.profiles, args.tests,
                     '' if args.network == 'default' else args.network)
    if args.freeze_images:
        freeze_test_images(plan)
    write(args.output, plan)
    print(canonical({k: plan[k] for k in ('mode', 'cell', 'reason', 'candidate_cells', 'expected_revisit_weeks')}))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write('### Periodic sample plan\n\nSource `' + plan['source_sha'] + '`; cell `' + plan['cell']
                      + '`; reason `' + plan['reason'] + '`.\n\n'
                      + str(plan['omitted_cells']) + ' cells intentionally omitted. This is sampled compatibility, '
                      'not complete current-main coverage or a release support guarantee.\n\n'
                      '| Profile | Tests | Network |\n| --- | --- | --- |\n')
            for e in plan['entries']:
                out.write('| ' + e['profile']['id'] + ' | ' + ', '.join(e['tests']) + ' | ' + (e['network'] or 'default') + ' |\n')


if __name__ == '__main__':
    main()
