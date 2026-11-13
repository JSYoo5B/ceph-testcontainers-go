#!/usr/bin/env python3
"""Freeze a small random periodic sample for flaky/patch discovery, not certification."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random
import re
import subprocess
import time

import run_image_matrix as images
import tag_scenarios as tags

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 'ceph-periodic-sample/v1'
BUNDLE_SCHEMA = SCHEMA + '/bundle'
DAILY_CRON = '41 4 * * *'
BUDGET_SECONDS = 110 * 60
RUNNER_MINUTES = {'plan': 15, 'sample_including_upload': 120, 'history': 15}
NATIVE_SECONDS = 20 * 60
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
            if (not re.fullmatch('(?:' + re.escape(images.REGISTRY) + r'|quay\.io/ceph/ceph)@sha256:[a-f0-9]{64}', row['reference'])
                    or set(row['platforms']) != {'linux/amd64', 'linux/arm64'}
                    or any(images.DIGEST.fullmatch(d) is None for d in row['platforms'].values())):
                raise ValueError('invalid discovery image identity')
        elif row['eligibility'] != 'artifact_unavailable':
            raise ValueError('unknown image eligibility')
    return data['releases']

def image_identity(release, discovery, entries=()):
    """The image identity of a plan. A historical aggregate serves only the
    five dependency roles: whether one ships the modules of an optional role
    is not established, so a plan whose entries declare one is not eligible
    on it rather than run on a guessed image."""
    inventory = patch_inventory()
    if discovery:
        row = dict(inventory[release])
        optional = sorted({role for item in entries if item['profile']['image_roles']
                           for role in images.parse_roles(item['profile']['image_roles'])
                           if role in images.OPTIONAL_ROLE_ENV})
        if optional and row['eligibility'] == 'available':
            row.update(eligibility='artifact_unavailable', unavailable_roles=optional)
        return row
    return {'eligibility': 'available', 'layout': 'roles',
            'references': images.image_references('official', 'roles', release, images.published_roles('official')),
            'cpu_baseline': inventory.get(release, {}).get('cpu_baseline', {})}

def entry(profile, tests, network, core=False):
    tests = sorted(tests)
    identity = {'profile': profile['id'], 'tags': profile['tags'], 'tests': tests, 'network': network}
    return {'id': profile['id'] + '-' + digest(identity)[:10], 'profile': profile,
            'tests': tests, 'network': network, 'core': core,
            'native_seconds': min(600 if core else NATIVE_SECONDS, tags.duration_seconds(profile['timeout']))}

def validate_receipt(receipt):
    if (receipt.get('schema') != SCHEMA or not SHA.fullmatch(receipt.get('source_sha', ''))
            or receipt.get('receipt_sha256') != digest({k: v for k, v in receipt.items() if k != 'receipt_sha256'})
            or not isinstance(receipt.get('entries'), list)
            or receipt.get('mode') not in ('rotate', 'discovery', 'targeted')
            or not re.fullmatch(r'\d+-\d+', receipt.get('execution', ''))
            or not re.fullmatch(r'\d+\.\d+\.\d+', receipt.get('release', ''))):
        raise ValueError('invalid periodic sample receipt')
    dt.datetime.fromisoformat(receipt['selected_at'])
    if not isinstance(receipt.get('advances'), bool) or not isinstance(receipt.get('complete'), bool):
        raise ValueError('receipt lacks attempt/completion state')
    return receipt

def validate_plan(plan, catalog):
    if (plan.get('schema') != SCHEMA or not SHA.fullmatch(plan.get('source_sha', ''))
            or plan.get('plan_sha256') != digest({k: v for k, v in plan.items() if k != 'plan_sha256'})
            or plan.get('catalogue_sha256') != digest(catalog)
            or plan.get('maximum_runner_minutes') != sum(RUNNER_MINUTES.values())
            or plan.get('budget_seconds') != BUDGET_SECONDS or not plan.get('entries')):
        raise ValueError('invalid, stale or unbounded periodic plan')
    expected_images = image_identity(plan['release'], 'reference' in plan['images'], plan['entries'])
    if plan['images'] != expected_images:
        raise ValueError('planned images differ from the source-declared references')
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

def freeze_test_images(plan, resolved=None, deadline=None):
    """Resolve Ryuk and source-proven additional images before native work.

    A failed analysis/resolution stays an attributable preparation failure in
    the plan, rather than disappearing or selecting a different cohort.
    """
    import in_test_images
    import role_image_pins
    deadline = time.monotonic() + 180 if deadline is None else deadline
    resolved = {} if resolved is None else resolved
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
            result = run(in_test_images.analysis_command(p['package'], p['tags'], item['tests'], plan['platform']),
                         capture_output=True, text=True, timeout=60)
            if result.returncode:
                raise ValueError('test image analysis failed')
            uses = in_test_images.validate_analysis(json.loads(result.stdout), p['package'], item['tests'])
            planned, _ = in_test_images.plan_images(uses, plan['release'], True, 'reference' in plan['images'])
            references.update(image['reference'] for image in planned)
        for reference in sorted(references):
            if reference not in resolved:
                resolved[reference] = role_image_pins.resolve(reference, run)
            manifest = resolved[reference]
            if '@' in reference and reference.rsplit('@', 1)[1] != manifest:
                raise ValueError('source-pinned helper resolved to another digest')
            plan['test_images'][reference] = reference.split('@')[0] + '@' + manifest
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        plan['test_images_error'] = 'helper_image_identity_unavailable'
    plan.pop('plan_sha256', None)
    plan['plan_sha256'] = digest(plan)
    return plan


def selected_entry(catalog, profile, chosen=None, network='', core=False):
    full = tags.selected_tests(catalog, profile['package'], profile['tags'])
    chosen = full if chosen is None else sorted(chosen)
    modes = profile.get('networks') or ['']
    if network and network not in modes:
        raise ValueError('network is not declared by this profile')
    if network and network != modes[0]:
        chosen = sorted(set(chosen) & set(profile.get('network_tests', [])))
    if not chosen:
        raise ValueError('empty sampled parent selection')
    if not network and set(chosen) & set(profile.get('network_tests', [])):
        network = modes[0]
    return entry(profile, chosen, network, core)


def sample_plan(catalog, source_sha, mode, now, release='', profiles='', tests='', network='', seed=''):
    if not SHA.fullmatch(source_sha):
        raise ValueError('source must be an exact commit SHA')
    if mode == 'auto':
        mode = 'discovery' if now.weekday() in (1, 4) else 'rotate'
    if mode not in ('rotate', 'discovery', 'targeted'):
        raise ValueError('unknown sample mode')
    seed = seed or source_sha + ':' + os.environ.get('GITHUB_RUN_ID', 'local') + ':' + os.environ.get('GITHUB_RUN_ATTEMPT', '1')
    rng = random.Random(seed)
    required = tags.discover(catalog)['include']
    optional = tags.discover_optional(catalog)['include']
    if mode == 'targeted':
        if release not in patch_inventory() or not profiles:
            raise ValueError('targeted requires a catalogued patch and profile IDs')
        identifiers = profiles.split(',')
        available = {p['id']:p for p in required + optional}
        if len(set(identifiers)) != len(identifiers) or set(identifiers) - set(available):
            raise ValueError('unknown or duplicate profile IDs')
        entries = []
        for identifier in identifiers:
            profile = available[identifier]
            full = tags.selected_tests(catalog, profile['package'], profile['tags'])
            chosen = sorted(set(tests.split(',')) & set(full)) if tests else full
            if network and not set(chosen) <= set(profile.get('network_tests', [])):
                raise ValueError('explicit network requires mode-declaring tests')
            entries.append(selected_entry(catalog, profile, chosen, network))
        if tests and set(tests.split(',')) != {t for e in entries for t in e['tests']}:
            raise ValueError('targeted test selection names unknown parents')
        versions = [release]
    else:
        pool = list(patch_inventory()) if mode == 'discovery' else list(images.RELEASES)
        versions = rng.sample(pool, min(len(pool), rng.randint(2, 3)))
        if mode == 'rotate':
            candidates = [p for p in required if p['requires_ceph'] and p['category'] in ('short','topology','multicluster','recovery')]
            chosen_profiles = rng.sample(candidates, min(len(candidates), rng.randint(2, 3)))
            entries = [selected_entry(catalog, p, network=rng.choice(p.get('networks') or [''])) for p in chosen_profiles]
        else:
            core = []
            for name in CORE:
                owners = [p for p in required if name in tags.selected_tests(catalog,p['package'],p['tags'])]
                if len(owners) != 1:
                    raise ValueError('discovery core test has no unique owner')
                core.append(selected_entry(catalog, owners[0], [name], core=True))
            entries = rng.sample(core, rng.randint(2, 3))
    runs = []
    for version in versions:
        chosen = list(entries)
        if mode == 'discovery' and version in ('19.2.2','19.2.3'):
            p = next(p for p in required if p['batch'] == 'discovery_schedule')
            chosen.append(selected_entry(catalog,p,['TestDiscoveryTrashScheduleBoundary']))
        if mode == 'discovery' and version in ('19.2.5','19.2.6','19.2.7'):
            name = 'TestMultiClusterRGWMultisite'
            p = next(p for p in required if name in tags.selected_tests(catalog,p['package'],p['tags']))
            chosen.append(selected_entry(catalog,p,[name]))
        plan = {'schema':SCHEMA,'mode':mode,'source_sha':source_sha,'selected_at':now.isoformat(),
                'cell':version+'/'+digest([e['id'] for e in chosen])[:12],
                'reason':'explicit_target' if mode == 'targeted' else 'random_sample',
                'release':version,'images':image_identity(version,mode == 'discovery' or version not in images.RELEASES,chosen),
                'platform':'linux/amd64','entries':chosen,'catalogue_sha256':digest(catalog),
                'budget_seconds':BUDGET_SECONDS,'maximum_runner_minutes':sum(RUNNER_MINUTES.values()),
                'scope':'flaky_discovery_sample_not_support'}
        plan['plan_sha256'] = digest(plan)
        runs.append(plan)
    bundle = {'schema':BUNDLE_SCHEMA,'source_sha':source_sha,'mode':mode,'seed':seed,
              'budget_seconds':BUDGET_SECONDS,'maximum_runner_minutes':sum(RUNNER_MINUTES.values()),
              'selected_at':now.isoformat(),'runs':runs,'scope':'flaky_discovery_sample_not_support'}
    bundle['plan_sha256'] = digest(bundle)
    return bundle


def validate_bundle(bundle,catalog):
    if (bundle.get('schema') != BUNDLE_SCHEMA or not SHA.fullmatch(bundle.get('source_sha',''))
            or bundle.get('plan_sha256') != digest({k:v for k,v in bundle.items() if k != 'plan_sha256'})
            or bundle.get('budget_seconds') != BUDGET_SECONDS or not 1 <= len(bundle.get('runs',[])) <= 3
            or len({p['release'] for p in bundle['runs']}) != len(bundle['runs'])):
        raise ValueError('invalid frozen periodic sample')
    helpers = {}
    for plan in bundle['runs']:
        if plan['source_sha'] != bundle['source_sha'] or plan['mode'] != bundle['mode']:
            raise ValueError('sample source/mode differs between versions')
        validate_plan(plan,catalog)
        for ref,pin in plan.get('test_images',{}).items():
            if helpers.get(ref,pin) != pin:
                raise ValueError('a helper image changed within this execution')
            helpers[ref] = pin


def validate_bundle_receipt(receipt):
    if (receipt.get('schema') != BUNDLE_SCHEMA or not SHA.fullmatch(receipt.get('source_sha',''))
            or not re.fullmatch(r'\d+-\d+',receipt.get('execution',''))
            or receipt.get('receipt_sha256') != digest({k:v for k,v in receipt.items() if k != 'receipt_sha256'})
            or not isinstance(receipt.get('complete'),bool) or not isinstance(receipt.get('versions'),list)):
        raise ValueError('invalid periodic bundle receipt')
    return receipt


def freeze_bundle(bundle):
    cache,deadline = {},time.monotonic()+180
    for plan in bundle['runs']:
        freeze_test_images(plan,cache,deadline)
    bundle.pop('plan_sha256',None)
    bundle['plan_sha256'] = digest(bundle)
    return bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=('auto','rotate','discovery','targeted'),default='auto')
    parser.add_argument('--source',required=True)
    parser.add_argument('--release',default='')
    parser.add_argument('--profiles',default='')
    parser.add_argument('--tests',default='')
    parser.add_argument('--network',choices=('','default','host','bridge'),default='')
    parser.add_argument('--seed',default='',help='Recorded reproducible random draw; normally derived from run identity')
    parser.add_argument('--freeze-images',action='store_true')
    parser.add_argument('--output',required=True,type=Path)
    args = parser.parse_args()
    catalog = tags.load_catalog()
    bundle = sample_plan(catalog,args.source,args.mode,dt.datetime.now(dt.timezone.utc),args.release,
                         args.profiles,args.tests,'' if args.network == 'default' else args.network,args.seed)
    if args.freeze_images:
        freeze_bundle(bundle)
    validate_bundle(bundle,catalog)
    write(args.output,bundle)
    print(canonical({'mode':bundle['mode'],'versions':[p['release'] for p in bundle['runs']],'seed':bundle['seed']}))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary,'a') as out:
            out.write('### Frozen flaky/discovery sample\n\nSource `'+bundle['source_sha']+'`; seed `'+bundle['seed']
                      +'`. This random sample does not certify complete coverage or release support.\n\n'
                      +'| Release | Profile | Tests | Network |\n| --- | --- | --- | --- |\n')
            for plan in bundle['runs']:
                for e in plan['entries']:
                    out.write('| '+plan['release']+' | '+e['profile']['id']+' | '+', '.join(e['tests'])+' | '+(e['network'] or 'default')+' |\n')


if __name__ == '__main__':
    main()
