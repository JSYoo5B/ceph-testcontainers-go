"""Selection/failure controls for sampled periodic work; no native success claims."""
import copy
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import periodic_execute as executor
import periodic_selection as sample
import scenario_history
import tag_scenarios as tags

NOW = dt.datetime(2026, 11, 4, tzinfo=dt.timezone.utc)
SHA = 'a' * 40


def history(cell, mode='rotate', date='2026-11-01T00:00:00+00:00', advances=True,
            complete=False, reason='random_sample'):
    row = {'schema': sample.SCHEMA, 'source_sha': SHA, 'execution': '1-1',
           'mode': mode, 'cell': cell, 'release': '20.2.4', 'selected_at': date, 'advances': advances,
           'complete': complete, 'reason': reason, 'entries': []}
    row['receipt_sha256'] = sample.digest(row)
    return row


class SelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = tags.load_catalog()

    def bundle(self, mode='rotate', **kwargs):
        return sample.sample_plan(self.catalog, SHA, mode, NOW, seed='control', **kwargs)

    def plan(self, mode='rotate', **kwargs):
        return self.bundle(mode, **kwargs)['runs'][0]

    def test_published_role_plans_supply_exactly_the_declared_roles(self):
        order = list(executor.images.DECLARABLE_ROLE_ENV)

        def declared(plan):
            roles = {'essential'} | {role for e in plan['entries'] for role in e['profile']['image_roles'].split(',') if role}
            return [role for role in order if role in roles]

        def environment(plan):
            prepared = {role: {'image_id': 'sha256:' + str(number) * 64}
                        for number, role in enumerate(plan['images']['references'], 1)}
            return executor.role_environment(plan, prepared), prepared

        # A plan whose entries need no optional role references none: its
        # tag is neither pulled nor able to fail the sample.
        plan = self.plan()
        self.assertEqual(list(plan['images']['references']), declared(plan))
        self.assertNotIn('mgr-modules', plan['images']['references'])
        self.assertEqual(plan['images']['references'],
                         {role: executor.images.role_tag('official', plan['release'], role) for role in declared(plan)})
        env, _ = environment(plan)
        self.assertEqual(env['SCENARIO_IMAGE_ROLES'], ','.join(declared(plan)))
        self.assertNotIn('CEPH_TEST_MGR_MODULES_IMAGE', env)
        # A plan with an entry that declares the optional role gets it.
        mgr = next(p for p in tags.discover(self.catalog)['include'] if p['batch'] == 'mgr_image')
        plan = self.plan('targeted', release='20.2.4', profiles=mgr['id'], tests='TestManagerMetrics')
        self.assertEqual(list(plan['images']['references']), mgr['image_roles'].split(','))
        env, prepared = environment(plan)
        self.assertEqual(env['SCENARIO_IMAGE_ROLES'], mgr['image_roles'])
        self.assertEqual(env['CEPH_TEST_MGR_MODULES_IMAGE'], prepared['mgr-modules']['image_id'])
        sample.validate_plan(plan, self.catalog)
        # The references follow the entries: a plan edited to reference
        # another role set is refused.
        widened = json.loads(json.dumps(plan))
        widened['images']['references']['rgw'] = executor.images.role_tag('official', '20.2.4', 'rgw')
        widened['plan_sha256'] = sample.digest({k: v for k, v in widened.items() if k != 'plan_sha256'})
        with self.assertRaisesRegex(ValueError, 'planned images differ'):
            sample.validate_plan(widened, self.catalog)
        # A historical aggregate serves the five dependency roles and declares
        # no optional role, so a test that needs one fails instead of falling
        # back to the aggregate.
        aggregate = {'images': {'reference': 'r@sha256:' + 'f' * 64}}
        environment = executor.role_environment(aggregate, {'all': {'image_id': 'sha256:' + 'f' * 64}})
        self.assertEqual(environment['SCENARIO_IMAGE_ROLES'], 'essential,client,cephfs,rbd-aux,rgw')
        self.assertNotIn('CEPH_TEST_MGR_MODULES_IMAGE', environment)

    def test_draw_is_small_distinct_and_reproducible(self):
        for mode in ('rotate','discovery'):
            first = self.bundle(mode)
            self.assertEqual(first, self.bundle(mode))
            self.assertIn(len(first['runs']), (2,3))
            self.assertEqual(len({r['release'] for r in first['runs']}),len(first['runs']))
            self.assertTrue(all(2 <= len(r['entries']) <= 4 for r in first['runs']))
            sample.validate_bundle(first,self.catalog)
            other = sample.sample_plan(self.catalog,SHA,mode,NOW,seed='another draw')
            self.assertNotEqual(first['plan_sha256'],other['plan_sha256'])

    def test_schedule_has_five_rotation_and_two_discovery_days(self):
        # Only the weekday routing is under test; no native work is claimed.
        with mock.patch.object(sample.tags,'discover',wraps=tags.discover):
            modes = [sample.sample_plan(self.catalog,SHA,'auto',NOW+dt.timedelta(days=d),seed='control')['mode'] for d in range(7)]
        self.assertEqual((modes.count('rotate'),modes.count('discovery')),(5,2))

    def test_bundle_rejects_mixed_sources_duplicate_versions_and_budgets(self):
        original = self.bundle()
        for kind in ('source','duplicate','budget'):
            bad = copy.deepcopy(original)
            if kind == 'source':
                bad['source_sha'] = 'b'*40
            elif kind == 'duplicate':
                bad['runs'][1] = bad['runs'][0]
            else:
                bad['budget_seconds'] += 1
            bad['plan_sha256'] = sample.digest({k:v for k,v in bad.items() if k != 'plan_sha256'})
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                sample.validate_bundle(bad,self.catalog)

    def test_discovery_does_not_expand_ordinary_supported_releases(self):
        supported = tuple(sample.images.RELEASES)
        inventory = sample.patch_inventory()
        self.assertEqual(len(inventory), 15)
        self.assertNotIn('19.2.2', supported)
        self.assertNotIn('20.2.5', supported)
        self.assertEqual(inventory['20.2.5']['variant'], 'upstream_aggregate')
        plan = self.plan('discovery')
        self.assertIn('reference', plan['images'])
        self.assertEqual(tuple(sample.images.RELEASES), supported)
        self.assertEqual(plan['scope'], 'flaky_discovery_sample_not_support')

    def test_targeted_same_sha_keeps_requested_patch_and_tests(self):
        first = self.plan()
        e = next(e for e in first['entries'] if e['profile']['checker'] == 'none')
        plan = self.plan('targeted', release='19.2.2', profiles=e['profile']['id'], tests=e['tests'][0])
        self.assertEqual((plan['release'], plan['source_sha'], len(plan['entries'])), ('19.2.2', SHA, 1))
        self.assertEqual(plan['entries'][0]['tests'], [e['tests'][0]])
        sample.validate_plan(plan, self.catalog)
        for changed in ('TestAbsent', e['tests'][0] + ',TestAbsent'):
            with self.assertRaises(ValueError):
                self.plan('targeted', release='19.2.2', profiles=e['profile']['id'], tests=changed)

    def test_a_historical_aggregate_is_ineligible_for_an_optional_role(self):
        mgr = next(p for p in tags.discover(self.catalog)['include'] if p['batch'] == 'mgr_image')
        self.assertIn('mgr-modules', mgr['image_roles'].split(','))
        plan = self.plan('targeted', release='19.2.2', profiles=mgr['id'], tests='TestManagerMetrics,TestManagerPrometheusExporter')
        self.assertEqual(plan['images']['eligibility'], 'artifact_unavailable')
        self.assertEqual(plan['images']['unavailable_roles'], ['mgr-modules'])
        self.assertIn('reference', plan['images'])
        sample.validate_plan(plan, self.catalog)
        # The executor stops on the planned ineligibility before it prepares
        # any image or exports a role environment.
        info = mock.Mock(returncode=0, stdout=json.dumps({'OSType': 'linux', 'Architecture': 'x86_64'}))
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(executor, 'read_command', return_value=info), \
                mock.patch.object(executor, 'cpu_eligible', return_value=True), \
                mock.patch.object(executor, 'role_environment') as environment, \
                self.assertRaisesRegex(RuntimeError, 'artifact_unavailable: the historical aggregate supplies no mgr-modules role'):
            executor.prepare_images(plan, Path(directory) / 'images')
        environment.assert_not_called()
        # The same profile on a published release keeps its role images, and
        # a profile without an optional role stays eligible on the aggregate.
        published = self.plan('targeted', release='20.2.4', profiles=mgr['id'], tests='TestManagerMetrics')
        self.assertEqual(published['images']['eligibility'], 'available')
        self.assertIn('mgr-modules', published['images']['references'])
        other = next(e for e in self.plan()['entries'] if e['profile']['checker'] == 'none' and 'mgr-modules' not in e['profile']['image_roles'])
        historical = self.plan('targeted', release='19.2.2', profiles=other['profile']['id'], tests=other['tests'][0])
        self.assertEqual(historical['images']['eligibility'], 'available')
        self.assertNotIn('unavailable_roles', historical['images'])

    def test_plan_rejects_changed_source_images_selection_and_native_budget(self):
        original = self.plan()
        sample.validate_plan(original, self.catalog)
        for change in ('digest', 'selection', 'native_budget', 'images', 'total_budget'):
            bad = copy.deepcopy(original)
            if change == 'digest':
                bad['source_sha'] = 'main'
            elif change == 'selection':
                bad['entries'][0]['tests'] = ['TestAbsent']
            elif change == 'native_budget':
                bad['entries'][0]['native_seconds'] = 100000
            elif change == 'images':
                bad['images']['references'][next(iter(bad['images']['references']))] = 'moving:tag'
            else:
                bad['budget_seconds'] = 1000000
            bad['plan_sha256'] = sample.digest({k:v for k,v in bad.items() if k != 'plan_sha256'})
            with self.subTest(change=change), self.assertRaises(ValueError):
                sample.validate_plan(bad, self.catalog)
        with self.assertRaises(ValueError):
            sample.validate_plan(original, [])

    def test_helper_resolution_failure_remains_a_frozen_preparation_failure(self):
        import in_test_images
        import role_image_pins
        plan = self.plan()
        with mock.patch.object(sample.subprocess, 'run', return_value=mock.Mock(returncode=0, stdout='{}')), \
                mock.patch.object(in_test_images, 'reaper_reference', return_value='testcontainers/ryuk:v1'), \
                mock.patch.object(in_test_images, 'validate_analysis', return_value=[]), \
                mock.patch.object(in_test_images, 'plan_images', return_value=([], [])), \
                mock.patch.object(role_image_pins, 'resolve', side_effect=RuntimeError('registry unavailable')):
            frozen = sample.freeze_test_images(plan)
        self.assertEqual(frozen['test_images_error'], 'helper_image_identity_unavailable')
        sample.validate_plan(frozen, self.catalog)

    def test_sample_selection_is_refused_before_native_on_unknown_tests_or_expanded_budget(self):
        item = self.plan()['entries'][0]
        profile = item['profile']
        for selection, timeout in [('TestAbsent', 1), (item['tests'][0], 1000000)]:
            with mock.patch.object(tags.subprocess, 'Popen') as popen, self.assertRaises(ValueError):
                tags.run_profile(self.catalog, tags.discover(self.catalog), profile['category'], profile['batch'],
                                 profile['package'], Path('/never/create'), selected_tests_text=selection,
                                 timeout_seconds=timeout)
            popen.assert_not_called()

    def test_packed_measurement_does_not_invent_runner_wait_or_full_acceptance(self):
        import ci_measurements
        row = history('cell')
        row['elapsed_seconds'] = 10
        row['entries'] = [{'native_started': True, 'phases': {'native': {'elapsed_seconds': 3}}}]
        row['receipt_sha256'] = sample.digest({k:v for k,v in row.items() if k != 'receipt_sha256'})
        with tempfile.TemporaryDirectory() as directory:
            sample.write(Path(directory)/'periodic-run.json', row)
            report = ci_measurements.periodic_samples(directory)
        observed = report['samples'][0]
        self.assertIsNone(observed['runner_seconds'])
        self.assertIsNone(observed['runner_queue_seconds'])
        self.assertFalse(observed['complete'])
        self.assertEqual(observed['phases']['native'], 3)

    def test_history_rejects_corruption_and_conflicting_ingestion(self):
        row = history('cell')
        sample.validate_receipt(row)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = root/'incoming'
            incoming.mkdir()
            sample.write(incoming/'periodic-run.json', row)
            scenario_history.collect(root/'history', incoming)
            scenario_history.collect(root/'history', incoming)
            saved = json.loads((root/'history/summary.json').read_text())
            self.assertEqual(saved['rows'], 0)  # never full-profile acceptance evidence
            row['complete'] = True
            with self.assertRaises(ValueError):
                sample.validate_receipt(row)
            row['receipt_sha256'] = sample.digest({k:v for k,v in row.items() if k != 'receipt_sha256'})
            sample.write(incoming/'periodic-run.json', row)
            with self.assertRaises(ValueError):
                scenario_history.collect(root/'history', incoming)

    def test_versions_share_one_deadline_and_stop_after_dirty_cleanup(self):
        # This explicit leaf substitute checks coordinator admission only,
        # not Ceph, Docker cleanup, or native test success.
        for dirty in (False,True):
            bundle = self.bundle()
            calls = []
            def leaf(path,directory,trial,runner,deadline,binaries):
                plan = json.loads(Path(path).read_text())
                calls.append((deadline,id(binaries)))
                sample.write(Path(directory)/'periodic-run.json',{
                    'release':plan['release'],'complete':not dirty,
                    'classification':'cleanup_failure' if dirty else 'passed'})
                return int(dirty)
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                sample.write(root/'plan.json',bundle)
                with mock.patch.object(tags,'load_catalog',return_value=self.catalog), \
                        mock.patch.object(executor,'execute',side_effect=leaf):
                    code = executor.execute_bundle(root/'plan.json',root/'out',trial=True)
                receipt = json.loads((root/'out/periodic-bundle.json').read_text())
                sample.validate_bundle_receipt(receipt)
            self.assertEqual(code,int(dirty))
            self.assertEqual(len(set(calls)),1)
            self.assertEqual(len(calls),1 if dirty else len(bundle['runs']))
            self.assertEqual(len(receipt['unstarted_versions']),len(bundle['runs'])-len(calls))



class ExecutionControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = tags.load_catalog()
        cls.plan = sample.sample_plan(cls.catalog, SHA, 'rotate', NOW, seed='control')['runs'][0]

    def run_control(self, failure='', cleanup_pass=True, exhaust_after_images=False):
        """Fake commands test admission/checkpoints only; they run no Ceph."""
        calls, clock = [], [0.0]
        def fake(argv, log, seconds, env=None):
            phase = 'native' if 'native' in argv else 'test_images' if any('in_test_images.py' in s for s in argv) else 'compile' if 'compile' in argv else 'modules'
            calls.append(phase)
            if phase == 'test_images' and exhaust_after_images:
                clock[0] = (self.plan['budget_seconds'] - self.plan['entries'][0]['native_seconds']
                            - executor.CLEANUP_RESERVE + 1)
            if phase == 'native' and failure == 'interrupt_before_native':
                raise InterruptedError()
            if phase == 'native':
                path = Path(argv[argv.index('--directory') + 1])
                item = next(e for e in self.plan['entries'] if e['id'] == argv[argv.index('--entry') + 1])
                sample.write(path/'started.json', {'tests': sorted(item['tests'])})
                sample.write(path/'report.json', {'passed': True, 'classification': 'passed'})
            if failure == 'interrupt_' + phase:
                raise InterruptedError()
            return {'passed': phase != failure, 'exit_code': int(phase == failure), 'timed_out': False, 'elapsed_seconds': 1}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sample.write(root/'plan.json', self.plan)
            with mock.patch.object(tags, 'load_catalog', return_value=self.catalog), \
                    mock.patch.object(executor.time, 'monotonic', side_effect=lambda: clock[0]), \
                    mock.patch.object(executor.images, 'source_manifest', return_value={}), \
                    mock.patch.object(executor, 'read_command', return_value=mock.Mock(returncode=0, stdout=SHA)), \
                    mock.patch.object(executor.shutil, 'disk_usage', return_value=mock.Mock(free=30 << 30)), \
                    mock.patch.object(executor.runner_storage, 'storage_snapshot', return_value={
                        'layout': {'verified': True}, 'filesystems': [{'available_bytes': 30 << 30, 'below_mon_warn': False}]}), \
                    mock.patch.object(executor, 'prepare_images', return_value={'prepared_environment': {}}), \
                    mock.patch.object(executor.cleanup, 'snapshot', return_value={'passed': True}), \
                    mock.patch.object(executor.cleanup, 'check', return_value={'passed': cleanup_pass}), \
                    mock.patch.dict(os.environ, {'GITHUB_RUN_ID':'42', 'GITHUB_RUN_ATTEMPT':'1'}):
                code = executor.execute(root/'plan.json', root/'results', trial=True, runner=fake)
            result = json.loads((root/'results/periodic-run.json').read_text())
        sample.validate_receipt(result)
        return code, result, calls

    def test_failed_preparation_has_no_native_credit_but_advances_attempt(self):
        code, result, calls = self.run_control('modules')
        self.assertEqual(code, 1)
        self.assertNotIn('native', calls)
        self.assertTrue(result['advances'])
        self.assertFalse(result['complete'])

    def test_interrupted_before_native_does_not_advance(self):
        _, result, _ = self.run_control('interrupt_modules')
        self.assertFalse(result['advances'])
        self.assertEqual(result['classification'], 'interrupted')

    def test_interruption_after_admission_before_native_has_no_turn_credit(self):
        _, result, _ = self.run_control('interrupt_before_native')
        self.assertFalse(result['advances'])
        self.assertFalse(result['entries'][0]['native_started'])
        self.assertFalse(result['complete'])

    def test_interrupted_native_retains_started_checkpoint_and_missing_cases(self):
        _, result, _ = self.run_control('interrupt_native')
        self.assertTrue(result['advances'])
        self.assertTrue(result['entries'][0]['native_started'])
        self.assertFalse(result['complete'])
        self.assertTrue(result['unstarted_entries'])

    def test_cleanup_failure_stops_admission_even_after_image_preparation_failure(self):
        for phase in ('', 'test_images', 'interrupt_test_images', 'interrupt_native'):
            with self.subTest(phase=phase):
                _, result, calls = self.run_control(phase, cleanup_pass=False)
                self.assertEqual(len(result['entries']), 1)
                self.assertEqual(result['classification'], 'cleanup_failure')
                self.assertLessEqual(calls.count('native'), 1)

    def test_budget_exit_preserves_cleanup_failure_before_native(self):
        # The fake clock advances during image preparation; fake cleanup
        # observations exercise stop propagation only, without running Docker.
        for clean in (True, False):
            with self.subTest(clean=clean):
                code, result, calls = self.run_control(cleanup_pass=clean, exhaust_after_images=True)
                self.assertEqual(code, 1)
                self.assertEqual(len(result['entries']), 1)
                entry = result['entries'][0]
                self.assertEqual(entry['classification'], 'budget_not_started')
                self.assertEqual(entry['cleanup']['passed'], clean)
                self.assertFalse(entry['native_started'])
                self.assertNotIn('native', calls)
                self.assertFalse(result['complete'])
                self.assertEqual(result['classification'], 'incomplete_or_failed' if clean else 'cleanup_failure')

    def test_nonzero_native_exit_cannot_become_a_pass_from_a_stale_report(self):
        _, result, _ = self.run_control('native')
        self.assertFalse(result['complete'])
        self.assertTrue(all(e['classification'] != 'passed' for e in result['entries']))

    def test_successful_control_keeps_its_sample_scope_and_all_started_entries(self):
        code, result, _ = self.run_control()
        self.assertEqual(code, 0)
        self.assertTrue(result['complete'])
        self.assertTrue(all(e['native_started'] for e in result['entries']))
        self.assertEqual(result['plan']['scope'], 'flaky_discovery_sample_not_support')

    def test_characterization_preserves_first_observation_and_requires_all_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'native.log'
            cases = sorted(executor.SCHEDULE_CASES)
            text = ''.join(json.dumps({'case':c,'code':0,'out':'[]'}) + '\n' for c in cases)
            path.write_text(text)
            self.assertEqual(set(executor.characterization(path)), set(cases))
            path.write_text(text + json.dumps({'case':cases[0],'code':22,'out':'[]'}))
            with self.assertRaises(ValueError):
                executor.characterization(path)
            path.write_text('\n'.join(text.splitlines()[1:]))
            with self.assertRaises(ValueError):
                executor.characterization(path)

    def test_cpu_baseline_requires_actual_loader_evidence(self):
        for text, want in [('x86-64-v3 (supported, searched)', True), ('x86-64-v2 (supported, searched)', False)]:
            result = mock.Mock(returncode=0, stdout=text)
            self.assertEqual(executor.cpu_eligible({'amd64':'x86-64-v3'}, 'amd64', lambda argv: result), want)

    def test_owned_command_timeout_is_not_a_success(self):
        with tempfile.TemporaryDirectory() as directory:
            result = executor.command([sys.executable, '-c', 'import time; time.sleep(30)'], Path(directory)/'log', 0.02)
        self.assertTrue(result['timed_out'])
        self.assertFalse(result['passed'])


if __name__ == '__main__':
    unittest.main()
