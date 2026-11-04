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
            complete=False, reason='oldest_attempt'):
    row = {'schema': sample.SCHEMA, 'source_sha': SHA, 'execution': '1-1',
           'mode': mode, 'cell': cell, 'release': '20.2.4', 'selected_at': date, 'advances': advances,
           'complete': complete, 'reason': reason, 'entries': []}
    row['receipt_sha256'] = sample.digest(row)
    return row


class SelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = tags.load_catalog()

    def plan(self, mode='rotate', rows=(), **kwargs):
        return sample.make_plan(self.catalog, list(rows), SHA, mode, NOW, **kwargs)

    def test_required_profile_network_inventory_is_retained_exactly_once(self):
        core, groups = sample.scope(self.catalog)
        self.assertEqual({t for e in core for t in e['tests']}, set(sample.CORE))
        required = tags.discover(self.catalog)
        expected = tags.network_matrix([p for p in required['include'] if p['requires_ceph']], 'all')
        actual = [e for g in groups for e in g['entries']]
        self.assertEqual(len(actual), len(expected))
        self.assertEqual(len({e['id'] for e in actual}), len(actual))
        self.assertEqual({(e['profile']['id'], e['network']) for e in actual},
                         {(e['id'].removesuffix('-' + e['network']), e['network']) for e in expected})
        self.assertTrue(all(e['native_seconds'] <= 1200 for e in actual))

    def test_schedule_has_five_rotation_and_two_discovery_days(self):
        modes = [sample.make_plan(self.catalog, [], SHA, 'auto', NOW + dt.timedelta(days=d))['mode'] for d in range(7)]
        self.assertEqual((modes.count('rotate'), modes.count('discovery')), (5, 2))

    def test_complete_cartesian_rotation_before_repeating_a_cell(self):
        _, groups = sample.scope(self.catalog)
        candidates = [{'id': release + '/' + group['id']} for release in sample.images.RELEASES for group in groups]
        rows, seen = [], set()
        for i in range(len(candidates)):
            chosen, _ = sample.choose(candidates, rows, 'rotate')
            self.assertNotIn(chosen['id'], seen)
            seen.add(chosen['id'])
            # Failures advance exactly like successes, but remain incomplete.
            rows.append(history(chosen['id'], date=(NOW + dt.timedelta(seconds=i)).isoformat()))
        first, _ = sample.choose(candidates, rows, 'rotate')
        self.assertEqual(first['id'], rows[0]['cell'])

    def test_supported_releases_interleave_in_the_initial_rotation(self):
        rows = []
        releases = []
        for index in range(len(sample.images.RELEASES)):
            plan = self.plan(rows=rows)
            releases.append(plan['release'])
            rows.append(history(plan['cell'], date=(NOW + dt.timedelta(seconds=index)).isoformat()))
        self.assertEqual(releases, list(sample.images.RELEASES))

    def test_cancelled_before_native_has_no_turn_credit(self):
        candidates = [{'id': 'a'}, {'id': 'b'}]
        picked, _ = sample.choose(candidates, [history('a', advances=False)], 'rotate')
        self.assertEqual(picked['id'], 'a')

    def test_new_patch_priority_alternates_with_oldest_attempt(self):
        candidates = [{'id': v, 'release': v} for v in ('19.2.0', '19.2.1', '21.2.0')]
        first, reason = sample.choose(candidates, [], 'discovery')
        self.assertEqual((first['release'], reason), ('21.2.0', 'new_patch'))
        rows = [history('21.2.0', mode='discovery', reason=reason)]
        second, reason = sample.choose(candidates, rows, 'discovery')
        self.assertEqual((second['release'], reason), ('19.2.0', 'oldest_attempt'))

    def test_failure_does_not_replace_last_complete_evidence(self):
        first = self.plan()
        old = history(first['cell'], complete=True)
        failed = history(first['cell'], date=NOW.isoformat())
        # Restrict selection to this cell to inspect last complete provenance.
        with mock.patch.object(sample, 'choose', return_value=({'id': first['cell'], 'release': first['release'],
                                                               'entries': first['entries']}, 'oldest_attempt')):
            plan = self.plan(rows=[old, failed])
        self.assertEqual(plan['last_complete'], old['selected_at'])

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
        self.assertEqual(plan['scope'], 'sampled_compatibility_not_support')

    def test_targeted_same_sha_keeps_requested_patch_and_tests(self):
        first = self.plan()
        e = first['entries'][0]
        plan = self.plan('targeted', release='19.2.2', profiles=e['profile']['id'], tests=e['tests'][0])
        self.assertEqual((plan['release'], plan['source_sha'], len(plan['entries'])), ('19.2.2', SHA, 1))
        self.assertEqual(plan['entries'][0]['tests'], [e['tests'][0]])
        sample.validate_plan(plan, self.catalog)
        for changed in ('TestAbsent', e['tests'][0] + ',TestAbsent'):
            with self.assertRaises(ValueError):
                self.plan('targeted', release='19.2.2', profiles=e['profile']['id'], tests=changed)

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
                bad['images']['references']['essential'] = 'moving:tag'
            else:
                bad['budget_seconds'] = 1000000
            bad['plan_sha256'] = sample.digest({k:v for k,v in bad.items() if k != 'plan_sha256'})
            with self.subTest(change=change), self.assertRaises(ValueError):
                sample.validate_plan(bad, self.catalog)
        with self.assertRaises(ValueError):
            sample.validate_plan(original, [])

    def test_helper_resolution_failure_remains_a_frozen_preparation_failure(self):
        import in_test_images
        import periodic_campaign
        plan = self.plan()
        with mock.patch.object(sample.subprocess, 'run', return_value=mock.Mock(returncode=0, stdout='{}')), \
                mock.patch.object(in_test_images, 'reaper_reference', return_value='testcontainers/ryuk:v1'), \
                mock.patch.object(in_test_images, 'validate_analysis', return_value=[]), \
                mock.patch.object(in_test_images, 'plan_images', return_value=([], [])), \
                mock.patch.object(periodic_campaign, 'resolve', side_effect=RuntimeError('registry unavailable')):
            frozen = sample.freeze_test_images(plan)
        self.assertEqual(frozen['test_images_error'], 'helper_image_identity_unavailable')
        sample.validate_plan(frozen, self.catalog)

    def test_sample_selection_is_refused_before_native_on_unknown_tests_or_expanded_budget(self):
        core, _ = sample.scope(self.catalog)
        profile = core[0]['profile']
        for selection, timeout in [('TestAbsent', 1), (core[0]['tests'][0], 1000000)]:
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


class ExecutionControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = tags.load_catalog()
        cls.plan = sample.make_plan(cls.catalog, [], SHA, 'rotate', NOW)

    def run_control(self, failure='', cleanup_pass=True):
        """Fake commands test admission/checkpoints only; they run no Ceph."""
        calls = []
        def fake(argv, log, seconds, env=None):
            phase = 'native' if 'native' in argv else 'test_images' if any('in_test_images.py' in s for s in argv) else 'compile' if 'compile' in argv else 'modules'
            calls.append(phase)
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
        for phase in ('', 'test_images'):
            with self.subTest(phase=phase):
                _, result, calls = self.run_control(phase, cleanup_pass=False)
                self.assertEqual(len(result['entries']), 1)
                self.assertEqual(result['classification'], 'cleanup_failure')
                self.assertLessEqual(calls.count('native'), 1)

    def test_nonzero_native_exit_cannot_become_a_pass_from_a_stale_report(self):
        _, result, _ = self.run_control('native')
        self.assertFalse(result['complete'])
        self.assertTrue(all(e['classification'] != 'passed' for e in result['entries']))

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
