"""Test CI measurement aggregation on small synthetic runs, without GitHub."""

import json
from datetime import date
from pathlib import Path
import tempfile
import unittest

import ci_measurements as measure


MARKERS = [{"name": "pr-scope", "sha": "a" * 40}, {"name": "binary-reuse", "sha": "b" * 40}]


def at(minute, second=0):
    return "2026-10-17T10:{:02d}:{:02d}Z".format(minute, second)


def step(name, start, end):
    return {"name": name, "started_at": start, "completed_at": end, "conclusion": "success"}


def runtime_job(name, created, started, native, conclusion="success", attempt=1):
    """A runtime job: setup 0-1, compile 1-2, Docker 2, images 2-3, native
    3-3+native, cleanup one more minute, reporting zero."""
    base = int(started[14:16])
    steps = [step("Set up job", at(base), at(base + 1)),
             step("Compile selected tagged suite", at(base + 1), at(base + 2)),
             step("Docker baseline and engine identity", at(base + 2), at(base + 2)),
             step("Resolve native published Ceph roles", at(base + 2), at(base + 3)),
             step("Native assertions and completion proof", at(base + 3), at(base + 3 + native)),
             step("Verify owned Docker resource cleanup", at(base + 3 + native), at(base + 4 + native))]
    return {"name": name, "run_attempt": attempt, "conclusion": conclusion, "created_at": created,
            "started_at": started, "completed_at": at(base + 4 + native), "steps": steps}


def plan_job(created, started, completed):
    return {"name": "Scenarios / Plan", "run_attempt": 1, "conclusion": "success", "created_at": created,
            "started_at": started, "completed_at": completed, "steps": [step("Set up job", started, completed)]}


def run(identifier, name, sha, created, conclusion="success", event="pull_request", status="completed"):
    return {"id": identifier, "name": name, "event": event, "head_sha": sha, "created_at": created,
            "status": status, "conclusion": conclusion}


class PhaseTests(unittest.TestCase):
    def test_steps_map_to_phases_and_other_steps_are_reporting(self):
        job = runtime_job("Scenarios / sessions", at(0), at(1), 5)
        job["steps"].append(step("Save compile, native and phase results", at(10), at(10, 30)))
        phases = measure.job_phases(job)
        self.assertEqual(phases["setup"], 60)
        self.assertEqual(phases["compile"], 60)
        self.assertEqual(phases["docker"], 0)
        self.assertEqual(phases["images"], 60)
        self.assertEqual(phases["native"], 300)
        self.assertEqual(phases["cleanup"], 60)
        self.assertEqual(phases["reporting"], 30)

    def test_only_jobs_with_a_native_step_are_runtime_jobs(self):
        self.assertTrue(measure.is_runtime(runtime_job("Scenarios / x", at(0), at(0), 1)))
        self.assertFalse(measure.is_runtime(plan_job(at(0), at(0), at(1))))

    def test_profile_key_reads_the_release_from_the_caller(self):
        self.assertEqual(measure.profile_key("Ceph short", "Scenarios / sessions"), ("tentacle", "sessions"))
        self.assertEqual(measure.profile_key("Ceph periodic", "Squid short / sessions (bridge)"),
                         ("squid", "sessions (bridge)"))
        self.assertEqual(measure.profile_key("Ceph periodic", "Representative / sessions #2"),
                         ("tentacle", "sessions"))
        self.assertEqual(measure.profile_key("Docker checks", "Scenarios / dockerbridge"), ("none", "dockerbridge"))

    def test_percentile_is_nearest_rank(self):
        self.assertEqual(measure.percentile([5, 1, 3, 2, 4], 50), 3)
        self.assertEqual(measure.percentile(range(1, 11), 90), 9)
        self.assertIsNone(measure.percentile([], 50))


class ExecutionTests(unittest.TestCase):
    def fixture(self):
        runs = [run(1, "Ceph short", "h1", at(0)), run(2, "Code checks", "h1", at(0, 5)),
                # A head whose run was cancelled is not a comparable execution.
                run(3, "Ceph short", "h2", at(0), conclusion="cancelled"), run(4, "Code checks", "h2", at(0)),
                # A head that ran only Code checks has no runtime workflow.
                run(5, "Code checks", "h3", at(0)),
                # The periodic workflow is not part of an execution.
                run(6, "Ceph periodic", "h1", at(0), event="schedule"),
                # A second run of one workflow for the same head is ignored.
                run(7, "Ceph short", "h1", at(30))]
        jobs = {"1": [plan_job(at(0), at(0, 30), at(1)),
                      runtime_job("Scenarios / a", at(1), at(2), 3),
                      runtime_job("Scenarios / b", at(1), at(4), 1),
                      # A rerun attempt does not count again.
                      runtime_job("Scenarios / b", at(9), at(10), 1, attempt=2),
                      {"name": "Scenarios / c", "run_attempt": 1, "conclusion": "skipped",
                       "created_at": at(1), "started_at": at(1), "completed_at": at(1), "steps": []}],
                "2": [plan_job(at(0, 5), at(0, 10), at(2))], "4": [], "5": [], "6": [], "7": []}
        contains = {"h1": {"a" * 40: True, "b" * 40: False}, "h2": {"a" * 40: True, "b" * 40: True}}
        return runs, jobs, contains

    def test_execution_groups_first_attempt_family_runs_and_skips_cancelled(self):
        runs, jobs, contains = self.fixture()
        found, skipped = measure.executions(runs, jobs, contains, MARKERS)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["cohort"], "pr-scope")
        self.assertEqual(found[0]["runs"], {"Ceph short": 1, "Code checks": 2})
        self.assertEqual(skipped, {"cancelled run": 1, "no runtime workflow": 1})

    def test_execution_metrics_measure_runner_time_wall_and_acquisition(self):
        runs, jobs, contains = self.fixture()
        metrics = measure.execution_metrics(measure.executions(runs, jobs, contains, MARKERS)[0][0])
        # Held runners: plan 30 s, a 7 min, b 5 min, Code plan 110 s.
        self.assertEqual(metrics["jobs"], 4)
        self.assertEqual(metrics["runtime_jobs"], 2)
        self.assertEqual(metrics["skipped_jobs"], 1)
        self.assertEqual(metrics["runner_minutes"], round((30 + 420 + 300 + 110) / 60, 2))
        self.assertEqual(metrics["billed_minutes"], 1 + 7 + 5 + 2)
        # The last job (b) completes at 10:09; the first run started at 10:00.
        self.assertEqual(metrics["wall_minutes"], 9.0)
        self.assertEqual(sorted(metrics["acquisition_seconds"]), [5, 30, 60, 180])
        self.assertEqual(metrics["phase_minutes"]["native"], 4.0)

    def test_unfetched_jobs_or_markers_leave_an_execution_out(self):
        runs, jobs, contains = self.fixture()
        del jobs["2"]
        self.assertEqual(measure.executions(runs, jobs, contains, MARKERS)[1]["jobs not fetched"], 1)
        runs, jobs, contains = self.fixture()
        del contains["h1"]
        self.assertEqual(measure.executions(runs, jobs, contains, MARKERS)[1]["markers not fetched"], 1)

    def test_cohort_is_the_newest_marker_a_head_contains(self):
        self.assertEqual(measure.cohort_of(MARKERS, {}), "baseline")
        self.assertEqual(measure.cohort_of(MARKERS, {"a" * 40: True, "b" * 40: False}), "pr-scope")
        self.assertEqual(measure.cohort_of(MARKERS, {"a" * 40: True, "b" * 40: True}), "binary-reuse")

    def test_cohort_summary_splits_cohort_and_event(self):
        metrics = [{"cohort": "x", "event": "push", "conclusion": "success", "jobs": n, "runtime_jobs": n,
                    "runner_minutes": n, "billed_minutes": n, "wall_minutes": n, "acquisition_seconds": [n],
                    "phase_minutes": dict.fromkeys(measure.PHASE_NAMES, n)} for n in (1, 2, 3)]
        metrics.append({**metrics[0], "event": "pull_request", "conclusion": "failure"})
        summary = measure.cohort_summary(metrics)
        self.assertEqual([(entry["event"], entry["executions"], entry["succeeded"]) for entry in summary],
                         [("pull_request", 1, 0), ("push", 3, 3)])
        self.assertEqual(summary[1]["wall_minutes"]["median"], 2)


class ProfileTests(unittest.TestCase):
    def test_profile_distributions_use_successful_first_attempts_of_uncancelled_runs(self):
        runs = [run(1, "Ceph short", "h", at(0)), run(2, "Ceph short", "h", at(0), conclusion="cancelled")]
        jobs = {"1": [runtime_job("Scenarios / a", at(0), at(1), 2),
                      runtime_job("Scenarios / a", at(0), at(1), 4),
                      runtime_job("Scenarios / a", at(0), at(1), 9, conclusion="failure")],
                "2": [runtime_job("Scenarios / a", at(0), at(1), 8)]}
        measured = measure.runtime_jobs(runs, jobs, MARKERS, {"h": {"a" * 40: True, "b" * 40: True}})
        self.assertEqual(len(measured), 2)
        [profile] = measure.profile_distributions(measured, "binary-reuse")
        self.assertEqual((profile["release"], profile["batch"], profile["jobs"]), ("tentacle", "a", 2))
        self.assertEqual(profile["native"]["median"], 120)
        self.assertEqual(profile["native"]["max"], 240)

    def test_phase_comparison_uses_only_batches_of_every_cohort(self):
        def job(cohort, batch, compile_seconds):
            phases = dict.fromkeys(measure.PHASE_NAMES, 0.0)
            phases["compile"] = compile_seconds
            return {"cohort": cohort, "release": "tentacle", "batch": batch, "duration": 1, "phases": phases}
        jobs = [job("old", "a", 30), job("old", "gone", 99), job("new", "a", 10), job("new", "added", 1)]
        old, new = measure.phase_comparison(jobs, ["old", "new"])
        self.assertEqual((old["batches"], old["jobs"], old["compile"]["median"]), (1, 1, 30))
        self.assertEqual((new["jobs"], new["compile"]["median"]), (1, 10))

    def test_peak_concurrency_counts_overlapping_held_runners(self):
        jobs = [plan_job(at(0), at(0), at(5)), plan_job(at(0), at(1), at(3)), plan_job(at(0), at(3), at(4)),
                {"conclusion": "skipped", "started_at": at(0), "completed_at": at(9)}]
        # The second job ends at 10:03 as the third starts, so at most two overlap.
        self.assertEqual(measure.peak_concurrency(jobs), {"peak": 2, "at": at(1)})


def timed(name, created, started, completed, identifier=0):
    return {"id": identifier, "name": name, "run_attempt": 1, "conclusion": "success", "created_at": created,
            "started_at": started, "completed_at": completed, "steps": [step("Set up job", started, completed)]}


class RunnerTests(unittest.TestCase):
    def test_runner_limit_is_the_running_count_that_queued_jobs_waited_for(self):
        # Two jobs hold runners from 10:00 to 10:10; three more were queued
        # at 10:00 and each started when one of three earlier ones ended.
        jobs = [timed("a", at(0), at(0), at(10)), timed("b", at(0), at(0), at(10)),
                timed("c", at(0), at(0), at(5)), timed("d", at(0), at(5), at(8)),
                timed("e", at(0), at(8), at(9)), timed("quick", at(0), at(0, 30), at(1))]
        limit = measure.runner_limit(jobs)
        self.assertEqual(limit["queued_jobs"], 2)
        self.assertEqual((limit["mode"], limit["limit"], limit["mode_share"]), (2, 3, 1.0))
        self.assertEqual(measure.runner_limit([])["limit"], None)

    def test_a_job_cancelled_while_queued_held_no_runner(self):
        queued = {"name": "x", "conclusion": "cancelled", "created_at": at(0), "started_at": at(0),
                  "completed_at": at(5), "steps": []}
        self.assertFalse(measure.ran(queued))
        self.assertEqual(measure.peak_concurrency([queued, queued])["peak"], 0)

    def test_executions_without_a_held_runner_are_left_out(self):
        runs = [run(1, "Ceph short", "h", at(0), conclusion="failure")]
        jobs = {"1": [{"name": "Scenarios / Plan", "run_attempt": 1, "conclusion": "cancelled",
                       "created_at": at(0), "started_at": at(0), "completed_at": at(1), "steps": []}]}
        found, skipped = measure.executions(runs, jobs, {"h": {}}, [])
        self.assertEqual((found, skipped), ([], {"no job held a runner": 1}))

    def test_contention_splits_executions_by_an_overlapping_periodic_run(self):
        runs = [run(1, "Ceph short", "early", at(0)), run(2, "Ceph short", "late", at(30)),
                run(3, "Ceph periodic", "main", at(25), event="schedule")]
        jobs = {"1": [timed("Scenarios / a", at(0), at(1), at(10))],
                "2": [timed("Scenarios / a", at(30), at(40), at(50))],
                "3": [timed("R / a", at(25), at(26), at(45)), timed("R / b", at(25), at(26), at(45))]}
        found, _ = measure.executions(runs, jobs, {"early": {}, "late": {}}, [])
        metrics = [measure.execution_metrics(execution) for execution in found]
        intervals = measure.busy_intervals(runs, jobs, "Ceph periodic", 2)
        by_key = {(execution["event"], execution["head_sha"]): execution for execution in found}
        overlapping, alone = measure.contention_summary(metrics, by_key, intervals, "baseline")
        self.assertEqual((overlapping["periodic_overlap"], overlapping["executions"]), (True, 1))
        self.assertEqual(overlapping["acquisition_seconds"]["median"], 600)
        self.assertEqual((alone["periodic_overlap"], alone["executions"]), (False, 1))
        self.assertEqual(measure.busy_intervals(runs, jobs, "Ceph periodic", 3), [])

    def test_matrix_runs_count_one_tag_set_per_batch_across_releases_and_network_modes(self):
        runs = [run(1, "Ceph periodic", "main", at(0), event="schedule")]
        jobs = {"1": [runtime_job("Tentacle short / a", at(0), at(1), 1),
                      runtime_job("Squid short / a", at(0), at(1), 1),
                      runtime_job("Squid short / a (bridge)", at(0), at(1), 1),
                      runtime_job("Squid short / b", at(0), at(1), 1)]}
        [summary] = measure.matrix_runs(runs, jobs, "Ceph periodic", 2)
        self.assertEqual((summary["runtime_jobs"], summary["tag_sets"]), (4, 2))
        self.assertEqual(summary["compile_minutes"], 4.0)
        self.assertEqual(measure.matrix_runs(runs, jobs, "Ceph periodic", 5), [])

    def test_caller_groups_split_plans_from_runtime_jobs_in_matrix_order(self):
        jobs = [timed("Gate", at(0), at(0), at(1), 1), timed("Squid short / Plan", at(1), at(1), at(2), 2),
                timed("Squid short / b", at(2), at(2), at(5), 4), timed("Squid short / a", at(2), at(2), at(3), 3)]
        self.assertEqual(measure.caller_groups(jobs), {"Squid short": (60, [60, 180])})


class SimulationTests(unittest.TestCase):
    def test_unbounded_callers_share_the_runners_first_come_first_served(self):
        groups = {"x": (0, [60, 60, 60]), "y": (0, [60])}
        self.assertEqual(measure.simulate(groups, 4), {"makespan_minutes": 1.0, "peak_runners": 4})
        self.assertEqual(measure.simulate(groups, 2), {"makespan_minutes": 2.0, "peak_runners": 2})

    def test_a_plan_runs_before_its_callers_jobs(self):
        self.assertEqual(measure.simulate({"x": (60, [60])}, 4)["makespan_minutes"], 2.0)

    def test_a_job_limit_bounds_each_caller_and_may_differ_by_caller(self):
        groups = {"Tentacle short": (0, [60] * 4), "Tentacle recovery": (0, [60] * 2)}
        bounded = measure.simulate(groups, 40, {"Tentacle short": 2, "Tentacle recovery": 1})
        self.assertEqual(bounded, {"makespan_minutes": 2.0, "peak_runners": 3})
        self.assertEqual(measure.simulate(groups, 40, 1)["makespan_minutes"], 4.0)
        limits = measure.category_limits(groups, {"short": 2})
        self.assertEqual(limits, {"Tentacle short": 2, "Tentacle recovery": None})

    def test_a_caller_limit_admits_the_next_caller_when_one_finishes(self):
        groups = {"a": (0, [120]), "b": (0, [60]), "c": (0, [60])}
        self.assertEqual(measure.simulate(groups, 40, caller_limit=2)["makespan_minutes"], 2.0)
        self.assertEqual(measure.simulate(groups, 40, caller_limit=1)["makespan_minutes"], 4.0)

    def test_schedule_summary_compares_periodic_bounds_and_the_pull_request_reserve(self):
        periodic = [{"Tentacle short": (0, [60] * 4)}]
        requests = [{"Ceph short/Scenarios": (0, [60] * 4)}]
        summary = measure.schedule_summary(periodic, requests, 4, {"short": 1}, 2)
        self.assertEqual(summary["periodic"][0]["unbounded"]["makespan_minutes"], 1.0)
        self.assertEqual(summary["periodic"][0]["bounded"]["makespan_minutes"], 4.0)
        self.assertEqual(summary["pull_requests"]["all_runners"]["median"], 1.0)
        self.assertEqual(summary["pull_requests"]["reserve"]["median"], 2.0)


class PackingTests(unittest.TestCase):
    def profile(self, native, shared=20.0, compile_seconds=30.0, cleanup=10.0):
        entry = {"native": {"median": native, "p90": native}}
        for phase in measure.PHASE_NAMES:
            entry[phase] = {"median": 0.0}
        entry["setup"]["median"] = shared
        entry["compile"]["median"] = compile_seconds
        entry["cleanup"]["median"] = cleanup
        entry["native"] = {"median": native, "p90": native}
        return entry

    def test_packing_saves_shared_phases_and_serializes_member_phases(self):
        profiles = [self.profile(10), self.profile(20), self.profile(30), self.profile(500)]
        estimate = measure.packing_estimate(profiles, 2, 60)
        self.assertEqual(estimate["candidates"], 3)
        self.assertEqual((estimate["jobs_before"], estimate["jobs_after"]), (3, 2))
        # One pack of two saves one member's 20 s of setup; the lone third
        # member saves nothing.
        self.assertEqual(estimate["saved_runner_minutes"], round(20 / 60, 2))
        # The pack runs the shorter member (30 + 10 + 10 s) after the longer.
        self.assertEqual(estimate["added_wall_seconds_max"], 50)


class CacheTests(unittest.TestCase):
    def test_fetch_reads_each_finished_record_once_and_report_works_offline(self):
        calls = []

        def api(path, jq):
            calls.append(path)
            if "/jobs?" in path:
                return [runtime_job("Scenarios / a", at(0), at(1), 2)]
            return [run(11, "Ceph short", "h", at(0)), run(12, "Ceph short", "h2", at(0), status="in_progress")]

        def compare(repo, base, head):
            calls.append("compare " + base[:1] + " " + head)
            return {"a": "ahead", "b": "behind"}[base[:1]]

        with tempfile.TemporaryDirectory() as directory:
            day = date(2026, 10, 16)
            first = measure.fetch(directory, "o/r", day, day, MARKERS, today=date(2026, 10, 18), api=api,
                                  compare=compare)
            self.assertEqual(first, {"run lists": 1, "job lists": 1, "marker answers": 1})
            calls.clear()
            second = measure.fetch(directory, "o/r", day, day, MARKERS, today=date(2026, 10, 18), api=api,
                                   compare=compare)
            self.assertEqual((second, calls), ({}, []))
            self.assertEqual(json.loads((Path(directory) / "contains" / "h.json").read_text()),
                             {"a" * 40: True, "b" * 40: False})
            result = measure.report(directory, MARKERS)
            self.assertEqual(result["profiles"], [])
            self.assertEqual(result["phase_comparison"][0]["cohort"], "pr-scope")

    def test_a_failed_request_is_counted_and_fetched_again_later(self):
        def api(path, jq):
            if "/jobs?" in path:
                raise ValueError("HTTP 502")
            return [run(11, "Ceph short", "h", at(0))]

        with tempfile.TemporaryDirectory() as directory:
            day = date(2026, 10, 16)
            result = measure.fetch(directory, "o/r", day, day, MARKERS, today=date(2026, 10, 18), api=api,
                                   compare=lambda repo, base, head: "ahead")
            self.assertEqual(result, {"run lists": 1, "failed requests": 1, "marker answers": 1})
            self.assertFalse((Path(directory) / "jobs" / "11.json").exists())

    def test_markers_are_named_commits_in_order(self):
        self.assertEqual(measure.parse_markers(["x=" + "1" * 40]), [{"name": "x", "sha": "1" * 40}])
        for bad in (["x"], ["baseline=" + "1" * 40], ["x=" + "1" * 40, "x=" + "2" * 40]):
            with self.assertRaises(ValueError):
                measure.parse_markers(bad)


if __name__ == "__main__":
    unittest.main()
