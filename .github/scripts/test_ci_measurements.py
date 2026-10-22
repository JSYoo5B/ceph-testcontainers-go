"""Test CI measurement aggregation on small synthetic runs, without GitHub."""

import io
import json
from datetime import date
from pathlib import Path
import tempfile
import unittest
import unittest.mock

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


class CohortTests(unittest.TestCase):
    """Join history rows to measured jobs and group by exact configuration."""

    def setUp(self):
        import test_scenario_history as rows
        self.rows = rows
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.count = 0
        self.runs, self.jobs = [], {}

    def attempt(self, run_id, measured_id, native, result="passed", attempt="1", **metadata):
        """A measured runtime job and the history row that its artifacts give."""
        self.count += 1
        job = self.rows.Job(self.root / str(self.count)).passing()
        if result == "failed":
            job.phases(native="failure")
            job.native(self.rows.FAILING_LOG, "native-test-failure", 40.0)
        elif result == "cancelled":
            job.phases(native="cancelled")
        row = job.row(**({"run_id": str(run_id), "job_id": str(measured_id), "run_attempt": attempt} | metadata))
        if not any(run["id"] == run_id for run in self.runs):
            self.runs.append(run(run_id, "Ceph recovery", "a" * 40, at(0)))
        measured = runtime_job("Scenarios / rbd", at(0), at(1), native, attempt=int(attempt))
        measured["id"] = measured_id
        self.jobs.setdefault(str(run_id), []).append(measured)
        return row

    def test_repetitions_of_one_configuration_join_with_an_outcome_ledger(self):
        rows = [self.attempt(100, 200, 5), self.attempt(101, 201, 7), self.attempt(102, 202, 9, "failed"),
                self.attempt(103, 203, 2, "cancelled")]
        result = measure.cohorts(self.runs, self.jobs, rows, "owner/repo")
        self.assertEqual((result["rows"], result["joined"]), (4, 4))
        self.assertEqual((result["unmatched_rows"], result["unmatched_jobs"], result["incomparable"]), ([], [], []))
        self.assertEqual(len(result["configs"]), 1)
        config = result["configs"][0]
        self.assertEqual((config["attempts"], config["release"]), (4, "20.2.4"))
        self.assertEqual(config["identity"]["checkout_tree"], "c" * 40)
        self.assertEqual(config["outcomes"], {"assertion_failure": {"attempts": 1, "runner_seconds": 780.0},
                                              "cancelled": {"attempts": 1, "runner_seconds": 360.0},
                                              "passed": {"attempts": 2, "runner_seconds": 1200.0}})
        # Ranges are of passed attempts only: 5 and 7 native minutes.
        self.assertEqual(config["passed"]["native"], {"count": 2, "min": 300.0, "median": 300.0, "max": 420.0})
        self.assertEqual(config["passed"]["duration"]["count"], 2)
        self.assertEqual([sample["job_id"] for sample in config["samples"]], ["200", "201", "202", "203"])

    def test_another_tree_is_another_configuration(self):
        rows = [self.attempt(100, 200, 5), self.attempt(101, 201, 5, checkout_tree="f" * 40)]
        result = measure.cohorts(self.runs, self.jobs, rows, "owner/repo")
        self.assertEqual([config["attempts"] for config in result["configs"]], [1, 1])
        self.assertEqual(len({config["config_key"] for config in result["configs"]}), 2)

    def test_rows_and_jobs_that_do_not_join_stay_visible(self):
        joined = self.attempt(100, 200, 5)
        uncached = self.attempt(101, 201, 5)
        del self.jobs["101"]
        other_repo = self.attempt(102, 202, 5, repository="other/repo")
        no_job = self.attempt(103, 203, 5, job_id="")
        wrong_attempt = self.attempt(104, 204, 5)
        self.jobs["104"][0]["run_attempt"] = 2
        orphan = runtime_job("Scenarios / orphan", at(0), at(1), 3)
        orphan["id"] = 299
        self.jobs["100"].append(orphan)
        incomparable = self.attempt(105, 205, 5, checkout_commit="")
        result = measure.cohorts(self.runs, self.jobs, [joined, uncached, other_repo, no_job, wrong_attempt,
                                                        incomparable, joined], "owner/repo")
        reasons = [(row["run_id"], row["reason"]) for row in result["unmatched_rows"]]
        self.assertEqual(reasons, [("101", "job is not in the measurement cache"), ("102", "row of another repository"),
                                   ("103", "row has no job ID"), ("104", "job is not in the measurement cache"),
                                   ("100", "another row names the same job")])
        self.assertEqual([(job["run_id"], job["job_id"]) for job in result["unmatched_jobs"]],
                         [("100", "299"), ("102", "202"), ("103", "203"), ("104", "204")])
        self.assertEqual([row["run_id"] for row in result["incomparable"]], ["105"])
        self.assertEqual([config["attempts"] for config in result["configs"]], [1])

    def test_cli_reads_stored_history_and_artifact_rows_once(self):
        rows = [self.attempt(100, 200, 5), self.attempt(101, 201, 6)]
        cache, history = self.root / "cache", self.root / "history"
        (cache / "runs").mkdir(parents=True)
        (cache / "jobs").mkdir()
        (cache / "runs" / "2026-10-17.json").write_text(json.dumps(self.runs))
        for run_id, run_jobs in self.jobs.items():
            (cache / "jobs" / (run_id + ".json")).write_text(json.dumps(run_jobs))
        (history / "rows").mkdir(parents=True)
        (history / "rows" / "100-1.jsonl").write_text(json.dumps(rows[0]) + "\n")
        (history / "artifacts" / "a").mkdir(parents=True)
        (history / "artifacts" / "a" / "history-row.json").write_text(json.dumps(rows[0]))
        (history / "artifacts" / "b").mkdir(parents=True)
        (history / "artifacts" / "b" / "history-row.json").write_text(json.dumps(rows[1]))
        output = self.root / "cohorts.json"
        with unittest.mock.patch("sys.stdout", new=io.StringIO()) as printed:
            self.assertEqual(measure.main(["cohorts", "--cache", str(cache), "--history", str(history),
                                           "--repo", "owner/repo", "--output", str(output)]), 0)
        result = json.loads(output.read_text())
        self.assertEqual((result["rows"], result["joined"], result["configs"][0]["attempts"]), (2, 2, 2))
        self.assertIn("| recovery-rbd-12345678 | 20.2.4 | - | ", printed.getvalue())
        self.assertIn("passed 2", printed.getvalue())


MODULE_KEY = "go-modules-Linux-" + "e" * 64
BUILD_KEY = "go-build-Linux-1.26.10-" + "e" * 64
CACHE_REF = "ghcr.io/owner/images:buildcache-debian-20.2.4-amd64-control"


def log_text(*lines):
    """A job log as GitHub serves it: a byte order mark, then timestamped lines."""
    return "﻿" + "\n".join("2026-10-20T23:34:{:02d}.1234567Z {}".format(index % 60, line)
                                for index, line in enumerate(lines)) + "\n"


def cache_section(action, key, path, *results):
    return ("##[group]Run actions/cache/{}@v6".format(action), "with:", "  path: " + path, "  key: " + key,
            "  enableCrossOsArchive: false", "##[endgroup]") + results


GO_LOG = log_text(
    "Current runner version: '2.338.0'", "Cache mode: write",
    "##[group]Run actions/setup-go@v7", "go version go1.26.10 linux/amd64",
    *cache_section("restore", MODULE_KEY, "~/go/pkg/mod", "Cache hit for: " + MODULE_KEY,
                   "Cache restored successfully", "Cache restored from key: " + MODULE_KEY),
    *cache_section("restore", BUILD_KEY, "~/.cache/go-build",
                   "Cache restored from key: go-build-Linux-1.26.9-" + "e" * 64),
    "##[group]Run make vulncheck", "No vulnerabilities found.",
    # continue-on-error: the step and the job stay green after this warning.
    *cache_section("save", BUILD_KEY, "~/.cache/go-build",
                   "##[warning]Failed to save: Unable to reserve cache with key " + BUILD_KEY
                   + ", another job may be creating this cache."),
    "##[group]Run actions/upload-artifact@v7")
COLD_LOG = log_text(
    "Cache mode: write", "go version go1.27.3 linux/amd64",
    *cache_section("restore", MODULE_KEY, "~/go/pkg/mod", "Cache not found for input keys: " + MODULE_KEY),
    *cache_section("restore", BUILD_KEY, "~/.cache/go-build",
                   "##[warning]Failed to restore: Cache service responded with 503"),
    *cache_section("save", MODULE_KEY, "~/go/pkg/mod", "Sent 1 of 1 (100.0%)", "Cache saved with key: " + MODULE_KEY),
    # A save that ran and stated neither a write nor a failure.
    *cache_section("save", BUILD_KEY, "~/.cache/go-build", "Sent 0 of 1 (0.0%)"))
BUILDX_LOG = log_text(
    "Cache mode: write", "##[group]Run ./build-images.sh",
    "#7 importing cache manifest from " + CACHE_REF + "-mon",
    "#7 ERROR: failed to configure registry cache importer: " + CACHE_REF + "-mon: not found",
    "#8 importing cache manifest from " + CACHE_REF + "-osd",
    "#8 ERROR: failed to configure registry cache importer: unexpected status 500",
    # ignore-error=true: the build and the job stay green after this error.
    "#17 exporting cache to registry", "#17 preparing build cache for export done",
    "#17 ERROR: failed to push " + CACHE_REF + "-osd: denied: permission_denied",
    "#7 importing cache manifest from " + CACHE_REF,
    "#7 inferred cache manifest type: application/vnd.oci.image.manifest.v1+json done",
    "#10 CACHED", "#15 exporting cache to registry", "#15 sending cache export 0.2s done",
    "#15 writing cache image manifest sha256:" + "2" * 64 + " 0.0s done", "#15 DONE 0.2s",
    "#9 importing cache manifest from " + CACHE_REF + "-rgw",
    "#9 inferred cache manifest type: application/vnd.oci.image.manifest.v1+json done",
    "#16 exporting cache to registry", "#16 preparing build cache for export done")


def cache_job(identifier, *steps, name="Vulnerability check", conclusion="success"):
    """A green job with the given (step name, conclusion) cache steps."""
    return {"id": identifier, "name": name, "run_attempt": 1, "status": "completed", "conclusion": conclusion,
            "created_at": at(0), "started_at": at(1), "completed_at": at(5),
            "steps": [step("Set up job", at(1), at(1, 5))]
            + [{**step(step_name, at(2), at(2, 5)), "conclusion": step_conclusion}
               for step_name, step_conclusion in steps]}


def excerpt(text):
    lines, truncated = measure.cache_excerpt(text)
    return {"lines": lines, "truncated": truncated}


class CacheLedgerTests(unittest.TestCase):
    """Cache operations come from log lines only, never from a green job."""

    def outcomes(self, text):
        return [(entry["kind"], entry["operation"], entry["key"], entry["outcome"])
                for entry in measure.cache_entries(measure.cache_excerpt(text)[0])]

    def test_go_restores_and_an_ignored_save_failure(self):
        self.assertEqual(self.outcomes(GO_LOG), [("go-module", "restore", MODULE_KEY, "hit"),
                                                 ("go-build", "restore", BUILD_KEY, "partial-restore"),
                                                 ("go-build", "save", BUILD_KEY, "save-failed")])
        entry = measure.cache_entries(measure.cache_excerpt(GO_LOG)[0])[0]
        self.assertEqual((entry["toolchain"], entry["platform"], entry["cache_mode"]),
                         ("go1.26.10", "linux/amd64", "write"))

    def test_misses_failed_restores_confirmed_and_unconfirmed_saves(self):
        self.assertEqual(self.outcomes(COLD_LOG), [("go-module", "restore", MODULE_KEY, "miss"),
                                                   ("go-build", "restore", BUILD_KEY, "restore-failed"),
                                                   ("go-module", "save", MODULE_KEY, "saved"),
                                                   ("go-build", "save", BUILD_KEY, "unknown")])

    def test_registry_imports_and_exports_including_an_ignored_export_error(self):
        # The step names no literal cache-to ref, so no export has a known
        # destination, although each follows an import of a matching name.
        self.assertEqual(self.outcomes(BUILDX_LOG), [("registry", "import", CACHE_REF + "-mon", "miss"),
                                                     ("registry", "import", CACHE_REF + "-osd", "restore-failed"),
                                                     ("registry", "export", None, "save-failed"),
                                                     ("registry", "import", CACHE_REF, "hit"),
                                                     ("registry", "export", None, "saved"),
                                                     ("registry", "import", CACHE_REF + "-rgw", "hit"),
                                                     ("registry", "export", None, "unknown")])

    def test_an_export_destination_is_only_an_explicit_cache_to_ref(self):
        source, base, destination = "registry.example/source:cache", "registry.example/base:cache", \
            "registry.example/dest:cache"
        build = ("#6 importing cache manifest from " + source,
                 "#6 inferred cache manifest type: application/vnd.oci.image.manifest.v1+json done",
                 "#7 importing cache manifest from " + base,
                 "#7 ERROR: failed to configure registry cache importer: " + base + ": not found",
                 "#20 exporting cache to registry", "#20 writing cache image manifest sha256:" + "3" * 64 + " done")
        explicit = log_text("##[group]Run docker buildx build --cache-from=type=registry,ref=" + source
                            + " --cache-from=type=registry,ref=" + base + " --cache-to=type=registry,ref="
                            + destination + ",mode=max image", *build)
        self.assertEqual(self.outcomes(explicit), [("registry", "import", source, "hit"),
                                                   ("registry", "import", base, "miss"),
                                                   ("registry", "export", destination, "saved")])
        # A cache-to input of an action, read from its "with:" block.
        action = log_text("##[group]Run docker/build-push-action@v7", "with:",
                          "  cache-to: type=registry,ref=" + destination + ",mode=max", "##[endgroup]", *build)
        self.assertEqual(self.outcomes(action)[-1], ("registry", "export", destination, "saved"))
        # A variable reference, two destinations, or a cache-to of an
        # earlier step name no destination.
        for text in (log_text("##[group]Run docker buildx build --cache-to=type=registry,ref=$cache_ref image",
                              *build),
                     log_text("##[group]Run docker buildx build --cache-to=type=registry,ref=" + destination
                              + " --cache-to=type=registry,ref=registry.example/other:cache image", *build),
                     log_text("##[group]Run docker buildx build --cache-to=type=registry,ref=" + destination
                              + " first", "##[group]Run docker buildx build second", *build)):
            self.assertEqual(self.outcomes(text)[-1], ("registry", "export", None, "saved"))

    def test_the_excerpt_keeps_only_cache_lines_without_timestamps(self):
        lines, truncated = measure.cache_excerpt(GO_LOG)
        self.assertFalse(truncated)
        self.assertEqual(lines[:3], ["Cache mode: write", "##[group]Run actions/setup-go@v7",
                                     "go version go1.26.10 linux/amd64"])
        self.assertIn("##[group]Run make vulncheck", lines)
        self.assertNotIn("No vulnerabilities found.", lines)
        self.assertNotIn("Current runner version: '2.338.0'", lines)
        lines, truncated = measure.cache_excerpt(log_text(*cache_section("restore", MODULE_KEY, "~/go/pkg/mod",
                                                                         *["Received"] * 500)))
        self.assertEqual((len(lines), truncated), (measure.CACHE_EXCERPT_LINES, True))

    def test_ledger_rows_without_a_log_are_unknown_and_skipped_saves_are_not_writes(self):
        runs = [run(11, "Code checks", "h", at(0)), {**run(12, "Code checks", "h2", at(0), event="push"),
                                                       "head_branch": "main"}]
        jobs = {"11": [cache_job(21, ("Restore the Go module cache", "success"),
                                 ("Save the Go build cache", "skipped")),
                       cache_job(22, ("Restore the Go module cache", "success")),
                       cache_job(23, ("Restore the Go module cache", "success"))],
                "12": [cache_job(31, ("Restore the Go module cache", "success"), ("Restore the Go build cache",
                                                                                 "success"),
                                 ("Save the Go build cache", "success"))]}
        logs = {"22": {"unavailable": "HTTP 410"}, "23": excerpt(COLD_LOG), "31": excerpt(GO_LOG)}
        rows = measure.cache_ledger(runs, jobs, logs, "owner/repo")
        summary = [(row["job_id"], row["event"], row["kind"], row["operation"], row["requested"], row["outcome"],
                    row["evidence_source"]) for row in rows]
        self.assertEqual(summary, [
            ("21", "pull_request", "go-module", "restore", True, "unknown", "none"),
            ("21", "pull_request", "go-build", "save", False, "save-skipped", "step"),
            ("22", "pull_request", "go-module", "restore", True, "unknown", "none"),
            ("23", "pull_request", "go-module", "restore", True, "miss", "log"),
            ("23", "pull_request", "go-build", "restore", True, "restore-failed", "log"),
            ("23", "pull_request", "go-module", "save", True, "saved", "log"),
            ("23", "pull_request", "go-build", "save", True, "unknown", "log"),
            ("31", "push", "go-module", "restore", True, "hit", "log"),
            ("31", "push", "go-build", "restore", True, "partial-restore", "log"),
            # The job and its save step are green; the log says nothing was written.
            ("31", "push", "go-build", "save", True, "save-failed", "log")])
        self.assertEqual(rows[2]["evidence"], "log unavailable: HTTP 410")
        self.assertEqual({row["repository"] for row in rows}, {"owner/repo"})
        self.assertEqual(rows[-1]["head_branch"], "main")
        conditions = measure.ledger_by_job(rows)
        self.assertEqual(measure.cache_condition(conditions[("12", "1", "31")]),
                         "go-build=partial-restore,go-module=hit")
        self.assertEqual(measure.cache_condition(conditions[("11", "1", "21")]), "go-module=unknown")
        self.assertEqual(measure.cache_condition(()), "no cache restore")
        self.assertLessEqual({row["outcome"] for row in rows}, set(measure.CACHE_OUTCOMES))

    def test_api_cache_steps_without_an_excerpt_section_stay_unknown(self):
        runs = [run(11, "Code checks", "h", at(0))]
        steps = (("Restore the Go module cache", "success"), ("Restore the Go build cache", "success"),
                 ("Save the Go build cache", "success"))
        jobs = {"11": [cache_job(21, *steps), cache_job(22, *steps)]}
        # Truncated inside the first section: the later sections are lost.
        lines, truncated = measure.cache_excerpt(log_text(*cache_section("restore", MODULE_KEY, "~/go/pkg/mod",
                                                                         *["Received"] * 500)))
        logs = {"21": {"lines": lines, "truncated": truncated}, "22": {"lines": [], "truncated": False}}
        rows = measure.cache_ledger(runs, jobs, logs, "owner/repo")
        summary = [(row["job_id"], row["kind"], row["operation"], row["outcome"], row["evidence_source"],
                    row["excerpt_truncated"], row.get("step")) for row in rows]
        self.assertEqual(summary, [
            ("21", "go-module", "restore", "unknown", "log", True, "Restore the Go module cache"),
            ("21", "go-build", "restore", "unknown", "none", True, "Restore the Go build cache"),
            ("21", "go-build", "save", "unknown", "none", True, "Save the Go build cache"),
            ("22", "go-module", "restore", "unknown", "none", False, "Restore the Go module cache"),
            ("22", "go-build", "restore", "unknown", "none", False, "Restore the Go build cache"),
            ("22", "go-build", "save", "unknown", "none", False, "Save the Go build cache")])
        self.assertEqual(rows[1]["evidence"], "no cache section in the fetched log excerpt, which is truncated")
        self.assertEqual(rows[3]["evidence"], "no cache section in the fetched log")
        conditions = measure.ledger_by_job(rows)
        self.assertEqual(measure.cache_condition(conditions[("11", "1", "21")]),
                         "go-build=unknown,go-module=unknown")
        # An empty excerpt is no evidence that nothing was restored.
        self.assertEqual(measure.cache_condition(conditions[("11", "1", "22")]),
                         "go-build=unknown,go-module=unknown")

    def test_excerpt_sections_match_api_steps_in_order_and_unnamed_sections_stay(self):
        runs = [run(11, "Code checks", "h", at(0))]
        jobs = {"11": [cache_job(21, ("Restore the Go build cache", "success"),
                                 ("Save the Go build cache", "success"))]}
        rows = measure.cache_ledger(runs, jobs, {"21": excerpt(COLD_LOG)}, "owner/repo")
        self.assertEqual([(row["kind"], row["operation"], row["outcome"], row.get("step")) for row in rows], [
            ("go-module", "restore", "miss", None),
            ("go-build", "restore", "restore-failed", "Restore the Go build cache"),
            ("go-module", "save", "saved", None),
            ("go-build", "save", "unknown", "Save the Go build cache")])

    def test_every_outcome_is_named(self):
        texts = (GO_LOG, COLD_LOG, BUILDX_LOG)
        found = {entry["outcome"] for text in texts for entry in measure.cache_entries(measure.cache_excerpt(text)[0])}
        self.assertEqual(found | {"save-skipped"}, set(measure.CACHE_OUTCOMES))

    def test_fetch_reads_no_log_unless_asked_and_then_a_bounded_number_once(self):
        calls = []

        def api(path, jq):
            if "/jobs?" in path:
                run_id = int(path.split("/runs/")[1].split("/")[0])
                return [cache_job(run_id * 10 + index, ("Restore the Go module cache", "success"))
                        for index in range(2)] + [cache_job(run_id * 10 + 5, name="Lint")]
            return [run(11, "Code checks", "h", at(0)), run(12, "Code checks", "h2", at(1))]

        def log(repo, job_id):
            calls.append(job_id)
            if job_id == 111:
                raise LookupError("HTTP 410: logs expired")
            if job_id == 120:
                raise ValueError("HTTP 502")
            return GO_LOG

        with tempfile.TemporaryDirectory() as directory:
            day, today = date(2026, 10, 16), date(2026, 10, 18)
            options = {"today": today, "api": api, "compare": lambda repo, base, head: "ahead", "log": log,
                       "workers": 1}
            measure.fetch(directory, "o/r", day, day, [], **options)
            self.assertEqual(calls, [])
            first = measure.fetch(directory, "o/r", day, day, [], cache_logs=3, **options)
            self.assertEqual(first, {"cache logs": 1, "unavailable cache logs": 1, "failed requests": 1})
            self.assertEqual(calls, [110, 111, 120])
            stored = measure.load_cache_logs(directory)
            self.assertEqual(sorted(stored), ["110", "111"])
            self.assertEqual(stored["111"]["unavailable"], "HTTP 410: logs expired")
            self.assertTrue(all("2026-10-20T" not in line for line in stored["110"]["lines"]))
            calls.clear()
            measure.fetch(directory, "o/r", day, day, [], cache_logs=5, **options)
            # Only the failed and the not yet read logs; a job without a cache step is never read.
            self.assertEqual(calls, [120, 121])
            calls.clear()
            measure.fetch(directory, "o/r", day, day, [], cache_logs=5, cache_log_jobs="^Lint$", **options)
            self.assertEqual(calls, [115, 120, 125])

    def test_cli_lists_the_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            for name in ("runs", "jobs", "cachelogs"):
                (cache / name).mkdir()
            (cache / "runs" / "2026-10-17.json").write_text(json.dumps([run(11, "Code checks", "h", at(0))]))
            (cache / "jobs" / "11.json").write_text(json.dumps([cache_job(21, ("Restore the Go module cache",
                                                                               "success"))]))
            (cache / "cachelogs" / "21.json").write_text(json.dumps(excerpt(GO_LOG)))
            output = cache / "caches.json"
            with unittest.mock.patch("sys.stdout", new=io.StringIO()) as printed:
                self.assertEqual(measure.main(["caches", "--cache", str(cache), "--repo", "owner/repo",
                                               "--output", str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertEqual((result["jobs"], result["logs"], len(result["rows"])), (1, 1, 3))
            self.assertIn("| go-build | save | save-failed | 1 |", printed.getvalue())
            self.assertIn("| 11 | 1 | Vulnerability check | pull_request | go-module | restore | " + MODULE_KEY
                          + " | go1.26.10 | hit |", printed.getvalue())
            with unittest.mock.patch("sys.stderr", new=io.StringIO()):
                self.assertEqual(measure.main(["fetch", "--cache", str(cache), "--repo", "o/r",
                                               "--since", "2026-10-16", "--cache-logs", "-1"]), 1)


class CohortCacheTests(unittest.TestCase):
    """Split a configuration's joined samples by the cache condition of their jobs."""

    setUp = CohortTests.setUp
    attempt = CohortTests.attempt

    def test_samples_carry_their_cache_condition_and_split_without_changing_ranges(self):
        rows = [self.attempt(100, 200, 5), self.attempt(101, 201, 7), self.attempt(102, 202, 9),
                self.attempt(103, 203, 2, "failed")]
        for run_id in ("100", "101", "102", "103"):
            self.jobs[run_id][0]["steps"].insert(1, step("Restore the Go build cache", at(1), at(1, 5)))
        logs = {"200": excerpt(GO_LOG), "201": excerpt(GO_LOG), "202": excerpt(COLD_LOG)}
        ledger = measure.cache_ledger(self.runs, self.jobs, logs, "owner/repo")
        plain = measure.cohorts(self.runs, self.jobs, rows, "owner/repo")
        result = measure.cohorts(self.runs, self.jobs, rows, "owner/repo", ledger)
        config, before = result["configs"][0], plain["configs"][0]
        self.assertNotIn("by_cache", before)
        self.assertEqual((config["passed"], config["outcomes"]), (before["passed"], before["outcomes"]))
        self.assertEqual([sample["cache_condition"] for sample in config["samples"]],
                         ["go-build=partial-restore,go-module=hit", "go-build=partial-restore,go-module=hit",
                          "go-build=restore-failed,go-module=miss", "go-build=unknown"])
        warm = config["by_cache"]["go-build=partial-restore,go-module=hit"]
        self.assertEqual((warm["attempts"], warm["passed"]["native"]),
                         (2, {"count": 2, "min": 300.0, "median": 300.0, "max": 420.0}))
        self.assertEqual(config["by_cache"]["go-build=unknown"]["outcomes"],
                         {"assertion_failure": {"attempts": 1, "runner_seconds": 360.0}})
        self.assertEqual(config["by_cache"]["go-build=unknown"]["passed"]["duration"]["count"], 0)
        self.assertIn("| go-build=restore-failed,go-module=miss | 1 | passed 1 | ",
                      measure.cohorts_markdown(result))

    def test_cli_joins_the_ledger_from_the_cache(self):
        rows = [self.attempt(100, 200, 5)]
        cache, history = self.root / "cache", self.root / "history"
        for name in ("runs", "jobs", "cachelogs"):
            (cache / name).mkdir(parents=True)
        (cache / "runs" / "2026-10-17.json").write_text(json.dumps(self.runs))
        (cache / "jobs" / "100.json").write_text(json.dumps(self.jobs["100"]))
        (cache / "cachelogs" / "200.json").write_text(json.dumps(excerpt(COLD_LOG)))
        (history / "rows").mkdir(parents=True)
        (history / "rows" / "100-1.jsonl").write_text(json.dumps(rows[0]) + "\n")
        output = self.root / "cohorts.json"
        with unittest.mock.patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(measure.main(["cohorts", "--cache", str(cache), "--history", str(history),
                                           "--repo", "owner/repo", "--output", str(output)]), 0)
        sample = json.loads(output.read_text())["configs"][0]["samples"][0]
        self.assertEqual(sample["cache_condition"], "go-build=restore-failed,go-module=miss")
