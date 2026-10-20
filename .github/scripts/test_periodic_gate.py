"""Test the periodic matrix gate without GitHub.

Runs and jobs have the shape of the Actions API: a cancelled run is
status "completed" with conclusion "cancelled", and the jobs of a called
workflow are named "<caller> / Plan" and "<caller> / <batch>".
"""

import datetime
import io
import os
import subprocess
import re
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import periodic_gate as gate

NOW = datetime.datetime(2026, 10, 16, 12, 0, tzinfo=datetime.timezone.utc)
HEAD, OLD = "a" * 40, "b" * 40


def run(number, sha, hours_ago, status="completed", conclusion="success"):
    created = (NOW - datetime.timedelta(hours=hours_ago)).isoformat().replace("+00:00", "Z")
    return {"id": number, "head_sha": sha, "created_at": created, "status": status,
            "conclusion": None if status != "completed" else conclusion}


def job(name, conclusion, status="completed"):
    return {"name": name, "status": status, "conclusion": conclusion if status == "completed" else None}


GATE_ONLY = [job("Gate", "success"), job("Squid short", "skipped"), job("Tentacle short", "skipped")]
PLANNED = [job("Gate", "success"), job("Tentacle short / Plan", "success")]
EXECUTED = [*PLANNED, job("Tentacle short / rbd_fixtures_locks", "success")]
FAILED = [*PLANNED, job("Tentacle short / rbd_fixtures_locks", "failure")]


class GateTests(unittest.TestCase):
    def decide(self, runs, jobs, event="schedule", release_phase=False):
        """jobs maps a run id to its jobs; any other run had only its gate."""
        return gate.decide(event, release_phase, HEAD, NOW, runs, lambda r: jobs.get(r["id"], GATE_ONLY))

    def test_a_new_commit_on_main_runs_once(self):
        self.assertTrue(self.decide([run(3, HEAD, 3), run(2, OLD, 6)], {2: EXECUTED})[0])
        # Once a run executed it, later firings on the same commit skip,
        # however many skipped gates came in between.
        executed, reason = self.decide([run(5, HEAD, 1), run(4, HEAD, 3), run(3, HEAD, 6), run(2, OLD, 9)],
                                       {3: EXECUTED, 2: EXECUTED})
        self.assertFalse(executed)
        self.assertIn("already executed", reason)

    def test_a_run_cancelled_before_its_batches_is_not_an_execution(self):
        # The review's case: Gate and a Plan job succeeded, then the run was
        # cancelled before any batch started.
        cancelled = run(3, HEAD, 1, conclusion="cancelled")
        self.assertTrue(self.decide([cancelled], {3: PLANNED})[0])
        # Batches that finished before the cancellation do not make it one.
        self.assertTrue(self.decide([cancelled], {3: [*EXECUTED, job("Squid short / pools", "cancelled")]})[0])
        # A run that ended after Gate and Plan alone executed nothing either.
        self.assertTrue(self.decide([run(3, HEAD, 1)], {3: PLANNED})[0])

    def test_a_failed_execution_is_recorded_and_not_repeated(self):
        executed, reason = self.decide([run(3, HEAD, 2, conclusion="failure")], {3: FAILED})
        self.assertFalse(executed)
        self.assertIn("already executed", reason)

    def test_an_execution_in_progress_counts_once_a_batch_started(self):
        going = run(3, HEAD, 1, status="in_progress")
        started = [*PLANNED, job("Tentacle short / rbd_fixtures_locks", None, status="in_progress")]
        self.assertFalse(self.decide([going], {3: started})[0])
        self.assertTrue(self.decide([going], {3: PLANNED})[0])

    def test_an_unchanged_commit_repeats_after_a_week(self):
        executed, reason = self.decide([run(3, HEAD, 24 * 7)], {3: EXECUTED})
        self.assertTrue(executed)
        self.assertIn("7 days", reason)
        self.assertFalse(self.decide([run(3, HEAD, 24 * 7 - 1)], {3: EXECUTED})[0])

    def test_an_execution_behind_more_than_fifty_skipped_gates_is_remembered(self):
        # The review's case: one execution, then 55 skipped 3-hourly gates,
        # all within the week.
        skipped = [run(100 + i, HEAD, 3 * (i + 1)) for i in range(55)]
        executed, reason = self.decide([*skipped, run(3, HEAD, 3 * 56 - 1)], {3: EXECUTED})
        self.assertFalse(executed)
        self.assertIn("already executed", reason)

    def test_release_phase_and_dispatch_always_run(self):
        runs = [run(3, HEAD, 1)]
        self.assertEqual(self.decide(runs, {3: EXECUTED}, release_phase=True), (True, "release phase"))
        self.assertEqual(self.decide(runs, {3: EXECUTED}, event="workflow_dispatch"), (True, "dispatched by hand"))

    def test_without_an_earlier_execution_it_runs(self):
        self.assertTrue(self.decide([], {})[0])
        self.assertTrue(self.decide([run(3, HEAD, 1), run(2, HEAD, 4)], {})[0])

    def test_a_run_that_ended_in_planning_is_not_an_execution(self):
        # The review's case: the generated Releases planner succeeded, a
        # called Plan job failed and no batch started. Planning is not native
        # work, so the next firing runs the commit again.
        planned = [job("Gate", "success"), job("Releases", "success"), job("Environment / Plan", "success"),
                   job("Tentacle short / Plan", "failure"), job("Tentacle short / rbd_fixtures_locks", "skipped")]
        failed = run(3, HEAD, 1, conclusion="failure")
        self.assertFalse(gate.executed(failed, planned))
        self.assertTrue(self.decide([failed], {3: planned})[0])
        # A Releases job that failed, or one still running, is no execution
        # either.
        self.assertFalse(gate.executed(failed, [job("Gate", "success"), job("Releases", "failure")]))
        self.assertFalse(gate.executed(run(3, HEAD, 1, status="in_progress"),
                                       [job("Gate", "success"), job("Releases", None, status="in_progress")]))
        # Positive control: once a generated caller's batch ran to an end,
        # passing or failing, the run executed the matrix.
        for conclusion in ("success", "failure"):
            ran = [*planned[:3], job("Tentacle short / Plan", "success"),
                   job("Tentacle short / rbd_fixtures_locks", conclusion)]
            self.assertTrue(gate.executed(failed, ran))
            self.assertFalse(self.decide([failed], {3: ran})[0])

    def test_campaign_runs_are_not_executions_of_the_matrix(self):
        # A daily campaign on the current commit must not make the gate skip
        # a commit whose matrix never ran.
        campaign = [job("Gate", "success"), job("Pin", "success"), job("Representative / Plan", "success"),
                    job("Representative / client_auth #1", "failure"), job("History", "success")]
        self.assertTrue(self.decide([run(3, HEAD, 1)], {3: campaign})[0])
        self.assertFalse(self.decide([run(3, HEAD, 1)], {3: [*EXECUTED, job("History", "success")]})[0])

    def test_campaign_schedule_and_representative_dispatch_select_the_campaign(self):
        cron = "23 4 * * *"
        self.assertEqual(gate.campaign("schedule", cron, cron, ""), (True, "campaign schedule"))
        self.assertEqual(gate.campaign("schedule", "41 */3 * * *", cron, ""), (False, ""))
        self.assertEqual(gate.campaign("schedule", "", "", ""), (False, ""))
        self.assertEqual(gate.campaign("workflow_dispatch", "", cron, "representative"),
                         (True, "campaign dispatched by hand"))
        self.assertEqual(gate.campaign("workflow_dispatch", "", cron, "full"), (False, ""))
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            environment = {"GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "9", "GITHUB_EVENT_NAME": "schedule",
                           "GITHUB_SHA": HEAD, "GITHUB_OUTPUT": str(output), "PERIODIC_SCHEDULE": cron}
            with mock.patch.dict(os.environ, environment, clear=True), \
                    mock.patch.object(gate, "github") as calls, mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(gate.main(["--workflow", "ceph-periodic.yml", "--campaign-schedule", cron]), 0)
            calls.assert_not_called()
            self.assertEqual(output.read_text().splitlines(), ["run=false", "reason=campaign schedule", "campaign=true"])

    def test_main_reads_every_page_of_the_week_and_excludes_its_own_run(self):
        # An execution on the second page must still be found.
        first = [run(200 + i, HEAD, 1 + i) for i in range(100)]
        responses = {
            "repos/o/r/actions/workflows/ceph-periodic.yml/runs?branch=main&per_page=100&created=%3E%3D2026-10-09T12:00:00Z":
                [{"workflow_runs": [run(9, HEAD, 0, status="in_progress"), *first]},
                 {"workflow_runs": [run(3, HEAD, 120)]}],
            "repos/o/r/actions/runs/3/jobs?per_page=25&filter=latest": [{"jobs": PLANNED}, {"jobs": EXECUTED[2:]}],
        }

        def api(path):
            if path in responses:
                return responses[path]
            self.assertRegex(path, r"^repos/o/r/actions/runs/2\d\d/jobs")
            return [{"jobs": GATE_ONLY}]

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            environment = {"GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "9", "GITHUB_EVENT_NAME": "schedule",
                           "GITHUB_SHA": HEAD, "GITHUB_OUTPUT": str(output), "CI_RELEASE_PHASE": ""}
            with mock.patch.dict(os.environ, environment, clear=True), \
                    mock.patch.object(gate, "github", side_effect=api) as calls, \
                    mock.patch.object(gate.datetime, "datetime", wraps=datetime.datetime) as clock, \
                    mock.patch("sys.stdout", new=io.StringIO()):
                clock.now.return_value = NOW
                self.assertEqual(gate.main(["--workflow", "ceph-periodic.yml"]), 0)
            self.assertEqual(output.read_text().splitlines()[0], "run=false")
            self.assertEqual(output.read_text().splitlines()[2], "campaign=false")
            self.assertNotIn("repos/o/r/actions/runs/9/jobs?per_page=25&filter=latest",
                             [call.args[0] for call in calls.call_args_list])


class ReaderTests(unittest.TestCase):
    """The gate's GitHub reads: bounded retries of transient errors only."""

    def reader(self, outcomes, budget=gate.READ_BUDGET):
        self.calls, self.sleeps, self.now = [], [], [0.0]
        outcomes = list(outcomes)

        def run(args, **kwargs):
            self.calls.append((args, kwargs["timeout"]))
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            code, stdout, stderr = outcome
            return subprocess.CompletedProcess(args, code, stdout, stderr)

        def sleep(seconds):
            self.sleeps.append(seconds)
            self.now[0] += seconds

        return gate.Reader(run=run, sleep=sleep, clock=lambda: self.now[0], budget=budget)

    def test_a_transient_failure_is_retried_until_the_pages_arrive(self):
        read = self.reader([(1, "", "gh: Server Error (HTTP 502)"),
                            (1, "", "stream error: stream ID 1; CANCEL; received from peer"),
                            subprocess.TimeoutExpired("gh", 60),
                            (0, '[{"jobs": [1]}, {"jobs": [2]}]', "")])
        with mock.patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(read("repos/o/r/actions/runs/3/jobs"), [{"jobs": [1]}, {"jobs": [2]}])
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(self.sleeps, list(gate.RETRY_DELAYS[:3]))

    def test_exhausted_retries_fail_with_the_last_error(self):
        read = self.reader([(1, "", "gh: Server Error (HTTP 503)")] * (len(gate.RETRY_DELAYS) + 1))
        with mock.patch("sys.stdout", new=io.StringIO()), self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            read("repos/o/r/actions/runs/3/jobs")
        self.assertEqual(len(self.calls), len(gate.RETRY_DELAYS) + 1)

    def test_the_time_budget_bounds_the_retries(self):
        read = self.reader([(1, "", "gh: Server Error (HTTP 502)")] * 10, budget=datetime.timedelta(seconds=10))
        with mock.patch("sys.stdout", new=io.StringIO()), self.assertRaisesRegex(RuntimeError, "HTTP 502"):
            read("repos/o/r/actions/runs/3/jobs")
        self.assertLess(self.now[0], 10)
        self.assertTrue(all(timeout <= 10 for _, timeout in self.calls))

    def test_permanent_errors_are_not_retried(self):
        for stderr in ("gh: Bad credentials (HTTP 401)", "gh: Resource not accessible by integration (HTTP 403)",
                       "gh: Not Found (HTTP 404)"):
            read = self.reader([(1, "", stderr)])
            with self.assertRaisesRegex(RuntimeError, re.escape(stderr)):
                read("repos/o/r/actions/runs/3/jobs")
            self.assertEqual((len(self.calls), self.sleeps), (1, []))

    def test_a_failed_process_is_a_failure_even_without_stderr(self):
        # Valid JSON on stdout of a failed gh cannot prove a complete history.
        for stderr in ("", "  \n"):
            read = self.reader([(1, '[{"jobs": []}]', stderr)])
            with self.assertRaisesRegex(RuntimeError, "exit status 1"):
                read("repos/o/r/actions/runs/3/jobs")
            self.assertEqual((len(self.calls), self.sleeps), (1, []))
        read = self.reader([(0, '[{"jobs": []}]', "")])
        self.assertEqual(read("repos/o/r/actions/runs/3/jobs"), [{"jobs": []}])

    def test_malformed_json_fails_instead_of_reading_as_empty(self):
        read = self.reader([(0, '[{"jobs": [', "")])
        with self.assertRaisesRegex(RuntimeError, "malformed JSON"):
            read("repos/o/r/actions/runs/3/jobs")

    def test_an_unreadable_history_fails_the_gate(self):
        # Never an empty history: main raises, so the Gate job fails.
        def api(path):
            raise RuntimeError("gh api " + path + " failed: gh: Server Error (HTTP 502)")

        environment = {"GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "9", "GITHUB_EVENT_NAME": "schedule",
                       "GITHUB_SHA": HEAD, "CI_RELEASE_PHASE": ""}
        with mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.object(gate, "github", side_effect=api), \
                self.assertRaisesRegex(RuntimeError, "HTTP 502"):
            gate.main(["--workflow", "ceph-periodic.yml"])


if __name__ == "__main__":
    unittest.main()
