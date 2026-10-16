"""Test the periodic matrix gate without GitHub.

Runs and jobs have the shape of the Actions API: a cancelled run is
status "completed" with conclusion "cancelled", and the jobs of a called
workflow are named "<caller> / Plan" and "<caller> / <batch>".
"""

import datetime
import io
import os
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

    def test_main_reads_every_page_of_the_week_and_excludes_its_own_run(self):
        # An execution on the second page must still be found.
        first = [run(200 + i, HEAD, 1 + i) for i in range(100)]
        responses = {
            "repos/o/r/actions/workflows/ceph-periodic.yml/runs?branch=main&per_page=100&created=%3E%3D2026-10-09T12:00:00Z":
                [{"workflow_runs": [run(9, HEAD, 0, status="in_progress"), *first]},
                 {"workflow_runs": [run(3, HEAD, 120)]}],
            "repos/o/r/actions/runs/3/jobs?per_page=100&filter=latest": [{"jobs": PLANNED}, {"jobs": EXECUTED[2:]}],
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
            self.assertNotIn("repos/o/r/actions/runs/9/jobs?per_page=100&filter=latest",
                             [call.args[0] for call in calls.call_args_list])


if __name__ == "__main__":
    unittest.main()
