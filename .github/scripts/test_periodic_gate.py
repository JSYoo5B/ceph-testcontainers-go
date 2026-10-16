"""Test the periodic matrix gate without GitHub."""

import datetime
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import periodic_gate as gate

NOW = datetime.datetime(2026, 10, 16, 12, 0, tzinfo=datetime.timezone.utc)
HEAD, OLD = "a" * 40, "b" * 40


def run(number, sha, hours_ago, status="completed"):
    created = (NOW - datetime.timedelta(hours=hours_ago)).isoformat().replace("+00:00", "Z")
    return {"id": number, "head_sha": sha, "created_at": created, "status": status}


EXECUTED = [{"name": "Gate", "conclusion": "success"}, {"name": "Squid short / Plan", "conclusion": "success"},
            {"name": "Squid short / rbd_fixtures_locks", "conclusion": "failure"}]
SKIPPED = [{"name": "Gate", "conclusion": "success"}, {"name": "Squid short", "conclusion": "skipped"},
           {"name": "Tentacle short", "conclusion": "skipped"}]


class GateTests(unittest.TestCase):
    def decide(self, runs, executed_ids, event="schedule", release_phase=False):
        return gate.decide(event, release_phase, HEAD, NOW, runs,
                           lambda r: EXECUTED if r["id"] in executed_ids else SKIPPED)

    def test_a_new_commit_on_main_runs_once(self):
        runs = [run(3, HEAD, 3), run(2, OLD, 6)]
        self.assertEqual(self.decide(runs, {2})[0], True)
        # Once a run executed it, later schedules on the same commit skip,
        # however many skipped gates came in between.
        runs = [run(5, HEAD, 1), run(4, HEAD, 3), run(3, HEAD, 6), run(2, OLD, 9)]
        executed, reason = self.decide(runs, {3, 2})
        self.assertFalse(executed)
        self.assertIn("already executed", reason)

    def test_an_execution_in_progress_counts(self):
        self.assertFalse(self.decide([run(3, HEAD, 1, status="in_progress")], {3})[0])

    def test_a_failed_execution_is_not_repeated_until_the_commit_changes(self):
        # A failure stays a recorded first failure; the gate does not rerun it.
        self.assertFalse(self.decide([run(3, HEAD, 2)], {3})[0])

    def test_an_unchanged_commit_repeats_after_a_week(self):
        executed, reason = self.decide([run(3, HEAD, 24 * 7)], {3})
        self.assertTrue(executed)
        self.assertIn("7 days", reason)
        self.assertFalse(self.decide([run(3, HEAD, 24 * 7 - 1)], {3})[0])

    def test_release_phase_and_dispatch_always_run(self):
        runs = [run(3, HEAD, 1)]
        self.assertEqual(self.decide(runs, {3}, release_phase=True), (True, "release phase"))
        self.assertEqual(self.decide(runs, {3}, event="workflow_dispatch"), (True, "dispatched by hand"))

    def test_without_an_earlier_execution_it_runs(self):
        self.assertTrue(self.decide([], set())[0])
        self.assertTrue(self.decide([run(3, HEAD, 1), run(2, HEAD, 4)], set())[0])
        # A cancelled or otherwise unknown status is not an execution.
        self.assertTrue(self.decide([run(3, HEAD, 1, status="cancelled")], {3})[0])

    def test_main_writes_the_decision_and_excludes_its_own_run(self):
        responses = {
            "repos/o/r/actions/workflows/ceph-periodic.yml/runs?branch=main&per_page=50":
                {"workflow_runs": [run(9, HEAD, 0, status="in_progress"), run(3, HEAD, 2)]},
            "repos/o/r/actions/runs/3/jobs?per_page=100": {"jobs": EXECUTED},
        }
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            environment = {"GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "9", "GITHUB_EVENT_NAME": "schedule",
                           "GITHUB_SHA": HEAD, "GITHUB_OUTPUT": str(output), "CI_RELEASE_PHASE": ""}
            with mock.patch.dict(os.environ, environment, clear=True), \
                    mock.patch.object(gate, "github", side_effect=lambda path: responses[path]) as api, \
                    mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(gate.main(["--workflow", "ceph-periodic.yml"]), 0)
            self.assertEqual(output.read_text().splitlines()[0], "run=false")
            self.assertNotIn("repos/o/r/actions/runs/9/jobs?per_page=100", [call.args[0] for call in api.call_args_list])


if __name__ == "__main__":
    unittest.main()
