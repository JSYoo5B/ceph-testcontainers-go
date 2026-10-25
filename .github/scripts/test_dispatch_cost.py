"""Conserved job windows and refused comparisons, using the real existing joins."""
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import dispatch_cost as cost
import dispatch_join
import scenario_history
import tag_scenarios
from test_ci_measurements import BUILD_KEY, cache_section, log_text
from test_scenario_history import Job, PROFILE

REPO = "owner/repository"


def clock(second):
    return "2026-10-25T00:00:" + str(second).zfill(2) + "Z"


def step(name, start, end):
    return {"name": name, "conclusion": "success", "started_at": clock(start), "completed_at": clock(end)}


def job(identifier, name, run=700, start=10, end=30, steps=None, **extra):
    return {"id": identifier, "run_id": run, "run_attempt": 1, "name": "Scenarios / " + name,
            "url": "https://api.example.test/repos/" + REPO + "/actions/jobs/" + str(identifier),
            "status": "completed", "conclusion": "success", "runner_id": 7, "runner_name": "synthetic-runner",
            "labels": ["ubuntu-24.04"], "created_at": clock(0), "started_at": clock(start), "completed_at": clock(end),
            "steps": steps if steps is not None else [step("Set up job", start, 15),
                step("Native assertions and completion proof", 15, 25),
                step("Verify owned Docker resource cleanup", 25, 29), step("Complete job", 29, 30)], **extra}


class DispatchCostTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        fixture = Job(self.temporary.name).passing()
        self.row = fixture.row(repository=REPO, run_id="700", run_attempt="1", job_id="10")
        self.row["build"]["build_key"].update(package=PROFILE["package"], tags=PROFILE["tags"],
                                               go_environment=dict(tag_scenarios.GO_EXECUTION_ENV))
        self.row["build"]["native_build_key"] = copy.deepcopy(self.row["build"]["build_key"])
        self.rehash()
        self.entry = {"id": PROFILE["id"], "name": "rbd", "category": "recovery", "batch": "rbd",
                      "package": "./internal/integration", "network": "", "tests": "", "repetition": "1",
                      "artifact_suffix": "", "job_timeout": 35}
        # Serialize through the real expanded-dispatch producer.
        self.dispatch = tag_scenarios.dispatch_inventory({"include": [PROFILE]}, [self.entry], "default", 1,
            {"GITHUB_REPOSITORY": REPO, "GITHUB_RUN_ID": "700", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": "a" * 40,
             "SCENARIO_CATEGORY": "recovery", "SCENARIO_RELEASE": "20.2.4"})
        self.jobs = [job(1, "Plan", start=2, end=8, steps=[step("Set up job", 2, 4), step("Discover", 4, 8)]),
                     job(10, "rbd")]
        self.run = {"id": 700, "run_attempt": 1, "name": "Ceph recovery", "event": "pull_request",
                    "repository": {"full_name": REPO}, "created_at": clock(0), "head_sha": "a" * 40,
                    "status": "completed", "conclusion": "success"}
        self.receipts = ([("native/history-row.json", self.row)], [])

    def assess(self, **kwargs):
        return cost.assess(self.dispatch, [{"total_count": len(self.jobs), "jobs": self.jobs}],
                           self.run, self.receipts, **kwargs)

    def rehash(self):
        self.row["row_sha256"] = scenario_history.row_hash(self.row)

    def second(self):
        self.run["id"] = 701
        self.dispatch["context"]["run_id"] = "701"
        for j in self.jobs:
            j["run_id"] = 701
            j["id"] += 20
            j["url"] = "https://api.example.test/repos/" + REPO + "/actions/jobs/" + str(j["id"])
        self.row["execution_key"].update(run_id="701", job_id="30")
        self.rehash()
        return self.assess()

    def test_producer_dispatch_joins_and_cost_is_conserved_once(self):
        report = self.assess()
        self.assertTrue(report["comparison"]["eligible"], report["comparison"]["reasons"])
        self.assertTrue(report["conservation"]["ok"])
        self.assertEqual(report["conservation"]["known_runner_seconds"], 26)
        self.assertEqual(report["metrics"]["observed_wall_seconds"], 30)
        self.assertEqual(report["metrics"]["known_step_seconds"]["native"], 10)
        self.assertEqual(report["buckets"]["matched_passed"]["known_runner_seconds"], 20)
        self.assertEqual(report["buckets"]["unmatched_actual"]["known_runner_seconds"], 6)
        self.assertFalse(report["claims"]["billing"])
        self.assertFalse(report["claims"]["dag_critical_path"])

    def test_two_equivalent_complete_attempts_have_descriptive_ranges(self):
        first, second = self.assess(), self.second()
        report = cost.compare([first, second])
        self.assertEqual(report["status"], "matched", report)
        self.assertEqual(report["matched_ranges"]["known_runner_seconds"], {"count": 2, "min": 26, "median": 26, "max": 26})
        self.assertFalse(report["claims"]["causal_savings"])
        self.assertEqual(cost.compare([first, first])["status"], "uncomparable")

    def test_same_profile_but_different_source_image_network_or_cache_never_matches(self):
        first = self.assess()
        self.second()
        for field, edit in (
            ("source", lambda: self.row["source"].update(checkout_tree="f" * 40)),
            ("compiled source", lambda: self.row["build"].update(sources_sha256="f" * 64)),
            ("native binding", lambda: self.row["build"].update(native_build_key={})),
            ("image", lambda: self.row["runtime"]["roles"]["control"].update(image_id="sha256:" + "8" * 64)),
            ("network", lambda: self.row["execution_key"].update(network="bridge")),
            ("platform", lambda: self.row["runtime"].update(platform="linux/arm64")),
        ):
            with self.subTest(field=field):
                baseline = copy.deepcopy(self.row)
                edit(); self.rehash()
                report = self.assess()
                self.assertEqual(cost.compare([first, report])["status"], "uncomparable")
                self.row.clear(); self.row.update(baseline)
        self.jobs[1]["steps"][0]["name"] = "Restore the Go build cache"
        report = self.assess()
        self.assertIn("unknown_cache_condition", report["comparison"]["reasons"])
        self.assertEqual(cost.compare([first, report])["status"], "uncomparable")

    def test_foreign_repository_and_missing_dispatch_context_never_match(self):
        first = self.assess()
        self.second()
        self.run["repository"]["full_name"] = "other/repository"
        self.dispatch["context"]["repository"] = "other/repository"
        self.row["execution_key"]["repository"] = "other/repository"
        for job in self.jobs:
            job["url"] = job["url"].replace(REPO, "other/repository")
        self.rehash()
        second = self.assess()
        self.assertTrue(second["comparison"]["eligible"])
        self.assertEqual(cost.compare([first, second])["status"], "uncomparable")
        del self.dispatch["networks"]
        self.assertFalse(self.assess()["comparison"]["eligible"])

    def cache_rows(self, *, restore=True):
        lines = []
        if restore:
            lines += cache_section("restore", BUILD_KEY, "~/.cache/go-build",
                                   "Cache restored from key: " + BUILD_KEY)
        lines += cache_section("save", BUILD_KEY, "~/.cache/go-build",
                               "Cache saved with key: " + BUILD_KEY)
        excerpt, truncated = cost.measurements.cache_excerpt(log_text(*lines))
        return cost.measurements.cache_ledger([self.run], {str(self.run["id"]): self.jobs},
            {str(self.jobs[1]["id"]): {"lines": excerpt, "truncated": truncated}}, REPO)

    def test_partial_cache_ledger_cannot_upgrade_missing_restore_evidence(self):
        self.jobs[1]["steps"][0]["name"] = "Restore the Go build cache"
        self.jobs[1]["steps"][-1]["name"] = "Save the Go build cache"
        first = None
        for attempt in range(2):
            if attempt:
                self.second()
            rows = self.cache_rows(restore=False)
            self.assertEqual([row["outcome"] for row in rows], ["unknown", "saved"])
            for ledger in ([], rows, [row for row in rows if row["operation"] == "save"]):
                report = self.assess(cache_ledger=ledger)
                self.assertIn("unknown_cache_condition", report["comparison"]["reasons"])
                if first is not None:
                    self.assertEqual(cost.compare([first, report])["status"], "uncomparable")
            first = report

    def test_each_cache_step_requires_its_own_matching_operation(self):
        self.jobs[1]["steps"][0]["name"] = "Restore the Go build cache"
        self.jobs[1]["steps"][-1]["name"] = "Save the Go build cache"
        rows = self.cache_rows()
        self.assertTrue(self.assess(cache_ledger=rows)["comparison"]["eligible"])
        for change in ({"step": "Restore another cache"}, {"operation": "save"},
                       {"kind": "go-module"}, {"evidence_source": "step"}):
            with self.subTest(change=change):
                altered = [rows[0] | change, rows[1]]
                self.assertFalse(self.assess(cache_ledger=altered)["comparison"]["eligible"])
        self.jobs[1]["steps"][1]["name"] = "Restore the Go build cache"
        self.assertFalse(self.assess(cache_ledger=rows)["comparison"]["eligible"],
                         "one row cannot cover two observed operations")

    def test_complete_producer_cache_ledgers_still_compare(self):
        self.jobs[1]["steps"][0]["name"] = "Restore the Go build cache"
        self.jobs[1]["steps"][-1]["name"] = "Save the Go build cache"
        first = self.assess(cache_ledger=self.cache_rows())
        self.second()
        second = self.assess(cache_ledger=self.cache_rows())
        self.assertEqual(cost.compare([first, second])["status"], "matched")

    def test_failed_and_cancelled_allocations_retain_cost(self):
        for conclusion in ("failure", "cancelled", "timed_out"):
            with self.subTest(conclusion=conclusion):
                self.jobs[1]["conclusion"] = conclusion
                self.run["conclusion"] = conclusion
                report = self.assess()
                self.assertEqual(report["buckets"]["matched_" + conclusion]["known_runner_seconds"], 20)
                self.assertEqual(report["metrics"]["known_runner_seconds"], 26)
                self.assertFalse(report["comparison"]["eligible"])

    def test_empty_steps_with_assigned_runner_keep_measured_cancelled_window(self):
        self.jobs[1].update(steps=[], conclusion="cancelled")
        report = self.assess()
        record = next(r for r in report["jobs"] if r["job_id"] == "10")
        self.assertEqual(record["allocation"], "allocated")
        self.assertEqual(record["runner_seconds"], 20)
        self.assertEqual(record["unattributed_seconds"], 20)
        self.assertEqual(report["metrics"]["known_runner_seconds"], 26)
        self.assertFalse(report["comparison"]["eligible"])

    def test_executed_step_evidence_requires_a_valid_recorded_outcome(self):
        self.jobs[1].update(runner_id=0, runner_name="")
        self.assertEqual(self.assess()["metrics"]["known_runner_seconds"], 26)
        for invalid in (None, False, 0, "skipped", "invalid"):
            with self.subTest(outcome=invalid):
                self.jobs[1]["steps"] = [step("Set up job", 10, 15) | {"conclusion": invalid}]
                report = self.assess()
                self.assertEqual(report["conservation"]["unknown_cost_jobs"], 1)
                self.assertEqual(report["metrics"]["known_runner_seconds"], 6)
                self.assertFalse(report["comparison"]["eligible"])

    def test_empty_steps_without_assignment_or_missing_window_stay_unknown_not_zero(self):
        self.jobs[1].update(steps=[], conclusion="cancelled", runner_id=0, runner_name="")
        for assigned in (False, True):
            with self.subTest(assigned=assigned):
                if assigned:
                    self.jobs[1].update(runner_id=7, completed_at=None)
                report = self.assess()
                record = next(r for r in report["jobs"] if r["job_id"] == "10")
                self.assertIsNone(record["runner_seconds"])
                self.assertEqual(report["conservation"]["unknown_cost_jobs"], 1)
                self.assertEqual(report["metrics"]["known_runner_seconds"], 6)
                self.assertFalse(report["comparison"]["eligible"])

    def test_identical_duplicate_job_is_counted_once_but_not_comparable(self):
        self.jobs.append(copy.deepcopy(self.jobs[1]))
        report = self.assess()
        self.assertEqual(report["conservation"]["duplicate_records"], 1)
        self.assertEqual(report["metrics"]["known_runner_seconds"], 26)
        self.assertEqual(report["buckets"]["conflicts"]["known_runner_seconds"], 20)
        self.assertFalse(report["comparison"]["eligible"])

    def test_conflicting_duplicate_windows_are_not_arbitrarily_chosen(self):
        self.jobs.append(copy.deepcopy(self.jobs[1]) | {"completed_at": clock(35)})
        report = self.assess()
        record = next(r for r in report["jobs"] if r["job_id"] == "10")
        self.assertIsNone(record["runner_seconds"])
        self.assertEqual([r["runner_seconds"] for r in record["candidate_windows"]], [20, 25])
        self.assertEqual(report["metrics"]["known_runner_seconds"], 6)
        self.assertTrue(report["conservation"]["ok"])

    def test_missing_malformed_and_reversed_timestamps_never_become_free_jobs(self):
        original = copy.deepcopy(self.jobs[1])
        for change in ({"completed_at": None}, {"started_at": "invalid"}, {"completed_at": clock(5)},
                       {"steps": [None]}, {"created_at": "invalid"}, {"steps": None}):
            with self.subTest(change=change):
                self.jobs[1] = original | change
                report = self.assess()
                self.assertFalse(report["comparison"]["eligible"])
                self.assertTrue(report["conservation"]["ok"])
                record = next(r for r in report["jobs"] if r["job_id"] == "10")
                self.assertNotEqual(record["runner_seconds"], 0)

    def test_unmatched_and_out_of_scope_windows_are_kept(self):
        self.jobs.append(job(99, "stray"))
        report = self.assess()
        self.assertEqual(report["metrics"]["known_runner_seconds"], 46)
        self.assertEqual(report["buckets"]["unmatched_actual"]["known_runner_seconds"], 26)
        self.jobs[-1]["name"] = "Other / stray"
        report = self.assess()
        self.assertEqual(report["buckets"]["out_of_scope"]["known_runner_seconds"], 20)
        self.assertFalse(report["comparison"]["eligible"])

    def test_incomplete_or_conflicting_receipts_keep_allocation_cost(self):
        self.receipts = ([], [])
        report = self.assess()
        self.assertEqual(report["buckets"]["matched_incomplete"]["known_runner_seconds"], 20)
        other = copy.deepcopy(self.row)
        other["source"]["checkout_tree"] = "9" * 40
        other["row_sha256"] = scenario_history.row_hash(other)
        self.receipts = ([("one/history-row.json", self.row), ("two/history-row.json", other)], [])
        report = self.assess()
        self.assertEqual(report["buckets"]["conflicts"]["known_runner_seconds"], 20)
        self.assertFalse(report["comparison"]["eligible"])

    def test_partial_job_page_and_foreign_run_are_inconclusive(self):
        with self.assertRaises(dispatch_join.Inconclusive):
            cost.assess(self.dispatch, [{"total_count": 9, "jobs": self.jobs}], self.run)
        with self.assertRaises(ValueError):
            cost.assess(self.dispatch, [{"total_count": 2, "jobs": self.jobs}], self.run | {"id": 999})

    def test_unidentified_and_bad_image_records_remain_uncomparable(self):
        del self.jobs[1]["id"]
        report = self.assess()
        self.assertEqual(report["conservation"]["unknown_cost_jobs"], 1)
        self.assertFalse(report["comparison"]["eligible"])
        self.jobs[1]["id"] = 10
        self.row["runtime"]["roles"]["control"]["image_id"] = "mutable-tag"
        self.rehash()
        self.assertFalse(self.assess()["comparison"]["eligible"])

    def test_mutated_report_cannot_supply_a_comparison_sample(self):
        first, second = self.assess(), self.second()
        second["metrics"]["known_runner_seconds"] = 0
        with self.assertRaisesRegex(ValueError, "intact"):
            cost.compare([first, second])

    def test_cli_is_offline_and_writes_a_report(self):
        root = Path(self.temporary.name)
        for name, value in (("dispatch", self.dispatch), ("run", self.run),
                            ("jobs", {"total_count": len(self.jobs), "jobs": self.jobs})):
            (root / (name + ".json")).write_text(json.dumps(value))
        with mock.patch("sys.stdout", new=io.StringIO()), mock.patch("sys.stderr", new=io.StringIO()):
            code = cost.main(["assess", "--dispatch", str(root / "dispatch.json"), "--run", str(root / "run.json"),
                              "--jobs", str(root / "jobs.json"), "--output", str(root / "report.json")])
        self.assertEqual(code, 0)
        report = json.loads((root / "report.json").read_bytes())
        self.assertTrue(report["conservation"]["ok"])
        self.assertFalse(report["comparison"]["eligible"], "missing execution receipts cannot become a comparable total")


if __name__ == "__main__":
    unittest.main()
