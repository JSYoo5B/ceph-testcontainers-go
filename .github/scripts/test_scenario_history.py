"""Verify history rows join phases independently and never invent a pass."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import scenario_history as history


PROFILE = {"id": "recovery-rbd-12345678", "profile": "recovery/rbd/internal-integration", "category": "recovery",
           "batch": "rbd", "package": "./internal/integration", "tags": "ci,ci_recovery,ci_batch_rbd",
           "checker": "none", "case": "", "requires_ceph": True, "networks": []}
METADATA = {"repository": "owner/repo", "workflow": "owner/repo/.github/workflows/recovery.yml@refs/heads/main",
            "run_id": "100", "run_attempt": "1", "job_id": "200", "repetition_id": "1",
            "requested_head": "a" * 40, "checkout_commit": "b" * 40, "checkout_tree": "c" * 40,
            "event": "pull_request", "release": "20.2.4"}
PASSING_LOG = """=== RUN   TestRBDRecovery
=== RUN   TestRBDRecovery/primary
--- PASS: TestRBDRecovery (12.50s)
    --- PASS: TestRBDRecovery/primary (10.25s)
=== RUN   TestRBDSnapshot
--- PASS: TestRBDSnapshot (3.00s)
PASS
"""
FAILING_LOG = """=== RUN   TestRBDRecovery
=== RUN   TestRBDRecovery/primary
    recovery_test.go:10: wait expired
--- FAIL: TestRBDRecovery (40.00s)
    --- FAIL: TestRBDRecovery/primary (39.50s)
=== RUN   TestRBDSnapshot
FAIL
"""


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")


class Job:
    """One runtime job's artifact directories, built like the workflow's."""

    def __init__(self, root):
        self.root = Path(root)
        self.scenario, self.cleanup, self.images = (self.root / "scenario", self.root / "cleanup",
                                                    self.root / "images")

    def phases(self, **outcomes):
        values = {name: "success" for name in history.PHASES} | outcomes
        write(self.scenario / "phases.json", {"check": "Recovery", "profile": PROFILE["id"], "phases": [
            {"phase": name, "label": name, "outcome": values[name], "display": values[name] or "not run"}
            for name in history.PHASES]})

    def modules(self, passed=True):
        write(self.scenario / "modules/modules-report.json",
              {"passed": passed, "classification": "prepared" if passed else "modules-failure",
               "elapsed_seconds": 1.0, "exit_code": 0 if passed else 1, "timed_out": False})

    def compile(self, passed=True):
        report = {"profile": PROFILE, "passed": passed, "elapsed_seconds": 20.0, "timed_out": False,
                  "exit_code": 0 if passed else 1, "classification": "compiled" if passed else "compile-failure"}
        if passed:
            report["binary"] = {"binary": "internal-integration.test", "sha256": "d" * 64,
                                "build_key": {"go_version": "go1.26.2", "goos": "linux", "goarch": "amd64"},
                                "sources": {"go.mod": "e" * 64}, "tests": ["TestRBDRecovery", "TestRBDSnapshot"]}
        else:
            report["errors"] = []
        write(self.scenario / "compile/compile-report.json", report)
        write(self.scenario / "compile/compile.log", "" if passed else "undefined: helper\n")

    def native(self, log=PASSING_LOG, classification="passed", elapsed=15.5):
        passed = classification == "passed"
        write(self.scenario / "native/native.log", log)
        write(self.scenario / "native/report.json", {
            "profile": PROFILE, "network": "", "exit_code": 0 if passed else 1, "elapsed_seconds": elapsed,
            "completion": {"passed": passed, "errors": [] if passed else ["native log contains failed tests"]},
            "strict_completion": None, "classification": classification, "passed": passed,
            "binary": {"sha256": "d" * 64}})

    def docker(self, before=True, after=True):
        if before is not None:
            write(self.cleanup / "before.json", {"passed": before, "declared_github_revision": "b" * 40})
        if after is not None:
            report = {"passed": after, "new_resources": {"containers": [] if after else ["f" * 64],
                                                         "networks": [], "volumes": []}}
            if not after:
                report["error"] = "New Testcontainers resources remain after cleanup grace"
            write(self.cleanup / "after.json", report)

    def roles(self):
        write(self.images / "summary.json", {
            "release": "20.2.4", "variant": "official", "layout": "roles", "platform": "linux/amd64",
            "preparation_passed": True, "image_preparation_seconds": 30.0,
            "images": {role: {"reference": "ghcr.io/x/images:official-20.2.4-" + role,
                              "image_id": "sha256:" + str(index) * 64, "repo_digests": [],
                              "platform": "linux/amd64"} for index, role in enumerate(("control", "osd"))}})

    def passing(self):
        self.phases()
        self.modules()
        self.compile()
        self.docker()
        self.roles()
        self.native()
        return self

    def row(self, **metadata):
        return history.normalize(self.scenario, self.cleanup, self.images, METADATA | metadata)


def phase(row, name):
    return next(item for item in row["phases"] if item["phase"] == name)


class NormalizeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.job = Job(self.directory.name)

    def test_a_native_receipt_of_another_binary_is_not_comparable(self):
        # The same profile's native receipt ran an executable that the
        # compile receipt did not build: the row keeps both identities and
        # does not claim comparability, without inventing a failure.
        self.job.passing()
        report = json.loads((self.job.scenario / "native/report.json").read_text())
        report["binary"] = {"sha256": "9" * 64, "build_key": {"go_version": "go1.26.3"}}
        write(self.job.scenario / "native/report.json", report)
        row = self.job.row()
        self.assertEqual(row["build"]["native_binary"], "differs")
        self.assertEqual(row["build"]["binary_sha256"], "d" * 64)
        self.assertEqual(row["build"]["native_binary_sha256"], "9" * 64)
        self.assertEqual(row["build"]["native_build_key"], {"go_version": "go1.26.3"})
        self.assertEqual(row["comparability"], {"status": "incomplete", "reasons": ["native_binary_identity_mismatch"]})
        self.assertEqual(row["outcome"]["status"], "passed")
        self.assertEqual(phase(row, "native")["status"], "passed")
        # The matching case keeps the native identity too.
        self.job.native()
        matching = self.job.row()
        self.assertEqual(matching["build"]["native_binary_sha256"], "d" * 64)
        self.assertEqual(matching["comparability"], {"status": "complete", "reasons": []})

    def test_passing_job(self):
        row = self.job.passing().row()
        self.assertEqual(row["schema"], history.SCHEMA)
        self.assertEqual(row["outcome"]["status"], "passed")
        self.assertIsNone(row["first_failure"])
        self.assertEqual(row["execution_key"], {
            "repository": "owner/repo", "workflow": METADATA["workflow"], "run_id": "100", "run_attempt": "1",
            "job_id": "200", "repetition_id": "1", "profile_id": PROFILE["id"], "checker": "none", "case": "",
            "network": ""})
        self.assertEqual(row["source"]["requested_head"], "a" * 40)
        self.assertEqual(row["source"]["checkout_commit"], "b" * 40)
        self.assertEqual(row["build"]["native_binary"], "matches_compile")
        self.assertEqual(row["runtime"]["roles"]["osd"]["image_id"], "sha256:" + "1" * 64)
        self.assertEqual(row["comparability"], {"status": "complete", "reasons": []})
        self.assertEqual([(test["name"], test["result"], test["seconds"]) for test in row["tests"]], [
            ("TestRBDRecovery", "PASS", 12.5), ("TestRBDRecovery/primary", "PASS", 10.25),
            ("TestRBDSnapshot", "PASS", 3.0)])
        self.assertEqual(row["row_sha256"], history.row_hash(row))
        self.assertEqual(row, self.job.row(), "the same receipts must give the same row")

    def test_native_failure_keeps_first_failure(self):
        job = self.job.passing()
        job.phases(native="failure")
        job.native(FAILING_LOG, "native-test-failure", 41.0)
        row = job.row()
        self.assertEqual(row["outcome"]["status"], "failed")
        self.assertEqual(row["outcome"]["failed_phases"], ["native"])
        self.assertEqual(row["first_failure"], {
            "phase": "native", "workflow": "failure", "evidence": "failed",
            "classification": "native-test-failure", "error": "native log contains failed tests",
            "test": "TestRBDRecovery"})
        self.assertEqual([(test["name"], test["result"]) for test in row["tests"]], [
            ("TestRBDRecovery", "FAIL"), ("TestRBDRecovery/primary", "FAIL"), ("TestRBDSnapshot", "RUN")])

    def test_compile_failure_skips_later_phases(self):
        job = self.job
        job.phases(compile="failure", docker="skipped", images="skipped", native="skipped", cleanup="skipped")
        job.modules()
        job.compile(passed=False)
        row = job.row()
        self.assertEqual(row["outcome"]["status"], "failed")
        self.assertEqual(row["first_failure"]["phase"], "compile")
        self.assertEqual(row["first_failure"]["classification"], "compile-failure")
        self.assertEqual(row["outcome"]["skipped_phases"], ["docker", "images", "native", "cleanup"])
        self.assertEqual(phase(row, "native")["evidence"], "missing")
        self.assertEqual(row["tests"], "missing")
        self.assertEqual(row["build"]["build_key"], "missing")
        self.assertIn("missing_build_identity", row["comparability"]["reasons"])

    def test_cleanup_failure_after_native_pass_stays_a_cleanup_failure(self):
        job = self.job.passing()
        job.phases(cleanup="failure")
        (job.cleanup / "after.json").unlink()
        job.docker(before=None, after=False)
        row = job.row()
        self.assertEqual(phase(row, "native")["status"], "passed")
        self.assertEqual(phase(row, "cleanup")["status"], "failed")
        self.assertEqual(phase(row, "cleanup")["new_resources"], {"containers": 1, "networks": 0, "volumes": 0})
        self.assertEqual(row["outcome"]["status"], "failed")
        self.assertEqual(row["first_failure"]["phase"], "cleanup")

    def test_cleanup_receipt_failure_wins_over_step_success(self):
        job = self.job.passing()
        (job.cleanup / "after.json").unlink()
        job.docker(before=None, after=False)
        self.assertEqual(job.row()["outcome"]["status"], "failed")

    def test_missing_native_report_is_missing_not_passed(self):
        job = self.job.passing()
        (job.scenario / "native/report.json").unlink()
        row = job.row()
        self.assertEqual(phase(row, "native")["workflow"], "success")
        self.assertEqual(phase(row, "native")["evidence"], "missing")
        self.assertEqual(phase(row, "native")["status"], "missing")
        self.assertEqual(row["outcome"]["status"], "incomplete")
        self.assertEqual(row["outcome"]["missing_phases"], ["native"])

    def test_missing_phases_json_is_not_a_pass(self):
        job = self.job.passing()
        (job.scenario / "phases.json").unlink()
        row = job.row()
        self.assertEqual(row["outcome"]["status"], "incomplete")
        self.assertTrue(all(item["workflow"] == "missing" for item in row["phases"]))

    def test_unreadable_receipt_is_invalid(self):
        job = self.job.passing()
        write(job.scenario / "native/report.json", "{not json")
        row = job.row()
        self.assertEqual(phase(row, "native")["status"], "invalid")
        self.assertEqual(row["outcome"]["status"], "incomplete")
        self.assertIn("scenario/native/report.json", row["evidence"]["problems"])

    def test_profile_without_ceph_needs_no_images(self):
        global PROFILE
        saved = PROFILE
        PROFILE = saved | {"requires_ceph": False}
        self.addCleanup(lambda: globals().__setitem__("PROFILE", saved))
        job = self.job.passing()
        (job.images / "summary.json").unlink()
        job.phases(images="skipped")
        row = job.row()
        self.assertEqual(phase(row, "images")["status"], "not_required")
        self.assertEqual(row["runtime"]["status"], "not_applicable")
        self.assertEqual(row["outcome"]["status"], "passed")

    def test_cancelled_native_step(self):
        job = self.job.passing()
        job.phases(native="cancelled")
        (job.scenario / "native/report.json").unlink()
        row = job.row()
        self.assertEqual(row["outcome"]["status"], "cancelled")
        self.assertIsNone(row["first_failure"])

    def test_conflicting_profile_identity_is_rejected(self):
        self.job.passing()
        with self.assertRaises(ValueError):
            self.job.row(profile="other-profile")


class IngestTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.history = self.root / "history.jsonl"

    def row(self, name="job", **metadata):
        return Job(self.root / name).passing().row(**metadata)

    def test_identical_reingest_is_deduplicated(self):
        row = self.row()
        self.assertEqual(history.ingest(self.history, [row]), (1, 0))
        content = self.history.read_text()
        self.assertEqual(history.ingest(self.history, [row, row]), (0, 2))
        self.assertEqual(self.history.read_text(), content)

    def test_another_attempt_is_a_new_row(self):
        history.ingest(self.history, [self.row()])
        self.assertEqual(history.ingest(self.history, [self.row("retry", run_attempt="2")]), (1, 0))
        self.assertEqual(len(history.read_history(self.history)), 2)

    def test_conflicting_reingest_keeps_both_hashes(self):
        first = self.row()
        history.ingest(self.history, [first])
        content = self.history.read_text()
        job = Job(self.root / "job")
        job.phases(cleanup="failure")
        second = job.row()
        with self.assertRaises(history.ConflictError) as raised:
            history.ingest(self.history, [second])
        conflict = raised.exception.conflicts[0]
        self.assertEqual(conflict["existing"]["row_sha256"], first["row_sha256"])
        self.assertEqual(conflict["incoming"]["row_sha256"], second["row_sha256"])
        self.assertNotEqual(conflict["existing"]["evidence"], conflict["incoming"]["evidence"])
        self.assertEqual(self.history.read_text(), content)

    def test_edited_row_is_rejected(self):
        row = self.row()
        row["outcome"]["status"] = "failed"
        with self.assertRaises(ValueError):
            history.ingest(self.history, [row])

    def test_row_without_run_identity_is_rejected(self):
        with self.assertRaises(ValueError):
            history.ingest(self.history, [self.row(run_id="")])

    def test_command_line_reports_conflict(self):
        first = self.root / "first.json"
        second = self.root / "second.json"
        write(first, self.row())
        job = Job(self.root / "job")
        job.phases(native="failure")
        write(second, job.row())
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(history.main(["ingest", "--history", str(self.history), str(first)]), 0)
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            self.assertEqual(history.main(["ingest", "--history", str(self.history), str(second)]), 1)
        self.assertEqual(len(json.loads(error.getvalue())["conflicts"]), 1)


class SummaryTest(unittest.TestCase):
    def test_percentiles_and_cancelled_exclusion(self):
        with tempfile.TemporaryDirectory() as directory:
            root, rows = Path(directory), []
            for attempt, seconds in enumerate((10.0, 20.0, 30.0, 40.0, 50.0), 1):
                job = Job(root / str(attempt)).passing()
                job.native(elapsed=seconds)
                rows.append(job.row(run_attempt=str(attempt)))
            job = Job(root / "failed").passing()
            job.phases(native="failure")
            job.native(FAILING_LOG, "native-test-failure", 100.0)
            rows.append(job.row(run_attempt="6"))
            job = Job(root / "cancelled").passing()
            job.phases(native="cancelled")
            job.native(elapsed=1000.0)
            rows.append(job.row(run_attempt="7"))
            job = Job(root / "cleanup").passing()
            job.phases(cleanup="failure")
            rows.append(job.row(run_attempt="8"))
        result = history.summarize(rows)["profiles"][PROFILE["id"] + "@20.2.4"]
        self.assertEqual(result["rows"], 8)
        self.assertEqual(result["cancelled"], 1)
        self.assertEqual(result["failure_denominator"], 7)
        self.assertEqual(result["passed"], 5)
        self.assertEqual(result["failed"], 2)
        self.assertEqual(result["failures_by_phase"], {"cleanup": 1, "native": 1})
        self.assertEqual(result["failures_by_classification"],
                         {"cleanup:workflow-failure": 1, "native:native-test-failure": 1})
        # 10, 15.5, 20, 30, 40, 50, 100: the cancelled 1000 s is excluded.
        self.assertEqual(result["native_seconds"], {"samples": 7, "p50": 30.0, "p90": 100.0})

    def test_percentile_of_no_samples(self):
        self.assertIsNone(history.percentile([], 50))
        self.assertEqual(history.percentile([4.0], 90), 4.0)


if __name__ == "__main__":
    unittest.main()
