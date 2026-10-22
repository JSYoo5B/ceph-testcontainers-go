"""Verify history rows join phases independently and never invent a pass."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

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
# A run that stopped at TestRBDRecovery: TestRBDSnapshot never started.
TRUNCATED_LOG = """=== RUN   TestRBDRecovery
    recovery_test.go:10: wait expired
--- FAIL: TestRBDRecovery (40.00s)
panic: test timed out after 45s
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


# A synthetic bug ID: a test input only, never registered in the catalogue.
BUG = "ceph_bug_synthetic_a"


def select_bug(job, compile_selection=(BUG,), native_selection=(BUG,)):
    """Rewrite a job's receipts as if built with a bug compatibility selection."""
    for path, selection in (("compile/compile-report.json", compile_selection),
                            ("native/report.json", native_selection)):
        report = json.loads((job.scenario / path).read_text())
        report["profile"] = PROFILE | {"tags": ",".join((PROFILE["tags"], *selection)),
                                       "bug_compatibility": list(selection)}
        if "build_key" in (report.get("binary") or {}):
            report["binary"]["build_key"] = report["binary"]["build_key"] | {"tags": report["profile"]["tags"]}
        write(job.scenario / path, report)
    return job


class BugCompatibilityProvenanceTest(unittest.TestCase):
    """A row names the selected paths of its receipts, and refuses receipts
    of another selection than the workflow's or than each other's."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_row_records_the_receipts_selection(self):
        ordinary = Job(self.root / "ordinary").passing().row(bug_compatibility="")
        self.assertEqual(ordinary["profile"]["bug_compatibility"], [])
        selected = select_bug(Job(self.root / "selected").passing()).row(bug_compatibility=" " + BUG + " ,")
        self.assertEqual(selected["profile"]["bug_compatibility"], [BUG])
        self.assertIn(BUG, selected["build"]["tags"].split(","))
        # Without the workflow's selection the receipts' own is recorded.
        self.assertEqual(select_bug(Job(self.root / "unchecked").passing()).row()["profile"]["bug_compatibility"],
                         [BUG])

    def test_a_job_without_receipts_records_no_selection(self):
        job = Job(self.root / "job")
        job.phases(modules="failure", compile="skipped", native="skipped")
        self.assertEqual(job.row(bug_compatibility="")["profile"]["bug_compatibility"], history.MISSING)

    def test_mismatched_receipts_write_no_row(self):
        cases = {
            "another bug compatibility selection than this workflow": (select_bug, {"bug_compatibility": ""}),
            "another bug compatibility selection than this workflow's: none": (
                lambda job: job, {"bug_compatibility": BUG}),
            "different tags or bug compatibility selections": (
                lambda job: select_bug(job, native_selection=()), {}),
        }
        for message, (change, metadata) in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                change(Job(self.root / str(len(message))).passing()).row(**metadata)

    def test_a_selection_outside_the_profile_tags_is_refused(self):
        job = Job(self.root / "job").passing()
        for path in ("compile/compile-report.json", "native/report.json"):
            report = json.loads((job.scenario / path).read_text())
            report["profile"] = PROFILE | {"bug_compatibility": [BUG]}
            write(job.scenario / path, report)
        with self.assertRaisesRegex(ValueError, "not a sorted subset of their profile tags"):
            job.row()

    def test_another_selection_is_another_configuration_and_keeps_existing_keys(self):
        ordinary = Job(self.root / "ordinary").passing().row()
        selected = select_bug(Job(self.root / "selected").passing()).row(run_id="101")
        self.assertNotEqual(history.config_key(ordinary), history.config_key(selected))
        self.assertNotIn("bug_compatibility", history.config_identity(ordinary))

    def test_summary_puts_the_selection_beside_the_image_identities(self):
        summary = history.provenance_summary(select_bug(Job(self.root / "job").passing()).row())
        self.assertIn("| Bug compatibility selection | `" + BUG + "` |", summary)
        self.assertIn("| Image `osd` | sha256:" + "1" * 64 + " |", summary)
        self.assertIn("| Release | 20.2.4 |", summary)
        ordinary = history.provenance_summary(Job(self.root / "ordinary").passing().row())
        self.assertIn("| Bug compatibility selection | none (ordinary paths) |", ordinary)
        self.assertEqual(history.markdown_cell("a|b\nc"), "a\\|b c")

    def test_command_line_writes_the_summary_and_refuses_a_mismatch(self):
        job = select_bug(Job(self.root / "job").passing())
        arguments = ["normalize", "--scenario", str(job.scenario), "--cleanup", str(job.cleanup),
                     "--images", str(job.images), "--profile", PROFILE["id"], "--repository", "owner/repo",
                     "--run-id", "1", "--run-attempt", "1", "--summary", str(self.root / "summary.md"),
                     "--output", str(self.root / "row.json")]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(history.main(arguments + ["--bug-compatibility", BUG]), 0)
        self.assertIn("`" + BUG + "`", (self.root / "summary.md").read_text())
        (self.root / "row.json").unlink()
        (self.root / "summary.md").unlink()
        with contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(history.main(arguments + ["--bug-compatibility", ""]), 1)
        self.assertIn("another bug compatibility selection", error.getvalue())
        self.assertFalse((self.root / "row.json").exists())
        self.assertFalse((self.root / "summary.md").exists())


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


class ClassifyTest(unittest.TestCase):
    """Same-configuration classification over rows of a fixed cohort."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.count = 0

    def attempt(self, result="passed", repetition="1", run_id="100", **metadata):
        self.count += 1
        job = Job(self.root / str(self.count)).passing()
        if result == "failed":
            job.phases(native="failure")
            job.native(FAILING_LOG, "native-test-failure", 40.0)
        elif result == "other-failure":
            job.phases(native="failure")
            job.native(PASSING_LOG.replace("--- PASS: TestRBDSnapshot", "--- FAIL: TestRBDSnapshot"), "native-test-failure")
        elif result == "truncated":
            job.phases(native="failure")
            job.native(TRUNCATED_LOG, "native-test-failure", 45.0)
        elif result == "image":
            job.phases(images="failure")
        elif result == "cancelled":
            job.phases(native="cancelled")
        elif result == "timeout":
            job.phases(native="failure")
            job.native(FAILING_LOG, "native-test-failure")
            report = json.loads((job.scenario / "native/report.json").read_text())
            report["timed_out"] = True
            write(job.scenario / "native/report.json", report)
        return job.row(repetition_id=repetition, run_id=run_id, **metadata)

    def profile(self, rows):
        return history.classify(rows)["profiles"][PROFILE["id"] + "@20.2.4"]

    def only_config(self, rows):
        configs = self.profile(rows)["configs"]
        self.assertEqual(len(configs), 1)
        return next(iter(configs.values()))

    def test_repetitions_of_one_cohort_share_one_configuration(self):
        rows = [self.attempt(repetition=str(index)) for index in range(1, 4)]
        config = self.only_config(rows)
        self.assertEqual((config["attempts"], config["passed"], config["failed"]), (3, 3, 0))
        self.assertEqual(config["classification"], "confirmed_pass")
        self.assertIsNone(config["first_failure"])
        self.assertEqual(config["identity"]["checkout_tree"], "c" * 40)
        self.assertEqual(config["identity"]["images"], {"control": "sha256:" + "0" * 64, "osd": "sha256:" + "1" * 64})

    def test_pass_and_fail_in_one_configuration_is_suspected_intermittency_with_the_first_failure_kept(self):
        rows = [self.attempt("passed", "1"), self.attempt("failed", "2"), self.attempt("passed", "3")]
        config = self.only_config(list(reversed(rows)))
        self.assertEqual(config["classification"], "suspected_intermittent")
        self.assertEqual(config["first_failure"], {"run_id": "100", "run_attempt": "1", "job_id": "200",
                                                   "repetition_id": "2", "phase": "native",
                                                   "classification": "native-test-failure",
                                                   "test": "TestRBDRecovery", "kind": "assertion_failure"})
        # TestRBDSnapshot started in the failed attempt but has no result there.
        self.assertEqual(config["tests"], {"TestRBDRecovery": {"failed": 1, "ran": 3},
                                           "TestRBDRecovery/primary": {"failed": 1, "ran": 3},
                                           "TestRBDSnapshot": {"failed": 0, "ran": 2}})

    def test_same_failure_every_time_is_deterministic_and_different_failures_are_confirmed(self):
        same = [self.attempt("failed", str(index)) for index in (1, 2)]
        self.assertEqual(self.only_config(same)["classification"], "deterministic_failure")
        mixed = [self.attempt("failed", "1"), self.attempt("other-failure", "2")]
        self.assertEqual(self.only_config(mixed)["classification"], "confirmed_fail")
        self.assertEqual(self.only_config(same[:1])["classification"], "insufficient_samples")

    def test_runner_image_and_cancelled_attempts_stay_out_of_the_scenario_record(self):
        rows = [self.attempt("passed", "1"), self.attempt("image", "2"), self.attempt("cancelled", "3"),
                self.attempt("passed", "4")]
        config = self.only_config(rows)
        self.assertEqual((config["attempts"], config["failed"]), (2, 0))
        self.assertEqual(config["excluded"], {"cancelled": 1, "image_failure": 1})
        self.assertEqual(config["classification"], "confirmed_pass")
        recent = self.profile(rows)["recent"]
        self.assertEqual((recent["rows"], recent["attempts"], recent["failed"]), (4, 2, 0))
        self.assertEqual(history.attempt_kind(self.attempt("timeout")), "timeout")

    def test_another_tree_or_image_is_another_configuration(self):
        base = self.attempt()
        other_tree = self.attempt(checkout_tree="f" * 40, run_id="101")
        job = Job(self.root / "image").passing()
        summary = json.loads((job.images / "summary.json").read_text())
        summary["images"]["osd"]["image_id"] = "sha256:" + "9" * 64
        write(job.images / "summary.json", summary)
        other_image = job.row(run_id="102")
        incomparable = self.attempt(checkout_commit="", run_id="103")
        profile = self.profile([base, other_tree, other_image, incomparable])
        self.assertEqual(len(profile["configs"]), 3)
        self.assertEqual(profile["incomparable_rows"], 1)
        # Newest configuration first.
        self.assertEqual([config["last_run_id"] for config in profile["configs"].values()], ["102", "101", "100"])

    def test_old_configurations_and_attempts_leave_the_window(self):
        rows = [self.attempt(run_id=str(1000 + index), checkout_tree=str(index % 10) * 40)
                for index in range(history.CONFIGS_KEPT + 2)]
        rows += [self.attempt("failed", run_id=str(2000 + index)) for index in range(history.RECENT_ATTEMPTS)]
        profile = self.profile(rows)
        self.assertEqual(len(profile["configs"]), history.CONFIGS_KEPT)
        self.assertEqual(profile["recent"]["failed"], history.RECENT_ATTEMPTS)

    def test_collect_stores_one_append_only_file_per_run_attempt(self):
        rows = self.root / "artifacts"
        store = self.root / "store"
        for name, row in (("tag-scenario-a-r1", self.attempt(repetition="1")),
                          ("tag-scenario-a-r2", self.attempt("failed", "2")),
                          ("tag-scenario-b", self.attempt(run_id="101"))):
            write(rows / name / "history-row.json", row)
        write(rows / "tag-scenario-local" / "history-row.json", self.attempt(run_id=""))
        write(rows / "tag-scenario-broken" / "history-row.json", "{")
        result = history.collect(store, rows)
        self.assertEqual((result["added"], result["duplicates"], result["rows"]), (3, 0, 3))
        self.assertEqual(sorted(item["path"] for item in result["rejected"]),
                         ["tag-scenario-broken/history-row.json", "tag-scenario-local/history-row.json"])
        self.assertEqual(sorted(path.name for path in (store / "rows").iterdir()), ["100-1.jsonl", "101-1.jsonl"])
        stored = (store / "rows/100-1.jsonl").read_text()
        summary = json.loads((store / "summary.json").read_text())
        self.assertEqual(summary["schema"], history.CLASSIFICATION_SCHEMA)
        self.assertEqual(summary["rows"], 3)
        # Collecting the same artifacts again changes nothing.
        again = history.collect(store, rows)
        self.assertEqual((again["added"], again["duplicates"]), (0, 3))
        self.assertEqual((store / "rows/100-1.jsonl").read_text(), stored)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(history.main(["classify", "--history-dir", str(store)]), 0)
        self.assertEqual(json.loads(output.getvalue()), summary)

    def test_notes_put_a_failure_beside_earlier_same_config_attempts(self):
        rows = [self.attempt("passed", "1"), self.attempt("failed", "2"), self.attempt("passed", "3")]
        summary = history.classify(rows)
        current = self.attempt("failed", "4", run_id="105")
        notes = history.history_notes(summary, current, ["TestRBDRecovery", "TestRBDSnapshot"])
        self.assertEqual(notes, ["the profile failed 1/3 recent attempts on 20.2.4",
                                 "TestRBDRecovery failed 1/3 earlier same-config attempts that ran it",
                                 "TestRBDSnapshot failed 0/2 earlier same-config attempts that ran it",
                                 "same-config record: 2 passed, 1 failed (suspected_intermittent)"])
        other = self.attempt("failed", checkout_tree="f" * 40, run_id="106")
        self.assertEqual(history.history_notes(summary, other, ["TestRBDRecovery"])[-1],
                         "no earlier same-config attempt")
        self.assertEqual(history.history_notes({"schema": "other"}, current, []), [])
        self.assertEqual(history.history_notes({"schema": history.CLASSIFICATION_SCHEMA, "profiles": {}}, current, []),
                         ["no earlier attempt of this profile on 20.2.4 is in the scenario history"])

    def test_truncated_earlier_runs_are_not_evidence_that_a_later_test_ran(self):
        rows = [self.attempt("truncated", "1"), self.attempt("truncated", "2")]
        summary = history.classify(rows)
        config = self.only_config(rows)
        self.assertEqual(config["tests"], {"TestRBDRecovery": {"failed": 2, "ran": 2}})
        current = self.attempt("other-failure", "3", run_id="105")
        self.assertEqual(history.history_notes(summary, current, ["TestRBDSnapshot", "TestRBDRecovery"]), [
            "the profile failed 2/2 recent attempts on 20.2.4",
            "TestRBDSnapshot has no observed outcome in 2 earlier same-config attempts",
            "TestRBDRecovery failed 2/2 earlier same-config attempts that ran it",
            "same-config record: 0 passed, 2 failed (deterministic_failure)"])
        # A stored record with no observed run is not a 0/0 count either.
        summary["profiles"][PROFILE["id"] + "@20.2.4"]["configs"][history.config_key(current)]["tests"][
            "TestRBDSnapshot"] = {"failed": 0, "ran": 0}
        self.assertIn("TestRBDSnapshot has no observed outcome in 2 earlier same-config attempts",
                      history.history_notes(summary, current, ["TestRBDSnapshot"]))

    def test_pass_only_history_counts_every_observed_pass(self):
        rows = [self.attempt("passed", str(index)) for index in (1, 2, 3)]
        summary = history.classify(rows)
        self.assertEqual(self.only_config(rows)["tests"], {"TestRBDRecovery": {"failed": 0, "ran": 3},
                                                           "TestRBDRecovery/primary": {"failed": 0, "ran": 3},
                                                           "TestRBDSnapshot": {"failed": 0, "ran": 3}})
        current = self.attempt("other-failure", "4", run_id="105")
        self.assertEqual(history.history_notes(summary, current, ["TestRBDSnapshot", "TestRBDMissing"])[1:3], [
            "TestRBDSnapshot failed 0/3 earlier same-config attempts that ran it",
            "TestRBDMissing has no observed outcome in 3 earlier same-config attempts"])

    def test_mixed_history_counts_each_test_over_the_attempts_that_ran_it(self):
        rows = [self.attempt("passed", "1"), self.attempt("truncated", "2"), self.attempt("other-failure", "3"),
                self.attempt("image", "4"), self.attempt("passed", "5")]
        summary = history.classify(rows)
        config = self.only_config(rows)
        self.assertEqual(config["attempts"], 4)
        self.assertEqual(config["tests"], {"TestRBDRecovery": {"failed": 1, "ran": 4},
                                           "TestRBDRecovery/primary": {"failed": 0, "ran": 3},
                                           "TestRBDSnapshot": {"failed": 1, "ran": 3}})
        current = self.attempt("other-failure", "6", run_id="105")
        self.assertEqual(history.history_notes(summary, current, ["TestRBDSnapshot", "TestRBDRecovery"])[1:3], [
            "TestRBDSnapshot failed 1/3 earlier same-config attempts that ran it",
            "TestRBDRecovery failed 1/4 earlier same-config attempts that ran it"])

    def test_failure_reporter_adds_history_notes_without_changing_its_status(self):
        rows = [self.attempt("passed", "1"), self.attempt("failed", "2")]
        summary_path, row_path, log_path = self.root / "summary.json", self.root / "row.json", self.root / "native.log"
        write(summary_path, history.classify(rows))
        write(row_path, self.attempt("failed", "3", run_id="105"))
        write(log_path, FAILING_LOG)
        import report_test_failures as reporter
        with contextlib.redirect_stdout(io.StringIO()) as output, mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(reporter.main([str(log_path), "--profile", PROFILE["id"], "--row", str(row_path),
                                            "--history-summary", str(summary_path)]), 0)
        lines = output.getvalue().splitlines()
        self.assertIn("::error title=Go test result::" + PROFILE["id"] + ": TestRBDRecovery", lines)
        self.assertIn("::notice title=Scenario history::" + PROFILE["id"]
                      + ": TestRBDRecovery failed 1/2 earlier same-config attempts that ran it", lines)
        write(summary_path, "not json")
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(reporter.main([str(log_path), "--profile", PROFILE["id"], "--row", str(row_path),
                                            "--history-summary", str(summary_path)]), 0)
        self.assertIn("::notice title=Scenario history::" + PROFILE["id"]
                      + ": scenario history unavailable for this job.", output.getvalue().splitlines())
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(reporter.main([str(log_path), "--profile", PROFILE["id"]]), 0)
        self.assertNotIn("Scenario history", output.getvalue())


if __name__ == "__main__":
    unittest.main()
