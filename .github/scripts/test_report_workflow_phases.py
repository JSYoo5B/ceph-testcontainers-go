"""Verify workflow receipts report actual outcomes without masking failures."""

from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import report_workflow_phases as reporter


def environment(**overrides):
    values = {
        "SCENARIO_CHECK_NAME": "Recovery",
        "SCENARIO_PROFILE": "recovery-rbd-12345678",
        "SCENARIO_REQUIRES_CEPH": "true",
        **{variable: "success" for _, _, variable in reporter.PHASES},
    }
    return values | overrides


def phase(result, name):
    return next(item for item in result["phases"] if item["phase"] == name)


@contextmanager
def temporary_working_directory():
    previous = Path.cwd()
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        os.chdir(root)
        try:
            yield root
        finally:
            os.chdir(previous)


class WorkflowPhaseTests(unittest.TestCase):
    def test_native_and_cleanup_failures_both_appear_without_root_cause_claim(self):
        result = reporter.report(environment(SCENARIO_NATIVE_OUTCOME="failure", SCENARIO_CLEANUP_OUTCOME="failure"))
        self.assertEqual(result["failed_phases"], ["native", "cleanup"])
        self.assertEqual(result["cancelled_phases"], [])
        text = reporter.summary(result)
        self.assertIn("| Native assertions and completion | failure |", text)
        self.assertIn("| Docker resource cleanup | failure |", text)
        self.assertIn("do not establish its root cause", text)
        self.assertNotIn("passed", result)

    def test_compile_failure_does_not_label_skipped_native_assertions_as_failed(self):
        result = reporter.report(environment(
            SCENARIO_COMPILE_OUTCOME="failure", SCENARIO_DOCKER_OUTCOME="skipped",
            SCENARIO_IMAGES_OUTCOME="skipped", SCENARIO_NATIVE_OUTCOME="skipped",
            SCENARIO_CLEANUP_OUTCOME="skipped"))
        self.assertEqual(result["failed_phases"], ["compile"])
        self.assertEqual(phase(result, "native")["display"], "skipped")
        self.assertEqual(phase(result, "cleanup")["display"], "skipped")
        text = reporter.summary(result)
        self.assertIn("| Compilation | failure |", text)
        self.assertIn("| Native assertions and completion | skipped |", text)
        self.assertNotIn("| Native assertions and completion | failure |", text)

    def test_docker_baseline_failure_preserves_not_run_and_skipped_outcomes(self):
        values = environment(SCENARIO_DOCKER_OUTCOME="failure", SCENARIO_IMAGES_OUTCOME="skipped",
                             SCENARIO_CLEANUP_OUTCOME="skipped")
        del values["SCENARIO_NATIVE_OUTCOME"]
        result = reporter.report(values)
        self.assertEqual(result["failed_phases"], ["docker"])
        self.assertEqual(phase(result, "native")["outcome"], "")
        self.assertEqual(phase(result, "native")["display"], "not run")
        self.assertIn("| Docker baseline | failure |", reporter.summary(result))

    def test_only_an_expected_no_ceph_image_skip_is_not_required(self):
        result = reporter.report(environment(SCENARIO_REQUIRES_CEPH="false", SCENARIO_IMAGES_OUTCOME="skipped"))
        self.assertEqual(phase(result, "images")["outcome"], "skipped")
        self.assertEqual(phase(result, "images")["display"], "not required")
        self.assertEqual(result["failed_phases"], [])
        required = reporter.report(environment(SCENARIO_IMAGES_OUTCOME="skipped"))
        self.assertEqual(phase(required, "images")["display"], "skipped")
        failed = reporter.report(environment(SCENARIO_REQUIRES_CEPH="false", SCENARIO_IMAGES_OUTCOME="failure"))
        self.assertEqual(phase(failed, "images")["display"], "failure")
        self.assertEqual(failed["failed_phases"], ["images"])

    def test_missing_and_cancelled_phases_never_fabricate_success(self):
        values = {key: value for key, value in environment().items() if not key.endswith("_OUTCOME")}
        values["SCENARIO_COMPILE_OUTCOME"] = "cancelled"
        result = reporter.report(values)
        self.assertEqual(result["cancelled_phases"], ["compile"])
        self.assertEqual(result["failed_phases"], [])
        self.assertEqual(phase(result, "compile")["display"], "cancelled")
        for name in ("docker", "images", "native", "cleanup"):
            self.assertEqual(phase(result, name)["display"], "not run")
        text = reporter.summary(result)
        self.assertNotIn("success", text)
        self.assertNotIn("PASS", text)
        self.assertIn("| Compilation | cancelled |", text)

    def test_unsafe_identifiers_outcomes_and_ceph_flag_are_rejected(self):
        cases = (
            ("SCENARIO_CHECK_NAME", "Recovery\n::error::secret"),
            ("SCENARIO_CHECK_NAME", "Recovery%0Asecret"),
            ("SCENARIO_CHECK_NAME", "Recovery%25secret"),
            ("SCENARIO_CHECK_NAME", "Recovery,secret"),
            ("SCENARIO_CHECK_NAME", "R" * 65),
            ("SCENARIO_CHECK_NAME", ""),
            ("SCENARIO_PROFILE", "recovery\n::error::secret"),
            ("SCENARIO_PROFILE", "recovery%0Asecret"),
            ("SCENARIO_PROFILE", "recovery%25secret"),
            ("SCENARIO_PROFILE", "r" * 81),
            ("SCENARIO_PROFILE", ""),
            ("SCENARIO_NATIVE_OUTCOME", "failure\n::error::secret"),
            ("SCENARIO_NATIVE_OUTCOME", "failure%0Asecret"),
            ("SCENARIO_NATIVE_OUTCOME", "PASS"),
            ("SCENARIO_REQUIRES_CEPH", "true\nsecret"),
            ("SCENARIO_REQUIRES_CEPH", "TRUE"),
        )
        for key, value in cases:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                reporter.report(environment(**{key: value}))


class WorkflowPhaseMainTests(unittest.TestCase):
    def test_main_writes_receipt_and_appends_summary_for_every_actual_failure(self):
        values = environment(SCENARIO_NATIVE_OUTCOME="failure", SCENARIO_CLEANUP_OUTCOME="failure")
        with temporary_working_directory() as root:
            summary_path = root / "step-summary.md"
            summary_path.write_text("Existing job summary\n", encoding="utf-8")
            values["GITHUB_STEP_SUMMARY"] = str(summary_path)
            with patch.dict(os.environ, values, clear=True), patch("sys.stdout", new=io.StringIO()) as output:
                self.assertEqual(reporter.main(), 0)
                self.assertEqual(reporter.main(), 0)
            receipt = json.loads((root / "artifacts/scenario/phases.json").read_text(encoding="utf-8"))
            text = summary_path.read_text(encoding="utf-8")
        self.assertEqual(receipt["failed_phases"], ["native", "cleanup"])
        self.assertTrue(text.startswith("Existing job summary\n"))
        self.assertEqual(text.count("### Recovery / recovery-rbd-12345678"), 2)
        self.assertEqual(text.count("| Native assertions and completion | failure |"), 2)
        self.assertEqual(text.count("| Docker resource cleanup | failure |"), 2)
        annotations = output.getvalue().splitlines()
        self.assertEqual(len(annotations), 4)
        self.assertTrue(all(line.startswith("::error title=") for line in annotations))
        self.assertEqual(sum("Native assertions and completion failure" in line for line in annotations), 2)
        self.assertEqual(sum("Docker resource cleanup failure" in line for line in annotations), 2)

    def test_main_reports_compile_failure_without_annotation_for_skipped_native(self):
        values = environment(SCENARIO_COMPILE_OUTCOME="failure", SCENARIO_DOCKER_OUTCOME="skipped",
                             SCENARIO_IMAGES_OUTCOME="skipped", SCENARIO_NATIVE_OUTCOME="skipped",
                             SCENARIO_CLEANUP_OUTCOME="skipped")
        with temporary_working_directory() as root, patch.dict(os.environ, values, clear=True), \
                patch("sys.stdout", new=io.StringIO()) as output:
            self.assertEqual(reporter.main(), 0)
            receipt = json.loads((root / "artifacts/scenario/phases.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["failed_phases"], ["compile"])
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertIn("::error title=Recovery / Compilation::Compilation failure", output.getvalue())
        self.assertNotIn("Native assertions", output.getvalue())

    def test_main_emits_cancelled_warning_and_does_not_require_summary_file(self):
        values = environment(SCENARIO_NATIVE_OUTCOME="cancelled", SCENARIO_CLEANUP_OUTCOME="cancelled")
        with temporary_working_directory() as root, patch.dict(os.environ, values, clear=True), \
                patch("sys.stdout", new=io.StringIO()) as output:
            self.assertEqual(reporter.main(), 0)
            receipt = json.loads((root / "artifacts/scenario/phases.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["cancelled_phases"], ["native", "cleanup"])
        self.assertEqual(receipt["failed_phases"], [])
        self.assertEqual(len(output.getvalue().splitlines()), 2)
        self.assertTrue(all(line.startswith("::warning title=") for line in output.getvalue().splitlines()))
        self.assertNotIn("::error", output.getvalue())

    def test_main_invalid_input_returns_failure_without_secret_leak_or_artifacts(self):
        for key, value in (("SCENARIO_CHECK_NAME", "Recovery\nsecret%0A::error::injected"),
                           ("SCENARIO_PROFILE", "secret%25::error::injected"),
                           ("SCENARIO_NATIVE_OUTCOME", "failure\nsecret%0A::error::injected")):
            with self.subTest(key=key), temporary_working_directory() as root, \
                    patch.dict(os.environ, environment(**{key: value}), clear=True), \
                    patch("sys.stdout", new=io.StringIO()) as output, patch("sys.stderr", new=io.StringIO()) as errors:
                self.assertEqual(reporter.main(), 1)
                self.assertFalse((root / "artifacts").exists())
            self.assertEqual(output.getvalue(), "")
            self.assertIn("Workflow phase reporting failed", errors.getvalue())
            self.assertNotIn("secret", errors.getvalue())
            self.assertNotIn("::error", errors.getvalue())
            self.assertNotIn("%0A", errors.getvalue())
            self.assertNotIn("%25", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
