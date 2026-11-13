#!/usr/bin/env python3
"""The Experiments workflow's command and result check."""

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

import run_experiment as experiment

LINE = ("    case_test.go:9: EXPERIMENT_RESULT case=%s result=%s image=x symptoms=[]\n")


class CommandTests(unittest.TestCase):
    def test_the_default_image_runs_without_an_injection(self):
        command = experiment.command("rgw_empty_sse_header")
        self.assertNotIn("-ldflags", command)
        self.assertEqual(command[-1], experiment.MODULE + "/internal/integration/experiments/rgw_empty_sse_header")

    def test_an_image_is_injected_into_the_case_package_variable(self):
        command = experiment.command("rgw_empty_sse_header", "quay.io/ceph/ceph:v20.2.5@sha256:" + "4" * 64)
        flag = command[command.index("-ldflags") + 1]
        self.assertEqual(flag, "-X " + experiment.MODULE + "/internal/integration/experiments/rgw_empty_sse_header_test.image="
                         "quay.io/ceph/ceph:v20.2.5@sha256:" + "4" * 64)

    def test_unknown_cases_and_unsafe_images_are_refused(self):
        for case, image in (("missing_case", ""), ("../rgw", ""), ("rgw_empty_sse_header", "x;rm -rf /"),
                            ("rgw_empty_sse_header", "-X other.var=y")):
            with self.subTest(case=case, image=image), self.assertRaises(ValueError):
                experiment.command(case, image)


class CheckTests(unittest.TestCase):
    def test_the_default_image_must_reproduce(self):
        self.assertEqual(experiment.check_default(LINE % ("a", "REPRODUCED") + LINE % ("a/b", "REPRODUCED")), [])
        self.assertEqual(len(experiment.check_default(LINE % ("a", "REPRODUCED") + LINE % ("a/b", "NOT_REPRODUCED"))), 1)

    def test_a_fixed_image_must_resolve_each_reproduced_name(self):
        self.assertEqual(experiment.check_fixed(LINE % ("a", "NOT_REPRODUCED"), ["a"]), [])
        self.assertEqual(experiment.check_fixed(LINE % ("a", "REPRODUCED"), ["a"]), ["a: REPRODUCED, want NOT_REPRODUCED"])

    def test_a_fixed_run_that_skips_a_reproduced_name_fails(self):
        log = LINE % ("a/second", "NOT_REPRODUCED")
        self.assertEqual(experiment.check_fixed(log, ["a/first"]), ["a/first: no result, want NOT_REPRODUCED"])
        self.assertEqual(len(experiment.check_fixed(LINE % ("a/first", "NOT_REPRODUCED"), ["a/first", "a/second"])), 1)

    def test_a_name_is_not_matched_by_its_prefix(self):
        self.assertEqual(len(experiment.check_fixed(LINE % ("a/first-part", "NOT_REPRODUCED"), ["a/first"])), 1)

    def test_a_name_printed_with_both_results_fails(self):
        log = LINE % ("a", "NOT_REPRODUCED") + LINE % ("a", "REPRODUCED")
        self.assertEqual(len(experiment.check_fixed(log, ["a"])), 1)

    def test_a_defect_only_the_fixed_run_reaches_is_not_judged(self):
        log = LINE % ("a/first", "NOT_REPRODUCED") + LINE % ("a/later", "REPRODUCED")
        self.assertEqual(experiment.check_fixed(log, ["a/first"]), [])

    def test_a_run_without_a_result_line_fails(self):
        want = ["the case printed no EXPERIMENT_RESULT line"]
        self.assertEqual(experiment.check_default("--- PASS: TestCase (0.00s)\n"), want)
        self.assertEqual(experiment.check_fixed("--- PASS: TestCase (0.00s)\n", []), want)


class MainTests(unittest.TestCase):
    def runs(self, argv, logs, codes=None):
        """main's exit code and the images it ran, with each run printing logs[image]."""
        images = []

        def popen(command, **_):
            image = command[command.index("-ldflags") + 1].split("image=")[1] if "-ldflags" in command else ""
            images.append(image)
            process = mock.Mock()
            process.stdout = io.StringIO(logs[image])
            process.wait.return_value = (codes or {}).get(image, 0)
            return process

        with mock.patch.object(experiment.subprocess, "Popen", popen), redirect_stdout(io.StringIO()):
            return experiment.main(argv), images

    FIXED = ["--case", "rgw_empty_sse_header", "--image", "fixed"]

    def test_the_default_image_alone_runs_once(self):
        self.assertEqual(self.runs(["--case", "rgw_empty_sse_header"], {"": LINE % ("a", "REPRODUCED")}), (0, [""]))

    def test_a_fixed_image_runs_after_the_default_control(self):
        logs = {"": LINE % ("a", "REPRODUCED"), "fixed": LINE % ("a", "NOT_REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs), (0, ["", "fixed"]))

    def test_a_trigger_that_reproduces_on_no_image_fails_the_fixed_dispatch(self):
        logs = {"": LINE % ("a", "NOT_REPRODUCED"), "fixed": LINE % ("a", "NOT_REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs), (1, ["", "fixed"]))

    def test_a_fixed_image_that_still_reproduces_fails(self):
        logs = {"": LINE % ("a", "REPRODUCED"), "fixed": LINE % ("a", "REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs)[0], 1)

    def test_the_fixed_run_must_resolve_the_defect_the_default_reproduced(self):
        logs = {"": LINE % ("a/first", "REPRODUCED"), "fixed": LINE % ("a/second", "NOT_REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs)[0], 1)

    def test_a_later_defect_still_present_does_not_fail_a_resolved_first_one(self):
        logs = {"": LINE % ("a/first", "REPRODUCED"),
                "fixed": LINE % ("a/first", "NOT_REPRODUCED") + LINE % ("a/second", "REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs)[0], 0)

    def test_a_failed_go_test_fails_either_run(self):
        logs = {"": LINE % ("a", "REPRODUCED"), "fixed": LINE % ("a", "NOT_REPRODUCED")}
        self.assertEqual(self.runs(self.FIXED, logs, {"": 1})[0], 1)
        self.assertEqual(self.runs(self.FIXED, logs, {"fixed": 1})[0], 1)


if __name__ == "__main__":
    unittest.main()
