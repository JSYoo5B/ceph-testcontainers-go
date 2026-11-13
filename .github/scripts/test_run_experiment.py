#!/usr/bin/env python3
"""The Experiments workflow's command and result check."""

import unittest

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
        self.assertEqual(experiment.check(LINE % ("a", "REPRODUCED") + LINE % ("a/b", "REPRODUCED")), [])
        self.assertEqual(len(experiment.check(LINE % ("a", "REPRODUCED") + LINE % ("a/b", "NOT_REPRODUCED"))), 1)

    def test_a_fixed_image_must_not_reproduce(self):
        self.assertEqual(experiment.check(LINE % ("a", "NOT_REPRODUCED"), "fixed"), [])
        self.assertEqual(len(experiment.check(LINE % ("a", "REPRODUCED"), "fixed")), 1)

    def test_a_run_without_a_result_line_fails(self):
        self.assertEqual(experiment.check("--- PASS: TestCase (0.00s)\n"), ["the case printed no EXPERIMENT_RESULT line"])


if __name__ == "__main__":
    unittest.main()
