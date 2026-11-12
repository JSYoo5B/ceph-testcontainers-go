#!/usr/bin/env python3
"""Expected reproduction verdicts: rows, the log comparison and the workflow."""

import json
from pathlib import Path
import re
import tempfile
import unittest

import reproduction_rows as rows
import tag_scenarios as engine

ROOT = Path(__file__).resolve().parents[2]


def line(unit, verdict, expected="none"):
    return ("    case_test.go:12: REPRODUCTION_VERDICT case=%s verdict=%s expected=%s attempts=1 "
            "platform=linux/amd64 network=bridge images=all=sha256:a observation=trigger: x" % (unit, verdict, expected))


class ExpectationTests(unittest.TestCase):
    def test_parse_accepts_one_unit_and_an_expectable_verdict(self):
        self.assertIsNone(rows.parse_expectation(""))
        self.assertEqual(rows.parse_expectation("rgw_case=REPRODUCED"), ("rgw_case", "REPRODUCED"))
        self.assertEqual(rows.parse_expectation("cephfs_case/same-named=NOT_REPRODUCED"),
                         ("cephfs_case/same-named", "NOT_REPRODUCED"))
        for value in ("rgw_case=INCONCLUSIVE", "rgw_case=reproduced", "rgw_case", "=REPRODUCED",
                      "Rgw_case=REPRODUCED", "rgw_case/a/b=REPRODUCED"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                rows.parse_expectation(value)

    def test_check_requires_the_units_own_matching_line(self):
        reproduced = line("rgw_case", "REPRODUCED", "REPRODUCED")
        for name, text, expectation, met in (
            ("expected reproduction matches", reproduced, ("rgw_case", "REPRODUCED"), True),
            ("expected absence matches", line("rgw_case", "NOT_REPRODUCED", "NOT_REPRODUCED"),
             ("rgw_case", "NOT_REPRODUCED"), True),
            ("expected reproduction, absent", line("rgw_case", "NOT_REPRODUCED", "REPRODUCED"),
             ("rgw_case", "REPRODUCED"), False),
            ("expected absence, reproduced", line("rgw_case", "REPRODUCED", "NOT_REPRODUCED"),
             ("rgw_case", "NOT_REPRODUCED"), False),
            ("inconclusive", line("rgw_case", "INCONCLUSIVE", "REPRODUCED"), ("rgw_case", "REPRODUCED"), False),
            ("missing line, e.g. a skip that never started the unit", "--- SKIP: TestCase (0.00s)",
             ("rgw_case", "REPRODUCED"), False),
            ("only another unit reported", line("rgw_other", "REPRODUCED"), ("rgw_case", "REPRODUCED"), False),
            ("two lines of the unit", reproduced + "\n" + reproduced, ("rgw_case", "REPRODUCED"), False),
            ("omitted expectation", line("rgw_case", "REPRODUCED"), None, True),
        ):
            with self.subTest(name):
                result = rows.check(text, expectation)
                self.assertEqual(result["met"], met, result)
        omitted = rows.check(line("rgw_case", "INCONCLUSIVE"), None)
        self.assertEqual(omitted["verdicts"][0]["verdict"], "INCONCLUSIVE")
        self.assertEqual(omitted["verdicts"][0]["attempts"], 1)

    def test_changing_only_the_expected_value_keeps_the_actual_verdict(self):
        text = line("rgw_case", "REPRODUCED", "REPRODUCED")
        met = rows.check(text, ("rgw_case", "REPRODUCED"))
        unmet = rows.check(text, ("rgw_case", "NOT_REPRODUCED"))
        self.assertTrue(met["met"])
        self.assertFalse(unmet["met"])
        self.assertEqual(met["verdicts"], unmet["verdicts"])


class RowValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        package = self.root / "internal/integration/experiments/rgw_case"
        package.mkdir(parents=True)
        (package / "case_test.go").write_text(
            'func TestCase(t *testing.T) {\n\trepro := harness.StartReproduction(t, "rgw_case", "trigger")\n}\n')
        self.plan = {"include": [{"batch": "rgw_case", "package": "./internal/integration/experiments/rgw_case"}]}
        self.row = {"id": "rgw-case-20.2.4", "unit": "rgw_case", "batch": "rgw_case", "release": "20.2.4",
                    "platform": "linux/amd64", "expected": "REPRODUCED", "evidence": "native run"}

    def tearDown(self):
        self.temporary.cleanup()

    def validate(self, *row_list):
        return rows.validate({"schema": rows.SCHEMA, "rows": list(row_list)}, self.plan, {"20.2.4", "21.2.0"}, self.root)

    def test_a_valid_row_and_a_row_without_expectation(self):
        investigation = {key: value for key, value in self.row.items() if key != "expected"}
        investigation.update(id="rgw-case-21.2.0", release="21.2.0")
        self.assertEqual(len(self.validate(self.row, investigation)), 2)
        self.assertEqual(rows.resolve([self.row], "rgw-case-20.2.4"),
                         {"batch": "rgw_case", "release": "20.2.4", "expected_verdict": "rgw_case=REPRODUCED",
                          "goarch": "amd64"})
        self.assertEqual(rows.resolve([investigation], "rgw-case-21.2.0")["expected_verdict"], "")

    def test_invalid_rows_are_refused_before_running(self):
        for name, change in (
            ("inconclusive is not expectable", {"expected": "INCONCLUSIVE"}),
            ("unknown verdict", {"expected": "FIXED"}),
            ("not an optional batch", {"batch": "rgw_other"}),
            ("unit the batch does not start", {"unit": "rgw_other"}),
            ("release without pinned role images", {"release": "20.2.5"}),
            ("another platform", {"platform": "linux/arm64"}),
            ("no evidence", {"evidence": " "}),
            ("unknown field", {"network": "host"}),
        ):
            with self.subTest(name), self.assertRaises(ValueError):
                self.validate({**self.row, **change})
        with self.assertRaises(ValueError):
            self.validate(self.row, {**self.row})
        with self.assertRaises(ValueError):
            self.validate(self.row, {**self.row, "id": "rgw-case-again"})
        with self.assertRaises(ValueError):
            rows.validate({"schema": "other", "rows": [self.row]}, self.plan, {"20.2.4"}, self.root)

    def test_runner_refuses_an_expectation_for_a_unit_the_package_does_not_start(self):
        package = "internal/integration/experiments/rgw_case"
        rows.require_unit(("rgw_case", "REPRODUCED"), package, self.root)
        rows.require_unit(None, package, self.root)
        with self.assertRaises(ValueError):
            rows.require_unit(("rgw_other", "REPRODUCED"), package, self.root)


class RepositoryRowsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        catalog = engine.load_catalog()
        cls.optional = engine.discover_optional(catalog)
        cls.rows = rows.validate(json.loads(rows.ROWS.read_text()), cls.optional, rows.role_releases())

    def test_workflow_offers_exactly_the_rows(self):
        workflow = (ROOT / ".github/workflows/reproduction-rows.yml").read_text()
        choices = re.findall(r"(?m)^          - ([a-z0-9.-]+)$", workflow)
        self.assertEqual(choices, [row["id"] for row in self.rows])
        self.assertIn("python3 .github/scripts/reproduction_rows.py resolve --row \"$ROW\" --github-output \"$GITHUB_OUTPUT\"",
                      workflow)
        self.assertIn("      expected_verdict: ${{ needs.row.outputs.expected_verdict }}\n", workflow)
        self.assertIn("      release: ${{ needs.row.outputs.release }}\n", workflow)
        self.assertIn("      optional_batch: ${{ needs.row.outputs.batch }}\n", workflow)

    def test_tagged_runtime_passes_the_expectation_to_the_native_step(self):
        runtime = (ROOT / ".github/workflows/tagged-runtime.yml").read_text()
        self.assertEqual(runtime.count("CEPH_TEST_EXPECTED_VERDICT: ${{ inputs.expected_verdict }}"), 1)

    def test_every_row_runs_a_native_regression_batch(self):
        for row in self.rows:
            with self.subTest(row=row["id"]):
                selected = engine.select_optional_plan(self.optional, row["batch"])["include"]
                self.assertTrue(all("native_regression" in profile["tags"].split(",") for profile in selected))


if __name__ == "__main__":
    unittest.main()
