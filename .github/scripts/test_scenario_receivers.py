"""Exercise the receiver leaf gate using synthetic logs; never start Docker."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_scenario_receivers as checker


def successful_log(network):
    tests = (checker.PARENT, checker.PARENT + "/" + network, *checker.required_leaves(network))
    lines = ["=== RUN   " + name for name in tests]
    lines += ["    --- PASS: " + name + " (1.00s)" for name in tests[2:]]
    lines += ["--- PASS: " + name + " (6.00s)" for name in (tests[1], tests[0])]
    return "\n".join([*lines, "PASS", "ok\t" + checker.PACKAGE + "\t6.00s", ""])


class ReceiverCoverageTests(unittest.TestCase):
    def test_complete_bridge_and_host_fixtures_have_exact_disjoint_leaf_inventory(self):
        all_leaves = []
        for network in checker.NETWORKS:
            result = checker.validate(successful_log(network), network)
            self.assertTrue(result["passed"], result)
            self.assertEqual(len(result["required_leaves"]), 5)
            self.assertEqual(len(result["counts"]), 7)
            self.assertTrue(all(count == {"run": 1, "pass": 1}
                                for count in result["counts"].values()))
            self.assertEqual(result["selector"], "^" + checker.PARENT + "$/^" + network + "$")
            all_leaves.extend(result["required_leaves"])
        self.assertEqual(len(all_leaves), 10)
        self.assertEqual(len(set(all_leaves)), 10)

    def test_empty_or_filtered_parent_pass_never_counts_as_complete(self):
        for log in ("", "PASS\nok\t" + checker.PACKAGE + "\t0.00s\n",
                    "=== RUN   " + checker.PARENT + "\n--- PASS: " + checker.PARENT +
                    " (0.00s)\nPASS\nok\t" + checker.PACKAGE + "\t0.00s\n"):
            with self.subTest(log=log):
                self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_any_missing_duplicate_or_skipped_scope_fails_even_with_parent_pass(self):
        original = successful_log("bridge")
        for leaf in checker.required_leaves("bridge"):
            run = "=== RUN   " + leaf + "\n"
            passed = "    --- PASS: " + leaf + " (1.00s)\n"
            for label, log in (
                    ("missing run", original.replace(run, "")),
                    ("missing pass", original.replace(passed, "")),
                    ("duplicate run", original.replace(run, run + run)),
                    ("duplicate pass", original.replace(passed, passed + passed)),
                    ("skip", original.replace(passed, passed.replace("PASS", "SKIP"))),
                    ("fail", original.replace(passed, passed.replace("PASS", "FAIL")))):
                with self.subTest(leaf=leaf, mutation=label):
                    self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_wrong_network_foreign_scope_or_extra_parent_fails(self):
        original = successful_log("bridge")
        for log in (successful_log("host"),
                    original.replace("scope-4", "scope-5"),
                    original.replace("scope-4", "scope-4/filtered-child"),
                    original + "=== RUN   TestOther\n--- PASS: TestOther (0.00s)\n"):
            with self.subTest(log=log):
                self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_parent_and_network_run_pass_must_be_exactly_once(self):
        original = successful_log("bridge")
        for name in (checker.PARENT, checker.PARENT + "/bridge"):
            for event in ("=== RUN   " + name + "\n", "--- PASS: " + name + " (6.00s)\n"):
                for log in (original.replace(event, ""), original.replace(event, event + event)):
                    self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_scope_order_package_failure_and_missing_or_duplicate_completion_fail(self):
        original = successful_log("bridge")
        for log in (original.replace("scope-0", "scope-x").replace("scope-1", "scope-0").replace("scope-x", "scope-1"),
                    original.replace("\nPASS\n", "\n"),
                    original.replace("\nPASS\n", "\nPASS\nPASS\n"),
                    original.replace("ok\t" + checker.PACKAGE + "\t6.00s\n", ""),
                    original + "ok\t" + checker.PACKAGE + "\t6.00s\n",
                    original + "FAIL\n", original + "panic: fixture failed\n"):
            with self.subTest(log=log):
                self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_pass_before_run_or_package_completion_before_parent_is_not_a_completed_fixture(self):
        original = successful_log("bridge")
        first = "    --- PASS: " + checker.required_leaves("bridge")[0] + " (1.00s)\n"
        for log in (first + original.replace(first, ""),
                    "PASS\n" + original.replace("\nPASS\n", "\n")):
            self.assertFalse(checker.validate(log, "bridge")["passed"])

    def test_unknown_network_is_not_an_aggregate_or_empty_filter(self):
        for network in ("all", "", "brid", "host/scope-0"):
            with self.assertRaises(ValueError):
                checker.validate(successful_log("bridge"), network)

    def test_cli_preserves_json_receipt_and_exit_status_for_success_and_false_green(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.log"
            for log, code in ((successful_log("bridge"), 0), ("PASS\n", 1)):
                path.write_text(log)
                result = subprocess.run([sys.executable, str(Path(checker.__file__)), str(path),
                                         "--network", "bridge"], capture_output=True, text=True,
                                        timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)
                receipt = json.loads(result.stdout)
                self.assertEqual(receipt, checker.validate(log, "bridge"))
            missing = subprocess.run([sys.executable, str(Path(checker.__file__)),
                                      str(path.with_name("absent.log")), "--network", "host"],
                                     capture_output=True, text=True, timeout=10)
            self.assertEqual(missing.returncode, 1)
            self.assertFalse(json.loads(missing.stdout)["passed"])


if __name__ == "__main__":
    unittest.main()
