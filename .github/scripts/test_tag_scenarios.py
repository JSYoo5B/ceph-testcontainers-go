"""Test source-owned tag planning and execution gates without Docker."""

import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import tag_scenarios as engine


def tag(name):
    return {"op": "tag", "tag": name}


def combine(op, *values):
    result = values[0]
    for value in values[1:]:
        result = {"op": op, "children": [result, value]}
    return result


def negate(value):
    return {"op": "not", "children": [value]}


def fixture(path="internal/integration/new_test.go", tests=("TestNewFeature",), category="short", batch="basic"):
    expression = combine("or", tag("all"), combine("and", tag("integration"),
                         combine("or", negate(tag("ci")), combine("and", tag("ci_" + category),
                         combine("or", negate(tag("ci_batch")), tag("ci_batch_" + batch))))))
    return {"path": path, "package": "integration_test", "constraint": expression,
            "tags": ["all", "integration", "ci", "ci_" + category, "ci_batch", "ci_batch_" + batch],
            "tests": list(tests), "metadata": ["timeout=20m job-timeout=35"], "sha256": "a" * 64}


def native_log(names=("TestNewFeature",), package="example/internal/integration"):
    lines = []
    for name in names:
        lines += ["=== RUN   " + name, "--- PASS: " + name + " (1.00s)"]
    return "\n".join([*lines, "PASS", "ok\t" + package + "\t1.002s", ""])


class TagPlannerTests(unittest.TestCase):
    def test_new_test_and_new_file_join_existing_batch_without_ci_edit(self):
        first = fixture(tests=("TestExisting", "TestNewInSameFile"))
        second = fixture(path="internal/integration/another_test.go", tests=("TestNewInNewFile",))
        plan = engine.discover([first, second])
        self.assertEqual(plan["required_profiles"], 1)
        self.assertEqual(plan["required_executions"], 3)
        self.assertEqual(plan["required_parents"]["./internal/integration"],
                         ["TestExisting", "TestNewInNewFile", "TestNewInSameFile"])
        self.assertNotIn("Test", plan["include"][0]["tags"])

    def test_new_bucket_is_discovered_and_disjoint_by_tags(self):
        plan = engine.discover([fixture(), fixture(path="internal/integration/second_test.go",
                           tests=("TestSecond",), category="recovery", batch="slow")])
        self.assertEqual([(profile["category"], profile["batch"]) for profile in plan["include"]],
                         [("recovery", "slow"), ("short", "basic")])

    def test_new_capability_tag_is_selected_from_source(self):
        file = fixture()
        file["tags"].append("new_capability")
        file["constraint"] = combine("and", file["constraint"], combine("or", tag("all"), tag("new_capability")))
        profile = engine.discover([file])["include"][0]
        self.assertIn("new_capability", profile["tags"].split(","))

    def test_platform_and_toolchain_tags_cannot_be_enabled_as_capabilities(self):
        for reserved in ("linux", "darwin", "amd64", "arm64", "cgo", "go1.25", "amd64.v3", "race"):
            file = fixture()
            file["tags"].append(reserved)
            file["constraint"] = combine("and", file["constraint"], tag(reserved))
            with self.subTest(tag=reserved), self.assertRaisesRegex(ValueError, "platform/toolchain"):
                engine.discover([file])
        with self.assertRaisesRegex(ValueError, "platform-specific"):
            engine.discover([fixture(path="internal/integration/runtime_linux_test.go")])

    def test_long_batch_names_keep_distinct_stable_artifact_ids(self):
        prefix = "a" * 90
        first = fixture(batch=prefix + "first")
        second = fixture(path="internal/integration/second_test.go", tests=("TestSecond",), batch=prefix + "second")
        profiles = engine.discover([first, second])["include"]
        self.assertNotEqual(profiles[0]["id"], profiles[1]["id"])
        self.assertTrue(all(len(profile["id"]) <= 80 for profile in profiles))

    def test_unknown_category_and_missing_classification_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown CI category"):
            engine.discover([fixture(category="invented")])
        file = fixture()
        file["tags"] = ["all", "integration"]
        file["constraint"] = combine("or", tag("all"), tag("integration"))
        with self.assertRaisesRegex(ValueError, "missing a CI category"):
            engine.discover([file])

    def test_duplicate_parent_ownership_and_tag_leak_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "overlapping"):
            engine.discover([fixture(), fixture(path="internal/integration/dup_test.go", batch="other")])
        first, second = fixture(), fixture(path="internal/integration/second_test.go", tests=("TestSecond",), batch="other")
        second["constraint"] = combine("or", tag("all"), combine("and", tag("integration"), tag("ci_short")))
        with self.assertRaisesRegex(ValueError, "overlap"):
            engine.discover([first, second])

    def test_missing_all_tag_is_rejected(self):
        file = fixture()
        file["constraint"] = file["constraint"]["children"][1]
        with self.assertRaisesRegex(ValueError, "missing from the all tag"):
            engine.discover([file])

    def test_missing_invalid_or_conflicting_metadata_is_rejected(self):
        for metadata in ([], ["timeout=20m"], ["timeout=20m job-timeout=20"],
                         ["timeout=20m job-timeout=35 unknown=x"],
                         ["timeout=20m job-timeout=35 timeout=10m"],
                         ["timeout=20m job-timeout=35 case=bridge"],
                         ["timeout=$(touch /tmp/unsafe) job-timeout=35"],
                         ["timeout=20m job-timeout=35", "timeout=20m job-timeout=35"]):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                file = fixture()
                file["metadata"] = metadata
                engine.discover([file])
        second = fixture(path="internal/integration/second_test.go", tests=("TestSecond",))
        second["metadata"] = ["timeout=21m job-timeout=35"]
        with self.assertRaisesRegex(ValueError, "conflicting"):
            engine.discover([fixture(), second])

    def test_malformed_input_cannot_spawn_native_test_or_allocate_artifacts(self):
        file = fixture()
        file["metadata"] = ["timeout=20m job-timeout=35 checker=shell case=x"]
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=[file]), \
                patch.object(engine.subprocess, "Popen") as popen, patch.object(engine, "compile_inventory") as compiled:
            output = Path(temporary) / "never-created"
            with patch("sys.stderr", new=io.StringIO()):
                exit_code = engine.main(["run", "--category", "short", "--batch", "basic", "--directory", str(output)])
            self.assertEqual(exit_code, 1)
            self.assertFalse(output.exists())
            popen.assert_not_called()
            compiled.assert_not_called()

    def test_empty_and_unsupported_profile_fail_before_execution(self):
        with self.assertRaisesRegex(ValueError, "no required"):
            engine.discover([])
        file = fixture()
        plan = engine.discover([file])
        with patch.object(engine, "compile_inventory") as compiled, self.assertRaisesRegex(ValueError, "unsupported"):
            engine.run_profile([file], plan, "short", "unknown", None, Path("/never/create"))
        compiled.assert_not_called()

    def test_optional_and_sdk_tests_need_explicit_extra_tag(self):
        for opt_in in ("ci_optional", "ci_sdk"):
            file = fixture(path="internal/integration/optional_test.go", tests=("TestOptional",))
            file["tags"].append(opt_in)
            with self.assertRaisesRegex(ValueError, "explicit extra tag"):
                engine.discover([fixture(), file])
            file["constraint"] = combine("and", file["constraint"], tag("native_regression"))
            file["tags"].append("native_regression")
            self.assertEqual(engine.discover([fixture(), file])["required_executions"], 1)

    def test_explicit_optional_source_profile_owns_tags_and_budgets(self):
        file = fixture(path="internal/integration/optional_test.go", category="optional", batch="native_rgw_translation")
        file["constraint"] = combine("and", file["constraint"], tag("native_regression"))
        file["tags"].append("native_regression")
        plan = engine.discover_optional([file])
        self.assertEqual(len(plan["include"]), 1)
        profile = plan["include"][0]
        self.assertEqual(profile["batch"], "native_rgw_translation")
        self.assertIn("native_regression", profile["tags"].split(","))
        self.assertEqual(profile["timeout"], "20m")
        self.assertEqual(profile["job_timeout"], 35)
        self.assertEqual(engine.discover([fixture(), file])["required_executions"], 1)

    def test_optional_unknown_batch_never_executes(self):
        file = fixture()
        plan = engine.discover_optional([file])
        with patch.object(engine, "compile_inventory") as compiled, self.assertRaisesRegex(ValueError, "unsupported"):
            engine.run_profile([file], plan, "optional", "not_declared", None, Path("/never/create"))
        compiled.assert_not_called()

    def test_code_and_sdk_bridge_profiles_have_no_ceph_requirement(self):
        code = fixture(path="internal/integration/code_test.go", category="code", batch="code", tests=("TestCode",))
        sdk = fixture(path="internal/dockerbridge/runtime_test.go", tests=("TestSDKBridge",),
                      category="environment", batch="sdk")
        plan = engine.discover([code, sdk])
        self.assertEqual([profile["requires_ceph"] for profile in plan["include"]], [False, False])
        self.assertEqual(len({profile["id"] for profile in plan["include"]}), 2)
        self.assertTrue(engine.evaluate(sdk["constraint"], {"all"}))
        self.assertTrue(engine.evaluate(sdk["constraint"], {"integration"}))
        self.assertFalse(engine.evaluate(sdk["constraint"], {"integration", "ci", "ci_short"}))
        self.assertTrue(engine.evaluate(sdk["constraint"], {"integration", "ci", "ci_environment"}))

    def test_category_plans_are_disjoint_and_exhaustive_without_narrowing_coverage(self):
        catalog = [fixture(path="internal/integration/" + category + "_test.go",
                           tests=("Test" + category.title(),), category=category, batch=category)
                   for category in engine.CATEGORIES]
        plan = engine.discover(catalog)
        selected = [engine.select_required_plan(plan, category) for category in engine.RUNTIME_CATEGORIES]
        global_profiles = [profile for profile in plan["include"] if profile["category"] != "code"]
        self.assertEqual(sorted(profile["id"] for item in selected for profile in item["include"]),
                         sorted(profile["id"] for profile in global_profiles))
        self.assertEqual(sum(len(item["include"]) for item in selected), len(global_profiles))
        for category, item in zip(engine.RUNTIME_CATEGORIES, selected):
            self.assertEqual({profile["category"] for profile in item["include"]}, {category})
            self.assertEqual(item["coverage_scope"], "required")
            self.assertEqual(item["required_include"], plan["include"])
            self.assertEqual(item["required_parents"], plan["required_parents"])
            self.assertEqual(item["required_executions"], plan["required_executions"])
            self.assertEqual(item["required_profiles"], plan["required_profiles"])
        self.assertEqual(len(plan["include"]), len(engine.CATEGORIES))

    def test_empty_or_unsupported_category_cannot_produce_a_matrix(self):
        plan = engine.discover([fixture()])
        for category in ("code", "unknown", "environment"):
            with self.subTest(category=category), self.assertRaises(ValueError):
                engine.select_required_plan(plan, category)
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=[fixture()]), \
                patch("sys.stderr", new=io.StringIO()), patch.object(engine, "verify") as verifier:
            output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--category", "environment", "--verify", "--github-output", str(output)]), 1)
            self.assertFalse(output.exists())
            verifier.assert_not_called()

    def test_filtered_plan_verification_still_covers_the_entire_required_universe(self):
        catalog = [fixture(), fixture(path="internal/dockerbridge/runtime_test.go", category="environment",
                                     batch="sdk", tests=("TestEnvironment",))]
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), \
                patch.object(engine, "verify", return_value={"passed": True}) as verifier, \
                patch("sys.stdout", new=io.StringIO()):
            output = Path(temporary) / "plan.json"
            github_output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--category", "environment", "--verify", "--output", str(output),
                                          "--github-output", str(github_output)]), 0)
            report = json.loads(output.read_text())
            matrix = json.loads(github_output.read_text().split("=", 1)[1])
        verified_plan = verifier.call_args.args[1]
        self.assertEqual(len(verified_plan["include"]), 2)
        self.assertEqual(report["required_profiles"], 2)
        self.assertEqual(report["selected_profiles"], 1)
        self.assertEqual(report["compiled_coverage"], {"passed": True})
        self.assertEqual(matrix["include"], report["include"])
        self.assertEqual(matrix["include"][0]["category"], "environment")
        self.assertFalse(matrix["include"][0]["requires_ceph"])

    def test_optional_plan_is_explicit_and_cannot_claim_required_coverage(self):
        optional = fixture(path="internal/integration/optional_test.go", category="optional", batch="native_rgw_translation")
        optional["constraint"] = combine("and", optional["constraint"], tag("native_regression"))
        optional["tags"].append("native_regression")
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=[fixture(), optional]), \
                patch.object(engine, "verify") as verifier, patch("sys.stdout", new=io.StringIO()):
            output = Path(temporary) / "plan.json"
            github_output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--optional", "--batch", "native_rgw_translation",
                                          "--output", str(output), "--github-output", str(github_output)]), 0)
            report = json.loads(output.read_text())
            matrix = json.loads(github_output.read_text().split("=", 1)[1])
        self.assertEqual(report["coverage_scope"], "optional")
        self.assertEqual(report["selected_batch"], "native_rgw_translation")
        self.assertEqual(matrix["include"], report["include"])
        self.assertEqual(matrix["include"][0]["category"], "optional")
        self.assertFalse(any(key.startswith("required_") for key in report))
        self.assertNotIn("compiled_coverage", report)
        verifier.assert_not_called()
        with self.assertRaisesRegex(ValueError, "unsupported or empty"):
            engine.select_optional_plan(engine.discover_optional([optional]), "not_declared")

    def test_optional_plan_requires_explicit_batch_and_rejects_required_flags(self):
        arguments = (["plan", "--optional"], ["plan", "--batch", "native_rgw_translation"],
                     ["plan", "--optional", "--batch", "native_rgw_translation", "--verify"],
                     ["plan", "--optional", "--batch", "native_rgw_translation", "--category", "short"])
        for command in arguments:
            with self.subTest(command=command), patch.object(engine, "load_catalog") as catalog, \
                    patch("sys.stderr", new=io.StringIO()):
                self.assertEqual(engine.main(command), 1)
                catalog.assert_not_called()

    def test_optional_compile_uses_only_the_explicit_optional_source_profile(self):
        optional = fixture(path="internal/integration/optional_test.go", category="optional", batch="native_rgw_translation")
        optional["constraint"] = combine("and", optional["constraint"], tag("native_regression"))
        optional["tags"].append("native_regression")
        with patch.object(engine, "load_catalog", return_value=[fixture(), optional]), \
                patch.object(engine, "compile_profile", return_value={"passed": True}) as compiler, \
                patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(engine.main(["compile", "--category", "optional", "--batch", "native_rgw_translation",
                                          "--directory", "/never/create"]), 0)
        selected_plan = compiler.call_args.args[0]
        self.assertEqual(selected_plan["coverage_scope"], "optional")
        self.assertEqual(len(selected_plan["include"]), 1)
        self.assertEqual(selected_plan["include"][0]["category"], "optional")
        self.assertIn("native_regression", selected_plan["include"][0]["tags"].split(","))

    def test_compiled_inventory_must_match_all_category_and_batch_sources(self):
        file = fixture()
        plan = engine.discover([file])
        with patch.object(engine, "compile_inventory", return_value=["TestNewFeature"]) as compiled:
            report = engine.verify([file], plan)
        self.assertTrue(report["passed"])
        self.assertEqual(len(report["profiles"]), 3)
        self.assertEqual(compiled.call_count, 3)
        with patch.object(engine, "compile_inventory", return_value=["TestDifferent"]), self.assertRaisesRegex(ValueError, "all-tag"):
            engine.verify([file], plan)

    def test_plan_github_matrix_excludes_code_and_records_source_metadata(self):
        catalog = [fixture(), fixture(path="internal/integration/code_test.go", category="code", batch="pure", tests=("TestCode",))]
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), patch("sys.stdout", new=io.StringIO()):
            output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--github-output", str(output)]), 0)
            matrix = json.loads(output.read_text().split("=", 1)[1])
        self.assertEqual(len(matrix["include"]), 1)
        self.assertEqual(matrix["include"][0]["category"], "short")
        self.assertEqual(matrix["include"][0]["timeout"], "20m")


class CompletionTests(unittest.TestCase):
    def test_started_leaf_missing_pass_skip_duplicate_and_package_failure_rejected(self):
        good = native_log(("TestNewFeature", "TestNewFeature/leaf"))
        self.assertTrue(engine.completion(good, ["TestNewFeature"], "example/internal/integration")["passed"])
        for invalid in (good.replace("--- PASS: TestNewFeature/leaf (1.00s)\n", ""),
                        good.replace("--- PASS: TestNewFeature/leaf", "--- SKIP: TestNewFeature/leaf"),
                        good + "=== RUN   TestNewFeature\n",
                        good.replace("ok\t", "FAIL\t")):
            with self.subTest(log=invalid):
                self.assertFalse(engine.completion(invalid, ["TestNewFeature"], "example/internal/integration")["passed"])

    def test_pass_before_run_or_after_package_completion_is_rejected(self):
        before = "--- PASS: TestNewFeature (1.00s)\n=== RUN   TestNewFeature\nPASS\nok\texample/internal/integration\t1.002s\n"
        after = "=== RUN   TestNewFeature\nPASS\nok\texample/internal/integration\t1.002s\n--- PASS: TestNewFeature (1.00s)\n"
        for invalid in (before, after):
            self.assertFalse(engine.completion(invalid, ["TestNewFeature"], "example/internal/integration")["passed"])

    def test_strict_completion_uses_existing_checker_and_original_case(self):
        for checker, case in (("quiescence", "process-quiescence-bridge-peer"),
                              ("recovery", "process-recovery-host-directory"), ("receivers", "host")):
            module = __import__(engine.CHECKERS[checker][0])
            with patch.object(module, "validate", return_value={"passed": True}) as validator:
                self.assertTrue(engine.strict_completion("native log", {"checker": checker, "case": case})["passed"])
            validator.assert_called_once_with("native log", case)

    def test_isolated_scope_ownership_covers_exact_original_aggregate(self):
        checker = "receivers"
        parent = engine.CHECKERS[checker][1]
        aggregate = fixture(tests=(parent,), category="recovery", batch="unused")
        aggregate["constraint"] = combine("or", tag("all"), combine("and", tag("ci_recovery"), negate(tag("ci_batch"))))
        aggregate["tags"] = ["all", "ci_recovery", "ci_batch"]
        aggregate["metadata"] = []
        profiles = []
        for network in ("bridge", "host"):
            file = fixture(path="internal/integration/" + network + "_test.go", tests=(parent,), category="recovery", batch=network)
            file["constraint"] = combine("and", tag("ci"), tag("ci_recovery"), tag("ci_batch"), tag("ci_batch_" + network))
            file["tags"].remove("all")
            file["metadata"] = ["timeout=20m job-timeout=35 checker=receivers case=" + network]
            profiles.append(file)
        self.assertEqual(engine.discover([aggregate, *profiles])["required_executions"], 2)
        with self.assertRaisesRegex(ValueError, "do not cover"):
            engine.discover([aggregate, profiles[0]])

    def test_native_command_has_tags_and_never_test_name_selector(self):
        file = fixture()
        plan = engine.discover([file])
        process = unittest.mock.Mock()
        process.stdout = io.StringIO(native_log())
        process.wait.return_value = 0
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "go.mod").write_text("module example\n")
            output = root / "output"
            with patch.object(engine, "compile_inventory", return_value=file["tests"]), \
                    patch.object(engine.subprocess, "Popen", return_value=process) as popen, \
                    patch("sys.stdout", new=io.StringIO()), \
                    patch.dict(os.environ, {"CEPH_TEST_RBD_CLIENT_IMAGE": "override", "CEPH_TEST_VAULT_IMAGE": "vault-custom"}):
                report = engine.run_profile([file], plan, "short", "basic", None, output, root)
            self.assertTrue(report["passed"])
            command = popen.call_args.args[0]
            self.assertFalse(any(argument in ("-run", "-short", "-failfast") for argument in command))
            self.assertIn("-mod=readonly", command)
            self.assertIn("-count=1", command)
            self.assertIn("-timeout=20m", command)
            self.assertNotIn("CEPH_TEST_RBD_CLIENT_IMAGE", popen.call_args.kwargs["env"])
            self.assertEqual(popen.call_args.kwargs["env"]["CEPH_TEST_VAULT_IMAGE"], "vault-custom")
            self.assertTrue((output / "native.log").is_file())
            self.assertTrue((output / "report.json").is_file())

    def test_compile_phase_does_not_execute_any_test(self):
        plan = engine.discover([fixture()])
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as compiler:
            report = engine.compile_profile(plan, "short", "basic", None, Path(temporary) / "compile")
        self.assertTrue(report["passed"])
        command = compiler.call_args.args[0]
        self.assertIn("-c", command)
        self.assertFalse(any(argument.startswith(("-run", "-short", "-list")) for argument in command))


class GoASTCatalogTests(unittest.TestCase):
    def test_ast_ignores_strings_comments_methods_and_finds_aliased_testing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "go.mod").write_text("module example\ngo 1.25\n")
            (root / "fixture_test.go").write_text('''//go:build all || integration
//ci: timeout=20m job-timeout=35

package fixture
import test "testing"
// func TestInComment(t *testing.T) {}
var text = "func TestInString(t *testing.T) {}"
type receiver int
func (receiver) TestMethod(t *test.T) {}
func TestActual(t *test.T) {}
func helper(t *test.T) {}
''')
            catalog = engine.load_catalog(root)
        self.assertEqual(catalog[0]["tests"], ["TestActual"])
        self.assertEqual(catalog[0]["metadata"], ["timeout=20m job-timeout=35"])
        self.assertTrue(engine.evaluate(catalog[0]["constraint"], {"all"}))


if __name__ == "__main__":
    unittest.main()
