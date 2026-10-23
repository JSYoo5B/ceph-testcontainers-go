"""Test source-owned tag planning and execution gates without Docker."""

import io
import json
import os
import re
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

def github_outputs(path):
    """The key=value lines that a step appended to GITHUB_OUTPUT."""
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


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

    def test_image_roles_are_declared_per_batch_and_default_to_all_five(self):
        declared = fixture(path="internal/integration/control_test.go", tests=("TestControl",), batch="control")
        declared["metadata"] = ["timeout=10m job-timeout=20 image-roles=cephfs,essential"]
        plan = engine.discover([fixture(), declared])
        roles = {profile["batch"]: profile["image_roles"] for profile in plan["include"]}
        self.assertEqual(roles, {"basic": "essential,client,cephfs,rbd-aux,rgw", "control": "essential,cephfs"})
        self.assertEqual(engine.IMAGE_ROLES, ("essential", "client", "cephfs", "rbd-aux", "rgw"))
        for value in ("osd", "control,control", "control,client", "control,", "Control"):
            with self.subTest(roles=value), self.assertRaisesRegex(ValueError, "image-roles must list"):
                declared["metadata"] = ["timeout=10m job-timeout=20 image-roles=" + value]
                engine.discover([declared])
        # One batch declares one set, like its budget.
        declared["metadata"] = ["timeout=20m job-timeout=35 image-roles=essential"]
        same_batch = fixture(path="internal/integration/second_test.go", tests=("TestSecond",))
        declared_batch = fixture(path="internal/integration/control_test.go", tests=("TestControl",))
        declared_batch["metadata"] = declared["metadata"]
        with self.assertRaisesRegex(ValueError, "conflicting budgets, checker scope or image roles"):
            engine.discover([same_batch, declared_batch])

    def test_image_roles_need_a_ceph_profile(self):
        code = fixture(path="internal/integration/code_test.go", category="code", batch="pure", tests=("TestCode",))
        sdk = fixture(path="internal/dockerbridge/runtime_test.go", tests=("TestSDKBridge",),
                      category="environment", batch="sdk")
        self.assertEqual([profile["image_roles"] for profile in engine.discover([code, sdk])["include"]], ["", ""])
        for file in (code, sdk):
            file["metadata"] = ["timeout=20m job-timeout=35 image-roles=essential"]
            with self.subTest(path=file["path"]), self.assertRaisesRegex(ValueError, "needs a Ceph runtime profile"):
                engine.discover([file])

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
        self.assertEqual(profile["image_roles"], "essential,client,cephfs,rbd-aux,rgw")
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
            matrix = json.loads(github_outputs(github_output)["matrix"])
        verified_plan = verifier.call_args.args[1]
        self.assertEqual(len(verified_plan["include"]), 2)
        self.assertEqual(report["required_profiles"], 2)
        self.assertEqual(report["selected_profiles"], 1)
        self.assertEqual(report["compiled_coverage"], {"passed": True})
        # A batch without network modes is one entry, as before the axis.
        self.assertEqual(matrix["include"], [{**profile, "network": "", "name": profile["batch"], "tests": "",
                                              "repetition": "1", "artifact_suffix": ""}
                                             for profile in report["include"]])
        self.assertEqual(matrix["include"][0]["category"], "environment")
        self.assertFalse(matrix["include"][0]["requires_ceph"])

    def representative_catalog(self):
        """One representative batch per theme, plus an ordinary batch."""
        catalog = []
        for index, theme in enumerate(engine.REPRESENTATIVE_THEMES):
            file = fixture(path="internal/integration/" + theme + "_test.go", tests=("TestTheme" + str(index),),
                           batch="theme_" + str(index))
            file["metadata"] = ["timeout=20m job-timeout=30 representative=" + theme]
            catalog.append(file)
        catalog.append(fixture())
        return catalog

    def test_representative_set_is_declared_in_source_and_covers_every_theme(self):
        catalog = self.representative_catalog()
        plan = engine.discover(catalog)
        selected = engine.select_representative_plan(plan)
        self.assertEqual(sorted(profile["representative"] for profile in selected["include"]),
                         sorted(engine.REPRESENTATIVE_THEMES))
        self.assertNotIn("basic", [profile["batch"] for profile in selected["include"]])
        # The required universe stays whole; the campaign only repeats a part.
        self.assertEqual(len(selected["required_include"]), len(engine.REPRESENTATIVE_THEMES) + 1)
        with self.assertRaisesRegex(ValueError, "lacks the theme"):
            engine.select_representative_plan(engine.discover(catalog[1:]))
        for value in ("Flaky", "smoke", "flaky,auth"):
            with self.subTest(theme=value), self.assertRaisesRegex(ValueError, "representative takes one of"):
                catalog[0]["metadata"] = ["timeout=20m job-timeout=30 representative=" + value]
                engine.discover(catalog)

    def test_representative_theme_is_one_per_batch_like_its_budget(self):
        first = fixture()
        first["metadata"] = ["timeout=20m job-timeout=35 representative=flaky"]
        second = fixture(path="internal/integration/second_test.go", tests=("TestSecond",))
        with self.assertRaisesRegex(ValueError, "conflicting"):
            engine.discover([first, second])
        second["metadata"] = first["metadata"]
        self.assertEqual(engine.discover([first, second])["include"][0]["representative"], "flaky")

    def test_repetitions_give_each_attempt_its_own_job_and_artifacts_but_one_profile_id(self):
        plan = engine.select_representative_plan(engine.discover(self.representative_catalog()))
        entries = engine.network_matrix(plan["include"], "default")
        self.assertEqual(engine.repeat_matrix(entries, 1), entries)
        repeated = engine.repeat_matrix(entries, 3)
        self.assertEqual(len(repeated), 3 * len(entries))
        self.assertEqual([entry["repetition"] for entry in repeated[:len(entries) + 1]],
                         ["1"] * len(entries) + ["2"])
        names = {(entry["id"], entry["artifact_suffix"]) for entry in repeated}
        self.assertEqual(len(names), len(repeated))
        self.assertEqual({entry["id"] for entry in repeated}, {entry["id"] for entry in entries})
        self.assertTrue(all(entry["name"].endswith(" #" + entry["repetition"]) for entry in repeated))
        for value in (0, engine.MAX_REPETITIONS + 1, "3"):
            with self.subTest(repetitions=value), self.assertRaisesRegex(ValueError, "repetitions must be"):
                engine.repeat_matrix(entries, value)
        many = [{**entries[0], "id": "e" + str(index)} for index in range(engine.MAX_CAMPAIGN_JOBS)]
        with self.assertRaisesRegex(ValueError, "jobs; the bound"):
            engine.repeat_matrix(many, 2)
        long = [{**entry, "job_timeout": 360} for entry in entries]
        with self.assertRaisesRegex(ValueError, "minutes; the bound"):
            engine.repeat_matrix(long, 2)

    def test_representative_plan_cli_writes_the_repeated_matrix(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "load_catalog", return_value=self.representative_catalog()), \
                patch("sys.stdout", new=io.StringIO()), patch("sys.stderr", new=io.StringIO()):
            output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--representative", "--repetitions", "2",
                                          "--github-output", str(output)]), 0)
            values = github_outputs(output)
            self.assertEqual(values["count"], str(2 * len(engine.REPRESENTATIVE_THEMES)))
            for arguments in (["--repetitions", "2"], ["--representative", "--category", "short"],
                              ["--representative", "--verify"], ["--representative", "--repetitions", "11"]):
                with self.subTest(arguments=arguments):
                    self.assertEqual(engine.main(["plan", *arguments]), 1)

    def test_optional_role_profile_prepares_images_outside_integration_package(self):
        file = fixture(path="rbd/role_test.go", tests=("TestRoleMirror",),
                       category="optional", batch="role_mirror")
        file["tags"] += ["role_images"]
        file["constraint"] = combine("and", file["constraint"], tag("role_images"))
        file["metadata"] = ["timeout=20m job-timeout=35 image-roles=essential,client,rbd-aux"]
        plan = engine.discover_optional([file])
        profile = plan["include"][0]
        self.assertTrue(profile["requires_ceph"])
        self.assertEqual(profile["image_roles"], "essential,client,rbd-aux")
        self.assertIn("role_images", profile["tags"].split(","))
        self.assertEqual(engine.discover([fixture(), file])["required_executions"], 1)

    def test_optional_batch_cannot_be_representative(self):
        optional = fixture(path="internal/integration/optional_test.go", tests=("TestOptional",), batch="regression")
        optional["tags"] = ["ci_optional", "ci_batch_regression", "native_regression"]
        optional["constraint"] = combine("and", tag("ci_optional"), tag("ci_batch_regression"))
        optional["metadata"] = ["timeout=20m job-timeout=35 representative=flaky"]
        with self.assertRaisesRegex(ValueError, "optional batch cannot be representative"):
            engine.discover_optional([optional])

    def test_network_modes_make_one_entry_per_mode_of_the_scope(self):
        host = fixture(tests=("TestHostFirst", "TestModeless"))
        host["networks"], host["network_tests"] = ["host", "bridge"], ["TestHostFirst"]
        same = fixture(path="internal/integration/same_test.go", tests=("TestSame",))
        same["networks"], same["network_tests"] = ["host", "bridge"], ["TestSame"]
        # A file without modes joins the batch and runs in its default mode.
        modeless = fixture(path="internal/integration/modeless_test.go", tests=("TestOther",))
        rgw = fixture(path="internal/integration/rgw_test.go", tests=("TestRGW",), batch="rgw")
        rgw["networks"], rgw["network_tests"] = ["bridge"], ["TestRGW"]
        plain = fixture(path="internal/integration/plain_test.go", tests=("TestPlain",), batch="plain")
        plan = engine.discover([host, same, modeless, rgw, plain])
        self.assertEqual({profile["batch"]: (profile["networks"], profile["network_tests"]) for profile in plan["include"]},
                         {"basic": (["host", "bridge"], ["TestHostFirst", "TestSame"]), "rgw": (["bridge"], ["TestRGW"]),
                          "plain": ([], [])})
        default = engine.network_matrix(plan["include"], "default")
        self.assertEqual([(entry["name"], entry["network"], entry["tests"]) for entry in default],
                         [("basic", "host", ""), ("plain", "", ""), ("rgw", "bridge", "")])
        everything = engine.network_matrix(plan["include"], "all")
        self.assertEqual([(entry["name"], entry["network"], entry["tests"]) for entry in everything],
                         [("basic", "host", ""), ("basic (bridge)", "bridge", "TestHostFirst,TestSame"),
                          ("plain", "", ""), ("rgw", "bridge", "")])
        # The default entry keeps the profile's ID; another mode's is distinct.
        basic = [entry for entry in everything if entry["batch"] == "basic"]
        self.assertEqual(basic[0]["id"], plan["include"][0]["id"])
        self.assertEqual(basic[1]["id"], plan["include"][0]["id"] + "-bridge")
        with self.assertRaisesRegex(ValueError, "network scope"):
            engine.network_matrix(plan["include"], "some")

    def test_network_modes_must_agree_within_a_batch(self):
        for name, modes in {"other modes": (["bridge"], ["host", "bridge"]),
                            "other default": (["bridge", "host"], ["host", "bridge"])}.items():
            first = fixture(tests=("TestFirst",))
            second = fixture(path="internal/integration/second_test.go", tests=("TestSecond",))
            first["networks"], second["networks"] = modes
            first["network_tests"], second["network_tests"] = ["TestFirst"], ["TestSecond"]
            with self.subTest(name), self.assertRaisesRegex(ValueError, "declare different networkModes"):
                engine.discover([first, second])
        for modes in (["overlay"], ["host", "host"]):
            file = fixture()
            file["networks"], file["network_tests"] = modes, ["TestNewFeature"]
            with self.subTest(modes=modes), self.assertRaisesRegex(ValueError, "distinct modes"):
                engine.discover([file])
        helper = fixture(path="internal/integration/helpers_test.go", tests=())
        helper["metadata"], helper["networks"] = [], ["host"]
        with self.assertRaisesRegex(ValueError, "file that declares the test"):
            engine.discover([fixture(), helper])

    def test_plan_writes_the_network_scope_to_the_matrix(self):
        file = fixture()
        file["networks"], file["network_tests"] = ["host", "bridge"], ["TestNewFeature"]
        for scope, networks in (("default", ["host"]), ("all", ["host", "bridge"])):
            with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=[file]), \
                    patch("sys.stdout", new=io.StringIO()):
                github_output = Path(temporary) / "github-output"
                self.assertEqual(engine.main(["plan", "--category", "short", "--networks", scope,
                                              "--github-output", str(github_output)]), 0)
                outputs = github_outputs(github_output)
            matrix = json.loads(outputs["matrix"])
            self.assertEqual([entry["network"] for entry in matrix["include"]], networks, scope)
            self.assertEqual(outputs["count"], str(len(networks)))

    def test_run_selects_only_tests_that_declare_network_modes(self):
        file = fixture(tests=("TestNetworked", "TestModeless"))
        file["networks"], file["network_tests"] = ["host", "bridge"], ["TestNetworked"]
        plan = engine.discover([file])
        for selection in ("TestModeless", "TestNetworked,TestNetworked", "TestMissing"):
            with self.subTest(selection), tempfile.TemporaryDirectory() as temporary, \
                    self.assertRaisesRegex(ValueError, "test selection"):
                engine.run_profile([file], plan, "short", "basic", None, Path(temporary) / "native", tests=selection)
        commands = []

        class Child:
            def __init__(self, command, **_):
                commands.append(command)
                self.stdout = io.StringIO(native_log(("TestNetworked",)))

            def wait(self):
                return 0
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "compile_inventory", return_value=["TestModeless", "TestNetworked"]), \
                patch.object(engine.subprocess, "Popen", Child), patch("sys.stdout", new=io.StringIO()):
            root = Path(temporary)
            (root / "go.mod").write_text("module example\n")
            report = engine.run_profile([file], plan, "short", "basic", None, root / "native", root=root, tests="TestNetworked")
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["compiled_tests"], ["TestNetworked"])
        self.assertEqual(commands[0][commands[0].index("-run") + 1], "^(TestNetworked)$")

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
            matrix = json.loads(github_outputs(github_output)["matrix"])
        self.assertEqual(report["coverage_scope"], "optional")
        self.assertEqual(report["selected_batch"], "native_rgw_translation")
        # A batch without network modes is one entry, as before the axis.
        self.assertEqual(matrix["include"], [{**profile, "network": "", "name": profile["batch"], "tests": "",
                                              "repetition": "1", "artifact_suffix": ""}
                                             for profile in report["include"]])
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

    def test_changed_scope_keeps_only_batches_whose_own_test_files_changed(self):
        catalog = [fixture(path="internal/integration/one_test.go", category="topology", batch="one", tests=("TestOne",)),
                   fixture(path="internal/integration/one_more_test.go", category="topology", batch="one", tests=("TestOneMore",)),
                   fixture(path="internal/integration/two_test.go", category="topology", batch="two", tests=("TestTwo",))]
        plan = engine.select_required_plan(engine.discover(catalog), "topology")
        self.assertEqual([profile["sources"] for profile in plan["include"]],
                         [["internal/integration/one_more_test.go", "internal/integration/one_test.go"],
                          ["internal/integration/two_test.go"]])
        for changed, batches in {
                ("internal/integration/one_more_test.go",): ["one"],
                ("internal/integration/one_test.go", "internal/integration/two_test.go"): ["one", "two"],
                # A shared helper, a public package or docs select nothing:
                # the periodic matrix runs those batches on main.
                ("internal/integration/one_integration_helpers_test.go", "ceph/ceph.go", "docs/ceph.md"): [],
                (): []}.items():
            with self.subTest(changed=changed):
                selected = engine.select_changed_plan(plan, [*changed, ""])
                self.assertEqual([profile["batch"] for profile in selected["include"]], batches)
                self.assertEqual(selected["selected_scope"], "changed")
                self.assertEqual(selected["changed_files"], sorted(changed))
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), \
                patch("sys.stdout", new=io.StringIO()), patch("sys.stderr", new=io.StringIO()):
            changed, output = Path(temporary) / "changed", Path(temporary) / "github-output"
            changed.write_text("internal/integration/two_test.go\nREADME.md\n")
            self.assertEqual(engine.main(["plan", "--category", "topology", "--changed-files", str(changed),
                                          "--github-output", str(output)]), 0)
            outputs = github_outputs(output)
            self.assertEqual([profile["batch"] for profile in json.loads(outputs["matrix"])["include"]], ["two"])
            self.assertEqual(outputs["count"], "1")
            output.unlink()
            changed.write_text("docs/ceph.md\n")
            self.assertEqual(engine.main(["plan", "--category", "topology", "--changed-files", str(changed),
                                          "--github-output", str(output)]), 0)
            self.assertEqual(github_outputs(output), {"matrix": '{"include":[]}', "count": "0"})
            for arguments in (["--changed-files", str(changed)],
                              ["--category", "topology", "--verify", "--changed-files", str(changed)]):
                self.assertEqual(engine.main(["plan", *arguments]), 1)

    def test_declaration_selection_is_recorded_without_changing_the_matrix(self):
        catalog = [fixture(path="internal/integration/one_test.go", category="topology", batch="one", tests=("TestOne",)),
                   fixture(path="internal/integration/two_test.go", category="topology", batch="two", tests=("TestTwo",))]
        plan = engine.select_required_plan(engine.discover(catalog), "topology")
        one, two = (profile["id"] for profile in plan["include"])
        calls = []

        def tool(batches, base, root):
            calls.append((batches, base))
            return {"merge_base": "abc", "fallback": [], "changed_declarations": ["x_test.go: func helper"],
                    "batches": [{"id": one, "selected": True, "reason": "func TestOne reaches func helper"},
                                {"id": two, "selected": False, "reason": "none of 1 roots reaches a changed declaration"}]}

        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), \
                patch.object(engine, "run_batchselect", side_effect=tool), patch("sys.stdout", new=io.StringIO()):
            output, github = Path(temporary) / "plan.json", Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--category", "topology", "--select-declarations", "origin/main",
                                          "--output", str(output), "--github-output", str(github)]), 0)
            report = json.loads(output.read_text())
            matrix = json.loads(github_outputs(github)["matrix"])
        self.assertEqual(calls, [([{"id": profile["id"], "tags": profile["tags"]} for profile in plan["include"]],
                                  "origin/main")])
        # Advisory: both batches still run.
        self.assertEqual([entry["batch"] for entry in matrix["include"]], ["one", "two"])
        self.assertEqual([profile["batch"] for profile in report["include"]], ["one", "two"])
        selection = report["declaration_selection"]
        self.assertTrue(selection["advisory"])
        self.assertEqual(selection["selected"], {one: "func TestOne reaches func helper"})
        self.assertEqual(selection["excluded"], {two: "none of 1 roots reaches a changed declaration"})
        self.assertEqual((selection["selected_profiles"], selection["total_profiles"]), (1, 2))
        self.assertEqual(selection["changed_declarations"], ["x_test.go: func helper"])
        with patch("sys.stdout", new=io.StringIO()), patch("sys.stderr", new=io.StringIO()):
            self.assertEqual(engine.main(["plan", "--optional", "--batch", "x", "--select-declarations", "main"]), 1)

    def test_applied_declaration_selection_narrows_the_matrix_and_writes_the_summary(self):
        catalog = [fixture(path="internal/integration/one_test.go", category="short", batch="one", tests=("TestOne",)),
                   fixture(path="internal/integration/two_test.go", category="short", batch="two", tests=("TestTwo",))]
        plan = engine.select_required_plan(engine.discover(catalog), "short")
        one, two = (profile["id"] for profile in plan["include"])
        result = {"merge_base": "abc", "fallback": [], "changed_declarations": ["x_test.go: func helper"],
                  "batches": [{"id": one, "selected": True, "reason": "func TestOne reaches func helper"},
                              {"id": two, "selected": False, "reason": "none of 1 roots reaches a changed declaration"}]}
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), \
                patch.object(engine, "run_batchselect", return_value=result), patch("sys.stdout", new=io.StringIO()):
            output, github, summary = (Path(temporary) / name for name in ("plan.json", "github-output", "summary.md"))
            summary.write_text("earlier\n")
            self.assertEqual(engine.main(["plan", "--category", "short", "--select-declarations", "base",
                                          "--apply-declarations", "--summary", str(summary),
                                          "--output", str(output), "--github-output", str(github)]), 0)
            report = json.loads(output.read_text())
            outputs = github_outputs(github)
            text = summary.read_text()
        self.assertEqual([entry["batch"] for entry in json.loads(outputs["matrix"])["include"]], ["one"])
        self.assertEqual(outputs["count"], "1")
        self.assertEqual((report["selected_scope"], report["selected_profiles"]), ("declarations", 1))
        self.assertFalse(report["declaration_selection"]["advisory"])
        self.assertEqual(text, "earlier\n### Declaration selection: short\n\n1 of 2 batches selected since abc.\n\n"
                               "Only the selected batches run here, by opt-in. The selection is advisory: "
                               "pushes to main run every batch.\n\n"
                               "| Batch | Runs | Reason |\n| --- | --- | --- |\n"
                               "| " + one + " | yes | func TestOne reaches func helper |\n"
                               "| " + two + " | no | none of 1 roots reaches a changed declaration |\n")
        with patch("sys.stdout", new=io.StringIO()), patch("sys.stderr", new=io.StringIO()):
            for arguments in (["--category", "short", "--apply-declarations"],
                              ["--select-declarations", "base", "--apply-declarations"],
                              ["--category", "short", "--select-declarations", "base", "--apply-declarations",
                               "--changed-since", "base"]):
                self.assertEqual(engine.main(["plan", *arguments]), 1, arguments)

    def test_declaration_summary_names_the_fallback(self):
        selection = {"base": "main", "fallback": ["go.mod changed"], "selected": {"short-a": "every batch: go.mod changed"},
                     "excluded": {}, "selected_profiles": 1, "total_profiles": 1, "error": "x | y"}
        text = engine.declaration_summary(selection, "short")
        self.assertIn("1 of 1 batches selected since main.", text)
        self.assertIn("Every batch runs: go.mod changed", text)
        self.assertIn("the analysis failed: x \\| y", text)

    def test_declaration_selection_falls_back_to_every_profile(self):
        catalog = [fixture(path="internal/integration/one_test.go", batch="one", tests=("TestOne",)),
                   fixture(path="internal/dockerbridge/bridge_test.go", category="environment", batch="bridge",
                           tests=("TestBridge",))]
        profiles = engine.discover(catalog)["include"]
        ids = {profile["batch"]: profile["id"] for profile in profiles}
        for runner, reason in (
                (lambda batches, base, root: {"fallback": ["go.mod changed"], "batches": [
                    {"id": ids["one"], "selected": True, "reason": "every batch: go.mod changed"}]},
                 "every batch: go.mod changed"),
                (lambda batches, base, root: {"fallback": [], "batches": []}, "analysis unavailable"),
                (lambda batches, base, root: (_ for _ in ()).throw(ValueError("type check")), "analysis unavailable"),
                (lambda batches, base, root: (_ for _ in ()).throw(subprocess.TimeoutExpired("go", 1)),
                 "analysis unavailable")):
            with self.subTest(reason=reason):
                selection = engine.declaration_selection(profiles, "main", runner=runner)
                self.assertEqual(selection["excluded"], {})
                self.assertTrue(selection["selected"][ids["one"]].startswith(reason))
                self.assertIn("is not analysed", selection["selected"][ids["bridge"]])
                self.assertEqual(selection["selected_profiles"], 2)
        # Nothing to analyse: the tool is not run.
        selection = engine.declaration_selection([profile for profile in profiles if profile["batch"] == "bridge"],
                                                 "main", runner=lambda *arguments: self.fail("ran the tool"))
        self.assertEqual(list(selection["selected"].values()), ["package ./internal/dockerbridge is not analysed"])

    def test_run_batchselect_passes_the_head_and_fails_closed(self):
        completed = subprocess.CompletedProcess([], 0, stdout='{"batches": []}', stderr="")
        with patch.object(engine.subprocess, "run", return_value=completed) as run:
            self.assertEqual(engine.run_batchselect([{"id": "x", "tags": "a"}], "base", "/tree", head="abc"),
                             {"batches": []})
            command = run.call_args.args[0]
            self.assertEqual(command[:4], ["go", "run", "-mod=readonly", "./internal/tools/batchselect"])
            self.assertEqual(command[command.index("-head") + 1], "abc")
            self.assertEqual(command[command.index("-repo") + 1], str(engine.ROOT))
            self.assertEqual(command[command.index("-package") + 1], "./internal/integration")
            engine.run_batchselect([], "base", tool="/bin/tool")
            self.assertEqual(run.call_args.args[0][0], "/bin/tool")
            self.assertNotIn("-head", run.call_args.args[0])
        failed = subprocess.CompletedProcess([], 1, stdout="", stderr="boom")
        with patch.object(engine.subprocess, "run", return_value=failed), self.assertRaisesRegex(ValueError, "boom"):
            engine.run_batchselect([], "base")

    def test_plan_github_matrix_excludes_code_and_records_source_metadata(self):
        catalog = [fixture(), fixture(path="internal/integration/code_test.go", category="code", batch="pure", tests=("TestCode",))]
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=catalog), patch("sys.stdout", new=io.StringIO()):
            output = Path(temporary) / "github-output"
            self.assertEqual(engine.main(["plan", "--github-output", str(output)]), 0)
            matrix = json.loads(github_outputs(output)["matrix"])
        self.assertEqual(len(matrix["include"]), 1)
        self.assertEqual(matrix["include"][0]["category"], "short")
        self.assertEqual(matrix["include"][0]["timeout"], "20m")
        self.assertEqual(matrix["include"][0]["image_roles"], "essential,client,cephfs,rbd-aux,rgw")


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

    def test_strict_completion_composes_the_checker_case_from_mode_and_kind(self):
        for checker, kind, network, case in (("quiescence", "peer", "bridge", "process-quiescence-bridge-peer"),
                                             ("recovery", "directory", "host", "process-recovery-host-directory"),
                                             ("receivers", "", "host", "host")):
            module = __import__(engine.CHECKERS[checker][0])
            with patch.object(module, "validate", return_value={"passed": True}) as validator:
                self.assertTrue(engine.strict_completion("native log", {"checker": checker, "case": kind}, network)["passed"])
            validator.assert_called_once_with("native log", case)

    def checker_files(self, checker, kinds, networks=("host", "bridge")):
        """An all-tag aggregate and one CI file per receipt kind, each in
        every network mode, as the split checker batches are declared."""
        parent = engine.CHECKERS[checker][1]
        aggregate = fixture(tests=(parent,), category="recovery", batch="unused")
        aggregate["constraint"] = combine("or", tag("all"), combine("and", tag("ci_recovery"), negate(tag("ci_batch"))))
        aggregate["tags"] = ["all", "ci_recovery", "ci_batch"]
        aggregate["metadata"] = []
        files = []
        for kind in kinds:
            batch = checker + ("_" + kind if kind else "")
            file = fixture(path="internal/integration/" + batch + "_test.go", tests=(parent,), category="recovery", batch=batch)
            file["constraint"] = combine("and", tag("ci"), tag("ci_recovery"), tag("ci_batch"), tag("ci_batch_" + batch))
            file["tags"].remove("all")
            file["metadata"] = ["timeout=20m job-timeout=35 checker=" + checker + (" case=" + kind if kind else "")]
            file["networks"], file["network_tests"] = list(networks), [parent]
            files.append(file)
        return aggregate, files

    def test_checker_batches_cover_the_aggregate_in_every_network_mode(self):
        for checker, kinds in (("receivers", ("",)), ("quiescence", ("peer", "directory")),
                               ("recovery", ("peer", "directory"))):
            with self.subTest(checker=checker):
                aggregate, files = self.checker_files(checker, kinds)
                plan = engine.discover([aggregate, *files])
                self.assertEqual(plan["required_executions"], len(kinds))
                self.assertEqual([profile["networks"] for profile in plan["include"]], [["host", "bridge"]] * len(kinds))
                # A kind's file that leaves out a mode, or a missing kind,
                # leaves a case of the checker unowned.
                _, narrow = self.checker_files(checker, kinds, ("host",))
                with self.assertRaisesRegex(ValueError, "do not cover"):
                    engine.discover([aggregate, *narrow])
                if len(files) > 1:
                    with self.assertRaisesRegex(ValueError, "do not cover"):
                        engine.discover([aggregate, files[0]])

    def test_checker_metadata_names_a_kind_and_the_file_names_its_modes(self):
        for checker, case in (("receivers", "host"), ("quiescence", ""), ("quiescence", "bridge-peer"),
                              ("recovery", "process-recovery-host-peer"), ("quiescence", "other")):
            aggregate, files = self.checker_files(checker, ("peer",) if checker != "receivers" else ("",))
            files[0]["metadata"] = ["timeout=20m job-timeout=35 checker=" + checker + (" case=" + case if case else "")]
            with self.subTest(checker=checker, case=case), self.assertRaisesRegex(ValueError, "checker case"):
                engine.discover([aggregate, *files])
        aggregate, files = self.checker_files("receivers", ("",))
        del files[0]["networks"]
        with self.assertRaisesRegex(ValueError, "networkModes"):
            engine.discover([aggregate, *files])

    def test_checker_run_selects_one_mode_and_checks_its_case(self):
        aggregate, files = self.checker_files("quiescence", ("peer", "directory"))
        plan = engine.discover([aggregate, *files])
        parent = engine.CHECKERS["quiescence"][1]
        module = __import__(engine.CHECKERS["quiescence"][0])
        unset = {key: value for key, value in os.environ.items() if key != "CEPH_TEST_NETWORK"}
        for environment, network, tests in ((unset, "host", ""), ({**unset, "CEPH_TEST_NETWORK": "bridge"}, "bridge", parent)):
            process = unittest.mock.Mock()
            process.stdout = io.StringIO(native_log((parent,)))
            process.wait.return_value = 0
            with self.subTest(network=network), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "go.mod").write_text("module example\n")
                with patch.object(engine, "compile_inventory", return_value=[parent]), \
                        patch.object(engine.subprocess, "Popen", return_value=process) as popen, \
                        patch.object(module, "validate", return_value={"passed": True}) as validator, \
                        patch("sys.stdout", new=io.StringIO()), patch.dict(os.environ, environment, clear=True):
                    report = engine.run_profile([aggregate, *files], plan, "recovery", "quiescence_peer", None,
                                                root / "output", root, tests=tests)
                self.assertTrue(report["passed"])
                self.assertEqual(report["network"], network)
                self.assertEqual(popen.call_args.kwargs["env"]["CEPH_TEST_NETWORK"], network)
                validator.assert_called_once_with(unittest.mock.ANY, "process-quiescence-" + network + "-peer")
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {"CEPH_TEST_NETWORK": "none"}), \
                self.assertRaisesRegex(ValueError, "network mode"):
            engine.run_profile([aggregate, *files], plan, "recovery", "quiescence_peer", None,
                               Path(temporary) / "output", Path(temporary))

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
                    patch.dict(os.environ, {"CEPH_TEST_RBD_CLIENT_IMAGE": "override", "CEPH_TEST_VAULT_IMAGE": "vault-custom",
                                            "GOFLAGS": "-overlay=/unrelated/source.json", "GOWORK": "/unrelated/go.work",
                                            "GOENV": "/custom/goenv", "GOCACHE": "/custom/cache", "GOPROXY": "off",
                                            "DOCKER_HOST": "unix:///custom/docker.sock", "TESTCONTAINERS_HOST_OVERRIDE": "localhost"}):
                report = engine.run_profile([file], plan, "short", "basic", None, output, root)
            self.assertTrue(report["passed"])
            command = popen.call_args.args[0]
            self.assertFalse(any(argument in ("-run", "-short", "-failfast") for argument in command))
            self.assertIn("-mod=readonly", command)
            self.assertIn("-count=1", command)
            self.assertIn("-timeout=20m", command)
            # The retained artifact flags precede the package argument.
            self.assertEqual(command[-3:], ["-artifacts", "-outputdir=" + str((output / engine.TEST_ARTIFACTS).resolve()),
                                            "./internal/integration"])
            self.assertTrue((output / engine.TEST_ARTIFACTS).is_dir())
            self.assertNotIn("CEPH_TEST_RBD_CLIENT_IMAGE", popen.call_args.kwargs["env"])
            self.assertEqual(popen.call_args.kwargs["env"]["CEPH_TEST_VAULT_IMAGE"], "vault-custom")
            for key, value in engine.GO_EXECUTION_ENV.items():
                self.assertEqual(popen.call_args.kwargs["env"][key], value)
            for key, value in {"GOENV": "/custom/goenv", "GOCACHE": "/custom/cache", "GOPROXY": "off",
                               "DOCKER_HOST": "unix:///custom/docker.sock", "TESTCONTAINERS_HOST_OVERRIDE": "localhost"}.items():
                self.assertEqual(popen.call_args.kwargs["env"][key], value)
            self.assertEqual(report["go_environment"], {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"})
            self.assertTrue((output / "native.log").is_file())
            self.assertTrue((output / "report.json").is_file())

    def test_test_artifacts_lists_retained_files_without_following_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / "_artifacts" / "internal/integration" / "TestX" / "123" / "ceph-abc-0011"
            archive.mkdir(parents=True)
            (archive / "manifest.json").write_text("{}\n")
            (archive / "diagnostics.json").write_text("{}\n")
            (archive / "outside").symlink_to("/")
            listing = engine.test_artifacts(directory)
        self.assertEqual(listing["directory"], engine.TEST_ARTIFACTS)
        self.assertEqual([file["path"] for file in listing["files"]], [
            "_artifacts/internal/integration/TestX/123/ceph-abc-0011/diagnostics.json",
            "_artifacts/internal/integration/TestX/123/ceph-abc-0011/manifest.json"])
        self.assertEqual(listing["manifests"], 1)
        self.assertEqual(listing["files"][0]["bytes"], 3)

    def test_compile_phase_does_not_execute_any_test(self):
        plan = engine.discover([fixture()])
        outcome = {"exit_code": 0, "timed_out": False, "timeout_seconds": 180, "elapsed_seconds": 1.0}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "bounded_go", return_value=(outcome, "")) as compiler, \
                patch.object(engine, "binary_receipt", return_value={"binary": "receipt"}) as receipt:
            report = engine.compile_profile(plan, "short", "basic", None, Path(temporary) / "compile")
        self.assertTrue(report["passed"])
        self.assertEqual(report["binary"], {"binary": "receipt"})
        self.assertEqual(receipt.call_args.args[1].name, "internal-integration.test")
        self.assertEqual(report["go_environment"], {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"})
        command = compiler.call_args.args[0]
        self.assertIn("-c", command)
        self.assertFalse(any(argument.startswith(("-run", "-short", "-list")) for argument in command))
        self.assertEqual(compiler.call_args.args[2], engine.COMPILE_TIMEOUT_SECONDS)


def binary_log(names=("TestNewFeature",)):
    """What the compiled test binary itself prints: PASS, but no ok line."""
    return native_log(names).rsplit("ok\t", 1)[0]


BUILD_KEY = {"package": "./internal/integration", "go_version": "go1.26.2", "goos": "linux", "goarch": "amd64"}
SOURCES = {"go.mod": "b" * 64, "internal/integration/new_test.go": "c" * 64}


class BinaryReuseTests(unittest.TestCase):
    """run --binary executes the compile phase's binary, or nothing at all."""

    def compiled(self, root, profile, tests, changes, content=b"binary"):
        """A compile directory as compile_profile leaves it."""
        directory = root / "compile"
        directory.mkdir()
        name = engine.binary_name(profile["package"])
        (directory / name).write_bytes(content)
        receipt = {"binary": name, "sha256": engine.hashlib.sha256(content).hexdigest(),
                   "build_key": dict(BUILD_KEY), "sources": dict(SOURCES), "tests": sorted(tests)}
        report = {"schema": engine.SCHEMA, "profile": profile, "passed": True, "classification": "compiled",
                  "binary": receipt}
        for key, value in changes.items():
            (receipt if key in receipt else report)[key] = value
        (directory / "compile-report.json").write_text(json.dumps(report))
        return directory

    def run_binary(self, file, compile_changes=None, tests="", log=None, inventory=None, key=None, sources=None,
                   content=b"binary", environment=None):
        plan = engine.discover([file])
        commands = []

        class Child:
            def __init__(self, command, **kwargs):
                commands.append((command, kwargs))
                self.stdout = io.StringIO(log if log is not None else binary_log(tuple(file["tests"])))

            def wait(self):
                return 0
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "go.mod").write_text("module example\n")
            directory = self.compiled(root, plan["include"][0], file["tests"], compile_changes or {})
            if content != b"binary":
                (directory / engine.binary_name(plan["include"][0]["package"])).write_bytes(content)
            output = root / "native"
            with patch.object(engine, "build_key", return_value=dict(key or BUILD_KEY)), \
                    patch.object(engine, "build_sources", return_value=dict(sources or SOURCES)), \
                    patch.object(engine, "binary_inventory",
                                 return_value=sorted(inventory if inventory is not None else file["tests"])), \
                    patch.object(engine, "compile_inventory") as compiled, \
                    patch.object(engine.subprocess, "Popen", Child), patch("sys.stdout", new=io.StringIO()), \
                    patch.dict(os.environ, environment or {}):
                try:
                    report = engine.run_profile([file], plan, "short", "basic", None, output, root, tests=tests,
                                                binary=directory)
                finally:
                    self.created = output.exists()
            compiled.assert_not_called()
        return report, commands, directory, root, self.created

    def test_reuse_runs_the_verified_binary_from_its_package_directory(self):
        report, commands, directory, root, _ = self.run_binary(
            fixture(), environment={"CEPH_TEST_RBD_CLIENT_IMAGE": "override", "GOFLAGS": "-overlay=/x.json"})
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["execution"], "test-binary")
        self.assertEqual(report["completion"]["execution"], "test-binary")
        command, options = commands[0]
        binary = str((directory / "internal-integration.test").resolve())
        artifacts = str((root / "native" / engine.TEST_ARTIFACTS).resolve())
        self.assertEqual(command, [binary, "-test.v", "-test.count=1", "-test.timeout=20m",
                                   "-test.artifacts", "-test.outputdir=" + artifacts])
        self.assertEqual(report["test_artifacts"], {"directory": engine.TEST_ARTIFACTS, "files": [], "manifests": 0})
        self.assertEqual(options["cwd"], root / "internal/integration")
        self.assertNotIn("CEPH_TEST_RBD_CLIENT_IMAGE", options["env"])
        for name, value in engine.GO_EXECUTION_ENV.items():
            self.assertEqual(options["env"][name], value)
        self.assertEqual(report["binary"]["sha256"], engine.hashlib.sha256(b"binary").hexdigest())
        self.assertEqual(report["binary"]["build_key"], BUILD_KEY)

    def test_network_entry_selects_its_tests_with_the_binary_run_flag(self):
        file = fixture(tests=("TestNetworked", "TestModeless"))
        file["networks"], file["network_tests"] = ["host", "bridge"], ["TestNetworked"]
        report, commands, *_ = self.run_binary(file, tests="TestNetworked", log=binary_log(("TestNetworked",)),
                                               environment={"CEPH_TEST_NETWORK": "bridge"})
        self.assertTrue(report["passed"], report)
        command, options = commands[0]
        self.assertEqual(command[1:4], ["-test.v", "-test.count=1", "-test.timeout=20m"])
        self.assertEqual(command[4], "-test.artifacts")
        self.assertTrue(command[5].startswith("-test.outputdir=/"), command)
        self.assertEqual(command[6:], ["-test.run=^(TestNetworked)$"])
        self.assertEqual(options["env"]["CEPH_TEST_NETWORK"], "bridge")

    def test_each_receipt_mismatch_fails_closed_before_running_anything(self):
        cases = {
            "sha256 differs": {"content": b"tampered"},
            "build key differs": {"key": BUILD_KEY | {"goarch": "arm64"}},
            "different source files": {"sources": SOURCES | {"go.mod": "d" * 64}},
            "recorded inventory differs": {"compile_changes": {"tests": ["TestOther"]}},
            "own inventory differs": {"inventory": ["TestNewFeature", "TestInjected"]},
            "another profile": {"compile_changes": {"profile": {"id": "short-other-00000000", "tags": "x"}}},
            "did not record a successful binary": {"compile_changes": {"passed": False}},
            "unexpected binary": {"compile_changes": {"binary": "../other.test"}},
        }
        for message, arguments in cases.items():
            with self.subTest(message), patch.object(engine.subprocess, "Popen") as popen:
                with self.assertRaisesRegex(ValueError, message):
                    self.run_binary(fixture(), **arguments)
                popen.assert_not_called()
                self.assertFalse(self.created)

    def test_missing_receipt_fails_closed_and_never_recompiles(self):
        file = fixture()
        plan = engine.discover([file])
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine.subprocess, "Popen") as popen, \
                patch.object(engine, "compile_inventory") as compiled, \
                patch.object(engine, "bounded_go") as compiler:
            output = Path(temporary) / "native"
            with self.assertRaisesRegex(ValueError, "receipt is missing"):
                engine.run_profile([file], plan, "short", "basic", None, output, Path(temporary),
                                   binary=Path(temporary) / "compile")
            self.assertFalse(output.exists())
        popen.assert_not_called()
        compiled.assert_not_called()
        compiler.assert_not_called()

    def test_failed_verification_allocates_no_native_directory(self):
        with self.assertRaises(ValueError):
            self.run_binary(fixture(), content=b"tampered")
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "load_catalog", return_value=[fixture()]), \
                patch.object(engine.subprocess, "Popen") as popen, patch("sys.stderr", new=io.StringIO()):
            output = Path(temporary) / "native"
            code = engine.main(["run", "--category", "short", "--batch", "basic", "--directory", str(output),
                                "--binary", str(Path(temporary) / "compile")])
            self.assertEqual(code, 1)
            self.assertFalse(output.exists())
        popen.assert_not_called()

    def test_without_binary_run_and_optional_keep_go_test(self):
        optional = fixture(path="internal/integration/optional_test.go", category="optional", batch="native_rgw_translation")
        optional["constraint"] = combine("and", optional["constraint"], tag("native_regression"))
        optional["tags"].append("native_regression")
        for arguments, binary in ((["run", "--category", "short", "--batch", "basic"], None),
                                  (["run", "--category", "short", "--batch", "basic", "--binary", "c"], Path("c")),
                                  (["optional", "--batch", "native_rgw_translation"], None),
                                  (["optional", "--batch", "native_rgw_translation", "--binary", "c"], Path("c"))):
            with self.subTest(arguments=arguments), \
                    patch.object(engine, "load_catalog", return_value=[fixture(), optional]), \
                    patch.object(engine, "run_profile", return_value={"passed": True}) as runner, \
                    patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(engine.main([*arguments, "--directory", "native"]), 0)
                self.assertEqual(runner.call_args.kwargs["binary"], binary)
        process = unittest.mock.Mock()
        process.stdout = io.StringIO(native_log())
        process.wait.return_value = 0
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "compile_inventory", return_value=["TestNewFeature"]), \
                patch.object(engine, "verified_binary") as verifier, \
                patch.object(engine.subprocess, "Popen", return_value=process) as popen, \
                patch("sys.stdout", new=io.StringIO()):
            root = Path(temporary)
            (root / "go.mod").write_text("module example\n")
            report = engine.run_profile([fixture()], engine.discover([fixture()]), "short", "basic", None,
                                        root / "native", root)
        verifier.assert_not_called()
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["execution"], "go-test")
        self.assertNotIn("binary", report)
        self.assertEqual(popen.call_args.args[0][:2], ["go", "test"])
        self.assertEqual(popen.call_args.kwargs["cwd"], root)

    def test_binary_completion_needs_its_own_final_pass_and_exit_status(self):
        package = "example/internal/integration"
        good = binary_log(("TestNewFeature", "TestNewFeature/leaf"))
        self.assertTrue(engine.completion(good, ["TestNewFeature"], package, "test-binary")["passed"])
        # Each format rejects the other's package completion.
        self.assertFalse(engine.completion(good, ["TestNewFeature"], package)["passed"])
        self.assertFalse(engine.completion(native_log(), ["TestNewFeature"], package, "test-binary")["passed"])
        for invalid in (good.replace("PASS\n", ""),
                        good + "PASS\n",
                        good + "=== RUN   TestNewFeature\n",
                        good + "late output\n",
                        good.replace("--- PASS: TestNewFeature/leaf (1.00s)\n", ""),
                        good.replace("--- PASS: TestNewFeature/leaf", "--- SKIP: TestNewFeature/leaf"),
                        "PASS\n"):
            with self.subTest(log=invalid):
                self.assertFalse(engine.completion(invalid, ["TestNewFeature"], package, "test-binary")["passed"])
        with self.assertRaisesRegex(ValueError, "execution format"):
            engine.completion(good, ["TestNewFeature"], package, "guess")
        report = engine.completion(good, ["TestNewFeature"], package, "test-binary")
        self.assertEqual(engine.failure_kind(1, report, None), "process-or-build-failure")
        self.assertEqual(engine.failure_kind(0, report, None), "passed")

    def test_strict_checkers_accept_only_the_declared_execution_format(self):
        import check_scenario_quiescence as quiescence
        import check_scenario_receivers as receivers
        import check_scenario_recovery as recovery
        import test_scenario_quiescence
        import test_scenario_receivers
        import test_scenario_recovery
        cases = ((quiescence, test_scenario_quiescence.native_leaf_log, sorted(quiescence.CASES)[0]),
                 (recovery, test_scenario_recovery.native_leaf_log, sorted(recovery.CASES)[0]),
                 (receivers, test_scenario_receivers.successful_log, receivers.NETWORKS[0]))
        for module, record, case in cases:
            with self.subTest(checker=module.__name__):
                driver = record(case)
                self.assertTrue(module.validate(driver, case)["passed"])
                self.assertFalse(module.validate(driver, case, execution="test-binary")["passed"])
                lines = driver.rstrip("\n").splitlines()
                self.assertRegex(lines[-1], r"^ok\s")
                direct = "\n".join(lines[:-1]) + "\n"
                self.assertTrue(module.validate(direct, case, execution="test-binary")["passed"])
                self.assertFalse(module.validate(direct, case)["passed"])
                self.assertFalse(module.validate(direct + "trailing\n", case, execution="test-binary")["passed"])
                self.assertFalse(module.validate(direct.replace("\nPASS\n", "\n"), case, execution="test-binary")["passed"])
                with self.assertRaisesRegex(ValueError, "execution format"):
                    module.validate(direct, case, execution="guess")
        profile = {"checker": "receivers", "case": ""}
        with patch.object(receivers, "validate", return_value={"passed": True}) as validator:
            engine.strict_completion("log", profile, "host", "test-binary")
        validator.assert_called_once_with("log", "host", execution="test-binary")


# A go command that writes to both streams, leaves a child process that keeps
# running in its process group, and then never finishes on its own. The child
# records its PID so that a test can check it is gone.
STALLED_GO = """#!/bin/sh
echo "go: downloading example.com/module v1.0.0"
echo "stalled on purpose" >&2
sleep 300 &
echo $! > "$STALLED_GO_CHILD"
sleep 300
"""

# A go command that fails at once with a compiler error.
FAILING_GO = """#!/bin/sh
echo "./broken_test.go:3:2: undefined: missingHelper" >&2
exit 1
"""


class BoundedPreparationTests(unittest.TestCase):
    """Run the preparation phases against stand-in go executables."""

    def run_with(self, script, phase):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "bin").mkdir()
            go = root / "bin" / "go"
            go.write_text(script)
            go.chmod(0o755)
            child = root / "child.pid"
            environment = {"PATH": str(root / "bin") + os.pathsep + os.environ["PATH"], "STALLED_GO_CHILD": str(child)}
            with patch.dict(os.environ, environment), \
                    patch.object(engine, "COMPILE_TIMEOUT_SECONDS", 2), patch.object(engine, "MODULES_TIMEOUT_SECONDS", 2):
                report = phase(root / "receipt")
            files = {path.name: path.read_text() for path in (root / "receipt").iterdir()}
            pid = int(child.read_text()) if child.exists() else None
        return report, files, pid

    def assertGone(self, pid):
        """pid no longer runs. A killed grandchild of the go command stays a
        zombie until init reaps it, and kill(pid, 0) still succeeds on a
        zombie, so its state decides."""
        self.assertIsNotNone(pid)
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        if state and not state.startswith("Z"):
            self.fail("a process of the timed-out go command is still running (state " + state + ")")

    def test_a_zombie_counts_as_gone(self):
        # An exited child that nobody waited for yet is a zombie.
        child = subprocess.Popen(["true"])
        try:
            os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
            os.kill(child.pid, 0)
            self.assertGone(child.pid)
        finally:
            child.wait()
        running = subprocess.Popen(["sleep", "30"])
        try:
            with self.assertRaisesRegex(AssertionError, "still running"):
                self.assertGone(running.pid)
        finally:
            running.kill()
            running.wait()

    def compile(self, directory):
        return engine.compile_profile(engine.discover([fixture()]), "short", "basic", None, directory)

    def test_compile_timeout_keeps_partial_output_and_a_failed_receipt(self):
        report, files, pid = self.run_with(STALLED_GO, self.compile)
        self.assertFalse(report["passed"])
        self.assertEqual(report["classification"], "compile-timeout")
        self.assertTrue(report["timed_out"])
        self.assertIsNone(report["exit_code"])
        self.assertEqual(report["timeout_seconds"], 2)
        self.assertGreaterEqual(report["elapsed_seconds"], 2)
        self.assertEqual(json.loads(files["compile-report.json"]), report)
        self.assertIn("go: downloading example.com/module v1.0.0", files["compile.log"])
        self.assertIn("stalled on purpose", files["compile.log"])
        self.assertNotIn("internal-integration.test", files)
        self.assertGone(pid)

    def test_compile_failure_keeps_the_compiler_error(self):
        report, files, _ = self.run_with(FAILING_GO, self.compile)
        self.assertEqual((report["classification"], report["exit_code"], report["timed_out"]), ("compile-failure", 1, False))
        self.assertIn("undefined: missingHelper", files["compile.log"])

    def test_module_preparation_timeout_keeps_partial_output_and_a_failed_receipt(self):
        report, files, pid = self.run_with(STALLED_GO, engine.prepare_modules)
        self.assertEqual((report["passed"], report["classification"], report["exit_code"]), (False, "modules-timeout", None))
        self.assertEqual(report["command"], ["go", "mod", "download"])
        self.assertEqual(json.loads(files["modules-report.json"]), report)
        self.assertIn("go: downloading example.com/module v1.0.0", files["modules.log"])
        self.assertGone(pid)

    def test_compile_cli_exits_nonzero_after_a_timeout(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "load_catalog", return_value=[fixture()]), \
                patch.object(engine, "bounded_go", return_value=({"exit_code": None, "timed_out": True, "timeout_seconds": 180,
                                                                  "elapsed_seconds": 180.0}, "partial")), \
                patch("sys.stdout", new_callable=io.StringIO):
            directory = Path(temporary) / "compile"
            code = engine.main(["compile", "--category", "short", "--batch", "basic", "--directory", str(directory)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads((directory / "compile-report.json").read_text())["classification"], "compile-timeout")

    def build_cache_catalog(self):
        return [fixture(), fixture("internal/integration/other_test.go", ("TestOther",), batch="other"),
                fixture("internal/dockerbridge/bridge_test.go", ("TestBridge",), category="environment", batch="bridge"),
                fixture("internal/integration/pure_test.go", ("TestPure",), category="code", batch="pure")]

    def test_build_cache_compiles_one_runtime_profile_per_package_and_keeps_no_binary(self):
        plan = engine.discover(self.build_cache_catalog())
        outcome = {"exit_code": 0, "timed_out": False, "timeout_seconds": 180, "elapsed_seconds": 1.0}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "bounded_go", return_value=(outcome, "built")) as compiler:
            directory = Path(temporary) / "buildcache"
            report = engine.warm_build_cache(plan, directory)
            self.assertEqual(sorted(path.name for path in directory.iterdir()),
                             ["buildcache-report.json", "internal-dockerbridge.log", "internal-integration.log"])
        self.assertTrue(report["passed"])
        commands = [call.args[0] for call in compiler.call_args_list]
        self.assertEqual([command[-1] for command in commands], ["./internal/dockerbridge", "./internal/integration"])
        for command in commands:
            self.assertEqual(command[:4], ["go", "test", "-mod=readonly", command[3]])
            self.assertIn("-c", command)
            self.assertFalse(any(argument.startswith(("-run", "-list", "-short")) for argument in command))
        # A runtime profile's tags, never the code profile's.
        self.assertIn("ci_short", commands[1][3])
        self.assertTrue(all(call.args[2] == engine.COMPILE_TIMEOUT_SECONDS for call in compiler.call_args_list))

    def test_build_cache_failure_is_reported_and_the_cli_exits_nonzero(self):
        failed = {"exit_code": 1, "timed_out": False, "timeout_seconds": 180, "elapsed_seconds": 1.0}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(engine, "load_catalog", return_value=self.build_cache_catalog()), \
                patch.object(engine, "bounded_go", return_value=(failed, "compile error")), \
                patch("sys.stdout", new_callable=io.StringIO):
            directory = Path(temporary) / "buildcache"
            self.assertEqual(engine.main(["buildcache", "--directory", str(directory)]), 1)
            report = json.loads((directory / "buildcache-report.json").read_text())
            self.assertFalse(report["passed"])
            self.assertIn("compile error", (directory / "internal-integration.log").read_text())


class GoSourceEnvironmentTests(unittest.TestCase):
    """Execute real, offline Go controls to prove injected sources are ignored."""

    def source(self, root, body='t.Fatal("original assertion executed")', imports='"testing"'):
        root.mkdir()
        (root / "go.mod").write_text("module example\ngo 1.25\n")
        path = root / "fixture_test.go"
        path.write_text('''//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_basic))))
//ci: timeout=20s job-timeout=1

package fixture
import (''' + imports + ''')
func TestOriginal(t *testing.T) { ''' + body + " }\n")
        return path

    def offline_environment(self):
        return os.environ | {"CGO_ENABLED": "0", "GOENV": "off", "GOWORK": "off", "GOFLAGS": "",
                             "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local"}

    def control(self, root, environment, *arguments):
        return subprocess.run(["go", *arguments], cwd=root, env=environment, text=True,
                              capture_output=True, timeout=60)

    def overlay(self, directory, original, replacement, name="overlay"):
        path = directory / (name + ".json")
        path.write_text(json.dumps({"Replace": {str(original.resolve()): str(replacement.resolve())}}))
        return "-overlay=" + str(path)

    def test_inherited_and_persisted_overlays_cannot_replace_native_assertions(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            root = directory / "source"
            source = self.source(root)
            replacement = directory / "replacement.go"
            replacement.write_text(source.read_text().replace('t.Fatal("original assertion executed")', ""))
            overlay = self.overlay(directory, source, replacement)
            environment = self.offline_environment()
            command = ["test", "-mod=readonly", "-tags=all", "-count=1", "-v", "-timeout=20s", "."]
            original = self.control(root, environment, *command)
            self.assertNotEqual(original.returncode, 0, original.stdout + original.stderr)
            self.assertIn("original assertion executed", original.stdout)
            persisted = directory / "goenv"
            persisted.write_text("GOFLAGS=" + overlay + "\n")
            for kind in ("inherited", "persisted"):
                with self.subTest(kind=kind):
                    injected = environment | {"GOFLAGS": overlay}
                    if kind == "persisted":
                        injected.pop("GOFLAGS")
                        injected["GOENV"] = str(persisted)
                    rewritten = self.control(root, injected, *command)
                    self.assertEqual(rewritten.returncode, 0, rewritten.stdout + rewritten.stderr)
                    self.assertNotIn("original assertion executed", rewritten.stdout)
                    with patch.dict(os.environ, injected, clear=True), patch("sys.stdout", new=io.StringIO()):
                        catalog = engine.load_catalog(root)
                        plan = engine.discover(catalog)
                        report = engine.run_profile(catalog, plan, "short", "basic", ".",
                                                    directory / (kind + "-native"), root)
                    self.assertFalse(report["passed"])
                    self.assertEqual(report["classification"], "native-test-failure")
                    self.assertEqual(report["compiled_tests"], ["TestOriginal"])
                    self.assertIn("TestOriginal", report["completion"]["failed_tests"])
                    self.assertIn("original assertion executed", (directory / (kind + "-native") / "native.log").read_text())
                    self.assertEqual(report["go_environment"], {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"})
                    self.assertNotIn("GOENV", report["go_environment"])

    def test_catalog_and_compiled_inventory_ignore_source_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            root = directory / "source"
            source = self.source(root)
            environment = self.offline_environment()
            replacement = directory / "catalog.go"
            replacement.write_text('package main\nimport "fmt"\nfunc main() { fmt.Println("[]") }\n')
            tool = engine.ROOT / "internal/tools/tagcatalog/main.go"
            injected = environment | {"GOFLAGS": self.overlay(directory, tool, replacement, "catalog-overlay")}
            rewritten = self.control(root, injected, "run", "-mod=readonly", str(tool), str(root))
            self.assertEqual(rewritten.returncode, 0, rewritten.stdout + rewritten.stderr)
            self.assertEqual(json.loads(rewritten.stdout), [])
            with patch.dict(os.environ, injected, clear=True):
                catalog = engine.load_catalog(root)
            self.assertEqual(catalog[0]["tests"], ["TestOriginal"])
            replacement = directory / "inventory.go"
            replacement.write_text(source.read_text().replace("TestOriginal", "TestInjected"))
            injected["GOFLAGS"] = self.overlay(directory, source, replacement, "inventory-overlay")
            rewritten = self.control(root, injected, "test", "-mod=readonly", "-tags=all", "-list=.", ".")
            self.assertEqual(rewritten.returncode, 0, rewritten.stdout + rewritten.stderr)
            self.assertIn("TestInjected\n", rewritten.stdout)
            with patch.dict(os.environ, injected, clear=True):
                self.assertEqual(engine.compile_inventory(".", "all", root), ["TestOriginal"])

    def test_compile_artifact_contains_original_assertion_despite_overlay(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            root = directory / "source"
            source = self.source(root)
            replacement = directory / "replacement.go"
            replacement.write_text(source.read_text().replace('t.Fatal("original assertion executed")', ""))
            injected = self.offline_environment() | {"GOFLAGS": self.overlay(directory, source, replacement)}
            with patch.dict(os.environ, injected, clear=True):
                plan = engine.discover(engine.load_catalog(root))
                compiled = engine.compile_profile(plan, "short", "basic", ".", directory / "compile", root)
            self.assertTrue(compiled["passed"])
            self.assertEqual(compiled["go_environment"], {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"})
            actual = subprocess.run([str(directory / "compile/root.test"), "-test.v", "-test.timeout=20s"],
                                    text=True, capture_output=True, timeout=30)
            self.assertNotEqual(actual.returncode, 0, actual.stdout + actual.stderr)
            self.assertIn("original assertion executed", actual.stdout)

    def test_run_reuses_the_compiled_binary_and_rejects_a_changed_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            root = directory / "source"
            source = self.source(root, body='t.Log("original assertion executed")')
            with patch.dict(os.environ, self.offline_environment(), clear=True), patch("sys.stdout", new=io.StringIO()):
                catalog = engine.load_catalog(root)
                plan = engine.discover(catalog)
                compiled = engine.compile_profile(plan, "short", "basic", ".", directory / "compile", root)
                self.assertTrue(compiled["passed"], compiled)
                self.assertEqual(compiled["binary"]["tests"], ["TestOriginal"])
                self.assertEqual(set(compiled["binary"]["sources"]), {"go.mod", "fixture_test.go"})
                report = engine.run_profile(catalog, plan, "short", "basic", ".", directory / "native", root,
                                            binary=directory / "compile")
                self.assertTrue(report["passed"], report)
                self.assertEqual(report["execution"], "test-binary")
                self.assertEqual(report["command"][0], str(directory / "compile/root.test"))
                log = (directory / "native/native.log").read_text()
                self.assertIn("original assertion executed", log)
                self.assertNotRegex(log, r"(?m)^ok\s")
                source.write_text(source.read_text().replace("original assertion", "edited assertion"))
                with self.assertRaisesRegex(ValueError, "different source files"):
                    engine.run_profile(catalog, plan, "short", "basic", ".", directory / "edited", root,
                                       binary=directory / "compile")
                self.assertFalse((directory / "edited").exists())

    def test_external_workspace_cannot_replace_local_dependency(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            root = directory / "source"
            self.source(root, 'if !value.Valid() { t.Fatal("original dependency executed") }',
                        '"testing"; "fixture.invalid/value"')
            for name, value in (("original", "false"), ("replacement", "true")):
                dependency = directory / name
                dependency.mkdir()
                (dependency / "go.mod").write_text("module fixture.invalid/value\ngo 1.25\n")
                (dependency / "value.go").write_text("package value\nfunc Valid() bool { return " + value + " }\n")
            with (root / "go.mod").open("a") as module:
                module.write("require fixture.invalid/value v0.0.0\nreplace fixture.invalid/value => ../original\n")
            workspace = directory / "go.work"
            workspace.write_text("go 1.25\nuse ./source\nreplace fixture.invalid/value => ./replacement\n")
            injected = self.offline_environment() | {"GOWORK": str(workspace)}
            rewritten = self.control(root, injected, "test", "-tags=all", "-count=1", "-v", "-timeout=20s", ".")
            self.assertEqual(rewritten.returncode, 0, rewritten.stdout + rewritten.stderr)
            with patch.dict(os.environ, injected, clear=True), patch("sys.stdout", new=io.StringIO()):
                catalog = engine.load_catalog(root)
                plan = engine.discover(catalog)
                report = engine.run_profile(catalog, plan, "short", "basic", ".", directory / "native", root)
            self.assertFalse(report["passed"])
            self.assertEqual(report["classification"], "native-test-failure")
            self.assertIn("original dependency executed", (directory / "native/native.log").read_text())


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


class VersionedRoleDeclarationTests(unittest.TestCase):
    def test_v2_roles_are_explicit_and_mixed_contracts_are_refused(self):
        declared = fixture()
        declared["metadata"] = ["timeout=20m job-timeout=35 image-roles=client,essential"]
        plan = engine.discover([declared])
        self.assertEqual(plan["include"][0]["image_roles"],"essential,client")
        declared["metadata"] = ["timeout=20m job-timeout=35 image-roles=essential,control"]
        with self.assertRaisesRegex(ValueError,"image-roles must list"):
            engine.discover([declared])


# Synthetic bug IDs: test inputs only, never registered in the catalogue.
BUG_A, BUG_B = "ceph_bug_synthetic_a", "ceph_bug_synthetic_b"


def bug_registry(supported=(BUG_A, BUG_B), retired=(), conflicts=()):
    entries = [{"tag": name, "tracker": "https://example.invalid/" + name, "status": "supported",
                "sources": ["rbd/" + name + ".go"]} for name in supported]
    entries += [{"tag": name, "tracker": "https://example.invalid/" + name, "status": "retired", "sources": []}
                for name in retired]
    return engine.bug_registry({"schema": engine.BUG_REGISTRY_SCHEMA, "tags": entries,
                                "conflicts": [list(pair) for pair in conflicts]})


def bug_branches(name=BUG_A, tests=("TestNewFeature",), batch="basic", stem="new", category="short"):
    """An ordinary test file and its compatibility path for one bug."""
    ordinary = fixture(path="internal/integration/" + stem + "_test.go", tests=tests, category=category, batch=batch)
    compatibility = fixture(path="internal/integration/" + stem + "_" + name + "_test.go", tests=tests,
                            category=category, batch=batch)
    ordinary["constraint"] = combine("and", ordinary["constraint"], negate(tag(name)))
    compatibility["constraint"] = combine("and", compatibility["constraint"], tag(name))
    for file in (ordinary, compatibility):
        file["tags"].append(name)
    return [ordinary, compatibility]


class BugCompatibilitySelectionTests(unittest.TestCase):
    def test_bug_tags_in_source_are_never_promoted_to_capabilities(self):
        catalog = bug_branches() + [fixture(path="internal/integration/unrelated_test.go",
                                            tests=("TestUnrelated",), batch="unrelated")]
        bugs = engine.bug_selection(bug_registry(), "")
        self.assertNotIn(BUG_A, engine.capability_tags(catalog))
        plan = engine.discover(catalog, bugs)
        for profile in plan["include"]:
            self.assertNotIn(BUG_A, profile["tags"].split(","))
            self.assertEqual(profile["bug_compatibility"], [])
        self.assertEqual(plan["bug_compatibility"], [])
        self.assertNotIn(BUG_A, plan["capability_tags"])

    def test_no_selection_compiles_the_ordinary_path_and_lists_both_sources(self):
        plan = engine.discover(bug_branches(), engine.bug_selection(bug_registry(), ""))
        profile = plan["include"][0]
        self.assertEqual(plan["required_executions"], 1)
        self.assertEqual(plan["required_parents"]["./internal/integration"], ["TestNewFeature"])
        self.assertEqual(profile["sources"], ["internal/integration/new_ceph_bug_synthetic_a_test.go",
                                              "internal/integration/new_test.go"])
        catalog = bug_branches()
        compiled = [file["path"] for file in catalog
                    if engine.evaluate(file["constraint"], set(profile["tags"].split(",")))]
        self.assertEqual(compiled, ["internal/integration/new_test.go"])

    def test_selection_compiles_only_the_selected_compatibility_paths(self):
        catalog = bug_branches() + bug_branches(BUG_B, tests=("TestOther",), batch="other", stem="other")
        for selection, expected in (("", set()), (BUG_A, {BUG_A}), (BUG_B, {BUG_B}),
                                    (BUG_B + "," + BUG_A, {BUG_A, BUG_B})):
            with self.subTest(selection=selection):
                plan = engine.discover(catalog, engine.bug_selection(bug_registry(), selection))
                self.assertEqual(plan["bug_compatibility"], sorted(expected))
                self.assertEqual(plan["required_executions"], 2)
                for profile in plan["include"]:
                    tags = set(profile["tags"].split(","))
                    self.assertEqual(tags & {BUG_A, BUG_B}, expected)
                    compiled = sorted(file["path"] for file in catalog if engine.evaluate(file["constraint"], tags))
                    paths = {"basic": ("new", BUG_A), "other": ("other", BUG_B)}[profile["batch"]]
                    suffix = "_" + paths[1] if paths[1] in expected else ""
                    self.assertEqual(compiled, ["internal/integration/" + paths[0] + suffix + "_test.go"])

    def test_invalid_selection_is_refused_before_discovery(self):
        registry = bug_registry(retired=("ceph_bug_synthetic_old",), conflicts=((BUG_A, BUG_B),))
        cases = {"ceph_bug_synthetic_typo": "unregistered", "integration": "not a bug compatibility tag",
                 BUG_A + "," + BUG_A: "twice", "ceph_bug_synthetic_old": "retired",
                 BUG_A + "," + BUG_B: "conflicting"}
        for selection, message in cases.items():
            with self.subTest(selection=selection), self.assertRaisesRegex(ValueError, message):
                engine.bug_selection(registry, selection)
        self.assertEqual(engine.bug_selection(registry, " , ")["selected"], ())
        self.assertEqual(engine.bug_selection(registry, BUG_A)["selected"], (BUG_A,))

    def test_cli_refuses_an_invalid_selection_before_reading_sources(self):
        with patch.object(engine, "load_catalog") as catalog, patch("sys.stderr", new=io.StringIO()) as error:
            self.assertEqual(engine.main(["plan", "--bug-compatibility", "ceph_bug_synthetic_typo"]), 1)
        catalog.assert_not_called()
        self.assertIn("unregistered bug compatibility tag", error.getvalue())

    def test_unregistered_or_retired_bug_tag_in_source_is_refused(self):
        with self.assertRaisesRegex(ValueError, "unregistered bug compatibility tag"):
            engine.discover(bug_branches("ceph_bug_synthetic_typo"), engine.bug_selection(bug_registry(), ""))
        with self.assertRaisesRegex(ValueError, "unregistered bug compatibility tag"):
            engine.discover(bug_branches())
        retired = bug_registry(supported=(), retired=(BUG_A,))
        with self.assertRaisesRegex(ValueError, "retired bug compatibility tag still guards"):
            engine.discover(bug_branches(), engine.bug_selection(retired, ""))

    def test_bug_tag_cannot_select_tests(self):
        bugs = engine.bug_selection(bug_registry(), "")
        # The planner characterization in #24: a test enabled by all or the bug.
        selector = fixture()
        selector["tags"].append(BUG_A)
        selector["constraint"] = combine("and", selector["constraint"], combine("or", tag("all"), tag(BUG_A)))
        unrelated = fixture(path="internal/integration/unrelated_test.go", tests=("TestUnrelated",),
                            batch="unrelated")
        with self.assertRaisesRegex(ValueError, "changes the selected tests"):
            engine.discover([selector, unrelated], bugs)
        # Only one path declares the test, so one selection loses it.
        with self.assertRaisesRegex(ValueError, "changes the selected tests"):
            engine.discover([bug_branches()[1], unrelated], bugs)
        renamed = bug_branches()
        renamed[1]["tests"] = ["TestNewFeatureWorkaround"]
        with self.assertRaisesRegex(ValueError, "changes the selected tests"):
            engine.discover(renamed, bugs)

    def test_conflicting_alternatives_compare_only_valid_selections(self):
        ordinary, first = bug_branches(BUG_A)
        second = bug_branches(BUG_B)[1]
        ordinary["tags"].append(BUG_B)
        ordinary["constraint"] = combine("and", ordinary["constraint"], negate(tag(BUG_B)))
        catalog = [ordinary, first, second]
        registry = bug_registry(conflicts=((BUG_A, BUG_B),))
        for selection in ("", BUG_A, BUG_B):
            with self.subTest(selection=selection):
                plan = engine.discover(catalog, engine.bug_selection(registry, selection))
                self.assertEqual(plan["required_parents"]["./internal/integration"], ["TestNewFeature"])
                profile = plan["include"][0]
                self.assertEqual(engine.selected_tests(catalog, profile["package"], profile["tags"]),
                                 ["TestNewFeature"])
        with self.assertRaisesRegex(ValueError, "conflicting"):
            engine.bug_selection(registry, BUG_A + "," + BUG_B)
        # Without the declaration A+B is a reachable selection; it would compile
        # both alternates, so the planner must still compare it.
        with self.assertRaisesRegex(ValueError, "duplicate test declaration"):
            engine.discover(catalog, engine.bug_selection(bug_registry(), BUG_A))

    def test_compatibility_path_in_another_batch_is_refused(self):
        ordinary, compatibility = bug_branches()
        moved = bug_branches(batch="moved")[1]
        with self.assertRaisesRegex(ValueError, "changes the selected tests"):
            engine.discover([ordinary, moved], engine.bug_selection(bug_registry(), ""))
        self.assertEqual(engine.discover([ordinary, compatibility],
                                         engine.bug_selection(bug_registry(), BUG_A))["required_profiles"], 1)

    def test_optional_profile_carries_the_selection_and_keeps_its_contract(self):
        files = bug_branches(batch="native_bug", category="optional")
        for file in files:
            file["tags"].append("native_regression")
            file["constraint"] = combine("and", file["constraint"], tag("native_regression"))
        for selection in ("", BUG_A):
            with self.subTest(selection=selection):
                plan = engine.discover_optional(files, engine.bug_selection(bug_registry(), selection))
                self.assertEqual(plan["bug_compatibility"], [selection] if selection else [])
                self.assertEqual(BUG_A in plan["include"][0]["tags"].split(","), bool(selection))

    def test_verify_compiles_the_all_and_category_builds_with_the_selection(self):
        catalog = bug_branches()
        plan = engine.discover(catalog, engine.bug_selection(bug_registry(), BUG_A))
        requested = []

        def inventory(package, tags, root=None):
            requested.append(tags)
            return engine.selected_tests(catalog, package, tags)
        with patch.object(engine, "compile_inventory", side_effect=inventory):
            engine.verify(catalog, plan)
        self.assertEqual(requested[0], "all," + BUG_A)
        self.assertTrue(all(BUG_A in tags.split(",") for tags in requested))

    def test_binary_of_another_selection_fails_its_receipt_check(self):
        catalog = bug_branches()
        ordinary = engine.discover(catalog, engine.bug_selection(bug_registry(), ""))["include"][0]
        selected = engine.discover(catalog, engine.bug_selection(bug_registry(), BUG_A))["include"][0]
        self.assertEqual(ordinary["id"], selected["id"])
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            report = {"schema": engine.SCHEMA, "passed": True, "profile": ordinary,
                      "binary": {"binary": engine.binary_name(ordinary["package"])}}
            (directory / "compile-report.json").write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, "belongs to another profile"):
                engine.verified_binary(directory, selected, ["TestNewFeature"])


class BugCompatibilityMatrixEntryTests(unittest.TestCase):
    """A runtime job recomputes its profile with its own selection; a matrix
    entry of the Plan job's selection must have the same tags."""

    def plans(self):
        catalog = bug_branches()
        return (catalog, engine.discover(catalog, engine.bug_selection(bug_registry(), "")),
                engine.discover(catalog, engine.bug_selection(bug_registry(), BUG_A)))

    def test_compile_of_another_selection_fails_before_building(self):
        _, ordinary, selected = self.plans()
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine, "bounded_go") as compiler:
            directory = Path(temporary) / "compile"
            for plan, entry in ((ordinary, selected), (selected, ordinary)):
                with self.subTest(tags=entry["include"][0]["tags"]), \
                        self.assertRaisesRegex(ValueError, "selected different bug compatibility paths"):
                    engine.compile_profile(plan, "short", "basic", None, directory,
                                           profile_tags=entry["include"][0]["tags"])
            self.assertFalse(directory.exists())
        compiler.assert_not_called()

    def test_run_of_another_selection_fails_before_running(self):
        catalog, ordinary, selected = self.plans()
        with tempfile.TemporaryDirectory() as temporary, patch.object(engine.subprocess, "Popen") as popen, \
                patch.object(engine, "verified_binary") as verifier:
            output = Path(temporary) / "native"
            with self.assertRaisesRegex(ValueError, "selected different bug compatibility paths"):
                engine.run_profile(catalog, selected, "short", "basic", None, output, Path(temporary),
                                   binary=Path(temporary) / "compile", profile_tags=ordinary["include"][0]["tags"])
            self.assertFalse(output.exists())
        popen.assert_not_called()
        verifier.assert_not_called()

    def test_matching_entry_tags_are_accepted(self):
        _, ordinary, selected = self.plans()
        for plan in (ordinary, selected):
            engine.matrix_tags(plan["include"][0], plan["include"][0]["tags"])
            engine.matrix_tags(plan["include"][0], None)

    def test_cli_refuses_a_mismatched_entry_or_selection_before_allocating(self):
        optional = fixture(path="internal/integration/optional_test.go", category="optional", batch="native_bug")
        optional["constraint"] = combine("and", optional["constraint"], tag("native_regression"))
        optional["tags"].append("native_regression")
        cases = {
            "selected different bug compatibility paths": ["--profile-tags", "ci," + BUG_A],
            "unregistered bug compatibility tag": ["--bug-compatibility", "ceph_bug_synthetic_typo"],
            "not a bug compatibility tag": ["--bug-compatibility", "integration"],
        }
        for message, arguments in cases.items():
            for command in (["compile", "--category", "short", "--batch", "basic"],
                            ["run", "--category", "short", "--batch", "basic", "--binary", "compile"],
                            ["optional", "--batch", "native_bug", "--binary", "compile"]):
                with self.subTest(message=message, command=command[0]), tempfile.TemporaryDirectory() as temporary, \
                        patch.object(engine, "load_catalog", return_value=[fixture(), optional]), \
                        patch.object(engine, "bounded_go") as compiler, \
                        patch.object(engine.subprocess, "Popen") as popen, \
                        patch("sys.stderr", new=io.StringIO()) as error:
                    directory = Path(temporary) / "out"
                    self.assertEqual(engine.main(command + arguments + ["--directory", str(directory)]), 1)
                    self.assertFalse(directory.exists())
                    compiler.assert_not_called()
                    popen.assert_not_called()
                    self.assertIn(message, error.getvalue())

    def test_environment_never_carries_the_selection(self):
        environment = {"GOFLAGS": "-tags=" + BUG_A, "SCENARIO_BUG_COMPATIBILITY": BUG_A,
                       "BUG_COMPATIBILITY": BUG_A}
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, environment), \
                patch.object(engine, "load_catalog", return_value=[fixture()]), \
                patch("sys.stdout", new=io.StringIO()):
            output = Path(temporary) / "plan.json"
            self.assertEqual(engine.main(["plan", "--category", "short", "--output", str(output),
                                          "--github-output", str(Path(temporary) / "github")]), 0)
            plan = json.loads(output.read_text())
            self.assertEqual(plan["bug_compatibility"], [])
            self.assertTrue(all(BUG_A not in profile["tags"].split(",") for profile in plan["include"]))
            matrix = json.loads(github_outputs(Path(temporary) / "github")["matrix"])["include"]
            self.assertTrue(all(entry["bug_compatibility"] == [] for entry in matrix))
            self.assertNotIn("-tags", engine.go_environment()["GOFLAGS"])


class BugCompatibilityRegistryTests(unittest.TestCase):
    def test_malformed_registry_is_refused(self):
        def document(**changes):
            entry = {"tag": BUG_A, "tracker": "https://example.invalid/a", "status": "supported",
                     "sources": ["rbd/a.go"]}
            entry.update(changes)
            return {"schema": engine.BUG_REGISTRY_SCHEMA, "tags": [entry], "conflicts": []}
        cases = [({"schema": "other", "tags": [], "conflicts": []}, "unknown schema"),
                 ({"schema": engine.BUG_REGISTRY_SCHEMA, "tags": []}, "exactly schema"),
                 (document(tag="bug_a"), "ceph_bug_<id>"), (document(tag="ceph_bug_A"), "ceph_bug_<id>"),
                 (document(tag="ceph_bug_a_"), "ceph_bug_<id>"), (document(tracker="tracker 1"), "https"),
                 (document(status="draft"), "status"), (document(sources=[]), "names its source"),
                 (document(sources=["../x.go"]), "repository Go files"),
                 (document(sources=["rbd/a.go", "rbd/a.go"]), "repository Go files"),
                 (document(extra=True), "exactly tag")]
        twice = document()
        twice["tags"].append(dict(twice["tags"][0]))
        cases.append((twice, "registered twice"))
        for pair, message in (([BUG_A, BUG_A], "two distinct"), ([BUG_A, BUG_B], "two distinct")):
            conflict = document()
            conflict["conflicts"] = [pair]
            cases.append((conflict, message))
        for value, message in cases:
            with self.subTest(message=message, value=value), self.assertRaisesRegex(ValueError, message):
                engine.bug_registry(value)

    def test_registered_sources_must_constrain_on_their_tag(self):
        registry = bug_registry(supported=(BUG_A,))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "missing"):
                engine.bug_registry_sources(registry, root)
            source = root / "rbd" / (BUG_A + ".go")
            source.parent.mkdir()
            source.write_text("//go:build !" + BUG_A + "x\n\npackage rbd\n")
            with self.assertRaisesRegex(ValueError, "does not constrain"):
                engine.bug_registry_sources(registry, root)
            source.write_text("//go:build !" + BUG_A + "\n\npackage rbd\n")
            engine.bug_registry_sources(registry, root)

    def test_checked_in_registry_matches_the_catalogue(self):
        registry = engine.load_bug_registry()
        engine.bug_registry_sources(registry)
        catalogue = (engine.ROOT / "docs/guides/bug-compatibility-tags.md").read_text()
        documented = set(re.findall(r"`(ceph_bug_[a-z0-9_]+)`", catalogue))
        self.assertEqual(documented, set(registry["tags"]))


if __name__ == "__main__":
    unittest.main()
