"""Check source-owned CI scheduling and workflow phases without Docker.

The Go AST catalog reads source only. Compiled profile verification belongs to
`tag_scenarios.py plan --verify`; this suite does not compile every batch again.
"""

import contextlib
import io
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest

import report_test_failures as failure_reporter
import tag_scenarios as engine


ROOT = Path(__file__).resolve().parents[2]


def blocks(text, pattern):
    matches = list(re.finditer(pattern, text, re.MULTILINE))
    return [(match[1], text[match.start():matches[index + 1].start()
             if index + 1 < len(matches) else len(text)]) for index, match in enumerate(matches)]


def steps(job):
    return [text for _, text in blocks(job, r"^      - (.+)$")]


def scalar(text, key):
    values = re.findall(r"^\s+(?:-\s+)?" + re.escape(key) + r":\s*(.*?)\s*$", text, re.MULTILINE)
    if len(values) != 1:
        raise AssertionError("expected exactly one " + key + " field")
    return values[0]


def run_command(step):
    match = re.search(r"^( +)(?:-\s+)?run:\s*(.*?)\s*$", step, re.MULTILINE)
    if not match:
        return ""
    if match[2] not in ("|", "|-", "|+", ">", ">-", ">+"):
        return match[2]
    indent = len(match[1])
    lines = []
    for line in step[match.end():].splitlines():
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        if line.strip():
            lines.append(line.strip())
    return "\n".join(lines)


def engine_step(job_steps, operation):
    matches = [step for step in job_steps if re.search(
        r"\btag_scenarios\.py " + re.escape(operation) + r"(?:\s|$)", run_command(step))]
    if len(matches) != 1:
        raise AssertionError("expected exactly one tag runner operation: " + operation)
    return matches[0]


def engine_command(job_steps, operation, option=None):
    commands = [shlex.split(line) for line in run_command(engine_step(job_steps, operation)).splitlines()
                if re.search(r"\btag_scenarios\.py " + re.escape(operation) + r"(?:\s|$)", line)]
    if option:
        commands = [command for command in commands if option in command]
    if len(commands) != 1:
        raise AssertionError("expected one engine command for selected operation/option")
    return commands[0]


class SourceDrivenWorkflowTests(unittest.TestCase):
    # Workflow categories are public check types, never a list of Test names.
    ENTRY_POINTS = {
        "docker.yml": ("Docker checks", "environment"),
        "ceph-short.yml": ("Ceph short", "short"),
        "ceph-topology.yml": ("Ceph topology", "topology"),
        "ceph-multicluster.yml": ("Ceph multicluster", "multicluster"),
        "ceph-recovery.yml": ("Ceph recovery", "recovery"),
    }

    @classmethod
    def setUpClass(cls):
        cls.workflows = {path.name: path.read_text()
                         for path in (ROOT / ".github/workflows").glob("*.yml")}
        cls.jobs = {}
        for filename, workflow in cls.workflows.items():
            cls.jobs[filename] = dict(blocks(workflow.split("\njobs:\n", 1)[1],
                                             r"^  ([\w-]+):$"))
        cls.catalog = engine.load_catalog()
        cls.plan = engine.discover(cls.catalog)
        cls.optional = engine.discover_optional(cls.catalog)

    def test_pr_checks_are_separate_workflows_with_independent_entry_points(self):
        expected = {"code.yml", "tagged-runtime.yml", "native-regressions.yml", *self.ENTRY_POINTS}
        self.assertEqual(set(self.workflows), expected)
        self.assertNotIn("test.yml", self.workflows)
        for filename, (name, category) in self.ENTRY_POINTS.items():
            with self.subTest(workflow=filename):
                workflow = self.workflows[filename]
                self.assertRegex(workflow, r"(?m)^name: " + re.escape(name) + r"$")
                for event in ("push", "pull_request", "workflow_dispatch"):
                    self.assertRegex(workflow, r"(?m)^  " + event + r":")
                self.assertIn("  push:\n    branches: [main]\n  pull_request:\n  workflow_dispatch:", workflow)
                self.assertEqual(scalar(workflow, "group"),
                                 "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || (github.event_name == 'push' && github.ref) || github.run_id }}")
                self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
                self.assertNotRegex(workflow, r"(?m)^    needs:")
                callers = [job for job in self.jobs[filename].values()
                           if "uses: ./.github/workflows/tagged-runtime.yml" in job]
                self.assertEqual(len(callers), 1)
                self.assertEqual(scalar(callers[0].split("    with:", 1)[0], "name"), "Scenarios")
                self.assertEqual(scalar(callers[0], "category"), category)
                self.assertEqual(scalar(callers[0], "check_name"), name)
                self.assertNotIn("steps:", callers[0])
                self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
        reusable = self.workflows["tagged-runtime.yml"]
        self.assertRegex(reusable, r"(?m)^  workflow_call:")
        self.assertNotRegex(reusable, r"(?m)^  (?:push|pull_request|workflow_dispatch):")
        self.assertNotIn("concurrency:", reusable)
        optional = self.workflows["native-regressions.yml"]
        self.assertRegex(optional, r"(?m)^  workflow_dispatch:")
        self.assertNotRegex(optional, r"(?m)^  (?:push|pull_request|workflow_call):")
        self.assertNotIn("concurrency:", optional)

    def test_entry_categories_partition_source_profiles_without_ci_name_inventories(self):
        categories = [category for _, category in self.ENTRY_POINTS.values()]
        self.assertEqual(set(categories), set(engine.RUNTIME_CATEGORIES))
        self.assertEqual(len(categories), len(set(categories)))
        selected = []
        for category in categories:
            plan = engine.select_required_plan(self.plan, category)
            self.assertTrue(plan["include"])
            self.assertTrue(all(profile["category"] == category for profile in plan["include"]))
            selected.extend(profile["id"] for profile in plan["include"])
        expected = [profile["id"] for profile in self.plan["include"]
                    if profile["category"] != "code"]
        self.assertEqual(set(selected), set(expected))
        self.assertEqual(len(selected), len(set(selected)))
        self.assertTrue(any(profile["category"] == "code" for profile in self.plan["include"]))
        for workflow in self.workflows.values():
            self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
            self.assertNotRegex(workflow, r"(?:^|\s)-run(?:=|\s)")
            self.assertNotRegex(workflow, r"(?:^|\s)-short(?:=|\s)")

    def test_reusable_plan_filters_category_and_keeps_source_owned_job_budgets(self):
        plan, runtime = self.jobs["tagged-runtime.yml"]["plan"], self.jobs["tagged-runtime.yml"]["runtime"]
        command = engine_command(steps(plan), "plan", "--category")
        self.assertNotIn("--verify", command)
        self.assertEqual(command[command.index("--category") + 1], "$SCENARIO_CATEGORY")
        self.assertEqual(scalar(engine_step(steps(plan), "plan"), "SCENARIO_CATEGORY"), "${{ inputs.category }}")
        self.assertIn("--github-output", command)
        self.assertEqual(command[command.index("--github-output") + 1], "$GITHUB_OUTPUT")
        self.assertIn("--output", command)
        self.assertIn("matrix: ${{ steps.plan.outputs.matrix }}", plan)
        self.assertEqual(scalar(runtime, "needs"), "plan")
        self.assertEqual(scalar(runtime, "matrix"), "${{ fromJSON(needs.plan.outputs.matrix) }}")
        self.assertEqual(scalar(runtime, "timeout-minutes"), "${{ matrix.job_timeout }}")
        self.assertEqual(scalar(runtime, "fail-fast"), "false")
        self.assertEqual(scalar(plan.split("    steps:", 1)[0], "name"), "Plan")
        self.assertEqual(scalar(runtime.split("    steps:", 1)[0], "name"), "${{ matrix.batch }}")
        self.assertNotRegex(runtime, r"(?m)^        (?:case|group|include|exclude):")

    def test_runtime_compiles_before_environment_then_runs_and_cleans_on_same_runner(self):
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        runtime_steps = steps(runtime)
        compiled = engine_step(runtime_steps, "compile")
        native = engine_step(runtime_steps, "run")
        baseline = next(step for step in runtime_steps if "phase: snapshot" in step)
        prepared = next(step for step in runtime_steps if "uses: ./.github/actions/scenario-images" in step)
        cleanup = next(step for step in runtime_steps if "phase: check" in step)
        self.assertLess(runtime_steps.index(compiled), runtime_steps.index(baseline))
        self.assertLess(runtime_steps.index(baseline), runtime_steps.index(prepared))
        self.assertLess(runtime_steps.index(prepared), runtime_steps.index(native))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(cleanup))
        self.assertEqual(scalar(prepared, "if"), "matrix.requires_ceph")
        self.assertEqual(scalar(cleanup, "if"),
                         "${{ always() && steps.docker_baseline.outcome == 'success' }}")
        self.assertEqual(scalar(baseline, "artifact_name"), scalar(cleanup, "artifact_name"))
        self.assertIn("${{ matrix.id }}", scalar(cleanup, "artifact_name"))
        for step in (compiled, baseline, native):
            self.assertNotRegex(step, r"(?m)^\s+(?:if|continue-on-error):")
        self.assertNotRegex(runtime, r"(?m)^    continue-on-error:")
        self.assertNotIn("actions/download-artifact", runtime)

    def test_selected_source_profile_is_recomputed_and_no_name_selector_reaches_native(self):
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])
        for operation in ("compile", "run"):
            step = engine_step(runtime_steps, operation)
            command = engine_command(runtime_steps, operation)
            self.assertEqual(command[:3], ["python3", ".github/scripts/tag_scenarios.py", operation])
            for name in ("category", "batch", "package"):
                variable = "SCENARIO_" + name.upper()
                self.assertEqual(command[command.index("--" + name) + 1], "$" + variable)
                self.assertEqual(scalar(step, variable), "${{ matrix." + name + " }}")
            self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast", "--timeout", "--tags"))
                                 for argument in command))
            self.assertNotIn("make", command)
        compiled = engine_command(runtime_steps, "compile")
        native = engine_command(runtime_steps, "run")
        self.assertNotEqual(compiled[compiled.index("--directory") + 1],
                            native[native.index("--directory") + 1])

    def test_code_workflow_has_visible_unit_race_static_and_coverage_jobs(self):
        workflow = self.workflows["code.yml"]
        jobs = self.jobs["code.yml"]
        self.assertRegex(workflow, r"(?m)^name: Code checks$")
        self.assertEqual(set(jobs), {"unit", "race", "static", "tag-coverage"})
        self.assertEqual({scalar(job.split("    steps:", 1)[0], "name") for job in jobs.values()},
                         {"Unit tests", "Race detector", "Vet, compile and CI tooling", "Compiled tag coverage and helpers"})
        for event in ("push", "pull_request", "workflow_dispatch"):
            self.assertRegex(workflow, r"(?m)^  " + event + r":")
        self.assertIn("  push:\n    branches: [main]\n  pull_request:\n  workflow_dispatch:", workflow)
        self.assertEqual(scalar(workflow, "group"),
                         "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || (github.event_name == 'push' && github.ref) || github.run_id }}")
        self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
        for block in jobs.values():
            self.assertNotIn("./.github/actions/scenario-images", block)
            self.assertNotIn("./.github/actions/runtime-cleanup", block)
            self.assertNotRegex(block, r"\bdocker\s|docker://|docker/setup-")
            self.assertNotRegex(block, r"(?m)^    needs:")
        self.assertIn("make test", jobs["unit"])
        self.assertIn("make race", jobs["race"])
        self.assertIn("make vet", jobs["static"])
        self.assertIn("make tag-compile", jobs["static"])
        self.assertIn("python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v", jobs["static"])
        self.assertTrue(list((ROOT / ".github/scripts").glob("test_*.py")))
        coverage_steps = steps(jobs["tag-coverage"])
        coverage = shlex.split(run_command(engine_step(coverage_steps, "plan")))
        self.assertIn("--verify", coverage)
        self.assertNotIn("--category", coverage)
        code_step = engine_step(coverage_steps, "code")
        self.assertNotIn("--batch", run_command(code_step))
        self.assertNotIn("--category", run_command(code_step))

    def test_source_profiles_have_positive_budgets_safe_ids_and_exact_checker_scopes(self):
        profiles = self.plan["include"]
        identifiers = [profile["id"] for profile in profiles]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        for profile in profiles:
            with self.subTest(profile=profile["profile"]):
                self.assertRegex(profile["id"], r"\A[a-z][a-z0-9_-]{0,79}\Z")
                self.assertGreater(profile["job_timeout"], 0)
                self.assertIsNotNone(engine.DURATION.fullmatch(profile["timeout"]))
                self.assertTrue(engine.selected_tests(self.catalog, profile["package"], profile["tags"]))
                self.assertNotIn("all", profile["tags"].split(","))
                self.assertIn("ci_batch_" + profile["batch"], profile["tags"].split(","))
                self.assertNotIn("native_regression", profile["tags"].split(","))
        for checker, (module_name, _) in engine.CHECKERS.items():
            module = __import__(module_name)
            expected = set(module.NETWORKS if checker == "receivers" else module.CASES)
            actual = [profile["case"] for profile in profiles if profile["checker"] == checker]
            self.assertEqual(set(actual), expected)
            self.assertEqual(len(actual), len(set(actual)))

    def test_docker_environment_checks_are_source_owned_and_do_not_require_ceph_images(self):
        profiles = engine.select_required_plan(self.plan, "environment")["include"]
        self.assertTrue(profiles)
        self.assertTrue(all(not profile["requires_ceph"] for profile in profiles))
        self.assertTrue(all(profile["package"] == "./internal/dockerbridge" for profile in profiles))
        for category in ("short", "topology", "multicluster", "recovery"):
            self.assertTrue(all(profile["requires_ceph"] for profile in
                                engine.select_required_plan(self.plan, category)["include"]))

    def test_optional_native_regressions_have_explicit_tag_runner_and_source_budget(self):
        jobs = self.jobs["native-regressions.yml"]
        self.assertEqual(len(jobs), 1)
        caller = next(iter(jobs.values()))
        self.assertEqual(scalar(caller.split("    with:", 1)[0], "name"), "Scenarios")
        self.assertNotRegex(caller, r"(?m)^    needs:")
        self.assertEqual(scalar(caller, "uses"), "./.github/workflows/tagged-runtime.yml")
        self.assertEqual(scalar(caller, "category"), "optional")
        batch = scalar(caller, "optional_batch")
        profiles = [profile for profile in self.optional["include"] if profile["batch"] == batch]
        self.assertEqual(len(profiles), 1)
        selected = engine.select_optional_plan(self.optional, batch)
        self.assertEqual(selected["include"], profiles)
        self.assertIn("ci_optional", profiles[0]["tags"].split(","))
        self.assertIn("native_regression", profiles[0]["tags"].split(","))
        self.assertFalse(any(profile["batch"] == batch for profile in self.plan["include"]))
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        self.assertEqual(scalar(runtime, "timeout-minutes"), "${{ matrix.job_timeout }}")
        optional = engine_command(steps(runtime), "optional")
        self.assertEqual(optional[optional.index("--batch") + 1], "$SCENARIO_BATCH")
        self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast")) for argument in optional))
        self.assertIn("CEPH_TEST_RGW_IMAGE: ${{ inputs.rgw_image || env.CEPH_TEST_RGW_IMAGE }}", runtime)
        self.assertNotRegex(runtime, r"(?m)^    continue-on-error:")
        plan = self.jobs["tagged-runtime.yml"]["plan"]
        plan_command = engine_command(steps(plan), "plan", "--optional")
        self.assertEqual(plan_command[plan_command.index("--batch") + 1], "$SCENARIO_OPTIONAL_BATCH")
        self.assertNotIn("--verify", plan_command)

    def test_result_uploads_and_failure_reports_preserve_native_failure(self):
        for filename, jobs in self.jobs.items():
            for name, block in jobs.items():
                uploads = [step for step in steps(block) if "uses: actions/upload-artifact" in step]
                for upload in uploads:
                    with self.subTest(workflow=filename, job=name):
                        self.assertEqual(scalar(upload, "if"), "always()")
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])
        native = engine_step(runtime_steps, "run")
        reporter = next(step for step in runtime_steps if "report_test_failures.py" in run_command(step))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(reporter))
        self.assertEqual(scalar(reporter, "if"), "failure()")
        self.assertEqual(scalar(reporter, "continue-on-error"), "true")
        command = shlex.split(run_command(reporter))
        directory = engine_command(runtime_steps, "run")[-1]
        self.assertEqual(command[2], directory + "/native.log")
        self.assertEqual(command[command.index("--profile") + 1], "${{ matrix.id }}")
        for profile in self.plan["include"]:
            self.assertIsNotNone(failure_reporter.PROFILE.fullmatch(profile["id"]))
        phases = next(step for step in runtime_steps if "report_workflow_phases.py" in run_command(step))
        self.assertEqual(scalar(phases, "if"), "always()")
        self.assertNotIn("continue-on-error", phases)
        self.assertLess(runtime_steps.index(native), runtime_steps.index(phases))
        cleanup = next(step for step in runtime_steps if "phase: check" in step)
        self.assertLess(runtime_steps.index(cleanup), runtime_steps.index(phases))
        for name, step_id in (("COMPILE", "compile"), ("DOCKER", "docker_baseline"),
                              ("IMAGES", "role_images"), ("NATIVE", "native"), ("CLEANUP", "cleanup")):
            self.assertEqual(scalar(phases, "SCENARIO_" + name + "_OUTCOME"),
                             "${{ steps." + step_id + ".outcome }}")
        self.assertEqual(scalar(phases, "SCENARIO_CHECK_NAME"), "${{ inputs.check_name }}")
        self.assertEqual(scalar(phases, "SCENARIO_PROFILE"), "${{ matrix.id }}")
        self.assertEqual(scalar(phases, "SCENARIO_REQUIRES_CEPH"), "${{ matrix.requires_ceph }}")

    def test_all_target_selects_source_union_and_tag_compile_covers_explicit_opt_ins(self):
        for target in ("test-all", "tag-compile"):
            result = subprocess.run(["make", "--no-print-directory", "-n", target], cwd=ROOT,
                                    text=True, capture_output=True, timeout=10, check=True)
            commands = [shlex.split(line) for line in result.stdout.splitlines() if " go test " in line]
            self.assertTrue(commands)
            for command in commands:
                self.assertIn("CGO_ENABLED=0", command)
                self.assertIn("-mod=readonly", command)
                self.assertIn("./...", command)
            if target == "test-all":
                self.assertEqual(len(commands), 1)
                self.assertIn("-tags=all", commands[0])
                self.assertIn("-count=1", commands[0])
                self.assertIn("-v", commands[0])
                self.assertFalse(any(token in ("-run", "-short", "-failfast") for token in commands[0]))
            else:
                tag_sets = [{tag for tag in token.removeprefix("-tags=").split(",")}
                            for command in commands for token in command if token.startswith("-tags=")]
                self.assertIn({"all"}, tag_sets)
                self.assertIn({"all", "goceph", "native_regression"}, tag_sets)


class FailureReporterSafetyTests(unittest.TestCase):
    def report(self, text=None, profile="tag-scenario", extra=()):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "native.log"
            if text is not None:
                path.write_text(text)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = failure_reporter.main([str(path), "--profile", profile, *extra])
            return status, output.getvalue()

    def test_only_completed_failure_names_are_published_once(self):
        log = """=== RUN   TestExample/leaf
secret diagnostic token=private-key
    --- FAIL: TestExample/leaf (1.23s)
    --- FAIL: TestExample/leaf (1.23s)
--- FAIL: TestExample (1.23s)
FAIL example/internal/integration 1.23s
"""
        status, output = self.report(log)
        self.assertEqual(status, 0)
        self.assertEqual(output.count("TestExample/leaf"), 1)
        self.assertEqual(output.count("::error "), 2)
        self.assertNotIn("private-key", output)
        self.assertNotIn("secret diagnostic", output)

    def test_missing_or_incomplete_log_is_notice_only_and_preserves_exit_status(self):
        for log in (None, "=== RUN   TestIncomplete\npanic: secret\n", ""):
            status, output = self.report(log)
            self.assertEqual(status, 0)
            self.assertIn("::notice ", output)
            self.assertNotIn("::error ", output)
            self.assertNotIn("secret", output)

    def test_malformed_failure_names_and_profile_arguments_are_not_exposed(self):
        bad_log = "--- FAIL: TestExample/::error-secret (1.00s)\n--- FAIL: TestExample (1.00s) secret\n"
        status, output = self.report(bad_log)
        self.assertEqual(status, 0)
        self.assertNotIn("::error ", output)
        self.assertNotIn("secret", output)
        for profile in ("secret\n::error::private-key", "bad%0Avalue", ""):
            status, output = self.report(bad_log, profile)
            self.assertEqual(status, 0)
            self.assertNotIn("private-key", output)
            self.assertNotIn("secret", output)
        status, output = self.report(bad_log, extra=("--unknown=private-key",))
        self.assertEqual(status, 0)
        self.assertNotIn("private-key", output)

    def test_successful_package_does_not_emit_false_failure_annotations(self):
        status, output = self.report("PASS\nok example/internal/integration 2.01s\n")
        self.assertEqual(status, 0)
        self.assertEqual(output, "")


if __name__ == "__main__":
    unittest.main()
