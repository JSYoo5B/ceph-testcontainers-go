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
    match = re.search(r"^\s+(?:-\s+)?run:\s*(.*?)\s*$", step, re.MULTILINE)
    return match[1] if match else ""


def engine_step(job_steps, operation):
    matches = [step for step in job_steps if re.search(
        r"\btag_scenarios\.py " + re.escape(operation) + r"(?:\s|$)", run_command(step))]
    if len(matches) != 1:
        raise AssertionError("expected exactly one tag runner operation: " + operation)
    return matches[0]


class SourceDrivenWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/test.yml").read_text()
        cls.jobs = dict(blocks(cls.workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))
        cls.catalog = engine.load_catalog()
        cls.plan = engine.discover(cls.catalog)
        cls.optional = engine.discover_optional(cls.catalog)

    def test_workflow_uses_discovered_matrix_and_source_owned_job_budgets(self):
        plan, runtime = self.jobs["plan"], self.jobs["runtime"]
        self.assertEqual(scalar(plan, "needs"), "code-check")
        self.assertIn("matrix: ${{ steps.plan.outputs.matrix }}", plan)
        command = shlex.split(run_command(engine_step(steps(plan), "plan")))
        self.assertIn("--verify", command)
        self.assertIn("--github-output", command)
        self.assertEqual(command[command.index("--github-output") + 1], "$GITHUB_OUTPUT")
        self.assertIn("--output", command)
        self.assertEqual(scalar(runtime, "needs"), "plan")
        self.assertEqual(scalar(runtime, "matrix"), "${{ fromJSON(needs.plan.outputs.matrix) }}")
        self.assertEqual(scalar(runtime, "timeout-minutes"), "${{ matrix.job_timeout }}")
        self.assertEqual(scalar(runtime, "fail-fast"), "false")
        self.assertNotRegex(runtime, r"(?m)^        (?:case|group|include|exclude):")
        self.assertNotRegex(self.workflow, r"\bTest[A-Z][A-Za-z0-9_]*")

    def test_runtime_compiles_before_environment_then_runs_and_cleans(self):
        runtime_steps = steps(self.jobs["runtime"])
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
                         "${{ always() && steps.runtime_cleanup_baseline.outcome == 'success' }}")
        self.assertEqual(scalar(baseline, "artifact_name"), scalar(cleanup, "artifact_name"))
        self.assertIn("${{ matrix.id }}", scalar(cleanup, "artifact_name"))
        for step in (compiled, baseline, native):
            self.assertNotRegex(step, r"(?m)^\s+(?:if|continue-on-error):")
        self.assertNotRegex(self.jobs["runtime"], r"(?m)^    continue-on-error:")

    def test_selected_source_profile_is_recomputed_and_no_name_selector_reaches_native(self):
        for operation in ("compile", "run"):
            command = shlex.split(run_command(engine_step(steps(self.jobs["runtime"]), operation)))
            self.assertEqual(command[:3], ["python3", ".github/scripts/tag_scenarios.py", operation])
            for name in ("category", "batch", "package"):
                self.assertEqual(command[command.index("--" + name) + 1], "${{ matrix." + name + " }}")
            self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast", "--timeout", "--tags"))
                                 for argument in command))
            self.assertNotIn("make", command)
        compile_command = shlex.split(run_command(engine_step(steps(self.jobs["runtime"]), "compile")))
        native_command = shlex.split(run_command(engine_step(steps(self.jobs["runtime"]), "run")))
        self.assertNotEqual(compile_command[compile_command.index("--directory") + 1],
                            native_command[native_command.index("--directory") + 1])

    def test_code_and_plan_stages_do_not_allocate_docker_or_prepare_images(self):
        for name in ("code-check", "plan"):
            block = self.jobs[name]
            self.assertNotIn("./.github/actions/scenario-images", block)
            self.assertNotIn("./.github/actions/runtime-cleanup", block)
            self.assertNotRegex(block, r"\bdocker\s|docker://|docker/setup-")
        code = self.jobs["code-check"]
        self.assertIn("run: make check", code)
        self.assertIn("test_tag_scenarios.py", code)
        self.assertIn("test_scenario_shards.py", code)
        self.assertIn("test_scenario_images.py", code)
        self.assertIn("test_scenario_quiescence.py", code)
        self.assertIn("test_scenario_recovery.py", code)
        self.assertIn("test_scenario_receivers.py", code)
        self.assertTrue(any(profile["category"] == "code" for profile in self.plan["include"]))
        code_step = engine_step(steps(code), "code")
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

    def test_sdk_bridge_profiles_are_source_owned_and_do_not_require_ceph_images(self):
        profiles = [profile for profile in self.plan["include"] if profile["package"] == "./internal/dockerbridge"]
        self.assertTrue(profiles)
        self.assertTrue(all(not profile["requires_ceph"] for profile in profiles))
        native = run_command(engine_step(steps(self.jobs["runtime"]), "run"))
        self.assertIn("--package '${{ matrix.package }}'", native)
        self.assertNotIn("./internal/dockerbridge", self.workflow)

    def test_optional_native_regressions_have_explicit_tag_runner_and_source_budget(self):
        optional_jobs = [block for block in self.jobs.values() if "tag_scenarios.py optional " in block]
        self.assertEqual(len(optional_jobs), 1)
        block = optional_jobs[0]
        header = block.split("    steps:\n", 1)[0]
        self.assertIn("github.event_name == 'workflow_dispatch'", scalar(header, "if"))
        self.assertIn("inputs.rgw_native_regressions", scalar(header, "if"))
        self.assertEqual(scalar(block, "needs"), "plan")
        command = shlex.split(run_command(engine_step(steps(block), "optional")))
        batch = command[command.index("--batch") + 1]
        profiles = [profile for profile in self.optional["include"] if profile["batch"] == batch]
        self.assertEqual(len(profiles), 1)
        self.assertEqual(int(scalar(block, "timeout-minutes")), profiles[0]["job_timeout"])
        self.assertIn("ci_optional", profiles[0]["tags"].split(","))
        self.assertIn("native_regression", profiles[0]["tags"].split(","))
        self.assertFalse(any(profile["batch"] == batch for profile in self.plan["include"]))
        self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast")) for argument in command))
        self.assertIn("CEPH_TEST_RGW_IMAGE: ${{ inputs.rgw_image || env.CEPH_TEST_RGW_IMAGE }}", block)
        self.assertNotRegex(block, r"(?m)^    continue-on-error:")

    def test_every_result_upload_is_unconditional_and_native_reporter_cannot_mask_failure(self):
        for name in ("code-check", "plan", "runtime"):
            uploads = [step for step in steps(self.jobs[name]) if "uses: actions/upload-artifact" in step]
            self.assertTrue(uploads)
            for upload in uploads:
                self.assertEqual(scalar(upload, "if"), "always()")
        runtime_steps = steps(self.jobs["runtime"])
        native = engine_step(runtime_steps, "run")
        reporter = next(step for step in runtime_steps if "report_test_failures.py" in run_command(step))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(reporter))
        self.assertEqual(scalar(reporter, "if"), "failure()")
        self.assertEqual(scalar(reporter, "continue-on-error"), "true")
        command = shlex.split(run_command(reporter))
        directory = shlex.split(run_command(native))[-1]
        self.assertEqual(command[2], directory + "/native.log")
        self.assertIsNotNone(failure_reporter.PROFILE.fullmatch(command[command.index("--profile") + 1]))

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
