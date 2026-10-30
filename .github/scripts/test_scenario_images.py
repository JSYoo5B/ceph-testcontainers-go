"""Check scenario image selection with Python and make dry-runs only.

The actual composite shell body runs against a temporary Python stub. Neither
Docker nor Go is invoked, and no YAML package or image builder is required.
"""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import run_image_matrix as runner


ROOT = Path(__file__).resolve().parents[2]
ROLE_ENV = ("CEPH_TEST_ESSENTIAL_IMAGE", "CEPH_TEST_CLIENT_IMAGE", "CEPH_TEST_CEPHFS_IMAGE", "CEPH_TEST_RBD_AUX_IMAGE", "CEPH_TEST_RGW_IMAGE")
ROLES = ROLE_ENV
CONSUMERS = ("CEPH_TEST_RBD_CLIENT_IMAGE", "CEPH_TEST_VAULT_IMAGE")
IMAGES = {name: "sha256:" + digit * 64
          for name, digit in zip(ROLES, "12345")}
IMAGES.update({name: "sha256:" + digit * 64 for name, digit in zip(ROLE_ENV, "56789")})
# The Make targets that run Docker tests. scenario takes its tests from the tag
# runner, so the Makefile keeps no test list for it.
DRY_RUN_TARGETS = ("scenario-default", "scenario")
SCENARIO_SELECTION = ("SCENARIO_CATEGORY=recovery", "SCENARIO_BATCH=container_pause")
# A -run selection in the Makefile is a name prefix or the empty match, never
# a list of tests, except the representative image-compatibility set.
SELECTION_RUN = re.compile(r"\^(?:\$\$|Test[A-Za-z]+)")
PREP = "./.github/actions/scenario-images"
CLEANUP = "./.github/actions/runtime-cleanup"


def blocks(text, expression):
    """Read the checked-in indentation convention, without a YAML dependency."""
    starts = list(re.finditer(expression, text, re.M))
    return [(match.group(1), text[match.start():
             starts[index + 1].start() if index + 1 < len(starts) else len(text)])
            for index, match in enumerate(starts)]


def scalar(block, key):
    matches = re.findall(r"^\s+(?:-\s+)?" + re.escape(key) + r":\s*(.*?)\s*$",
                         block, re.M)
    if len(matches) != 1:
        raise AssertionError(f"expected one {key!r} in step: {block}")
    return matches[0]


def steps(block, indent=6):
    return [value for _, value in blocks(block, r"^" + " " * indent + r"- (.+)$")]


def check_dry_run_source(text):
    if re.search(r"\$[({]\s*(?:shell|file)\b", text):
        raise AssertionError("dry-run boundary requires no make shell/file evaluation")
    if re.search(r"^\t\s*[@+-]*\+", text, re.M):
        raise AssertionError("dry-run boundary refuses forced recipe execution")
    targets = blocks(text, r"^([\w.-]+):[ \t]*$")
    target_bodies = dict(targets)
    for name in DRY_RUN_TARGETS:
        if name not in target_bodies or re.search(
                r"\$\(\s*MAKE\s*\)|\$\{\s*MAKE\s*\}", target_bodies[name]):
            raise AssertionError("scenario dry-run cannot execute recursive recipes")


def step_run(step):
    fields = list(re.finditer(r"^( +)(?:- )?run:\s*(.*?)\s*$", step, re.M))
    if not fields:
        return ""
    if len(fields) != 1:
        raise AssertionError("expected one run field per workflow step")
    field = fields[0]
    value = field.group(2)
    if value not in ("|", "|-", "|+", ">", ">-", ">+"):
        return value
    body = []
    for line in step[field.end():].splitlines():
        if line.strip() and len(line) - len(line.lstrip()) <= len(field.group(1)):
            break
        body.append(line)
    return "\n".join(body)


def check_environment_prefix(prefix):
    if not prefix or prefix[0] != "env":
        raise AssertionError("scenario command must begin with env")
    index = 0
    while index < len(prefix):
        token = prefix[index]
        if token in ("env", "CGO_ENABLED=0"):
            index += 1
        elif token == "-u" and index + 1 < len(prefix) and prefix[index + 1] in (*ROLES, *CONSUMERS, "CEPH_TEST_IMAGE"):
            index += 2
        else:
            raise AssertionError("unapproved executable or option in scenario env prefix")


class ScenarioImageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.makefile = (ROOT / "Makefile").read_text()
        # GNU make may evaluate functions and recursive recipes during -n.
        check_dry_run_source(cls.makefile)
        cls.workflows = {path.name: path.read_text()
                         for path in (ROOT / ".github/workflows").glob("*.yml")}
        cls.jobs = dict(blocks(cls.workflows["tagged-runtime.yml"].split("\njobs:\n", 1)[1],
                              r"^  ([\w-]+):$"))
        optional_jobs = dict(blocks(cls.workflows["native-regressions.yml"].split("\njobs:\n", 1)[1],
                                   r"^  ([\w-]+):$"))
        cls.optional_entry = next(iter(optional_jobs.values()))
        cls.optional = cls.jobs["runtime"]
        cls.action = (ROOT / ".github/actions/scenario-images/action.yml").read_text()

    def environment(self, overrides=None):
        env = os.environ.copy()
        for name in (*ROLES, *CONSUMERS, "SCENARIO_IMAGE_LAYOUT", "MAKEFLAGS",
                     "MFLAGS", "GNUMAKEFLAGS", "MAKEFILES", "MAKEOVERRIDES"):
            env.pop(name, None)
        env.update(IMAGES)
        env.update({name: "inherited-consumer:unrelated" for name in CONSUMERS})
        env["CGO_ENABLED"] = "1"
        if overrides:
            for name, value in overrides.items():
                if value is None:
                    env.pop(name, None)
                else:
                    env[name] = value
        return env

    def dry_run(self, target, env, extra=(), success=True):
        result = subprocess.run(["make", "--no-print-directory", "-n", target, *SCENARIO_SELECTION, *extra],
                                cwd=ROOT, env=env, capture_output=True, text=True,
                                timeout=10)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        commands = [shlex.split(line) for line in result.stdout.splitlines()
                    if " go test " in line or "/tag_scenarios.py run " in line]
        return result, commands

    def prepared_environment(self, command, env):
        # Execute the generated env prefix against Python, never the Go recipe.
        index = next(i for i, token in enumerate(command) if token in ("go", "python3"))
        self.assertIn(command[index:index + 2], (["go", "test"], ["python3", ".github/scripts/tag_scenarios.py"]))
        check_environment_prefix(command[:index])
        program = "import json,os; print(json.dumps(dict(os.environ)))"
        result = subprocess.run([*command[:index], sys.executable, "-c", program],
                                env=env, capture_output=True, text=True,
                                timeout=10, check=True)
        return json.loads(result.stdout)

    def assert_image_environment(self, target, command, env, roles):
        actual = self.prepared_environment(command, env)
        for name in ROLES:
            if roles:
                self.assertEqual(actual.get(name), IMAGES[name])
                self.assertNotIn(name, command)
            else:
                self.assertNotIn(name, actual)
                self.assertEqual(command[command.index(name) - 1], "-u")
        # The tag runner itself drops the RBD client image and sets the Go
        # flags, so the Make prefix passes the consumers through unchanged.
        for name in CONSUMERS:
            self.assertEqual(actual.get(name), env[name])
        self.assertEqual(actual.get("CGO_ENABLED"), "0")
        if "go" in command:
            self.assertIn("-mod=readonly", command)

    def test_environment_prefix_rejects_executables_and_unapproved_options(self):
        for prefix in (["env", "sh", "-c", "docker info"],
                       ["env", "-S", "docker info"],
                       ["env", "-u", "PATH"], ["env", "EXTERNAL_COMMAND=go"]):
            with self.subTest(prefix=prefix), mock.patch("subprocess.run") as run:
                with self.assertRaises(AssertionError):
                    self.prepared_environment([*prefix, "go", "test"], self.environment())
                run.assert_not_called()

    def test_workflow_command_reader_ignores_target_in_step_label(self):
        step = ("      - name: Run make scenario-default\n"
                "        shell: bash\n"
                "        run: |\n"
                "          set -o pipefail\n"
                "          make scenario-topology\n")
        self.assertNotRegex(step_run(step), r"(?m)^\s*make scenario-default(?:\s|$)")
        self.assertRegex(step_run(step), r"(?m)^\s*make scenario-topology(?:\s|$)")

    def test_dry_run_refuses_make_functions_and_conditional_recursion(self):
        for function in ("$(shell docker info)", "${shell docker info}",
                         "$(file >external,value)", "${file >external,value}"):
            with self.subTest(function=function):
                with self.assertRaises(AssertionError):
                    check_dry_run_source(self.makefile + "\nUNSAFE := " + function + "\n")
        for recursive in ("$(MAKE)", "${MAKE}"):
            with self.subTest(recursive=recursive):
                source = self.makefile.replace("scenario:\n",
                    "scenario:\nifeq (x,x)\n\t" + recursive + " other-goal\nendif\n", 1)
                with self.assertRaises(AssertionError):
                    check_dry_run_source(source)

    def test_default_layout_clears_inherited_server_images(self):
        env = self.environment()
        for target in DRY_RUN_TARGETS:
            with self.subTest(target=target):
                _, commands = self.dry_run(target, env)
                self.assertTrue(commands)
                for command in commands:
                    self.assert_image_environment(target, command, env, roles=False)

    def test_explicit_all_matches_default_without_requiring_role_values(self):
        env = self.environment({name: None for name in ROLES})
        for target in DRY_RUN_TARGETS:
            with self.subTest(target=target):
                _, default = self.dry_run(target, env)
                _, explicit = self.dry_run(target, env, ("SCENARIO_IMAGE_LAYOUT=all",))
                self.assertEqual(explicit, default)
                for command in explicit:
                    self.assert_image_environment(target, command, env, roles=False)

    def test_explicit_roles_preserve_all_four_ids_and_fixture_consumer_boundary(self):
        env = self.environment({"SCENARIO_IMAGE_LAYOUT": "roles"})
        for target in DRY_RUN_TARGETS:
            with self.subTest(target=target):
                _, commands = self.dry_run(target, env)
                self.assertTrue(commands)
                for command in commands:
                    self.assert_image_environment(target, command, env, roles=True)
                _, selected = self.dry_run(target, self.environment(),
                                           ("SCENARIO_IMAGE_LAYOUT=roles",))
                self.assertEqual(selected, commands)

    def test_each_missing_empty_or_whitespace_role_fails_before_go(self):
        for target in DRY_RUN_TARGETS:
            for name in ROLES:
                for value in (None, "", " \t "):
                    with self.subTest(target=target, name=name, value=value):
                        env = self.environment({"SCENARIO_IMAGE_LAYOUT": "roles", name: value})
                        result, commands = self.dry_run(target, env, success=False)
                        self.assertIn(name + " is required for SCENARIO_IMAGE_LAYOUT=roles",
                                      result.stderr)
                        self.assertFalse(commands)
                        self.assertEqual(result.stdout, "")

    def test_unknown_or_empty_layout_fails_before_go(self):
        for target in DRY_RUN_TARGETS:
            for value in ("unknown", ""):
                with self.subTest(target=target, value=value):
                    result, commands = self.dry_run(
                        target, self.environment(), ("SCENARIO_IMAGE_LAYOUT=" + value,),
                        success=False)
                    self.assertIn("Unknown SCENARIO_IMAGE_LAYOUT", result.stderr)
                    self.assertFalse(commands)
                    self.assertEqual(result.stdout, "")

    def test_scenario_requires_a_selection_and_delegates_to_the_tag_runner(self):
        env = self.environment()
        _, commands = self.dry_run("scenario", env)
        self.assertEqual(len(commands), 1)
        command = commands[0][commands[0].index("python3"):]
        self.assertEqual(command, ["python3", ".github/scripts/tag_scenarios.py", "run",
                                   "--category", "recovery", "--batch", "container_pause",
                                   "--directory", "artifacts/scenario-recovery-container_pause"])
        for missing in ("SCENARIO_CATEGORY", "SCENARIO_BATCH"):
            with self.subTest(missing=missing):
                result = subprocess.run(["make", "--no-print-directory", "scenario",
                                         *SCENARIO_SELECTION, missing + "="],
                                        cwd=ROOT, env=env, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(missing + " is required", result.stderr)

    def test_makefile_keeps_no_test_lists(self):
        selections = re.findall(r"-run '([^']*)'", self.makefile)
        self.assertTrue(selections)
        for selection in selections:
            with self.subTest(selection=selection):
                if "|" in selection:
                    names = re.fullmatch(r"\^Test\((.*)\)\$\$", selection)
                    self.assertIsNotNone(names)
                    expanded = set()
                    for name in re.split(r"\|(?![^()]*\))", names[1]):
                        group = re.fullmatch(r"(\w+)\((.*)\)", name)
                        if group:
                            expanded.update("Test" + group[1] + part for part in group[2].split("|"))
                        else:
                            expanded.add("Test" + name)
                    self.assertEqual(expanded, set(runner.EXPECTED_TESTS))
                else:
                    self.assertRegex(selection, SELECTION_RUN)

    def test_runtime_prepares_roles_between_baseline_and_native_proof(self):
        job_steps = steps(self.jobs["runtime"])
        prep_index = next(i for i, step in enumerate(job_steps) if "uses: " + PREP in step)
        native_index = next(i for i, step in enumerate(job_steps)
                            if "tag_scenarios.py run " in step_run(step))
        cleanups = [(i, step) for i, step in enumerate(job_steps) if "uses: " + CLEANUP in step]
        self.assertEqual(len(cleanups), 2)
        (before_index, before), (after_index, after) = cleanups
        self.assertLess(before_index, prep_index)
        self.assertLess(prep_index, native_index)
        self.assertLess(native_index, after_index)
        self.assertEqual(scalar(before, "id"), "docker_baseline")
        self.assertEqual(scalar(before, "phase"), "snapshot")
        self.assertEqual(scalar(after, "phase"), "check")
        self.assertEqual(scalar(before, "artifact_name"), scalar(after, "artifact_name"))
        self.assertEqual(scalar(after, "if"),
                         "${{ always() && steps.docker_baseline.outcome == 'success' }}")
        self.assertEqual(scalar(job_steps[prep_index], "if"), "matrix.requires_ceph")
        # The planner gives every Ceph profile its declared roles, or all four.
        self.assertEqual(scalar(job_steps[prep_index], "roles"), "${{ matrix.image_roles }}")
        self.assertNotIn("continue-on-error", job_steps[prep_index])

    def test_generated_profiles_use_fresh_runner_resources_and_distinct_artifact_namespaces(self):
        self.assertEqual(set(self.jobs), {"plan", "runtime"})
        block = self.jobs["runtime"]
        self.assertEqual(scalar(block, "needs"), "plan")
        self.assertIn("matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}", block)
        self.assertNotIn("actions/download-artifact", block)
        self.assertEqual(block.count("uses: actions/checkout@v7"), 1)
        job_steps = steps(block)
        prep = next(step for step in job_steps if "uses: " + PREP in step)
        baseline = next(step for step in job_steps if "uses: " + CLEANUP in step and "phase: snapshot" in step)
        native = next(step for step in job_steps if "uses: actions/upload-artifact" in step)
        artifact_name = re.findall(r"^          name: (.+)$", native, re.M)
        self.assertEqual(len(artifact_name), 1)
        namespaces = [scalar(prep, "artifact_name"), scalar(baseline, "artifact_name"), artifact_name[0]]
        self.assertEqual(len(set(namespaces)), len(namespaces))
        self.assertTrue(all("${{ matrix.id }}" in value for value in namespaces))
        # The discovery unit suite proves uniqueness of the source-generated id.
        # No image/layout axis multiplies the source-owned runtime batches.
        self.assertNotRegex(block, r"(?m)^        (?:variant|layout|case|group):")

    def test_image_matrix_is_manual_and_only_ceph_runtime_jobs_prepare_roles(self):
        for workflow in self.workflows.values():
            self.assertNotIn("--variant '${{ matrix.variant }}'", workflow)
            self.assertNotIn("--layout '${{ matrix.layout }}'", workflow)
            self.assertNotIn("quay.io/", workflow)
            self.assertNotIn("image-compatibility:", workflow)
        self.assertRegex(self.makefile, r"(?m)^image-compatibility:$")
        self.assertRegex(self.makefile, r"(?m)^image-matrix:$")
        self.assertIn("$GITHUB_ACTION_PATH/../../scripts/run_image_matrix.py", self.composite_script())
        self.assertEqual(self.jobs["runtime"].count("uses: " + PREP), 1)
        self.assertEqual(self.optional.count("uses: " + PREP), 1)
        prepare = next(step for step in steps(self.jobs["runtime"]) if "uses: " + PREP in step)
        self.assertEqual(scalar(prepare, "if"), "matrix.requires_ceph")
        for workflow in (self.workflows["code.yml"], self.jobs["plan"]):
            self.assertNotIn("uses: " + PREP, workflow)
            self.assertNotIn("uses: " + CLEANUP, workflow)
        for filename in ("docker.yml", "ceph-short.yml", "ceph-topology.yml",
                         "ceph-multicluster.yml", "ceph-recovery.yml"):
            self.assertIn("uses: ./.github/workflows/tagged-runtime.yml", self.workflows[filename])
            self.assertNotIn("uses: " + PREP, self.workflows[filename])

    def test_optional_regressions_prepare_roles_and_override_only_the_rgw_role(self):
        block = self.optional
        job_steps = steps(block)
        prepare_index = next(i for i, step in enumerate(job_steps) if "uses: " + PREP in step)
        run_index = next(i for i, step in enumerate(job_steps)
                         if "tag_scenarios.py optional --batch " in step_run(step))
        cleanup = [(i, step) for i, step in enumerate(job_steps) if "uses: " + CLEANUP in step]
        self.assertEqual(len(cleanup), 2)
        self.assertLess(cleanup[0][0], prepare_index)
        self.assertLess(prepare_index, run_index)
        self.assertLess(run_index, cleanup[1][0])
        prepare, run = job_steps[prepare_index], job_steps[run_index]
        self.assertEqual(scalar(prepare, "artifact_name"), "scenario-images-${{ matrix.id }}-${{ inputs.release }}${{ matrix.artifact_suffix }}")
        self.assertEqual(scalar(prepare, "if"), "matrix.requires_ceph")
        self.assertNotIn("continue-on-error", prepare)
        self.assertEqual(scalar(self.optional_entry, "category"), "optional")
        self.assertEqual(scalar(self.optional_entry, "optional_batch"),
                         "${{ inputs.failure_archive && 'failure_archive' || 'native_rgw_translation' }}")
        self.assertEqual(scalar(self.optional_entry, "rgw_image"), "${{ inputs.rgw_image }}")
        self.assertEqual(scalar(run, "CEPH_TEST_RGW_IMAGE"),
                         "${{ inputs.rgw_image || env.CEPH_TEST_RGW_IMAGE }}")
        # Role images come from the preparation step; the run step sets only
        # the RGW override and the matrix entry's network mode.
        self.assertEqual(re.findall(r"^          (CEPH_TEST_\w+):", run, re.M), ["CEPH_TEST_RGW_IMAGE", "CEPH_TEST_NETWORK"])
        self.assertEqual(scalar(run, "CEPH_TEST_NETWORK"), "${{ matrix.network }}")
        self.assertNotIn("run:", self.optional_entry)
        self.assertNotIn("quay.io/", block)

    def composite_script(self):
        self.assertEqual(scalar(self.action, "using"), "composite")
        action_steps = steps(self.action, indent=4)
        self.assertEqual(len(action_steps), 2)
        self.assertEqual(scalar(action_steps[0], "shell"), "bash")
        # The release and roles inputs reach the script only through the
        # environment. Without a roles input the action prepares all four.
        self.assertEqual(scalar(action_steps[0], "SCENARIO_RELEASE"), "${{ inputs.release }}")
        self.assertEqual(scalar(action_steps[0], "SCENARIO_ROLES"), "${{ inputs.roles }}")
        self.assertEqual(scalar(action_steps[0], "SCENARIO_DIGESTS"), "${{ inputs.digests }}")
        inputs = dict(blocks(self.action.split("\nruns:\n", 1)[0], r"^  ([\w-]+):$"))
        self.assertEqual(scalar(inputs["release"], "default"), "'" + runner.RELEASE + "'")
        self.assertEqual(scalar(inputs["roles"], "default"), "'" + ",".join(runner.ROLE_ENV) + "'")
        script = action_steps[0].split("      run: |\n", 1)[1]
        script = "\n".join(line[8:] for line in script.splitlines()) + "\n"
        logical_lines = script.replace("\\\n", "").splitlines()
        # Whitelist the actual two commands before executing a changed action.
        # This refuses future Docker/Go/build commands instead of running them.
        commands = [shlex.split(line) for line in logical_lines if line.strip()]
        self.assertEqual(commands, [
            ["python3", "$GITHUB_ACTION_PATH/../../scripts/run_image_matrix.py",
             "--variant", "official", "--layout", "roles", "--release", "$SCENARIO_RELEASE", "--prepare-only",
             "--roles", "$SCENARIO_ROLES", "--digests", "$SCENARIO_DIGESTS",
             "--platform", "linux/amd64", "--github-env", "$GITHUB_ENV",
             "--output-dir", "artifacts/scenario-images"],
            ["printf", "%s\\n", "SCENARIO_IMAGE_LAYOUT=roles", "SCENARIO_IMAGE_ROLES=$SCENARIO_ROLES", ">>", "$GITHUB_ENV"],
        ])
        upload = action_steps[1]
        self.assertEqual(scalar(upload, "if"), "always()")
        self.assertEqual(scalar(upload, "uses"), "actions/upload-artifact@v7")
        self.assertTrue(upload.startswith("    - name: Save selected image identities\n"))
        self.assertIn("name: ${{ inputs.artifact_name }}", upload)
        self.assertEqual(scalar(upload, "path"), "artifacts/scenario-images/")
        self.assertEqual(scalar(upload, "if-no-files-found"), "error")
        return script

    def run_composite(self, code, roles="essential,client,cephfs,rbd-aux,rgw", digests=""):
        script = self.composite_script()
        with tempfile.TemporaryDirectory(prefix="scenario images ") as directory:
            root = Path(directory)
            environment_file = root / "GitHub env with spaces"
            existing = "EXISTING_KEY=preserve-this\n"
            environment_file.write_text(existing)
            calls = root / "calls.json"
            stub = root / "python3"
            stub.write_text("#!" + sys.executable + "\n" +
                            "import json,os,pathlib,sys\n"
                            "args=sys.argv[1:]\n"
                            "pathlib.Path(os.environ['SCENARIO_TEST_CALLS']).write_text(json.dumps(args))\n"
                            "code=int(os.environ['SCENARIO_TEST_EXIT'])\n"
                            "if code == 0:\n"
                            " p=pathlib.Path(args[args.index('--github-env')+1])\n"
                            " with p.open('a') as out:\n"
                            "  for name,value in json.loads(os.environ['SCENARIO_TEST_IMAGES']).items():\n"
                            "   out.write(name+'='+value+'\\n')\n"
                            "sys.exit(code)\n")
            stub.chmod(0o700)
            env = self.environment({
                "PATH": str(root) + os.pathsep + os.environ.get("PATH", ""),
                "GITHUB_ACTION_PATH": str(ROOT / ".github/actions/scenario-images"),
                "GITHUB_ENV": str(environment_file), "SCENARIO_TEST_CALLS": str(calls),
                "SCENARIO_TEST_EXIT": str(code),
                "SCENARIO_TEST_IMAGES": json.dumps({runner.ROLE_ENV[role]: IMAGES[runner.ROLE_ENV[role]]
                                                    for role in roles.split(",")}),
                "SCENARIO_RELEASE": "19.2.5", "SCENARIO_ROLES": roles, "SCENARIO_DIGESTS": digests,
            })
            result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail",
                                     "-c", script], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=10)
            arguments = json.loads(calls.read_text())
            self.assertEqual(Path(arguments[0]).resolve(), ROOT / ".github/scripts/run_image_matrix.py")
            self.assertEqual(arguments[1:], [
                "--variant", "official", "--layout", "roles", "--release", "19.2.5", "--prepare-only",
                "--roles", roles, "--digests", digests,
                "--platform", "linux/amd64", "--github-env", str(environment_file),
                "--output-dir", "artifacts/scenario-images",
            ])
            return result, existing, environment_file.read_text()

    def test_composite_selects_layout_only_after_successful_preparation(self):
        for roles, names in (("essential,client,cephfs,rbd-aux,rgw", ROLE_ENV), ("essential", ROLE_ENV[:1])):
            with self.subTest(roles=roles):
                result, existing, actual = self.run_composite(0, roles)
                self.assertEqual(result.returncode, 0, result.stderr)
                role_lines = "".join(name + "=" + IMAGES[name] + "\n" for name in names)
                self.assertEqual(actual, existing + role_lines + "SCENARIO_IMAGE_LAYOUT=roles\n"
                                 + "SCENARIO_IMAGE_ROLES=" + roles + "\n")
                self.assertEqual(actual.count("SCENARIO_IMAGE_LAYOUT=roles\n"), 1)

    def test_composite_passes_campaign_digest_pins_as_one_argument(self):
        pins = "essential=sha256:" + "a" * 64 + ",client=sha256:" + "b" * 64
        result, _, _ = self.run_composite(0, "essential,client", pins)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_composite_failed_preparation_never_selects_roles(self):
        for code in (1, 17):
            with self.subTest(exit_code=code):
                result, existing, actual = self.run_composite(code)
                self.assertEqual(result.returncode, code)
                self.assertEqual(actual, existing)
                self.assertNotIn("SCENARIO_IMAGE_LAYOUT=", actual)
                self.assertNotIn("SCENARIO_IMAGE_ROLES=", actual)


if __name__ == "__main__":
    unittest.main()
