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


ROOT = Path(__file__).resolve().parents[2]
ROLES = ("CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE", "CEPH_TEST_RGW_IMAGE",
         "CEPH_TEST_MDS_IMAGE")
CONSUMERS = ("CEPH_TEST_RBD_CLIENT_IMAGE", "CEPH_TEST_VAULT_IMAGE")
IMAGES = {name: "sha256:" + digit * 64
          for name, digit in zip(ROLES, "1234")}
PRIMARY = (
    "scenario-default", "scenario-topology", "scenario-multicluster-topology",
    "scenario-cephfs-removal", "scenario-rbd-receivers",
    "scenario-topology-extensions", "scenario-cluster-fixtures",
    "scenario-cephfs-fixtures", "scenario-rados-fixtures",
    "scenario-rbd-fixtures", "scenario-rgw-fixtures", "scenario-rgw-sync-fixtures",
    "scenario-mirror-initial-daemons", "scenario-rbd-namespaces",
    "scenario-storage-bootstrap", "scenario-manager-bootstrap",
    "scenario-rbd-namespace-observation", "scenario-mds-bootstrap",
    "scenario-mds-replacement", "scenario-last-mds-replacement",
)
FIXTURES = frozenset(name for name in PRIMARY if name.endswith("-fixtures"))
DRY_RUN_TARGETS = (*PRIMARY, "scenario-rgw-sync-supported", "scenario-diagnostics")
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
        elif token == "-u" and index + 1 < len(prefix) and prefix[index + 1] in (*ROLES, *CONSUMERS):
            index += 2
        else:
            raise AssertionError("unapproved executable or option in scenario env prefix")


class ScenarioImageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.makefile = (ROOT / "Makefile").read_text()
        # GNU make may evaluate functions and recursive recipes during -n.
        check_dry_run_source(cls.makefile)
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        cls.jobs = dict(blocks(workflow.split("\njobs:\n", 1)[1],
                              r"^  ([\w-]+):$"))
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
        result = subprocess.run(["make", "--no-print-directory", "-n", target, *extra],
                                cwd=ROOT, env=env, capture_output=True, text=True,
                                timeout=10)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        commands = [shlex.split(line) for line in result.stdout.splitlines()
                    if " go test " in line]
        return result, commands

    def prepared_environment(self, command, env):
        # Execute the generated env prefix against Python, never the Go recipe.
        index = command.index("go")
        self.assertEqual(command[index:index + 2], ["go", "test"])
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
        for name in CONSUMERS:
            if target in FIXTURES:
                self.assertNotIn(name, actual)
                self.assertEqual(command[command.index(name) - 1], "-u")
            else:
                self.assertEqual(actual.get(name), env[name])
        self.assertEqual(actual.get("CGO_ENABLED"), "0")
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
                source = self.makefile.replace("scenario-rgw-sync-fixtures:\n",
                    "scenario-rgw-sync-fixtures:\nifeq (x,x)\n\t" + recursive + " other-goal\nendif\n", 1)
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
        for target in ("scenario-default", "scenario-rgw-fixtures"):
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
        for target in ("scenario-default", "scenario-rgw-fixtures"):
            for value in ("unknown", ""):
                with self.subTest(target=target, value=value):
                    result, commands = self.dry_run(
                        target, self.environment(), ("SCENARIO_IMAGE_LAYOUT=" + value,),
                        success=False)
                    self.assertIn("Unknown SCENARIO_IMAGE_LAYOUT", result.stderr)
                    self.assertFalse(commands)
                    self.assertEqual(result.stdout, "")

    def test_sync_shards_and_supported_aggregate_preserve_role_boundary(self):
        env = self.environment({"SCENARIO_IMAGE_LAYOUT": "roles"})
        _, supported = self.dry_run("scenario-rgw-sync-supported", env)
        self.assertEqual(len(supported), 2)
        for command in supported:
            self.assert_image_environment("scenario-rgw-sync-supported", command, env, True)
        for group in ("all", "policy", "account", "translation"):
            with self.subTest(group=group):
                _, commands = self.dry_run("scenario-rgw-sync-fixtures", env,
                                           ("SCENARIO_RGW_SYNC_GROUP=" + group,))
                self.assertEqual(len(commands), 2 if group == "all" else 1)
                for command in commands:
                    self.assert_image_environment("scenario-rgw-sync-fixtures", command, env, True)

    def test_every_primary_job_prepares_after_own_baseline_and_before_make(self):
        self.assertEqual({name for name in self.jobs if name.startswith("scenario-")},
                         set(PRIMARY))
        artifacts = []
        for name in PRIMARY:
            with self.subTest(job=name):
                job_steps = steps(self.jobs[name])
                prep = [i for i, step in enumerate(job_steps) if "uses: " + PREP in step]
                cleanups = [(i, step) for i, step in enumerate(job_steps)
                            if "uses: " + CLEANUP in step]
                self.assertEqual(len(prep), 1)
                self.assertEqual(len(cleanups), 2)
                (before_index, before), (after_index, after) = cleanups
                make = [i for i, step in enumerate(job_steps)
                        if re.search(r"(?m)^\s*make " + re.escape(name) + r"(?:\s|$)", step_run(step))]
                self.assertEqual(len(make), 1)
                self.assertLess(before_index, prep[0])
                self.assertLess(prep[0], make[0])
                self.assertLess(make[0], after_index)
                self.assertEqual(scalar(before, "id"), "runtime_cleanup_baseline")
                self.assertEqual(scalar(before, "phase"), "snapshot")
                self.assertEqual(scalar(after, "phase"), "check")
                self.assertEqual(scalar(before, "artifact_name"), scalar(after, "artifact_name"))
                self.assertEqual(scalar(after, "if"),
                                 "${{ always() && steps.runtime_cleanup_baseline.outcome == 'success' }}")
                preparation = job_steps[prep[0]]
                self.assertNotRegex(preparation, r"(?m)^\s+(?:if|continue-on-error):")
                artifacts.append(scalar(preparation, "artifact_name"))
        self.assertEqual(len(set(artifacts)), len(PRIMARY))

    def test_existing_compatibility_matrix_remains_twelve_native_cells(self):
        job = self.jobs["image-compatibility"]
        self.assertIn("variant: [official, debian, ubuntu]", job)
        self.assertIn("layout: [all, roles]", job)
        architectures = re.findall(r"- name: (amd64|arm64)\n\s+runner: ([\w.-]+)", job)
        self.assertEqual(architectures, [("amd64", "ubuntu-24.04"),
                                         ("arm64", "ubuntu-24.04-arm")])
        self.assertEqual(3 * 2 * len(architectures), 12)
        self.assertNotRegex(job, r"(?m)^\s+(?:include|exclude):")
        self.assertNotIn("uses: " + PREP, job)
        self.assertNotIn("--prepare-only", job)
        for option in ("--variant '${{ matrix.variant }}'", "--layout '${{ matrix.layout }}'",
                       "--platform 'linux/${{ matrix.architecture.name }}'"):
            self.assertIn(option, job)
        self.assertIn("python3 .github/scripts/run_image_matrix.py", job)

    def composite_script(self):
        self.assertEqual(scalar(self.action, "using"), "composite")
        action_steps = steps(self.action, indent=4)
        self.assertEqual(len(action_steps), 2)
        self.assertEqual(scalar(action_steps[0], "shell"), "bash")
        script = action_steps[0].split("      run: |\n", 1)[1]
        script = "\n".join(line[8:] for line in script.splitlines()) + "\n"
        logical_lines = script.replace("\\\n", "").splitlines()
        # Whitelist the actual two commands before executing a changed action.
        # This refuses future Docker/Go/build commands instead of running them.
        commands = [shlex.split(line) for line in logical_lines if line.strip()]
        self.assertEqual(commands, [
            ["python3", "$GITHUB_ACTION_PATH/../../scripts/run_image_matrix.py",
             "--variant", "official", "--layout", "roles", "--prepare-only",
             "--platform", "linux/amd64", "--github-env", "$GITHUB_ENV",
             "--output-dir", "artifacts/scenario-images"],
            ["printf", "%s\\n", "SCENARIO_IMAGE_LAYOUT=roles", ">>", "$GITHUB_ENV"],
        ])
        upload = action_steps[1]
        self.assertEqual(scalar(upload, "if"), "always()")
        self.assertEqual(scalar(upload, "uses"), "actions/upload-artifact@v7")
        self.assertTrue(upload.startswith("    - name: Save selected image identities\n"))
        self.assertIn("name: ${{ inputs.artifact_name }}", upload)
        self.assertEqual(scalar(upload, "path"), "artifacts/scenario-images/")
        self.assertEqual(scalar(upload, "if-no-files-found"), "error")
        return script

    def run_composite(self, code):
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
                "SCENARIO_TEST_EXIT": str(code), "SCENARIO_TEST_IMAGES": json.dumps(IMAGES),
            })
            result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail",
                                     "-c", script], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=10)
            arguments = json.loads(calls.read_text())
            self.assertEqual(Path(arguments[0]).resolve(), ROOT / ".github/scripts/run_image_matrix.py")
            self.assertEqual(arguments[1:], [
                "--variant", "official", "--layout", "roles", "--prepare-only",
                "--platform", "linux/amd64", "--github-env", str(environment_file),
                "--output-dir", "artifacts/scenario-images",
            ])
            return result, existing, environment_file.read_text()

    def test_composite_selects_layout_only_after_successful_preparation(self):
        result, existing, actual = self.run_composite(0)
        self.assertEqual(result.returncode, 0, result.stderr)
        role_lines = "".join(name + "=" + IMAGES[name] + "\n" for name in ROLES)
        self.assertEqual(actual, existing + role_lines + "SCENARIO_IMAGE_LAYOUT=roles\n")
        self.assertEqual(actual.count("SCENARIO_IMAGE_LAYOUT=roles\n"), 1)

    def test_composite_failed_preparation_never_selects_roles(self):
        for code in (1, 17):
            with self.subTest(exit_code=code):
                result, existing, actual = self.run_composite(code)
                self.assertEqual(result.returncode, code)
                self.assertEqual(actual, existing)
                self.assertNotIn("SCENARIO_IMAGE_LAYOUT=", actual)


if __name__ == "__main__":
    unittest.main()
