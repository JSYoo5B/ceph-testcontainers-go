"""Ryuk and test image preparation; never invokes Docker or Go."""

import argparse
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import in_test_images as prepare
import run_image_matrix as matrix
import tag_scenarios


OLD = "quay.io/ceph/ceph:v20.2.3@sha256:d195020de02512030118e772cef7859e92904e91eb4cb21acb503f8b94118137"
REAPER = "testcontainers/ryuk:0.14.0"
TIMEOUT = 'Error response from daemon: Get "https://registry-1.docker.io/v2/": net/http: TLS handshake timeout\n'


class Engine:
    """The go and docker commands of one preparation, answered from fields."""

    def __init__(self, root):
        self.root = Path(root)
        module = self.root / "testcontainers-go@v0.44.0/internal/config"
        module.mkdir(parents=True)
        (module / "config.go").write_text('package config\n\nconst ReaperDefaultImage = "' + REAPER + '"\n')
        self.module = module.parents[1]
        self.analysis = []
        self.missing = set()
        self.pulls = {}
        self.commands = []

    def __call__(self, command, timeout):
        self.commands.append(command)
        if command[:3] == ["go", "run", "-mod=readonly"]:
            arguments = dict(zip(command[4::2], command[5::2]))
            return subprocess.CompletedProcess(command, 0, json.dumps({
                "schema": prepare.ANALYSIS_SCHEMA, "package": arguments["-package"], "tags": arguments["-images"],
                "tests": arguments["-tests"].split(","), "images": self.analysis}), "")
        if command[:3] == ["go", "list", "-m"]:
            return subprocess.CompletedProcess(command, 0, str(self.module) + "\n", "")
        if command[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(command, 0, json.dumps({"OSType": "linux", "Architecture": "x86_64"}), "")
        if command[:3] == ["docker", "image", "inspect"]:
            reference = command[-1]
            if reference in self.missing:
                return subprocess.CompletedProcess(command, 1, "[]", "Error: No such image: " + reference)
            return subprocess.CompletedProcess(command, 0, json.dumps([{
                "Id": "sha256:" + format(abs(hash(reference)), "x").rjust(64, "0")[:64], "Os": "linux",
                "Architecture": "amd64", "RepoDigests": []}]), "")
        if command[:2] == ["docker", "pull"]:
            reference = command[-1]
            results = self.pulls.get(reference) or [(0, "pulled\n")]
            code, output = results.pop(0) if len(results) > 1 else results[0]
            if code == 0:
                self.missing.discard(reference)
            return subprocess.CompletedProcess(command, code, output, "")
        raise AssertionError("unexpected command: %r" % command)


class PreparationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.engine = Engine(self.root)
        self.compile = self.root / "compile"
        self.compile_report("./internal/integration", ["TestAuthCiphers", "TestOther"])
        self.output = self.root / "test-images"

    def compile_report(self, package, tests, passed=True):
        self.compile.mkdir(exist_ok=True)
        report = {"passed": passed, "profile": {"package": package, "tags": "integration,ci"}}
        if passed:
            report["binary"] = {"tests": tests}
        (self.compile / "compile-report.json").write_text(json.dumps(report))

    def use(self, kind, reference="", tests=("TestAuthCiphers",), position="harness/x.go:1"):
        self.engine.analysis.append({"kind": kind, "reference": reference, "declaration": "func X",
                                     "position": position, "tests": list(tests)})

    def run_preparation(self, tests="", release="20.2.4", roles="essential,client", layout="roles"):
        arguments = argparse.Namespace(compile=self.compile, tests=tests, release=release, roles=roles,
                                       directory=self.output)
        sleeps = []
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = prepare.prepare(arguments, environ={"SCENARIO_IMAGE_LAYOUT": layout} if layout else {},
                                   runner=self.engine, sleep=sleeps.append)
        self.sleeps = sleeps
        return code, json.loads((self.output / "summary.json").read_text())

    def references(self, summary):
        return [image["reference"] for image in summary["images"]]

    def test_reaper_and_named_images_are_prepared_and_the_role_fallback_is_skipped(self):
        self.use("image", OLD)
        self.use("image", matrix.DEFAULT_IMAGE, position="harness/services_integration.go:35")
        self.engine.missing |= {REAPER, OLD}
        code, summary = self.run_preparation()
        self.assertEqual(code, 0, summary)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["reaper"]["reference"], REAPER)
        self.assertEqual(self.references(summary), [OLD])
        self.assertEqual(summary["images"][0]["tests"], ["TestAuthCiphers"])
        self.assertRegex(summary["images"][0]["image_id"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual([item["reference"] for item in summary["skipped"]], [matrix.DEFAULT_IMAGE])
        self.assertEqual(sorted(summary["pulls"]), ["image-1", "reaper"])
        analysis = next(command for command in self.engine.commands if command[:2] == ["go", "run"])
        self.assertEqual(analysis[analysis.index("-tests") + 1], "TestAuthCiphers,TestOther")
        self.assertEqual(analysis[analysis.index("-with") + 1], ",".join(tag_scenarios.INTEGRATION_HARNESS))
        self.assertTrue((self.output / "pull-reaper.log").exists())

    def test_docker_only_job_prepares_ryuk_and_the_role_fallback(self):
        self.compile_report("./internal/dockerbridge", ["TestNetwork"])
        self.use("image", "busybox:1.37.0", tests=("TestNetwork",))
        self.use("image", matrix.DEFAULT_IMAGE, tests=("TestNetwork",))
        self.engine.missing.add(REAPER)
        code, summary = self.run_preparation(roles="", layout=None)
        self.assertEqual(code, 0, summary)
        self.assertEqual(self.references(summary), ["busybox:1.37.0", matrix.DEFAULT_IMAGE])
        self.assertEqual(summary["skipped"], [])
        analysis = next(command for command in self.engine.commands if command[:2] == ["go", "run"])
        self.assertNotIn("-with", analysis)

    def test_matrix_selection_narrows_the_analysed_tests(self):
        self.compile_report("./internal/integration", ["TestA", "TestB"])
        code, summary = self.run_preparation(tests="TestB")
        self.assertEqual(code, 0, summary)
        self.assertEqual(summary["tests"], ["TestB"])
        self.output = self.root / "other"
        code, summary = self.run_preparation(tests="TestC")
        self.assertEqual(code, 1)
        self.assertIn("not compiled", summary["error"])
        self.assertNotIn("reaper", summary)

    def test_run_time_prefix_is_the_official_image_of_the_release_by_tag(self):
        self.use("prefix", "quay.io/ceph/ceph:v")
        code, summary = self.run_preparation(release="19.2.5")
        self.assertEqual(code, 0, summary)
        tagged, _, digest = matrix.RELEASES["19.2.5"].partition("@")
        self.assertEqual(self.references(summary), [tagged])
        self.assertIn(digest, summary["images"][0]["note"])
        self.engine.analysis = []
        self.use("prefix", "ghcr.io/other/image:")
        self.output = self.root / "unknown"
        code, summary = self.run_preparation()
        self.assertEqual(code, 1)
        self.assertIn("cannot name", summary["error"])

    def test_pinned_role_file_prepares_the_declared_roles_of_every_release(self):
        self.use("role-pins")
        code, summary = self.run_preparation(roles="essential,client")
        self.assertEqual(code, 0, summary)
        expected = sorted(matrix.REGISTRY + "@" + matrix.published_role_pins(release)[role]
                          for release in matrix.RELEASES for role in ("essential", "client"))
        self.assertEqual(sorted(self.references(summary)), expected)

    def test_transport_failures_are_retried_and_classified(self):
        self.use("image", OLD)
        self.engine.missing |= {REAPER, OLD}
        self.engine.pulls[REAPER] = [(1, TIMEOUT), (0, "pulled\n")]
        self.engine.pulls[OLD] = [(1, TIMEOUT)]
        code, summary = self.run_preparation()
        self.assertEqual(code, 1)
        self.assertEqual([record["exit_code"] for record in summary["pulls"]["reaper"]], [1, 0])
        self.assertEqual(len(summary["pulls"]["image-1"]), 3)
        self.assertEqual(self.sleeps, [10, 10, 30])
        self.assertEqual(summary["classification"], "registry-transport")
        self.assertEqual(summary["registry_failure"]["image"], "image-1")
        self.assertEqual(summary["registry_failure"]["reference"], OLD)
        self.assertEqual(summary["registry_failure"]["phase"], "registry")
        self.assertFalse(summary["passed"])

    def test_registry_refusal_is_not_retried(self):
        self.engine.missing.add(REAPER)
        self.engine.pulls[REAPER] = [(1, "Error response from daemon: manifest unknown\n")]
        code, summary = self.run_preparation()
        self.assertEqual(code, 1)
        self.assertEqual(len(summary["pulls"]["reaper"]), 1)
        self.assertNotIn("classification", summary)
        self.assertEqual(self.sleeps, [])

    def test_receipts_and_analysis_are_validated(self):
        self.compile_report("./internal/integration", [], passed=False)
        code, summary = self.run_preparation()
        self.assertEqual(code, 1)
        self.assertIn("no built binary", summary["error"])
        self.compile_report("./internal/integration", ["TestA"])
        self.engine.analysis = [{"kind": "other", "tests": []}]
        self.output = self.root / "invalid"
        code, summary = self.run_preparation()
        self.assertEqual(code, 1)
        self.assertIn("invalid entry", summary["error"])

    def test_reaper_reference_needs_a_single_constant(self):
        self.assertEqual(prepare.reaper_reference(str(self.engine.module) + "\n"), REAPER)
        for source in ("", 'const ReaperDefaultImage = "testcontainers/ryuk"\n',
                       'const ReaperDefaultImage = "a:1"\nconst ReaperDefaultImage = "b:2"\n'):
            with self.subTest(source=source):
                (self.engine.module / prepare.REAPER_SOURCE).write_text("package config\n" + source)
                with self.assertRaisesRegex(ValueError, "ReaperDefaultImage"):
                    prepare.reaper_reference(str(self.engine.module))
        with self.assertRaisesRegex(ValueError, "no module directory"):
            prepare.reaper_reference("relative\n")


class WorkflowTests(unittest.TestCase):
    def test_every_runtime_job_prepares_test_images_before_the_native_step(self):
        text = (prepare.ROOT / ".github/workflows/tagged-runtime.yml").read_text()
        step = text.index("      - name: Prepare Ryuk and the test images\n")
        native = text.index("      - name: Native assertions and completion proof\n")
        roles = text.index("      - name: Resolve native published Ceph roles\n")
        self.assertLess(roles, step)
        self.assertLess(step, native)
        block = text[step:text.index("      - name:", step + 1)]
        self.assertNotIn("if:", block)
        self.assertIn("python3 .github/scripts/in_test_images.py --compile artifacts/scenario/compile", block)
        self.assertIn("SCENARIO_TEST_IMAGES_OUTCOME: ${{ steps.test_images.outcome }}", text)


if __name__ == "__main__":
    unittest.main()
