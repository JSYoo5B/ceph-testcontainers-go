"""Boundary tests for the matrix harness; never invoke Docker, Go, or make."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("run_image_matrix.py")
SPEC = importlib.util.spec_from_file_location("image_matrix", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
MODULE = "github.com/jsyoo5b/ceph-testcontainers-go"


def passing_log():
    return "".join("=== RUN   " + name + "\n--- PASS: " + name + " (1.23s)\n"
                   for name in runner.EXPECTED_TESTS) + "PASS\nok\t" + MODULE + "/internal/integration\t12.34s\n"


class Runtime:
    def __init__(self):
        self.info = {"OSType": "linux", "Architecture": "aarch64", "ServerVersion": "28.0.4"}
        self.inspect_overrides = {}
        self.missing = set()
        self.inspect_error = None
        self.pulled = set()
        self.capture_commands = []
        self.stream_commands = []
        self.make_environment = None
        self.make_log = passing_log()
        self.make_code = 0
        self.pull_code = 0
        self.go_code = 0

    def capture(self, command, **kwargs):
        self.capture_commands.append(command)
        if command == ["go", "version"]:
            return subprocess.CompletedProcess(command, self.go_code, "go version go1.25.0 darwin/arm64\n", "")
        if command == ["docker", "info", "--format", "{{json .}}"]:
            return subprocess.CompletedProcess(command, 0, json.dumps(self.info), "")
        if command[:4] == ["docker", "image", "inspect", "--"]:
            reference = command[4]
            if self.inspect_error:
                return subprocess.CompletedProcess(command, 1, "", self.inspect_error)
            if reference in self.missing and reference not in self.pulled:
                return subprocess.CompletedProcess(command, 1, "[]", "Error response from daemon: No such image: " + reference)
            platform = runner.native_platform(self.info)
            image = {"Id": "sha256:" + hashlib.sha256(reference.encode()).hexdigest(),
                     "Os": "linux", "Architecture": platform.split("/")[1],
                     "RepoDigests": [reference.split("@")[0] + "@sha256:" + "a" * 64]}
            image.update(self.inspect_overrides.get(reference, {}))
            return subprocess.CompletedProcess(command, 0, json.dumps([image]), "")
        raise AssertionError("unexpected command (no builds, prune, or Docker run permitted): " + repr(command))

    def popen(self, command, **kwargs):
        self.stream_commands.append(command)
        if command == ["make", "image-compatibility"]:
            self.make_environment = kwargs["env"].copy()
            output, code = self.make_log, self.make_code
        elif command[:3] == ["docker", "pull", "--platform"]:
            self.pulled.add(command[-1])
            output, code = "pulled\n", self.pull_code
        else:
            raise AssertionError("unexpected streaming command: " + repr(command))
        process = mock.Mock()
        process.stdout = io.StringIO(output)
        process.wait.return_value = code
        return process


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "module"
        self.root.mkdir()
        self.write("Makefile", "image-compatibility:\n\tgo test\n")
        self.write("go.mod", "module " + MODULE + "\n\ngo 1.25.0\n")
        self.write("go.sum", "example\n")
        self.write(".github/scripts/run_image_matrix.py", SCRIPT.read_text())
        self.write("ceph/ceph.go", 'package ceph\nconst DefaultImage = "' + runner.OFFICIAL_IMAGE + '"\n')
        self.write("internal/integration/lifecycle.go", "package integration\n")
        self.write("ceph/internal/scripts/control.sh", "#!/bin/sh\n")
        self.runtime = Runtime()
        self.output = self.root / "artifacts" / "result"

    def write(self, relative, value):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        return path

    def invoke(self, arguments=(), env=None, missing_executable=None):
        with mock.patch.object(runner, "ROOT", self.root), \
                mock.patch.object(runner.shutil, "which", side_effect=lambda name: None if name == missing_executable else "/tools/" + name), \
                mock.patch.object(runner.subprocess, "run", side_effect=self.runtime.capture), \
                mock.patch.object(runner.subprocess, "Popen", side_effect=self.runtime.popen), \
                mock.patch.dict(os.environ, env or {}, clear=True), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return runner.main(["--output-dir", str(self.output), *arguments])

    def summary(self):
        return json.loads((self.output / "summary.json").read_text())

    def test_default_matches_official_image_and_all_pins_four_components(self):
        self.assertEqual(runner.ROLE_ENV, {"control": "CEPH_TEST_IMAGE", "osd": "CEPH_TEST_OSD_IMAGE",
                                          "rgw": "CEPH_TEST_RGW_IMAGE", "mds": "CEPH_TEST_MDS_IMAGE"})
        original = (SCRIPT.parents[2] / "ceph/ceph.go").read_text()
        self.assertIn('const DefaultImage = "' + runner.OFFICIAL_IMAGE + '"', original)
        self.assertEqual(self.invoke(env={name: "unrelated:mutable" for name in runner.ROLE_ENV.values()}), 0)
        report = self.summary()
        self.assertEqual(report["images"]["all"]["reference"], runner.OFFICIAL_IMAGE)
        self.assertEqual(report["images"]["all"]["repo_digests"],
                         [runner.OFFICIAL_IMAGE.split("@")[0] + "@sha256:" + "a" * 64])
        self.assertEqual(report["platform"], "linux/arm64")
        image_id = report["images"]["all"]["image_id"]
        for name in runner.ROLE_ENV.values():
            self.assertEqual(self.runtime.make_environment[name], image_id)
        self.assertEqual(report["image_builds"], 0)
        self.assertEqual(report["runtime_result"], "passed")
        self.assertEqual(report["test_exit_code"], 0)
        self.assertEqual(len(report["tests"]["results"]), 9)
        self.assertFalse(self.runtime.pulled)

    def test_all_six_reference_mappings(self):
        for variant in ("official", "debian", "ubuntu"):
            for layout in ("all", "roles"):
                with self.subTest(variant=variant, layout=layout):
                    refs = runner.image_references(variant, layout, "20.2.4")
                    if layout == "all":
                        self.assertEqual(set(refs), {"all"})
                        expected = runner.OFFICIAL_IMAGE if variant == "official" else runner.REGISTRY + ":" + variant + "-20.2.4-all"
                        self.assertEqual(refs["all"], expected)
                    else:
                        self.assertEqual(set(refs), {"control", "osd", "rgw", "mds"})
                        for role, reference in refs.items():
                            self.assertEqual(reference, runner.REGISTRY + ":" + variant + "-20.2.4-" + role)

    def test_roles_freeze_four_component_ids(self):
        self.assertEqual(self.invoke(["--variant", "official", "--layout", "roles", "--platform", "linux/arm64"],
                                     env={"MAKEFLAGS": "-n OTHER=wrong", "MFLAGS": "-s", "GNUMAKEFLAGS": "-n",
                                          "MAKEFILES": "/unrelated/makefile", "MAKEOVERRIDES": "CEPH_TEST_IMAGE=other",
                                          "GOFLAGS": "-overlay=/external/source.json",
                                          "GOWORK": "/external/go.work", "GOENV": "/external/goenv",
                                          "DOCKER_HOST": "unix:///custom/socket", "TESTCONTAINERS_HOST_OVERRIDE": "localhost"}), 0)
        report = self.summary()
        self.assertEqual(len(report["images"]), 4)
        self.assertEqual(len([command for command in self.runtime.capture_commands if command[1:3] == ["image", "inspect"]]), 4)
        for role in ("control", "osd", "rgw", "mds"):
            self.assertEqual(self.runtime.make_environment[runner.ROLE_ENV[role]], report["images"][role]["image_id"])
        self.assertEqual(self.runtime.make_environment["CGO_ENABLED"], "0")
        self.assertEqual(self.runtime.make_environment["GOWORK"], "off")
        self.assertEqual(self.runtime.make_environment["GOFLAGS"], "-mod=readonly")
        self.assertEqual(self.runtime.make_environment["GOENV"], "/external/goenv")
        self.assertEqual(self.runtime.make_environment["DOCKER_HOST"], "unix:///custom/socket")
        self.assertEqual(self.runtime.make_environment["TESTCONTAINERS_HOST_OVERRIDE"], "localhost")
        for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEFILES", "MAKEOVERRIDES"):
            self.assertNotIn(name, self.runtime.make_environment)
        self.assertFalse(any(runner.OFFICIAL_IMAGE in command for command in self.runtime.capture_commands))

    def test_only_missing_image_is_pulled_for_native_platform_and_reinspected(self):
        reference = runner.REGISTRY + ":debian-20.2.4-all"
        self.runtime.missing.add(reference)
        self.assertEqual(self.invoke(["--variant", "debian"]), 0)
        self.assertEqual(self.runtime.stream_commands, [["docker", "pull", "--platform", "linux/arm64", reference],
                                                        ["make", "image-compatibility"]])
        inspected = [command for command in self.runtime.capture_commands if command[1:3] == ["image", "inspect"]]
        self.assertEqual(inspected, [["docker", "image", "inspect", "--", reference]] * 2)
        self.assertTrue((self.output / "pull-all.log").exists())

    def test_daemon_inspect_failure_is_not_misclassified_as_absent_image(self):
        self.runtime.inspect_error = "permission denied while trying to connect to Docker"
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertEqual(self.summary()["runtime_result"], "not_run")
        self.assertIn("absence was not confirmed", self.summary()["error"])

    def test_platform_mismatch_never_pulls_or_runs_tests(self):
        self.assertEqual(self.invoke(["--platform", "linux/amd64"]), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertFalse(any(command[1:3] == ["image", "inspect"] for command in self.runtime.capture_commands))
        self.assertIn("emulation", self.summary()["error"])

    def test_supported_engine_aliases_and_invalid_info_fail_closed(self):
        for arch, expected in (("amd64", "linux/amd64"), ("x86_64", "linux/amd64"),
                               ("arm64", "linux/arm64"), ("aarch64", "linux/arm64")):
            self.assertEqual(runner.native_platform({"OSType": "linux", "Architecture": arch}), expected)
        for value in (None, [], {"OSType": "windows", "Architecture": "amd64"},
                      {"OSType": "linux", "Architecture": "ppc64le"}, {"OSType": "linux", "Architecture": []}):
            with self.subTest(info=value), self.assertRaises(RuntimeError):
                runner.native_platform(value)

    def test_wrong_image_architecture_rejected_before_make(self):
        self.runtime.inspect_overrides[runner.OFFICIAL_IMAGE] = {"Architecture": "amd64"}
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIn("image platform differs", self.summary()["error"])

    def test_native_amd64_explicit_platform_runs_without_emulation(self):
        self.runtime.info["Architecture"] = "x86_64"
        self.assertEqual(self.invoke(["--variant", "ubuntu", "--platform", "linux/amd64"]), 0)
        self.assertEqual(self.summary()["platform"], "linux/amd64")

    def test_nonimmutable_image_id_rejected(self):
        self.runtime.inspect_overrides[runner.OFFICIAL_IMAGE] = {"Id": "quay.io/ceph/ceph:v20.2.4"}
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIn("immutable local image ID", self.summary()["error"])

    def test_registry_digests_are_optional_but_never_manufactured_or_malformed(self):
        self.runtime.inspect_overrides[runner.OFFICIAL_IMAGE] = {"RepoDigests": None}
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.summary()["images"]["all"]["repo_digests"], [])
        for value in ({}, "sha256:" + "a" * 64, ["registry:tag"], [None]):
            with self.subTest(value=value):
                self.output = self.root / "artifacts" / ("invalid-digest-" + str(len(self.runtime.capture_commands)))
                self.runtime.inspect_overrides[runner.OFFICIAL_IMAGE] = {"RepoDigests": value}
                count = len(self.runtime.stream_commands)
                self.assertEqual(self.invoke(), 1)
                self.assertEqual(len(self.runtime.stream_commands), count)
                self.assertIn("invalid registry digest metadata", self.summary()["error"])

    def test_partial_image_resolution_is_retained_on_later_failure(self):
        reference = runner.REGISTRY + ":ubuntu-20.2.4-rgw"
        self.runtime.inspect_overrides[reference] = {"Os": "windows"}
        self.assertEqual(self.invoke(["--variant", "ubuntu", "--layout", "roles"]), 1)
        self.assertEqual(set(self.summary()["images"]), {"control", "osd"})
        self.assertFalse(self.runtime.stream_commands)

    def test_failed_pull_does_not_start_make(self):
        self.runtime.missing.add(runner.OFFICIAL_IMAGE)
        self.runtime.pull_code = 7
        self.assertEqual(self.invoke(), 1)
        self.assertEqual(len(self.runtime.stream_commands), 1)
        self.assertIn("exit 7", self.summary()["error"])

    def test_missing_caller_go_is_error_and_has_report_without_docker(self):
        self.assertEqual(self.invoke(missing_executable="go"), 1)
        self.assertFalse(self.runtime.capture_commands)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIn("go is required", self.summary()["error"])

    def test_existing_output_never_overwrites_or_calls_tools(self):
        self.output.mkdir(parents=True)
        sentinel = self.output / "summary.json"
        sentinel.write_text("original")
        self.assertEqual(self.invoke(), 2)
        self.assertEqual(sentinel.read_text(), "original")
        self.assertFalse(self.runtime.capture_commands)
        self.assertFalse(self.runtime.stream_commands)

    def test_make_failure_propagates_its_exit_even_with_complete_pass_log(self):
        self.runtime.make_code = 7
        self.assertEqual(self.invoke(), 7)
        self.assertEqual(self.summary()["test_exit_code"], 7)
        self.assertFalse(self.summary()["passed"])
        self.assertEqual(self.summary()["runtime_result"], "failed")

    def test_make_signal_exit_is_normalized_and_never_success(self):
        self.runtime.make_code = -15
        self.assertEqual(self.invoke(), 143)
        self.assertEqual(self.summary()["test_exit_code"], -15)
        self.assertEqual(self.summary()["exit_code"], 143)

    def test_make_zero_with_missing_top_test_is_failure(self):
        name = runner.EXPECTED_TESTS[-1]
        self.runtime.make_log = passing_log().replace("=== RUN   " + name + "\n--- PASS: " + name + " (1.23s)\n", "")
        self.assertEqual(self.invoke(), 1)
        self.assertEqual(self.summary()["test_exit_code"], 0)
        self.assertEqual(self.summary()["tests"]["missing"], [name])

    def test_skip_or_failed_child_is_rejected_despite_nine_top_passes(self):
        for status in ("SKIP", "FAIL"):
            with self.subTest(status=status):
                self.output = self.root / "artifacts" / status.lower()
                self.runtime.make_log = passing_log() + "    --- " + status + ": TestClusterLifecycle/client (0.00s)\n"
                self.assertEqual(self.invoke(), 1)
                key = "skipped" if status == "SKIP" else "failed"
                self.assertEqual(self.summary()["tests"][key], ["TestClusterLifecycle/client"])

    def test_completion_parser_rejects_cached_missing_start_duplicate_and_foreign_scope(self):
        good = passing_log()
        variants = {
            "cached": good.replace("12.34s", "(cached)"),
            "no_package": good.replace("ok\t" + MODULE + "/internal/integration\t12.34s\n", ""),
            "no_start": good.replace("=== RUN   " + runner.EXPECTED_TESTS[0] + "\n", ""),
            "duplicate": good + "--- PASS: " + runner.EXPECTED_TESTS[0] + " (1.23s)\n",
            "foreign": good + "=== RUN   TestUnselected\n--- PASS: TestUnselected (0.01s)\n",
            "package_fail": good + "FAIL\n",
            "no_tests": good.replace("12.34s", "12.34s [no tests to run]"),
        }
        for label, log in variants.items():
            with self.subTest(label=label):
                path = self.root / (label + ".log")
                path.write_text(log)
                self.assertFalse(runner.completed_tests(path, MODULE)["passed"])

    def test_source_manifest_binds_go_shell_and_target_and_changes_with_input(self):
        manifest = runner.source_manifest(self.root)
        paths = {entry["path"] for entry in manifest["files"]}
        self.assertTrue({"Makefile", "go.mod", "go.sum", "ceph/ceph.go", "ceph/internal/scripts/control.sh"}.issubset(paths))
        self.write("ceph/internal/scripts/control.sh", "#!/bin/sh\nprintf changed\n")
        self.assertNotEqual(manifest["sha256"], runner.source_manifest(self.root)["sha256"])
        self.assertEqual(self.invoke(env={"GITHUB_SHA": "a" * 40}), 0)
        self.assertEqual(self.summary()["source"]["declared_github_revision"], "a" * 40)

    def test_symlink_source_input_is_rejected_without_running_tools(self):
        path = self.root / "ceph" / "ceph.go"
        path.unlink()
        path.symlink_to(self.write("external.go", "package ceph\n"))
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.capture_commands)

    def test_unrecognized_release_and_platform_are_cli_errors(self):
        for arguments in (("--release", "20.2.5"), ("--platform", "linux/arm/v7"), ("--variant", "other")):
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                runner.parse_arguments(arguments)
            self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
