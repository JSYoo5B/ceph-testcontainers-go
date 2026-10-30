"""Boundary tests for the matrix harness; never invoke Docker, Go, or make."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("run_image_matrix.py")
SPEC = importlib.util.spec_from_file_location("image_matrix", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
MODULE = "github.com/JSYoo5B/ceph-testcontainers-go"


def passing_log():
    return "".join("=== RUN   " + name + "\n--- PASS: " + name + " (1.23s)\n"
                   for name in runner.EXPECTED_TESTS) + "".join("PASS\nok\t" + MODULE + "/" + package + "\t12.34s\n"
                                                                 for package in runner.EXPECTED_PACKAGES)


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
        self.write(".github/ceph-role-images.json", (SCRIPT.parents[2] / ".github/ceph-role-images.json").read_text())
        self.write("ceph/ceph.go", 'package ceph\nconst DefaultImage = "' + runner.DEFAULT_IMAGE + '"\n')
        self.write("internal/integration/lifecycle.go", "package integration\n")
        self.write("ceph/internal/scripts/essential.sh", "#!/bin/sh\n")
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

    def test_aggregate_pins_all_roles(self):
        self.assertEqual(self.invoke(env={name: "unrelated:mutable" for name in runner.ROLE_ENV.values()}), 0)
        report = self.summary()
        self.assertEqual(report["images"]["all"]["reference"], runner.DEFAULT_IMAGE)
        self.assertEqual(report["images"]["all"]["repo_digests"],
                         [runner.DEFAULT_IMAGE.split("@")[0] + "@sha256:" + "a" * 64])
        self.assertEqual(report["platform"], "linux/arm64")
        image_id = report["images"]["all"]["image_id"]
        for name in runner.ROLE_ENV.values():
            self.assertEqual(self.runtime.make_environment[name], image_id)
        self.assertEqual(report["image_builds"], 0)
        self.assertEqual(report["runtime_result"], "passed")
        self.assertEqual(report["test_exit_code"], 0)
        self.assertEqual(len(report["tests"]["results"]), 9)
        self.assertFalse(self.runtime.pulled)

    def test_role_preparation_exports_only_selected_ids_and_contract(self):
        github_env = self.root / "github-env"
        self.assertEqual(self.invoke(["--layout", "roles", "--prepare-only",
                                      "--roles", "essential,client", "--github-env", str(github_env)]), 0)
        report = self.summary()
        self.assertEqual(set(report["images"]), {"essential", "client"})
        prepared = report["prepared_environment"]
        self.assertEqual(set(prepared), {"CEPH_TEST_ESSENTIAL_IMAGE", "CEPH_TEST_CLIENT_IMAGE"})
        self.assertEqual(dict(line.split("=", 1) for line in github_env.read_text().splitlines()), prepared)
        self.assertTrue(report["preparation_passed"])
        self.assertEqual(report["runtime_result"], "not_run")
        self.assertIsNone(self.runtime.make_environment)
        self.assertTrue(all("@sha256:" in item["reference"] for item in report["images"].values()))

    def test_all_six_reference_mappings(self):
        for variant in ("official", "debian", "ubuntu"):
            for layout in ("all", "roles"):
                with self.subTest(variant=variant, layout=layout):
                    refs = runner.image_references(variant, layout, "20.2.4")
                    if layout == "all":
                        self.assertEqual(set(refs), {"all"})
                        expected = runner.REGISTRY + "@" + runner.published_role_pins("20.2.4", variant)["all"]
                        self.assertEqual(refs["all"], expected)
                    else:
                        self.assertEqual(set(refs), set(runner.ROLE_ENV))
                        for role, reference in refs.items():
                            self.assertEqual(reference, runner.REGISTRY + "@" + runner.published_role_pins("20.2.4", variant)[role])

    def test_roles_freeze_five_role_ids(self):
        self.assertEqual(self.invoke(["--variant", "official", "--layout", "roles", "--platform", "linux/arm64"],
                                     env={"MAKEFLAGS": "-n OTHER=wrong", "MFLAGS": "-s", "GNUMAKEFLAGS": "-n",
                                          "MAKEFILES": "/unrelated/makefile", "MAKEOVERRIDES": "CEPH_TEST_ESSENTIAL_IMAGE=other",
                                          "GOFLAGS": "-overlay=/external/source.json",
                                          "GOWORK": "/external/go.work", "GOENV": "/external/goenv",
                                          "DOCKER_HOST": "unix:///custom/socket", "TESTCONTAINERS_HOST_OVERRIDE": "localhost"}), 0)
        report = self.summary()
        self.assertEqual(len(report["images"]), 5)
        self.assertEqual(len([command for command in self.runtime.capture_commands if command[1:3] == ["image", "inspect"]]), 5)
        for role in tuple(runner.ROLE_ENV):
            self.assertEqual(self.runtime.make_environment[runner.ROLE_ENV[role]], report["images"][role]["image_id"])
        self.assertEqual(self.runtime.make_environment["CGO_ENABLED"], "0")
        self.assertEqual(self.runtime.make_environment["GOWORK"], "off")
        self.assertEqual(self.runtime.make_environment["GOFLAGS"], "-mod=readonly")
        self.assertEqual(self.runtime.make_environment["GOENV"], "/external/goenv")
        self.assertEqual(self.runtime.make_environment["DOCKER_HOST"], "unix:///custom/socket")
        self.assertEqual(self.runtime.make_environment["TESTCONTAINERS_HOST_OVERRIDE"], "localhost")
        for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEFILES", "MAKEOVERRIDES"):
            self.assertNotIn(name, self.runtime.make_environment)
        self.assertFalse(any(runner.DEFAULT_IMAGE in command for command in self.runtime.capture_commands))

    def test_prepare_roles_exports_only_selected_ids_after_all_images_validate(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        self.runtime.info["ID"] = "native-engine"
        self.runtime.go_code = 9
        original_capture = self.runtime.capture

        def capture(command, **kwargs):
            self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
            return original_capture(command, **kwargs)

        self.runtime.capture = capture
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(env_file)],
                                     env={"GITHUB_SHA": "a" * 40, "GOFLAGS": "-overlay=unrelated"},
                                     missing_executable="go"), 0)
        report = self.summary()
        self.assertEqual(report["scope"], "image_preparation")
        self.assertTrue(report["preparation_passed"])
        self.assertFalse(report["passed"])
        self.assertEqual(report["runtime_result"], "not_run")
        self.assertIsNone(report["test_exit_code"])
        self.assertIsNone(report["test_command"])
        self.assertNotIn("tests", report)
        self.assertNotIn("go_version", report)
        self.assertNotIn("test_environment", report)
        self.assertEqual(report["docker"]["ID"], "native-engine")
        self.assertEqual(report["source"]["declared_github_revision"], "a" * 40)
        self.assertEqual(len(report["images"]), 5)
        self.assertEqual(report["image_builds"], 0)
        self.assertGreaterEqual(report["image_preparation_seconds"], 0)
        for command in report["commands"]:
            self.assertGreaterEqual(command["seconds"], 0)
            self.assertEqual(command["argv"][0], "docker")
        self.assertEqual(len(self.runtime.capture_commands), 6)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIsNone(self.runtime.make_environment)
        self.assertFalse((self.output / "integration.log").exists())
        expected = {runner.ROLE_ENV[role]: image["image_id"] for role, image in report["images"].items()}
        lines = env_file.read_text().splitlines()
        self.assertEqual(lines[0], "EXISTING=keep")
        self.assertEqual(dict(line.split("=", 1) for line in lines[1:]), expected)
        self.assertEqual(len(lines), 6)
        self.assertEqual(report["prepared_environment"], expected)
        self.assertEqual(report["github_env"]["appended_keys"], list(runner.ROLE_ENV.values()))

    def test_prepare_late_role_failure_never_appends_environment(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        reference = runner.image_references("official", "roles", "20.2.4")["cephfs"]
        self.runtime.inspect_overrides[reference] = {"Architecture": "amd64"}
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(env_file)]), 1)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        report = self.summary()
        self.assertEqual(set(report["images"]), {"essential", "client"})
        self.assertFalse(report["preparation_passed"])
        self.assertFalse(report["passed"])
        self.assertEqual(report["runtime_result"], "not_run")
        self.assertNotIn("github_env", report)
        self.assertFalse(self.runtime.stream_commands)

    def test_prepare_absent_role_only_pulls_native_image_and_never_make(self):
        reference = runner.image_references("official", "roles", "20.2.4")["client"]
        self.runtime.missing.add(reference)
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles"], missing_executable="make"), 0)
        self.assertEqual(self.runtime.stream_commands,
                         [["docker", "pull", "--platform", "linux/arm64", reference]])
        inspected = [command[-1] for command in self.runtime.capture_commands
                     if command[1:3] == ["image", "inspect"]]
        self.assertEqual(inspected.count(reference), 2)
        self.assertEqual(len(inspected), 6)
        self.assertEqual(set(self.summary()["prepared_environment"]), set(runner.ROLE_ENV.values()))
        self.assertFalse((self.output / "integration.log").exists())

    def test_essential_only_batch_never_touches_other_roles(self):
        # An RGW pull that the registry refuses fails a four-role
        # preparation, but a batch that declares only the essential role never
        # asks for that image.
        env_file = self.write("github-env", "EXISTING=keep\n")
        rgw = runner.image_references("official", "roles", "20.2.4")["rgw"]
        self.runtime.missing.add(rgw)
        self.runtime.pull_code = 1
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(env_file)]), 1)
        self.assertEqual(self.summary()["error"], "rgw image pull failed (exit 1)")
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")

        self.output = self.root / "artifacts" / "essential-only"
        self.runtime.capture_commands.clear()
        self.runtime.stream_commands.clear()
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--roles", "essential",
                                      "--github-env", str(env_file)]), 0)
        report = self.summary()
        essential = runner.image_references("official", "roles", "20.2.4")["essential"]
        self.assertEqual(report["roles"], ["essential"])
        self.assertEqual(set(report["images"]), {"essential"})
        self.assertEqual([command[-1] for command in self.runtime.capture_commands
                          if command[1:3] == ["image", "inspect"]], [essential])
        self.assertFalse(self.runtime.stream_commands)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\nCEPH_TEST_ESSENTIAL_IMAGE=" + report["images"]["essential"]["image_id"] + "\n")
        self.assertEqual(report["github_env"]["appended_keys"], ["CEPH_TEST_ESSENTIAL_IMAGE"])

    def test_declared_roles_keep_their_order_and_fail_on_a_required_image(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--roles", "cephfs,essential"]), 0)
        self.assertEqual(self.summary()["roles"], ["essential", "cephfs"])
        self.assertEqual(list(self.summary()["prepared_environment"]), ["CEPH_TEST_ESSENTIAL_IMAGE", "CEPH_TEST_CEPHFS_IMAGE"])
        essential = runner.image_references("official", "roles", "20.2.4")["essential"]
        for name, change in (("absent", lambda: (self.runtime.missing.add(essential), setattr(self.runtime, "pull_code", 1))),
                             ("wrong platform", lambda: self.runtime.inspect_overrides.update({essential: {"Architecture": "amd64"}}))):
            with self.subTest(essential=name):
                self.runtime = Runtime()
                change()
                self.output = self.root / "artifacts" / name.replace(" ", "-")
                self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--roles", "essential",
                                              "--github-env", str(env_file)]), 1)
                self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
                self.assertFalse(self.summary()["preparation_passed"])
                self.assertEqual(self.summary()["images"], {})

    def test_roles_are_validated_before_tools_or_output(self):
        for arguments in (["--prepare-only", "--layout", "roles", "--roles", "client"],
                          ["--prepare-only", "--layout", "roles", "--roles", "essential,essential"],
                          ["--prepare-only", "--layout", "roles", "--roles", "essential,unknown"],
                          ["--prepare-only", "--layout", "roles", "--roles", ""],
                          ["--prepare-only", "--layout", "all", "--roles", "essential"],
                          ["--layout", "roles", "--roles", "essential"]):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit) as error:
                self.invoke(arguments)
            self.assertEqual(error.exception.code, 2)
            self.assertFalse(self.output.exists())
            self.assertFalse(self.runtime.capture_commands)
        with self.assertRaises(ValueError):
            runner.image_references("official", "all", "20.2.4", ("essential",))

    def test_digest_pins_pull_the_pinned_manifest_of_each_role(self):
        # A campaign pins the role tags once, so that every attempt runs the
        # same manifests even if a tag moves during the campaign.
        essential, client = "sha256:" + "c" * 64, "sha256:" + "d" * 64
        pinned = runner.REGISTRY + "@" + essential
        self.runtime.missing.add(pinned)
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--roles", "essential",
                                      "--digests", "essential=" + essential + ",client=" + client]), 0)
        self.assertEqual(self.runtime.stream_commands, [["docker", "pull", "--platform", "linux/arm64", pinned]])
        report = self.summary()
        self.assertEqual(report["images"]["essential"]["reference"], pinned)
        self.assertEqual(report["pinned_digests"], {"essential": essential, "client": client})
        # Without pins the tags are pulled as before.
        self.assertEqual(runner.pin_references({"essential": "r:t"}, {}), {"essential": "r:t"})

    def test_digest_pins_are_validated_before_tools_or_output(self):
        digest = "sha256:" + "e" * 64
        for value in ("essential=" + digest[:-1], "unknown=" + digest, "essential=" + digest + ",essential=" + digest,
                      "essential", "essential=sha512:" + "e" * 64):
            with self.subTest(digests=value), self.assertRaises(SystemExit) as error:
                self.invoke(["--prepare-only", "--layout", "roles", "--digests", value])
            self.assertEqual(error.exception.code, 2)
            self.assertFalse(self.output.exists())
            self.assertFalse(self.runtime.capture_commands)
        with self.assertRaises(SystemExit):
            self.invoke(["--prepare-only", "--layout", "all", "--digests", "essential=" + digest])
        self.assertEqual(runner.parse_digests(""), {})

    def test_export_requires_exactly_the_declared_roles(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        image = "sha256:" + "a" * 64
        four = {name: image for name in runner.ROLE_ENV.values()}
        for environment, roles in ((four, ("essential",)), ({}, ("essential",)),
                                   ({"CEPH_TEST_CLIENT_IMAGE": image}, ("client",)),
                                   ({"CEPH_TEST_ESSENTIAL_IMAGE": image}, ("essential", "cephfs"))):
            with self.subTest(environment=environment, roles=roles), self.assertRaises(RuntimeError):
                runner.append_role_environment(env_file, environment, roles)
            self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        runner.append_role_environment(env_file, {"CEPH_TEST_ESSENTIAL_IMAGE": image}, ("essential",))
        self.assertEqual(env_file.read_text(), "EXISTING=keep\nCEPH_TEST_ESSENTIAL_IMAGE=" + image + "\n")

    def test_prepare_platform_mismatch_never_exports_or_inspects(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        self.assertEqual(self.invoke(["--prepare-only", "--platform", "linux/amd64",
                                     "--github-env", str(env_file)]), 1)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        self.assertEqual(self.runtime.capture_commands, [["docker", "info", "--format", "{{json .}}"]])
        self.assertFalse(self.runtime.stream_commands)
        self.assertFalse(self.summary()["preparation_passed"])

    def test_github_env_is_rejected_without_prepare_before_tools_or_output(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        with self.assertRaises(SystemExit) as error:
            self.invoke(["--github-env", str(env_file)])
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        self.assertFalse(self.output.exists())
        self.assertFalse(self.runtime.capture_commands)
        self.assertFalse(self.runtime.stream_commands)

    def test_prepare_does_not_implicitly_write_inherited_github_env(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        self.assertEqual(self.invoke(["--prepare-only"], env={"GITHUB_ENV": str(env_file)}), 0)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        self.assertNotIn("github_env", self.summary())
        self.assertEqual(len(set(self.summary()["prepared_environment"].values())), 1)

    def test_export_rejects_other_keys_or_nonimmutable_values_without_appending(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        valid = {name: "sha256:" + "a" * 64 for name in runner.ROLE_ENV.values()}
        invalid = [valid | {"GOFLAGS": "-overlay=unrelated"},
                   {key: value for key, value in valid.items() if key != "CEPH_TEST_CEPHFS_IMAGE"},
                   valid | {"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:" + "a" * 64 + "\nOTHER=bad"},
                   valid | {"CEPH_TEST_ESSENTIAL_IMAGE": None}]
        for environment in invalid:
            with self.subTest(environment=environment), self.assertRaises(RuntimeError):
                runner.append_role_environment(env_file, environment)
            self.assertEqual(env_file.read_text(), "EXISTING=keep\n")

    def test_prepare_invalid_digest_metadata_never_appends_environment(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        reference = runner.image_references("official", "roles", "20.2.4")["cephfs"]
        self.runtime.inspect_overrides[reference] = {"RepoDigests": [reference + "@mutable"]}
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(env_file)]), 1)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        self.assertIn("invalid registry digest metadata", self.summary()["error"])
        self.assertNotIn("github_env", self.summary())

    def test_prepare_environment_write_failure_is_reported_without_runtime_success(self):
        self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(self.root)]), 1)
        report = self.summary()
        self.assertEqual(len(report["images"]), 5)
        self.assertFalse(report["preparation_passed"])
        self.assertFalse(report["passed"])
        self.assertEqual(report["runtime_result"], "not_run")
        self.assertNotIn("github_env", report)

    def test_environment_partial_write_is_rolled_back(self):
        env_file = self.write("github-env", "EXISTING=keep\n")
        original_write = os.write

        def partial_write(descriptor, content):
            return original_write(descriptor, content[:10])

        with mock.patch.object(runner.os, "write", side_effect=partial_write):
            self.assertEqual(self.invoke(["--prepare-only", "--layout", "roles", "--github-env", str(env_file)]), 1)
        self.assertEqual(env_file.read_text(), "EXISTING=keep\n")
        self.assertFalse(self.summary()["preparation_passed"])
        self.assertEqual(self.summary()["runtime_result"], "not_run")

    def test_only_missing_image_is_pulled_for_native_platform_and_reinspected(self):
        reference = runner.image_references("debian", "all", "20.2.4")["all"]
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
        self.runtime.inspect_overrides[runner.DEFAULT_IMAGE] = {"Architecture": "amd64"}
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIn("image platform differs", self.summary()["error"])

    def test_native_amd64_explicit_platform_runs_without_emulation(self):
        self.runtime.info["Architecture"] = "x86_64"
        self.assertEqual(self.invoke(["--variant", "ubuntu", "--platform", "linux/amd64"]), 0)
        self.assertEqual(self.summary()["platform"], "linux/amd64")

    def test_nonimmutable_image_id_rejected(self):
        self.runtime.inspect_overrides[runner.DEFAULT_IMAGE] = {"Id": "quay.io/ceph/ceph:v20.2.4"}
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.runtime.stream_commands)
        self.assertIn("immutable local image ID", self.summary()["error"])

    def test_registry_digests_are_optional_but_never_manufactured_or_malformed(self):
        self.runtime.inspect_overrides[runner.DEFAULT_IMAGE] = {"RepoDigests": None}
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.summary()["images"]["all"]["repo_digests"], [])
        for value in ({}, "sha256:" + "a" * 64, ["registry:tag"], [None]):
            with self.subTest(value=value):
                self.output = self.root / "artifacts" / ("invalid-digest-" + str(len(self.runtime.capture_commands)))
                self.runtime.inspect_overrides[runner.DEFAULT_IMAGE] = {"RepoDigests": value}
                count = len(self.runtime.stream_commands)
                self.assertEqual(self.invoke(), 1)
                self.assertEqual(len(self.runtime.stream_commands), count)
                self.assertIn("invalid registry digest metadata", self.summary()["error"])

    def test_partial_image_resolution_is_retained_on_later_failure(self):
        reference = runner.image_references("ubuntu", "roles", "20.2.4")["rgw"]
        self.runtime.inspect_overrides[reference] = {"Os": "windows"}
        self.assertEqual(self.invoke(["--variant", "ubuntu", "--layout", "roles"]), 1)
        self.assertEqual(set(self.summary()["images"]), {"essential", "client", "cephfs", "rbd-aux"})
        self.assertFalse(self.runtime.stream_commands)

    def test_failed_pull_does_not_start_make(self):
        self.runtime.missing.add(runner.DEFAULT_IMAGE)
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
            "package_twice": good + "ok\t" + MODULE + "/internal/integration/rbd\t1.00s\n",
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
        self.assertTrue({"Makefile", "go.mod", "go.sum", "ceph/ceph.go", "ceph/internal/scripts/essential.sh"}.issubset(paths))
        self.write("ceph/internal/scripts/essential.sh", "#!/bin/sh\nprintf changed\n")
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

    def test_each_release_selects_its_own_published_images(self):
        self.assertEqual(runner.DEFAULT_IMAGE, runner.REGISTRY+"@"+runner.published_role_pins(runner.RELEASE)["all"])
        for release, official in runner.RELEASES.items():
            with self.subTest(release=release):
                self.assertRegex(official, r"^quay\.io/ceph/ceph:v" + re.escape(release) + r"@sha256:[0-9a-f]{64}$")
                self.assertEqual(runner.image_references("official", "all", release), {"all": runner.REGISTRY+"@"+runner.published_role_pins(release)["all"]})
                self.assertEqual(runner.image_references("ubuntu", "roles", release)["essential"],
                                 runner.REGISTRY+"@"+runner.published_role_pins(release,"ubuntu")["essential"])
                self.assertEqual(runner.parse_arguments(("--release", release)).release, release)


class RoleSelectionTests(unittest.TestCase):
    def test_library_default_and_all_variants_are_digest_pinned(self):
        source = (SCRIPT.parents[2] / "ceph/ceph.go").read_text()
        self.assertIn('const DefaultImage = "' + runner.DEFAULT_IMAGE + '"', source)
        for release in runner.RELEASES:
            for variant in ("official", "debian", "ubuntu"):
                pins = runner.published_role_pins(release, variant)
                refs = runner.image_references(variant, "roles", release)
                self.assertEqual(set(refs), set(runner.ROLE_ENV))
                self.assertEqual(refs, {role:runner.REGISTRY+"@"+pins[role] for role in runner.ROLE_ENV})
                self.assertEqual(runner.image_references(variant,"all",release), {"all":runner.REGISTRY+"@"+pins["all"]})
                override = "sha256:" + "f" * 64
                self.assertEqual(runner.pin_references(refs,{"essential":override})["essential"],runner.REGISTRY+"@"+override)

    def test_unknown_role_and_missing_essential_are_refused(self):
        self.assertEqual(runner.parse_arguments([]).roles, tuple(runner.ROLE_ENV))
        # Retired and incomplete selections: control and osd are not roles now.
        for roles in ("control", "client", "essential,osd", "essential,client,client"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                runner.parse_arguments(["--layout","roles","--prepare-only","--roles",roles])

    def test_missing_variant_pins_fail_before_runtime(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(runner,"ROOT",Path(directory)):
            root=Path(directory);(root/".github").mkdir()
            (root/".github/ceph-role-images.json").write_text(json.dumps({"schema":1,"images":{}}))
            with self.assertRaisesRegex(ValueError,"pins are incomplete"):
                runner.image_references("official","roles","20.2.4")

    def test_prepared_environment_has_exactly_selected_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"github-env"
            env={"CEPH_TEST_ESSENTIAL_IMAGE":"sha256:"+"1"*64,"CEPH_TEST_CLIENT_IMAGE":"sha256:"+"2"*64}
            runner.append_role_environment(output,env,("essential","client"))
            before=output.read_bytes()
            for change in ({"CEPH_TEST_CLIENT_IMAGE":"mutable:tag"},{"CEPH_TEST_CEPHFS_IMAGE":"sha256:"+"3"*64}):
                with self.assertRaises(RuntimeError):runner.append_role_environment(output,env|change,("essential","client"))
                self.assertEqual(output.read_bytes(),before)


if __name__ == "__main__":
    unittest.main()
