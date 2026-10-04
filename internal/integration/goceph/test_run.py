"""Test supplied-image admission and execution without calling Docker."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("goceph_run", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)

CLIENT_ID = "sha256:" + "1" * 64
RUNNER_ID = "sha256:" + "2" * 64


class SuppliedImagesTests(unittest.TestCase):
    def test_missing_image_inputs_fail_before_docker_or_output_creation(self):
        for arguments in ([], ["--client-image", "client"], ["--runner-image", "runner"]):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "output"
                stderr = io.StringIO()
                with mock.patch.dict(os.environ, {}, clear=True), \
                        mock.patch.object(runner.subprocess, "check_output") as capture, \
                        mock.patch.object(runner.subprocess, "Popen") as process, \
                        contextlib.redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as error:
                        runner.main([*arguments, "--output-dir", str(output)])
                self.assertEqual(error.exception.code, 2)
                self.assertIn("ceph-testcontainers-images", stderr.getvalue())
                self.assertFalse(output.exists())
                capture.assert_not_called()
                process.assert_not_called()

    def test_environment_images_and_cli_override(self):
        with mock.patch.dict(os.environ, {"CEPH_TEST_GOCEPH_CLIENT_IMAGE": "environment-client",
                                          "CEPH_TEST_GOCEPH_RUNNER_IMAGE": "environment-runner"}, clear=True):
            arguments = runner.parse_arguments(["--client-image", "cli-client"])
        self.assertEqual(arguments.client_image, "cli-client")
        self.assertEqual(arguments.runner_image, "environment-runner")

    def capture(self, command, **_):
        if command[1] == "info":
            return json.dumps({"OSType": "linux", "Architecture": "aarch64"})
        if command[1:3] == ["image", "inspect"]:
            self.assertEqual(command[3], "--")
            return json.dumps([{"Id": CLIENT_ID if command[-1] == "client" else RUNNER_ID,
                                "Os": "linux", "Architecture": "arm64"}])
        if command[1] == "ps":
            return ""
        self.fail("Unexpected Docker operation: " + str(command))

    def test_run_uses_inspected_ids_and_never_builds_or_pulls(self):
        process = mock.Mock()
        process.stdout = io.StringIO("ok integration.test 1.00s\n")
        process.wait.return_value = 0
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(runner.subprocess, "check_output", side_effect=self.capture) as capture, \
                    mock.patch.object(runner.subprocess, "Popen", return_value=process) as launch, \
                    contextlib.redirect_stdout(io.StringIO()):
                result = runner.main(["--client-image", "client", "--runner-image", "runner",
                                      "--output-dir", str(output)])
            command = launch.call_args.args[0]
            self.assertEqual(command[:3], ["docker", "run", "--pull=never"])
            self.assertIn(RUNNER_ID, command)
            self.assertIn("CEPH_TEST_GOCEPH_CLIENT_IMAGE=" + CLIENT_ID, command)
            for invocation in [*capture.call_args_list, *launch.call_args_list]:
                self.assertNotIn(invocation.args[0][1], ("build", "pull"))
            report = json.loads((output / "summary.json").read_text())
            self.assertEqual(result, 0)
            self.assertTrue(report["passed"])
            self.assertEqual(report["image_builds"], 0)
            self.assertEqual(report["client_image"]["Id"], CLIENT_ID)
            self.assertEqual(report["runner_image"]["Id"], RUNNER_ID)

    def test_missing_local_image_does_not_start_runner(self):
        def capture(command, **kwargs):
            if command[1:3] == ["image", "inspect"]:
                raise subprocess.CalledProcessError(1, command)
            return self.capture(command, **kwargs)

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(runner.subprocess, "check_output", side_effect=capture), \
                    mock.patch.object(runner.subprocess, "Popen") as launch, \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = runner.main(["--client-image", "client", "--runner-image", "runner",
                                      "--output-dir", str(output)])
            launch.assert_not_called()
            report = json.loads((output / "summary.json").read_text())
            self.assertEqual(result, 1)
            self.assertFalse(report["passed"])
            self.assertIn("unavailable locally", report["error"])


if __name__ == "__main__":
    unittest.main()
