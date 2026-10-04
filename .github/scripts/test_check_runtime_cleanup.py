import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "check_runtime_cleanup", Path(__file__).with_name("check_runtime_cleanup.py"))
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)

OLD_CONTAINER, NEW_CONTAINER = "a" * 64, "b" * 64
OLD_NETWORK, NEW_NETWORK = "c" * 64, "d" * 64


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


class Docker:
    def __init__(self, clock=None):
        self.clock = clock or Clock()
        self.info = {"ID": "engine-original", "OSType": "linux", "Architecture": "aarch64"}
        self.containers = [OLD_CONTAINER]
        self.networks = [OLD_NETWORK]
        self.calls = []
        self.hook = None

    def run(self, command, **kwargs):
        self.calls.append((list(command), kwargs))
        if self.hook:
            result = self.hook(command, kwargs)
            if result is not None:
                return result
        if command == cleanup.ENGINE_COMMAND:
            output = json.dumps(self.info)
        elif command == cleanup.RESOURCE_COMMANDS["containers"]:
            output = "\n".join(self.containers)
        elif command == cleanup.RESOURCE_COMMANDS["networks"]:
            output = "\n".join(self.networks)
        else:
            raise AssertionError("Unexpected Docker command: " + repr(command))
        return subprocess.CompletedProcess(command, 0, output, "")

    def reader(self):
        return cleanup.DockerReader(run=self.run, monotonic=self.clock.monotonic)


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.docker = Docker()

    def snapshot(self):
        return cleanup.snapshot(self.directory, self.docker.reader())

    def check(self, grace=3):
        return cleanup.check(self.directory, grace, self.docker.reader(),
                             self.docker.clock.monotonic, self.docker.clock.sleep)

    def test_existing_foreign_baseline_remains_and_new_results_are_saved(self):
        before = self.snapshot()
        after = self.check()
        self.assertTrue(before["passed"])
        self.assertTrue(after["passed"])
        self.assertEqual(after["resources"], before["resources"])
        self.assertEqual(after["new_resources"], {"containers": [], "networks": []})
        self.assertEqual(json.loads((self.directory / "before.json").read_text()), before)
        self.assertEqual(json.loads((self.directory / "after.json").read_text()), after)

    def test_normal_reaper_grace_converges_without_mutations(self):
        self.snapshot()
        self.docker.containers.append(NEW_CONTAINER)
        self.docker.networks.append(NEW_NETWORK)

        def after_grace(command, kwargs):
            if self.docker.clock.now >= 2:
                self.docker.containers = [OLD_CONTAINER]
                self.docker.networks = [OLD_NETWORK]

        self.docker.hook = after_grace
        after = self.check()
        self.assertTrue(after["passed"])
        self.assertEqual(after["observations"], 3)
        self.assertEqual(self.docker.clock.now, 2)

    def test_container_and_network_leaks_fail_after_bounded_grace(self):
        for kind, identity in [("containers", NEW_CONTAINER), ("networks", NEW_NETWORK)]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as path:
                directory = Path(path)
                cleanup.snapshot(directory, self.docker.reader())
                getattr(self.docker, kind).append(identity)
                after = cleanup.check(directory, 2, self.docker.reader(),
                                      self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertEqual(after["new_resources"][kind], [identity])
                self.assertEqual(self.docker.clock.now, 2)
                getattr(self.docker, kind).remove(identity)
                self.docker.clock.now = 0

    def test_docker_failure_is_never_an_empty_success(self):
        self.snapshot()

        def failing_list(command, kwargs):
            if command == cleanup.RESOURCE_COMMANDS["containers"]:
                return subprocess.CompletedProcess(command, 7, "", "daemon unavailable")

        self.docker.hook = failing_list
        after = self.check()
        self.assertFalse(after["passed"])
        self.assertIn("exit 7", after["error"])
        self.assertNotIn("new_resources", after)

    def test_timeout_and_missing_executable_fail_closed(self):
        for error in [FileNotFoundError("docker"), subprocess.TimeoutExpired("docker", 1)]:
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as path:
                cleanup.snapshot(path, self.docker.reader())

                def failing(command, kwargs):
                    raise error

                self.docker.hook = failing
                result = cleanup.check(path, 2, self.docker.reader(),
                                       self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(result["passed"])
                self.docker.hook = None

    def test_engine_switch_and_platform_change_fail(self):
        for key, changed in [("ID", "another-engine"), ("Architecture", "x86_64"), ("OSType", "windows")]:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as path:
                cleanup.snapshot(path, self.docker.reader())
                original = self.docker.info[key]
                self.docker.info[key] = changed
                result = cleanup.check(path, 2, self.docker.reader(),
                                       self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(result["passed"])
                self.docker.info[key] = original

    def test_malformed_engine_info_fails_closed(self):
        for info in [[], {"OSType": "linux", "ID": "engine-original", "Architecture": []},
                     {"OSType": "linux", "ID": None, "Architecture": "amd64"},
                     {"OSType": "linux", "ID": "engine-original", "Architecture": "riscv64"}]:
            with self.subTest(info=info), tempfile.TemporaryDirectory() as path:
                original = self.docker.info
                self.docker.info = info
                result = cleanup.snapshot(path, self.docker.reader())
                self.assertFalse(result["passed"])
                self.docker.info = original

    def test_engine_switch_between_lists_and_confirmation_fails(self):
        self.snapshot()

        def change_during_list(command, kwargs):
            if command == cleanup.RESOURCE_COMMANDS["networks"]:
                self.docker.info["ID"] = "changed-mid-observation"

        self.docker.hook = change_during_list
        after = self.check()
        self.assertFalse(after["passed"])
        self.assertIn("changed during", after["error"])

    def test_snapshot_rejects_changed_engine(self):
        def change_during_list(command, kwargs):
            if command == cleanup.RESOURCE_COMMANDS["networks"]:
                self.docker.info["ID"] = "changed-mid-snapshot"

        self.docker.hook = change_during_list
        result = self.snapshot()
        self.assertFalse(result["passed"])
        self.assertIn("changed during", result["error"])
        after = self.check()
        self.assertFalse(after["passed"])
        self.assertIn("successful supported snapshot", after["error"])

    def test_invalid_docker_ids_cannot_pass_snapshot_or_check(self):
        for value in ["a" * 12, "G" * 64, "-" + "a" * 63, "\"foreign\"", "a" * 64 + "\n" + "a" * 64]:
            with self.subTest(value=value), tempfile.TemporaryDirectory() as path:
                cleanup.snapshot(path, self.docker.reader())
                original = self.docker.containers
                self.docker.containers = [value]
                after = cleanup.check(path, 2, self.docker.reader(),
                                      self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.docker.containers = original

    def test_invalid_baseline_shapes_fail_before_docker_reads(self):
        valid = self.snapshot()
        for delta in [{"schema": True}, {"schema": 2}, {"action": "check"}, {"passed": False},
                      {"label": "foreign=true"}, {"resources": []}, {"resources": {"containers": []}},
                      {"resources": {"containers": ["a" * 12], "networks": []}},
                      {"engine": []}, {"engine": {"id": "", "platform": "linux/arm64"}}]:
            with self.subTest(delta=delta), tempfile.TemporaryDirectory() as path:
                directory = Path(path)
                (directory / "before.json").write_text(json.dumps(valid | delta))
                prior = len(self.docker.calls)
                after = cleanup.check(path, 2, self.docker.reader(),
                                      self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertEqual(len(self.docker.calls), prior)

    def test_missing_and_invalid_json_baseline_fail(self):
        after = self.check()
        self.assertFalse(after["passed"])
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            (directory / "before.json").write_text("{invalid")
            self.assertFalse(cleanup.check(directory, reader=self.docker.reader())["passed"])

    def test_result_overwrite_is_refused_without_docker_reads(self):
        before = self.snapshot()
        prior = len(self.docker.calls)
        with self.assertRaises(FileExistsError):
            self.snapshot()
        self.assertEqual(len(self.docker.calls), prior)
        self.check()
        prior = len(self.docker.calls)
        saved = (self.directory / "after.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.check()
        with self.assertRaises(FileExistsError):
            self.snapshot()
        self.assertEqual(len(self.docker.calls), prior)
        self.assertEqual((self.directory / "after.json").read_bytes(), saved)
        self.assertEqual(json.loads((self.directory / "before.json").read_text()), before)

    def test_inspection_is_read_only_and_each_call_has_a_deadline(self):
        self.snapshot()
        self.check()
        allowed = [cleanup.ENGINE_COMMAND, *cleanup.RESOURCE_COMMANDS.values()]
        for command, options in self.docker.calls:
            self.assertIn(command, allowed)
            self.assertGreater(options["timeout"], 0)
            self.assertLessEqual(options["timeout"], 10)
            self.assertFalse(options["check"])
            self.assertEqual(options["stdin"], subprocess.DEVNULL)
        self.assertIn("--all", cleanup.RESOURCE_COMMANDS["containers"])
        for command in cleanup.RESOURCE_COMMANDS.values():
            self.assertIn("--no-trunc", command)
            self.assertIn("label=" + cleanup.LABEL, command)

    def test_a_slow_docker_call_cannot_report_success_after_deadline(self):
        self.snapshot()

        def slow(command, options):
            self.docker.clock.now += 3

        self.docker.hook = slow
        after = self.check(2)
        self.assertFalse(after["passed"])
        self.assertIn("deadline", after["error"])

    def test_invalid_grace_is_rejected_without_a_result(self):
        self.snapshot()
        for grace in [0, -1, 61, float("inf"), float("nan")]:
            with self.subTest(grace=grace), self.assertRaises(ValueError):
                self.check(grace)
        self.assertFalse((self.directory / "after.json").exists())


if __name__ == "__main__":
    unittest.main()
