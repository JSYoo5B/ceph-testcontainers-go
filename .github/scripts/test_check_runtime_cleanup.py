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
OLD_VOLUME, NEW_VOLUME = "foreign_existing-volume.01", "ceph-tc_RAM.01"


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
        self.volumes = [OLD_VOLUME]
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
        elif command == cleanup.RESOURCE_COMMANDS["volumes"]:
            output = "\n".join(self.volumes)
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

    def snapshot(self, readiness=60):
        return cleanup.snapshot(self.directory, self.docker.reader(), readiness,
                                self.docker.clock.sleep)

    def check(self, grace=3):
        return cleanup.check(self.directory, grace, self.docker.reader(),
                             self.docker.clock.monotonic, self.docker.clock.sleep)

    def test_existing_foreign_baseline_remains_and_new_results_are_saved(self):
        before = self.snapshot()
        after = self.check()
        self.assertTrue(before["passed"])
        self.assertTrue(after["passed"])
        self.assertEqual(before["schema"], 2)
        self.assertEqual(after["schema"], 2)
        self.assertEqual(after["resources"], before["resources"])
        self.assertEqual(after["new_resources"], {"containers": [], "networks": [],
                                                 "volumes": []})
        self.assertEqual(after["resources"]["volumes"], [OLD_VOLUME])
        self.assertEqual(json.loads((self.directory / "before.json").read_text()), before)
        self.assertEqual(json.loads((self.directory / "after.json").read_text()), after)

    def test_normal_reaper_grace_converges_without_mutations(self):
        self.snapshot()
        self.docker.containers.append(NEW_CONTAINER)
        self.docker.networks.append(NEW_NETWORK)
        self.docker.volumes.append(NEW_VOLUME)

        def after_grace(command, kwargs):
            if self.docker.clock.now >= 2:
                self.docker.containers = [OLD_CONTAINER]
                self.docker.networks = [OLD_NETWORK]
                self.docker.volumes = [OLD_VOLUME]

        self.docker.hook = after_grace
        after = self.check()
        self.assertTrue(after["passed"])
        self.assertEqual(after["observations"], 3)
        self.assertEqual(self.docker.clock.now, 2)

    def test_container_network_and_volume_leaks_fail_after_bounded_grace(self):
        for kind, identity in [("containers", NEW_CONTAINER), ("networks", NEW_NETWORK),
                               ("volumes", NEW_VOLUME)]:
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
        for kind in cleanup.RESOURCE_KINDS:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as path:
                docker = Docker()
                cleanup.snapshot(path, docker.reader())

                def failing_list(command, kwargs):
                    if command == cleanup.RESOURCE_COMMANDS[kind]:
                        return subprocess.CompletedProcess(command, 7, "", "daemon unavailable")

                docker.hook = failing_list
                after = cleanup.check(path, 3, docker.reader(),
                                      docker.clock.monotonic, docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertIn("exit 7", after["error"])
                self.assertNotIn("new_resources", after)

    def test_volume_query_timeout_fails_snapshot_and_check_closed(self):
        for action in ("snapshot", "check"):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as path:
                docker = Docker()
                if action == "check":
                    cleanup.snapshot(path, docker.reader())

                def failing_volume_list(command, options):
                    if command == cleanup.RESOURCE_COMMANDS["volumes"]:
                        raise subprocess.TimeoutExpired(command, options["timeout"],
                                                        stderr=b"volume listing timeout")

                docker.hook = failing_volume_list
                result = (cleanup.snapshot(path, docker.reader()) if action == "snapshot"
                          else cleanup.check(path, 3, docker.reader(),
                                             docker.clock.monotonic, docker.clock.sleep))
                self.assertFalse(result["passed"])
                self.assertIn("timed out", result["error"])
                self.assertEqual(result["commands"][-1]["argv"],
                                 cleanup.RESOURCE_COMMANDS["volumes"])
                self.assertNotIn("new_resources", result)

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
        for kind in ("networks", "volumes"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as path:
                docker = Docker()
                cleanup.snapshot(path, docker.reader())

                def change_during_list(command, kwargs):
                    if command == cleanup.RESOURCE_COMMANDS[kind]:
                        docker.info["ID"] = "changed-mid-observation"

                docker.hook = change_during_list
                after = cleanup.check(path, 3, docker.reader(),
                                      docker.clock.monotonic, docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertIn("changed during", after["error"])

    def test_snapshot_rejects_changed_engine(self):
        for kind in ("networks", "volumes"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as path:
                docker = Docker()

                def change_during_list(command, kwargs):
                    if command == cleanup.RESOURCE_COMMANDS[kind]:
                        docker.info["ID"] = "changed-mid-snapshot"

                docker.hook = change_during_list
                result = cleanup.snapshot(path, docker.reader())
                self.assertFalse(result["passed"])
                self.assertIn("changed during", result["error"])
                after = cleanup.check(path, 3, docker.reader(),
                                      docker.clock.monotonic, docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertIn("successful supported snapshot", after["error"])

    def test_invalid_docker_ids_cannot_pass_snapshot_or_check(self):
        for value in ["a" * 12, "G" * 64, "-" + "a" * 63, "\"foreign\"", "a" * 64 + "\n" + "a" * 64]:
            for kind in ("containers", "networks"):
                for action in ("snapshot", "check"):
                    with self.subTest(value=value, kind=kind, action=action), \
                            tempfile.TemporaryDirectory() as path:
                        docker = Docker()
                        if action == "check":
                            cleanup.snapshot(path, docker.reader())
                        setattr(docker, kind, [value])
                        result = (cleanup.snapshot(path, docker.reader()) if action == "snapshot"
                                  else cleanup.check(path, 2, docker.reader(),
                                                     docker.clock.monotonic, docker.clock.sleep))
                        self.assertFalse(result["passed"])
                        self.assertIn("resource IDs", result["error"])

    def test_empty_volume_list_is_observed_and_valid(self):
        self.docker.volumes = []
        before = self.snapshot()
        after = self.check()
        self.assertTrue(before["passed"])
        self.assertTrue(after["passed"])
        self.assertEqual(before["resources"]["volumes"], [])
        self.assertEqual(after["resources"]["volumes"], [])
        self.assertEqual(after["new_resources"]["volumes"], [])
        self.assertIn(cleanup.RESOURCE_COMMANDS["volumes"],
                      [command for command, options in self.docker.calls])

    def test_valid_volume_names_are_independent_of_hex_resource_ids(self):
        names = ["aa", "RAM-data_01.2", "9-start", "a" * 64, OLD_VOLUME]
        self.docker.volumes = names
        before = self.snapshot()
        after = self.check()
        self.assertTrue(before["passed"])
        self.assertTrue(after["passed"])
        self.assertEqual(before["resources"]["volumes"], sorted(names))
        self.assertEqual(after["baseline_resources"]["volumes"], sorted(names))
        self.assertEqual(after["new_resources"]["volumes"], [])

    def test_invalid_volume_names_cannot_pass_snapshot_or_check(self):
        values = ["", "a", "-name", "_name", ".name", "../name",
                  "/tmp/name", "name/path", "name:tag", "with space", "한글-volume",
                  "name\tvalue", "name\x00value", '"foreign"', "name;rm",
                  NEW_VOLUME + "\n" + NEW_VOLUME]
        for value in values:
            for action in ("snapshot", "check"):
                with self.subTest(value=value, action=action), tempfile.TemporaryDirectory() as path:
                    docker = Docker()
                    if action == "check":
                        cleanup.snapshot(path, docker.reader())
                    docker.volumes = [value]
                    if value == "":
                        docker.volumes = ["", "valid-name"]
                    result = (cleanup.snapshot(path, docker.reader()) if action == "snapshot"
                              else cleanup.check(path, 3, docker.reader(),
                                                 docker.clock.monotonic, docker.clock.sleep))
                    self.assertFalse(result["passed"])
                    self.assertIn("volume names", result["error"])

    def test_old_schema_and_missing_volume_baselines_are_rejected_before_reads(self):
        valid = self.snapshot()
        legacy_resources = {kind: valid["resources"][kind]
                            for kind in ("containers", "networks")}
        for delta in [{"schema": 1, "resources": legacy_resources}, {"schema": 1},
                      {"resources": legacy_resources}]:
            with self.subTest(delta=delta), tempfile.TemporaryDirectory() as path:
                (Path(path) / "before.json").write_text(json.dumps(valid | delta))
                prior = len(self.docker.calls)
                after = cleanup.check(path, 3, self.docker.reader(),
                                      self.docker.clock.monotonic, self.docker.clock.sleep)
                self.assertFalse(after["passed"])
                self.assertEqual(len(self.docker.calls), prior)
                self.assertEqual(after["schema"], 2)
                self.assertNotIn("new_resources", after)

    def test_invalid_baseline_shapes_fail_before_docker_reads(self):
        valid = self.snapshot()
        for delta in [{"schema": True}, {"schema": 3}, {"action": "check"}, {"passed": False},
                      {"label": "foreign=true"}, {"resources": []}, {"resources": {"containers": []}},
                      {"resources": {"containers": ["a" * 12], "networks": [], "volumes": []}},
                      {"resources": {"containers": [], "networks": [], "volumes": "name"}},
                      {"resources": {"containers": [], "networks": [], "volumes": [None]}},
                      {"resources": {"containers": [], "networks": [], "volumes": [123]}},
                      {"resources": {"containers": [], "networks": [], "volumes": ["a"]}},
                      {"resources": {"containers": [], "networks": [],
                                     "volumes": [NEW_VOLUME, NEW_VOLUME]}},
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
        for kind, command in cleanup.RESOURCE_COMMANDS.items():
            if kind != "volumes":
                self.assertIn("--no-trunc", command)
            self.assertIn("label=" + cleanup.LABEL, command)
        self.assertEqual(cleanup.RESOURCE_COMMANDS["volumes"],
                         ["docker", "volume", "ls", "--quiet", "--filter",
                          "label=" + cleanup.LABEL])

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

    def test_snapshot_retries_startup_timeout_and_preserves_every_attempt(self):
        reads = 0

        def starting(command, options):
            nonlocal reads
            if command == cleanup.ENGINE_COMMAND:
                reads += 1
                if reads <= 2:
                    self.docker.clock.now += options["timeout"]
                    raise subprocess.TimeoutExpired(command, options["timeout"],
                                                    stderr=b"daemon startup still pending")

        self.docker.hook = starting
        before = self.snapshot()
        self.assertTrue(before["passed"])
        self.assertTrue(before["readiness"]["passed"])
        attempts = before["readiness"]["attempts"]
        self.assertEqual([a["passed"] for a in attempts], [False, False, True])
        self.assertEqual([(a["command_begin"], a["command_end"]) for a in attempts],
                         [(0, 1), (1, 2), (2, 3)])
        self.assertEqual(len(before["commands"]), 8)
        for command in before["commands"][:2]:
            self.assertEqual(command["argv"], cleanup.ENGINE_COMMAND)
            self.assertEqual(command["stderr"], "daemon startup still pending")
            self.assertEqual(command["error_type"], "TimeoutExpired")
            self.assertIsNone(command["exit_code"])
        self.assertEqual(self.docker.clock.now, 22)
        self.assertEqual(before["engine"], before["readiness"]["engine"])
        self.assertEqual(before["resources"], {"containers": [OLD_CONTAINER],
                                               "networks": [OLD_NETWORK],
                                               "volumes": [OLD_VOLUME]})
        self.assertEqual(json.loads((self.directory / "before.json").read_text()), before)

    def test_snapshot_retries_only_recognized_socket_startup_errors(self):
        for stderr in ["Cannot connect to the Docker daemon at unix:///var/run/docker.sock. "
                       "Is the docker daemon running?",
                       'error during connect: dial unix /var/run/docker.sock: connect: connection refused',
                       'dial unix /var/run/docker.sock: connect: no such file or directory']:
            with self.subTest(stderr=stderr), tempfile.TemporaryDirectory() as path:
                docker = Docker()

                def first_connection(command, options):
                    if len(docker.calls) == 1:
                        return subprocess.CompletedProcess(command, 1, "", stderr)

                docker.hook = first_connection
                before = cleanup.snapshot(path, docker.reader(), 3, docker.clock.sleep)
                self.assertTrue(before["passed"])
                self.assertEqual(len(before["readiness"]["attempts"]), 2)
                self.assertTrue(before["readiness"]["attempts"][0]["retryable"])
                self.assertEqual(before["commands"][0]["stderr"], stderr)
                self.assertEqual(before["commands"][0]["exit_code"], 1)
                self.assertEqual(docker.clock.now, 1)

    def test_exhausted_startup_readiness_is_bounded_and_cannot_create_a_baseline(self):
        def never_ready(command, options):
            self.docker.clock.now += options["timeout"]
            raise subprocess.TimeoutExpired(command, options["timeout"], stderr=b"not ready")

        self.docker.hook = never_ready
        before = self.snapshot(12)
        self.assertFalse(before["passed"])
        self.assertFalse(before["readiness"]["passed"])
        self.assertIn("readiness deadline", before["error"])
        self.assertIn("inspection timed out", before["error"])
        self.assertEqual(self.docker.clock.now, 12)
        self.assertEqual([r["timeout_seconds"] for r in before["commands"]], [10, 1])
        self.assertEqual(len(before["readiness"]["attempts"]), 2)
        self.assertEqual([c[0] for c in self.docker.calls], [cleanup.ENGINE_COMMAND] * 2)
        self.assertNotIn("resources", before)
        self.assertNotIn("engine", before)
        self.assertFalse((self.directory / "after.json").exists())
        after = self.check()
        self.assertFalse(after["passed"])
        self.assertIn("successful supported snapshot", after["error"])
        self.assertEqual(len(self.docker.calls), 2)

    def test_permanent_startup_errors_never_retry(self):
        cases = ["permission denied while trying to connect to the Docker daemon socket",
                 "Cannot connect to the Docker daemon: permission denied",
                 "Cannot connect to the Docker daemon at unix:///var/run/docker.sock: permission denied",
                 "Cannot connect to the Docker daemon at tcp://remote.example:2375. Is the docker daemon running?",
                 "Cannot connect to the Docker daemon. Is the docker daemon running on this host?",
                 "Error response from daemon: authorization denied",
                 "unknown server error"]
        for stderr in cases:
            with self.subTest(stderr=stderr), tempfile.TemporaryDirectory() as path:
                docker = Docker()
                docker.hook = lambda command, options: subprocess.CompletedProcess(
                    command, 1, "", stderr)
                before = cleanup.snapshot(path, docker.reader(), 3, docker.clock.sleep)
                self.assertFalse(before["passed"])
                self.assertFalse(before["readiness"]["attempts"][0]["retryable"])
                self.assertEqual(len(docker.calls), 1)
                self.assertEqual(docker.clock.now, 0)
                self.assertEqual(before["commands"][0]["stderr"], stderr)
        with tempfile.TemporaryDirectory() as path:
            def missing(command, options):
                raise FileNotFoundError("docker")

            docker = Docker()
            docker.hook = missing
            before = cleanup.snapshot(path, docker.reader(), 3, docker.clock.sleep)
            self.assertFalse(before["passed"])
            self.assertEqual(len(docker.calls), 1)
            self.assertEqual(before["commands"][0]["error_type"], "FileNotFoundError")
            self.assertFalse(before["readiness"]["attempts"][0]["retryable"])

    def test_invalid_startup_engine_observation_never_retries(self):
        for output in ["{invalid", "[]", json.dumps({"ID": "", "OSType": "linux",
                                                       "Architecture": "amd64"}),
                       json.dumps({"ID": "native", "OSType": "linux",
                                   "Architecture": "riscv64"}),
                       json.dumps({"ID": "native", "OSType": "windows",
                                   "Architecture": "amd64"})]:
            with self.subTest(output=output), tempfile.TemporaryDirectory() as path:
                docker = Docker()
                docker.hook = lambda command, options: subprocess.CompletedProcess(
                    command, 0, output, "")
                before = cleanup.snapshot(path, docker.reader(), 3, docker.clock.sleep)
                self.assertFalse(before["passed"])
                self.assertEqual(len(docker.calls), 1)
                self.assertFalse(before["readiness"]["attempts"][0]["retryable"])
                self.assertNotIn("resources", before)

    def test_late_valid_startup_observation_cannot_publish_ready(self):
        def slow(command, options):
            self.docker.clock.now += 3

        self.docker.hook = slow
        before = self.snapshot(2)
        self.assertFalse(before["passed"])
        self.assertFalse(before["readiness"]["passed"])
        self.assertEqual(len(self.docker.calls), 1)
        self.assertEqual(before["commands"][0]["error_type"], "ObservationDeadlineExpired")
        self.assertNotIn("resources", before)

    def test_engine_switch_after_readiness_cannot_publish_a_baseline(self):
        def changed_engine(command, options):
            if command == cleanup.ENGINE_COMMAND and len(self.docker.calls) == 2:
                self.docker.info["ID"] = "another-engine"

        self.docker.hook = changed_engine
        before = self.snapshot()
        self.assertTrue(before["readiness"]["passed"])
        self.assertFalse(before["passed"])
        self.assertIn("changed after baseline readiness", before["error"])
        self.assertEqual(len(self.docker.calls), 2)
        self.assertNotIn("resources", before)

    def test_authoritative_snapshot_failure_is_not_retried_as_startup(self):
        def failed_list(command, options):
            if command == cleanup.RESOURCE_COMMANDS["containers"]:
                raise subprocess.TimeoutExpired(command, options["timeout"],
                                                stderr=b"listing timeout")

        self.docker.hook = failed_list
        before = self.snapshot()
        self.assertTrue(before["readiness"]["passed"])
        self.assertFalse(before["passed"])
        self.assertEqual(len(before["readiness"]["attempts"]), 1)
        self.assertEqual(len(self.docker.calls), 3)
        self.assertEqual(before["commands"][-1]["argv"], cleanup.RESOURCE_COMMANDS["containers"])
        self.assertEqual(before["commands"][-1]["stderr"], "listing timeout")

    def test_check_connection_failure_does_not_receive_startup_retry_or_extra_grace(self):
        self.snapshot()
        prior = len(self.docker.calls)
        self.docker.hook = lambda command, options: subprocess.CompletedProcess(
            command, 1, "", "Cannot connect to the Docker daemon")
        after = self.check(2)
        self.assertFalse(after["passed"])
        self.assertEqual(len(self.docker.calls), prior + 1)
        self.assertEqual(after["grace_seconds"], 2)
        self.assertNotIn("readiness", after)
        self.assertNotIn("new_resources", after)

    def test_invalid_readiness_is_rejected_without_reads_or_a_result(self):
        for seconds in [0, -1, 61, float("inf"), float("nan")]:
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                self.snapshot(seconds)
        self.assertEqual(self.docker.calls, [])
        self.assertFalse((self.directory / "before.json").exists())


if __name__ == "__main__":
    unittest.main()
