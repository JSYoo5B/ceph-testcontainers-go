"""Exercise the setup entry point with injected engine facts and a temporary user profile.

No Docker command runs: FakeSystem answers the few commands the script
issues. Every engine value below is synthetic.
"""
from contextlib import redirect_stderr
import gzip
from io import StringIO
import json
import os
from pathlib import Path
import tempfile
import unittest

import desktop_setup as setup
import wslconfig

GB = 1 << 30
SECRET_ENDPOINT = "tcp://build-box.corp.example:2376"
SECRET_SOCKET = "unix:///Users/someone/.docker/run/docker.sock"


def engine(kernel="6.6.87.2-microsoft-standard-WSL2", cpus=16, memory=15.5, system="Docker Desktop", os_type="linux"):
    return {"OperatingSystem": system, "OSType": os_type, "Architecture": "x86_64", "KernelVersion": kernel,
            "NCPU": cpus, "MemTotal": int(memory * GB), "ServerVersion": "29.9.3", "CgroupVersion": "2",
            "Driver": "overlayfs", "Name": "private-engine-name",
            "Labels": ["com.docker.desktop.address=" + SECRET_SOCKET]}


class FakeSystem:
    def __init__(self, platform="win32", info=None, endpoint="npipe:////./pipe/dockerDesktopLinuxEngine",
                 context="desktop-linux", environ=None, files=None, probe=None, home="/nonexistent-home"):
        self.platform = platform
        self.machine = "amd64"
        self.info = engine() if info is None else info
        self.endpoint = endpoint
        self.context = context
        self.environ = dict(environ or {})
        self.files = dict(files or {})
        self.probe = probe
        self._home = home
        self.commands = []

    def home(self):
        return self._home

    def read_text(self, path):
        return self.files.get(path)

    def exists(self, path):
        return path in self.files

    def run(self, args, timeout=30):
        self.commands.append(list(args))
        if "context" in args:
            if self.endpoint is None:
                return 1, b""
            return 0, json.dumps({"Name": self.context, "Endpoints": {"docker": {"Host": self.endpoint}}}).encode()
        if "info" in args:
            if not self.info:
                return 1, json.dumps({"ServerErrors": ["cannot connect to " + SECRET_ENDPOINT]}).encode()
            return 0, json.dumps(self.info).encode()
        if "version" in args:
            server = {"Platform": {"Name": "Docker Desktop 4.99.0 (1)"}} if self.info else None
            return (0 if server else 1), json.dumps({"Client": {}, "Server": server}).encode()
        if "run" in args:
            program = args[args.index("--entrypoint") + 1]
            target = args[-1]
            if self.probe is None or (program, target) not in self.probe:
                return 125, b""
            return 0, self.probe[(program, target)]
        raise AssertionError("unexpected command: %r" % (args,))


def probe(swap_kib=1048572, free_kib=40 * 1024 * 1024, config="CONFIG_DM_DELAY=y\nCONFIG_BLK_DEV_NBD=m\n"):
    return {
        ("cat", "/proc/meminfo"): ("MemTotal: 1 kB\nSwapTotal: %d kB\n" % swap_kib).encode(),
        ("df", "/"): ("Filesystem 1024-blocks Used Available Capacity Mounted on\noverlay 1 1 %d 1%% /\n"
                      % free_kib).encode(),
        ("cat", "/proc/config.gz"): gzip.compress(("# CONFIG_DM_FLAKEY is not set\n" + config).encode()),
    }


class SetupCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.profile = Path(directory.name)
        self.config = self.profile / ".wslconfig"

    def windows(self, **arguments):
        return FakeSystem(environ={"USERPROFILE": str(self.profile)}, **arguments)

    def run_main(self, system, *argv):
        out = StringIO()
        status = setup.main(list(argv), system=system, out=out)
        return status, out.getvalue()

    def report(self, system, *argv):
        status, text = self.run_main(system, *(argv + ("--format", "json")))
        return status, json.loads(text)

    def outcomes(self, report):
        return {item["name"]: item["outcome"] for item in report["settings"]}

    def names(self):
        return sorted(entry.name for entry in self.profile.iterdir())


class ProfileTest(SetupCase):
    def test_shipped_profiles_are_valid_and_state_their_basis(self):
        profiles = setup.load_profiles(os.path.join(os.path.dirname(os.path.abspath(setup.__file__)), "profiles.json"))
        self.assertIn("single-cluster", profiles)
        for name, profile in profiles.items():
            self.assertTrue(profile["description"] and profile["basis"], name)

    def test_profile_values_are_editable_from_the_command_line_and_a_file(self):
        custom = self.profile / "mine.json"
        custom.write_text(json.dumps({"profiles": {"mine": {"cpus": 2, "memory_gib": 3, "swap_gib": 0,
                                                            "disk_free_gib": 0}}}))
        _, report = self.report(self.windows(), "--profile-file", str(custom), "--profile", "mine",
                                "--memory-gib", "6.5", "--kernel-feature", "nbd")
        self.assertEqual(report["profile"], {"name": "mine", "cpus": 2, "memory_gib": 6.5, "swap_gib": 0,
                                             "disk_free_gib": 0, "kernel_features": ["nbd"]})

    def test_invalid_profiles_are_rejected_before_any_command(self):
        system = self.windows()
        for argv in (("--profile", "nope"), ("--cpus", "0"), ("--memory-gib", "-1"), ("--memory-gib", "0"),
                     ("--profile-file", str(self.profile / "absent.json"))):
            with self.subTest(argv=argv), redirect_stderr(StringIO()) as errors:
                self.assertEqual(self.run_main(system, *argv)[0], setup.EXIT_REFUSED)
                self.assertIn("desktop_setup: ", errors.getvalue())
        self.assertEqual(system.commands, [])


class SelectionTest(SetupCase):
    def test_testcontainers_host_wins_over_docker_host_and_context(self):
        home = str(self.profile)
        system = FakeSystem(home=home, environ={"DOCKER_HOST": "unix:///var/run/docker.sock"},
                            files={os.path.join(home, ".testcontainers.properties"):
                                   "# comment\ndocker.host = unix:///x\ntc.host = %s\n" % SECRET_ENDPOINT})
        _, report = self.report(system)
        self.assertEqual(report["engine"]["selected_by"], "tc.host of ~/.testcontainers.properties")
        self.assertEqual(report["engine"]["backend"], "remote")
        self.assertIn(["docker", "--host", SECRET_ENDPOINT, "info", "--format", "{{json .}}"], system.commands)

    def test_docker_host_wins_over_the_context(self):
        system = FakeSystem(environ={"DOCKER_HOST": "ssh://someone@build-box"})
        _, report = self.report(system)
        self.assertEqual((report["engine"]["selected_by"], report["engine"]["transport"]), ("DOCKER_HOST", "ssh"))
        self.assertFalse(any("context" in command for command in system.commands))

    def test_backends_are_classified_from_client_and_engine_facts(self):
        linuxkit = engine(kernel="6.12.54-linuxkit")
        native = engine(kernel="6.8.0-generic", system="Ubuntu 24.04.3 LTS")
        cases = (
            ("desktop-windows-wsl2", FakeSystem()),
            ("desktop-windows-hyperv", FakeSystem(info=linuxkit)),
            ("desktop-windows-containers", FakeSystem(info=engine(kernel="10.0 26100", os_type="windows"))),
            ("desktop-macos", FakeSystem(platform="darwin", info=linuxkit, endpoint=SECRET_SOCKET)),
            ("desktop-linux", FakeSystem(platform="linux", info=linuxkit, endpoint="unix:///x/docker.sock")),
            ("linux-engine", FakeSystem(platform="linux", info=native, endpoint="unix:///var/run/docker.sock")),
            ("desktop-windows-wsl2", FakeSystem(platform="linux", endpoint="unix:///var/run/docker.sock",
                                                files={"/proc/version": "Linux version 6.6 (Microsoft@Microsoft.com)"})),
            # A local socket that is not Docker Desktop: another VM product or a forwarded socket.
            ("unrecognized", FakeSystem(platform="darwin", info=native, endpoint="unix:///x/docker.sock")),
            ("unrecognized", FakeSystem(info=engine(kernel="6.1.0-custom"))),
            ("unrecognized", FakeSystem(endpoint="fd://3")),
            # A remote engine is never taken for this machine's Desktop, even when it is one somewhere.
            ("remote", FakeSystem(endpoint="tcp://127.0.0.1:2375")),
            ("remote", FakeSystem(platform="darwin", endpoint=SECRET_ENDPOINT)),
            ("unreachable", FakeSystem(info={})),
            ("unreachable", FakeSystem(endpoint=None, info={})),
        )
        for backend, system in cases:
            with self.subTest(backend=backend, platform=system.platform, endpoint=system.endpoint):
                self.assertEqual(self.report(system)[1]["engine"]["backend"], backend)


class DefaultInvocationTest(SetupCase):
    def test_default_command_only_reads(self):
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        system = self.windows(info=engine(cpus=2, memory=1.9))
        status, report = self.report(system)
        self.assertEqual((status, report["mode"], report["writes"]), (setup.EXIT_OK, "plan", []))
        self.assertEqual(self.outcomes(report), {"cpus": "planned", "memory": "planned", "disk-free": "unverifiable"})
        self.assertEqual(report["planned_changes"], ["processors=4", "memory=4GB"])
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=2GB\n")
        self.assertEqual(self.names(), [".wslconfig"])
        # Read-only Docker queries, and neither a container, a restart nor a WSL command.
        self.assertEqual(sorted(command[1] for command in system.commands), ["context", "info", "version"])
        self.assertTrue(all(command[0] == "docker" for command in system.commands))

    def test_text_output_names_engine_profile_changes_scope_and_manual_fields(self):
        _, text = self.run_main(self.windows(info=engine(cpus=2, memory=1.9)), "--profile", "block-fault-delay")
        for expected in ("Backend: desktop-windows-wsl2", "Profile: block-fault-delay (cpus=4 memory=4GiB",
                         "Planned [wsl2] changes", "processors=4, memory=4GB", "every WSL 2 distribution",
                         "kernel:dm-delay", "unverifiable", "Written: nothing", "--probe-image"):
            self.assertIn(expected, text)
        self.assertLess(len(text.splitlines()), 30)


class ApplyTest(SetupCase):
    def test_apply_needs_the_scope_confirmation(self):
        system = self.windows(info=engine(cpus=2, memory=1.9))
        status, report = self.report(system, "apply")
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("every WSL 2 distribution", report["refusal"])
        self.assertEqual(self.names(), [])

    def test_apply_writes_reports_pending_restart_and_then_changes_nothing(self):
        self.config.write_bytes(b"# mine\r\n[wsl2]\r\nmemory=2GB\r\nkernelCommandLine=quiet\r\n")
        system = self.windows(info=engine(cpus=2, memory=1.9))
        status, report = self.report(system, "apply", "--confirm-wsl-global")
        self.assertEqual((status, report["writes"]), (setup.EXIT_OK, ["memory", "processors"]))
        self.assertEqual(self.config.read_bytes(),
                         b"# mine\r\n[wsl2]\r\nmemory=4GB\r\nkernelCommandLine=quiet\r\nprocessors=4\r\n")
        # The file has the values and the running engine does not: a restart is pending, not done.
        self.assertEqual(self.outcomes(report)["memory"], "pending-restart")
        self.assertEqual(self.outcomes(report)["cpus"], "pending-restart")
        self.assertTrue(report["restart"]["pending"])
        self.assertIn("wsl --shutdown", report["restart"]["action"])
        self.assertEqual(report["config"]["owned_keys"], ["memory", "processors"])
        self.assertFalse(any(word in command for command in system.commands for word in ("restart", "stop", "wsl")))

        before = {name: (self.profile / name).read_bytes() for name in self.names()}
        status, report = self.report(system, "apply", "--confirm-wsl-global")
        self.assertEqual((status, report["writes"]), (setup.EXIT_OK, []))
        self.assertEqual({name: (self.profile / name).read_bytes() for name in self.names()}, before)
        self.assertEqual(self.report(system, "verify")[0], setup.EXIT_UNMET)

        # After the operator restarts WSL, the same engine reports the new limits.
        system.info = engine(cpus=4, memory=3.8)
        status, report = self.report(system, "verify", "--disk-free-gib", "0")
        self.assertEqual((status, report["satisfied"], report["restart"]["pending"]), (setup.EXIT_OK, True, False))
        self.assertEqual(self.outcomes(report), {"cpus": "applied", "memory": "applied"})

    def test_apply_creates_a_missing_config_and_restore_removes_it(self):
        system = self.windows(info=engine(cpus=2, memory=1.9))
        self.assertEqual(self.run_main(system, "apply", "--confirm-wsl-global")[0], setup.EXIT_OK)
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\r\nmemory=4GB\r\nprocessors=4\r\n")
        status, report = self.report(system, "restore")
        self.assertEqual((status, report["writes"]), (setup.EXIT_OK, ["memory", "processors"]))
        self.assertTrue(report["restart"]["pending"])
        self.assertEqual(self.names(), [])

    def test_an_engine_that_already_has_enough_gets_no_config_change(self):
        system = self.windows()
        status, report = self.report(system, "apply", "--confirm-wsl-global", "--disk-free-gib", "0")
        self.assertEqual((status, report["writes"], report["satisfied"]), (setup.EXIT_OK, [], True))
        self.assertEqual(self.names(), [])

    def test_swap_is_planned_from_the_config_and_verified_only_through_the_probe(self):
        system = self.windows()
        _, report = self.report(system, "--swap-gib", "2")
        self.assertEqual(self.outcomes(report)["swap"], "planned")
        self.run_main(system, "apply", "--confirm-wsl-global", "--swap-gib", "2")
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\r\nswap=2GB\r\n")
        _, report = self.report(system, "verify", "--swap-gib", "2")
        self.assertEqual(self.outcomes(report)["swap"], "unverifiable")
        system.probe = probe(swap_kib=1024 * 1024)
        _, report = self.report(system, "verify", "--swap-gib", "2", "--probe-image", "busybox")
        self.assertEqual(self.outcomes(report)["swap"], "pending-restart")
        system.probe = probe(swap_kib=2 * 1024 * 1024)
        _, report = self.report(system, "verify", "--swap-gib", "2", "--probe-image", "busybox")
        self.assertEqual(self.outcomes(report)["swap"], "applied")

    def test_remote_and_wrong_engine_are_refused_without_touching_the_local_config(self):
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        small = engine(cpus=2, memory=1.9)
        home = str(self.profile)
        systems = {
            "remote daemon": self.windows(info=small, endpoint=SECRET_ENDPOINT),
            "DOCKER_HOST elsewhere": FakeSystem(info=small, environ={"USERPROFILE": home,
                                                                     "DOCKER_HOST": "ssh://someone@build-box"}),
            "tc.host elsewhere": FakeSystem(info=small, home=home, environ={"USERPROFILE": home}, files={
                os.path.join(home, ".testcontainers.properties"): "tc.host=" + SECRET_ENDPOINT}),
            "engine down": self.windows(info={}),
            "Hyper-V backend": self.windows(info=engine(kernel="6.12.54-linuxkit", cpus=2, memory=1.9)),
            "Windows containers": self.windows(info=engine(kernel="10.0 26100", os_type="windows", cpus=2, memory=1.9)),
            "unknown local engine": self.windows(info=engine(kernel="6.1.0-custom", cpus=2, memory=1.9)),
            "inside WSL": FakeSystem(platform="linux", info=small, endpoint="unix:///var/run/docker.sock",
                                     environ={"USERPROFILE": home}, files={"/proc/version": "microsoft"}),
            "no user profile": FakeSystem(info=small),
        }
        for name, system in systems.items():
            with self.subTest(name=name):
                status, report = self.report(system, "apply", "--confirm-wsl-global")
                self.assertEqual(status, setup.EXIT_REFUSED)
                self.assertTrue(report["refusal"].startswith("no configuration was written: "))
                self.assertEqual((report["config"]["route"], report["writes"]), ("none", []))
                self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=2GB\n")
                self.assertEqual(self.names(), [".wslconfig"])
                self.assertNotIn("planned", self.outcomes(report).values())

    def test_remote_engine_observations_still_apply(self):
        status, report = self.report(self.windows(endpoint=SECRET_ENDPOINT), "verify", "--disk-free-gib", "0")
        self.assertEqual((status, self.outcomes(report)), (setup.EXIT_OK, {"cpus": "applied", "memory": "applied"}))
        _, report = self.report(self.windows(endpoint=SECRET_ENDPOINT, info=engine(memory=1.9)), "verify")
        self.assertEqual(self.outcomes(report)["memory"], "manual")
        self.assertIn("machine or VM that runs the selected daemon", report["manual_steps"][0])

    def test_macos_gets_manual_steps_and_engine_verification(self):
        system = FakeSystem(platform="darwin", endpoint=SECRET_SOCKET, home=str(self.profile),
                            info=engine(kernel="6.12.54-linuxkit", cpus=2, memory=1.9),
                            files={setup.ADMIN_SETTINGS["darwin"]: "{}"})
        status, report = self.report(system, "apply", "--confirm-wsl-global")
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertEqual(self.outcomes(report), {"cpus": "manual", "memory": "manual", "disk-free": "unverifiable"})
        self.assertIn("Settings > Resources > Advanced and set CPU limit to at least 4, Memory limit to at least "
                      "4.00 GiB", report["manual_steps"][0])
        self.assertEqual(report["managed_settings"], "present")
        self.assertIn("administrator", report["manual_steps"][1])
        self.assertEqual(self.names(), [])
        system.info = engine(kernel="6.12.54-linuxkit", cpus=8, memory=7.75)
        status, report = self.report(system, "apply", "--disk-free-gib", "0")
        self.assertEqual((status, report["satisfied"], report["manual_steps"]), (setup.EXIT_OK, True, []))

    def test_malformed_or_unsafe_config_is_refused_with_manual_lines(self):
        small = engine(cpus=2, memory=1.9)
        for content, problem in ((b"[wsl2]\nnot a setting\n", "malformed"), (b"[wsl2]\nmemory=plenty\n", "ambiguous"),
                                 (b"[wsl2]\n[wsl2]\n", "ambiguous"), ("[wsl2]\n".encode("utf-16-le"), "encoding")):
            with self.subTest(content=content):
                self.config.write_bytes(content)
                status, report = self.report(self.windows(info=small), "apply", "--confirm-wsl-global")
                self.assertEqual((status, report["config"]["problem"]), (setup.EXIT_REFUSED, problem))
                self.assertEqual(self.outcomes(report)["memory"], "manual")
                self.assertIn("set: processors=4; memory=4GB", report["manual_steps"][0])
                self.assertEqual(self.config.read_bytes(), content)
                self.assertEqual(self.names(), [".wslconfig"])

    def test_config_that_cannot_be_edited_in_place_is_refused(self):
        managed = self.profile / "managed"
        managed.write_bytes(b"[wsl2]\nmemory=2GB\n")
        os.symlink(managed, self.config)
        status, report = self.report(self.windows(info=engine(cpus=2, memory=1.9)), "apply", "--confirm-wsl-global")
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("symbolic link", report["refusal"])
        self.assertEqual(managed.read_bytes(), b"[wsl2]\nmemory=2GB\n")

    def test_partial_failure_is_reported_and_rolled_back(self):
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        real = wslconfig.Files.write_atomic

        def write_atomic(files, path, data):
            if path.endswith(wslconfig.STATE_SUFFIX):
                raise OSError("injected failure at " + path)
            real(files, path, data)

        wslconfig.Files.write_atomic = write_atomic
        self.addCleanup(setattr, wslconfig.Files, "write_atomic", real)
        status, text = self.run_main(self.windows(info=engine(cpus=2, memory=1.9)), "apply", "--confirm-wsl-global")
        self.assertEqual(status, setup.EXIT_FAILED)
        self.assertIn("Failed: the state file could not be written (OSError); the config is back", text)
        self.assertNotIn(str(self.profile), text)
        self.assertIn("Written: nothing", text)
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=2GB\n")


class RestoreTest(SetupCase):
    def test_restore_refuses_a_later_edit_and_keeps_it(self):
        system = self.windows(info=engine(cpus=2, memory=1.9))
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        self.run_main(system, "apply", "--confirm-wsl-global")
        self.config.write_bytes(b"[wsl2]\nmemory=12GB\nprocessors=4\n")
        status, report = self.report(system, "restore")
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("restoring would overwrite that edit", report["refusal"])
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=12GB\nprocessors=4\n")

    def test_restore_does_not_need_the_engine(self):
        system = self.windows(info=engine(cpus=2, memory=1.9))
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        self.run_main(system, "apply", "--confirm-wsl-global")
        system.info = {}
        self.assertEqual(self.run_main(system, "restore")[0], setup.EXIT_OK)
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=2GB\n")
        self.assertEqual(self.names(), [".wslconfig"])

    def test_restore_is_refused_off_windows_and_without_a_record(self):
        self.assertEqual(self.run_main(FakeSystem(platform="darwin"), "restore")[0], setup.EXIT_REFUSED)
        status, report = self.report(self.windows(), "restore")
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("no recorded change", report["refusal"])


class VerifyTest(SetupCase):
    def test_kernel_features_are_reported_apart_from_limits(self):
        system = self.windows(info=engine(cpus=2, memory=1.9), probe=probe(config="CONFIG_BLK_DEV_NBD=m\n"))
        status, report = self.report(system, "verify", "--profile", "block-fault-delay", "--kernel-feature", "nbd",
                                     "--probe-image", "busybox")
        self.assertEqual(status, setup.EXIT_UNMET)
        self.assertEqual(self.outcomes(report), {"cpus": "planned", "memory": "planned", "disk-free": "applied",
                                                 "kernel:dm-delay": "unavailable", "kernel:nbd": "unverifiable"})
        kinds = {item["name"]: item["kind"] for item in report["settings"]}
        self.assertEqual((kinds["memory"], kinds["kernel:dm-delay"]), ("limit", "kernel-feature"))
        self.assertTrue(any("not a limit" in step and "kernel:dm-delay" in step for step in report["manual_steps"]))
        # No config key raises a kernel feature, so nothing is planned for it.
        self.assertEqual(report["planned_changes"], ["processors=4", "memory=4GB"])

        system.info = engine(cpus=4, memory=3.8)
        system.probe = probe()
        status, report = self.report(system, "verify", "--profile", "block-fault-delay", "--probe-image", "busybox")
        self.assertEqual((status, self.outcomes(report)["kernel:dm-delay"]), (setup.EXIT_OK, "available"))

    def test_probe_starts_isolated_one_shot_containers_without_pulling(self):
        system = self.windows(probe=probe())
        self.report(system, "verify", "--probe-image", "busybox")
        runs = [command for command in system.commands if "run" in command]
        self.assertEqual(len(runs), 3)
        for command in runs:
            self.assertEqual(command[:6], ["docker", "run", "--rm", "--pull=never", "--network", "none"])

    def test_probe_failure_and_unreadable_values_stay_unverifiable(self):
        broken = probe()
        broken.update({("df", "/"): b"garbage", ("cat", "/proc/config.gz"): b"not gzip"})
        cases = (None, broken)
        for answers in cases:
            with self.subTest(answers=answers is not None):
                status, report = self.report(self.windows(probe=answers), "verify", "--profile", "block-fault-delay",
                                             "--probe-image", "absent")
                self.assertEqual(status, setup.EXIT_UNMET)
                self.assertEqual(self.outcomes(report)["disk-free"], "unverifiable")
                self.assertEqual(self.outcomes(report)["kernel:dm-delay"], "unverifiable")
                self.assertEqual(report["probe"]["image_ran"], answers is not None)

    def test_low_disk_is_a_manual_step_that_deletes_nothing(self):
        _, report = self.report(self.windows(probe=probe(free_kib=1024)), "verify", "--probe-image", "busybox")
        self.assertEqual(self.outcomes(report)["disk-free"], "manual")
        self.assertTrue(any("This script deletes nothing" in step for step in report["manual_steps"]))

    def test_unreachable_engine_is_unverifiable_not_unmet_limits(self):
        status, report = self.report(self.windows(info={}), "verify")
        self.assertEqual(status, setup.EXIT_UNMET)
        self.assertEqual(set(self.outcomes(report).values()), {"unverifiable"})


class PrivacyTest(SetupCase):
    def test_outputs_leave_out_endpoints_paths_names_and_engine_errors(self):
        home = str(self.profile)
        self.config.write_bytes(b"[wsl2]\nmemory=plenty\n")
        systems = (
            FakeSystem(platform="darwin", endpoint=SECRET_SOCKET, context="work-laptop", home=home),
            FakeSystem(home=home, environ={"USERPROFILE": home}, files={
                os.path.join(home, ".testcontainers.properties"): "tc.host=" + SECRET_ENDPOINT}),
            FakeSystem(environ={"DOCKER_HOST": SECRET_ENDPOINT, "USERPROFILE": home}, info={}),
            self.windows(info=engine(cpus=2, memory=1.9)),
        )
        for system in systems:
            for command in ("plan", "apply", "verify", "restore"):
                for fmt in ("text", "json"):
                    with self.subTest(command=command, fmt=fmt):
                        _, text = self.run_main(system, command, "--confirm-wsl-global", "--format", fmt)
                        for private in (SECRET_ENDPOINT, "build-box", SECRET_SOCKET, "someone", "work-laptop", home,
                                        "private-engine-name", "plenty"):
                            self.assertNotIn(private, text)


if __name__ == "__main__":
    unittest.main()
