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
import kernel
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
                 context="desktop-linux", environ=None, files=None, probe=None, home="/nonexistent-home", vm=None):
        self.platform = platform
        self.machine = "amd64"
        self.info = engine() if info is None else info
        self.endpoint = endpoint
        self.context = context
        self.environ = dict(environ or {})
        self.files = dict(files or {})
        self.probe = probe
        self._home = home
        self.vm = vm
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
        if "volume" in args or "--privileged" in args or any(arg.startswith("ceph-testcontainers-") for arg in args):
            if self.vm is None:
                return 125, b""
            return self.vm.run(args)
        if "run" in args:
            program = args[args.index("--entrypoint") + 1]
            target = args[-1]
            if self.probe is None or (program, target) not in self.probe:
                return 125, b""
            return 0, self.probe[(program, target)]
        raise AssertionError("unexpected command: %r" % (args,))


class FakeVM:
    """The kernel of a synthetic engine VM, driven by the helper's Docker commands."""

    def __init__(self, config=None, files=(), loaded=(), parameters=None, unload=True, modprobe=True):
        self.config = dict(config or {})
        self.files = set(files)
        self.loaded = set(loaded)
        self.parameters = dict(parameters or {})
        self.unload = unload
        self.modprobe = modprobe
        self.boot = "boot-1"
        self.refuse_entry = False
        self.fail_load = set()
        self.fail_set = set()
        self.fail_state_write = False
        self.busy = set()
        self.volume = None  # None, or {"labels": {...}, "state": text or None}
        self.fail_volume = set()  # volume subcommands that fail: "ls", "inspect"
        self.changes = []

    def run(self, args):
        if "volume" in args:
            action = args[args.index("volume") + 1]
            if action in self.fail_volume:
                return 1, b""
            if action == "ls":
                return 0, b"some-other-volume\n" + (kernel.STATE_VOLUME.encode() + b"\n" if self.volume else b"")
            if action == "inspect":
                if self.volume is None:
                    return 1, b""
                return 0, json.dumps({"Labels": self.volume["labels"]}).encode()
            if action == "create":
                key, value = args[args.index("--label") + 1].split("=")
                self.volume = self.volume or {"labels": {key: value}, "state": None}
                return 0, b""
            self.volume = None
            return 0, b""
        program = args[args.index("--entrypoint") + 1]
        tail = args[args.index("--entrypoint") + 3:]
        if "--privileged" not in args:
            if program == "cat":
                return (0, self.volume["state"].encode()) if self.volume and self.volume["state"] else (1, b"")
            if self.fail_state_write:
                return 1, b""
            self.volume["state"] = tail[-1]
            return 0, b""
        if self.refuse_entry:
            return 1, b""
        if program == "sh":  # the inspection script
            lines = ["boot\t" + self.boot, "modules_disabled\t0", "entered\tyes", "modules\tread"]
            lines += ["config\t%s=%s" % pair for pair in self.config.items()]
            lines += ["loaded\t" + name for name in sorted(self.loaded)]
            lines += ["file\tkernel/drivers/x/%s.ko.xz" % name for name in sorted(self.files)]
            lines += ["modprobe\tyes"] if self.modprobe else []
            for item in tail[3:]:
                kind, name = item.split(":", 1)
                if kind == "module" and name in self.loaded and self.unload:
                    lines.append("refcnt\t%s\t%d" % (name, 1 if name in self.busy else 0))
                if kind == "param":
                    lines.append("param\t%s\t%s" % (name, self.parameters.get(name.replace("/", "."), "")))
            return 0, "\n".join(lines).encode()
        if "modprobe" in tail:
            name = tail[-1]
            if "-r" in tail:
                if not self.unload or name in self.busy:
                    return 1, b""
                self.loaded.discard(name)
                self.changes.append("unload " + name)
                return 0, b""
            if name not in self.files or name in self.fail_load:
                return 1, b""
            self.loaded.add(name)
            self.changes.append("load " + name)
            return 0, b""
        name, value = tail[-2][len("/proc/sys/"):].replace("/", "."), tail[-1]
        if name in self.fail_set:
            return 1, b""
        self.parameters[name] = value
        self.changes.append("set %s=%s" % (name, value))
        return 0, b""


BLOCK = "CONFIG_BLK_DEV_LOOP=y\nCONFIG_BLK_DEV_DM=y\n"


def probe(swap_kib=1048572, free_kib=40 * 1024 * 1024, config="CONFIG_DM_DELAY=y\nCONFIG_BLK_DEV_NBD=m\n" + BLOCK):
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
                                             "disk_free_gib": 0, "kernel_features": ["nbd"],
                                             "kernel_parameters": {}})

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


class UnreadableInputTest(SetupCase):
    def test_unreadable_testcontainers_properties_do_not_fall_back_to_another_engine(self):
        home = str(self.profile)
        system = FakeSystem(home=home, environ={"USERPROFILE": home}, info=engine(cpus=2, memory=1.9))
        system.exists = lambda path: path == os.path.join(home, ".testcontainers.properties")
        status, report = self.report(system, "apply", "--confirm-wsl-global")
        self.assertEqual((status, report["engine"]["selected_by"], report["config"]["route"]),
                         (setup.EXIT_REFUSED, "unknown", "none"))
        self.assertEqual(self.names(), [])

    def test_unreadable_config_or_state_is_refused_not_treated_as_missing(self):
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        real = wslconfig.Files.read
        self.addCleanup(setattr, wslconfig.Files, "read", real)
        for suffix in (".wslconfig", wslconfig.STATE_SUFFIX):
            def read(files, path, suffix=suffix):
                if path.endswith(suffix):
                    raise PermissionError("injected failure at " + path)
                return real(files, path)

            wslconfig.Files.read = read
            # Without a readable record restore has nothing it may undo; an unreadable record is a failure.
            restore = setup.EXIT_FAILED if suffix == wslconfig.STATE_SUFFIX else setup.EXIT_REFUSED
            for command, expected in (("plan", setup.EXIT_OK), ("apply", setup.EXIT_REFUSED), ("restore", restore)):
                with self.subTest(suffix=suffix, command=command):
                    status, text = self.run_main(self.windows(info=engine(cpus=2, memory=1.9)), command,
                                                 "--confirm-wsl-global")
                    self.assertEqual(status, expected)
                    self.assertIn("PermissionError", text)
                    self.assertNotIn(str(self.profile), text)
                    self.assertNotIn("planned", text.split("Settings:")[1].split("Written:")[0])
            self.assertEqual(self.config.read_bytes(), b"[wsl2]\nmemory=2GB\n")
            self.assertEqual(self.names(), [".wslconfig"])

    def test_wsl_apply_can_be_extended_with_another_owned_key(self):
        machine = vm(files={"nbd"})
        system = self.windows(info=engine(cpus=2, memory=1.9), vm=machine)
        self.config.write_bytes(b"[wsl2]\nmemory=2GB\n")
        self.assertEqual(self.run_main(system, "apply", "--confirm-wsl-global")[0], setup.EXIT_OK)
        status, report = self.report(system, "apply", "--confirm-wsl-global", "--persist-kernel-modules",
                                     "--kernel-feature", "nbd", "--kernel-helper-image", "busybox")
        self.assertEqual((status, report["writes"]), (setup.EXIT_OK, ["loadkernelmodules"]))
        self.assertEqual(report["config"]["owned_keys"], ["loadkernelmodules", "memory", "processors"])
        self.assertEqual(self.run_main(system, "restore")[0], setup.EXIT_OK)
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
        system = self.windows(info=engine(cpus=2, memory=1.9), probe=probe(config="CONFIG_BLK_DEV_NBD=m\n" + BLOCK))
        status, report = self.report(system, "verify", "--profile", "block-fault-delay", "--kernel-feature", "nbd",
                                     "--probe-image", "busybox")
        self.assertEqual(status, setup.EXIT_UNMET)
        self.assertEqual(self.outcomes(report), {"cpus": "planned", "memory": "planned", "disk-free": "applied",
                                                 "kernel:loop": "available", "kernel:dm": "available",
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


MAC = dict(platform="darwin", endpoint=SECRET_SOCKET)
KERNEL = ("--disk-free-gib", "0", "--kernel-helper-image", "busybox")
WANT = ("--kernel-feature", "nbd", "--kernel-feature", "krbd", "--kernel-feature", "dm-delay", "--kernel-feature",
        "fuse", "--kernel-parameter", "fs.aio-max-nr=1048576")


def vm(**arguments):
    defaults = dict(config={"CONFIG_FUSE_FS": "y", "CONFIG_BLK_DEV_NBD": "m", "CONFIG_BLK_DEV_RBD": "m",
                            "CONFIG_MODULE_UNLOAD": "y"},
                    files={"nbd"}, parameters={"fs.aio-max-nr": "65536"})
    defaults.update(arguments)
    return FakeVM(**defaults)


class KernelStepTest(SetupCase):
    def mac(self, machine, **arguments):
        options = dict(MAC, info=engine(kernel="6.12.54-linuxkit"), home=str(self.profile), vm=machine)
        options.update(arguments)
        return FakeSystem(**options)

    def test_plan_reports_each_state_and_changes_nothing(self):
        machine = vm()
        system = self.mac(machine)
        status, report = self.report(system, *(WANT + KERNEL))
        self.assertEqual(status, setup.EXIT_OK)
        self.assertEqual(self.outcomes(report), {
            "cpus": "applied", "memory": "applied",
            "kernel:fuse": "available",       # built in
            "kernel:nbd": "planned",          # module file present, not loaded
            "kernel:krbd": "unavailable",     # built as a module, file not shipped
            "kernel:dm-delay": "unavailable",  # not built at all
            "sysctl:fs.aio-max-nr": "planned"})
        persists = {item["name"]: item.get("persists") for item in report["settings"]}
        self.assertEqual((persists["kernel:fuse"], persists["kernel:nbd"]), (True, False))
        self.assertEqual((machine.changes, machine.volume), ([], None))
        self.assertTrue(any("apply-kernel" in step and "kernel:nbd, sysctl:fs.aio-max-nr" in step
                            for step in report["manual_steps"]))
        self.assertTrue(any("kernel:dm-delay, kernel:krbd cannot be provided" in step and "does not build" in step
                            for step in report["manual_steps"]))
        for command in system.commands:
            if "run" in command:
                self.assertEqual(command[1:6], ["run", "--rm", "--pull=never", "--network", "none"])

    def test_without_the_helper_no_privileged_container_starts(self):
        system = self.mac(vm())
        _, report = self.report(system, *(WANT + ("--disk-free-gib", "0")))
        self.assertEqual({self.outcomes(report)[name] for name in ("kernel:nbd", "sysctl:fs.aio-max-nr")},
                         {"unverifiable"})
        self.assertFalse(any("run" in command or "volume" in command for command in system.commands))
        self.assertTrue(any("--kernel-helper-image" in step for step in report["manual_steps"]))

    def test_apply_kernel_needs_the_scope_confirmation(self):
        machine = vm()
        status, report = self.report(self.mac(machine), "apply-kernel", *(WANT + KERNEL))
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("every container on this engine", report["refusal"])
        self.assertEqual((machine.changes, machine.volume), ([], None))

    def test_apply_kernel_loads_sets_records_and_repeats_as_a_no_op(self):
        machine = vm()
        system = self.mac(machine)
        status, report = self.report(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        # The features this engine cannot provide stay open, so the run is not a full success of the profile,
        # but the step itself did what it could and failed nothing.
        self.assertEqual(status, setup.EXIT_OK)
        self.assertEqual(machine.changes, ["load nbd", "set fs.aio-max-nr=1048576"])
        self.assertEqual(report["kernel"]["actions"], [{"item": "kernel:nbd", "result": "loaded"},
                                                       {"item": "sysctl:fs.aio-max-nr", "result": "set"}])
        outcomes = self.outcomes(report)
        self.assertEqual((outcomes["kernel:nbd"], outcomes["sysctl:fs.aio-max-nr"], outcomes["kernel:krbd"]),
                         ("applied", "applied", "unavailable"))
        persists = {item["name"]: item.get("persists") for item in report["settings"]}
        self.assertEqual((persists["kernel:nbd"], persists["sysctl:fs.aio-max-nr"]), (False, False))
        self.assertEqual(report["kernel"]["owned"], ["module:nbd", "sysctl:fs.aio-max-nr"])
        self.assertTrue(any("gone when the engine VM restarts" in step for step in report["manual_steps"]))
        state = json.loads(machine.volume["state"])
        self.assertEqual(state["parameters"], {"fs.aio-max-nr": {"previous": 65536, "written": 1048576}})

        machine.changes.clear()
        status, report = self.report(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        self.assertEqual((status, machine.changes, report["kernel"]["actions"]), (setup.EXIT_OK, [], []))
        self.assertEqual(self.report(system, "verify", "--kernel-feature", "nbd", "--kernel-parameter",
                                     "fs.aio-max-nr=1048576", *KERNEL)[0], setup.EXIT_OK)

    def test_already_satisfied_items_are_not_touched_or_owned(self):
        machine = vm(loaded={"nbd"}, parameters={"fs.aio-max-nr": "2097152"})
        status, report = self.report(self.mac(machine), "apply-kernel", "--confirm-engine-kernel", "--kernel-feature",
                                     "nbd", "--kernel-parameter", "fs.aio-max-nr=1048576", *KERNEL)
        self.assertEqual((status, machine.changes, machine.volume, report["satisfied"]), (setup.EXIT_OK, [], None, True))
        details = {item["name"]: item["effective"] for item in report["settings"]}
        self.assertEqual(details["kernel:nbd"], "module already loaded")
        self.assertIn("already at least the requested value", details["sysctl:fs.aio-max-nr"])

    def test_failed_actions_are_reported_and_not_owned(self):
        machine = vm()
        machine.fail_load.add("nbd")
        machine.fail_set.add("fs.aio-max-nr")
        status, report = self.report(self.mac(machine), "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        self.assertEqual(status, setup.EXIT_FAILED)
        outcomes = self.outcomes(report)
        self.assertEqual((outcomes["kernel:nbd"], outcomes["sysctl:fs.aio-max-nr"]), ("failed", "failed"))
        self.assertEqual((machine.changes, machine.volume, report["kernel"]["owned"]), ([], None, []))

    def test_unwritable_record_puts_the_changes_back(self):
        machine = vm()
        machine.fail_state_write = True
        status, report = self.report(self.mac(machine), "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        self.assertEqual(status, setup.EXIT_FAILED)
        self.assertIn("put back", report["failure"])
        self.assertEqual(machine.changes, ["load nbd", "set fs.aio-max-nr=1048576", "unload nbd",
                                           "set fs.aio-max-nr=65536"])
        self.assertEqual((machine.loaded, machine.parameters["fs.aio-max-nr"]), (set(), "65536"))

    def test_restore_kernel_puts_parameters_back_and_unloads_unused_modules(self):
        machine = vm()
        system = self.mac(machine)
        self.run_main(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        status, report = self.report(system, "restore-kernel", *KERNEL)
        self.assertEqual(status, setup.EXIT_OK)
        self.assertEqual(report["kernel"]["actions"], [{"item": "sysctl:fs.aio-max-nr", "result": "restored"},
                                                       {"item": "module:nbd", "result": "unloaded"}])
        self.assertEqual((machine.loaded, machine.parameters["fs.aio-max-nr"], machine.volume), (set(), "65536", None))
        status, report = self.report(system, "restore-kernel", *KERNEL)
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("no recorded kernel change", report["refusal"])

    def test_restore_kernel_reports_what_it_cannot_undo(self):
        cases = {
            "this kernel cannot unload modules": lambda machine: setattr(machine, "unload", False),
            "in use": lambda machine: machine.busy.add("nbd"),
            "changed after this tool set it": lambda machine: machine.parameters.update({"fs.aio-max-nr": "3000000"}),
        }
        for expected, change in cases.items():
            with self.subTest(expected=expected):
                machine = vm()
                system = self.mac(machine)
                self.run_main(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
                change(machine)
                before = dict(machine.parameters)
                status, report = self.report(system, "restore-kernel", *KERNEL)
                self.assertEqual(status, setup.EXIT_UNMET)
                kept = [action for action in report["kernel"]["actions"] if action["result"].startswith("kept")]
                self.assertEqual(len(kept), 1)
                self.assertIn(expected, kept[0]["result"])
                if "changed" in expected:
                    self.assertEqual(machine.parameters, before)
                else:
                    self.assertIn("nbd", machine.loaded)
                self.assertIsNone(machine.volume)

    def test_a_restarted_vm_voids_the_record(self):
        machine = vm()
        system = self.mac(machine)
        self.run_main(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        machine.boot, machine.loaded, machine.parameters = "boot-2", set(), {"fs.aio-max-nr": "65536"}
        machine.changes.clear()
        _, report = self.report(system, *(WANT + KERNEL))
        self.assertEqual((report["kernel"]["owned"], self.outcomes(report)["kernel:nbd"]), ([], "planned"))
        status, report = self.report(system, "restore-kernel", *KERNEL)
        self.assertEqual((status, machine.changes, machine.volume), (setup.EXIT_OK, [], None))
        self.assertIn("restarted", report["kernel"]["actions"][0]["result"])

    def test_owned_parameter_changed_later_blocks_another_apply(self):
        machine = vm()
        system = self.mac(machine)
        self.run_main(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        machine.parameters["fs.aio-max-nr"] = "100"
        machine.changes.clear()
        status, report = self.report(system, "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        self.assertEqual((status, machine.changes), (setup.EXIT_FAILED, []))
        self.assertIn("changed after this tool set it", report["failure"])

    def test_apply_can_be_extended_without_restoring_earlier_changes(self):
        first = ("--kernel-parameter", "fs.aio-max-nr=1048576")
        both = ["sysctl:fs.aio-max-nr", "sysctl:vm.max_map_count"]
        cases = {
            "another parameter": (first + ("--kernel-parameter", "vm.max_map_count=262144"),
                                  ["set vm.max_map_count=262144"], both),
            "a module": (first + ("--kernel-feature", "nbd"), ["load nbd"], ["module:nbd", "sysctl:fs.aio-max-nr"]),
            "only the new parameter": (("--kernel-parameter", "vm.max_map_count=262144"),
                                       ["set vm.max_map_count=262144"], both),
        }
        for name, (second, changes, owned) in cases.items():
            with self.subTest(name=name):
                machine = vm(parameters={"fs.aio-max-nr": "65536", "vm.max_map_count": "65530"})
                system = self.mac(machine)
                self.assertEqual(self.run_main(system, "apply-kernel", "--confirm-engine-kernel", *(first + KERNEL))[0],
                                 setup.EXIT_OK)
                machine.changes.clear()
                status, report = self.report(system, "apply-kernel", "--confirm-engine-kernel", *(second + KERNEL))
                self.assertEqual((status, machine.changes), (setup.EXIT_OK, changes))
                self.assertEqual(report["kernel"]["owned"], owned)
                # The first change is still owned with its first previous value, and one restore undoes both.
                self.assertEqual(json.loads(machine.volume["state"])["parameters"]["fs.aio-max-nr"],
                                 {"previous": 65536, "written": 1048576})
                self.assertEqual(self.run_main(system, "restore-kernel", *KERNEL)[0], setup.EXIT_OK)
                self.assertEqual((machine.parameters, machine.loaded),
                                 ({"fs.aio-max-nr": "65536", "vm.max_map_count": "65530"}, set()))

    def test_owned_parameter_changed_later_is_seen_even_when_not_requested(self):
        machine = vm(parameters={"fs.aio-max-nr": "65536", "vm.max_map_count": "65530"})
        system = self.mac(machine)
        self.run_main(system, "apply-kernel", "--confirm-engine-kernel", "--kernel-parameter", "fs.aio-max-nr=1048576",
                      *KERNEL)
        machine.parameters["fs.aio-max-nr"] = "100"
        machine.changes.clear()
        status, report = self.report(system, "apply-kernel", "--confirm-engine-kernel", "--kernel-parameter",
                                     "vm.max_map_count=262144", *KERNEL)
        self.assertEqual((status, machine.changes), (setup.EXIT_FAILED, []))
        self.assertIn("fs.aio-max-nr changed after this tool set it", report["failure"])

    def test_failed_volume_inspection_is_not_an_absent_volume(self):
        label = dict([kernel.STATE_LABEL.split("=")])
        earlier = json.dumps({"version": 1, "boot": "boot-1", "modules": [],
                              "parameters": {"vm.max_map_count": {"previous": 65530, "written": 262144}}})
        volumes = {"no volume": None, "foreign volume": {"labels": {}, "state": "theirs"},
                   "own volume": {"labels": label, "state": earlier}}
        for failing in ("ls", "inspect"):
            for name, volume in volumes.items():
                if failing == "inspect" and volume is None:
                    continue  # a listing without the name never reaches the inspection
                for command in ("apply-kernel", "restore-kernel"):
                    with self.subTest(failing=failing, volume=name, command=command):
                        machine = vm()
                        machine.volume = None if volume is None else dict(volume)
                        machine.fail_volume.add(failing)
                        status, report = self.report(self.mac(machine), command, "--confirm-engine-kernel",
                                                     *(WANT + KERNEL))
                        self.assertEqual(status, setup.EXIT_REFUSED)
                        self.assertIn("could not be inspected", report["refusal"])
                        self.assertEqual((machine.changes, machine.volume), ([], volume))

    def test_failed_volume_inspection_stops_kernel_apply_before_any_write(self):
        # The library call itself, with a foreign volume whose inspection alone fails.
        machine = vm()
        machine.volume = {"labels": {}, "state": "theirs"}
        machine.fail_volume.add("inspect")
        helper = kernel.Helper(self.mac(machine), ["docker"], "busybox")
        actions, error = kernel.apply(helper, ["nbd"], {"fs.aio-max-nr": 1048576})
        self.assertEqual(actions, [])
        self.assertIn("could not be inspected", error)
        self.assertEqual((machine.changes, machine.volume), ([], {"labels": {}, "state": "theirs"}))

    def test_state_is_never_written_into_a_volume_that_appeared_without_the_label(self):
        machine = vm()
        helper = kernel.Helper(self.mac(machine), ["docker"], "busybox")
        real = machine.run

        def run(args):
            if "volume" in args and "create" in args:  # someone else creates the name first
                machine.volume = {"labels": {}, "state": "theirs"}
                return 0, b""
            return real(args)

        machine.run = run
        actions, error = kernel.apply(helper, ["nbd"], {"fs.aio-max-nr": 1048576})
        self.assertEqual(actions, [])
        self.assertIn("put back", error)
        self.assertEqual(machine.volume, {"labels": {}, "state": "theirs"})
        self.assertEqual((machine.loaded, machine.parameters["fs.aio-max-nr"]), (set(), "65536"))

    def test_unreadable_loaded_modules_are_not_taken_for_unloaded(self):
        machine = vm()
        system = self.mac(machine)
        self.run_main(system, "apply-kernel", "--confirm-engine-kernel", "--kernel-feature", "nbd", *KERNEL)
        real = machine.run

        def run(args):
            status, output = real(args)
            if "--privileged" in args and args[args.index("--entrypoint") + 1] == "sh":
                output = b"\n".join(line for line in output.split(b"\n")
                                    if not line.startswith((b"modules\t", b"loaded\t")))
            return status, output

        machine.run = run
        _, report = self.report(system, "--kernel-feature", "nbd", *KERNEL)
        self.assertEqual(self.outcomes(report)["kernel:nbd"], "unverifiable")
        status, report = self.report(system, "restore-kernel", *KERNEL)
        self.assertEqual(status, setup.EXIT_UNMET)
        self.assertIn("could not be read", report["kernel"]["actions"][0]["result"])
        self.assertIn("nbd", machine.loaded)

    def test_foreign_or_unreadable_state_volume_is_refused(self):
        for volume, expected in (({"labels": {}, "state": "{}"}, "not this tool's"),
                                 ({"labels": dict([kernel.STATE_LABEL.split("=")]), "state": "garbage"}, "unreadable")):
            with self.subTest(expected=expected):
                machine = vm()
                machine.volume = volume
                for command in ("apply-kernel", "restore-kernel"):
                    status, report = self.report(self.mac(machine), command, "--confirm-engine-kernel",
                                                 *(WANT + KERNEL))
                    self.assertEqual(status, setup.EXIT_REFUSED)
                    self.assertIn(expected, report["refusal"])
                self.assertEqual((machine.changes, machine.volume), ([], volume))

    def test_other_engines_are_not_modified_and_get_operator_commands(self):
        native = engine(kernel="6.8.0-generic", system="Ubuntu 24.04.3 LTS")
        systems = {
            "remote": FakeSystem(endpoint=SECRET_ENDPOINT, vm=vm()),
            "linux-engine": FakeSystem(platform="linux", info=native, endpoint="unix:///var/run/docker.sock", vm=vm()),
            "unrecognized": FakeSystem(platform="darwin", info=native, endpoint="unix:///x/docker.sock", vm=vm()),
        }
        for backend, system in systems.items():
            for command in ("apply-kernel", "restore-kernel"):
                with self.subTest(backend=backend, command=command):
                    status, report = self.report(system, command, "--confirm-engine-kernel", *(WANT + KERNEL))
                    self.assertEqual((status, report["engine"]["backend"]), (setup.EXIT_REFUSED, backend))
                    self.assertIn("not this script's to change", report["refusal"])
                    self.assertEqual((system.vm.changes, system.vm.volume), ([], None))
                    self.assertEqual(self.outcomes(report)["kernel:nbd"], "manual")
                    self.assertTrue(any("sudo modprobe nbd; sudo sysctl -w fs.aio-max-nr=1048576" in step
                                        for step in report["manual_steps"]))

    def test_missing_helper_and_refused_entry_change_nothing(self):
        machine = vm()
        status, report = self.report(self.mac(machine), "apply-kernel", "--confirm-engine-kernel",
                                     *(WANT + ("--disk-free-gib", "0")))
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("--kernel-helper-image", report["refusal"])
        machine.refuse_entry = True
        status, report = self.report(self.mac(machine), "apply-kernel", "--confirm-engine-kernel", *(WANT + KERNEL))
        self.assertEqual(status, setup.EXIT_REFUSED)
        self.assertIn("did not enter the engine VM", report["refusal"])
        self.assertEqual(self.outcomes(report)["kernel:nbd"], "unverifiable")
        self.assertEqual(machine.changes, [])

    def test_kernel_apply_itself_skips_what_is_not_loadable_or_already_there(self):
        machine = vm(files={"nbd", "dm_crypt"}, loaded={"dm_crypt"}, parameters={"fs.aio-max-nr": "65536"})
        helper = kernel.Helper(self.mac(machine), ["docker"], "busybox")
        actions, error = kernel.apply(helper, ["fuse", "krbd", "dm-delay", "dm-crypt", "nbd"], {"fs.aio-max-nr": 1024})
        self.assertEqual((actions, error), ([{"item": "kernel:nbd", "result": "loaded"}], None))
        self.assertEqual(machine.changes, ["load nbd"])

    def test_module_present_but_not_loadable_is_unavailable(self):
        _, report = self.report(self.mac(vm(modprobe=False)), "--kernel-feature", "nbd", *KERNEL)
        self.assertEqual(self.outcomes(report)["kernel:nbd"], "unavailable")

    def test_invalid_kernel_parameters_are_rejected_before_any_command(self):
        system = self.mac(vm())
        for value in ("fs.aio-max-nr", "fs.aio-max-nr=lots", "aio=1", "fs/../x=1", "fs.aio max=1"):
            with self.subTest(value=value), redirect_stderr(StringIO()):
                self.assertEqual(self.run_main(system, "--kernel-parameter", value)[0], setup.EXIT_REFUSED)
        self.assertEqual(system.commands, [])

    def test_wsl_modules_persist_through_loadkernelmodules_only_on_request(self):
        machine = vm(files={"nbd", "rbd", "libceph"})
        system = self.windows(vm=machine)
        self.config.write_bytes(b"[wsl2]\r\nloadKernelModules=tun\r\n")
        want = ("--kernel-feature", "nbd", "--kernel-feature", "krbd", "--kernel-feature", "fuse") + KERNEL
        _, report = self.report(system, *want)
        self.assertEqual(report["planned_changes"], [])
        _, report = self.report(system, "--persist-kernel-modules", *want)
        self.assertEqual(report["planned_changes"], ["loadKernelModules=tun,rbd,nbd"])
        status, report = self.report(system, "apply", "--confirm-wsl-global", "--persist-kernel-modules", *want)
        self.assertEqual((status, report["writes"]), (setup.EXIT_OK, ["loadkernelmodules"]))
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\r\nloadKernelModules=tun,rbd,nbd\r\n")
        self.assertEqual(machine.changes, [])
        status, report = self.report(system, "apply", "--confirm-wsl-global", "--persist-kernel-modules", *want)
        self.assertEqual(report["writes"], [])
        self.assertEqual(self.run_main(system, "restore")[0], setup.EXIT_OK)
        self.assertEqual(self.config.read_bytes(), b"[wsl2]\r\nloadKernelModules=tun\r\n")

    def test_kernel_outputs_stay_free_of_private_values(self):
        system = self.mac(vm(), context="work-laptop")
        for command in ("plan", "apply-kernel", "restore-kernel"):
            for fmt in ("text", "json"):
                _, text = self.run_main(system, command, "--confirm-engine-kernel", "--format", fmt, *(WANT + KERNEL))
                for private in (SECRET_SOCKET, "someone", "work-laptop", "boot-1", str(self.profile)):
                    self.assertNotIn(private, text)


if __name__ == "__main__":
    unittest.main()
