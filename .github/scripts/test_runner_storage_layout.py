import importlib.util
import json
from pathlib import Path
import subprocess
import unittest


SPEC = importlib.util.spec_from_file_location("runner_storage_layout", Path(__file__).with_name("runner_storage_layout.py"))
layout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(layout)

DOCKER = "/var/lib/docker"
MOUNTS = "1 0 8:1 / / rw - ext4 /dev/root rw\n"


class ReadOnlyHost:
    """Explicit command/file answers exercise discovery without a Docker daemon.

    Unexpected commands fail the test, including any attempted host mutation.
    No image, mount, process or filesystem behavior is simulated beyond the
    literal discovery responses supplied by each test.
    """

    def __init__(self, volumes=(), processes="", files=None, paths=()):
        self.paths = {DOCKER, *paths}
        self.files = {"/proc/self/mountinfo": MOUNTS, **(files or {})}
        self.commands = {
            ("docker", "volume", "ls", "--quiet"): "".join(v["Name"] + "\n" for v in volumes),
            ("ps", "-eo", "args="): processes,
        }
        if volumes:
            self.commands[("docker", "volume", "inspect", "--", *(v["Name"] for v in volumes))] = json.dumps(volumes)
        self.calls = []

    def run(self, command):
        self.calls.append(command)
        result = self.commands[tuple(command)]
        if isinstance(result, Exception):
            raise result
        return result

    def read(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def discover(self, **info):
        return layout.storage_roots({"DockerRootDir": DOCKER, "Driver": "overlay2", **info},
                                    self.run, self.paths.__contains__, self.read)


class StorageLayoutTests(unittest.TestCase):
    def test_classic_local_volumes_and_nested_mounts_keep_separate_filesystems(self):
        volume = {"Name": "data", "Driver": "local", "Options": None, "Mountpoint": "/mnt/volumes/data"}
        nested = DOCKER + "/volumes/other/_data"
        host = ReadOnlyHost(volumes=[volume], paths=[volume["Mountpoint"], nested], files={
            "/proc/self/mountinfo": MOUNTS +
            "2 1 8:2 / /mnt/volumes/data rw - ext4 /dev/volume rw\n" +
            "3 1 8:3 / /var/lib/docker/volumes/other/_data rw - ext4 /dev/other rw\n" +
            "4 1 8:4 / /unrelated rw - ext4 /dev/unrelated rw\n"})
        result = host.discover()
        self.assertTrue(result["verified"], result)
        self.assertEqual(result["roots"], [DOCKER, volume["Mountpoint"], nested])
        self.assertEqual(result["reasons"], [])
        self.assertEqual(len(host.calls), 2)

    def test_mountinfo_escapes_are_decoded_without_prefix_sibling_matches(self):
        path = DOCKER + "/volume with space"
        host = ReadOnlyHost(paths=[path], files={"/proc/self/mountinfo": MOUNTS +
            "2 1 8:2 / /var/lib/docker/volume\\040with\\040space rw - ext4 /dev/x rw\n" +
            "3 1 8:3 / /var/lib/docker-other rw - ext4 /dev/y rw\n"})
        self.assertEqual(host.discover()["roots"], [DOCKER, path])

    def test_plugin_and_option_backed_volumes_require_fallback_even_when_mounted(self):
        for driver, options in (("plugin", None), ("local", {"type": "nfs", "device": "server:/data"})):
            with self.subTest(driver=driver, options=options):
                volume = {"Name": "data", "Driver": driver, "Options": options, "Mountpoint": DOCKER + "/volumes/data/_data"}
                result = ReadOnlyHost(volumes=[volume], paths=[volume["Mountpoint"]]).discover()
                self.assertFalse(result["verified"])
                self.assertIn("volume-storage-not-qualified", result["reasons"])
                self.assertIn(volume["Mountpoint"], result["roots"])

    def test_invalid_or_unreadable_mountinfo_cannot_verify_layout(self):
        for contents in ("", "broken\n", "1 0 8:1 / relative rw - ext4 /dev/root rw\n"):
            with self.subTest(contents=contents):
                result = ReadOnlyHost(files={"/proc/self/mountinfo": contents}).discover()
                self.assertFalse(result["verified"])
                self.assertIn("mount-table-unavailable", result["reasons"])
        host = ReadOnlyHost()
        host.files.clear()
        self.assertIn("mount-table-unavailable", host.discover()["reasons"])

    def test_containerd_explicit_root_is_recorded_but_not_associated_with_docker(self):
        for flag in ("--root /srv/containerd", "--root=/srv/containerd"):
            with self.subTest(flag=flag):
                host = ReadOnlyHost(processes="/usr/bin/containerd " + flag + "\n",
                                    paths=["/srv/containerd", "/var/lib/containerd"])
                result = host.discover(Driver="overlayfs", DriverStatus=[["driver-type", "io.containerd.snapshotter.v1"]])
                self.assertFalse(result["verified"])
                self.assertIn("/srv/containerd", result["roots"])
                self.assertIn("/var/lib/containerd", result["roots"])
                self.assertIn("containerd-daemon-association-unverified", result["reasons"])
                self.assertNotIn("containerd-root-unresolved", result["reasons"])

    def test_containerd_explicit_config_root_and_cli_precedence(self):
        for flag in ("--config /etc/fixture-containerd.toml", "-c /etc/fixture-containerd.toml"):
            with self.subTest(flag=flag):
                host = ReadOnlyHost(processes="containerd " + flag,
                                    files={"/etc/fixture-containerd.toml": 'version = 2\nroot = "/srv/config-root"\n'},
                                    paths=["/srv/config-root"])
                result = host.discover(Driver="overlayfs")
                self.assertIn("/srv/config-root", result["roots"])
                self.assertFalse(result["verified"])
        host = ReadOnlyHost(processes="containerd --root=/srv/cli-root --config=/unreadable.toml",
                            paths=["/srv/cli-root"])
        self.assertIn("/srv/cli-root", host.discover(Driver="overlayfs")["roots"])

    def test_containerd_candidates_never_substitute_for_an_unresolved_root(self):
        host = ReadOnlyHost(processes="containerd\nnot-containerd --root /unrelated\n",
                            paths=["/var/lib/containerd", DOCKER + "/containerd/daemon"])
        result = host.discover(Driver="overlayfs")
        self.assertEqual(result["roots"], [DOCKER, "/var/lib/containerd", DOCKER + "/containerd/daemon"])
        self.assertIn("containerd-root-unresolved", result["reasons"])
        self.assertFalse(result["verified"])

    def test_containerd_default_config_matches_docker_socket_and_storage_root(self):
        host = ReadOnlyHost(processes="/usr/bin/containerd\n", paths=["/srv/content"], files={
            "/etc/containerd/config.toml": 'root = "/srv/content"\nimports = []\n[grpc]\naddress = "/run/fixture.sock"\n'})
        result = host.discover(Driver="overlayfs", DriverStatus=[["driver-type", "io.containerd.snapshotter.v1"]],
                               Containerd={"Address": "/run/fixture.sock"})
        self.assertTrue(result["verified"], result)
        self.assertIn("/srv/content", result["roots"])

    def test_containerd_cli_address_and_root_override_config(self):
        for flag in ("--address=/run/fixture.sock", "-a /run/fixture.sock"):
            with self.subTest(flag=flag):
                host = ReadOnlyHost(processes="containerd --root=/srv/cli " + flag,
                                    paths=["/srv/cli"], files={"/etc/containerd/config.toml":
                                    'root = "/unavailable"\n[grpc]\naddress = "/run/other.sock"\n'})
                result = host.discover(Driver="overlayfs", DriverStatus=[["driver-type", "io.containerd.snapshotter.v1"]],
                                       Containerd={"Address": "/run/fixture.sock"})
                self.assertTrue(result["verified"], result)
                self.assertIn("/srv/cli", result["roots"])

    def test_containerd_wrong_remote_or_ambiguous_socket_does_not_verify(self):
        for endpoint, processes in (("/run/wrong.sock", "containerd"),
                                    ("tcp://127.0.0.1:1234", "containerd"),
                                    ("/run/fixture.sock", "containerd\ncontainerd")):
            with self.subTest(endpoint=endpoint, processes=processes):
                host = ReadOnlyHost(processes=processes, paths=["/srv/content"], files={
                    "/etc/containerd/config.toml": 'root = "/srv/content"\n[grpc]\naddress = "/run/fixture.sock"\n'})
                result = host.discover(Driver="overlayfs", DriverStatus=[["driver-type", "io.containerd.snapshotter.v1"]],
                                       Containerd={"Address": endpoint})
                self.assertFalse(result["verified"])
                self.assertIn("containerd-daemon-association-unverified", result["reasons"])

    def test_containerd_imports_proxy_and_snapshotter_overrides_keep_fallback(self):
        for extra in ('imports = ["/etc/other.toml"]\n', '[proxy_plugins.remote]\ntype = "snapshot"\n',
                      '[plugins."io.containerd.snapshotter.v1.overlayfs"]\nroot_path = "/srv/snapshots"\n'):
            with self.subTest(extra=extra):
                config = 'root = "/srv/content"\n' + extra + '[grpc]\naddress = "/run/fixture.sock"\n'
                host = ReadOnlyHost(processes="containerd", paths=["/srv/content", "/srv/snapshots"],
                                    files={"/etc/containerd/config.toml": config})
                result = host.discover(Driver="overlayfs", DriverStatus=[["driver-type", "io.containerd.snapshotter.v1"]],
                                       Containerd={"Address": "/run/fixture.sock"})
                self.assertFalse(result["verified"])
                if "root_path" in extra:
                    self.assertIn("/srv/snapshots", result["roots"])

    def test_missing_root_or_failed_volume_discovery_refuses_verification(self):
        host = ReadOnlyHost()
        host.paths.clear()
        result = host.discover()
        self.assertEqual(result["roots"], [])
        self.assertIn("docker-root-unavailable", result["reasons"])
        for failure in (RuntimeError("daemon unavailable"), subprocess.TimeoutExpired("docker", 1)):
            with self.subTest(failure=type(failure).__name__):
                host = ReadOnlyHost()
                host.commands[("docker", "volume", "ls", "--quiet")] = failure
                result = host.discover()
                self.assertFalse(result["verified"])
                self.assertIn("volume-discovery-unavailable", result["reasons"])


if __name__ == "__main__":
    unittest.main()
