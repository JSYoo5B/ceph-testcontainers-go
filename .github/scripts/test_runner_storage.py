import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("runner_storage", Path(__file__).with_name("runner_storage.py"))
storage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(storage)

GIB = 1 << 30


def statvfs(total, avail, files=1000, ffree=900):
    """An os.statvfs_result-shaped value with 4 KiB fragments."""
    return os.statvfs_result((4096, 4096, total // 4096, avail // 4096, avail // 4096, files, ffree, ffree, 0, 255))


class Docker:
    def __init__(self, root="/var/lib/docker"):
        self.root, self.calls = root, []
        self.df = [{"Type": "Images", "TotalCount": "3", "Active": "0", "Size": "2.1GB", "Reclaimable": "2.1GB"},
                   {"Type": "Containers", "TotalCount": "0", "Active": "0", "Size": "0B", "Reclaimable": "0B"}]

    def __call__(self, command, timeout=None):
        self.calls.append(command)
        if command[:2] == ["docker", "info"]:
            return self.root + "\n"
        if command[:3] == ["docker", "system", "df"]:
            return "".join(json.dumps(row) + "\n" for row in self.df)
        if command[:2] == ["sudo", "rm"]:
            return ""
        raise AssertionError("unexpected command: %r" % command)


class FilesystemTests(unittest.TestCase):
    def test_the_share_is_floored_like_the_mons(self):
        fs = storage.filesystem("/x", lambda path: statvfs(72 * GIB, int(72 * GIB * 0.139)))
        self.assertEqual(fs["available_percent"], 13)
        self.assertTrue(fs["below_mon_warn"])
        self.assertFalse(fs["below_mon_crit"])
        fs = storage.filesystem("/x", lambda path: statvfs(100 * GIB, 30 * GIB))
        self.assertEqual(fs["available_percent"], 30)
        self.assertFalse(fs["below_mon_warn"])

    def test_an_empty_filesystem_is_refused(self):
        with self.assertRaises(ValueError):
            storage.filesystem("/x", lambda path: statvfs(0, 0))


class RecordTests(unittest.TestCase):
    def test_a_receipt_per_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            docker = Docker()
            report = storage.record(directory, "images", docker, lambda path: statvfs(72 * GIB, 20 * GIB))
            saved = json.loads((Path(directory) / "storage-images.json").read_text())
            self.assertEqual(saved["filesystem"]["path"], "/var/lib/docker")
            self.assertEqual(saved["docker"]["Images"]["Size"], "2.1GB")
            self.assertEqual(report["filesystem"]["available_percent"], 27)
            # A second receipt of one phase never replaces the first.
            with self.assertRaises(FileExistsError):
                storage.record(directory, "images", docker, lambda path: statvfs(72 * GIB, 10 * GIB))
            self.assertEqual(json.loads((Path(directory) / "storage-images.json").read_text())["filesystem"]["available_percent"], 27)

    def test_phase_names_and_engine_answers_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            for phase in ("free", "Upper", "../x", ""):
                with self.assertRaises(ValueError, msg=phase):
                    storage.record(directory, phase, Docker(), lambda path: statvfs(GIB, GIB))
            with self.assertRaises(ValueError):
                storage.record(directory, "after", Docker(root="relative"), lambda path: statvfs(GIB, GIB))
            docker = Docker()
            docker.df.append(dict(docker.df[0]))
            with self.assertRaises(ValueError):
                storage.record(directory, "after", docker, lambda path: statvfs(GIB, GIB))


class FreeTests(unittest.TestCase):
    def test_only_a_github_hosted_runner_is_freed(self):
        with tempfile.TemporaryDirectory() as directory:
            for environ in ({}, {"RUNNER_ENVIRONMENT": "self-hosted"}):
                docker = Docker()
                with self.assertRaises(RuntimeError):
                    storage.free(directory, environ, docker, lambda path: statvfs(GIB, GIB), lambda path: True)
                self.assertFalse([c for c in docker.calls if c[0] == "sudo"])

    def test_removes_only_the_listed_directories_that_exist(self):
        with tempfile.TemporaryDirectory() as directory:
            docker = Docker()
            sizes = iter([statvfs(72 * GIB, 9 * GIB), statvfs(72 * GIB, 40 * GIB)])
            present = {"/usr/local/lib/android", "/opt/ghc"}
            report = storage.free(directory, {"RUNNER_ENVIRONMENT": "github-hosted"}, docker,
                                  lambda path: next(sizes), present.__contains__, iter([0, 42]).__next__)
            removals = [c for c in docker.calls if c[0] == "sudo"]
            self.assertEqual(removals, [["sudo", "rm", "-rf", "--one-file-system", "/usr/local/lib/android", "/opt/ghc"]])
            self.assertEqual(report["removed"], ["/usr/local/lib/android", "/opt/ghc"])
            self.assertEqual((report["before"]["available_percent"], report["filesystem"]["available_percent"]), (12, 55))
            self.assertTrue((Path(directory) / "storage-free.json").exists())

    def test_nothing_present_removes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            docker = Docker()
            storage.free(directory, {"RUNNER_ENVIRONMENT": "github-hosted"}, docker,
                         lambda path: statvfs(GIB, GIB), lambda path: False)
            self.assertFalse([c for c in docker.calls if c[0] == "sudo"])

    def test_every_listed_directory_is_absolute_and_outside_docker(self):
        for path in storage.PREINSTALLED:
            self.assertTrue(path.startswith("/") and ".." not in path, path)
            self.assertFalse(path.startswith("/var/lib/docker"), path)



# A decision fixture supplies explicit filesystem observations; it never
# invokes Docker, removes runner directories or allocates pressure space.
def snapshot(available, total=100 * GIB, verified=True, other=None):
    def fs(device, avail, mon=True):
        return {"device": device, "paths": ["/docker" if mon else "/images"], "mon_store": mon,
                "available_bytes": avail, "total_bytes": total, "free_inodes": 1000000}
    rows = [fs(1, available)]
    if other is not None:
        rows.append(fs(2, other, False))
    return {"layout": {"verified": verified, "reasons": []}, "filesystems": rows}


BUDGET = {"runtime_bytes": 4 * GIB, "image_prepare_bytes": 8 * GIB, "minimum_free_inodes": 100000}


class PreparationTests(unittest.TestCase):
    def test_sufficient_space_skips_without_calling_reclamation(self):
        with tempfile.TemporaryDirectory() as directory:
            report = storage.prepare(directory, BUDGET, snapshot=lambda: snapshot(60 * GIB),
                                     reclaim=lambda: self.fail("must not reclaim"))
            self.assertTrue(report["passed"])
            self.assertEqual((report["decision"], report["reclamation_seconds"]), ("skip", 0))
            self.assertEqual(report["headroom"]["requirements"][0]["required_bytes"], 42 * GIB)
            self.assertFalse((Path(directory) / "storage-free.json").exists())

    def test_low_space_unknown_work_and_unknown_layout_keep_cleanup(self):
        for budget, before, reason in ((BUDGET, snapshot(40 * GIB), "insufficient-headroom"),
                                       (None, snapshot(80 * GIB), "unknown-workload"),
                                       (BUDGET, snapshot(80 * GIB, verified=False), "unknown-storage-layout")):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                calls, states = [], iter([before, snapshot(65 * GIB)])
                def reclaim():
                    calls.append(True)
                    return {"seconds": 73, "removed": ["/usr/share/dotnet"]}
                report = storage.prepare(directory, budget, snapshot=lambda: next(states), reclaim=reclaim)
                self.assertTrue(report["passed"])
                self.assertEqual((report["decision"], report["reason"]), ("reclaim", reason))
                self.assertEqual((calls, report["reclamation_seconds"]), ([True], 73))

    def test_failed_post_cleanup_guard_preserves_failure_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            report = storage.prepare(directory, BUDGET, snapshot=lambda: snapshot(41 * GIB),
                                     reclaim=lambda: {"seconds": 1, "removed": []})
            self.assertFalse(report["passed"])
            self.assertIn("insufficient", report["error"])
            self.assertFalse(json.loads((Path(directory) / "storage-prepare.json").read_text())["passed"])

    def test_separate_filesystem_and_inode_pressure_cannot_borrow_mon_space(self):
        observed = snapshot(90 * GIB, other=11 * GIB)
        result = storage.headroom(observed, BUDGET, True)
        self.assertFalse(result["passed"])
        self.assertFalse(result["requirements"][1]["passed"])
        observed["filesystems"][1]["available_bytes"] = 90 * GIB
        observed["filesystems"][1]["free_inodes"] = 99999
        self.assertFalse(storage.headroom(observed, BUDGET, True)["passed"])

    def test_always_is_a_control_and_post_image_check_uses_runtime_reserve(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []
            report = storage.prepare(directory, BUDGET, "always", snapshot=lambda: snapshot(90 * GIB),
                                     reclaim=lambda: calls.append(1) or {"seconds": 3, "removed": []})
            self.assertTrue(report["passed"])
            self.assertEqual(calls, [1])
            ready = storage.ready(directory, BUDGET, snapshot=lambda: snapshot(34 * GIB))
            self.assertTrue(ready["passed"])
            self.assertEqual(ready["headroom"]["image_prepare_bytes"], 0)
            with tempfile.TemporaryDirectory() as failed:
                self.assertFalse(storage.ready(failed, BUDGET, snapshot=lambda: snapshot(33 * GIB))["passed"])

    def test_pressure_is_bounded_above_ceph_floor_and_cleanup_requires_ownership(self):
        self.assertEqual(storage.pressure_size(snapshot(60 * GIB), BUDGET), 19 * GIB)
        for available in (41 * GIB, 90 * GIB):
            with self.assertRaises(RuntimeError):
                storage.pressure_size(snapshot(available), BUDGET)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qualification-pressure.bin"
            path.write_bytes(b"owned test allocation")
            with self.assertRaises(RuntimeError):
                storage.release_pressure(directory)
            self.assertTrue(path.exists())
            identity = path.stat()
            marker = Path(directory) / "storage-pressure.json"
            marker.write_text(json.dumps({"device": identity.st_dev, "inode": identity.st_ino + 1}))
            with self.assertRaises(RuntimeError):
                storage.release_pressure(directory)
            marker.write_text(json.dumps({"device": identity.st_dev, "inode": identity.st_ino}))
            storage.release_pressure(directory)
            self.assertFalse(path.exists())
            storage.release_pressure(directory)


class SnapshotTests(unittest.TestCase):
    def test_devices_are_deduplicated_and_remote_context_cannot_authorize_skip(self):
        from types import SimpleNamespace
        def run(command):
            if command[:2] == ["docker", "info"]:
                return json.dumps({"DockerRootDir": "/docker", "OSType": "linux", "Architecture": "x86_64"})
            return json.dumps([{"Endpoints": {"docker": {"Host": endpoint}}}])
        def layout(info, runner):
            return {"roots": ["/docker", "/volumes", "/images"], "verified": True, "reasons": []}
        def stat(path):
            return SimpleNamespace(st_dev=2 if path == "/images" else 1)
        for endpoint, verified in (("unix:///var/run/docker.sock", True), ("ssh://remote", False)):
            observed = storage.storage_snapshot(run, lambda path: statvfs(100 * GIB, 80 * GIB), stat, {}, layout)
            self.assertEqual(observed["layout"]["verified"], verified)
            self.assertEqual(len(observed["filesystems"]), 2)
            self.assertEqual(observed["filesystems"][0]["paths"], ["/docker", "/volumes"])
            self.assertTrue(observed["filesystems"][0]["mon_store"])
            self.assertFalse(observed["filesystems"][1]["mon_store"])


class QualificationTests(unittest.TestCase):
    def test_pinned_compiled_cohort_matches_and_changes_expire_it(self):
        import hashlib
        import copy
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".github").mkdir()
            (root / "compile").mkdir()
            pins = {"images": {"20.2.4": {"official": {"essential": "sha256:one", "client": "sha256:two"}}}}
            pins_file = root / ".github/ceph-role-images.json"
            pins_file.write_text(json.dumps(pins))
            sources = {"fixture.go": "synthetic-source-hash"}
            build = {"passed": True, "profile": {"id": "short-watch", "package": "./internal/integration", "image_roles": "essential,client"},
                     "binary": {"tests": ["TestWatcher"], "sources": sources,
                                "build_key": {"goos": "linux", "goarch": "amd64", "tags": "ci,watch"}}}
            budget = dict(BUDGET, schema=1, profile="short-watch", package="./internal/integration", tests=["TestWatcher"],
                          roles="essential,client", releases=["20.2.4"], platform="linux/amd64", tags="ci,watch",
                          sources_sha256=hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                          role_pins_sha256=hashlib.sha256(pins_file.read_bytes()).hexdigest())
            (root / ".github/runner-storage.json").write_text(json.dumps(budget))
            compile_file = root / "compile/compile-report.json"
            def check(value, **kwargs):
                compile_file.write_text(json.dumps(value))
                return storage.qualification(root / "compile", "20.2.4", "essential,client", root=root, **kwargs)
            self.assertIsNotNone(check(build))
            self.assertIsNotNone(check(build, digests="essential=sha256:one,client=sha256:two"))
            self.assertIsNone(check(build, digests="essential=sha256:other"))
            self.assertIsNone(check(build, tests="TestOther"))
            for field, value in (("sources", {"fixture.go": "changed"}), ("tests", ["TestWatcher", "TestNew"])):
                changed = copy.deepcopy(build)
                changed["binary"][field] = value
                self.assertIsNone(check(changed))
            for key, value in (("goarch", "arm64"), ("tags", "different")):
                changed = copy.deepcopy(build)
                changed["binary"]["build_key"][key] = value
                self.assertIsNone(check(changed))
            pins_file.write_text(json.dumps(pins) + "\n")
            self.assertIsNone(check(build))

    def test_missing_receipts_or_budget_are_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(storage.qualification(Path(directory), "20.2.4", "essential,client", root=Path(directory)))

if __name__ == "__main__":
    unittest.main()
