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


if __name__ == "__main__":
    unittest.main()
