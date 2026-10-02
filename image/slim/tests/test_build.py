import contextlib
import hashlib
import io
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import build


class PackageInputTests(unittest.TestCase):
    def parse(self, arguments):
        with contextlib.redirect_stderr(io.StringIO()):
            return build.arguments(arguments)

    def test_source_modes_and_base_must_agree(self):
        for arguments in ([], ["--deb-packages", "ceph.deb"],
                          ["--source-image", "quay.io/ceph/ceph:v20.2.4", "--base-image", "ubuntu:24.04"],
                          ["--source-image", "source", "--deb-directory", "packages", "--base-image", "base"]):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                self.parse(arguments)
        self.assertEqual(self.parse(["--deb-directory", "packages", "--base-image", "ubuntu:24.04"]).base_image,
                         "ubuntu:24.04")

    def test_untrusted_filename_never_becomes_staged_name(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / 'ceph $(touch unexpected); "patched".deb'
            package.write_bytes(b"company artifact")
            args = self.parse(["--deb-packages", str(package), "--base-image", "ubuntu:24.04"])
            files = build.deb_inputs(args)
            self.assertEqual(files[0][1]["filename"], package.name)
            self.assertEqual(files[0][1]["staged_filename"], "0000.deb")
            self.assertEqual(files[0][1]["sha256"], hashlib.sha256(package.read_bytes()).hexdigest())

    def test_empty_directory_and_duplicate_input_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse(["--deb-directory", directory, "--base-image", "ubuntu:24.04"])
            with self.assertRaises(build.BuildError):
                build.deb_inputs(args)
            package = Path(directory) / "ceph.deb"
            package.write_bytes(b"package")
            args = self.parse(["--deb-packages", str(package), str(package), "--base-image", "ubuntu:24.04"])
            with self.assertRaises(build.BuildError):
                build.deb_inputs(args)

    def test_plan_archive_cannot_escape_build_context(self):
        for value in ("/groups/common.tar", "groups/../outside.tar", "manifests/common.tar", "groups/common.zip"):
            with self.subTest(value=value), self.assertRaises(build.BuildError):
                build.archive_path(value, "groups")


if __name__ == "__main__":
    unittest.main()
