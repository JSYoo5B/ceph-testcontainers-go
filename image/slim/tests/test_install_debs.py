"""Pure installer tests: no APT, dpkg, Docker, root, or network required."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "install_debs.py"
SPEC = importlib.util.spec_from_file_location("ceph_install_debs", SCRIPT)
installer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = installer
SPEC.loader.exec_module(installer)


def control(package="ceph-mon", version="19.2.3-0ubuntu0.24.04.1", architecture="arm64",
            source="ceph"):
    fields = ["Package: " + package, "Version: " + version, "Architecture: " + architecture]
    if source is not None:
        fields.append("Source: " + source)
    return "\n".join(fields) + "\nDescription: example\n folded description\n"


def installed_line(package="ceph-mon", version="19.2.3-0ubuntu0.24.04.1", architecture="arm64",
                   source="ceph", source_version=None, status="installed", error_flag="ok"):
    return "\t".join((package, version, architecture, status, error_flag,
                      source, source_version or version)) + "\n"


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.debs = self.directory / "debs"
        self.debs.mkdir()
        self.controls = {}
        self.query_arguments = []
        self.inventory = installed_line()

    def stage(self, staged="0000.deb", original="ceph-mon_19.2.3_arm64.deb", metadata=None):
        payload = (staged + ": local archive bytes").encode()
        (self.debs / staged).write_bytes(payload)
        self.controls[staged] = metadata or control()
        return {"filename": original, "staged_filename": staged,
                "sha256": hashlib.sha256(payload).hexdigest()}

    def query(self, *arguments):
        self.query_arguments.append(arguments)
        if arguments == ("dpkg", "--print-architecture"):
            return "arm64\n"
        if arguments[:2] == ("dpkg-deb", "--field"):
            self.assertTrue(Path(arguments[2]).is_absolute())
            return self.controls[Path(arguments[2]).name]
        if arguments[:2] == ("dpkg-query", "-W"):
            self.assertEqual(arguments[2], "--showformat=" + installer.INSTALLED_FORMAT)
            return self.inventory
        self.fail("Unexpected command: " + str(arguments))

    def collect(self, entries, required=()):
        return installer.collect_local_packages(self.debs, entries, "arm64", self.query, required)

    def inputs(self, entries):
        return {"packages": entries, "base_image": "ubuntu:24.04",
                "base_image_id": "sha256:base", "platform": "linux/arm64"}

    def run_installer(self, entries, install_fn):
        return installer.install_and_record(self.debs, self.inputs(entries),
                                            self.directory / "source-packages.json",
                                            self.query, install_fn,
                                            self.directory / "policy-rc.d",
                                            required_packages=("ceph-mon",))

    def test_metadata_overrides_filename_and_retains_source_version(self):
        entry = self.stage(original="arbitrary original name.deb",
                           metadata=control(source="ceph (19.2.3-0ubuntu0.24.04.1)"))
        package = self.collect([entry])[0]
        self.assertEqual(package.identity, "ceph-mon:arm64")
        self.assertEqual(package.filename, "arbitrary original name.deb")
        self.assertEqual(package.source, "ceph")
        self.assertEqual(package.source_version, "19.2.3-0ubuntu0.24.04.1")
        self.assertEqual(installer.parse_control(control())["Description"], "example\nfolded description")

    def test_source_defaults_and_architecture_all(self):
        entry = self.stage(metadata=control(package="python3-cephfs", architecture="all", source=None))
        package = self.collect([entry])[0]
        self.assertEqual(package.architecture, "all")
        self.assertEqual(package.source, "python3-cephfs")
        self.assertEqual(package.source_version, package.version)

    def test_hash_is_checked_before_dpkg_metadata(self):
        entry = self.stage()
        entry["sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
            self.collect([entry])
        self.assertEqual(self.query_arguments, [])

    def test_duplicate_identity_rejected_even_for_different_versions(self):
        first = self.stage()
        second = self.stage("0001.deb", "other.deb", control(version="20.2.4-1"))
        with self.assertRaisesRegex(RuntimeError, "Duplicate local Debian package identity: ceph-mon:arm64"):
            self.collect([first, second])

    def test_foreign_architecture_rejected(self):
        entry = self.stage(metadata=control(architecture="amd64"))
        with self.assertRaisesRegex(RuntimeError, "Foreign .deb architecture"):
            self.collect([entry])

    def test_missing_role_root_rejected_locally(self):
        entry = self.stage()
        with self.assertRaisesRegex(RuntimeError, "must be supplied locally: ceph-mds"):
            self.collect([entry], required=("ceph-mon", "ceph-mds"))

    def test_unlisted_archive_rejected(self):
        entry = self.stage()
        self.stage("0001.deb", "unexpected.deb")
        with self.assertRaisesRegex(RuntimeError, "differ from the input manifest: 0001.deb"):
            self.collect([entry])

    def test_staged_path_traversal_and_symlinks_rejected(self):
        entry = self.stage()
        unsafe = dict(entry, staged_filename="../0000.deb")
        with self.assertRaisesRegex(RuntimeError, "staged_filename must be a .deb basename"):
            self.collect([unsafe])
        actual = self.directory / "actual.deb"
        (self.debs / "0000.deb").rename(actual)
        (self.debs / "0000.deb").symlink_to(actual)
        with self.assertRaisesRegex(RuntimeError, "not a regular file"):
            self.collect([entry])

    def test_duplicate_control_field_rejected(self):
        entry = self.stage(metadata=control() + "Package: malicious\n")
        with self.assertRaisesRegex(RuntimeError, "duplicate Debian control field"):
            self.collect([entry])

    def test_installed_version_and_source_must_match_local_archive(self):
        package = self.collect([self.stage()])
        for field, value in (("version", "20.2.4-1"), ("source", "unrelated"),
                             ("source_version", "19.2.4-1")):
            with self.subTest(field=field):
                inventory = installer.parse_installed(installed_line())
                inventory["ceph-mon:arm64"][field] = value
                with self.assertRaisesRegex(RuntimeError, "unexpected " + field):
                    installer.audit_installed(package, inventory)

    def test_remote_ceph_dependency_rejected_but_distro_dependency_allowed(self):
        packages = self.collect([self.stage()])
        distro = installed_line("libstdc++6", "14.2.0-1", source="gcc-14")
        installer.audit_installed(packages, installer.parse_installed(installed_line() + distro))
        remote_ceph = installed_line("librados2", source="ceph")
        with self.assertRaisesRegex(RuntimeError, "not supplied locally: librados2:arm64"):
            installer.audit_installed(packages, installer.parse_installed(installed_line() + remote_ceph))

    def test_not_fully_installed_or_broken_package_rejected(self):
        packages = self.collect([self.stage()])
        with self.assertRaisesRegex(RuntimeError, "not fully installed"):
            installer.audit_installed(packages, installer.parse_installed(installed_line(status="unpacked")))
        with self.assertRaisesRegex(RuntimeError, "broken installed Debian package"):
            installer.parse_installed(installed_line(error_flag="reinstreq"))

    def test_install_absolute_argv_and_write_audited_provenance(self):
        entry = self.stage(original="spaces and $(not-a-command).deb")
        commands = []

        def fake_install(arguments):
            commands.append(arguments)
            policy = self.directory / "policy-rc.d"
            self.assertEqual(policy.read_text(), "#!/bin/sh\nexit 101\n")
            self.assertEqual(stat.S_IMODE(policy.stat().st_mode), 0o755)

        result = self.run_installer([entry], fake_install)
        self.assertEqual(commands, [["apt-get", "-y", "--no-install-recommends", "--reinstall",
                                     "--allow-downgrades", "install",
                                     str((self.debs / "0000.deb").resolve())]])
        self.assertFalse((self.directory / "policy-rc.d").exists())
        self.assertEqual(result["base_image"], "ubuntu:24.04")
        self.assertEqual(result["base_image_id"], "sha256:base")
        self.assertEqual(result["platform"], "linux/arm64")
        self.assertEqual(result["native_architecture"], "arm64")
        self.assertEqual(result["packages"][0]["filename"], entry["filename"])
        self.assertEqual(result["packages"][0]["sha256"], entry["sha256"])
        self.assertNotIn("path", result["packages"][0])
        self.assertEqual(json.loads((self.directory / "source-packages.json").read_text()), result)

    def test_restore_original_policy_after_apt_failure(self):
        policy = self.directory / "policy-rc.d"
        policy.write_bytes(b"original policy\x00bytes")
        policy.chmod(0o700)

        def fail_install(arguments):
            raise subprocess.CalledProcessError(100, arguments)

        with self.assertRaises(subprocess.CalledProcessError):
            self.run_installer([self.stage()], fail_install)
        self.assertEqual(policy.read_bytes(), b"original policy\x00bytes")
        self.assertEqual(stat.S_IMODE(policy.stat().st_mode), 0o700)
        self.assertFalse((self.directory / "source-packages.json").exists())
        self.assertEqual(list(self.directory.glob(".ceph-policy-*")), [])

    def test_restore_original_symlink_policy(self):
        policy = self.directory / "policy-rc.d"
        policy.symlink_to("missing-relative-original")
        self.run_installer([self.stage()], lambda arguments: None)
        self.assertTrue(policy.is_symlink())
        self.assertEqual(os.readlink(policy), "missing-relative-original")

    def test_archive_mutation_during_apt_rejected_before_provenance(self):
        entry = self.stage()

        def mutate_archive(arguments):
            (self.debs / "0000.deb").write_bytes(b"replaced during install")

        with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
            self.run_installer([entry], mutate_archive)
        self.assertFalse((self.directory / "source-packages.json").exists())
        self.assertFalse((self.directory / "policy-rc.d").exists())

    def test_repository_ceph_dependency_prevents_success_manifest(self):
        self.inventory += installed_line("ceph-base", source="ceph")
        with self.assertRaisesRegex(RuntimeError, "not supplied locally: ceph-base:arm64"):
            self.run_installer([self.stage()], lambda arguments: None)
        self.assertFalse((self.directory / "source-packages.json").exists())


if __name__ == "__main__":
    unittest.main()
