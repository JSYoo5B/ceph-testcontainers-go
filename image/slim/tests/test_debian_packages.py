#!/usr/bin/env python3
"""Pure fixtures for the dpkg closure and role filesystem contracts."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import assemble
import debian_packages as debian
import package_roles


def record(name, architecture="amd64", version="1", depends="", pre_depends="",
           provides="", multi_arch="", essential=False, source=None, status="installed"):
    fields = dict.fromkeys(debian.METADATA_FIELDS, "")
    fields.update({"Package": name, "Architecture": architecture, "Version": version,
                   "db:Status-Status": status, "db:Status-Eflag": "ok",
                   "Depends": depends, "Pre-Depends": pre_depends, "Provides": provides,
                   "Multi-Arch": multi_arch, "Essential": "yes" if essential else "no",
                   "Installed-Size": "2", "source:Package": source or name,
                   "source:Version": version})
    return "\t".join(fields[field] for field in debian.METADATA_FIELDS)


def inventory(*records):
    return debian.parse_inventory("\n".join(records))


def version_compare(installed, operator, required):
    # Fixtures need only integer versions; production uses dpkg, never this.
    installed, required = int(installed), int(required)
    return {"<<": installed < required, "<=": installed <= required,
            "=": installed == required, ">=": installed >= required,
            ">>": installed > required}[operator]


class MetadataTests(unittest.TestCase):
    def test_installed_states_multiarch_identities_and_source(self):
        packages = inventory(record("libfoo", multi_arch="same", source="ceph"),
                             record("libfoo", "arm64", multi_arch="same"),
                             record("removed", status="config-files"))
        self.assertEqual(set(packages), {"libfoo:amd64", "libfoo:arm64"})
        self.assertEqual(packages["libfoo:amd64"]["source_package"], "ceph")
        self.assertEqual(packages["libfoo:amd64"]["installed_size_bytes"], 2048)

    def test_files_preserve_multiarch_python_and_debian_licenses(self):
        packages = inventory(record("python3-rados", source="ceph"))
        payload = ("PACKAGE\tpython3-rados\tamd64\n/./\n"
                   "/usr/lib/python3/dist-packages/rados.cpython-312-x86_64-linux-gnu.so\n"
                   "/usr/share/doc/python3-rados/copyright\n")
        with patch.object(os.path, "isfile", return_value=False):
            debian.add_inventory_files(packages, payload)
        package = packages["python3-rados:amd64"]
        self.assertNotIn("/", package["files"])
        self.assertIn("/usr/lib/python3/dist-packages/rados.cpython-312-x86_64-linux-gnu.so", package["files"])
        self.assertEqual(package["licenses"], {"/usr/share/doc/python3-rados/copyright"})

    def test_reject_source_only_relationship_and_bad_provides(self):
        with self.assertRaisesRegex(RuntimeError, "Unsupported"):
            debian.parse_relationships("libfoo [amd64]")
        with self.assertRaisesRegex(RuntimeError, "Invalid"):
            debian.parse_relationships("virtual-api (>= 2)", provides=True)

    def test_real_dpkg_files_indentation_preserves_filename_trailing_space(self):
        packages = inventory(record("ceph-common", source="ceph"))
        payload = ("PACKAGE\tceph-common\tamd64\n /.\n /usr/bin/ceph\n"
                   " /usr/share/doc/ceph-common/copyright\n /usr/share/filename with trailing space \n")
        with patch.object(os.path, "isfile", return_value=False):
            debian.add_inventory_files(packages, payload)
        package = packages["ceph-common:amd64"]
        self.assertEqual(package["files"], ["/usr/bin/ceph", "/usr/share/doc/ceph-common/copyright",
                                           "/usr/share/filename with trailing space "])
        self.assertEqual(package["licenses"], {"/usr/share/doc/ceph-common/copyright"})


class ClosureTests(unittest.TestCase):
    def resolver(self, packages):
        return debian.DependencyResolver(packages, "amd64", version_compare)

    def test_alternatives_predepends_cycles_and_versioned_virtual(self):
        packages = inventory(
            record("daemon", depends="absent | virtual-api (>= 2), oldlib (>= 2) | fallback", pre_depends="loader"),
            record("loader", depends="daemon"),
            record("provider", version="9", provides="virtual-api (= 3)"),
            record("oldlib", version="1"), record("fallback"),
            record("unversioned", version="99", provides="virtual-api"),
        )
        selected = self.resolver(packages).closure(("daemon",))
        self.assertEqual(selected, {"daemon:amd64", "loader:amd64", "provider:amd64", "fallback:amd64"})

    def test_unversioned_provider_does_not_satisfy_versioned_dependency(self):
        packages = inventory(record("daemon", depends="virtual-api (>= 2)"),
                             record("provider", version="99", provides="virtual-api"))
        with self.assertRaisesRegex(RuntimeError, "daemon:amd64: virtual-api"):
            self.resolver(packages).closure(("daemon",))

    def test_concrete_or_virtual_name_uses_provided_version(self):
        packages = inventory(record("daemon", depends="api (>= 3)"),
                             record("api", version="1"),
                             record("provider", version="1", provides="api (= 4)"))
        self.assertEqual(self.resolver(packages).closure(("daemon",)), {"daemon:amd64", "provider:amd64"})

    def test_multiarch_allowed_any_foreign_native_and_explicit(self):
        packages = inventory(
            record("daemon", depends="interpreter:any, data, compiler:native, library:arm64"),
            record("interpreter", "arm64", multi_arch="allowed"),
            record("data", "arm64", multi_arch="foreign"),
            record("compiler", "amd64", multi_arch="foreign"),
            record("compiler", "arm64", multi_arch="foreign"),
            record("library", "amd64", multi_arch="same"),
            record("library", "arm64", multi_arch="same"),
        )
        self.assertEqual(self.resolver(packages).closure(("daemon",)), {
            "daemon:amd64", "interpreter:arm64", "data:arm64", "compiler:amd64", "library:arm64",
        })

    def test_any_requires_allowed_and_arch_all_is_not_implicitly_foreign(self):
        packages = inventory(record("daemon", "arm64", depends="data"),
                             record("data", "all"))
        resolver = self.resolver(packages)
        self.assertFalse(resolver.architecture_matches(packages["data:all"], None, "arm64"))
        self.assertFalse(resolver.architecture_matches(packages["data:all"], "any", "amd64"))
        foreign = inventory(record("data", "all", multi_arch="foreign"))["data:all"]
        self.assertTrue(resolver.architecture_matches(foreign, None, "arm64"))
        self.assertFalse(resolver.architecture_matches(foreign, "any", "amd64"))

    def test_version_comparison_delegates_epoch_and_revision_to_dpkg(self):
        with patch.object(debian.subprocess, "run") as run:
            run.return_value.returncode = 0
            self.assertTrue(debian.compare_versions("2:20.2.4-1~company", ">=", "1:20.2.4-1"))
            self.assertEqual(run.call_args.args[0], ["dpkg", "--compare-versions",
                             "2:20.2.4-1~company", ">=", "1:20.2.4-1"])

    def test_missing_root_and_missing_dependency_are_fatal(self):
        packages = inventory(record("daemon", depends="absent"))
        with self.assertRaisesRegex(RuntimeError, "not installed: missing"):
            self.resolver(packages).closure(("missing",))
        with self.assertRaisesRegex(RuntimeError, "Unsatisfied installed Debian dependency"):
            self.resolver(packages).closure(("daemon",))


class ProvenanceTests(unittest.TestCase):
    def test_requires_exact_local_version_and_architecture(self):
        packages = inventory(record("ceph-mon", version="20", source="ceph"))
        entry = {"package": "ceph-mon", "version": "20", "architecture": "amd64",
                 "sha256": "a" * 64, "filename": "ceph-mon.deb"}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "provenance.json"
            path.write_text(json.dumps({"packages": [entry], "base_image": "ubuntu"}))
            self.assertEqual(debian.load_provenance(packages, "amd64", str(path))["base_image"], "ubuntu")
            entry["version"] = "21"
            path.write_text(json.dumps({"packages": [entry]}))
            with self.assertRaisesRegex(RuntimeError, "exact version"):
                debian.load_provenance(packages, "amd64", str(path))
            entry.update(version="20", architecture="arm64")
            path.write_text(json.dumps({"packages": [entry]}))
            with self.assertRaisesRegex(RuntimeError, "architecture"):
                debian.load_provenance(packages, "amd64", str(path))

    def test_repo_ceph_library_fallback_is_rejected(self):
        packages = inventory(record("librados2", source="ceph"))
        with self.assertRaisesRegex(RuntimeError, "librados2:amd64"):
            debian.validate_ceph_packages(packages, set(packages), {"packages": []})


class RoleFilesystemTests(unittest.TestCase):
    def test_five_role_roots_use_debian_names_and_keep_client_bindings(self):
        self.assertEqual(set(debian.ROOT_PACKAGES), {"control", "osd", "rgw", "mds"})
        self.assertEqual(debian.ROOT_PACKAGES["rgw"], ("radosgw",))
        self.assertTrue({"python3-rados", "python3-rbd", "python3-cephfs", "ceph-mgr"}
                        .issubset(debian.ROOT_PACKAGES["control"]))
        self.assertNotIn("ceph-osd", debian.ROOT_PACKAGES["control"])
        self.assertNotIn("radosgw", debian.ROOT_PACKAGES["control"])

    def test_daemon_membership_stays_private_while_multiarched_library_is_shared(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            library = root / "usr/lib/x86_64-linux-gnu/ceph/libceph-common.so.2"
            library.parent.mkdir(parents=True)
            library.write_bytes(b"ceph shared library")
            members = {library.relative_to(root).as_posix(): set(package_roles.ROLES)}
            for role, name in (("control", "ceph-mgr"), ("osd", "ceph-osd"), ("rgw", "radosgw"), ("mds", "ceph-mds")):
                binary = root / "usr/bin" / name
                binary.parent.mkdir(exist_ok=True)
                binary.write_bytes(role.encode())
                members[binary.relative_to(root).as_posix()] = {role}
            groups, ordered = package_roles.partition_materialized(root, members)
            self.assertEqual(ordered, ["common", "control", "mds", "osd", "rgw"])
            self.assertIn("usr/lib/x86_64-linux-gnu/ceph/libceph-common.so.2", groups["common"]["paths"])
            self.assertEqual(groups["rgw"]["members"], ["rgw"])
            self.assertNotIn("usr/bin/radosgw", groups["control"]["paths"])

    def test_doc_license_links_are_kept_but_unrelated_docs_are_omitted(self):
        packages = inventory(record("ceph-mon", source="ceph"))
        packages["ceph-mon:amd64"]["licenses"] = {"/usr/share/doc/ceph-mon/copyright"}
        with patch.object(debian, "extra_paths", return_value=set()), \
                patch.object(os.path, "realpath", side_effect=lambda path: path.replace("ceph-mon", "ceph-base")):
            licenses = debian.license_paths(packages, set(packages))
        self.assertFalse(package_roles.analyze.omitted("/usr/share/doc/ceph-mon", licenses))
        self.assertFalse(package_roles.analyze.omitted("/usr/share/doc/ceph-base/copyright", licenses))
        self.assertTrue(package_roles.analyze.omitted("/usr/share/doc/ceph-base/changelog.gz", licenses))

    def test_usrmerge_parent_resolution_preserves_multiarch_final_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "source"
            (source / "usr/lib/x86_64-linux-gnu").mkdir(parents=True)
            os.symlink("usr/lib", source / "lib")
            os.symlink("libexample.so.1", source / "usr/lib/x86_64-linux-gnu/libexample.so")
            previous_root = assemble.ROOT
            try:
                assemble.ROOT = directory / "staged"
                destination = assemble.destination(str(source / "lib/x86_64-linux-gnu/libexample.so"))
                self.assertEqual(destination.relative_to(assemble.ROOT).as_posix(),
                                 str(source.resolve() / "usr/lib/x86_64-linux-gnu/libexample.so").lstrip("/"))
            finally:
                assemble.ROOT = previous_root

    def test_role_metadata_probes_do_not_require_other_roles_copyright(self):
        with tempfile.TemporaryDirectory() as directory:
            materialized = Path(directory) / "staged"
            probes = {"/tmp": {"mode": "1777"},
                      "/usr/share/doc/ceph-base/copyright": {"mode": "0644"},
                      "/usr/share/doc/ceph-common/copyright": {"mode": "0644"}}
            members = {"tmp": set(package_roles.ROLES),
                       "usr/share/doc/ceph-base/copyright": {"control", "osd", "mds"},
                       "usr/share/doc/ceph-common/copyright": set(package_roles.ROLES)}
            previous_root = assemble.ROOT
            try:
                assemble.ROOT = materialized
                # Use canonical paths without relying on this host's /tmp alias.
                with patch.object(assemble.os.path, "realpath", side_effect=lambda path: path):
                    rgw = package_roles.role_metadata_probes(probes, members, materialized, "rgw")
                    control = package_roles.role_metadata_probes(probes, members, materialized, "control")
                self.assertNotIn("/usr/share/doc/ceph-base/copyright", rgw)
                self.assertIn("/usr/share/doc/ceph-common/copyright", rgw)
                self.assertEqual(control, probes)
                self.assertEqual(package_roles.role_metadata_probes(probes, members, materialized, "all"), probes)
            finally:
                assemble.ROOT = previous_root

    def test_var_run_alias_and_target_directory_are_retained_and_probed(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory).resolve() / "source"
            (source / "var").mkdir(parents=True)
            (source / "run/ceph").mkdir(parents=True)
            os.symlink("../run", source / "var/run")
            staged = Path(directory).resolve() / "staged"
            previous_root = assemble.ROOT
            previous_licenses = set(assemble.license_files)
            try:
                assemble.ROOT = staged
                assemble.copied.clear()
                assemble.hardlinks.clear()
                assemble.license_files.clear()
                # Keep this fixture inside its temporary tree; macOS system
                # ancestors may have protected BSD flags unavailable to us.
                with patch.object(assemble, "omitted", side_effect=lambda path: not path.startswith(str(source))):
                    assemble.copy_path(str(source / "var/run"))
                staged_source = staged / str(source).lstrip("/")
                self.assertTrue((staged_source / "var/run").is_symlink())
                self.assertEqual(os.readlink(staged_source / "var/run"), "../run")
                self.assertTrue((staged_source / "run").is_dir())
                self.assertIn("var/run", debian.metadata_probe_paths(staged_source))
                self.assertEqual(package_roles.metadata_probes(staged_source, ("var/run",))["/var/run"]["link_target"], "../run")
                self.assertIn("/var/run", debian.EXTRA_FILES)
            finally:
                assemble.ROOT = previous_root
                assemble.license_files.clear()
                assemble.license_files.update(previous_licenses)
                assemble.copied.clear()
                assemble.hardlinks.clear()


if __name__ == "__main__":
    unittest.main()
