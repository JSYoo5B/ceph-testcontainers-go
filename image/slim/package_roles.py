#!/usr/bin/env python3
"""Materialize an installed package runtime once, then package role overlays.

Run inside the Linux source image with the package backend alongside this file.
No per-overlay package copying or symlink target traversal occurs: all
overlay content comes from the single fully materialized runtime filesystem.
"""

import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile

import analyze
import assemble


ROLES = ("control", "osd", "rgw", "mds")
ROOT_PACKAGES = {
    "control": (
        "ceph-mon", "ceph-mgr", "ceph-common", "python3-cephfs",
        "rbd-mirror", "cephfs-mirror",
    ),
    "osd": ("ceph-osd",),
    "rgw": ("ceph-radosgw",),
    "mds": ("ceph-mds",),
}
REQUIRED_EXECUTABLES = {
    "control": (
        "/usr/bin/ceph", "/usr/bin/ceph-mon", "/usr/bin/ceph-mgr",
        "/usr/bin/rados", "/usr/bin/rbd", "/usr/bin/ceph-authtool", "/usr/bin/monmaptool",
        "/usr/bin/rbd-mirror", "/usr/bin/cephfs-mirror",
    ),
    "osd": ("/usr/bin/ceph-osd",),
    "rgw": ("/usr/bin/radosgw", "/usr/bin/radosgw-admin"),
    "mds": ("/usr/bin/ceph-mds",),
}
COPYING = "/usr/share/doc/ceph/COPYING"
MANIFEST_DIRECTORY = Path("usr/share/ceph-testcontainers")
METADATA_PROBE_PATHS = (
    "etc/ceph", "var/lib/ceph", "run/ceph", "var/log/ceph", "tmp",
    "bin", "lib64", "usr/share/doc/ceph/COPYING",
)


def canonical_destination(source_path, materialized):
    """Match assembler parent alias resolution while retaining the final link."""
    return assemble.destination(source_path).relative_to(materialized).as_posix()


def plan_source_paths(paths, licenses, materialized):
    """Plan paths only; never copy a file or recurse through a staged symlink."""
    seen = set()
    result = set()

    def visit(path):
        path = os.path.normpath(path)
        if path in seen or analyze.omitted(path, licenses) or not os.path.lexists(path):
            return
        seen.add(path)
        info = os.lstat(path)
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
            return
        if path == "/":
            return
        visit(os.path.dirname(path))
        result.add(canonical_destination(path, materialized))
        if stat.S_ISLNK(info.st_mode):
            link = os.readlink(path)
            visit(link if os.path.isabs(link) else os.path.join(os.path.dirname(path), link))

    for path in sorted(paths):
        visit(path)
    return result


def tree_paths(root):
    paths = set()
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            paths.add((Path(directory) / name).relative_to(root).as_posix())
    return paths


def add_parent_memberships(path_members):
    for path, members in list(path_members.items()):
        parent = Path(path).parent
        while parent != Path("."):
            path_members.setdefault(parent.as_posix(), set()).update(members)
            parent = parent.parent


def group_name(members):
    if set(members) == set(ROLES):
        return "common"
    if len(members) == 1:
        return next(iter(members))
    return "shared-" + "-".join(sorted(members))


def partition_materialized(materialized, path_members):
    """Put every hardlinked inode in one membership group, including all paths."""
    inode_paths = {}
    for path in sorted(path_members):
        source = materialized / path
        if not os.path.lexists(source):
            raise RuntimeError("Planned path absent from materialized runtime: /" + path)
        info = os.lstat(source)
        if stat.S_ISREG(info.st_mode):
            inode_paths.setdefault((info.st_dev, info.st_ino), []).append(path)
    for paths in inode_paths.values():
        members = set().union(*(path_members[path] for path in paths))
        for path in paths:
            path_members[path] = set(members)
    add_parent_memberships(path_members)

    groups = {}
    for path, members in sorted(path_members.items()):
        if not members or not members.issubset(ROLES):
            raise RuntimeError("Invalid role membership for /" + path)
        name = group_name(members)
        group = groups.setdefault(name, {"members": sorted(members), "paths": [], "inodes": {}})
        group["paths"].append(path)
        info = os.lstat(materialized / path)
        if stat.S_ISREG(info.st_mode):
            group["inodes"][(info.st_dev, info.st_ino)] = info.st_size
    ordered = sorted(groups, key=lambda name: (
        0 if name == "common" else (1 if len(groups[name]["members"]) > 1 else 2),
        -len(groups[name]["members"]), name,
    ))
    return groups, ordered


def copy_metadata(source, target, follow_symlinks=True):
    info = os.stat(source) if follow_symlinks else os.lstat(source)
    # Apply ownership before modes/xattrs: chown can clear set-id/capabilities.
    os.chown(target, info.st_uid, info.st_gid, follow_symlinks=follow_symlinks)
    shutil.copystat(source, target, follow_symlinks=follow_symlinks)


def write_overlay(materialized, paths, target):
    """Copy assigned nodes directly; parent scaffolding never adds target files."""
    target.mkdir(parents=True)
    directories = set()
    hardlinks = {}

    def ensure_directory(relative):
        if relative == Path("."):
            return
        if relative.as_posix() in directories:
            return
        source = materialized / relative
        if not stat.S_ISDIR(os.lstat(source).st_mode):
            raise RuntimeError("Overlay parent is not a canonical directory: /" + relative.as_posix())
        ensure_directory(relative.parent)
        (target / relative).mkdir(exist_ok=True)
        directories.add(relative.as_posix())

    for path in sorted(paths):
        relative = Path(path)
        source = materialized / relative
        destination = target / relative
        info = os.lstat(source)
        ensure_directory(relative.parent)
        if stat.S_ISDIR(info.st_mode):
            ensure_directory(relative)
        elif stat.S_ISLNK(info.st_mode):
            os.symlink(os.readlink(source), destination)
            copy_metadata(source, destination, follow_symlinks=False)
        elif stat.S_ISREG(info.st_mode):
            inode = (info.st_dev, info.st_ino)
            if inode in hardlinks:
                os.link(hardlinks[inode], destination)
            else:
                shutil.copyfile(source, destination)
                copy_metadata(source, destination)
                hardlinks[inode] = destination
        else:
            raise RuntimeError("Unsupported materialized node: /" + path)
    # Restore directory metadata after child creation changes mtimes.
    for path in sorted(directories, key=lambda entry: (-len(Path(entry).parts), entry)):
        copy_metadata(materialized / path, target / path)
    copy_metadata(materialized, target)


def write_archive(directory, archive):
    """Preserve numeric ownership/modes/links; tarfile does not serialize xattrs."""
    with tarfile.open(archive, "w", format=tarfile.PAX_FORMAT, dereference=False) as output:
        for path in sorted(tree_paths(directory)):
            source = directory / path
            info = output.gettarinfo(str(source), arcname=path)
            info.uid = os.lstat(source).st_uid
            info.gid = os.lstat(source).st_gid
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            info.pax_headers = {}
            if info.isreg():
                with source.open("rb") as contents:
                    output.addfile(info, contents)
            else:
                output.addfile(info)


def group_summary(materialized, group, name, overlay):
    directories = 0
    symlinks = 0
    regular_paths = 0
    for path in group["paths"]:
        info = os.lstat(materialized / path)
        directories += int(stat.S_ISDIR(info.st_mode))
        symlinks += int(stat.S_ISLNK(info.st_mode))
        regular_paths += int(stat.S_ISREG(info.st_mode))
    return {
        "members": group["members"],
        "archive": "groups/" + name + ".tar",
        "path_count": len(group["paths"]),
        "overlay_path_count_including_parent_scaffolding": len(tree_paths(overlay)),
        "regular_file_bytes": sum(group["inodes"].values()),
        "unique_regular_file_inode_count": len(group["inodes"]),
        "regular_file_path_count": regular_paths,
        "directory_count": directories,
        "symlink_count": symlinks,
    }


def validate_executables(materialized, path_members, package_manager="rpm"):
    for role, executables in REQUIRED_EXECUTABLES.items():
        for path in executables:
            if not os.path.exists(path) or not os.access(path, os.X_OK):
                raise RuntimeError("Role %s requires missing/non-executable source path %s; source %s layout changed" % (role, path, package_manager))
            relative = canonical_destination(path, materialized)
            if role not in path_members.get(relative, set()):
                raise RuntimeError("Role %s does not include required executable %s" % (role, path))
            if not os.access(materialized / relative, os.X_OK):
                raise RuntimeError("Materialized executable lost its executable permission: " + path)


def metadata_probes(materialized, probe_paths=METADATA_PROBE_PATHS):
    result = {}
    for path in probe_paths:
        source = materialized / path
        info = os.lstat(source)
        entry = {"uid": info.st_uid, "gid": info.st_gid, "mode": "%04o" % stat.S_IMODE(info.st_mode)}
        if stat.S_ISLNK(info.st_mode):
            entry.update({"type": "symlink", "link_target": os.readlink(source)})
        elif stat.S_ISDIR(info.st_mode):
            entry["type"] = "directory"
        elif stat.S_ISREG(info.st_mode):
            entry["type"] = "regular"
        else:
            raise RuntimeError("Unexpected metadata probe type: /" + path)
        result["/" + path] = entry
    return result


def role_metadata_probes(probes, path_members, materialized, role):
    if role == "all":
        return dict(probes)
    return {path: metadata for path, metadata in probes.items()
            if role in path_members.get(canonical_destination(path, materialized), set())}


def main():
    output = Path(os.environ.get("OUTPUT_ROOT", "/role-output"))
    if not output.is_absolute():
        raise RuntimeError("OUTPUT_ROOT must be an absolute path")
    if output.exists() and any(output.iterdir()):
        raise RuntimeError("OUTPUT_ROOT must be empty before packaging")
    source_image = os.environ.get("SOURCE_IMAGE", "")
    if not re.search(r"(?:^sha256:|@sha256:)[0-9a-f]{64}$", source_image):
        raise RuntimeError("SOURCE_IMAGE must be an immutable sha256 image ID or repository digest")
    output.mkdir(parents=True, exist_ok=True)
    package_manager = os.environ.get("PACKAGE_BACKEND", "rpm")
    if package_manager == "dpkg":
        import debian_packages
        inventory = debian_packages.installed_inventory()
        native_architecture = debian_packages.query("dpkg", "--print-architecture").strip()
        base = debian_packages.BASE + debian_packages.essential_roots(inventory)
        roots = {role: tuple(dict.fromkeys(debian_packages.ROOT_PACKAGES[role] + base)) for role in ROLES}
        resolver = debian_packages.DependencyResolver(inventory, native_architecture)
        selected = {role: resolver.closure(roots[role]) for role in ROLES}
        provenance = debian_packages.load_provenance(inventory, native_architecture)
    elif package_manager == "rpm":
        inventory = analyze.installed_inventory()
        roots = {role: tuple(dict.fromkeys(ROOT_PACKAGES[role] + analyze.BASE)) for role in ROLES}
        selected = {role: analyze.closure(roots[role]) for role in ROLES}
    else:
        raise RuntimeError("Unsupported PACKAGE_BACKEND: " + package_manager)
    all_roots = tuple(dict.fromkeys(package for role in ROLES for package in roots[role]))
    all_selected = set().union(*(selected[role] for role in ROLES))
    if all_selected.difference(inventory):
        raise RuntimeError("Role dependency closure references uninstalled packages")

    materialized = output / ".materialized"
    if package_manager == "dpkg":
        debian_packages.validate_ceph_packages(inventory, all_selected, provenance)
        debian_packages.assemble_runtime(materialized, all_selected, inventory)
    else:
        assemble.ROOT = materialized
        assemble.PACKAGES = all_roots
        assemble.copied.clear()
        assemble.hardlinks.clear()
        assemble.license_files.clear()
        assemble.main()
        # The aggregate manifest is replaced by individual role manifests.
        shutil.rmtree(materialized / MANIFEST_DIRECTORY)

    path_members = {}
    extras = debian_packages.extra_paths() if package_manager == "dpkg" else analyze.extra_paths()
    role_licenses = {}
    for role in ROLES:
        licenses = (debian_packages.license_paths(inventory, selected[role]) if package_manager == "dpkg"
                    else set().union(*(inventory[p]["licenses"] for p in selected[role])))
        if package_manager == "rpm":
            licenses.add(COPYING)
        files = set(extras).union(*(inventory[p]["files"] for p in selected[role]))
        files.update(licenses)
        role_licenses[role] = licenses
        for path in plan_source_paths(files, licenses, materialized):
            path_members.setdefault(path, set()).add(role)
        # These generated directories are required in every runtime image.
        for source_path in assemble.WRITABLE_DIRS:
            path = canonical_destination(source_path, materialized)
            path_members.setdefault(path, set()).add(role)
    if package_manager == "rpm" and not os.path.isfile(COPYING):
        raise RuntimeError("Required Ceph license file missing: " + COPYING)
    add_parent_memberships(path_members)
    actual_paths = tree_paths(materialized)
    missing = actual_paths - set(path_members)
    unexpected = set(path_members) - actual_paths
    if missing or unexpected:
        raise RuntimeError("Role plan differs from materialized runtime; unassigned=%s absent=%s" % (
            sorted(missing)[:20], sorted(unexpected)[:20],
        ))

    groups, ordered = partition_materialized(materialized, path_members)
    validate_executables(materialized, path_members, package_manager)
    group_metadata = {}
    for name in ordered:
        overlay = output / "groups" / name
        write_overlay(materialized, groups[name]["paths"], overlay)
        write_archive(overlay, output / "groups" / (name + ".tar"))
        group_metadata[name] = group_summary(materialized, groups[name], name, overlay)

    ceph_version = subprocess.run(
        ["ceph", "--version"], check=True, text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    architecture = platform.machine()
    oci_architecture = {"aarch64": "arm64", "x86_64": "amd64"}.get(architecture, architecture)
    probes = metadata_probes(materialized, debian_packages.metadata_probe_paths(materialized)) if package_manager == "dpkg" else metadata_probes(materialized)
    role_metadata = {}
    roots["all"] = all_roots
    selected["all"] = all_selected
    for role in ROLES + ("all",):
        role_groups = [name for name in ordered if role == "all" or role in groups[name]["members"]]
        versions = (debian_packages.package_versions(inventory, selected[role]) if package_manager == "dpkg"
                    else sorted(name + "-" + inventory[name]["version"] for name in selected[role]))
        executables = list(REQUIRED_EXECUTABLES[role]) if role != "all" else sorted(set().union(*REQUIRED_EXECUTABLES.values()))
        metadata = {
            "role": role,
            "source_image": source_image,
            "ceph_version": ceph_version,
            "architecture": architecture,
            "oci_architecture": oci_architecture,
            "groups": role_groups,
            "manifest_archive": "manifests/" + role + ".tar",
            "root_packages": list(roots[role]),
            "package_versions": versions,
            "required_executables": executables,
            "metadata_probes": role_metadata_probes(probes, path_members, materialized, role),
            "logical_regular_file_bytes": sum(group_metadata[name]["regular_file_bytes"] for name in role_groups),
        }
        if package_manager == "dpkg":
            licenses = set().union(*role_licenses.values()) if role == "all" else role_licenses[role]
            metadata.update({
                "package_manager": package_manager,
                "debian_architecture": native_architecture,
                "local_package_provenance": provenance,
                "package_records": debian_packages.package_records(inventory, selected[role]),
                "license_paths": sorted({"/" + canonical_destination(path, materialized)
                                         for path in licenses if os.path.isfile(path)}),
                "library_directories": debian_packages.library_directories(inventory, selected[role]),
            })
        role_metadata[role] = metadata
        manifest_root = output / "manifests" / role
        manifest_directory = manifest_root / MANIFEST_DIRECTORY
        manifest_directory.mkdir(parents=True)
        (manifest_directory / "runtime-packages.txt").write_text(
            "# source-image: " + source_image + "\n" + "\n".join(versions) + "\n",
        )
        (manifest_directory / "image-manifest.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        write_archive(manifest_root, output / "manifests" / (role + ".tar"))

    plan = {
        "source_image": source_image,
        "ceph_version": ceph_version,
        "architecture": architecture,
        "oci_architecture": oci_architecture,
        "measurement": "logical regular-file bytes of disjoint materialized source inodes; excludes tar/container layer metadata and generated manifests",
        "archive_policy": "numeric uid/gid and modes; canonical relative paths; symlink/hardlink preservation; sorted entries and mtime=0",
        "archive_metadata_limits": "extended attributes (including capabilities and ACLs) are not serialized by Python tarfile",
        "ordered_groups": ordered,
        "groups": group_metadata,
        "roles": role_metadata,
        "logical_regular_file_bytes": sum(group["regular_file_bytes"] for group in group_metadata.values()),
        "metadata_probes": probes,
    }
    if package_manager == "dpkg":
        plan.update({field: role_metadata["all"][field] for field in (
            "package_manager", "debian_architecture", "local_package_provenance",
            "package_records", "license_paths", "library_directories",
        )})
    (output / "plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(materialized)
    print("Packaged %d disjoint groups for %s (%d logical regular-file bytes)" % (
        len(groups), ", ".join(ROLES + ("all",)), plan["logical_regular_file_bytes"],
    ))


def self_test():
    """Exercise partitions and Linux-compatible tar metadata without Docker/RPM."""
    with tempfile.TemporaryDirectory(prefix="ceph-role-fixture-") as temporary:
        root = Path(temporary)
        materialized = root / "runtime"
        (materialized / "usr/lib64").mkdir(parents=True)
        (materialized / "usr/bin").mkdir()
        (materialized / "etc/ceph").mkdir(parents=True)
        (materialized / "etc/ceph").chmod(0o2750)
        (materialized / "tmp").mkdir(mode=0o1777)
        (materialized / "tmp").chmod(0o1777)
        shared = materialized / "usr/lib64/shared.so"
        shared.write_bytes(b"shared-runtime")
        shared.chmod(0o755)
        if os.geteuid() == 0:
            # Linux Docker packaging also exercises non-root numeric owners.
            os.chown(shared, 167, 167)
        first = materialized / "usr/bin/control-helper"
        first.write_bytes(b"shared-control-osd-inode")
        first.chmod(0o755)
        second = materialized / "usr/bin/osd-helper"
        os.link(first, second)
        os.symlink("usr/lib64", materialized / "lib64")
        os.symlink("/usr/bin", materialized / "bin")
        private = materialized / "usr/bin/control"
        private.write_bytes(b"control-only")
        private.chmod(0o6751)
        expected_directory_mode = stat.S_IMODE(os.stat(materialized / "etc/ceph").st_mode)
        expected_file_mode = stat.S_IMODE(os.stat(private).st_mode)
        set_id_supported = expected_directory_mode == 0o2750 and expected_file_mode == 0o6751
        if sys.platform.startswith("linux") and os.geteuid() == 0:
            assert set_id_supported, "Linux fixture source filesystem stripped set-id bits"
        memberships = {path: set(ROLES) for path in tree_paths(materialized)}
        memberships["usr/bin/control-helper"] = {"control"}
        memberships["usr/bin/osd-helper"] = {"osd"}
        memberships["usr/bin/control"] = {"control"}
        groups, ordered = partition_materialized(materialized, memberships)
        assert ordered == ["common", "shared-control-osd", "control"], ordered
        assert groups["shared-control-osd"]["members"] == ["control", "osd"]
        assert len(groups["shared-control-osd"]["inodes"]) == 1
        for name in ordered:
            overlay = root / name
            write_overlay(materialized, groups[name]["paths"], overlay)
            archive = root / (name + ".tar")
            write_archive(overlay, archive)
            repeated = root / (name + "-repeat.tar")
            write_archive(overlay, repeated)
            assert archive.read_bytes() == repeated.read_bytes()
            with tarfile.open(archive) as packaged:
                for entry in packaged.getmembers():
                    source = overlay / entry.name
                    info = os.lstat(source)
                    assert not entry.name.startswith("/") and ".." not in Path(entry.name).parts
                    assert entry.uid == info.st_uid and entry.gid == info.st_gid
                    assert entry.mode == stat.S_IMODE(info.st_mode)
                    assert entry.mtime == 0 and entry.uname == "" and entry.gname == ""
                if name == "common":
                    assert packaged.getmember("tmp").mode == 0o1777
                    assert packaged.getmember("etc/ceph").mode == expected_directory_mode
                    assert packaged.getmember("bin").issym()
                    assert packaged.getmember("bin").linkname == "/usr/bin"
                if name == "shared-control-osd":
                    assert packaged.getmember("usr/bin/osd-helper").islnk()
                    assert packaged.getmember("usr/bin/osd-helper").linkname == "usr/bin/control-helper"
                if name == "control":
                    assert packaged.getmember("usr/bin/control").mode == expected_file_mode
        assert not (root / "control/usr/lib64/shared.so").exists()
        assert os.stat(root / "shared-control-osd/usr/bin/control-helper").st_ino == os.stat(root / "shared-control-osd/usr/bin/osd-helper").st_ino
    print("Role packaging fixture passed: disjoint files, hardlinks, symlinks, sticky modes, ownership and deterministic tar" + (
        "; set-id modes preserved" if set_id_supported else "; host stripped set-id source modes before copying"
    ))


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    elif sys.argv[1:]:
        raise SystemExit("Usage: package_roles.py [--self-test]")
    else:
        main()
