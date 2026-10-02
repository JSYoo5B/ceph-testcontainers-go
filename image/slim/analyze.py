#!/usr/bin/env python3
"""Compare installed Ceph runtime payloads without modifying the source image.

Run with the pinned upstream image's Python 3.9 interpreter. The accounting
matches assemble.py's package roots and exclusions, but reports logical regular
file bytes, not Docker image/layer sizes or registry transfer sizes.
"""

import json
import os
from pathlib import Path
import stat
import subprocess


BASE = ("bash", "coreutils-single", "hostname", "gawk", "ca-certificates", "filesystem")
PROFILES = {
    "full": (
        "ceph-mon", "ceph-mgr", "ceph-osd", "ceph-mds", "ceph-radosgw",
        "ceph-common", "python3-cephfs",
    ),
    "rados-client": ("ceph-common",),
    "mon-control": ("ceph-mon", "ceph-common"),
    "mgr": ("ceph-mgr",),
    "osd": ("ceph-osd",),
    "rgw": ("ceph-radosgw",),
    "mds": ("ceph-mds",),
    "rados-rbd-cluster": ("ceph-mon", "ceph-mgr", "ceph-osd", "ceph-common"),
    "rgw-only-cluster": (
        "ceph-mon", "ceph-mgr", "ceph-osd", "ceph-common", "ceph-radosgw",
    ),
    "cephfs-only-cluster": (
        "ceph-mon", "ceph-mgr", "ceph-osd", "ceph-common", "ceph-mds", "python3-cephfs",
    ),
}
ROLES = ("rados-client", "mon-control", "mgr", "osd", "rgw", "mds")
CLUSTERS = ("rados-rbd-cluster", "rgw-only-cluster", "cephfs-only-cluster")
OMIT = ("/usr/share/doc", "/usr/share/man", "/usr/share/info", "/dev", "/proc", "/sys")
EXTRA_FILES = (
    "/bin", "/lib", "/lib64", "/sbin", "/etc/alternatives",
    "/etc/passwd", "/etc/group", "/etc/nsswitch.conf", "/etc/ld.so.cache",
)
EXTRA_TREES = ("/etc/pki/ca-trust", "/etc/pki/tls", "/etc/ssl")


def query(*args):
    result = subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE)
    return result.stdout.splitlines()


def installed_inventory():
    inventory = {}
    for line in query("rpm", "-qa", "--qf", "%{NAME}\t%{SIZE}\t%{VERSION}-%{RELEASE}.%{ARCH}\n"):
        name, size, version = line.split("\t", 2)
        # RPM stores imported signing keys as multiple gpg-pubkey records;
        # these are metadata, not installed filesystem package payloads.
        if name == "gpg-pubkey":
            continue
        if name in inventory:
            raise RuntimeError("Multiple installed versions/architectures for " + name)
        inventory[name] = {"rpm_size_bytes": int(size), "version": version, "files": [], "licenses": set()}
    current = None
    for line in query("rpm", "-qa", "--qf", "PACKAGE\t%{NAME}\n[%{FILENAMES}\t%{FILEFLAGS:fflags}\n]"):
        if line.startswith("PACKAGE\t"):
            current = inventory.get(line.split("\t", 1)[1])
            continue
        if current is None:
            continue
        path, flags = line.rsplit("\t", 1)
        path = os.path.normpath(path)
        current["files"].append(path)
        if "l" in flags:
            current["licenses"].add(path)
    return inventory


def closure(roots):
    resolved = query(
        "dnf", "-q", "--cacheonly", "repoquery", "--installed", "--requires",
        "--resolve", "--recursive", "--qf", "%{name}", *roots,
    )
    return set(roots).union(name for name in resolved if name)


def omitted(path, licenses):
    if path in licenses:
        return False
    return (
        any(path == prefix or path.startswith(prefix + "/") for prefix in OMIT)
        or "__pycache__" in Path(path).parts
        or path.endswith((".pyc", ".pyo"))
    )


def extra_paths():
    paths = set(EXTRA_FILES)
    for tree in EXTRA_TREES:
        paths.add(tree)
        if os.path.isdir(tree) and not os.path.islink(tree):
            for directory, dirs, files in os.walk(tree, followlinks=False):
                paths.update(os.path.join(directory, name) for name in dirs + files)
    return paths


def measure(paths, licenses):
    """Account for the same source files and link targets that assembly copies."""
    seen = set()
    inodes = {}
    regular_paths = set()
    links = set()

    def visit(path):
        path = os.path.normpath(path)
        if path in seen or omitted(path, licenses) or not os.path.lexists(path):
            return
        seen.add(path)
        info = os.lstat(path)
        if path != "/":
            visit(os.path.dirname(path))
        if stat.S_ISREG(info.st_mode):
            inodes[(info.st_dev, info.st_ino)] = info.st_size
            regular_paths.add(path)
        elif stat.S_ISLNK(info.st_mode):
            links.add(path)
            target = os.readlink(path)
            visit(target if os.path.isabs(target) else os.path.join(os.path.dirname(path), target))

    for path in sorted(paths):
        visit(path)
    return {
        "inodes": inodes,
        "regular_paths": regular_paths,
        "symlink_paths": links,
        "bytes": sum(inodes.values()),
    }


def own_regular_files(package, licenses):
    """Attribute package-owned payload only; link targets belong to their owner."""
    inodes = {}
    for path in package["files"]:
        if omitted(path, licenses) or not os.path.lexists(path):
            continue
        info = os.lstat(path)
        if stat.S_ISREG(info.st_mode):
            inodes[(info.st_dev, info.st_ino)] = info.st_size
    return sum(inodes.values()), len(inodes)


def main():
    inventory = installed_inventory()
    extras = extra_paths()
    measured = {}
    profiles = {}
    closures = {}
    for name, packages in PROFILES.items():
        roots = tuple(dict.fromkeys(packages + BASE))
        selected = closure(roots)
        missing = selected.difference(inventory)
        if missing:
            raise RuntimeError("Uninstalled closure packages: " + ", ".join(sorted(missing)))
        licenses = set().union(*(inventory[p]["licenses"] for p in selected))
        files = set(extras).union(*(inventory[p]["files"] for p in selected))
        result = measure(files, licenses)
        measured[name] = result
        closures[name] = selected
        profiles[name] = {
            "root_packages": list(roots),
            "package_count": len(selected),
            "packages": sorted(selected),
            "regular_file_count": len(result["regular_paths"]),
            "unique_regular_file_inode_count": len(result["inodes"]),
            "symlink_count": len(result["symlink_paths"]),
            "regular_file_bytes": result["bytes"],
            "rpm_declared_size_bytes": sum(inventory[p]["rpm_size_bytes"] for p in selected),
            "retained_license_files": sorted(path for path in licenses if os.path.exists(path)),
        }

    full_bytes = measured["full"]["bytes"]
    comparisons = {}
    for name in CLUSTERS:
        omitted_inodes = measured["full"]["inodes"].keys() - measured[name]["inodes"].keys()
        excluded = sum(measured["full"]["inodes"][inode] for inode in omitted_inodes)
        comparisons[name] = {
            "excluded_package_count": len(closures["full"] - closures[name]),
            "excluded_packages": sorted(closures["full"] - closures[name]),
            "excluded_regular_file_bytes": excluded,
            "excluded_percent_of_full_payload": round(100.0 * excluded / full_bytes, 3),
        }

    role_union = {}
    for name in ROLES:
        role_union.update(measured[name]["inodes"])
    intersection = set(measured[ROLES[0]]["inodes"])
    for name in ROLES[1:]:
        intersection.intersection_update(measured[name]["inodes"])
    flat_sum = sum(measured[name]["bytes"] for name in ROLES)
    union_bytes = sum(role_union.values())
    role_exclusive = {}
    for name in ROLES:
        others = set().union(*(measured[p]["inodes"] for p in ROLES if p != name))
        unique = measured[name]["inodes"].keys() - others
        role_exclusive[name] = sum(measured[name]["inodes"][inode] for inode in unique)

    selected_packages = []
    all_licenses = set().union(*(inventory[p]["licenses"] for p in closures["full"]))
    for name in closures["full"]:
        owned_bytes, owned_files = own_regular_files(inventory[name], all_licenses)
        selected_packages.append({
            "package": name,
            "version": inventory[name]["version"],
            "regular_file_bytes": owned_bytes,
            "unique_regular_file_inode_count": owned_files,
            "rpm_declared_size_bytes": inventory[name]["rpm_size_bytes"],
            "used_by_profiles": sorted(p for p in PROFILES if name in closures[p]),
        })
    original_packages = [
        {"package": name, "version": p["version"], "rpm_declared_size_bytes": p["rpm_size_bytes"]}
        for name, p in inventory.items()
    ]
    candidate_paths = {
        "denc-plugins": sorted(
            path for path in measured["full"]["regular_paths"]
            if path.startswith("/usr/lib64/ceph/denc/")
        ),
        "rgw-standalone-tools": [
            "/usr/bin/radosgw-es", "/usr/bin/radosgw-object-expirer",
            "/usr/bin/rgw-policy-check",
        ],
        "cephfs-repair-tools": [
            "/usr/bin/cephfs-data-scan", "/usr/bin/cephfs-journal-tool",
            "/usr/bin/cephfs-table-tool",
        ],
        "rpm-database": sorted(
            path for path in measured["full"]["regular_paths"]
            if path.startswith(("/var/lib/rpm/", "/usr/lib/sysimage/rpm/"))
        ),
    }
    candidates = {}
    for name, paths in candidate_paths.items():
        result = measure(paths, all_licenses)
        candidates[name] = {
            "unique_regular_file_bytes": result["bytes"],
            "files": [
                {"path": path, "regular_file_bytes": os.stat(path).st_size}
                for path in sorted(result["regular_paths"])
            ],
        }

    output = {
        "source_image": os.environ.get("SOURCE_IMAGE", "unspecified"),
        "measurement": {
            "metric": "logical regular-file payload bytes from the existing source filesystem",
            "deduplication": "unique (st_dev, st_ino) per profile, including required symlink targets",
            "scope": "offline installed RPM closure plus assemble.py extra files and CA trees",
            "exclusions": "doc/man/info, Python bytecode, dev/proc/sys; RPM %license files retained",
            "not_measured": ["Docker layer size", "compressed registry transfer", "daemon RAM", "generated package manifest"],
            "package_ranking": "existing package-owned regular files; hardlinks can overlap between packages",
            "role_split_model": "each role assembled as an independent flattened image; common-layer reuse not assumed",
            "installed_package_count": "excludes imported gpg-pubkey RPM metadata records",
        },
        "installed_package_count": len(inventory),
        "profiles": profiles,
        "cluster_savings_vs_full": comparisons,
        "role_image_duplication": {
            "roles": list(ROLES),
            "separate_flattened_images_payload_bytes": flat_sum,
            "union_payload_bytes": union_bytes,
            "duplicated_payload_bytes": flat_sum - union_bytes,
            "shared_by_all_roles_payload_bytes": sum(role_union[inode] for inode in intersection),
            "exclusive_payload_bytes_by_role": role_exclusive,
        },
        "unvalidated_trimming_candidates": candidates,
        "top_20_selected_runtime_packages": sorted(selected_packages, key=lambda p: p["regular_file_bytes"], reverse=True)[:20],
        "top_30_selected_runtime_files": sorted(
            ({"path": path, "regular_file_bytes": os.stat(path).st_size}
             for path in measured["full"]["regular_paths"]),
            key=lambda entry: entry["regular_file_bytes"], reverse=True,
        )[:30],
        "top_20_original_rpm_packages": sorted(original_packages, key=lambda p: p["rpm_declared_size_bytes"], reverse=True)[:20],
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
