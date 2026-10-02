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
ALL_ROOTS = (
    "ceph-mon", "ceph-mgr", "ceph-osd", "ceph-mds", "ceph-radosgw",
    "ceph-common", "python3-cephfs",
    "rbd-mirror", "cephfs-mirror",
)
PROFILES = {
    "full": ALL_ROOTS,
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
    # Keep historical profiles above while adding the requested image roles.
    # "all" is an aggregate image; it is not a sixth deployment role.
    "mon-mgr": ("ceph-mon", "ceph-mgr", "ceph-common", "rbd-mirror", "cephfs-mirror"),
    "client": ("ceph-common", "python3-cephfs"),
    "all": ALL_ROOTS,
}
ROLES = ("rados-client", "mon-control", "mgr", "osd", "rgw", "mds")
DEPLOYMENT_ROLES = ("mon-mgr", "osd", "rgw", "mds", "client")
DEPLOYMENT_SCENARIOS = {
    "rbd": ("mon-mgr", "osd", "client"),
    "rgw": ("mon-mgr", "osd", "rgw"),
    "rgw-with-client": ("mon-mgr", "osd", "rgw", "client"),
    "cephfs": ("mon-mgr", "osd", "mds", "client"),
    "all": DEPLOYMENT_ROLES,
}
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


def package_regular_inodes(package, licenses):
    """Attribute package-owned payload only; link targets belong to their owner."""
    inodes = {}
    for path in package["files"]:
        if omitted(path, licenses) or not os.path.lexists(path):
            continue
        info = os.lstat(path)
        if stat.S_ISREG(info.st_mode):
            inodes[(info.st_dev, info.st_ino)] = info.st_size
    return inodes


def own_regular_files(package, licenses):
    inodes = package_regular_inodes(package, licenses)
    return sum(inodes.values()), len(inodes)


def deployment_partition(measured, closures, inventory):
    """Partition source inodes by exact membership in the five requested roles."""
    union = {}
    for role in DEPLOYMENT_ROLES:
        union.update(measured[role]["inodes"])
    role_sets = {role: set(measured[role]["inodes"]) for role in DEPLOYMENT_ROLES}
    common = set.intersection(*(role_sets[role] for role in DEPLOYMENT_ROLES))
    common_bytes = sum(union[inode] for inode in common)
    all_inodes = measured["all"]["inodes"]
    all_bytes = measured["all"]["bytes"]

    # Package attribution is supplementary: the partition itself is disjoint,
    # while RPM ownership can overlap for a hardlinked file.
    selected_packages = set().union(*(closures[role] for role in DEPLOYMENT_ROLES))
    licenses = set().union(*(inventory[p]["licenses"] for p in selected_packages))
    owners = {}
    for name in sorted(selected_packages):
        for inode in package_regular_inodes(inventory[name], licenses):
            owners.setdefault(inode, set()).add(name)
    representative_paths = {}
    for role in DEPLOYMENT_ROLES:
        for path in measured[role]["regular_paths"]:
            info = os.stat(path)
            inode = (info.st_dev, info.st_ino)
            canonical = os.path.realpath(path)
            if inode not in representative_paths or canonical < representative_paths[inode]:
                representative_paths[inode] = canonical

    membership_groups = {}
    for inode, size in union.items():
        membership = tuple(role for role in DEPLOYMENT_ROLES if inode in role_sets[role])
        membership_groups.setdefault(membership, {})[inode] = size
    groups = []
    for membership, inodes in membership_groups.items():
        package_bytes = {}
        unattributed = 0
        for inode, size in inodes.items():
            inode_owners = owners.get(inode, set())
            if not inode_owners:
                unattributed += size
            for name in inode_owners:
                package_bytes[name] = package_bytes.get(name, 0) + size
        groups.append({
            "roles": list(membership),
            "kind": "common" if len(membership) == len(DEPLOYMENT_ROLES) else (
                "exclusive" if len(membership) == 1 else "shared-subset"
            ),
            "regular_file_bytes": sum(inodes.values()),
            "unique_regular_file_inode_count": len(inodes),
            "unattributed_regular_file_bytes": unattributed,
            "top_10_owning_packages": [
                {"package": name, "regular_file_bytes": size}
                for name, size in sorted(package_bytes.items(), key=lambda entry: (-entry[1], entry[0]))[:10]
            ],
            "top_10_files": [
                {"path": representative_paths[inode], "regular_file_bytes": size}
                for inode, size in sorted(inodes.items(), key=lambda entry: (-entry[1], representative_paths[entry[0]]))[:10]
            ],
        })
    groups.sort(key=lambda group: (-len(group["roles"]), -group["regular_file_bytes"], group["roles"]))
    partition_bytes = sum(group["regular_file_bytes"] for group in groups)
    if partition_bytes != sum(union.values()):
        raise RuntimeError("Deployment membership partition is not disjoint")

    per_role = {}
    for role in DEPLOYMENT_ROLES:
        others = set().union(*(role_sets[other] for other in DEPLOYMENT_ROLES if other != role))
        exclusive = role_sets[role] - others
        extra = role_sets[role] - common
        per_role[role] = {
            "regular_file_bytes": measured[role]["bytes"],
            "common_regular_file_bytes": common_bytes,
            "extra_vs_common_regular_file_bytes": sum(union[inode] for inode in extra),
            "exclusive_vs_other_roles_regular_file_bytes": sum(union[inode] for inode in exclusive),
            "shared_subset_extra_regular_file_bytes": sum(union[inode] for inode in extra - exclusive),
        }

    scenarios = {}
    for name, roles in DEPLOYMENT_SCENARIOS.items():
        included = set().union(*(role_sets[role] for role in roles))
        excluded = set(all_inodes) - included
        added = included - set(all_inodes)
        scenario_packages = set().union(*(closures[role] for role in roles))
        excluded_bytes = sum(all_inodes[inode] for inode in excluded)
        used_groups = [group for group in groups if included.intersection(membership_groups[tuple(group["roles"])])]
        common_base_model = common_bytes + sum(measured[role]["bytes"] - common_bytes for role in roles)
        scenario_union_bytes = sum(union[inode] for inode in included)
        scenarios[name] = {
            "roles": list(roles),
            "client_optional": name in ("rgw", "rgw-with-client"),
            "package_count": len(scenario_packages),
            "unique_regular_file_inode_count": len(included),
            "union_regular_file_bytes": scenario_union_bytes,
            "excluded_vs_all_regular_file_bytes": excluded_bytes,
            "excluded_percent_of_all_payload": round(100.0 * excluded_bytes / all_bytes, 3),
            "additional_vs_all_regular_file_bytes": sum(union[inode] for inode in added),
            "separate_flattened_role_images_payload_bytes": sum(measured[role]["bytes"] for role in roles),
            "common_base_plus_independent_role_extras_payload_bytes": common_base_model,
            "duplicated_subset_shared_payload_bytes_with_common_base": common_base_model - scenario_union_bytes,
            "ideal_disjoint_shared_layers_payload_bytes": sum(group["regular_file_bytes"] for group in used_groups),
            "required_membership_groups": [group["roles"] for group in used_groups],
        }

    client_delta = measured["client"]["inodes"].keys() - measured["rados-client"]["inodes"].keys()
    binding_names = ("python3-cephfs", "python3-rados", "python3-rbd")
    memberships = [set(membership) for membership in membership_groups]
    laminar = all(
        not left.intersection(right) or left.issubset(right) or right.issubset(left)
        for index, left in enumerate(memberships) for right in memberships[index + 1:]
    )
    common_base_model = common_bytes + sum(measured[role]["bytes"] - common_bytes for role in DEPLOYMENT_ROLES)
    return {
        "roles": list(DEPLOYMENT_ROLES),
        "aggregate_profile": "all",
        "model": "logical source-file partition; potential layer reuse, not a built Docker image layout",
        "union_regular_file_bytes": sum(union.values()),
        "partition_regular_file_bytes": partition_bytes,
        "common_regular_file_bytes": common_bytes,
        "separate_flattened_role_images_payload_bytes": sum(measured[role]["bytes"] for role in DEPLOYMENT_ROLES),
        "common_base_plus_independent_role_extras_payload_bytes": common_base_model,
        "duplicated_subset_shared_payload_bytes_with_common_base": common_base_model - sum(union.values()),
        "membership_groups_are_nested_or_disjoint": laminar,
        "common_package_names": sorted(set.intersection(*(closures[role] for role in DEPLOYMENT_ROLES))),
        "union_matches_all_payload": set(union) == set(all_inodes),
        "union_missing_from_all_regular_file_bytes": sum(union[inode] for inode in set(union) - set(all_inodes)),
        "all_missing_from_union_regular_file_bytes": sum(all_inodes[inode] for inode in set(all_inodes) - set(union)),
        "roles_payload": per_role,
        "disjoint_membership_groups": groups,
        "scenarios": scenarios,
        "client_binding_check": {
            "ceph_common_already_includes_python3_cephfs": "python3-cephfs" in closures["rados-client"],
            "rados_client_bindings": [name for name in binding_names if name in closures["rados-client"]],
            "client_bindings": [name for name in binding_names if name in closures["client"]],
            "additional_packages_vs_rados_client": sorted(closures["client"] - closures["rados-client"]),
            "additional_regular_file_bytes_vs_rados_client": sum(measured["client"]["inodes"][inode] for inode in client_delta),
        },
    }


def main():
    inventory = installed_inventory()
    extras = extra_paths()
    measured = {}
    profiles = {}
    closures = {}
    closure_cache = {}
    for name, packages in PROFILES.items():
        roots = tuple(dict.fromkeys(packages + BASE))
        cache_key = tuple(sorted(roots))
        if cache_key not in closure_cache:
            closure_cache[cache_key] = closure(roots)
        selected = closure_cache[cache_key]
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
        "requested_role_partition": deployment_partition(measured, closures, inventory),
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
