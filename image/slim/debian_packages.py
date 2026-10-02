#!/usr/bin/env python3
"""Offline installed-dpkg backend for the shared Ceph role assembler.

APT installs the local .deb files and their distro dependencies in the source
image first. This module reads that installed database, never searches an APT
repository, and copies only one satisfied provider of each binary dependency.
Debian Policy chapters 5 and 7 define the dependency/Provides/Multi-Arch rules;
dpkg itself compares versions (including epochs and Debian revisions).
"""

from functools import lru_cache
import json
import os
from pathlib import Path
import re
import subprocess

import assemble


BASE = ("bash", "coreutils", "hostname", "gawk", "ca-certificates", "base-files")
ROOT_PACKAGES = {
    "control": (
        "ceph-mon", "ceph-mgr", "ceph-common", "python3-rados", "python3-rbd",
        "python3-cephfs", "rbd-mirror", "cephfs-mirror",
    ),
    "osd": ("ceph-osd",),
    "rgw": ("radosgw",),
    "mds": ("ceph-mds",),
}
EXTRA_FILES = (
    "/bin", "/lib", "/lib64", "/sbin", "/etc/alternatives",
    "/etc/passwd", "/etc/group", "/etc/nsswitch.conf", "/etc/ld.so.cache",
    "/etc/os-release", "/bin/sh", "/usr/bin/awk", "/var/run",
)
EXTRA_TREES = ("/etc/ssl", "/usr/share/common-licenses")
METADATA_FIELDS = (
    "Package", "Architecture", "Version", "db:Status-Status", "db:Status-Eflag",
    "Multi-Arch", "Essential", "Depends", "Pre-Depends", "Provides",
    "Installed-Size", "source:Package", "source:Version",
)
RELATION = re.compile(
    r"^\s*([a-z0-9][a-z0-9+.-]*)(?::([a-z0-9][a-z0-9-]*))?"
    r"\s*(?:\(\s*(<<|<=|=|>=|>>)\s*([^\s()]+)\s*\))?\s*$"
)


def query(*args):
    return subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE).stdout


def parse_relationships(value, provides=False):
    """Parse installed binary fields; source-only arch/profile syntax is invalid."""
    result = []
    if not value.strip():
        return result
    for group in value.split(","):
        alternatives = []
        for value in group.split("|"):
            match = RELATION.fullmatch(value)
            if not match:
                raise RuntimeError("Unsupported installed Debian relationship: " + value.strip())
            name, qualifier, operator, version = match.groups()
            if provides and (qualifier or operator not in (None, "=")):
                raise RuntimeError("Invalid Debian Provides entry: " + value.strip())
            alternatives.append({"name": name, "qualifier": qualifier,
                                 "operator": operator, "version": version})
        if provides and len(alternatives) != 1:
            raise RuntimeError("Provides cannot contain alternatives: " + group)
        result.append(alternatives)
    return result


def parse_inventory(metadata):
    """Keep architecture-qualified identities even for non-Multi-Arch packages."""
    inventory = {}
    for line in metadata.splitlines():
        fields = line.split("\t")
        if len(fields) != len(METADATA_FIELDS):
            raise RuntimeError("Malformed dpkg-query metadata line: " + line)
        record = dict(zip(METADATA_FIELDS, fields))
        if record["db:Status-Status"] != "installed":
            continue
        if record["db:Status-Eflag"] not in ("ok", ""):
            raise RuntimeError("Installed package needs repair: " + record["Package"])
        name, architecture, version = (record[field] for field in ("Package", "Architecture", "Version"))
        if not name or not architecture or not version:
            raise RuntimeError("Installed Debian package lacks name, architecture or version")
        identity = name + ":" + architecture
        if identity in inventory:
            raise RuntimeError("Duplicate installed Debian package: " + identity)
        multi_arch = record["Multi-Arch"] or "no"
        if multi_arch not in ("no", "same", "foreign", "allowed"):
            raise RuntimeError("Unsupported Multi-Arch value for " + identity + ": " + multi_arch)
        inventory[identity] = {
            "name": name, "architecture": architecture, "version": version,
            "multi_arch": multi_arch, "essential": record["Essential"] == "yes",
            "depends": parse_relationships(record["Depends"]),
            "pre_depends": parse_relationships(record["Pre-Depends"]),
            "provides": [group[0] for group in parse_relationships(record["Provides"], provides=True)],
            "installed_size_bytes": int(record["Installed-Size"] or "0") * 1024,
            "source_package": record["source:Package"] or name,
            "source_version": record["source:Version"] or version,
            "files": [], "licenses": set(),
        }
    return inventory


def add_inventory_files(inventory, output):
    current = None
    for line in output.splitlines():
        if line.startswith("PACKAGE\t"):
            fields = line.split("\t")
            if len(fields) != 3:
                raise RuntimeError("Malformed dpkg filesystem package header: " + line)
            current = inventory.get(fields[1] + ":" + fields[2])
            continue
        # db-fsys:Files prefixes filesystem entries with indentation. Keep
        # trailing whitespace, which can belong to a valid filename.
        line = line.lstrip()
        if current is None or not line:
            continue
        if not line.startswith("/"):
            raise RuntimeError("Non-absolute installed dpkg filesystem path: " + line)
        path = os.path.normpath(line)
        if path == "/":
            continue
        current["files"].append(path)
        # Debian puts licensing in copyright, sometimes alongside COPYING.
        filename = Path(path).name.lower()
        if path.startswith("/usr/share/doc/") and (
                filename == "copyright" or filename.startswith(("copying", "license", "licence"))):
            current["licenses"].add(path)
    for package in inventory.values():
        # A package's doc directory may be a link to another package's docs.
        copyright_path = "/usr/share/doc/" + package["name"] + "/copyright"
        if os.path.isfile(copyright_path):
            package["files"].append(copyright_path)
            package["licenses"].add(copyright_path)


def installed_inventory():
    showformat = "\t".join("${" + field + "}" for field in METADATA_FIELDS) + "\n"
    inventory = parse_inventory(query("dpkg-query", "-W", "--showformat=" + showformat))
    add_inventory_files(inventory, query(
        "dpkg-query", "-W", "--showformat=PACKAGE\t${Package}\t${Architecture}\n${db-fsys:Files}\n",
    ))
    if not inventory:
        raise RuntimeError("The source has no fully installed dpkg packages")
    return inventory


@lru_cache(maxsize=None)
def compare_versions(installed, operator, required):
    result = subprocess.run(["dpkg", "--compare-versions", installed, operator, required], check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError("dpkg could not compare Debian package versions")
    return result.returncode == 0


class DependencyResolver:
    def __init__(self, inventory, native_architecture, version_compare=compare_versions):
        self.inventory = inventory
        self.native_architecture = native_architecture
        self.version_compare = version_compare
        self.by_name = {}
        self.providers = {}
        for identity, package in sorted(inventory.items()):
            self.by_name.setdefault(package["name"], []).append(identity)
            for provided in package["provides"]:
                self.providers.setdefault(provided["name"], []).append((identity, provided["version"]))

    def architecture_matches(self, package, qualifier, depender_architecture):
        architecture = package["architecture"]
        effective = self.native_architecture if architecture == "all" else architecture
        depending = self.native_architecture if depender_architecture == "all" else depender_architecture
        if qualifier == "any":
            # Policy 5.6.34.4: only Multi-Arch: allowed permits explicit :any.
            return package["multi_arch"] == "allowed"
        if qualifier == "native":
            return effective == self.native_architecture
        if qualifier:
            return effective == qualifier
        return effective == depending or package["multi_arch"] == "foreign"

    def candidates(self, relation, depender_architecture, preferred=()):
        candidates = []
        concrete = [(identity, self.inventory[identity]["version"])
                    for identity in self.by_name.get(relation["name"], [])]
        for identity, version in concrete + self.providers.get(relation["name"], []):
            package = self.inventory[identity]
            if not self.architecture_matches(package, relation["qualifier"], depender_architecture):
                continue
            if relation["operator"] and (version is None or not self.version_compare(
                    version, relation["operator"], relation["version"])):
                continue
            if identity not in candidates:
                candidates.append(identity)
        # Prefer an existing root, then native architecture; retain concrete
        # packages ahead of virtual providers when neither criterion applies.
        order = {identity: index for index, identity in enumerate(candidates)}
        return sorted(candidates, key=lambda identity: (
            identity not in preferred,
            self.inventory[identity]["architecture"] not in (self.native_architecture, "all"),
            order[identity],
        ))

    def resolve_root(self, root):
        if root in self.inventory:
            package = self.inventory[root]
            if package["architecture"] not in ("all", self.native_architecture):
                raise RuntimeError("Ceph runtime root has a foreign architecture: " + root)
            return root
        relations = parse_relationships(root)
        if len(relations) != 1 or len(relations[0]) != 1:
            raise RuntimeError("A runtime root must name one Debian package: " + root)
        candidates = self.candidates(relations[0][0], self.native_architecture)
        if not candidates:
            raise RuntimeError("Required Debian runtime package is not installed: " + root)
        return candidates[0]

    def closure(self, roots):
        selected = {self.resolve_root(root) for root in roots}
        queue = sorted(selected)
        visited = set()
        while queue:
            identity = queue.pop()
            if identity in visited:
                continue
            visited.add(identity)
            package = self.inventory[identity]
            for alternatives in package["pre_depends"] + package["depends"]:
                chosen = None
                for relation in alternatives:
                    candidates = self.candidates(relation, package["architecture"], selected)
                    if candidates:
                        chosen = candidates[0]
                        break
                if chosen is None:
                    description = " | ".join(relation_text(relation) for relation in alternatives)
                    raise RuntimeError("Unsatisfied installed Debian dependency for %s: %s" % (identity, description))
                if chosen not in selected:
                    selected.add(chosen)
                    queue.append(chosen)
        return selected


def relation_text(relation):
    text = relation["name"]
    if relation["qualifier"]:
        text += ":" + relation["qualifier"]
    if relation["operator"]:
        text += " (%s %s)" % (relation["operator"], relation["version"])
    return text


def essential_roots(inventory):
    # Essential interfaces may be used without any explicit Depends field.
    return tuple(sorted(identity for identity, package in inventory.items() if package["essential"]))


def closure(roots, inventory, native_architecture=None):
    native = native_architecture or query("dpkg", "--print-architecture").strip()
    return DependencyResolver(inventory, native).closure(roots)


def extra_paths():
    paths = set(EXTRA_FILES)
    for tree in EXTRA_TREES:
        paths.add(tree)
        if os.path.isdir(tree) and not os.path.islink(tree):
            for directory, dirs, files in os.walk(tree, followlinks=False):
                paths.update(os.path.join(directory, name) for name in dirs + files)
    return paths


def license_paths(inventory, selected):
    licenses = set().union(*(inventory[identity]["licenses"] for identity in selected))
    licenses.update(path for path in extra_paths() if path.startswith("/usr/share/common-licenses/"))
    # Retain parent doc aliases while avoiding copying unrelated documentation.
    licenses.update(os.path.realpath(path) for path in list(licenses))
    for path in list(licenses):
        parent = Path(path).parent
        while parent != Path("/"):
            licenses.add(str(parent))
            parent = parent.parent
    return licenses


def assemble_runtime(materialized, selected, inventory):
    if materialized.exists() and any(materialized.iterdir()):
        raise RuntimeError("Runtime materialization directory must be empty")
    materialized.mkdir(exist_ok=True)
    assemble.ROOT = materialized
    assemble.copied.clear()
    assemble.hardlinks.clear()
    assemble.license_files.clear()
    assemble.license_files.update(license_paths(inventory, selected))
    paths = extra_paths().union(*(inventory[identity]["files"] for identity in selected))
    for path in sorted(paths):
        assemble.copy_path(path)
    for path in assemble.WRITABLE_DIRS:
        target = assemble.destination(path)
        target.mkdir(parents=True, exist_ok=True)
        target.chmod(0o1777 if path == "/tmp" else 0o755)
    print("Assembled %d installed Debian packages and %d source paths" % (len(selected), len(assemble.copied)))


def metadata_probe_paths(materialized):
    paths = ["etc/ceph", "var/lib/ceph", "run/ceph", "var/log/ceph", "tmp", "bin", "lib", "var/run"]
    paths += sorted(path.relative_to(materialized).as_posix()
                    for path in (materialized / "usr/lib").glob("*-linux-gnu*"))
    # Debian copyrights replace the RPM-specific Ceph COPYING location.
    for path in ("usr/share/doc/ceph-common/copyright", "usr/share/doc/ceph-base/copyright"):
        if os.path.lexists(materialized / path):
            paths.append(path)
    return [path for path in paths if os.path.lexists(materialized / path)]


def package_versions(inventory, selected):
    return sorted(identity + "=" + inventory[identity]["version"] for identity in selected)


def package_records(inventory, selected):
    return [{"package": inventory[identity]["name"],
             "architecture": inventory[identity]["architecture"],
             "version": inventory[identity]["version"],
             "source_package": inventory[identity]["source_package"],
             "source_version": inventory[identity]["source_version"]}
            for identity in sorted(selected)]


def library_directories(inventory, selected):
    """Report ELF library/plugin roots without assuming the RPM /usr/lib64."""
    result = set()
    for identity in selected:
        for path in inventory[identity]["files"]:
            if re.search(r"\.so(?:\.[^/]*)?$", path) and os.path.lexists(path):
                result.add(os.path.realpath(os.path.dirname(path)))
    return sorted(result)


def load_provenance(inventory, native_architecture, path=None):
    path = path or os.environ.get("LOCAL_PACKAGE_PROVENANCE")
    if not path:
        raise RuntimeError("Debian assembly requires LOCAL_PACKAGE_PROVENANCE for the supplied .deb files")
    provenance = json.loads(Path(path).read_text())
    packages = provenance.get("packages") if isinstance(provenance, dict) else None
    if not isinstance(packages, list) or not packages:
        raise RuntimeError("Local .deb provenance must contain a non-empty packages list")
    seen = set()
    for entry in packages:
        if not isinstance(entry, dict) or any(not isinstance(entry.get(field), str) or not entry[field]
                                               for field in ("package", "version", "architecture", "sha256")):
            raise RuntimeError("Malformed local .deb provenance entry")
        if not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]):
            raise RuntimeError("Malformed local .deb SHA256 for " + entry["package"])
        identity = entry["package"] + ":" + entry["architecture"]
        if identity in seen:
            raise RuntimeError("Duplicate local .deb provenance identity: " + identity)
        seen.add(identity)
        if entry["architecture"] not in ("all", native_architecture):
            raise RuntimeError("Local .deb architecture does not match source architecture: " + identity)
        if identity not in inventory or inventory[identity]["version"] != entry["version"]:
            raise RuntimeError("Supplied local .deb was not installed at its exact version: " + identity)
    return provenance


def validate_ceph_packages(inventory, selected, provenance):
    """Reject role daemons accidentally fetched from the distro Ceph release."""
    local = {entry["package"] + ":" + entry["architecture"] for entry in provenance["packages"]}
    required = set(ROOT_PACKAGES["control"] + ROOT_PACKAGES["osd"] + ROOT_PACKAGES["rgw"] + ROOT_PACKAGES["mds"])
    ceph_identities = {identity for identity in selected if inventory[identity]["name"] in required
                       or inventory[identity]["source_package"] == "ceph"}
    missing = sorted(ceph_identities - local)
    if missing:
        raise RuntimeError("Ceph role packages must come from supplied local .deb files; missing: " + ", ".join(missing))
    source_versions = {inventory[identity]["source_version"] for identity in ceph_identities
                       if inventory[identity]["source_package"] == "ceph"}
    if len(source_versions) > 1:
        raise RuntimeError("Local Ceph packages were built from different source versions: " + ", ".join(sorted(source_versions)))
    for role, roots in ROOT_PACKAGES.items():
        ceph = [inventory[identity] for identity in selected if inventory[identity]["name"] in roots]
        if not any(os.path.isfile(path) and os.stat(path).st_size
                   for package in ceph for path in package["licenses"]):
            raise RuntimeError("Required Debian Ceph copyright file missing for role " + role)
