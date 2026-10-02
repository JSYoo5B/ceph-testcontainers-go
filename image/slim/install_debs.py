#!/usr/bin/env python3
"""Install caller-supplied Ceph .debs and record their audited provenance.

Run inside the selected Debian/Ubuntu base image. APT may resolve ordinary
distro dependencies, but every installed binary from the Ceph source package
must have been supplied locally. No package filenames are interpreted by a
shell, and filenames do not determine package identities or versions.
"""

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile


# Keep these aligned with debian_packages.ROOT_PACKAGES for the five images.
REQUIRED_ROOT_PACKAGES = frozenset((
    "ceph-mon", "ceph-mgr", "ceph-common", "python3-rados", "python3-rbd",
    "python3-cephfs", "rbd-mirror", "cephfs-mirror", "ceph-osd", "radosgw",
    "ceph-mds",
))
PACKAGE_NAME = re.compile(r"[a-z0-9][a-z0-9+.-]+\Z")
ARCHITECTURE = re.compile(r"[a-z0-9][a-z0-9-]*\Z")
SOURCE_FIELD = re.compile(r"([a-z0-9][a-z0-9+.-]+)(?:\s+\(([^\s()]+)\))?\Z")
SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")
INSTALLED_FIELDS = (
    "Package", "Version", "Architecture", "db:Status-Status",
    "db:Status-Eflag", "source:Package", "source:Version",
)
INSTALLED_FORMAT = "\t".join("${" + field + "}" for field in INSTALLED_FIELDS) + "\n"


@dataclass(frozen=True)
class LocalPackage:
    path: Path
    filename: str
    sha256: str
    package: str
    version: str
    architecture: str
    source: str
    source_version: str

    @property
    def identity(self):
        return self.package + ":" + self.architecture

    def provenance(self):
        return {
            "package": self.package, "version": self.version,
            "architecture": self.architecture, "sha256": self.sha256,
            "filename": self.filename, "source": self.source,
            "source_version": self.source_version,
        }


def command_environment():
    return dict(os.environ, DEBIAN_FRONTEND="noninteractive", LC_ALL="C")


def query(*arguments):
    return subprocess.run(arguments, check=True, text=True, stdout=subprocess.PIPE,
                          env=command_environment()).stdout


def install(arguments):
    subprocess.run(arguments, check=True, env=command_environment())


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_control(output):
    """Read dpkg-deb --field output, retaining folded control fields."""
    fields = {}
    current = None
    for line in output.splitlines():
        if not line:
            continue
        if line.startswith((" ", "\t")):
            if current is None:
                raise RuntimeError("Debian control continuation has no field")
            fields[current] += "\n" + line[1:]
            continue
        name, separator, value = line.partition(":")
        if not separator or not name or name in fields:
            raise RuntimeError("Malformed or duplicate Debian control field: " + line)
        current = name
        fields[name] = value.strip()
    return fields


def source_identity(value, package, version):
    if not value:
        return package, version
    match = SOURCE_FIELD.fullmatch(value)
    if not match:
        raise RuntimeError("Malformed Debian Source field: " + value)
    source, source_version = match.groups()
    return source, source_version or version


def basename(value, field):
    if (not isinstance(value, str) or not value or value in (".", "..") or
            "/" in value or "\\" in value or not value.endswith(".deb")):
        raise RuntimeError(field + " must be a .deb basename")
    return value


def verify_hashes(packages):
    for package in packages:
        # A maintainer script must not replace a staged archive with a link.
        if not stat.S_ISREG(package.path.lstat().st_mode):
            raise RuntimeError("Staged .deb is not a regular file: " + str(package.path))
        actual = file_sha256(package.path)
        if actual != package.sha256:
            raise RuntimeError("SHA-256 mismatch for " + package.filename +
                               ": expected " + package.sha256 + ", got " + actual)


def collect_local_packages(directory, entries, native_architecture, query_fn=query,
                           required_packages=REQUIRED_ROOT_PACKAGES):
    directory = directory.resolve(strict=True)
    if not directory.is_dir() or not isinstance(entries, list) or not entries:
        raise RuntimeError("A .deb directory and nonempty packages array are required")
    if not ARCHITECTURE.fullmatch(native_architecture) or native_architecture == "all":
        raise RuntimeError("Invalid native dpkg architecture: " + native_architecture)
    packages = []
    staged_names = set()
    identities = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("Each packages entry must be an object")
        original = basename(entry.get("filename"), "filename")
        staged = basename(entry.get("staged_filename"), "staged_filename")
        expected_hash = entry.get("sha256")
        if not isinstance(expected_hash, str) or not SHA256.fullmatch(expected_hash):
            raise RuntimeError("Invalid SHA-256 for " + original)
        if staged in staged_names:
            raise RuntimeError("Duplicate staged .deb filename: " + staged)
        staged_names.add(staged)
        path = directory / staged
        if not stat.S_ISREG(path.lstat().st_mode):
            raise RuntimeError("Staged .deb is not a regular file: " + str(path))
        actual_hash = file_sha256(path)
        if actual_hash != expected_hash.lower():
            raise RuntimeError("SHA-256 mismatch for " + original)
        control = parse_control(query_fn("dpkg-deb", "--field", str(path)))
        package, version, architecture = (control.get(field, "") for field in
                                          ("Package", "Version", "Architecture"))
        if not PACKAGE_NAME.fullmatch(package) or not version or any(char.isspace() for char in version):
            raise RuntimeError("Invalid Debian package name or version in " + original)
        if architecture not in (native_architecture, "all"):
            raise RuntimeError("Foreign .deb architecture for " + package + ": " + architecture +
                               "; native architecture is " + native_architecture)
        source, source_version = source_identity(control.get("Source", ""), package, version)
        record = LocalPackage(path, original, actual_hash, package, version,
                              architecture, source, source_version)
        if record.identity in identities:
            raise RuntimeError("Duplicate local Debian package identity: " + record.identity)
        identities.add(record.identity)
        packages.append(record)
    supplied_files = {path.name for path in directory.glob("*.deb")}
    if supplied_files != staged_names:
        raise RuntimeError("Staged .deb files differ from the input manifest: " +
                           ", ".join(sorted(supplied_files ^ staged_names)))
    missing = set(required_packages) - {package.package for package in packages}
    if missing:
        raise RuntimeError("Required Ceph role packages must be supplied locally: " +
                           ", ".join(sorted(missing)))
    return sorted(packages, key=lambda package: package.identity)


def parse_installed(output):
    result = {}
    for line in output.splitlines():
        fields = line.split("\t")
        if len(fields) != len(INSTALLED_FIELDS):
            raise RuntimeError("Malformed dpkg-query inventory line: " + line)
        record = dict(zip(INSTALLED_FIELDS, fields))
        if record["db:Status-Status"] != "installed":
            continue
        name, version, architecture = (record[field] for field in
                                       ("Package", "Version", "Architecture"))
        if (record["db:Status-Eflag"] != "ok" or not PACKAGE_NAME.fullmatch(name) or
                not version or not ARCHITECTURE.fullmatch(architecture)):
            raise RuntimeError("Incomplete or broken installed Debian package: " + name)
        identity = name + ":" + architecture
        if identity in result:
            raise RuntimeError("Duplicate installed Debian package identity: " + identity)
        result[identity] = {
            "package": name, "version": version, "architecture": architecture,
            "source": record["source:Package"] or name,
            "source_version": record["source:Version"] or version,
        }
    return result


def audit_installed(packages, inventory):
    local = {package.identity: package for package in packages}
    for identity, installed in inventory.items():
        if installed["source"] == "ceph" and identity not in local:
            raise RuntimeError("Installed Ceph package was not supplied locally: " + identity)
    for identity, package in local.items():
        installed = inventory.get(identity)
        if installed is None:
            raise RuntimeError("Local Debian package is not fully installed: " + identity)
        for field in ("package", "version", "architecture", "source", "source_version"):
            expected = getattr(package, field)
            if installed[field] != expected:
                raise RuntimeError("Installed " + identity + " has unexpected " + field +
                                   ": " + installed[field] + "; expected " + expected)


@contextmanager
def suppress_service_start(policy_path=Path("/usr/sbin/policy-rc.d")):
    """Restore any original policy, including symlinks, after APT failures."""
    policy_path = Path(policy_path)
    if os.path.lexists(policy_path) and not (policy_path.is_file() or policy_path.is_symlink()):
        raise RuntimeError("Service policy is not a file or symlink: " + str(policy_path))
    with tempfile.TemporaryDirectory(prefix=".ceph-policy-", dir=policy_path.parent) as temporary:
        backup = Path(temporary) / "original"
        if os.path.lexists(policy_path):
            os.replace(policy_path, backup)
        try:
            policy_path.write_text("#!/bin/sh\nexit 101\n", encoding="ascii")
            policy_path.chmod(0o755)
            yield
        finally:
            if os.path.lexists(policy_path):
                policy_path.unlink()
            if os.path.lexists(backup):
                os.replace(backup, policy_path)


def install_and_record(directory, inputs, output, query_fn=query, install_fn=install,
                       policy_path=Path("/usr/sbin/policy-rc.d"),
                       required_packages=REQUIRED_ROOT_PACKAGES):
    if not isinstance(inputs, dict):
        raise RuntimeError("The input manifest must be an object")
    for field in ("base_image", "base_image_id", "platform"):
        if not isinstance(inputs.get(field), str) or not inputs[field]:
            raise RuntimeError("The input manifest needs " + field)
    native_architecture = query_fn("dpkg", "--print-architecture").strip()
    packages = collect_local_packages(directory, inputs.get("packages"), native_architecture,
                                      query_fn, required_packages)
    with suppress_service_start(policy_path):
        # APT can classify an explicitly selected local archive as a downgrade
        # even when its metadata version matches an installed public package.
        # Permit that selection so a repacked same-version local payload is
        # actually reinstalled; the exact-version/source audit still follows.
        install_fn(["apt-get", "-y", "--no-install-recommends", "--reinstall",
                    "--allow-downgrades", "install"] +
                   [str(package.path) for package in packages])
    verify_hashes(packages)
    inventory = parse_installed(query_fn("dpkg-query", "-W", "--showformat=" + INSTALLED_FORMAT))
    audit_installed(packages, inventory)
    provenance = {field: inputs[field] for field in ("base_image", "base_image_id", "platform")}
    provenance.update({
        "package_manager": "dpkg", "native_architecture": native_architecture,
        "packages": [package.provenance() for package in packages],
    })
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent,
                                     prefix=".source-packages-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(provenance, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.close()
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    return provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    inputs = json.loads(arguments.input.read_text(encoding="utf-8"))
    provenance = install_and_record(arguments.directory, inputs, arguments.output)
    print("Audited " + str(len(provenance["packages"])) + " local .deb packages on " +
          provenance["native_architecture"])


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print("Local .deb installation failed: " + str(error), file=sys.stderr)
        sys.exit(1)
