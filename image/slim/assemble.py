#!/usr/bin/env python3
"""Assemble an offline runtime from the installed Ceph image's RPM closure."""

import os
from pathlib import Path
import shutil
import stat
import subprocess


ROOT = Path("/runtime-rootfs")
PACKAGES = (
    "ceph-mon", "ceph-mgr", "ceph-osd", "ceph-mds", "ceph-radosgw",
    "ceph-common", "python3-cephfs", "rbd-mirror", "cephfs-mirror",
    "bash", "coreutils-single", "hostname",
    "gawk", "ca-certificates", "filesystem",
)
OMIT = ("/usr/share/doc", "/usr/share/man", "/usr/share/info", "/dev", "/proc", "/sys")
EXTRA_FILES = (
    "/bin", "/lib", "/lib64", "/sbin", "/etc/alternatives",
    "/etc/passwd", "/etc/group", "/etc/nsswitch.conf", "/etc/ld.so.cache",
)
EXTRA_TREES = ("/etc/pki/ca-trust", "/etc/pki/tls", "/etc/ssl")
WRITABLE_DIRS = (
    "/etc/ceph", "/var/lib/ceph", "/var/run/ceph", "/var/log/ceph",
    "/tmp", "/tc", "/dev", "/proc", "/sys",
)


def query(*args):
    result = subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE)
    return result.stdout.splitlines()


def omitted(path):
    if path in license_files:
        return False
    return (
        any(path == prefix or path.startswith(prefix + "/") for prefix in OMIT)
        or "__pycache__" in Path(path).parts
        or path.endswith((".pyc", ".pyo"))
    )


def destination(path):
    # Absolute symlinks inside a staging root would otherwise lead writes back
    # into the build image. Resolve source parents, but preserve the final link.
    parent = os.path.realpath(os.path.dirname(path))
    return ROOT / parent.lstrip("/") / os.path.basename(path)


copied = set()
hardlinks = {}
license_files = set()


def copy_path(path):
    path = os.path.normpath(path)
    if path in copied or omitted(path) or not os.path.lexists(path):
        return
    copied.add(path)
    info = os.lstat(path)
    if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
        return
    if path == "/":
        return
    copy_path(os.path.dirname(path))
    target = destination(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if stat.S_ISLNK(info.st_mode):
        link = os.readlink(path)
        source_target = link if os.path.isabs(link) else os.path.join(os.path.dirname(path), link)
        copy_path(source_target)
        if not os.path.lexists(target):
            target.symlink_to(link)
    elif stat.S_ISDIR(info.st_mode):
        target.mkdir(exist_ok=True)
        shutil.copystat(path, target)
    else:
        inode = (info.st_dev, info.st_ino)
        if os.path.lexists(target):
            # RPM payloads may mention both /lib64/foo and /usr/lib64/foo.
            return
        if inode in hardlinks:
            os.link(hardlinks[inode], target)
        else:
            shutil.copy2(path, target)
            if info.st_nlink > 1:
                hardlinks[inode] = target
    os.chown(target, info.st_uid, info.st_gid, follow_symlinks=False)
    # chown may clear set-id bits; restore source modes after ownership.
    shutil.copystat(path, target, follow_symlinks=False)


def copy_tree(path):
    copy_path(path)
    if os.path.isdir(path) and not os.path.islink(path):
        for directory, dirs, files in os.walk(path, followlinks=False):
            for name in dirs + files:
                copy_path(os.path.join(directory, name))


def main():
    if ROOT.exists() and any(ROOT.iterdir()):
        raise RuntimeError("/runtime-rootfs must be empty before assembly")
    ROOT.mkdir(exist_ok=True)
    resolved = query(
        "dnf", "-q", "--cacheonly", "repoquery", "--installed", "--requires",
        "--resolve", "--recursive", "--qf", "%{name}", *PACKAGES,
    )
    packages = sorted(set(PACKAGES).union(name for name in resolved if name))
    versions = query("rpm", "-q", "--qf", "%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n", *packages)
    files = query("rpm", "-ql", *packages)
    flags = query("rpm", "-q", "--qf", "[%{FILENAMES}\t%{FILEFLAGS:fflags}\n]", *packages)
    for entry in flags:
        path, attributes = entry.rsplit("\t", 1)
        if "l" in attributes:
            # Some packages keep %license files under otherwise omitted docs.
            license_files.add(os.path.normpath(path))
    for path in sorted(set(files).union(EXTRA_FILES)):
        copy_path(path)
    for path in EXTRA_TREES:
        copy_tree(path)
    for path in WRITABLE_DIRS:
        # /var/run is commonly an absolute symlink to /run.
        target = destination(path)
        target.mkdir(parents=True, exist_ok=True)
        target.chmod(0o1777 if path == "/tmp" else 0o755)
    manifest = ROOT / "usr/share/ceph-testcontainers/runtime-packages.txt"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    source = os.environ.get("SOURCE_IMAGE", "unspecified")
    manifest.write_text("# source-image: " + source + "\n" + "\n".join(sorted(versions)) + "\n")
    print("Assembled %d packages and %d source paths" % (len(packages), len(copied)))


if __name__ == "__main__":
    main()
