#!/usr/bin/env python3
"""Free and record the filesystem under Docker's data root on a CI runner.

Every container's writable layer, the MON store included, lives on the
filesystem that holds Docker's data root. A MON raises MON_DISK_LOW when that
filesystem's available share falls below mon_data_avail_warn (30% by
default), and a fixture that requires HEALTH_OK then fails before its own
assertions (issue 365). The runner image's preinstalled toolchains take much
of that filesystem.

free removes a fixed list of preinstalled runner directories, and only on a
GitHub-hosted runner, whose machine is discarded after the job. It never
touches Docker resources. record writes one receipt per phase: the
filesystem's bytes and inodes, its available share against the MON
thresholds, and Docker's own usage by type. Comparing the receipts separates
the runner's baseline, the pulled images and the run's own allocation.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


# Directories of the GitHub-hosted Ubuntu image that no job here uses: the
# Android SDK, .NET, the Haskell toolchains, CodeQL bundles and Boost. Go and
# Python come from setup-go and the system interpreter.
PREINSTALLED = (
    "/usr/local/lib/android",
    "/usr/share/dotnet",
    "/opt/ghc",
    "/usr/local/.ghcup",
    "/opt/hostedtoolcache/CodeQL",
    "/usr/local/share/boost",
)
# Ceph's defaults, in percent of the available share.
MON_DATA_AVAIL_WARN = 30
MON_DATA_AVAIL_CRIT = 5
PHASE = re.compile(r"[a-z][a-z0-9-]{0,31}\Z")
COMMAND_SECONDS = 120


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run_command(command, timeout=COMMAND_SECONDS):
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError("%s exited %d: %s" % (" ".join(command), result.returncode, result.stderr.strip()[-500:]))
    return result.stdout


def docker_root(run):
    root = run(["docker", "info", "--format", "{{.DockerRootDir}}"]).strip()
    if not root.startswith("/"):
        raise ValueError("Docker data root is not an absolute path: %r" % root)
    return root


def filesystem(path, statvfs=os.statvfs):
    """Bytes and inodes of the filesystem holding path, and its available
    share as Ceph computes it for a MON: f_bavail over f_blocks, floored."""
    value = statvfs(path)
    total, avail = value.f_blocks * value.f_frsize, value.f_bavail * value.f_frsize
    if total <= 0:
        raise ValueError("filesystem of %s reports no blocks" % path)
    percent = avail * 100 // total
    return {"path": path, "total_bytes": total, "available_bytes": avail,
            "free_bytes": value.f_bfree * value.f_frsize, "inodes": value.f_files,
            "free_inodes": value.f_ffree, "available_percent": percent,
            "below_mon_warn": percent < MON_DATA_AVAIL_WARN, "below_mon_crit": percent < MON_DATA_AVAIL_CRIT}


def docker_usage(run):
    """docker system df by type: images, containers, local volumes, build cache."""
    usage = {}
    for line in run(["docker", "system", "df", "--format", "{{json .}}"]).splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        kind = row.get("Type")
        if not isinstance(kind, str) or kind in usage:
            raise ValueError("docker system df row without a distinct type: %r" % line)
        usage[kind] = {key: row.get(key) for key in ("TotalCount", "Active", "Size", "Reclaimable")}
    if not usage:
        raise ValueError("docker system df reported nothing")
    return usage


def write(directory, name, report):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    report["finished_at"] = timestamp()
    content = json.dumps(report, indent=2) + "\n"
    # Exclusive creation keeps an earlier receipt of the same phase.
    with (directory / name).open("x", encoding="utf-8") as output:
        output.write(content)


def summary(phase, fs):
    gib = 1 << 30
    return ("Runner storage %s: %.1f of %.1f GiB available (%d%%) under %s; MON_DISK_LOW below %d%%%s"
            % (phase, fs["available_bytes"] / gib, fs["total_bytes"] / gib, fs["available_percent"], fs["path"],
               MON_DATA_AVAIL_WARN, ", below now" if fs["below_mon_warn"] else ""))


def record(directory, phase, run=run_command, statvfs=os.statvfs):
    if not PHASE.match(phase) or phase == "free":
        raise ValueError("phase must be a short lowercase name other than free: %r" % phase)
    report = {"schema": 1, "phase": phase, "started_at": timestamp()}
    root = docker_root(run)
    report["filesystem"] = filesystem(root, statvfs)
    report["docker"] = docker_usage(run)
    write(directory, "storage-%s.json" % phase, report)
    return report


def free(directory, environ=os.environ, run=run_command, statvfs=os.statvfs, exists=os.path.exists, monotonic=time.monotonic):
    if environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("free runs only on a GitHub-hosted runner, whose machine is discarded after the job")
    report = {"schema": 1, "phase": "free", "started_at": timestamp()}
    root = docker_root(run)
    report["before"] = filesystem(root, statvfs)
    present = [path for path in PREINSTALLED if exists(path)]
    started = monotonic()
    if present:
        run(["sudo", "rm", "-rf", "--one-file-system", *present], timeout=600)
    report["removed"] = present
    report["seconds"] = round(monotonic() - started, 3)
    report["filesystem"] = filesystem(root, statvfs)
    report["docker"] = docker_usage(run)
    write(directory, "storage-free.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("free", "record"):
        child = subparsers.add_parser(action)
        child.add_argument("--directory", type=Path, required=True)
        if action == "record":
            child.add_argument("--phase", required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "free":
            report = free(args.directory)
            print("Freed %d preinstalled directories in %.0f s" % (len(report["removed"]), report["seconds"]))
            print(summary("before free", report["before"]))
            print(summary("after free", report["filesystem"]), flush=True)
        else:
            report = record(args.directory, args.phase)
            print(summary(args.phase, report["filesystem"]), flush=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print("Runner storage %s FAIL: %s" % (args.action, error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
