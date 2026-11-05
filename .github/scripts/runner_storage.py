#!/usr/bin/env python3
"""Prepare and record actual Docker storage on disposable CI runners.

Only an exact source/image cohort with independently sufficient filesystem
headroom may skip the existing allowlist cleanup. Unknown work/layout keeps
cleanup. Both paths recheck bytes and inodes; a post-image check gates native
assertions before Ceph's 30% MON free-space threshold can be crossed.
"""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import runner_storage_layout


# Directories of the GitHub-hosted Ubuntu image that no job here uses: the
# Android SDK, .NET, Haskell, CodeQL and Boost. Go and Python come from
# setup-go and the system interpreter. Keep the ordinary cleanup unchanged.
PREINSTALLED = (
    "/usr/local/lib/android",
    "/usr/share/dotnet",
    "/opt/ghc",
    "/usr/local/.ghcup",
    "/opt/hostedtoolcache/CodeQL",
    "/usr/local/share/boost",
)
# Only after ordinary cleanup still fails admission: additional SDKs not used
# by integration jobs. Never expand this to the runner, tool-cache parents,
# system Python or Docker/containerd data.
SUPPLEMENTAL = (
    "/usr/share/swift",
    "/usr/local/share/powershell",
    "/opt/microsoft/powershell",
    "/usr/local/share/vcpkg",
)
# Ceph's defaults, in percent of the available share.
MON_DATA_AVAIL_WARN = 30
MON_DATA_AVAIL_CRIT = 5
PHASE = re.compile(r"[a-z][a-z0-9-]{0,31}\Z")
COMMAND_SECONDS = 120
PRESSURE_POLICIES = ("pressure", "pressure-supplemental")
# Docker engine platforms whose hosted runner storage layout has been
# measured. Another native architecture's runner keeps the conservative
# cleanup and minimum headroom guard, and its receipts record the layout it
# actually has; extending this needs that platform's own measured cohort
# budget, never a reuse of another platform's qualification.
QUALIFIED_STORAGE_PLATFORMS = ("linux/amd64",)
ARCHITECTURES = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
POLICIES = ("auto", "always", *PRESSURE_POLICIES)


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


def free(directory, environ=os.environ, run=run_command, statvfs=os.statvfs, exists=os.path.exists, monotonic=time.monotonic, supplemental=False):
    if environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("free runs only on a GitHub-hosted runner, whose machine is discarded after the job")
    phase = "free-supplemental" if supplemental else "free"
    report = {"schema": 1, "phase": phase, "started_at": timestamp()}
    root = docker_root(run)
    report["before"] = filesystem(root, statvfs)
    present = [path for path in (SUPPLEMENTAL if supplemental else PREINSTALLED) if exists(path)]
    started = monotonic()
    if present:
        run(["sudo", "rm", "-rf", "--one-file-system", *present], timeout=600)
    report["removed"] = present
    report["seconds"] = round(monotonic() - started, 3)
    report["filesystem"] = filesystem(root, statvfs)
    report["docker"] = docker_usage(run)
    write(directory, "storage-%s.json" % phase, report)
    return report


GIB = 1 << 30
ROOT = Path(__file__).resolve().parents[2]


def qualification(compile_directory, release, roles, digests="", tests="", root=ROOT, environ=os.environ):
    """An exact workload declaration/dependency and immutable image cohort only.

    A workload/dependency or image-pin change expires the small qualification;
    it does not grant an unmeasured workload the old storage allowance.
    """
    try:
        catalog = json.loads((root / ".github/runner-storage.json").read_text())
        compiled = json.loads((Path(compile_directory) / "compile-report.json").read_text())
        profile, binary = compiled["profile"], compiled["binary"]
        key = binary["build_key"]
        if catalog.get("schema") != 2 or not isinstance(catalog.get("profiles"), list):
            return None
        matches = [entry for entry in catalog["profiles"] if entry.get("profile") == profile["id"]]
        if len(matches) != 1:
            return None
        budget = matches[0]
        if "max_parallel_fixtures" in budget:
            limit = budget["max_parallel_fixtures"]
            raw = environ.get("CEPH_TEST_PARALLEL", "")
            if not isinstance(raw, str) or raw and re.fullmatch(r"\+?[0-9]+", raw) is None:
                return None
            parallel = int(raw) if raw else 1
            if type(limit) is not int or limit < 1 or not 1 <= parallel <= limit:
                return None
        selected = sorted(tests.split(",") if tests else binary["tests"])
        # Startup/data helpers are part of the workload's storage bound too.
        # Keep their declarations pinned without expiring on unrelated code.
        paths = sorted(set(profile["sources"] + budget.get("helper_sources", []) + ["go.mod", "go.sum"]))
        sources = {path: binary["sources"][path] for path in paths}
        source_hash = hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        pins_file = root / ".github/ceph-role-images.json"
        if (compiled.get("passed") is not True
                or profile["id"] != budget["profile"] or profile["package"] != budget["package"]
                or key["tags"] != budget["tags"]
                or selected != budget["tests"] or not set(selected) <= set(binary["tests"])
                or roles != budget["roles"] or profile["image_roles"] != roles
                or release not in budget["releases"]
                or key["goos"] + "/" + key["goarch"] != budget["platform"]
                or source_hash != budget["sources_sha256"]
                or hashlib.sha256(pins_file.read_bytes()).hexdigest() != budget["role_pins_sha256"]):
            return None
        pins = json.loads(pins_file.read_text())["images"][release]["official"]
        overrides = dict(entry.split("=", 1) for entry in digests.split(",") if entry)
        if any(role not in pins or digest != pins[role] for role, digest in overrides.items()):
            return None
        for name in ("image_prepare_bytes", "runtime_bytes", "minimum_free_inodes"):
            if type(budget[name]) is not int or budget[name] <= 0:
                return None
        return budget
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


def storage_snapshot(run=run_command, statvfs=os.statvfs, stat=os.stat, environ=os.environ, layout=None):
    info = json.loads(run(["docker", "info", "--format", "{{json .}}"]))
    layout = (layout or runner_storage_layout.storage_roots)(info, run)
    # A remote Docker context's host paths do not describe this process's
    # statvfs view. Never use them to authorize skipping reclamation.
    context = json.loads(run(["docker", "context", "inspect"]))
    endpoint = context[0]["Endpoints"]["docker"]["Host"]
    if (endpoint != "unix:///var/run/docker.sock" or environ.get("DOCKER_HOST") not in (None, "", endpoint)
            or environ.get("DOCKER_CONTEXT") not in (None, "", "default")):
        layout["verified"] = False
        layout["reasons"].append("docker-endpoint-not-qualified")
    architecture = info.get("Architecture") if isinstance(info.get("Architecture"), str) else ""
    platform = str(info.get("OSType", "")) + "/" + ARCHITECTURES.get(architecture, architecture)
    if platform not in QUALIFIED_STORAGE_PLATFORMS:
        layout["verified"] = False
        layout["reasons"].append("docker-platform-not-qualified")
    filesystems = {}
    docker_device = stat(info["DockerRootDir"]).st_dev
    for path in layout["roots"]:
        device = stat(path).st_dev
        if device in filesystems:
            filesystems[device]["paths"].append(path)
            continue
        fs = filesystem(path, statvfs)
        fs.update(device=device, paths=[path], docker_root=device == docker_device)
        filesystems[device] = fs
    if not filesystems:
        raise ValueError("no Docker storage filesystem could be inspected")
    return {"layout": layout, "filesystems": list(filesystems.values()),
            "engine": {"driver": info.get("Driver"), "driver_status": info.get("DriverStatus"),
                       "platform": platform, "architecture": architecture}}


def engine_budget(snapshot, budget):
    """The budget only for the engine platform it measured, else None: an
    amd64 cohort budget is no allowance on another architecture's engine."""
    if budget is None or budget.get("platform") == snapshot["engine"]["platform"]:
        return budget
    return None


def headroom(snapshot, budget, before_images):
    """Independent requirements per filesystem; never add unrelated free space.

    Reserve 30% on every candidate store filesystem after the image and
    workload allowance. Writable snapshots can hold the MON store on a
    different device from Docker's metadata root, so no candidate loses the
    floor based on a guessed MON location. Unknown work retains cleanup and a
    post-cleanup minimum guard, not a claim that its peak has been measured.
    """
    runtime = budget["runtime_bytes"] if budget else 2 * GIB
    images = budget["image_prepare_bytes"] if budget and before_images else 0
    inodes = budget["minimum_free_inodes"] if budget else 100000
    requirements = []
    for fs in snapshot["filesystems"]:
        mon = (fs["total_bytes"] * MON_DATA_AVAIL_WARN + 99) // 100
        required = mon + runtime + images
        requirements.append({"device": fs["device"], "paths": fs["paths"], "available_bytes": fs["available_bytes"],
                             "required_bytes": required, "minimum_free_inodes": inodes,
                             "free_inodes": fs["free_inodes"],
                             "passed": fs["available_bytes"] >= required and fs["free_inodes"] >= inodes})
    return {"passed": all(row["passed"] for row in requirements), "requirements": requirements,
            "runtime_bytes": runtime, "image_prepare_bytes": images,
            "qualification": "measured-cohort" if budget else "conservative-minimum-only"}


def pressure_size(observed, budget, supplemental=False):
    """Qualification only: cross the policy boundary while retaining Ceph's floor.

    At most 40 GiB (64 GiB after precleaning) on a disposable hosted runner;
    never approach ENOSPC.
    The file remains present during native assertions and is removed by the
    job's always-run owned cleanup step.
    """
    rows = headroom(observed, budget, True)["requirements"]
    target = next(row for row, fs in zip(rows, observed["filesystems"]) if fs["docker_root"])
    gap, maximum = (4 * GIB, 64 * GIB) if supplemental else (GIB, 40 * GIB)
    amount = target["available_bytes"] - target["required_bytes"] + gap
    if not GIB <= amount <= maximum:
        raise RuntimeError("qualification pressure would exceed its 1..%d GiB bound" % (maximum // GIB))
    fs = next(fs for fs in observed["filesystems"] if fs["docker_root"])
    floor = (fs["total_bytes"] * MON_DATA_AVAIL_WARN + 99) // 100 + budget["runtime_bytes"] + 2 * GIB
    if target["available_bytes"] - amount < floor:
        raise RuntimeError("qualification pressure would compromise the Ceph floor")
    return amount


def prepare(directory, budget, policy="auto", run=run_command, snapshot=None, reclaim=None, reclaim_supplemental=None):
    """Skip only a qualified sufficient-space workload; retain the fixed cleanup otherwise."""
    if policy not in POLICIES:
        raise ValueError("unknown storage policy: %s" % policy)
    snapshot = snapshot or (lambda: storage_snapshot(run))
    reclaim = reclaim or (lambda: free(directory, run=run))
    reclaim_supplemental = reclaim_supplemental or (lambda: free(directory, run=run, supplemental=True))
    report = {"schema": 2, "phase": "prepare", "started_at": timestamp(), "passed": False,
              "policy": policy, "qualified": budget is not None}
    try:
        report["before"] = snapshot()
        if budget is not None and engine_budget(report["before"], budget) is None:
            report["qualified"], report["budget_refused"] = False, "engine-platform-differs"
            budget = None
        preclean = None
        if policy in PRESSURE_POLICIES:
            if (os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted" or budget is None
                    or not report["before"]["layout"]["verified"]):
                raise RuntimeError("qualification pressure needs a verified qualified GitHub-hosted runner")
            if policy == "pressure-supplemental":
                # Consume the ordinary reclaimable SDKs first so injected
                # pressure cannot be repaired by that same baseline again.
                preclean = reclaim()
                report["standard_cleanup_before_pressure"] = True
                report["before"] = snapshot()
                if not report["before"]["layout"]["verified"]:
                    raise RuntimeError("storage layout changed during qualification setup")
            directory = Path(directory)
            directory.mkdir(parents=True, exist_ok=True)
            target = next(fs for fs in report["before"]["filesystems"] if fs["docker_root"])
            if directory.stat().st_dev != target["device"]:
                raise RuntimeError("qualification pressure directory is not on the Docker filesystem")
            already_short = not headroom(report["before"], budget, True)["passed"]
            amount = (0 if preclean is not None and already_short else
                      pressure_size(report["before"], budget, policy == "pressure-supplemental"))
            if amount:
                pressure = directory / "qualification-pressure.bin"
                with pressure.open("xb") as output:
                    identity = os.fstat(output.fileno())
                    write(directory, "storage-pressure.json", {"schema": 1, "device": identity.st_dev,
                                                               "inode": identity.st_ino, "bytes": amount})
                    os.posix_fallocate(output.fileno(), 0, amount)
            report["pressure_bytes"] = amount
            report["unpressured"] = report["before"]
            report["before"] = snapshot()
        report["before_headroom"] = headroom(report["before"], budget, True)
        if policy in PRESSURE_POLICIES and report["before_headroom"]["passed"]:
            raise RuntimeError("qualification pressure did not cross the headroom boundary")
        reason = ("comparison-always" if policy == "always" else "unknown-workload" if budget is None
                  else "unknown-storage-layout" if not report["before"]["layout"]["verified"]
                  else "insufficient-headroom" if not report["before_headroom"]["passed"] else "sufficient-headroom")
        report["decision"] = "skip" if reason == "sufficient-headroom" else "reclaim"
        report["reason"] = reason
        report["reclamation_seconds"] = 0
        if report["decision"] == "reclaim":
            freed = preclean if preclean is not None else reclaim()
            report["reclamation_seconds"] = freed["seconds"]
            report["removed"] = freed["removed"]
        report["after"] = snapshot()
        report["headroom"] = headroom(report["after"], budget, True)
        if not report["headroom"]["passed"] and report["decision"] == "reclaim":
            # A sufficient standard cleanup never pays for extra SDK removal.
            # Retain this boundary so the supplemental action and its actual
            # contribution remain observable rather than assumed from sizes.
            report["after_standard"] = report["after"]
            report["standard_headroom"] = report["headroom"]
            extra = reclaim_supplemental()
            report["supplemental_removed"] = extra["removed"]
            report["reclamation_seconds"] += extra["seconds"]
            report["removed"] = report.get("removed", []) + extra["removed"]
            report["after"] = snapshot()
            report["headroom"] = headroom(report["after"], budget, True)
        if not report["headroom"]["passed"]:
            raise RuntimeError("insufficient Docker filesystem headroom after preparation; see storage-prepare.json")
        report["passed"] = True
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    write(directory, "storage-prepare.json", report)
    return report


def release_pressure(directory):
    directory = Path(directory)
    pressure, receipt = directory / "qualification-pressure.bin", directory / "storage-pressure.json"
    if not pressure.exists():
        return
    if not receipt.exists():
        raise RuntimeError("refusing to remove an unowned qualification pressure file")
    owner, identity = json.loads(receipt.read_text()), pressure.stat()
    if owner.get("device") != identity.st_dev or owner.get("inode") != identity.st_ino:
        raise RuntimeError("qualification pressure file identity changed; refusing removal")
    pressure.unlink()


def ready(directory, budget, run=run_command, snapshot=None):
    """Recheck after actual pulls, before native tests can encounter MON_DISK_LOW."""
    observed = (snapshot or (lambda: storage_snapshot(run)))()
    allowed = engine_budget(observed, budget)
    result = headroom(observed, allowed, False)
    report = {"schema": 2, "phase": "ready", "started_at": timestamp(), "snapshot": observed,
              "headroom": result, "passed": result["passed"]}
    if budget is not None and allowed is None:
        report["budget_refused"] = "engine-platform-differs"
    write(directory, "storage-ready.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("free", "record", "prepare", "ready", "release-pressure"):
        child = subparsers.add_parser(action)
        child.add_argument("--directory", type=Path, required=True)
        if action == "record":
            child.add_argument("--phase", required=True)
        if action in ("prepare", "ready"):
            child.add_argument("--compile", type=Path, required=True)
            child.add_argument("--release", required=True)
            child.add_argument("--roles", required=True)
            child.add_argument("--digests", default="")
            child.add_argument("--tests", default="")
            if action == "prepare":
                child.add_argument("--policy", choices=POLICIES, default="auto")
    args = parser.parse_args(argv)
    try:
        if args.action == "release-pressure":
            # This single exclusive-created file is the only qualification
            # allocation; never delete pre-existing directories or Docker data.
            release_pressure(args.directory)
            print("Qualification pressure allocation removed or already absent.", flush=True)
            return 0
        if args.action in ("prepare", "ready"):
            budget = qualification(args.compile, args.release, args.roles, args.digests, args.tests)
            report = (prepare(args.directory, budget, args.policy) if args.action == "prepare"
                      else ready(args.directory, budget))
            print("Runner storage %s: %s" % (args.action, json.dumps(report, sort_keys=True)), flush=True)
            if os.environ.get("GITHUB_STEP_SUMMARY"):
                with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
                    output.write("\nRunner storage %s: %s; passed=%s; reclamation=%.3fs. See runner-storage receipts.\n" %
                                 (args.action, report.get("decision", "post-image check"), report["passed"],
                                  report.get("reclamation_seconds", 0)))
            if not report["passed"]:
                print("::error title=Runner storage preflight::Insufficient or unverified preparation; inspect runner-storage receipts.")
            return 0 if report["passed"] else 1
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
