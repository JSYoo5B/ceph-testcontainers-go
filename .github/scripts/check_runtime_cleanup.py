#!/usr/bin/env python3
"""Record and check Testcontainers resource cleanup without changing Docker.

Use snapshot before a runtime profile and check afterwards, including failures.
The comparison includes running/stopped containers, networks, and Ryuk, but
excludes Testcontainers resources already present in the snapshot. This scope
assumes an exclusive CI Docker engine; concurrent unrelated new Testcontainers
sessions on a shared engine cannot be attributed to the selected profile.
"""

import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time


LABEL = "org.testcontainers=true"
RESOURCE_KINDS = ("containers", "networks")
RESOURCE_COMMANDS = {
    "containers": ["docker", "container", "ls", "--all", "--quiet",
                   "--no-trunc", "--filter", "label=" + LABEL],
    "networks": ["docker", "network", "ls", "--quiet", "--no-trunc",
                 "--filter", "label=" + LABEL],
}
ENGINE_COMMAND = ["docker", "info", "--format", "{{json .}}"]
ID_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
READINESS_SECONDS = 60
STDERR_LIMIT = 65536


def command_stderr(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else ""


def startup_connection_failure(stderr):
    """Recognize daemon socket startup failures, never permission failures."""
    stderr = stderr.lower()
    if any(value in stderr for value in ("permission denied", "access is denied",
                                        "access denied", "authorization denied",
                                        "unauthorized", "forbidden")):
        return False
    if "unix://" not in stderr and "dial unix" not in stderr:
        return False
    if "cannot connect to the docker daemon" in stderr:
        return True
    return (("error during connect" in stderr or "dial unix" in stderr)
            and ("connection refused" in stderr or "no such file or directory" in stderr))


class DockerCommandError(RuntimeError):
    def __init__(self, command, exit_code, stderr):
        super().__init__("Docker read failed (exit " + str(exit_code)
                         + "): " + " ".join(command))
        self.transient_connection = startup_connection_failure(stderr)


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def validate_ids(value):
    if not isinstance(value, list) or any(
            not isinstance(item, str) or not ID_PATTERN.fullmatch(item)
            for item in value):
        raise ValueError("Docker resource IDs must be full 64-character hex strings")
    if len(value) != len(set(value)):
        raise ValueError("Docker resource IDs must be distinct")
    return sorted(value)


def validate_engine(value):
    if not isinstance(value, dict) or set(value) != {"id", "platform"}:
        raise ValueError("Docker engine identity must contain id and platform")
    identity = value["id"]
    if (not isinstance(identity, str) or not identity or len(identity) > 256
            or any(ord(char) < 33 or ord(char) == 127 for char in identity)):
        raise ValueError("Docker engine ID must be a nonempty printable identity")
    if value["platform"] not in ("linux/amd64", "linux/arm64"):
        raise ValueError("Docker engine must be native Linux AMD64 or ARM64")
    return dict(value)


def validate_baseline(value):
    if (not isinstance(value, dict) or type(value.get("schema")) is not int
            or value["schema"] != 1 or value.get("action") != "snapshot"
            or value.get("label") != LABEL or value.get("passed") is not True):
        raise ValueError("Cleanup baseline is not a successful supported snapshot")
    engine = validate_engine(value.get("engine"))
    resources = value.get("resources")
    if not isinstance(resources, dict) or set(resources) != set(RESOURCE_KINDS):
        raise ValueError("Cleanup baseline requires container and network ID lists")
    return engine, {kind: validate_ids(resources[kind]) for kind in RESOURCE_KINDS}


class DockerReader:
    """Only these fixed read-only Docker commands are issued."""

    def __init__(self, run=None, monotonic=None, command_timeout=10):
        self.run = run or subprocess.run
        self.monotonic = monotonic or time.monotonic
        self.command_timeout = command_timeout
        self.commands = []

    def capture(self, command, deadline=None):
        timeout = self.command_timeout
        if deadline is not None:
            timeout = min(timeout, deadline - self.monotonic())
        if timeout <= 0:
            raise TimeoutError("Cleanup observation deadline expired")
        record = {"argv": list(command), "timeout_seconds": timeout,
                  "exit_code": None}
        self.commands.append(record)
        started = self.monotonic()
        try:
            result = self.run(command, stdin=subprocess.DEVNULL, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout, check=False)
            record["exit_code"] = result.returncode
            stderr = command_stderr(result.stderr)
            record["stderr"] = stderr[:STDERR_LIMIT]
            record["stderr_truncated"] = len(stderr) > STDERR_LIMIT
        except subprocess.TimeoutExpired as error:
            stderr = command_stderr(error.stderr)
            record["stderr"] = stderr[:STDERR_LIMIT]
            record["stderr_truncated"] = len(stderr) > STDERR_LIMIT
            record["error_type"] = "TimeoutExpired"
            raise TimeoutError("Docker cleanup inspection timed out") from error
        except OSError as error:
            record["error_type"] = type(error).__name__
            record["error"] = str(error)
            raise
        finally:
            record["seconds"] = round(self.monotonic() - started, 3)
        if deadline is not None and self.monotonic() >= deadline:
            record["error_type"] = "ObservationDeadlineExpired"
            raise TimeoutError("Cleanup observation deadline expired")
        if result.returncode:
            record["error_type"] = "DockerCommandError"
            raise DockerCommandError(command, result.returncode, stderr)
        return result.stdout

    def engine(self, deadline=None):
        info = json.loads(self.capture(ENGINE_COMMAND, deadline))
        if not isinstance(info, dict) or info.get("OSType") != "linux":
            raise ValueError("Docker engine info must describe a Linux engine")
        raw_architecture = info.get("Architecture")
        if not isinstance(raw_architecture, str):
            raise ValueError("Docker engine architecture must be a string")
        architecture = {"amd64": "amd64", "x86_64": "amd64",
                        "arm64": "arm64", "aarch64": "arm64"}.get(
                            raw_architecture)
        return validate_engine({"id": info.get("ID"),
                                "platform": "linux/" + (architecture or "unknown")})

    def resources(self, deadline=None):
        return {kind: validate_ids(self.capture(RESOURCE_COMMANDS[kind], deadline)
                                  .splitlines())
                for kind in RESOURCE_KINDS}


def new_report(action):
    report = {"schema": 1, "action": action, "label": LABEL,
              "started_at": timestamp(), "passed": False, "commands": []}
    revision = os.environ.get("GITHUB_SHA")
    if revision and re.fullmatch(r"[0-9a-fA-F]{40}", revision):
        report["declared_github_revision"] = revision.lower()
    return report


def write_report(path, report):
    report["finished_at"] = timestamp()
    # Build JSON before opening the destination so serialization failure cannot
    # leave an empty result. Exclusive creation preserves earlier evidence.
    content = json.dumps(report, indent=2) + "\n"
    with path.open("x", encoding="utf-8") as output:
        output.write(content)


def prepare_engine(reader, report, readiness_seconds, sleep):
    """Observe startup before taking the one authoritative resource baseline."""
    readiness = {"budget_seconds": readiness_seconds, "passed": False,
                 "attempts": []}
    report["readiness"] = readiness
    deadline = reader.monotonic() + readiness_seconds
    last_error = None
    while True:
        if reader.monotonic() >= deadline:
            raise TimeoutError("Docker baseline readiness deadline expired: "
                               + str(last_error)) from last_error
        started = reader.monotonic()
        attempt = {"number": len(readiness["attempts"]) + 1,
                   "started_at": timestamp(), "passed": False,
                   "command_begin": len(reader.commands)}
        readiness["attempts"].append(attempt)
        try:
            engine = reader.engine(deadline)
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            last_error = error
            attempt["error_type"] = type(error).__name__
            attempt["error"] = str(error)
            retry = (isinstance(error, TimeoutError)
                     or isinstance(error, DockerCommandError)
                     and error.transient_connection)
            attempt["retryable"] = retry
            if not retry:
                raise
        else:
            attempt["passed"] = True
            attempt["engine"] = engine
            readiness["engine"] = engine
            readiness["passed"] = True
            return engine
        finally:
            attempt["finished_at"] = timestamp()
            attempt["seconds"] = round(reader.monotonic() - started, 3)
            attempt["command_end"] = len(reader.commands)
        available = deadline - reader.monotonic()
        if available <= 0:
            raise TimeoutError("Docker baseline readiness deadline expired: "
                               + str(last_error)) from last_error
        sleep(min(1, available))


def snapshot(directory, reader=None, readiness_seconds=READINESS_SECONDS, sleep=None):
    if not math.isfinite(readiness_seconds) or not 0 < readiness_seconds <= 60:
        raise ValueError("Docker readiness must be greater than zero and at most 60 seconds")
    reader = reader or DockerReader()
    sleep = sleep or time.sleep
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    before = directory / "before.json"
    if before.exists() or (directory / "after.json").exists():
        raise FileExistsError("Cleanup results already exist; choose a new directory")
    report = new_report("snapshot")
    try:
        ready_engine = prepare_engine(reader, report, readiness_seconds, sleep)
        report["engine"] = reader.engine()
        if report["engine"] != ready_engine:
            raise RuntimeError("Docker engine changed after baseline readiness")
        report["resources"] = reader.resources()
        if reader.engine() != report["engine"]:
            raise RuntimeError("Docker engine changed during cleanup snapshot")
        report["passed"] = True
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        report["error"] = str(error)
    report["commands"] = reader.commands
    write_report(before, report)
    return report


def check(directory, grace_seconds=30, reader=None, monotonic=None, sleep=None):
    if not math.isfinite(grace_seconds) or not 0 < grace_seconds <= 60:
        raise ValueError("Cleanup grace must be greater than zero and at most 60 seconds")
    reader = reader or DockerReader(monotonic=monotonic)
    monotonic = monotonic or reader.monotonic
    sleep = sleep or time.sleep
    directory = Path(directory)
    after = directory / "after.json"
    if after.exists():
        raise FileExistsError("Cleanup result already exists; it cannot be overwritten")
    report = new_report("check")
    report["grace_seconds"] = grace_seconds
    deadline = monotonic() + grace_seconds
    try:
        baseline = json.loads((directory / "before.json").read_text(encoding="utf-8"))
        engine, resources = validate_baseline(baseline)
        report["engine"] = engine
        report["baseline_resources"] = resources
        report["observations"] = 0
        while True:
            if reader.engine(deadline) != engine:
                raise RuntimeError("Docker engine differs from the cleanup snapshot")
            current = reader.resources(deadline)
            remaining = {kind: sorted(set(current[kind]) - set(resources[kind]))
                         for kind in RESOURCE_KINDS}
            report["observations"] += 1
            report["resources"] = current
            report["new_resources"] = remaining
            # Revalidate identity after both listings. An engine switch between
            # queries must not turn missing resources into a cleanup PASS.
            if reader.engine(deadline) != engine:
                raise RuntimeError("Docker engine changed during cleanup observation")
            if not any(remaining.values()):
                report["passed"] = True
                break
            available = deadline - monotonic()
            if available <= 0:
                raise TimeoutError("New Testcontainers resources remain after cleanup grace")
            sleep(min(1, available))
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        report["error"] = str(error)
    report["commands"] = reader.commands
    write_report(after, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("snapshot", "check"):
        child = subparsers.add_parser(action)
        child.add_argument("--directory", type=Path, required=True)
        if action == "check":
            child.add_argument("--grace-seconds", type=float, default=30)
        else:
            child.add_argument("--readiness-seconds", type=float,
                               default=READINESS_SECONDS)
    args = parser.parse_args(argv)
    try:
        report = snapshot(args.directory, readiness_seconds=args.readiness_seconds) \
            if args.action == "snapshot" else check(args.directory, args.grace_seconds)
    except (OSError, ValueError) as error:
        print("Cleanup inspection FAIL: " + str(error), file=sys.stderr)
        return 1
    message = "Cleanup " + args.action + ": " + ("PASS" if report["passed"] else "FAIL")
    if not report["passed"]:
        message += "; " + report.get("error", "inspection failed")
    print(message, flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
