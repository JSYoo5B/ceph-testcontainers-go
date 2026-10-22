#!/usr/bin/env python3
"""Probe delay and I/O-error tables on a test-owned scratch device chain.

The chain is a sparse file created by this probe, a loop device bound to that
file and one device-mapper mapping with a unique name and UUID. No host disk is
discovered, formatted or reconfigured, and nothing is installed. The probe never
starts Ceph: it is the scratch-device step that must precede any BlueStore use.

Run it as root on a disposable Linux host only:

  block_fault_probe.py run --directory DIR --output REPORT.json

Every command has a timeout. A timed-out control command does not prove that
the kernel transition happened or rolled back, so the probe then observes the
exact owned mapping again, records the state and stops changing it. Cleanup
removes a resource only after observing that nothing still uses it, and keeps
the remaining resources otherwise. A missing tool, privilege or target makes
the host ineligible; it is reported, not hidden as a failure.
"""

import argparse
import datetime
import errno as errno_codes
import hashlib
import json
import mmap
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid


SCHEMA = 1
SECTOR = 512
SIZE_BYTES = 64 * 1024 * 1024
# Offsets and lengths are 4 KiB aligned, which satisfies O_DIRECT on 512-byte
# and 4 KiB logical blocks.
IO_LENGTH = 64 * 1024
# Faults cover only a window in the middle of the device. On the first hosted
# run a whole-device delay also slowed the host's udev probe of the resumed
# mapping, which kept it open and timed out dmsetup's udev wait. Signature
# probes read near the start and end of a device, not here.
WINDOW_START = 32 * 1024 * 1024
WINDOW_LENGTH = 1024 * 1024
IO_OFFSET = WINDOW_START
CONTROL_TIMEOUT = 15
KILL_GRACE = 5
NODE_WAIT = 5
OPEN_WAIT = 10
OUTPUT_LIMIT = 4096
# Distinct values let an elapsed time identify the class that delayed it.
DELAYS_MS = {"read": 300, "write": 600, "flush": 900}
DELAY_FRACTION = 0.8
NAME_PREFIX = "ctc-blockfault-"
UUID_PREFIX = "CTC-BLOCKFAULT-"
LOSETUP_COLUMNS = "NAME,BACK-FILE,BACK-INO,BACK-MAJ:MIN,MAJ:MIN,OFFSET,SIZELIMIT,AUTOCLEAR"
REQUIRED_TARGETS = ("linear", "delay", "error")
OPTIONAL_TARGETS = ("flakey",)
TARGET_MODULES = {"delay": "dm_delay", "flakey": "dm_flakey"}
BASELINE_SEED = "baseline"
MUTATION_SEED = "mutation"
EXIT_CODES = {"complete": 0, "ineligible": 0, "failed": 1, "uncertain": 2}


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def clip(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    value = value or ""
    return value if len(value) <= OUTPUT_LIMIT else value[:OUTPUT_LIMIT] + "...[truncated]"


def pattern(seed, length):
    return hashlib.shake_256(seed.encode()).digest(length)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


class Result:
    def __init__(self, argv, rc=None, stdout="", stderr="", timed_out=False,
                 reaped=True, elapsed_s=0.0, error=None):
        self.argv = list(argv)
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.reaped = reaped
        self.elapsed_s = elapsed_s
        self.error = error

    @property
    def ok(self):
        return self.rc == 0 and not self.timed_out and self.error is None

    def record(self, name):
        entry = {"name": name, "argv": self.argv, "rc": self.rc,
                 "timed_out": self.timed_out, "elapsed_s": round(self.elapsed_s, 4),
                 "stdout": clip(self.stdout), "stderr": clip(self.stderr)}
        if not self.reaped:
            entry["reaped"] = False
        if self.error:
            entry["error"] = self.error
        return entry


class SubprocessRunner:
    """Runs a command with a timeout and does not wait forever for a killed child."""

    def run(self, argv, timeout):
        start = time.monotonic()
        try:
            proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            return Result(argv, error=type(exc).__name__ + ": " + str(exc))
        try:
            out, err = proc.communicate(timeout=timeout)
            return Result(argv, proc.returncode, out, err, elapsed_s=time.monotonic() - start)
        except subprocess.TimeoutExpired:
            proc.kill()
            reaped = True
            try:
                out, err = proc.communicate(timeout=KILL_GRACE)
            except subprocess.TimeoutExpired:
                # A child in uninterruptible I/O can outlive the kill.
                out, err, reaped = "", "", False
            return Result(argv, None, out, err, timed_out=True, reaped=reaped,
                          elapsed_s=time.monotonic() - start)


class Host:
    """Local filesystem and process facts; replaced by a fake in tests."""

    def which(self, name):
        return shutil.which(name)

    def euid(self):
        return os.geteuid()

    def platform(self):
        return sys.platform

    def uname(self):
        info = os.uname()
        return info.release, info.machine

    def make_workdir(self, parent):
        return tempfile.mkdtemp(prefix=NAME_PREFIX, dir=parent)

    def create_sparse(self, path, size):
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.ftruncate(fd, size)
            return file_identity(os.fstat(fd))
        finally:
            os.close(fd)

    def file_identity(self, path):
        try:
            return file_identity(os.stat(path))
        except FileNotFoundError:
            return None

    def block_rdev(self, path):
        try:
            info = os.stat(path)
        except FileNotFoundError:
            return None
        if not stat.S_ISBLK(info.st_mode):
            return "not-block"
        return "%d:%d" % (os.major(info.st_rdev), os.minor(info.st_rdev))

    def read_text(self, path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return None

    def write_text(self, path, text):
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(text)

    def openers(self, major_minor):
        """Processes with an open descriptor on the block device, from /proc."""
        found = []
        for pid in os.listdir("/proc"):
            if not pid.isdigit() or int(pid) == os.getpid():
                continue
            try:
                fds = os.listdir("/proc/%s/fd" % pid)
            except OSError:
                continue
            for fd in fds:
                try:
                    info = os.stat("/proc/%s/fd/%s" % (pid, fd))
                except OSError:
                    continue
                if stat.S_ISBLK(info.st_mode) and "%d:%d" % (
                        os.major(info.st_rdev), os.minor(info.st_rdev)) == major_minor:
                    found.append({"pid": int(pid), "comm": self.read_text("/proc/%s/comm" % pid)})
                    break
        return found

    def remove_file(self, path):
        os.unlink(path)

    def remove_dir(self, path):
        os.rmdir(path)

    def monotonic(self):
        return time.monotonic()

    def sleep(self, seconds):
        time.sleep(seconds)


def file_identity(info):
    return {"dev": "%d:%d" % (os.major(info.st_dev), os.minor(info.st_dev)),
            "ino": info.st_ino, "size": info.st_size}


# Parsers


def parse_targets(text):
    """Parses `dmsetup targets`: one `name vMAJ.MIN.PATCH` per line."""
    targets = {}
    for line in (text or "").splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].startswith("v"):
            targets[fields[0]] = fields[1][1:]
    return targets


def parse_dm_info(text):
    """Parses the multi-line `dmsetup info NAME` output."""
    fields = {}
    for line in (text or "").splitlines():
        key, sep, value = line.partition(":")
        if sep:
            fields[key.strip()] = value.strip()
    if "State" not in fields or "Major, minor" not in fields:
        return None
    major, _, minor = fields["Major, minor"].partition(",")
    tables = fields.get("Tables present", "")
    try:
        open_count = int(fields.get("Open count", ""))
    except ValueError:
        open_count = None
    return {
        "name": fields.get("Name"),
        "uuid": fields.get("UUID"),
        "state": fields["State"],
        "suspended": fields["State"] == "SUSPENDED",
        "live": "LIVE" in tables,
        "inactive": "INACTIVE" in tables,
        "open_count": open_count,
        "major_minor": "%s:%s" % (major.strip(), minor.strip()),
    }


def parse_dm_names(text):
    """Parses `dmsetup info -c --noheadings --separator | -o name,uuid`."""
    devices = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line == "No devices found":
            continue
        name, _, dm_uuid = line.partition("|")
        devices.append({"name": name, "uuid": dm_uuid})
    return devices


def parse_losetup(text):
    """Parses `losetup --json --list`; util-linux versions differ in value types."""
    if not (text or "").strip():
        return []
    data = json.loads(text)
    devices = []
    for entry in data.get("loopdevices", []):
        back = entry.get("back-file")
        if isinstance(back, str) and back.endswith(" (deleted)"):
            back = back[:-len(" (deleted)")]
        devices.append({
            "name": entry.get("name"),
            "back_file": back,
            "back_ino": None if entry.get("back-ino") is None else int(entry["back-ino"]),
            "back_dev": normalize_majmin(entry.get("back-maj:min")),
            "dev": normalize_majmin(entry.get("maj:min")),
            "offset": int(entry.get("offset") or 0),
            "sizelimit": int(entry.get("sizelimit") or 0),
        })
    return devices


def normalize_majmin(value):
    if value is None:
        return None
    major, _, minor = str(value).strip().partition(":")
    try:
        return "%d:%d" % (int(major), int(minor))
    except ValueError:
        return None


def normalize_table(text):
    """One line per target, single spaces, as `dmsetup table` prints it."""
    lines = (" ".join(line.split()) for line in (text or "").splitlines())
    return "\n".join(line for line in lines if line)


# Decisions


def classify_delay(elapsed_s, delay_ms, baseline_s):
    """A delay counts only when it exceeds a fast baseline by most of its value."""
    if elapsed_s is None or baseline_s is None:
        return "unverified"
    delay_s = delay_ms / 1000.0
    if baseline_s >= delay_s * (1 - DELAY_FRACTION):
        return "unverified"
    return "observed" if elapsed_s >= delay_s * DELAY_FRACTION else "not-observed"


def verdict(report):
    # Uncertain state outranks ineligibility found after resources existed.
    if report["uncertain"]:
        return "uncertain"
    if not report["eligibility"]["eligible"]:
        return "ineligible"
    if report["failures"]:
        return "failed"
    return "complete"


def associations(devices, identity):
    """Loop associations whose backing inode is the owned file."""
    return [d for d in devices
            if d["back_ino"] == identity["ino"] and d["back_dev"] == identity["dev"]]


class Probe:
    def __init__(self, runner, host, directory, allow_target_autoload=False,
                 script=None, python=None):
        self.runner = runner
        self.host = host
        self.directory = directory
        self.allow_target_autoload = allow_target_autoload
        self.script = script or os.path.abspath(__file__)
        self.python = python or sys.executable
        self.workdir = None
        self.backing = None
        self.identity = None
        self.loop = None
        self.loop_dev = None
        self.dm_dev = None
        self.dm_rdev = None
        self.name = NAME_PREFIX + uuid.uuid4().hex[:12]
        self.dm_uuid = UUID_PREFIX + uuid.uuid4().hex
        self.linear = None
        self.table_files = []
        self.baseline_hash = None
        self.baseline_elapsed = None
        # Set once the owned mapping may be suspended, foreign or busy; no
        # further control or destructive I/O is attempted afterwards.
        self.halted = False
        self.io_pending = False
        # Steps whose killed child could not be reaped.
        self.unreaped = []
        # Optional targets that are neither registered nor allowed to load.
        self.unavailable = set()
        self.report = {
            "schema": SCHEMA,
            "tool": "block_fault_probe",
            "started_at": utc_now(),
            "options": {"allow_target_autoload": allow_target_autoload,
                        "size_bytes": SIZE_BYTES, "io_offset": IO_OFFSET,
                        "fault_window": [WINDOW_START, WINDOW_LENGTH],
                        "io_length": IO_LENGTH, "delays_ms": dict(DELAYS_MS),
                        "control_timeout_s": CONTROL_TIMEOUT},
            "environment": {},
            "eligibility": {"eligible": False, "reasons": []},
            "resources": {"dm_name": self.name, "dm_uuid": self.dm_uuid},
            "steps": [],
            "phases": [],
            "cleanup": {},
            "uncertain": [],
            "failures": [],
        }

    # Helpers

    def run(self, name, argv, timeout=CONTROL_TIMEOUT):
        result = self.runner.run(argv, timeout)
        self.report["steps"].append(result.record(name))
        if result.timed_out and not result.reaped:
            # The child may still act on the chain after this observation.
            self.unreaped.append(name)
        return result

    def uncertain(self, reason):
        self.report["uncertain"].append(reason)

    def fail(self, reason):
        self.report["failures"].append(reason)

    def ineligible(self, reason):
        self.report["eligibility"]["eligible"] = False
        self.report["eligibility"]["reasons"].append(reason)

    def dmsetup(self, name, *args):
        return self.run(name, ["dmsetup"] + list(args))

    # Eligibility

    def check_eligibility(self):
        env = self.report["environment"]
        release, machine = self.host.uname()
        env.update({"platform": self.host.platform(), "kernel_release": release,
                    "machine": machine, "euid": self.host.euid()})
        reasons = []
        if env["platform"] != "linux":
            reasons.append("not-linux")
        if env["euid"] != 0:
            # losetup and the device-mapper ioctls need CAP_SYS_ADMIN.
            reasons.append("not-root")
        tools = {}
        for tool in ("losetup", "dmsetup"):
            path = self.host.which(tool)
            tools[tool] = {"found": path is not None}
            if path is None:
                reasons.append("missing-tool:" + tool)
        env["tools"] = tools
        if reasons:
            self.report["eligibility"]["reasons"] = reasons
            return False
        tools["losetup"]["version"] = self.version("losetup-version", ["losetup", "--version"])
        driver = self.dmsetup("dmsetup-version", "version")
        tools["dmsetup"]["version"] = clip(driver.stdout).strip() if driver.ok else None
        if not driver.ok or "Driver version" not in driver.stdout:
            self.report["eligibility"]["reasons"] = ["dm-driver-unavailable"]
            return False
        targets = self.dmsetup("dmsetup-targets", "targets")
        registered = parse_targets(targets.stdout) if targets.ok else None
        env["dm_targets_before"] = registered
        if registered is None:
            self.report["eligibility"]["reasons"] = ["dm-targets-unreadable"]
            return False
        modules = {}
        for target in REQUIRED_TARGETS + OPTIONAL_TARGETS:
            if target in registered:
                continue
            module = TARGET_MODULES.get(target)
            if module is None:
                reasons.append("target-not-registered:" + target)
                continue
            found = self.run("modinfo-" + module, ["modinfo", "-n", module])
            # Read-only lookup; the probe never loads a module itself.
            modules[module] = (found.stdout.strip() != "") if found.ok else False
            if not self.allow_target_autoload:
                reason = "target-not-registered:" + target
            elif not modules[module]:
                reason = "target-module-absent:" + target
            else:
                continue
            if target in REQUIRED_TARGETS:
                reasons.append(reason)
            else:
                self.unavailable.add(target)
                env.setdefault("optional_ineligible", []).append(reason)
        env["target_modules"] = modules
        self.report["eligibility"]["reasons"] = reasons
        return not reasons

    def version(self, name, argv):
        result = self.run(name, argv)
        return clip(result.stdout).strip() if result.ok else None

    # Setup

    def setup(self):
        res = self.report["resources"]
        self.workdir = self.host.make_workdir(self.directory)
        self.backing = os.path.join(self.workdir, "backing.img")
        self.identity = self.host.create_sparse(self.backing, SIZE_BYTES)
        res["backing"] = {"relative_path": os.path.basename(self.workdir) + "/backing.img",
                          "inode": self.identity["ino"], "size_bytes": self.identity["size"]}
        attach = self.run("losetup-attach", ["losetup", "--find", "--show", self.backing])
        if not attach.ok:
            if attach.timed_out:
                # A loop may be bound without its name; cleanup scans for it.
                self.uncertain("loop-attach-timed-out")
            else:
                self.ineligible("loop-attach-refused")
            return False
        self.loop = attach.stdout.strip()
        devices = self.list_loops("losetup-verify")
        owned = [d for d in (devices or []) if d["name"] == self.loop]
        if devices is None or len(owned) != 1 or associations(owned, self.identity) != owned:
            self.uncertain("loop-identity-unverified")
            return False
        if owned[0]["offset"] or owned[0]["sizelimit"]:
            self.uncertain("loop-unexpected-offset")
            return False
        self.loop_dev = owned[0]["dev"]
        res["loop"] = {"name": self.loop, "major_minor": self.loop_dev}
        sectors = SIZE_BYTES // SECTOR
        self.linear = "0 %d linear %s 0" % (sectors, self.loop_dev)
        create = self.dmsetup("dm-create", "create", self.name, "--uuid", self.dm_uuid,
                              "--table", self.linear)
        if create.timed_out:
            self.uncertain("dm-create-timed-out")
            self.halted = True
            self.observe("after-create-timeout")
            return False
        if not create.ok:
            self.fail("dm-create-refused")
            return False
        state = self.observe("after-create")
        if not self.identity_ok(state):
            return False
        self.dm_rdev = state["info"]["major_minor"]
        res["dm"] = {"major_minor": self.dm_rdev}
        self.dm_dev = self.device_node()
        if self.dm_dev is None:
            self.fail("dm-node-missing")
            return False
        res["dm"]["write_cache"] = self.host.read_text(
            "/sys/dev/block/%s/queue/write_cache" % self.dm_rdev)
        return True

    def device_node(self):
        path = "/dev/mapper/" + self.name
        deadline = self.host.monotonic() + NODE_WAIT
        while True:
            rdev = self.host.block_rdev(path)
            if rdev == self.dm_rdev:
                return path
            if rdev is not None or self.host.monotonic() >= deadline:
                # A node with another device number is not ours to use.
                return None
            self.host.sleep(0.2)

    def list_loops(self, name, *selector):
        result = self.run(name, ["losetup", "--json", "--list", "--output", LOSETUP_COLUMNS]
                          + list(selector))
        if not result.ok:
            return None
        try:
            return parse_losetup(result.stdout)
        except (ValueError, TypeError, AttributeError):
            return None

    # Observation

    def observe(self, label):
        """Fresh observation of the exact owned mapping."""
        info = self.dmsetup("dm-info:" + label, "info", self.name)
        live = self.dmsetup("dm-table:" + label, "table", self.name)
        inactive = self.dmsetup("dm-table-inactive:" + label, "table", "--inactive", self.name)
        parsed = parse_dm_info(info.stdout) if info.ok else None
        state = {"label": label, "complete": parsed is not None and live.ok and inactive.ok,
                 "info": parsed,
                 "live_table": normalize_table(live.stdout) if live.ok else None,
                 "inactive_table": normalize_table(inactive.stdout) if inactive.ok else None}
        if parsed and parsed["open_count"]:
            # Read-only: which processes hold the mapping open.
            state["openers"] = self.host.openers(parsed["major_minor"])
        return state

    def identity_ok(self, state):
        info = state["info"]
        if not state["complete"]:
            self.uncertain("observation-incomplete:" + state["label"])
            self.halted = True
            return False
        if info["uuid"] != self.dm_uuid or (self.dm_rdev and info["major_minor"] != self.dm_rdev):
            self.uncertain("mapping-identity-changed:" + state["label"])
            self.halted = True
            return False
        return True

    # Direct I/O

    def io(self, op, seed=BASELINE_SEED, timeout=None):
        argv = [self.python, "-I", self.script, "io", "--device", self.dm_dev,
                "--rdev", self.dm_rdev, "--op", op, "--offset", str(IO_OFFSET),
                "--length", str(IO_LENGTH), "--seed", seed]
        limit = timeout or (max(DELAYS_MS.values()) / 1000.0 + 20)
        result = self.run("io-" + op, argv, timeout=limit)
        if result.timed_out or not result.ok:
            # The bio may still be queued or have completed unobserved.
            self.io_pending = self.io_pending or result.timed_out or not result.reaped
            self.uncertain("io-%s-%s" % (op, "timed-out" if result.timed_out else "helper-failed"))
            self.halted = True
            return None
        try:
            return json.loads(result.stdout)
        except ValueError:
            self.uncertain("io-%s-unparsable" % op)
            self.halted = True
            return None

    # Table transitions

    def install(self, label, table):
        """Load, suspend and resume, observing each transition separately."""
        record = {"requested_table": table, "transitions": []}
        # A multi-target table is passed as a file in the owned work directory.
        path = os.path.join(self.workdir, "table-%s.txt" % label)
        self.host.write_text(path, table + "\n")
        self.table_files.append(path)
        for step, args in (("load", ("load", self.name, path)),
                           ("suspend", ("suspend", self.name)),
                           ("resume", ("resume", self.name))):
            result = self.dmsetup("dm-%s:%s" % (step, label), *args)
            state = self.observe("%s-%s" % (label, step))
            entry = {"step": step, "rc": result.rc, "timed_out": result.timed_out,
                     "observed": state}
            record["transitions"].append(entry)
            if result.timed_out:
                # The transition may or may not have happened; stay put.
                self.uncertain("dm-%s-timed-out:%s" % (step, label))
                self.halted = True
                record["outcome"] = "uncertain"
                return record
            if not self.identity_ok(state):
                record["outcome"] = "uncertain"
                return record
            if not result.ok:
                record["outcome"] = "rejected"
                if step == "load" and not state["info"]["inactive"] and not state["info"]["suspended"]:
                    return record
                self.uncertain("dm-%s-refused-with-state:%s" % (step, label))
                self.halted = True
                return record
        final = record["transitions"][-1]["observed"]
        record["installed_table"] = final["live_table"]
        if (final["info"]["suspended"] or final["info"]["inactive"]
                or final["live_table"] != normalize_table(table)):
            record["outcome"] = "mismatch"
            self.uncertain("installed-state-differs:" + label)
            self.halted = True
        else:
            record["outcome"] = "installed"
        return record

    def restore(self, label):
        record = self.install("restore-" + label, self.linear)
        if record["outcome"] == "installed":
            readback = self.io("read")
            record["readback"] = readback
            if readback is not None:
                if readback.get("errno") is not None or readback.get("sha256") != self.baseline_hash:
                    record["outcome"] = "restored-data-differs"
                    self.fail("restore-readback-differs:" + label)
                else:
                    record["outcome"] = "restored"
        elif record["outcome"] == "rejected":
            self.uncertain("restore-rejected:" + label)
            self.halted = True
        return record

    # Phases

    def baseline(self):
        write = self.io("write")
        flush = self.io("flush") if write is not None else None
        read = self.io("read") if flush is not None else None
        self.baseline_hash = sha256(pattern(BASELINE_SEED, IO_LENGTH))
        phase = {"name": "baseline", "table": self.linear,
                 "observations": [x for x in (write, flush, read) if x is not None]}
        self.report["phases"].append(phase)
        if read is None:
            return False
        if any(x.get("errno") is not None for x in (write, flush, read)):
            phase["outcome"] = "io-error"
            self.fail("baseline-io-error")
            return False
        if read.get("sha256") != self.baseline_hash:
            phase["outcome"] = "readback-differs"
            self.fail("baseline-readback-differs")
            return False
        phase["outcome"] = "exact"
        self.baseline_elapsed = {"read": read["elapsed_s"], "write": write["elapsed_s"],
                                 "flush": flush["elapsed_s"]}
        return True

    def windowed(self, target):
        """Linear mapping around one fault target covering the window."""
        dev = self.loop_dev
        start, length = WINDOW_START // SECTOR, WINDOW_LENGTH // SECTOR
        end = start + length
        rest = SIZE_BYTES // SECTOR - end
        return "\n".join(("0 %d linear %s 0" % (start, dev),
                          "%d %d %s" % (start, length, target % {"dev": dev, "off": start}),
                          "%d %d linear %s %d" % (end, rest, dev, end)))

    def phase_specs(self):
        delay = self.windowed("delay %%(dev)s %%(off)d %d %%(dev)s %%(off)d %d %%(dev)s %%(off)d %d" % (
            DELAYS_MS["read"], DELAYS_MS["write"], DELAYS_MS["flush"]))
        # up_interval 0 keeps flakey down for the whole window, so the table
        # load time does not decide whether an operation hit the down phase.
        flakey = "flakey %(dev)s %(off)d 0 3600 1 "
        return [
            {"name": "delay", "required": True, "target": "delay", "table": delay,
             # The write repeats the baseline bytes, so its outcome cannot
             # change the expected content.
             "ops": [("read", BASELINE_SEED, "delay"), ("write", BASELINE_SEED, "delay"),
                     ("flush", BASELINE_SEED, "delay")]},
            {"name": "error", "required": True, "target": "error", "table": self.windowed("error"),
             "ops": [("read", BASELINE_SEED, "eio"), ("write", MUTATION_SEED, "eio")]},
            {"name": "flakey-error-writes", "required": False, "target": "flakey",
             "table": self.windowed(flakey + "error_writes"),
             "ops": [("read", BASELINE_SEED, "exact"), ("write", MUTATION_SEED, "eio")]},
            {"name": "flakey-error-reads", "required": False, "target": "flakey",
             "table": self.windowed(flakey + "error_reads"),
             "ops": [("read", BASELINE_SEED, "eio"), ("write", BASELINE_SEED, "ok")]},
        ]

    def run_phase(self, spec):
        phase = {"name": spec["name"], "required": spec["required"],
                 "requested_fault": spec["table"]}
        self.report["phases"].append(phase)
        installed = self.install(spec["name"], spec["table"])
        phase["install"] = installed
        if installed["outcome"] == "rejected":
            if spec["required"]:
                self.ineligible("table-rejected:" + spec["name"])
            phase["effect"] = "not-installed"
            return
        if installed["outcome"] != "installed":
            phase["effect"] = "unverified"
            return
        observations = []
        effects = []
        for op, seed, expect in spec["ops"]:
            result = self.io(op, seed)
            if result is None:
                phase["observations"] = observations
                phase["effect"] = "unverified"
                return
            effect = self.effect(op, expect, result)
            result["expected"] = expect
            result["effect"] = effect
            observations.append(result)
            effects.append(effect)
        status = self.dmsetup("dm-status:" + spec["name"], "status", self.name)
        phase["status_after_io"] = normalize_table(status.stdout) if status.ok else None
        phase["observations"] = observations
        if "not-observed" in effects:
            phase["effect"] = "not-observed"
            self.fail("effect-not-observed:" + spec["name"])
        elif "unverified" in effects:
            phase["effect"] = "partly-unverified"
        else:
            phase["effect"] = "observed"
        phase["restore"] = self.restore(spec["name"])

    def effect(self, op, expect, result):
        err = result.get("errno")
        if expect == "eio":
            return "observed" if err == errno_codes.EIO else "not-observed"
        if err is not None:
            return "not-observed"
        if expect == "exact":
            return "observed" if result.get("sha256") == self.baseline_hash else "not-observed"
        if expect == "ok":
            return "observed"
        if op == "read" and result.get("sha256") != self.baseline_hash:
            return "not-observed"
        if op == "flush" and self.report["resources"]["dm"].get("write_cache") != "write back":
            # Without a volatile cache the kernel sends no flush to delay.
            return "unverified"
        return classify_delay(result["elapsed_s"], DELAYS_MS[op], self.baseline_elapsed[op])

    # Cleanup

    def cleanup(self):
        cleanup = self.report["cleanup"]
        retained = []
        cleanup["retained"] = retained
        if self.unreaped:
            # An absence observed now does not stop a live child from creating
            # or binding a resource later, so the whole chain stays in place.
            cleanup["skipped"] = "unreaped:" + ",".join(self.unreaped)
            if self.linear is not None:
                retained.append("dm-mapping")
            retained += ["loop", "backing-file"]
            self.uncertain("cleanup-retained:" + ",".join(retained))
            return
        mapping_gone = True
        if self.loop is not None and self.linear is not None:
            mapping_gone = self.remove_mapping(cleanup)
        if not mapping_gone:
            retained.append("dm-mapping")
        loop_gone = mapping_gone and self.detach_loop(cleanup)
        if self.loop is not None and not loop_gone:
            retained.append("loop")
        if self.backing is not None:
            if self.loop is None or loop_gone:
                self.remove_backing(cleanup, retained)
            else:
                retained.append("backing-file")
        if retained:
            self.uncertain("cleanup-retained:" + ",".join(retained))

    def remove_mapping(self, cleanup):
        entry = {}
        cleanup["mapping"] = entry
        state = self.observe("before-remove")
        entry["before"] = state
        if state["info"] is None and state["complete"] is False and self.mapping_absent(entry):
            entry["observed"] = "absent"
            return True
        if self.halted or self.io_pending:
            entry["skipped"] = "state-uncertain"
            return False
        if not self.identity_ok(state):
            entry["skipped"] = "identity"
            return False
        deadline = self.host.monotonic() + OPEN_WAIT
        while state["info"]["open_count"] and self.host.monotonic() < deadline:
            self.host.sleep(0.5)
            state = self.observe("before-remove-wait")
            if not self.identity_ok(state):
                entry["skipped"] = "identity"
                return False
        entry["before"] = state
        if state["info"]["suspended"] or state["info"]["open_count"]:
            entry["skipped"] = "suspended-or-open"
            return False
        # Explicit, non-forced, non-deferred removal once users are closed.
        result = self.dmsetup("dm-remove", "remove", self.name)
        entry["command"] = {"rc": result.rc, "timed_out": result.timed_out}
        absent = self.mapping_absent(entry)
        entry["observed"] = {True: "absent", False: "present", None: "unknown"}[absent]
        return absent is True

    def mapping_absent(self, entry):
        result = self.dmsetup("dm-scan", "info", "--columns", "--noheadings", "--separator", "|",
                              "--options", "name,uuid")
        if not result.ok:
            entry["scan"] = "failed"
            return None
        devices = parse_dm_names(result.stdout)
        return not any(d["name"] == self.name or d["uuid"] == self.dm_uuid for d in devices)

    def detach_loop(self, cleanup):
        entry = {}
        cleanup["loop"] = entry
        if self.loop is None:
            return True
        devices = self.list_loops("losetup-before-detach")
        if devices is None:
            entry["observed"] = "unknown"
            return False
        owned = associations(devices, self.identity)
        if [d["name"] for d in owned] != [self.loop] or owned[0]["dev"] != self.loop_dev:
            entry["observed"] = "identity-changed"
            return False
        result = self.run("losetup-detach", ["losetup", "--detach", self.loop])
        entry["command"] = {"rc": result.rc, "timed_out": result.timed_out}
        devices = self.list_loops("losetup-after-detach")
        if devices is None:
            entry["observed"] = "unknown"
            return False
        # Association release, not node disappearance, is the oracle.
        remaining = associations(devices, self.identity)
        entry["observed"] = "unbound" if not remaining else "bound"
        return not remaining

    def remove_backing(self, cleanup, retained):
        devices = self.list_loops("losetup-before-unlink")
        if devices is None or associations(devices, self.identity):
            cleanup["backing"] = "scan-failed" if devices is None else "still-associated"
            retained.append("backing-file")
            return
        current = self.host.file_identity(self.backing)
        if current is None or current["ino"] != self.identity["ino"] or current["dev"] != self.identity["dev"]:
            cleanup["backing"] = "identity-changed"
            retained.append("backing-file")
            return
        self.host.remove_file(self.backing)
        for path in self.table_files:
            self.host.remove_file(path)
        self.host.remove_dir(self.workdir)
        cleanup["backing"] = "removed"

    # Entry

    def execute(self):
        try:
            if not self.check_eligibility():
                return self.finish()
            self.report["eligibility"]["eligible"] = True
            if self.setup() and self.baseline():
                for spec in self.phase_specs():
                    if self.halted or self.report["failures"]:
                        break
                    if not self.report["eligibility"]["eligible"]:
                        break
                    if spec["target"] in self.unavailable:
                        self.report["phases"].append(
                            {"name": spec["name"], "required": False, "effect": "skipped-ineligible"})
                        continue
                    self.run_phase(spec)
        finally:
            if self.report["eligibility"]["eligible"] or self.backing is not None:
                targets = self.dmsetup("dmsetup-targets-after", "targets")
                self.report["environment"]["dm_targets_after"] = (
                    parse_targets(targets.stdout) if targets.ok else None)
                self.cleanup()
                # A killed dmsetup can leave its udev cookie semaphore behind.
                cookies = self.dmsetup("dmsetup-udevcookies", "udevcookies")
                self.report["cleanup"]["udev_cookies"] = clip(cookies.stdout).strip() if cookies.ok else None
        return self.finish()

    def finish(self):
        self.report["finished_at"] = utc_now()
        self.report["verdict"] = verdict(self.report)
        return self.report


# The bounded I/O helper runs in a child process so that a stuck bio cannot
# block the probe beyond the child's timeout.


def io_main(args):
    result = {"op": args.op, "offset": args.offset, "length": args.length, "seed": args.seed,
              "errno": None, "errno_name": None}
    flags = {"read": os.O_RDONLY | os.O_DIRECT, "write": os.O_WRONLY | os.O_DIRECT,
             "flush": os.O_WRONLY}[args.op]
    fd = os.open(args.device, flags)
    try:
        info = os.fstat(fd)
        rdev = "%d:%d" % (os.major(info.st_rdev), os.minor(info.st_rdev))
        if not stat.S_ISBLK(info.st_mode) or rdev != args.rdev:
            raise SystemExit("device %s is %s, not %s" % (args.device, rdev, args.rdev))
        buf = mmap.mmap(-1, args.length)
        if args.op == "write":
            data = pattern(args.seed, args.length)
            buf.write(data)
            result["sha256"] = sha256(data)
        start = time.monotonic()
        try:
            if args.op == "read":
                result["bytes"] = os.preadv(fd, [buf], args.offset)
            elif args.op == "write":
                result["bytes"] = os.pwritev(fd, [buf], args.offset)
            else:
                os.fsync(fd)
        except OSError as exc:
            result["errno"] = exc.errno
            result["errno_name"] = errno_codes.errorcode.get(exc.errno)
        result["elapsed_s"] = round(time.monotonic() - start, 4)
        if args.op == "read" and result["errno"] is None:
            result["sha256"] = sha256(buf[:result["bytes"]])
    finally:
        os.close(fd)
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run the probe on a disposable host")
    run.add_argument("--directory", required=True,
                     help="existing directory for the owned work directory")
    run.add_argument("--output", required=True, help="JSON report path")
    run.add_argument("--allow-target-autoload", action="store_true",
                     help="let a table load trigger the kernel's on-demand load of an "
                          "installed dm_delay/dm_flakey module; without it an unregistered "
                          "target is ineligibility")
    io = sub.add_parser("io", help="one bounded direct I/O (internal)")
    io.add_argument("--device", required=True)
    io.add_argument("--rdev", required=True)
    io.add_argument("--op", choices=("read", "write", "flush"), required=True)
    io.add_argument("--offset", type=int, required=True)
    io.add_argument("--length", type=int, required=True)
    io.add_argument("--seed", required=True)
    args = parser.parse_args(argv)
    if args.command == "io":
        return io_main(args)
    probe = Probe(SubprocessRunner(), Host(), args.directory, args.allow_target_autoload)
    report = probe.execute()
    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("verdict: %s" % report["verdict"])
    for reason in report["eligibility"]["reasons"]:
        print("ineligible: " + reason)
    for reason in report["uncertain"]:
        print("uncertain: " + reason)
    for reason in report["failures"]:
        print("failed: " + reason)
    return EXIT_CODES[report["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
