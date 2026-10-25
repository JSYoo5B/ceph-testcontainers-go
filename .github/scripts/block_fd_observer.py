"""Access-flag aware /proc observer for descriptors that refer to a block device.

Research helper for issue #7 (attachment-only baseline). It is pure Python with
an injectable host adapter, so every rule below is exercised without a Linux
block device, Docker or Ceph. It is NOT wired into the scratch probe and it does
not establish native attachment, workload exposure or exclusivity.

Why flags: a matching block device number is not proof that a process opened the
device for I/O. An O_PATH descriptor refers to the same inode but gets empty file
operations in the kernel and never reaches the device's open callback. Ceph
BlueStore's KernelDevice opens read/write direct and buffered descriptors
(src/blk/kernel/KernelDevice.cc, pinned Tentacle commit 7f793731), so only
read/write descriptors with and without O_DIRECT can admit an OSD attachment.

Rules kept here:
  * flags come from /proc/<pid>/fdinfo/<fd> ("flags:" line, octal); missing,
    duplicated or malformed values stay unknown, never an empty or zero value;
  * the O_DIRECT bit is architecture specific; an unknown machine leaves the
    direct/buffered split unknown;
  * an fd whose identity or flags read raced with a close or reuse is recorded as
    a race and leaves the snapshot unverified instead of being dropped;
  * the O_EXCL bit is cleared by the kernel after open, so it is not an oracle;
  * the verdict never inspects locks and never touches the descriptors.
"""
import errno
import os
import stat

O_ACCMODE = 0o3
O_PATH = 0o10000000
# O_DIRECT differs by architecture (asm/fcntl.h); other machines stay unknown.
O_DIRECT_BY_MACHINE = {
    "x86_64": 0o40000,
    "i686": 0o40000,
    "aarch64": 0o200000,
    "arm64": 0o200000,
}

PATH_ONLY = "path-only"
UNKNOWN_FLAGS = "unknown-flags"
READ_ONLY = "read-only"
WRITE_ONLY = "write-only"
READ_WRITE_DIRECT = "read-write-direct"
READ_WRITE_BUFFERED = "read-write-buffered"
READ_WRITE_UNKNOWN_DIRECT = "read-write-direct-unknown"
IO_CLASSES = {READ_ONLY, WRITE_ONLY, READ_WRITE_DIRECT, READ_WRITE_BUFFERED,
              READ_WRITE_UNKNOWN_DIRECT}


def parse_fdinfo_flags(text):
    """Return the octal flags integer from fdinfo text, or None when unknown."""
    if not isinstance(text, str):
        return None
    values = []
    for line in text.splitlines():
        if line.startswith("flags:"):
            values.append(line[len("flags:"):].strip())
    if len(values) != 1:
        return None
    value = values[0]
    if not value or any(ch not in "01234567" for ch in value):
        return None
    return int(value, 8)


def classify_flags(flags, machine):
    """Classify one descriptor's open flags (None means unreadable)."""
    if not isinstance(flags, int) or isinstance(flags, bool) or flags < 0:
        return UNKNOWN_FLAGS
    if flags & O_PATH:
        return PATH_ONLY
    access = flags & O_ACCMODE
    if access == 0:
        return READ_ONLY
    if access == 1:
        return WRITE_ONLY
    if access != 2:
        return UNKNOWN_FLAGS
    direct = O_DIRECT_BY_MACHINE.get(machine)
    if direct is None:
        return READ_WRITE_UNKNOWN_DIRECT
    return READ_WRITE_DIRECT if flags & direct else READ_WRITE_BUFFERED


class ProcHost:
    """Reads a /proc tree; proc_root is injectable for Docker-free controls."""

    def __init__(self, proc_root="/proc"):
        self.proc_root = proc_root

    def listdir(self, path):
        return os.listdir(self.proc_root + path)

    def stat(self, path):
        """Return (is_block, 'major:minor', inode) for a followed descriptor link."""
        info = os.stat(self.proc_root + path)
        return (stat.S_ISBLK(info.st_mode),
                "%d:%d" % (os.major(info.st_rdev), os.minor(info.st_rdev)),
                info.st_ino)

    def read_text(self, path):
        with open(self.proc_root + path, encoding="ascii", errors="replace") as handle:
            return handle.read()

    def getpid(self):
        return os.getpid()


def _gone(exc):
    return exc.errno in (errno.ENOENT, errno.ESRCH)


def observe(host, major_minor, machine):
    """Return an immutable-style snapshot of descriptors on one block device.

    The snapshot is complete only when every listing and read succeeded. Vanished
    processes and descriptors are recorded as races, not as absence.
    """
    snapshot = {
        "device": major_minor,
        "machine": machine,
        "observer_pid": host.getpid(),
        "descriptors": [],
        "errors": [],
        "races": [],
    }
    try:
        entries = host.listdir("")
    except OSError as exc:
        snapshot["errors"].append({"where": "proc", "errno": exc.errno})
        return _finish(snapshot)
    for entry in sorted(e for e in entries if e.isdigit()):
        pid = int(entry)
        base = "/%s/fd" % entry
        try:
            fds = host.listdir(base)
        except OSError as exc:
            if _gone(exc):
                snapshot["races"].append({"pid": pid, "fd": None, "why": "process-vanished"})
            else:
                snapshot["errors"].append({"pid": pid, "where": "fd-list", "errno": exc.errno})
            continue
        for fd in sorted(fds):
            _observe_fd(host, snapshot, major_minor, machine, pid, fd)
    return _finish(snapshot)


def _observe_fd(host, snapshot, major_minor, machine, pid, fd):
    link = "/%d/fd/%s" % (pid, fd)
    try:
        before = host.stat(link)
    except OSError as exc:
        if _gone(exc):
            snapshot["races"].append({"pid": pid, "fd": fd, "why": "fd-vanished"})
        else:
            snapshot["errors"].append({"pid": pid, "fd": fd, "where": "stat", "errno": exc.errno})
        return
    if not before[0] or before[1] != major_minor:
        return
    try:
        text = host.read_text("/%d/fdinfo/%s" % (pid, fd))
    except OSError as exc:
        if _gone(exc):
            snapshot["races"].append({"pid": pid, "fd": fd, "why": "fd-vanished"})
            return
        text = None
    try:
        after = host.stat(link)
    except OSError as exc:
        why = "fd-vanished" if _gone(exc) else "stat-error"
        snapshot["races"].append({"pid": pid, "fd": fd, "why": why})
        return
    if after != before:
        snapshot["races"].append({"pid": pid, "fd": fd, "why": "identity-changed"})
        return
    flags = parse_fdinfo_flags(text)
    snapshot["descriptors"].append({
        "pid": pid, "fd": fd, "inode": before[2],
        "flags": flags, "class": classify_flags(flags, machine),
    })


def _finish(snapshot):
    snapshot["complete"] = not snapshot["errors"]
    snapshot["coherent"] = snapshot["complete"] and not snapshot["races"]
    return snapshot


def attachment_verdict(snapshot, pid):
    """Decide whether `pid` holds I/O descriptors on the observed device.

    Returns one of: unverified, not-attached, path-only-reference, no-io-pair,
    foreign-opener, attached-io. 'attached-io' means only that, at this instant,
    the process held read/write descriptors with and without O_DIRECT on the
    device number. It says nothing about exclusivity, workload I/O or teardown.
    """
    if not isinstance(snapshot, dict) or snapshot.get("coherent") is not True:
        return "unverified"
    if snapshot.get("machine") not in O_DIRECT_BY_MACHINE:
        return "unverified"
    own = [d for d in snapshot["descriptors"] if d["pid"] == pid]
    others = [d for d in snapshot["descriptors"]
              if d["pid"] != pid and d["pid"] != snapshot["observer_pid"]]
    if any(d["class"] == UNKNOWN_FLAGS for d in own + others):
        return "unverified"
    if not own:
        return "not-attached"
    classes = {d["class"] for d in own}
    if classes == {PATH_ONLY}:
        return "path-only-reference"
    if READ_WRITE_DIRECT not in classes or READ_WRITE_BUFFERED not in classes:
        return "no-io-pair"
    if any(d["class"] != PATH_ONLY for d in others):
        return "foreign-opener"
    return "attached-io"
