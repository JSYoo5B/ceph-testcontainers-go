"""Identity join between an owned block mapping lease and an observed OSD attachment.

Research helper for issue #7 (attachment-only baseline). Pure Python over plain
dict records, so every rule is exercised without Linux devices, Docker or Ceph.
It is NOT wired into the scratch probe and it performs no observation itself.

What it joins (all of these must agree, field by field, for 'joined'):
  lease    the owned chain recorded before the OSD started: backing file
           (dev, ino, size), loop association (name, dev, backing dev/ino,
           offset, sizelimit), DM mapping (name, uuid, major:minor, normalized
           table, dependency list) and the daemon target (container id,
           container start generation).
  observed fresh records taken after the OSD started: the same DM/loop/file
           observations, the Docker instance record, the attachment snapshot
           from block_fd_observer, the daemon pid and its start identity, and
           optionally the borrow lease record from block_fd_borrow.

Rules kept here:
  * every compared field needs a presence and type check; a missing, null or
    wrongly typed value (on either side) is 'unverified', never an equal empty
    value;
  * ALL identity fields are compared. Matching the DM name or the major:minor
    alone does not join: a reused name, a replaced uuid, a changed table, a
    different dependency or a swapped backing inode each fail the join;
  * a mismatch of a fully present field is a 'conflict', kept apart from
    'unverified' (an incomplete observation); a conflict wins over unverified
    because a present contradiction cannot be repaired by observing more;
  * exactly one loop association may exist for the owned backing identity, and
    the DM dependency must be exactly that loop device;
  * the snapshot device must equal the lease mapping's major:minor and the
    daemon must be 'attached-io' there; a configured path is never an input;
  * the daemon generation (container id, start generation, pid and process
    start identity before and after any borrow) must be one instance;
  * the verdict is a point-in-time attachment join. It says nothing about
    workload exposure, exclusivity, fault effects or teardown.
"""
import importlib.util
from pathlib import Path


def _load(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_obs = _load("block_fd_observer")
_bor = _load("block_fd_borrow")

JOINED = "joined"
UNVERIFIED = "unverified"
CONFLICT = "conflict"

# Field kinds for presence/type checks.
STR = "str"          # non-empty string
INT = "int"          # non-negative int (bool is not an int here)
LIST = "list-of-str"  # list of non-empty strings

_LEASE_SPEC = {
    "backing": {"dev": STR, "ino": INT, "size": INT},
    "loop": {"name": STR, "dev": STR, "back_dev": STR, "back_ino": INT,
             "offset": INT, "sizelimit": INT},
    "dm": {"name": STR, "uuid": STR, "major_minor": STR, "table": STR,
           "dependencies": LIST},
    "daemon": {"container_id": STR, "start_generation": STR},
}


def _valid(kind, value):
    if kind == STR:
        return isinstance(value, str) and bool(value)
    if kind == INT:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if kind == LIST:
        return (isinstance(value, list) and bool(value)
                and all(isinstance(v, str) and v for v in value))
    return False


def _norm(kind, value):
    return sorted(value) if kind == LIST else value


class _Joiner:
    def __init__(self):
        self.checks = []

    def compare(self, name, kind, owned, seen):
        """Record one field comparison: match, mismatch or unknown."""
        if not (_valid(kind, owned) and _valid(kind, seen)):
            state = "unknown"
        elif _norm(kind, owned) == _norm(kind, seen):
            state = "match"
        else:
            state = "mismatch"
        self.checks.append({"field": name, "state": state})

    def unknown(self, name):
        self.checks.append({"field": name, "state": "unknown"})

    def mismatch(self, name):
        self.checks.append({"field": name, "state": "mismatch"})

    def match(self, name):
        self.checks.append({"field": name, "state": "match"})

    def verdict(self):
        states = {c["state"] for c in self.checks}
        if "mismatch" in states:
            return CONFLICT
        if "unknown" in states:
            return UNVERIFIED
        return JOINED


def _section(record, name):
    value = record.get(name) if isinstance(record, dict) else None
    return value if isinstance(value, dict) else {}


def _compare_section(j, prefix, spec, owned, seen):
    for field, kind in spec.items():
        j.compare("%s.%s" % (prefix, field), kind, owned.get(field), seen.get(field))


def _loop_associations(observed):
    """Return (list of associations to the owned backing, complete flag)."""
    loops = observed.get("loop_devices")
    if not isinstance(loops, dict) or loops.get("complete") is not True:
        return None
    entries = loops.get("entries")
    if not isinstance(entries, list) or not all(isinstance(e, dict) for e in entries):
        return None
    return entries


def join_attachment(lease, observed):
    """Join the owned lease to a fresh observation of the running OSD.

    Returns {'verdict': joined|unverified|conflict, 'checks': [...]}. `checks`
    lists every compared field with match/mismatch/unknown so that a caller can
    retain the whole comparison, not only the verdict.
    """
    j = _Joiner()
    if not isinstance(lease, dict) or not isinstance(observed, dict):
        j.unknown("records")
        return {"verdict": UNVERIFIED, "checks": j.checks}

    # Backing file, loop association and DM mapping: all fields of each.
    _compare_section(j, "backing", _LEASE_SPEC["backing"],
                     _section(lease, "backing"), _section(observed, "backing"))
    o_loop = _section(lease, "loop")
    entries = _loop_associations(observed)
    if entries is None:
        j.unknown("loop.scan")
    else:
        owned_back = _section(lease, "backing")
        assoc = [e for e in entries
                 if e.get("back_ino") == owned_back.get("ino")
                 and e.get("back_dev") == owned_back.get("dev")]
        if not _valid(INT, owned_back.get("ino")) or not _valid(STR, owned_back.get("dev")):
            j.unknown("loop.association-count")
        elif len(assoc) != 1:
            # Zero is absence of the owned association; several include a foreign one.
            j.mismatch("loop.association-count")
        else:
            j.match("loop.association-count")
            _compare_section(j, "loop", _LEASE_SPEC["loop"], o_loop, assoc[0])
    o_dm = _section(lease, "dm")
    s_dm = _section(observed, "dm")
    _compare_section(j, "dm", _LEASE_SPEC["dm"], o_dm, s_dm)
    # The mapping must sit directly on exactly the owned loop device.
    dep = s_dm.get("dependencies")
    if _valid(LIST, dep) and _valid(STR, o_loop.get("dev")):
        if sorted(dep) == [o_loop["dev"]]:
            j.match("dm.dependencies-are-owned-loop")
        else:
            j.mismatch("dm.dependencies-are-owned-loop")
    else:
        j.unknown("dm.dependencies-are-owned-loop")

    _join_daemon(j, lease, observed)
    return {"verdict": j.verdict(), "checks": j.checks}


def _join_daemon(j, lease, observed):
    o_daemon = _section(lease, "daemon")
    s_daemon = _section(observed, "daemon")
    _compare_section(j, "daemon", _LEASE_SPEC["daemon"], o_daemon, s_daemon)
    pid = s_daemon.get("pid")
    start = s_daemon.get("process_start")
    if not _valid(INT, pid) or pid == 0:
        j.unknown("daemon.pid")
    else:
        j.match("daemon.pid")
    if not _valid(STR, start):
        j.unknown("daemon.process_start")
    else:
        j.match("daemon.process_start")

    snapshot = observed.get("snapshot")
    device = _section(lease, "dm").get("major_minor")
    if not isinstance(snapshot, dict) or not _valid(STR, snapshot.get("device")):
        j.unknown("snapshot.device")
    elif not _valid(STR, device):
        j.unknown("snapshot.device")
    elif snapshot["device"] != device:
        j.mismatch("snapshot.device")
    else:
        j.match("snapshot.device")
    if _valid(INT, pid) and pid != 0:
        verdict = _obs.attachment_verdict(snapshot, pid)
        if verdict == "attached-io":
            j.match("snapshot.attachment")
        elif verdict == "unverified":
            j.unknown("snapshot.attachment")
        else:
            j.mismatch("snapshot.attachment")

    record = observed.get("borrow")
    if record is None:
        return  # metadata-only mode: no borrow was taken
    if not isinstance(record, dict):
        j.unknown("borrow.record")
        return
    evidence = _bor.attachment_evidence(record)
    if evidence == "borrowed-match":
        j.match("borrow.evidence")
    elif evidence in ("unverified", "no-borrow"):
        j.unknown("borrow.evidence")
    else:
        j.mismatch("borrow.evidence")
    j.compare("borrow.pid", INT, pid, record.get("pid"))
    j.compare("borrow.device", STR, device, record.get("device"))
    # The same process start identity must hold before, during and after.
    j.compare("borrow.start_before", STR, start, record.get("start_before"))
    j.compare("borrow.start_after", STR, start, record.get("start_after"))
