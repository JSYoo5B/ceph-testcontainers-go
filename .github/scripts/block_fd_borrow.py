"""Scoped descriptor-borrow lease for the block attachment observer (issue #7).

Research helper, pure Python with an injectable host adapter. It models the
lifetime rules for an observer that duplicates an OSD's block-device descriptors
(pidfd_getfd) to inspect the referenced open file. It does not implement the
Linux calls, so it establishes nothing about a real kernel, Docker or Ceph.

Why a lease: a borrowed descriptor is a reference to the target's open file. As
long as the observer holds it, the device open and its lock can outlive the
daemon. So the observation is scoped, every borrowed descriptor is closed on
every outcome (validation failure, exception, cancellation), and the closure is
recorded separately from the attachment evidence.

Rules kept here:
  * a denied or unsupported borrow is recorded as such and never escalated;
  * the daemon start identity is read before and after the scope; an unreadable
    or different value leaves the lease unverified (generation change);
  * each borrowed descriptor is checked for block type, device number, inode and
    I/O class against the coherent snapshot entry it was taken from; the
    borrowed descriptor is never read, written, relocked or re-flagged;
  * closure is a separate outcome. It is "closed" only when every borrowed
    descriptor (and the pidfd) was closed without error. A failed close stays
    "unconfirmed" and the first failure is kept apart from later close failures;
  * the release gate needs closed references, a confirmed daemon stop and a fresh
    coherent snapshot without descriptors from the observer or anyone else.
    A snapshot that does not list the observer's borrow cannot replace the
    closure record.
"""
import errno
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "block_fd_observer", Path(__file__).with_name("block_fd_observer.py"))
_obs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_obs)

GRANTED = "granted"
DENIED = "denied"
UNSUPPORTED = "unsupported"
PROCESS_GONE = "process-gone"
ERROR = "error"

CLOSED = "closed"
UNCONFIRMED = "unconfirmed"
NOTHING_BORROWED = "nothing-borrowed"
NOT_ENTERED = "not-entered"
ACTIVE = "active"


class BorrowLease:
    """Context manager that borrows the given (fd, snapshot entry) pairs.

    `entries` are descriptor dicts from a coherent block_fd_observer snapshot,
    all for `pid`. After the with-block `record` holds the evidence; exceptions
    from the body propagate after the closure ran.
    """

    def __init__(self, host, pid, device, machine, entries):
        self.host = host
        self.pid = pid
        self.device = device
        self.machine = machine
        self.entries = list(entries)
        self.record = {
            "pid": pid, "device": device,
            "start_before": None, "start_after": None,
            "permission": None, "permission_errno": None,
            "borrowed": [], "validation": [],
            "first_failure": None, "close_failures": [],
            "closure": NOT_ENTERED,
        }
        self._pidfd = None
        self._uncertain = False
        self._acquired = False

    def __enter__(self):
        # Python does not call __exit__ when __enter__ raises, so any escape from
        # the acquisition phase unwinds here and then re-raises.
        self.record["closure"] = ACTIVE
        try:
            self._acquire()
        except BaseException as exc:
            self._unwind(type(exc))
            raise
        return self

    def _acquire(self):
        rec = self.record
        try:
            rec["start_before"] = self.host.start_identity(self.pid)
        except OSError as exc:
            rec["permission"] = PROCESS_GONE if exc.errno in (errno.ENOENT, errno.ESRCH) else ERROR
            rec["first_failure"] = "start-identity"
            return
        self._uncertain = True  # an interrupt inside the call may leave a new fd
        try:
            self._pidfd = self.host.pidfd_open(self.pid)
            self._acquired = True
            self._uncertain = False
        except OSError as exc:
            self._uncertain = False
            self._permission_failure(exc)
            return
        for entry in self.entries:
            if not self._borrow(entry):
                break
        if rec["permission"] is None and rec["borrowed"]:
            rec["permission"] = GRANTED

    def _permission_failure(self, exc):
        rec = self.record
        rec["permission_errno"] = exc.errno
        if exc.errno in (errno.EPERM, errno.EACCES):
            rec["permission"] = DENIED
        elif exc.errno in (errno.ENOSYS, errno.EINVAL):
            rec["permission"] = UNSUPPORTED
        elif exc.errno in (errno.ESRCH, errno.ENOENT, errno.EBADF):
            rec["permission"] = PROCESS_GONE
        else:
            rec["permission"] = ERROR
        if rec["first_failure"] is None:
            rec["first_failure"] = "borrow-" + rec["permission"]

    def _borrow(self, entry):
        rec = self.record
        self._uncertain = True
        try:
            new_fd = self.host.pidfd_getfd(self._pidfd, int(entry["fd"]))
            self._uncertain = False
        except OSError as exc:
            self._uncertain = False
            self._permission_failure(exc)
            return False
        item = {"target_fd": entry["fd"], "borrowed_fd": new_fd, "closed": False}
        rec["borrowed"].append(item)
        problem = self._validate(new_fd, entry)
        item["validation"] = problem or "match"
        if problem and rec["first_failure"] is None:
            rec["first_failure"] = "validation-" + problem
        return True

    def _validate(self, new_fd, entry):
        """Compare the borrowed descriptor with its snapshot entry (metadata only)."""
        try:
            is_block, device, inode = self.host.fstat(new_fd)
            text = self.host.fdinfo(new_fd)
        except OSError:
            return "unreadable"
        if not is_block or device != self.device:
            return "device-mismatch"
        if inode != entry.get("inode"):
            return "inode-mismatch"
        flags = _obs.parse_fdinfo_flags(text)
        cls = _obs.classify_flags(flags, self.machine)
        if cls == _obs.UNKNOWN_FLAGS:
            return "flags-unknown"
        if cls != entry.get("class"):
            return "class-mismatch"
        return None

    def __exit__(self, exc_type, exc, tb):
        self._unwind(exc_type)
        return False

    def _unwind(self, exc_type):
        """Attempt every close, then settle the closure outcome and identity."""
        rec = self.record
        failures = rec["close_failures"]
        for item in rec["borrowed"]:
            if item["closed"]:
                continue
            try:
                self.host.close(item["borrowed_fd"])
                item["closed"] = True
            except OSError as err:
                failures.append({"fd": item["borrowed_fd"], "errno": err.errno})
        if self._pidfd is not None:
            try:
                self.host.close(self._pidfd)
                self._pidfd = None
            except OSError as err:
                failures.append({"fd": self._pidfd, "errno": err.errno})
        if failures or self._uncertain:
            rec["closure"] = UNCONFIRMED
        elif self._acquired:
            rec["closure"] = CLOSED
        else:
            rec["closure"] = NOTHING_BORROWED
        if exc_type is not None and rec["first_failure"] is None:
            rec["first_failure"] = "scope-" + exc_type.__name__
        try:
            rec["start_after"] = self.host.start_identity(self.pid)
        except Exception:
            rec["start_after"] = None


def attachment_evidence(record):
    """Summarize one lease record as attachment evidence (point in time only).

    Returns one of: unverified, denied, unsupported, generation-changed,
    validation-failed, no-borrow, borrowed-match. The word 'match' says that the
    borrowed descriptors matched the snapshot; it is not exclusivity, workload or
    teardown evidence.
    """
    if not isinstance(record, dict):
        return "unverified"
    if record.get("permission") in (DENIED, UNSUPPORTED):
        return record["permission"]
    before, after = record.get("start_before"), record.get("start_after")
    if not before or not after:
        return "unverified"
    if before != after:
        return "generation-changed"
    if record.get("permission") != GRANTED:
        return "unverified"
    borrowed = record.get("borrowed")
    if not borrowed:
        return "no-borrow"
    if any(item.get("validation") != "match" for item in borrowed):
        return "validation-failed"
    return "borrowed-match"


def release_gate(record, daemon_stopped, fresh_snapshot, device):
    """Decide whether the owned mapping may enter the release barriers.

    Returns release-eligible, observer-reference-unconfirmed, daemon-running,
    unverified or device-users-remain. `daemon_stopped` must be the literal True
    from the caller's stop confirmation. Only the closure record can show that the
    observer's own references are gone; an opener list is not a substitute.
    """
    if not isinstance(record, dict):
        return "unverified"
    if (not isinstance(device, str) or not device
            or record.get("device") != device):
        return "unverified"
    if record.get("closure") not in (CLOSED, NOTHING_BORROWED):
        return "observer-reference-unconfirmed"
    if daemon_stopped is not True:
        return "daemon-running"
    snap = fresh_snapshot
    if (not isinstance(snap, dict) or snap.get("coherent") is not True
            or snap.get("device") != device):
        return "unverified"
    if snap.get("descriptors"):
        return "device-users-remain"
    return "release-eligible"
