import errno
import importlib.util
from pathlib import Path
import unittest


def _load(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


obs = _load("block_fd_observer")
bor = _load("block_fd_borrow")

DEV = "252:0"
PID = 500
RDWR = 0o2
DIRECT = obs.O_DIRECT_BY_MACHINE["x86_64"]
DIRECT_FLAGS = RDWR | DIRECT | 0o100000
BUFFERED = RDWR | 0o2000000 | 0o100000


def info(flags):
    return "pos:\t0\nflags:\t%07o\nmnt_id:\t12\nino:\t77\n" % flags


def entries():
    return [
        {"pid": PID, "fd": "10", "inode": 77, "flags": DIRECT_FLAGS,
         "class": obs.READ_WRITE_DIRECT},
        {"pid": PID, "fd": "20", "inode": 77, "flags": BUFFERED,
         "class": obs.READ_WRITE_BUFFERED},
    ]


class FakeBorrowHost:
    def __init__(self):
        self.next_fd = 100
        self.table = {}          # borrowed fd -> (stat, info)
        self.targets = {
            "10": ((True, DEV, 77), info(DIRECT_FLAGS)),
            "20": ((True, DEV, 77), info(BUFFERED)),
        }
        self.starts = [("1234",), ("1234",)]
        self.start_error = None
        self.pidfd_error = None
        self.getfd_error = {}    # target fd -> OSError
        self.fstat_error = None
        self.close_error = {}    # fd -> OSError
        self.closed = []
        self.calls = []

    def start_identity(self, pid):
        self.calls.append("start")
        if self.start_error:
            raise self.start_error
        return self.starts.pop(0)[0]

    def pidfd_open(self, pid):
        self.calls.append("pidfd_open")
        if self.pidfd_error:
            raise self.pidfd_error
        self.pidfd = self.next_fd
        self.next_fd += 1
        return self.pidfd

    def pidfd_getfd(self, pidfd, target):
        self.calls.append("getfd:" + str(target))
        if str(target) in self.getfd_error:
            raise self.getfd_error[str(target)]
        fd = self.next_fd
        self.next_fd += 1
        self.table[fd] = self.targets[str(target)]
        return fd

    def fstat(self, fd):
        if self.fstat_error:
            raise self.fstat_error
        return self.table[fd][0]

    def fdinfo(self, fd):
        return self.table[fd][1]

    def close(self, fd):
        self.calls.append("close:" + str(fd))
        if fd in self.close_error:
            raise self.close_error[fd]
        self.closed.append(fd)

    def open_count(self):
        return len(self.table) + 1 - len(self.closed)


def run(host, body=None, ents=None):
    lease = bor.BorrowLease(host, PID, DEV, "x86_64", entries() if ents is None else ents)
    with lease:
        if body:
            body(lease)
    return lease.record


def clean_snapshot(**extra):
    snap = {"device": DEV, "coherent": True, "complete": True, "descriptors": []}
    snap.update(extra)
    return snap


class BorrowLeaseTest(unittest.TestCase):
    def test_two_expected_descriptors_borrowed_and_all_closed(self):
        host = FakeBorrowHost()
        rec = run(host)
        self.assertEqual(rec["permission"], bor.GRANTED)
        self.assertEqual(len(rec["borrowed"]), 2)
        self.assertEqual(rec["closure"], bor.CLOSED)
        self.assertEqual(sorted(host.closed), [100, 101, 102])
        self.assertEqual(bor.attachment_evidence(rec), "borrowed-match")

    def test_denied_borrow_is_recorded_and_not_escalated(self):
        host = FakeBorrowHost()
        host.getfd_error["10"] = OSError(errno.EPERM, "denied")
        rec = run(host)
        self.assertEqual(rec["permission"], bor.DENIED)
        self.assertEqual(rec["borrowed"], [])
        self.assertEqual(rec["closure"], bor.CLOSED)  # only the pidfd existed
        self.assertEqual(bor.attachment_evidence(rec), "denied")
        self.assertNotIn("getfd:20", host.calls)

    def test_denied_pidfd_open_holds_nothing(self):
        host = FakeBorrowHost()
        host.pidfd_error = OSError(errno.EACCES, "denied")
        rec = run(host)
        self.assertEqual(rec["closure"], bor.NOTHING_BORROWED)
        self.assertEqual(bor.attachment_evidence(rec), "denied")

    def test_unsupported_kernel_is_not_a_match(self):
        host = FakeBorrowHost()
        host.pidfd_error = OSError(errno.ENOSYS, "no")
        self.assertEqual(bor.attachment_evidence(run(host)), "unsupported")

    def test_partial_borrow_then_failure_closes_what_was_taken(self):
        host = FakeBorrowHost()
        host.getfd_error["20"] = OSError(errno.ESRCH, "gone")
        rec = run(host)
        self.assertEqual(len(rec["borrowed"]), 1)
        self.assertEqual(rec["closure"], bor.CLOSED)
        self.assertEqual(rec["permission"], bor.PROCESS_GONE)
        self.assertEqual(bor.attachment_evidence(rec), "unverified")

    def test_generation_change_is_not_attachment_evidence(self):
        host = FakeBorrowHost()
        host.starts = [("1234",), ("9999",)]
        rec = run(host)
        self.assertEqual(bor.attachment_evidence(rec), "generation-changed")
        self.assertEqual(rec["closure"], bor.CLOSED)

    def test_missing_start_identity_is_unverified(self):
        for bad in (None, ""):
            host = FakeBorrowHost()
            host.starts = [("1234",), (bad,)]
            self.assertEqual(bor.attachment_evidence(run(host)), "unverified")
        host = FakeBorrowHost()
        host.start_error = OSError(errno.ESRCH, "gone")
        rec = run(host)
        self.assertEqual(rec["closure"], bor.NOTHING_BORROWED)
        self.assertEqual(bor.attachment_evidence(rec), "unverified")

    def test_validation_failure_keeps_live_borrow_then_closes_it(self):
        host = FakeBorrowHost()
        host.targets["20"] = ((True, "8:0", 77), info(BUFFERED))
        seen = {}

        def body(lease):
            seen["open_inside"] = len(host.closed)

        rec = run(host, body)
        self.assertEqual(seen["open_inside"], 0)
        self.assertEqual(rec["closure"], bor.CLOSED)
        self.assertEqual(rec["first_failure"], "validation-device-mismatch")
        self.assertEqual(bor.attachment_evidence(rec), "validation-failed")

    def test_each_validation_mismatch_is_detected(self):
        cases = {
            "inode-mismatch": ((True, DEV, 78), info(DIRECT_FLAGS)),
            "device-mismatch": ((False, DEV, 77), info(DIRECT_FLAGS)),
            "flags-unknown": ((True, DEV, 77), "pos:\t0\n"),
            "class-mismatch": ((True, DEV, 77), info(BUFFERED)),
        }
        for want, value in cases.items():
            host = FakeBorrowHost()
            host.targets["10"] = value
            rec = run(host, ents=entries()[:1])
            self.assertEqual(rec["borrowed"][0]["validation"], want, want)
            self.assertEqual(bor.attachment_evidence(rec), "validation-failed")

    def test_unreadable_borrowed_descriptor_is_unverified_and_closed(self):
        host = FakeBorrowHost()
        host.fstat_error = OSError(errno.EIO, "io")
        rec = run(host)
        self.assertEqual(rec["borrowed"][0]["validation"], "unreadable")
        self.assertEqual(rec["closure"], bor.CLOSED)

    def test_body_exception_still_closes_and_propagates(self):
        host = FakeBorrowHost()
        lease = bor.BorrowLease(host, PID, DEV, "x86_64", entries())
        with self.assertRaises(KeyboardInterrupt):
            with lease:
                raise KeyboardInterrupt()
        self.assertEqual(lease.record["closure"], bor.CLOSED)
        self.assertEqual(lease.record["first_failure"], "scope-KeyboardInterrupt")
        self.assertEqual(len(host.closed), 3)

    def _interrupted_enter(self, host):
        lease = bor.BorrowLease(host, PID, DEV, "x86_64", entries())
        with self.assertRaises(KeyboardInterrupt):
            with lease:
                self.fail("body must not run")
        return lease

    def test_interrupt_during_validation_unwinds_acquired_references(self):
        host = FakeBorrowHost()
        host.fstat_error = KeyboardInterrupt()
        lease = self._interrupted_enter(host)
        rec = lease.record
        self.assertEqual(sorted(host.closed), [100, 101])  # pidfd and borrowed fd
        self.assertEqual(rec["closure"], bor.CLOSED)
        self.assertEqual(rec["first_failure"], "scope-KeyboardInterrupt")
        self.assertEqual(
            bor.release_gate(rec, True, clean_snapshot(), DEV), "release-eligible")

    def test_interrupt_during_validation_keeps_close_failures_separate(self):
        host = FakeBorrowHost()
        host.fstat_error = KeyboardInterrupt()
        host.close_error[101] = OSError(errno.EIO, "io")
        rec = self._interrupted_enter(host).record
        self.assertEqual(rec["closure"], bor.UNCONFIRMED)
        self.assertEqual(rec["first_failure"], "scope-KeyboardInterrupt")
        self.assertEqual(rec["close_failures"], [{"fd": 101, "errno": errno.EIO}])
        self.assertEqual(host.closed, [100])  # pidfd close still attempted
        self.assertEqual(
            bor.release_gate(rec, True, clean_snapshot(), DEV),
            "observer-reference-unconfirmed")

    def test_interrupt_inside_pidfd_getfd_is_unconfirmed(self):
        host = FakeBorrowHost()
        host.getfd_error["10"] = KeyboardInterrupt()
        rec = self._interrupted_enter(host).record
        self.assertEqual(host.closed, [100])
        self.assertEqual(rec["closure"], bor.UNCONFIRMED)  # a new fd may exist

    def test_interrupt_inside_pidfd_open_is_unconfirmed(self):
        host = FakeBorrowHost()
        host.pidfd_error = KeyboardInterrupt()
        rec = self._interrupted_enter(host).record
        self.assertEqual(rec["closure"], bor.UNCONFIRMED)

    def test_interrupt_before_any_acquisition_holds_nothing(self):
        host = FakeBorrowHost()
        host.start_error = KeyboardInterrupt()
        rec = self._interrupted_enter(host).record
        self.assertEqual(rec["closure"], bor.NOTHING_BORROWED)

    def test_close_failure_stays_unconfirmed_and_separate_from_first_failure(self):
        host = FakeBorrowHost()
        host.targets["10"] = ((True, "8:0", 77), info(DIRECT_FLAGS))
        host.close_error[101] = OSError(errno.EIO, "io")
        rec = run(host)
        self.assertEqual(rec["closure"], bor.UNCONFIRMED)
        self.assertEqual(rec["first_failure"], "validation-device-mismatch")
        self.assertEqual(rec["close_failures"], [{"fd": 101, "errno": errno.EIO}])
        self.assertEqual(sorted(host.closed), [100, 102])  # later closes still ran

    def test_borrowed_descriptors_are_never_read_or_written(self):
        host = FakeBorrowHost()
        run(host)
        self.assertEqual({c.split(":")[0] for c in host.calls},
                         {"start", "pidfd_open", "getfd", "close"})

    def test_attachment_evidence_rejects_non_records(self):
        for bad in (None, [], "x", {}):
            self.assertEqual(bor.attachment_evidence(bad), "unverified")


class ReleaseGateTest(unittest.TestCase):
    def closed_record(self):
        return run(FakeBorrowHost())

    def test_closed_stopped_and_empty_is_eligible(self):
        self.assertEqual(
            bor.release_gate(self.closed_record(), True, clean_snapshot(), DEV),
            "release-eligible")

    def test_osd_exit_before_observer_closure_blocks_release(self):
        host = FakeBorrowHost()
        host.close_error[101] = OSError(errno.EIO, "io")
        rec = run(host)
        # the daemon is gone and a snapshot lists nothing, yet the borrow may live
        self.assertEqual(bor.release_gate(rec, True, clean_snapshot(), DEV),
                         "observer-reference-unconfirmed")

    def test_record_without_closure_outcome_blocks(self):
        for rec in ({"device": DEV}, {"device": DEV, "closure": None},
                    {"device": DEV, "closure": "weird"},
                    {"device": DEV, "closure": bor.NOT_ENTERED}):
            self.assertEqual(bor.release_gate(rec, True, clean_snapshot(), DEV),
                             "observer-reference-unconfirmed")
        self.assertEqual(bor.release_gate(None, True, clean_snapshot(), DEV),
                         "unverified")

    def test_active_scope_is_not_release_eligible(self):
        host = FakeBorrowHost()
        lease = bor.BorrowLease(host, PID, DEV, "x86_64", entries())
        with lease:
            self.assertEqual(lease.record["closure"], bor.ACTIVE)
            self.assertEqual(
                bor.release_gate(lease.record, True, clean_snapshot(), DEV),
                "observer-reference-unconfirmed")
        self.assertEqual(
            bor.release_gate(lease.record, True, clean_snapshot(), DEV),
            "release-eligible")

    def test_unentered_lease_is_not_release_eligible(self):
        lease = bor.BorrowLease(FakeBorrowHost(), PID, DEV, "x86_64", entries())
        self.assertEqual(bor.release_gate(lease.record, True, clean_snapshot(), DEV),
                         "observer-reference-unconfirmed")

    def test_closure_record_is_joined_to_the_requested_device(self):
        rec = self.closed_record()  # for DEV
        other = "8:0"
        self.assertEqual(
            bor.release_gate(rec, True, clean_snapshot(device=other), other),
            "unverified")
        self.assertEqual(
            bor.release_gate(rec, True, clean_snapshot(device=other), DEV),
            "unverified")
        for bad in (None, "", 252):
            self.assertEqual(
                bor.release_gate(rec, True, clean_snapshot(), bad), "unverified")
        missing = dict(rec)
        del missing["device"]
        self.assertEqual(bor.release_gate(missing, True, clean_snapshot(), DEV),
                         "unverified")
        self.assertEqual(
            bor.release_gate(dict(rec, device=None), True, clean_snapshot(), DEV),
            "unverified")

    def test_running_or_unconfirmed_daemon_blocks(self):
        for stopped in (False, None, 1, "yes"):
            self.assertEqual(
                bor.release_gate(self.closed_record(), stopped, clean_snapshot(), DEV),
                "daemon-running")

    def test_snapshot_must_be_coherent_matching_and_empty(self):
        rec = self.closed_record()
        for snap in (None, clean_snapshot(coherent=False), clean_snapshot(coherent=None),
                     clean_snapshot(device="8:0"), {}):
            self.assertEqual(bor.release_gate(rec, True, snap, DEV), "unverified")
        left = clean_snapshot(descriptors=[{"pid": 9, "fd": "3", "class": obs.PATH_ONLY}])
        self.assertEqual(bor.release_gate(rec, True, left, DEV), "device-users-remain")

    def test_denied_borrow_with_nothing_held_can_still_release(self):
        host = FakeBorrowHost()
        host.pidfd_error = OSError(errno.EPERM, "no")
        rec = run(host)
        self.assertEqual(bor.release_gate(rec, True, clean_snapshot(), DEV),
                         "release-eligible")


if __name__ == "__main__":
    unittest.main()
