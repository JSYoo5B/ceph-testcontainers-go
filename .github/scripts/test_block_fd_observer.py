import errno
import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "block_fd_observer", Path(__file__).with_name("block_fd_observer.py"))
obs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(obs)

DEV = "252:0"
RDWR = 0o2
DIRECT = obs.O_DIRECT_BY_MACHINE["x86_64"]
CLOEXEC = 0o2000000
BUFFERED = RDWR | CLOEXEC | 0o100000
DIRECT_FLAGS = RDWR | DIRECT | 0o100000


def info(flags):
    return "pos:\t0\nflags:\t%07o\nmnt_id:\t12\nino:\t5\n" % flags


class FakeProc:
    """pid -> fd -> {'stat': (is_block, dev, ino) | exc, 'info': text | exc}."""

    def __init__(self, pids, me=1):
        self.pids = pids
        self.me = me
        self.proc_error = None
        self.after = {}      # (pid, fd) -> stat returned on the second call
        self.calls = {}

    def getpid(self):
        return self.me

    def listdir(self, path):
        if path == "":
            if self.proc_error:
                raise self.proc_error
            return [str(p) for p in self.pids] + ["self"]
        pid = int(path.split("/")[1])
        entry = self.pids[pid]
        if isinstance(entry, OSError):
            raise entry
        return list(entry)

    def stat(self, path):
        _, pid, _, fd = path.split("/")
        key = (int(pid), fd)
        self.calls[key] = self.calls.get(key, 0) + 1
        if self.calls[key] > 1 and key in self.after:
            value = self.after[key]
        else:
            value = self.pids[int(pid)][fd]["stat"]
        if isinstance(value, OSError):
            raise value
        return value

    def read_text(self, path):
        _, pid, _, fd = path.split("/")
        value = self.pids[int(pid)][fd]["info"]
        if isinstance(value, OSError):
            raise value
        return value


def fd(flags, dev=DEV, ino=77, block=True):
    return {"stat": (block, dev, ino), "info": info(flags)}


def osd_fds(n=2):
    out = {}
    for i in range(n):
        out[str(10 + i)] = fd(DIRECT_FLAGS)
        out[str(20 + i)] = fd(BUFFERED)
    return out


def run(pids, machine="x86_64", me=1):
    return obs.observe(FakeProc(pids, me), DEV, machine)


class FlagParsing(unittest.TestCase):
    def test_octal_flags(self):
        self.assertEqual(obs.parse_fdinfo_flags(info(0o2100002)), 0o2100002)

    def test_missing_malformed_duplicate_and_wrong_type_are_unknown(self):
        for text in ["pos:\t0\n", "flags:\t\n", "flags:\t0189\n", "flags:\t-1\n",
                     "flags:\t02\nflags:\t02\n", "", None, 5, b"flags:\t02\n"]:
            self.assertIsNone(obs.parse_fdinfo_flags(text), repr(text))

    def test_zero_is_a_real_value_and_none_is_not_zero(self):
        self.assertEqual(obs.parse_fdinfo_flags("flags:\t0\n"), 0)
        self.assertEqual(obs.classify_flags(None, "x86_64"), obs.UNKNOWN_FLAGS)
        self.assertEqual(obs.classify_flags(0, "x86_64"), obs.READ_ONLY)


class Classification(unittest.TestCase):
    def test_classes(self):
        c = obs.classify_flags
        self.assertEqual(c(obs.O_PATH, "x86_64"), obs.PATH_ONLY)
        self.assertEqual(c(obs.O_PATH | RDWR, "x86_64"), obs.PATH_ONLY)
        self.assertEqual(c(0o1, "x86_64"), obs.WRITE_ONLY)
        self.assertEqual(c(0o3, "x86_64"), obs.UNKNOWN_FLAGS)
        self.assertEqual(c(DIRECT_FLAGS, "x86_64"), obs.READ_WRITE_DIRECT)
        self.assertEqual(c(BUFFERED, "x86_64"), obs.READ_WRITE_BUFFERED)

    def test_o_direct_bit_is_architecture_specific(self):
        arm_direct = obs.O_DIRECT_BY_MACHINE["aarch64"] | RDWR
        self.assertEqual(obs.classify_flags(arm_direct, "aarch64"), obs.READ_WRITE_DIRECT)
        # The same bits mean something else on x86_64.
        self.assertEqual(obs.classify_flags(arm_direct, "x86_64"), obs.READ_WRITE_BUFFERED)

    def test_unknown_machine_does_not_guess_direct(self):
        self.assertEqual(obs.classify_flags(DIRECT_FLAGS, "riscv64"),
                         obs.READ_WRITE_UNKNOWN_DIRECT)

    def test_excl_bit_is_not_an_oracle(self):
        with_excl = DIRECT_FLAGS | 0o200
        self.assertEqual(obs.classify_flags(with_excl, "x86_64"), obs.READ_WRITE_DIRECT)
        self.assertEqual(obs.classify_flags(DIRECT_FLAGS, "x86_64"), obs.READ_WRITE_DIRECT)


class Observation(unittest.TestCase):
    def test_path_only_and_io_descriptors_differ_for_the_same_device(self):
        snap = run({5: {"3": fd(obs.O_PATH)}, 6: {"3": fd(DIRECT_FLAGS)}})
        classes = {d["pid"]: d["class"] for d in snap["descriptors"]}
        self.assertEqual(classes, {5: obs.PATH_ONLY, 6: obs.READ_WRITE_DIRECT})
        self.assertEqual(obs.attachment_verdict(snap, 5), "path-only-reference")

    def test_non_matching_devices_and_files_are_skipped(self):
        snap = run({5: {"3": fd(DIRECT_FLAGS, dev="252:1"),
                        "4": fd(DIRECT_FLAGS, block=False)}})
        self.assertEqual(snap["descriptors"], [])
        self.assertTrue(snap["coherent"])
        self.assertEqual(obs.attachment_verdict(snap, 5), "not-attached")

    def test_every_matching_descriptor_is_kept_per_process(self):
        snap = run({7: osd_fds(6)})
        self.assertEqual(len(snap["descriptors"]), 12)
        self.assertEqual(obs.attachment_verdict(snap, 7), "attached-io")

    def test_unreadable_flags_are_kept_as_unknown_and_unverified(self):
        pids = {7: osd_fds()}
        pids[7]["30"] = {"stat": (True, DEV, 77), "info": "pos:\t0\n"}
        snap = run(pids)
        unknown = [d for d in snap["descriptors"] if d["fd"] == "30"][0]
        self.assertIsNone(unknown["flags"])
        self.assertEqual(unknown["class"], obs.UNKNOWN_FLAGS)
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_listing_error_makes_snapshot_incomplete(self):
        snap = run({7: osd_fds(), 8: OSError(errno.EACCES, "denied")})
        self.assertFalse(snap["complete"])
        self.assertEqual(snap["errors"][0]["pid"], 8)
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_proc_listing_failure_is_unverified_not_absent(self):
        host = FakeProc({})
        host.proc_error = OSError(errno.EIO, "io")
        snap = obs.observe(host, DEV, "x86_64")
        self.assertFalse(snap["complete"])
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_vanished_process_and_fd_are_races(self):
        snap = run({7: osd_fds(), 8: OSError(errno.ENOENT, "gone")})
        self.assertTrue(snap["complete"])
        self.assertFalse(snap["coherent"])
        self.assertEqual(snap["races"][0]["why"], "process-vanished")
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")
        pids = {7: osd_fds()}
        pids[7]["10"]["info"] = OSError(errno.ENOENT, "closed")
        self.assertEqual(run(pids)["races"][0]["why"], "fd-vanished")

    def test_identity_change_between_stat_and_flags_read_is_a_race(self):
        host = FakeProc({7: osd_fds()})
        host.after[(7, "10")] = (True, DEV, 999)
        snap = obs.observe(host, DEV, "x86_64")
        self.assertEqual([r["why"] for r in snap["races"]], ["identity-changed"])
        self.assertNotIn("10", [d["fd"] for d in snap["descriptors"]])
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_flag_read_error_other_than_vanish_stays_unknown(self):
        pids = {7: osd_fds()}
        pids[7]["10"]["info"] = OSError(errno.EACCES, "denied")
        snap = run(pids)
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")


class Verdict(unittest.TestCase):
    def test_missing_pair_is_not_an_attachment(self):
        direct_only = {"10": fd(DIRECT_FLAGS)}
        buffered_only = {"10": fd(BUFFERED)}
        read_only = {"10": fd(0o2000000), "11": fd(0o2000000 | DIRECT)}
        for fds in (direct_only, buffered_only, read_only):
            self.assertEqual(obs.attachment_verdict(run({7: fds}), 7), "no-io-pair")

    def test_path_references_do_not_complete_an_io_pair(self):
        fds = {"10": fd(DIRECT_FLAGS), "11": fd(obs.O_PATH), "12": fd(obs.O_PATH | RDWR)}
        self.assertEqual(obs.attachment_verdict(run({7: fds}), 7), "no-io-pair")

    def test_path_only_foreign_reference_is_diagnostic_only(self):
        snap = run({7: osd_fds(), 9: {"3": fd(obs.O_PATH)}})
        self.assertEqual(obs.attachment_verdict(snap, 7), "attached-io")

    def test_foreign_io_opener_is_reported(self):
        snap = run({7: osd_fds(), 9: {"3": fd(0o2000000)}})
        self.assertEqual(obs.attachment_verdict(snap, 7), "foreign-opener")

    def test_foreign_unknown_flags_are_unverified(self):
        other = {"3": {"stat": (True, DEV, 77), "info": "flags:\tzz\n"}}
        snap = run({7: osd_fds(), 9: other})
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_observer_own_descriptor_is_not_a_foreign_opener_but_is_retained(self):
        snap = run({7: osd_fds(), 1: {"3": fd(DIRECT_FLAGS)}}, me=1)
        self.assertIn(1, [d["pid"] for d in snap["descriptors"]])
        self.assertEqual(obs.attachment_verdict(snap, 7), "attached-io")

    def test_unknown_machine_is_unverified(self):
        snap = run({7: osd_fds()}, machine="riscv64")
        self.assertEqual(obs.attachment_verdict(snap, 7), "unverified")

    def test_wrong_snapshot_shapes_are_unverified(self):
        for bad in (None, {}, {"coherent": 1}, "x"):
            self.assertEqual(obs.attachment_verdict(bad, 7), "unverified")

    def test_other_pid_expected_is_not_attached(self):
        snap = run({7: osd_fds()})
        self.assertEqual(obs.attachment_verdict(snap, 8), "not-attached")


class ProcHostControl(unittest.TestCase):
    def test_reads_a_synthetic_proc_tree_without_block_devices(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            (base / "5" / "fd").mkdir(parents=True)
            (base / "5" / "fd" / "3").write_text("x")
            (base / "5" / "fdinfo").mkdir()
            (base / "5" / "fdinfo" / "3").write_text(info(DIRECT_FLAGS))
            (base / "self").mkdir()
            snap = obs.observe(obs.ProcHost(root), DEV, "x86_64")
        self.assertTrue(snap["coherent"])
        self.assertEqual(snap["descriptors"], [])


if __name__ == "__main__":
    unittest.main()
