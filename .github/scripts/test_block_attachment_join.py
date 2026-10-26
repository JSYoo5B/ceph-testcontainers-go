import copy
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
join = _load("block_attachment_join")

PID = 500
DM_DEV = "252:0"
LOOP_DEV = "7:3"
DIRECT = obs.O_DIRECT_BY_MACHINE["x86_64"]


def lease():
    return {
        "backing": {"dev": "8:1", "ino": 4242, "size": 1 << 28},
        "loop": {"name": "/dev/loop3", "dev": LOOP_DEV, "back_dev": "8:1",
                 "back_ino": 4242, "offset": 0, "sizelimit": 1 << 28},
        "dm": {"name": "owned-map", "uuid": "owned-uuid", "major_minor": DM_DEV,
               "table": "0 524288 linear 7:3 0", "dependencies": [LOOP_DEV]},
        "daemon": {"container_id": "c0ffee", "start_generation": "2026-01-01T00:00:01Z"},
    }


def snapshot():
    return {
        "device": DM_DEV, "machine": "x86_64", "observer_pid": 1,
        "complete": True, "coherent": True, "errors": [], "races": [],
        "descriptors": [
            {"pid": PID, "fd": "10", "inode": 77, "flags": 0o2 | DIRECT,
             "class": obs.READ_WRITE_DIRECT},
            {"pid": PID, "fd": "11", "inode": 77, "flags": 0o2000002,
             "class": obs.READ_WRITE_BUFFERED},
        ],
    }


def observed():
    rec = lease()
    return {
        "backing": dict(rec["backing"]),
        "loop_devices": {"complete": True, "entries": [dict(rec["loop"], back_dev="8:1")]},
        "dm": copy.deepcopy(rec["dm"]),
        "daemon": dict(rec["daemon"], pid=PID, process_start="777"),
        "snapshot": snapshot(),
    }


def verdict(lease_rec=None, seen=None):
    return join.join_attachment(lease() if lease_rec is None else lease_rec,
                                observed() if seen is None else seen)


def borrow_record(**over):
    rec = {"pid": PID, "device": DM_DEV, "start_before": "777", "start_after": "777",
           "permission": "granted", "borrowed": [{"validation": "match"}],
           "closure": "closed", "close_failures": [], "first_failure": None}
    rec.update(over)
    return rec


class JoinedTest(unittest.TestCase):
    def test_exact_chain_joins_and_keeps_every_comparison(self):
        res = verdict()
        self.assertEqual(res["verdict"], join.JOINED, res["checks"])
        fields = {c["field"] for c in res["checks"]}
        for expected in ("backing.ino", "loop.back_ino", "loop.offset", "loop.sizelimit",
                         "dm.uuid", "dm.table", "dm.major_minor", "daemon.container_id",
                         "daemon.start_generation", "snapshot.device"):
            self.assertIn(expected, fields)

    def test_borrow_record_with_same_generation_joins(self):
        seen = observed()
        seen["borrow"] = borrow_record()
        self.assertEqual(verdict(seen=seen)["verdict"], join.JOINED)


class EveryFieldIsComparedTest(unittest.TestCase):
    """Each identity field, changed alone, must break the join."""

    def test_changed_observed_fields_conflict(self):
        cases = [
            ("backing.dev", lambda s: s["backing"].update(dev="8:2")),
            ("backing.ino", lambda s: s["backing"].update(ino=1)),
            ("backing.size", lambda s: s["backing"].update(size=1)),
            ("dm.name", lambda s: s["dm"].update(name="other")),
            ("dm.uuid", lambda s: s["dm"].update(uuid="other")),
            ("dm.major_minor", lambda s: (s["dm"].update(major_minor="252:1"))),
            ("dm.table", lambda s: s["dm"].update(table="0 524288 delay 7:3 0 100")),
            ("dm.dependencies", lambda s: s["dm"].update(dependencies=["7:4"])),
            ("daemon.container_id", lambda s: s["daemon"].update(container_id="x")),
            ("daemon.start_generation", lambda s: s["daemon"].update(start_generation="y")),
        ]
        for field, mutate in cases:
            with self.subTest(field=field):
                seen = observed()
                mutate(seen)
                res = verdict(seen=seen)
                self.assertEqual(res["verdict"], join.CONFLICT)
                self.assertTrue(any(c["field"].startswith(field.split(".")[0])
                                    and c["state"] == "mismatch" for c in res["checks"]))

    def test_each_loop_field_conflicts(self):
        for field, value in (("name", "/dev/loop9"), ("dev", "7:9"), ("back_dev", "8:2"),
                             ("back_ino", 1), ("offset", 4096), ("sizelimit", 1)):
            with self.subTest(field=field):
                seen = observed()
                seen["loop_devices"]["entries"][0][field] = value
                res = verdict(seen=seen)
                self.assertEqual(res["verdict"], join.CONFLICT)

    def test_name_and_major_minor_alone_do_not_join(self):
        seen = observed()
        seen["dm"].update(uuid="replacement", table="0 8 error")
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_snapshot_on_another_device_conflicts(self):
        seen = observed()
        seen["snapshot"]["device"] = "252:9"
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_borrow_pid_device_and_generation_each_conflict(self):
        for field, value in (("pid", 501), ("device", "252:9"),
                             ("start_before", "1"), ("start_after", "2")):
            with self.subTest(field=field):
                seen = observed()
                seen["borrow"] = borrow_record(**{field: value})
                self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_dependency_must_be_exactly_the_owned_loop(self):
        seen = observed()
        seen["dm"]["dependencies"] = [LOOP_DEV, "7:4"]
        res = verdict(lease_rec=dict(lease(), dm=dict(lease()["dm"], dependencies=[LOOP_DEV, "7:4"])),
                      seen=seen)
        self.assertEqual(res["verdict"], join.CONFLICT)
        self.assertIn({"field": "dm.dependencies-are-owned-loop", "state": "mismatch"},
                      res["checks"])


class LoopAssociationTest(unittest.TestCase):
    def test_second_association_to_same_backing_conflicts(self):
        seen = observed()
        seen["loop_devices"]["entries"].append(
            dict(seen["loop_devices"]["entries"][0], name="/dev/loop4", dev="7:4"))
        res = verdict(seen=seen)
        self.assertEqual(res["verdict"], join.CONFLICT)
        self.assertIn({"field": "loop.association-count", "state": "mismatch"}, res["checks"])

    def test_no_association_conflicts(self):
        seen = observed()
        seen["loop_devices"]["entries"] = []
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_unrelated_associations_are_ignored(self):
        seen = observed()
        seen["loop_devices"]["entries"].append(
            {"name": "/dev/loop5", "dev": "7:5", "back_dev": "8:1", "back_ino": 1,
             "offset": 0, "sizelimit": 0})
        self.assertEqual(verdict(seen=seen)["verdict"], join.JOINED)

    def test_incomplete_scan_is_unverified_not_absent(self):
        for loops in ({"complete": False, "entries": []}, {"entries": []}, None,
                      {"complete": True, "entries": None},
                      {"complete": True, "entries": ["x"]}):
            with self.subTest(loops=loops):
                seen = observed()
                seen["loop_devices"] = loops
                self.assertEqual(verdict(seen=seen)["verdict"], join.UNVERIFIED)


class MissingNullAndWrongTypeTest(unittest.TestCase):
    def paths(self):
        yield ("backing", "dev"), ("backing", "ino"), ("backing", "size")
        yield ("dm", "name"), ("dm", "uuid"), ("dm", "major_minor"), ("dm", "table"), ("dm", "dependencies")
        yield ("daemon", "container_id"), ("daemon", "start_generation"), ("daemon", "pid"), ("daemon", "process_start")

    def test_each_observed_field_missing_null_empty_or_mistyped_is_unverified(self):
        for group in self.paths():
            for section, field in group:
                for label, bad in (("missing", None), ("null", None), ("empty", ""),
                                   ("bool", True), ("negative", -1), ("list", [])):
                    seen = observed()
                    if label == "missing":
                        del seen[section][field]
                    else:
                        seen[section][field] = bad
                    with self.subTest(field=section + "." + field, kind=label):
                        res = verdict(seen=seen)
                        self.assertNotEqual(res["verdict"], join.JOINED)

    def test_empty_equals_empty_never_joins(self):
        base, seen = lease(), observed()
        for rec in (base, seen):
            rec["dm"]["uuid"] = ""
        self.assertEqual(verdict(base, seen)["verdict"], join.UNVERIFIED)

    def test_missing_lease_field_is_unverified(self):
        base = lease()
        del base["dm"]["uuid"]
        self.assertEqual(verdict(base)["verdict"], join.UNVERIFIED)
        base = lease()
        base["backing"]["ino"] = None
        res = verdict(base)
        self.assertEqual(res["verdict"], join.UNVERIFIED)

    def test_null_lease_and_observation_records(self):
        self.assertEqual(join.join_attachment(None, observed())["verdict"], join.UNVERIFIED)
        self.assertEqual(join.join_attachment(lease(), None)["verdict"], join.UNVERIFIED)
        self.assertEqual(join.join_attachment(lease(), {})["verdict"], join.UNVERIFIED)

    def test_conflict_outranks_unverified(self):
        seen = observed()
        seen["dm"]["uuid"] = "other"
        del seen["backing"]["size"]
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_bool_never_equals_an_integer_identity(self):
        base, seen = lease(), observed()
        base["backing"]["ino"] = 1
        seen["backing"]["ino"] = True
        base["loop"]["back_ino"] = 1
        seen["loop_devices"]["entries"][0]["back_ino"] = True
        self.assertNotEqual(verdict(base, seen)["verdict"], join.JOINED)
        self.assertEqual(verdict(base, seen)["verdict"], join.UNVERIFIED)

    def test_pid_zero_is_not_a_daemon(self):
        seen = observed()
        seen["daemon"]["pid"] = 0
        self.assertEqual(verdict(seen=seen)["verdict"], join.UNVERIFIED)


class SnapshotAndBorrowTest(unittest.TestCase):
    def test_incoherent_or_missing_snapshot_is_unverified(self):
        seen = observed()
        seen["snapshot"]["coherent"] = False
        self.assertEqual(verdict(seen=seen)["verdict"], join.UNVERIFIED)
        seen = observed()
        del seen["snapshot"]
        self.assertEqual(verdict(seen=seen)["verdict"], join.UNVERIFIED)

    def test_path_only_reference_does_not_join(self):
        seen = observed()
        for d in seen["snapshot"]["descriptors"]:
            d["class"] = obs.PATH_ONLY
            d["flags"] = obs.O_PATH
        res = verdict(seen=seen)
        self.assertEqual(res["verdict"], join.CONFLICT)

    def test_foreign_opener_does_not_join(self):
        seen = observed()
        seen["snapshot"]["descriptors"].append(
            {"pid": 900, "fd": "3", "inode": 77, "flags": 0o2, "class": obs.READ_WRITE_BUFFERED})
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_daemon_without_descriptors_does_not_join(self):
        seen = observed()
        seen["snapshot"]["descriptors"] = []
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_borrow_validation_failure_conflicts(self):
        seen = observed()
        seen["borrow"] = borrow_record(borrowed=[{"validation": "inode-mismatch"}])
        self.assertEqual(verdict(seen=seen)["verdict"], join.CONFLICT)

    def test_denied_borrow_is_not_attachment_evidence(self):
        seen = observed()
        seen["borrow"] = borrow_record(permission="denied", borrowed=[])
        self.assertNotEqual(verdict(seen=seen)["verdict"], join.JOINED)

    def test_missing_process_start_in_borrow_is_unverified(self):
        for field in ("start_before", "start_after"):
            seen = observed()
            seen["borrow"] = borrow_record(**{field: None})
            self.assertNotEqual(verdict(seen=seen)["verdict"], join.JOINED)

    def test_non_dict_borrow_record_is_unverified(self):
        seen = observed()
        seen["borrow"] = "closed"
        self.assertEqual(verdict(seen=seen)["verdict"], join.UNVERIFIED)


if __name__ == "__main__":
    unittest.main()
