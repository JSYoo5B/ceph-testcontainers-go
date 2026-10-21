"""Synthetic schema/negative controls; these tests are not native receipts."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import messenger_evidence as m


class MessengerEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in m.SOURCES:
            dst = self.root / name
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes((m.BASE / name).read_bytes())
        self.reference, _, self.bindings = m.catalogue(self.root)
        self.case = "TestTargetedCommandRecipe"
        self.fsid = "72b93a09-2a5b-4665-99a6-1047d3b81ec9"
        self.expected = {"release": "20.2.4", "platform": "linux/amd64", "images": ["sha256:" + "0" * 64]}
        self.parent = {
            "fsid": self.fsid, "osdmap_epoch": 9, "pgid": "1.0", "acting": [0, 1],
            "pg": {"pgid": "1.0", "acting": [0, 1], "epoch": 9},
            "mds": {"cluster_fsid": self.fsid, "id": 123, "mdsmap_epoch": 3, "state": "up:active", "fs_name": "test"},
            "client_refusals": {"osd.99": -2, "pg 99.0": -2}, "unknown_command": {"ret": -22},
            "paused": {"ret": -110}, "stopped": {"ret": -6},
            "server_refusals": {"librados": {"osd0": {"ret": -13}, "pg": {"ret": -13}}, "cli": [{"exit": 13}, {"exit": 13}]}}
        for osd in (0, 1):
            self.parent["osd." + str(osd)] = {"cluster_fsid": self.fsid, "osd_fsid": "2f946a2b-e3c7-4b23-a4e0-135e6076d84" + str(osd), "whoami": osd, "newest_map": 9}
        self.receipt = {"schema": "ceph-messenger-case/v1", "binding": "targeted-command/native-identities", "case": self.case,
                        "run_id": "32d3096e-df69-4d30-859f-c2fd97b76c04", "generation": self.fsid, "cleanup": "passed",
                        "sources": m.source_digests(self.root),
                        "cohort": dict(self.expected, ceph_version="ceph version 20.2.4 (" + self.reference["ceph_commit"] + ") tentacle (stable)", go_version="go1.26.9")}

    def event(self, action, test=None, **extra):
        result = {"Action": action, "Package": m.PACKAGE}
        if test:
            result["Test"] = test
        return result | extra

    def events(self):
        parent = json.dumps(self.parent, separators=(",", ":"))
        self.receipt["parent_sha256"] = m.digest(parent.encode())
        return [self.event("run", self.case),
                self.event("output", self.case, Output="case.go:1: TARGETED_COMMAND_RECEIPT " + parent + "\n"),
                self.event("output", self.case, Output="case.go:2: MSGR_CASE_RECEIPT " + json.dumps(self.receipt) + "\n"),
                self.event("pass", self.case), self.event("pass")]

    def check(self, events=None):
        raw = b"\n".join(json.dumps(e).encode() for e in (self.events() if events is None else events))
        return m.report(self.root, raw, self.expected)

    def reject(self, events=None):
        with self.assertRaises(m.EvidenceError):
            self.check(events)

    def test_complete_synthetic_case(self):
        result = self.check()
        self.assertEqual(len(result["native_cases"]), 1)
        self.assertEqual(result["source_declarations"], 357)
        self.assertEqual(result["mapped_declarations"], 2)
        self.assertEqual(len(result["unmapped_declarations"]), 355)
        self.assertFalse(result["wire_coverage_claimed"])
        self.assertFalse(result["consumer_implementation_claimed"])

    def test_unexecuted_is_not_covered(self):
        result = m.report(self.root)
        self.assertEqual(result["native_cases"], [])
        self.assertEqual(result["unexecuted_bindings"], list(self.bindings))

    def test_split_output_reassembles(self):
        events = self.events()
        line = events[2]["Output"]
        events[2:3] = [self.event("output", self.case, Output=line[:150]), self.event("output", self.case, Output=line[150:])]
        self.check(events)

    def test_missing_case_marker(self):
        events = self.events(); del events[2]; self.reject(events)

    def test_missing_parent(self):
        events = self.events(); del events[1]; self.reject(events)

    def test_duplicate_marker(self):
        events = self.events(); events.insert(3, events[2]); self.reject(events)

    def test_missing_test_pass(self):
        events = self.events(); del events[3]; self.reject(events)

    def test_missing_package_pass(self): self.reject(self.events()[:-1])
    def test_repeated_run(self): self.reject(self.events() + self.events())

    def test_package_pass_before_case_completion(self):
        events = self.events(); events[3], events[4] = events[4], events[3]; self.reject(events)


    def test_failed_cleanup_after_marker(self):
        events = self.events(); events[3]["Action"] = "fail"; self.reject(events)

    def test_post_pass_output(self):
        events = self.events(); events.insert(4, events[2]); self.reject(events)

    def test_case_receipt_before_parent(self):
        events = self.events(); events[1], events[2] = events[2], events[1]; self.reject(events)

    def test_truncated_json(self):
        events = self.events(); events[2]["Output"] = events[2]["Output"][:-10]; self.reject(events)

    def test_duplicate_json_key(self):
        with self.assertRaises(m.EvidenceError): m.loads('{"schema": 1, "schema": 2}')

    def test_nonfinite_json(self):
        with self.assertRaises(m.EvidenceError): m.loads('{"value": NaN}')

    def test_wrong_generation(self):
        self.receipt["generation"] = "2f946a2b-e3c7-4b23-a4e0-135e6076d840"; self.reject()

    def test_wrong_osd_identity(self):
        self.parent["osd.0"]["cluster_fsid"] = "other"; self.reject()

    def test_wrong_placement(self):
        self.parent["pg"]["acting"] = [1, 0]; self.reject()

    def test_wrong_mds_generation(self):
        self.parent["mds"]["id"] = 0; self.reject()

    def test_missing_auth_refusal(self):
        self.parent["server_refusals"]["librados"]["pg"]["ret"] = 0; self.reject()

    def test_missing_native_failure(self):
        self.parent["paused"]["ret"] = 0; self.reject()

    def test_changed_compiled_source(self):
        self.receipt["sources"]["internal/integration/*.go"] = "0" * 64; self.reject()

    def test_wrong_cohort(self):
        self.receipt["cohort"]["platform"] = "darwin/arm64"; self.reject()

    def test_wrong_image(self):
        self.receipt["cohort"]["images"] = ["sha256:" + "1" * 64]; self.reject()

    def test_wrong_native_build(self):
        self.receipt["cohort"]["ceph_version"] = "ceph version 20.2.4 (other)"; self.reject()

    def test_wrong_binding(self):
        self.receipt["binding"] = "unregistered"; self.reject()

    def test_incomplete_cleanup(self):
        self.receipt["cleanup"] = "pending"; self.reject()

    def test_unknown_fields(self):
        self.receipt["unverified"] = True; self.reject()

    def test_missing_parent_digest(self):
        events = self.events(); receipt = copy.deepcopy(self.receipt); receipt["parent_sha256"] = "0" * 64
        events[2]["Output"] = "MSGR_CASE_RECEIPT " + json.dumps(receipt) + "\n"; self.reject(events)

    def alter_binding(self, edit):
        path = self.root / m.BINDINGS; data = m.loads(path.read_bytes()); edit(data)
        path.write_text(json.dumps(data))
        with self.assertRaises(m.EvidenceError): m.catalogue(self.root)

    def test_unknown_declaration(self):
        self.alter_binding(lambda d: d["bindings"][0]["declarations"][0].update(symbol="MSG_UNKNOWN"))

    def test_duplicate_declaration_link(self):
        self.alter_binding(lambda d: d["bindings"][0]["declarations"].append(d["bindings"][0]["declarations"][0]))

    def test_duplicate_binding(self):
        self.alter_binding(lambda d: d["bindings"].append(d["bindings"][0]))

    def test_stale_case(self):
        self.alter_binding(lambda d: d["bindings"][0].update(case="TestNoSuchCase"))

    def test_unknown_requirement(self):
        self.alter_binding(lambda d: d["bindings"][0].update(requirement="missing"))

    def test_stale_reference(self):
        self.alter_binding(lambda d: d.update(ceph_commit="0" * 40))

    def test_wire_elevation_rejected(self):
        self.alter_binding(lambda d: d["bindings"][0].update(evidence="wire"))

    def test_cli_requires_execution(self):
        run = subprocess.run([sys.executable, str(m.BASE / ".github/scripts/messenger_evidence.py"), "--root", str(self.root), "--require-execution"], capture_output=True, text=True)
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("no native execution", run.stderr)


if __name__ == "__main__":
    unittest.main()
