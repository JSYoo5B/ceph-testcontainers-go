"""Exercise the recovery gate against retained native output; never start Docker."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import check_scenario_recovery as checker
import check_scenario_quiescence as original_checker


FIXTURE = Path(__file__).with_name("fixtures") / "cephfs-recovery-native.log"
PROVENANCE = FIXTURE.with_suffix(".provenance.json")


def native_leaf_log(case):
    """Project actual producer rows in their original order, retaining durations.

    This parser control is not a newly executed independent shard. The fixture
    records the real aggregate job and the original full log's hash/line numbers.
    """
    leaf = checker.required_leaf(case)
    ancestors = {checker.PARENT, leaf.rsplit("/", 1)[0], leaf}
    selected = []
    for line in FIXTURE.read_text().splitlines():
        run = re.fullmatch(r"=== RUN\s+(\S+)", line)
        passed = re.fullmatch(r"\s*--- PASS: (\S+) \([0-9.]+s\)", line)
        marker = next((marker for marker in checker.MARKERS if marker in line), None)
        if run and run[1] in ancestors or passed and passed[1] in ancestors:
            selected.append(line)
        elif marker and json.loads(line.split(marker, 1)[1])["case"] == leaf:
            selected.append(line)
        elif line == "PASS" or re.fullmatch(r"ok\s+" + re.escape(original_checker.PACKAGE) + r"\s+[0-9.]+s", line):
            selected.append(line)
    return "\n".join(selected) + "\n"


def mutate_proof(log, marker, change, occurrence=0):
    lines = log.splitlines()
    index = [i for i, line in enumerate(lines) if marker in line][occurrence]
    prefix, raw = lines[index].split(marker, 1)
    proof = json.loads(raw)
    change(proof)
    lines[index] = prefix + marker + json.dumps(proof)
    return "\n".join(lines) + "\n"


class RecoveryCoverageTests(unittest.TestCase):
    def setUp(self):
        self.case = "process-recovery-bridge-peer"
        self.log = native_leaf_log(self.case)

    def assert_rejected(self, log):
        self.assertFalse(checker.validate(log, self.case)["passed"])

    def test_actual_producer_source_and_preorder_are_bound(self):
        provenance = json.loads(PROVENANCE.read_text())
        self.assertEqual(hashlib.sha256(FIXTURE.read_bytes()).hexdigest(), provenance["fixture_sha256"])
        self.assertEqual(provenance["source_sha"], "4da274413513d68d42acb5548869663c031538ea")
        self.assertEqual(provenance["run_id"], 37804783231)
        self.assertEqual(provenance["job_id"], 113415201234)
        self.assertEqual(len(provenance["original_line_numbers"]), 120)
        self.assertEqual(provenance["original_line_numbers"], sorted(set(provenance["original_line_numbers"])))
        native = Path(__file__).resolve().parents[2] / "internal/integration/cephfs_process_quiescence_acknowledgment_integration_test.go"
        self.assertEqual(hashlib.sha256(native.read_bytes()).hexdigest(), provenance["original_test_file_sha256"])
        passes = re.findall(r"^\s*--- PASS: (\S+) ", FIXTURE.read_text(), re.M)
        self.assertEqual(passes[:3], [checker.PARENT, checker.PARENT + "/bridge", checker.PARENT + "/bridge/peer"])

    def test_four_actual_leaf_views_require_complete_native_history(self):
        for case in checker.CASES:
            report = checker.validate(native_leaf_log(case), case)
            self.assertTrue(report["passed"], report)
            self.assertEqual(len(report["byte_proofs"]), 14)
            self.assertEqual(len(report["absence_proofs"]), 3)
            self.assertEqual(len(report["lost_reply_proofs"]), 1)
            self.assertEqual(len(report["acknowledgments"]), 5)
            self.assertEqual(len(report["no_remove_replay_proofs"]), 2)
            self.assertEqual(len(report["recovered_proofs"]), 1)

    def test_empty_aggregate_parent_only_or_foreign_leaf_cannot_pass(self):
        for log in ("", "PASS\nok\t" + original_checker.PACKAGE + "\t0.00s\n",
                    FIXTURE.read_text(), native_leaf_log("process-recovery-host-peer"),
                    native_leaf_log("process-recovery-bridge-directory"),
                    self.log.replace("/bridge/peer", "/bridge/peer/only-one-phase")):
            self.assert_rejected(log)

    def test_all_original_test_and_native_records_are_required_once_in_order(self):
        records = [line for line in self.log.splitlines() if line.startswith("=== RUN")
                   or "--- PASS:" in line or any(marker in line for marker in checker.MARKERS)]
        for record in records:
            for mutation in (self.log.replace(record + "\n", ""),
                             self.log.replace(record, record + "\n" + record)):
                self.assert_rejected(mutation)
        proofs = [line for line in records if any(marker in line for marker in checker.MARKERS)]
        for first, second in zip(proofs, proofs[1:]):
            # Swapping two different actual proof rows detects phase ordering
            # defects without generating a happy-path log from checker tuples.
            self.assert_rejected(self.log.replace(first, "SWAP").replace(second, first).replace("SWAP", second))
        self.assert_rejected(proofs[0] + "\n" + self.log.replace(proofs[0] + "\n", ""))

    def test_preorder_and_all_terminal_passes_must_precede_package_success(self):
        lines = self.log.splitlines()
        passes = [line for line in lines if "--- PASS:" in line]
        prefix = [line for line in lines if line not in passes and line != "PASS" and not line.startswith("ok")]
        self.assert_rejected("\n".join(prefix + list(reversed(passes)) + ["PASS", "ok\t" + original_checker.PACKAGE + "\t1.00s"]) + "\n")
        for ancestor in passes[:-1]:
            self.assert_rejected(self.log.replace("\nPASS\n", "\n").replace(ancestor + "\n", ancestor + "\nPASS\n"))
        for log in (self.log.replace("--- PASS:", "--- SKIP:", 1),
                    self.log.replace("--- PASS:", "--- FAIL:", 1),
                    self.log.replace("\nPASS\n", "\n"), self.log.replace("\nPASS\n", "\nPASS\nPASS\n"),
                    self.log.replace(original_checker.PACKAGE, "example.com/foreign/package"),
                    self.log + "ok\t" + original_checker.PACKAGE + "\t1.00s\n",
                    self.log + "FAIL\n", self.log + "panic: test timed out\n"):
            self.assert_rejected(log)

    def test_bytes_absence_and_lost_reply_are_native_proofs(self):
        for marker, changes in (
                (checker.BYTES_MARKER, (("case", checker.PARENT + "/host/peer"), ("filesystem", "other"),
                    ("directory", "/other"), ("snapshot", "other"), ("bytes", 1), ("bytes", True),
                    ("sha256", "0" * 64), ("stage", "unknown"), ("role", "other"))),
                (checker.ABSENCE_MARKER, (("absent", False), ("absent", 1), ("count", 1), ("count", True),
                    ("duration_ms", 9999), ("duration_ms", 10000.0), ("filesystem", "other"), ("role", "source"))),
                (checker.LOST_MARKER, (("receipt_returned", False), ("cause_preserved", False),
                    ("original_intent_lost_reply", False), ("reply_losses", 2), ("reply_losses", True),
                    ("original_removals", 2), ("kind", "directory")))):
            for key, value in changes:
                self.assert_rejected(mutate_proof(self.log, marker, lambda proof: proof.update({key: value})))
        for marker in checker.MARKERS:
            row = next(line for line in self.log.splitlines() if marker in line)
            prefix = row.split(marker, 1)[0] + marker
            for raw in ('{"case":"wrong","case":"wrong"}', '{"value":NaN}', "null", "[]", "{broken", "true"):
                self.assert_rejected(self.log.replace(row, prefix + raw))
            self.assert_rejected(self.log.replace(row, row + " {}"))
            self.assert_rejected(self.log.replace(row, row + checker.BYTES_MARKER + "{}"))

    def test_ack_guard_boundary_and_original_binding_cannot_be_replaced(self):
        for occurrence in range(5):
            for key, value in (("native_commands", -1), ("native_commands", True),
                               ("peer_removals", 2), ("directory_removals", 1), ("reply_losses", 0)):
                self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER,
                    lambda proof: proof.update({key: value}), occurrence))
            self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER,
                lambda proof: proof.update({"acknowledged": not proof["acknowledged"]}), occurrence))
        self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER, lambda proof: proof.update({"native_commands": 1})))
        self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER, lambda proof: proof.update({"native_commands": 0}), 2))
        for key, value in (("PeerID", "other"), ("Directory", "/daemon-a"), ("SourceFilesystem", "other"),
                           ("SourceFilesystemID", 2), ("DestinationFilesystemID", True),
                           ("PolicyRemoved", False), ("OriginalQuiescent", False), ("Daemons", {})):
            self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER,
                lambda proof: proof["observation"].update({key: value}), 2))
        for key, value in (("EngineID", "other"), ("ContainerID", "a" * 64), ("StartedAt", "invalid"),
                           ("StartedAt", "0001-01-01T00:00:00Z"), ("StartedAt", "2026-10-09T01:00:00Z"),
                           ("InstanceID", "999"), ("InstanceID", "18446744073709551616"),
                           ("OriginalTaskEnded", False), ("OriginalWatcherRetired", False),
                           ("OriginalQuiescent", False), ("Evidence", "exited"), ("Problem", "unavailable")):
            self.assert_rejected(mutate_proof(self.log, checker.ACK_MARKER,
                lambda proof: next(iter(proof["observation"]["Daemons"].values())).update({key: value}), 2))

    def test_recovery_requires_distinct_original_and_replacement_generations(self):
        for key, value in (("kind", "directory"), ("original_peer", "other"), ("recovered_peer", ""),
                           ("original_cid", "a" * 64), ("replacement_cid", "short"),
                           ("original_gid", "999"), ("replacement_gid", "0"),
                           ("original_removals", 2), ("reply_losses", True), ("original_intent_lost_reply", False)):
            self.assert_rejected(mutate_proof(self.log, checker.RECOVERED_MARKER,
                lambda proof: proof.update({key: value})))
        for replacement, old in (("recovered_peer", "original_peer"), ("replacement_cid", "original_cid"),
                                 ("replacement_gid", "original_gid")):
            self.assert_rejected(mutate_proof(self.log, checker.RECOVERED_MARKER,
                lambda proof: proof.update({replacement: proof[old]})))
        for snapshot in ("initial", "backlog", "fresh"):
            for key, value in (("ID", True), ("ID", 0), ("Name", "foreign")):
                self.assert_rejected(mutate_proof(self.log, checker.RECOVERED_MARKER,
                    lambda proof: proof[snapshot].update({key: value})))
        self.assert_rejected(mutate_proof(self.log, checker.RECOVERED_MARKER,
            lambda proof: proof["fresh"].update({"ID": proof["backlog"]["ID"]})))
        for key, value in (("original_removals", 2), ("kind", "directory"), ("stage", "unknown")):
            self.assert_rejected(mutate_proof(self.log, checker.REPLAY_MARKER, lambda proof: proof.update({key: value})))

    def test_unknown_case_and_cli_false_green_or_missing_log_fail(self):
        for case in ("all", "process-recovery", "process-recovery-bridge", "", "process-recovery-host-peer/phase"):
            with self.assertRaises(ValueError):
                checker.validate("", case)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.log"
            for log, code in ((self.log, 0), ("PASS\n", 1)):
                path.write_text(log)
                result = subprocess.run([sys.executable, checker.__file__, str(path), "--case", self.case],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertEqual(json.loads(result.stdout), checker.validate(log, self.case))
            missing = subprocess.run([sys.executable, checker.__file__, str(path.with_name("absent")), "--case", self.case],
                                     capture_output=True, text=True, timeout=10)
            self.assertEqual(missing.returncode, 1)
            self.assertFalse(json.loads(missing.stdout)["passed"])


if __name__ == "__main__":
    unittest.main()
