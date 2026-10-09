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
import test_scenario_quiescence as retained_source


FIXTURE = Path(__file__).with_name("fixtures") / "cephfs-recovery-native.log"
PROVENANCE = FIXTURE.with_suffix(".provenance.json")
JSON_FIXTURE = FIXTURE.with_name("cephfs-recovery-native-json.log")
JSON_PROVENANCE = JSON_FIXTURE.with_suffix(".provenance.json")
SOURCE_FIXTURE = FIXTURE.with_name("cephfs-recovery-native.source.go.txt")


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
        self.assertEqual(hashlib.sha256(SOURCE_FIXTURE.read_bytes()).hexdigest(),
                         provenance["original_test_file_sha256"])
        # Both independently retained native formats share this original
        # producer, rather than being rebound to today's split-file bytes.
        json_provenance = json.loads(JSON_PROVENANCE.read_text())
        self.assertEqual(json_provenance["source_sha"], provenance["source_sha"])
        self.assertEqual(json_provenance["original_test_file_sha256"],
                         provenance["original_test_file_sha256"])
        passes = re.findall(r"^\s*--- PASS: (\S+) ", FIXTURE.read_text(), re.M)
        self.assertEqual(passes[:3], [checker.PARENT, checker.PARENT + "/bridge", checker.PARENT + "/bridge/peer"])

    def test_split_producer_parent_and_shared_recovery_assertions_preserve_original_declarations(self):
        snapshot = SOURCE_FIXTURE.read_text()
        root = Path(__file__).resolve().parents[2] / "internal/integration"
        parent = (root / "cephfs_process_quiescence_acknowledgment_integration_test.go").read_text()
        helpers = (root / "cephfs_process_quiescence_acknowledgment_integration_helpers_test.go").read_text()
        self.assertEqual(retained_source.producer_parent(snapshot, checker.PARENT),
                         retained_source.producer_parent(parent, checker.PARENT))
        helper = "testCephFSOriginalProcessQuiescenceRecovery"
        self.assertEqual(retained_source.producer_helpers(snapshot, helper),
                         retained_source.producer_helpers(helpers, helper))

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


class RecoveryJSONCoverageTests(unittest.TestCase):
    def setUp(self):
        self.case = "process-recovery-bridge-peer"
        self.raw = JSON_FIXTURE.read_text()
        self.rows = [json.loads(line) for line in self.raw.splitlines()]

    def assert_rejected(self, rows):
        raw = "\n".join(json.dumps(row) for row in rows) + "\n"
        self.assertFalse(checker.validate_json(raw, self.case)["passed"])

    def test_actual_native_json_postorder_is_bound_and_format_modes_are_explicit(self):
        provenance = json.loads(JSON_PROVENANCE.read_text())
        self.assertEqual(hashlib.sha256(JSON_FIXTURE.read_bytes()).hexdigest(), provenance["fixture_sha256"])
        self.assertEqual(provenance["original_log_sha256"], "ca2e4d6145b9f0144b0f3634906d066ea76870e1adf293a2c23768ee1505dbb2")
        self.assertEqual(provenance["original_log_bytes"], 156031)
        self.assertEqual(provenance["native_exit_code"], 0)
        self.assertIn("-json", provenance["native_argv"])
        self.assertEqual(len(provenance["original_line_numbers"]), 42)
        self.assertEqual(provenance["original_line_numbers"], sorted(set(provenance["original_line_numbers"])))
        passes = [row["Test"] for row in self.rows if row["Action"] == "pass" and "Test" in row]
        leaf = checker.required_leaf(self.case)
        self.assertEqual(passes, [leaf, leaf.rsplit("/", 1)[0], checker.PARENT])
        report = checker.validate_json(self.raw, self.case)
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["pass_order"], "postorder")
        self.assertEqual(report["json_observed_passes"], passes)
        text = "".join(row.get("Output", "") for row in self.rows)
        self.assertFalse(checker.validate(text, self.case)["passed"])
        self.assertTrue(checker.validate(text, self.case, pass_order="postorder")["passed"])
        # The original plain producer still requires parent/network/leaf.
        plain = native_leaf_log(self.case)
        self.assertTrue(checker.validate(plain, self.case)["passed"])
        self.assertFalse(checker.validate(plain, self.case, pass_order="postorder")["passed"])
        self.assertFalse(checker.validate_json(plain, self.case)["passed"])
        with self.assertRaises(ValueError):
            checker.validate(text, self.case, pass_order="either")

    def test_every_structured_and_output_event_is_required_once(self):
        for index in range(len(self.rows)):
            with self.subTest(row=index):
                self.assert_rejected(self.rows[:index] + self.rows[index + 1:])
                self.assert_rejected(self.rows[:index] + [self.rows[index], self.rows[index]] + self.rows[index + 1:])

    def test_structured_fail_skip_foreign_identity_and_false_package_pass_are_rejected(self):
        lifecycle = [i for i, row in enumerate(self.rows) if row["Action"] != "output"]
        for index in lifecycle:
            for key, value in (("Action", "fail"), ("Action", "skip"),
                               ("Package", "example.com/foreign/package"), ("Time", "invalid"),
                               ("Test", checker.PARENT + "/host/peer")):
                rows = [row.copy() for row in self.rows]
                rows[index][key] = value
                self.assert_rejected(rows)
        pass_indices = [i for i, row in enumerate(self.rows) if row["Action"] == "pass"]
        for index in pass_indices:
            for elapsed in (True, -1, float("inf"), float("nan"), "0"):
                rows = [row.copy() for row in self.rows]
                rows[index]["Elapsed"] = elapsed
                self.assert_rejected(rows)
        # A final package PASS cannot substitute for unfinished ancestor PASS.
        package = self.rows[-1]
        for index in pass_indices[:-1]:
            self.assert_rejected(self.rows[:index] + [package] + self.rows[index:-1])
        rows = [row.copy() for row in self.rows]
        first, last = pass_indices[0], pass_indices[-2]
        rows[first], rows[last] = rows[last], rows[first]
        self.assert_rejected(rows)

    def test_output_test_attribution_and_native_proof_placement_remain_bound(self):
        outputs = [i for i, row in enumerate(self.rows) if row["Action"] == "output"]
        for index in outputs:
            rows = [row.copy() for row in self.rows]
            rows[index]["Test"] = checker.PARENT
            # Parent's own RUN/PASS already has that identity; choose a foreign
            # selected ancestor so attribution still changes for those rows.
            if rows[index] == self.rows[index]:
                rows[index]["Test"] = checker.required_leaf(self.case)
            self.assert_rejected(rows)
        proof = next(i for i, row in enumerate(self.rows) if checker.BYTES_MARKER in row.get("Output", ""))
        self.assert_rejected([self.rows[proof]] + self.rows[:proof] + self.rows[proof + 1:])
        self.assert_rejected(self.rows[:proof] + self.rows[proof + 1:] + [self.rows[proof]])
        for raw in ("", self.raw + "{}\n", self.raw + "null\n", "{broken\n" + self.raw,
                    self.raw.replace('"Action":"start"', '"Action":"start","Action":"pass"', 1)):
            if raw != self.raw:
                self.assertFalse(checker.validate_json(raw, self.case)["passed"])

    def test_cli_json_requires_explicit_format_and_complete_structured_history(self):
        for options, code in (([], 1), (["--format", "go-json"], 0)):
            result = subprocess.run([sys.executable, checker.__file__, str(JSON_FIXTURE), "--case", self.case, *options],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, code, result.stderr)
            self.assertEqual(json.loads(result.stdout)["passed"], code == 0)


if __name__ == "__main__":
    unittest.main()
