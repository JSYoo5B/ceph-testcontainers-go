"""Exercise the quiescence gate with retained native output; never start Docker."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import check_scenario_quiescence as checker


FIXTURE = Path(__file__).with_name("fixtures") / "cephfs-quiescence-native.log"
PROVENANCE = FIXTURE.with_suffix(".provenance.json")
SOURCE_FIXTURE = FIXTURE.with_name("cephfs-quiescence-native.source.go.txt")


def retained_declaration_layout(source):
    """Allow only the observed gofmt blank line between these type declarations.

    Every other byte in the compared declarations stays significant, including
    Go's newline-sensitive syntax and embedded native probe script literals.
    This source parity is independent of retained native execution provenance.
    """
    before = "type cephFSOriginalProcessBaseline struct{ startedAt, instanceID string }\n"
    after = "type cephFSOriginalProcessRawState struct {"
    return source.replace(before + "\n" + after, before + after, 1)


def producer_parent(source, parent):
    # These retained producer parents contain only their nested t.Run loops;
    # gofmt keeps the sole function-closing brace at column zero.
    found = re.findall(r"(?ms)^func " + re.escape(parent) + r"\(.*?^\}", source)
    if len(found) != 1:
        raise ValueError("retained source must contain its exact original parent once")
    return found[0]


def producer_helpers(source, helper):
    marker = "\nfunc " + helper + "("
    if source.count(marker) != 1:
        raise ValueError("retained source must contain its exact fixture helper once")
    return source.split(marker, 1)[1]


def native_leaf_log(case):
    """Select original producer lines, preserving their actual output order.

    The aggregate fixture's durations are retained verbatim. This parser control
    is not a newly executed one-leaf fixture or a runtime performance result.
    """
    leaf = checker.required_leaf(case)
    ancestors = {checker.PARENT, leaf.rsplit("/", 1)[0], leaf}
    selected, current = [], None
    for line in FIXTURE.read_text().splitlines():
        run = re.fullmatch(r"=== RUN\s+(\S+)", line)
        passed = re.fullmatch(r"\s*--- PASS: (\S+) \([0-9.]+s\)", line)
        if run:
            current = run[1]
            if current in ancestors:
                selected.append(line)
        elif passed:
            if passed[1] in ancestors:
                selected.append(line)
        elif checker.EVIDENCE_MARKER in line:
            if current == leaf:
                selected.append(line)
        elif any(marker in line for marker in (checker.BYTES_MARKER, checker.ABSENCE_MARKER)):
            marker = checker.BYTES_MARKER if checker.BYTES_MARKER in line else checker.ABSENCE_MARKER
            if json.loads(line.split(marker, 1)[1])["case"] == leaf:
                selected.append(line)
        elif line == "PASS" or re.fullmatch(r"ok\s+" + re.escape(checker.PACKAGE) + r"\s+[0-9.]+s", line):
            selected.append(line)
    return "\n".join(selected) + "\n"


def mutate_proof(log, marker, change):
    lines = log.splitlines()
    index = next(i for i, line in enumerate(lines) if marker in line)
    prefix, raw = lines[index].split(marker, 1)
    proof = json.loads(raw)
    change(proof)
    lines[index] = prefix + marker + json.dumps(proof)
    return "\n".join(lines) + "\n"


class QuiescenceCoverageTests(unittest.TestCase):
    def test_retained_producer_source_and_actual_preorder_are_bound(self):
        provenance = json.loads(PROVENANCE.read_text())
        self.assertEqual(hashlib.sha256(FIXTURE.read_bytes()).hexdigest(), provenance["fixture_sha256"])
        self.assertEqual(provenance["source_sha"], "3ac07fe9525e5442fb646fdd67a92d321e0bc89d")
        self.assertEqual(provenance["run_id"], 37766891332)
        self.assertEqual(len(provenance["original_line_numbers"]), 72)
        # Historical logs bind the actual historical producer, independently
        # of today's source layout. Keep the original provenance SHA intact.
        self.assertEqual(hashlib.sha256(SOURCE_FIXTURE.read_bytes()).hexdigest(),
                         provenance["original_test_file_sha256"])
        # Inspect the producer fixture, not a positive log generated from the
        # checker's expected tuple: actual Go PASS begins with the parent.
        passes = re.findall(r"^\s*--- PASS: (\S+) ", FIXTURE.read_text(), re.M)
        self.assertEqual(passes[0], checker.PARENT)
        self.assertEqual(passes[1], checker.PARENT + "/bridge")
        self.assertEqual(passes[2], checker.PARENT + "/bridge/peer")

    def test_split_producer_parent_and_shared_assertions_preserve_original_declarations(self):
        retained = SOURCE_FIXTURE.read_text()
        root = Path(__file__).resolve().parents[2] / "internal/integration"
        current_parent = (root / "cephfs_process_quiescence_integration_test.go").read_text()
        current_helpers = (root / "cephfs_process_quiescence_integration_helpers_test.go").read_text()
        self.assertEqual(producer_parent(retained, checker.PARENT),
                         producer_parent(current_parent, checker.PARENT))
        helper = "testCephFSOriginalProcessQuiescence"
        self.assertEqual(retained_declaration_layout(producer_helpers(retained, helper)),
                         retained_declaration_layout(producer_helpers(current_helpers, helper)))

    def test_declaration_binding_retains_newlines_literals_operators_and_assertions(self):
        source = 'func fixture() { value++; require(value == 2, "a b"); raw := `a\n b` }'
        gap = ("type cephFSOriginalProcessBaseline struct{ startedAt, instanceID string }\n"
               "type cephFSOriginalProcessRawState struct {")
        self.assertEqual(retained_declaration_layout(gap),
                         retained_declaration_layout(gap.replace("\ntype", "\n\ntype")))
        for changed in (source.replace("++", "+ +"), source.replace("== 2", "== 3"),
                        source.replace('"a b"', '"ab"'), source.replace("`a\n b`", "`a\nb`"),
                        source.replace("require", "ignore")):
            self.assertNotEqual(retained_declaration_layout(source), retained_declaration_layout(changed))
        self.assertNotEqual(retained_declaration_layout("return call()"),
                            retained_declaration_layout("return\ncall()"))

    def test_each_actual_leaf_view_has_eight_byte_three_absence_and_three_identity_records(self):
        leaves = []
        for case in checker.CASES:
            result = checker.validate(native_leaf_log(case), case)
            self.assertTrue(result["passed"], result)
            self.assertEqual(len(result["counts"]), 3)
            self.assertTrue(all(count == {"run": 1, "pass": 1} for count in result["counts"].values()))
            self.assertEqual(len(result["byte_proofs"]), 8)
            self.assertEqual(len(result["absence_proofs"]), 3)
            self.assertEqual(len(result["original_process_evidence"]), 3)
            leaves.append(result["required_leaf"])
        self.assertEqual(len(set(leaves)), 4)

    def test_empty_parent_only_or_aggregate_log_is_not_a_completed_selected_fixture(self):
        for log in ("", "PASS\nok\t" + checker.PACKAGE + "\t0.00s\n",
                    "=== RUN   " + checker.PARENT + "\n--- PASS: " + checker.PARENT +
                    " (0.00s)\nPASS\nok\t" + checker.PACKAGE + "\t0.00s\n", FIXTURE.read_text()):
            self.assertFalse(checker.validate(log, "process-quiescence-bridge-peer")["passed"])

    def test_missing_duplicate_skipped_failed_or_foreign_test_events_are_rejected(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        events = [line for line in original.splitlines() if re.fullmatch(r"=== RUN\s+\S+", line)
                  or re.fullmatch(r"\s*--- PASS: \S+ \([0-9.]+s\)", line)]
        for event in events:
            for label, log in (("missing", original.replace(event + "\n", "")),
                               ("duplicate", original.replace(event, event + "\n" + event)),
                               ("foreign suffix", original.replace(event, event.replace(checker.PARENT, checker.PARENT + "Other")))):
                with self.subTest(event=event, mutation=label):
                    self.assertFalse(checker.validate(log, case)["passed"])
        for replacement in ("SKIP", "FAIL"):
            self.assertFalse(checker.validate(original.replace("--- PASS:", "--- " + replacement + ":", 1), case)["passed"])
        for log in (native_leaf_log("process-quiescence-host-peer"),
                    native_leaf_log("process-quiescence-bridge-directory"),
                    original.replace("/bridge/peer", "/bridge/peer/deeper-filter")):
            self.assertFalse(checker.validate(log, case)["passed"])

    def test_actual_preorder_and_package_completion_cannot_be_reordered_or_substituted(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        lines = original.splitlines(); results = [line for line in lines if "--- PASS:" in line]
        postorder = "\n".join(line for line in lines if line not in results and line != "PASS" and not line.startswith("ok"))
        postorder += "\n" + "\n".join(reversed(results)) + "\nPASS\nok\t" + checker.PACKAGE + "\t1.00s\n"
        self.assertFalse(checker.validate(postorder, case)["passed"])
        # A parent PASS does not finish its buffered network/leaf results.
        # Move package completion after each ancestor, reproducing the
        # actual producer preorder that exposed the receiver regression.
        incomplete = original.replace("\nPASS\n", "\n")
        for ancestor in results[:-1]:
            with self.subTest(early_package_after=ancestor):
                log = incomplete.replace(ancestor + "\n", ancestor + "\nPASS\n")
                self.assertFalse(checker.validate(log, case)["passed"])
        for log in (results[-1] + "\n" + original.replace(results[-1] + "\n", ""),
                    "PASS\n" + original.replace("\nPASS\n", "\n"),
                    original.replace("\nPASS\n", "\n"), original.replace("\nPASS\n", "\nPASS\nPASS\n"),
                    original.replace(checker.PACKAGE, "example.com/foreign/package"),
                    original + "ok\texample.com/foreign/package\t1.00s\n",
                    original + "ok\t" + checker.PACKAGE + "\t1.00s\n",
                    re.sub(r"^ok\s+.*$", "", original, flags=re.M),
                    original + "FAIL\n", original + "panic: test timed out\n"):
            self.assertFalse(checker.validate(log, case)["passed"])

    def test_every_native_proof_record_is_required_once_in_the_original_phase_order(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        records = [line for line in original.splitlines() if any(marker in line for marker in
                   (checker.BYTES_MARKER, checker.ABSENCE_MARKER, checker.EVIDENCE_MARKER))]
        for record in records:
            for log in (original.replace(record + "\n", ""), original.replace(record, record + "\n" + record)):
                self.assertFalse(checker.validate(log, case)["passed"])
        self.assertFalse(checker.validate(original.replace("later-run", "temporary").replace("exited", "later-run").replace("temporary", "exited"), case)["passed"])
        first = records[0] + "\n"
        self.assertFalse(checker.validate(first + original.replace(first, ""), case)["passed"])

    def test_positive_bytes_and_native_json_must_match_the_original_leaf_and_payload(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        for key, value in (("case", checker.PARENT + "/host/peer"), ("filesystem", ""),
                           ("directory", "/other"), ("snapshot", "after-original-process-quiescence"),
                           ("stage", "missing"), ("role", "other"), ("bytes", 1), ("bytes", True),
                           ("sha256", "0" * 64)):
            log = mutate_proof(original, checker.BYTES_MARKER, lambda proof: proof.update({key: value}))
            self.assertFalse(checker.validate(log, case)["passed"])
        for suffix in ("null", "{}", "[]", "{broken", '{"case":"wrong","case":"wrong"}',
                       ' {"bytes":NaN}', 'true', '123'):
            line = next(line for line in original.splitlines() if checker.BYTES_MARKER in line)
            self.assertFalse(checker.validate(original.replace(line, line.split(checker.BYTES_MARKER)[0] + checker.BYTES_MARKER + suffix), case)["passed"])
        marker = next(line for line in original.splitlines() if checker.BYTES_MARKER in line)
        self.assertFalse(checker.validate(original.replace(marker, marker + " {}"), case)["passed"])

    def test_short_or_unmeasured_absence_and_changed_original_filesystem_are_rejected(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        for key, value in (("case", checker.PARENT + "/bridge/directory"), ("filesystem", "other"),
                           ("directory", "/other"), ("snapshot", "before-original-process-removal"),
                           ("role", "source"), ("stage", "initial"), ("absent", False), ("absent", 1),
                           ("count", 1), ("count", True), ("count", 2.0),
                           ("duration_ms", 9999), ("duration_ms", True), ("duration_ms", 10000.0)):
            log = mutate_proof(original, checker.ABSENCE_MARKER, lambda proof: proof.update({key: value}))
            self.assertFalse(checker.validate(log, case)["passed"])
        rows = [line for line in original.splitlines() if checker.BYTES_MARKER in line]
        for row in rows[2:]:
            self.assertFalse(checker.validate(original.replace(row, row.replace('"tc-cephfs"', '"replaced-fs"')), case)["passed"])

    def test_process_binding_cannot_change_between_the_three_original_witnesses(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        records = [line for line in original.splitlines() if checker.EVIDENCE_MARKER in line]
        match = checker.EVIDENCE.search(records[1])
        for old, new in (("receipt=peer", "receipt=directory"), ("engine=" + match[3], "engine=other-engine"),
                         ("CID=" + match[4], "CID=" + "a" * 64), ("CID=" + match[4], "CID=short"),
                         ("StartedAt=" + match[5], "StartedAt=2026-10-09T01:00:00Z"),
                         ("StartedAt=" + match[5], "StartedAt=invalid"),
                         ("StartedAt=" + match[5], "StartedAt=0001-01-01T00:00:00Z"),
                         ("GID=" + match[6], "GID=0"), ("GID=" + match[6], "GID=01"),
                         ("GID=" + match[6], "GID=18446744073709551616"),
                         ("GID=" + match[6], "GID=999"),
                         ("pending gate retained", "pending gate released")):
            self.assertFalse(checker.validate(original.replace(records[1], records[1].replace(old, new)), case)["passed"])

    def test_same_invalid_timestamp_cannot_become_a_valid_process_binding(self):
        case = "process-quiescence-bridge-peer"
        original = native_leaf_log(case)
        for timestamp in ("2026-10-08T11:41:03+01:99", "2026-10-08T11:41:03+00:60",
                          "2026-10-08T11:41:03+24:00", "0001-01-01T01:00:00+01:00",
                          "0001-01-01T00:00:00.000000000Z"):
            with self.subTest(timestamp=timestamp):
                log = re.sub(r"StartedAt=\S+", "StartedAt=" + timestamp, original)
                self.assertFalse(checker.validate(log, case)["passed"])
        self.assertTrue(checker.valid_started_at("0001-01-01T00:00:00.000000001Z"))
        self.assertTrue(checker.valid_started_at("2026-10-08T11:41:03.123456789+23:59"))

    def test_unknown_case_is_not_an_aggregate_or_a_partial_filter(self):
        for case in ("all", "process-quiescence", "", "process-quiescence-bridge", "process-quiescence-host-peer/phase"):
            with self.assertRaises(ValueError):
                checker.validate("", case)

    def test_cli_emits_receipt_and_nonzero_for_false_green_or_missing_log(self):
        case = "process-quiescence-bridge-peer"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.log"
            for log, code in ((native_leaf_log(case), 0), ("PASS\n", 1)):
                path.write_text(log)
                result = subprocess.run([sys.executable, checker.__file__, str(path), "--case", case],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertEqual(json.loads(result.stdout), checker.validate(log, case))
            missing = subprocess.run([sys.executable, checker.__file__, str(path.with_name("absent")),
                                      "--case", case], capture_output=True, text=True, timeout=10)
            self.assertEqual(missing.returncode, 1)
            self.assertFalse(json.loads(missing.stdout)["passed"])


if __name__ == "__main__":
    unittest.main()
