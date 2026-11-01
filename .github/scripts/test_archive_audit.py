import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import archive_audit

# The tree is the real output of the finalization producer with a writer that
# keeps the first 17 bytes of diagnostics.json and then returns a no space
# error (see ceph/finalize/finalization.go writeReport).
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "finalization-partial-write"
# Real producer output with four raw capture copies (PR #118 contract): a valid
# identity, a same-size wrong digest, a read error after all bytes and a
# selection limit exceeded with 0 bytes. Only the first leaves a final file.
CAPTURE_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "finalization-capture-files"
VALID = "capture-1-valid.pcap"
WRONG_DIGEST = "capture-2-wrong-digest.pcap"
READ_ERROR = "capture-3-read-error.pcap"
OVER_LIMIT = "capture-4-over-limit.pcap"


class Archive(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "archive"
        shutil.copytree(FIXTURE, self.root)

    def manifest(self, change=None):
        path = self.root / "manifest.json"
        data = json.loads(path.read_text())
        if change:
            change(data)
            path.write_text(json.dumps(data))
        return data

    def files(self):
        return {r["name"]: r for r in archive_audit.audit(self.root)["files"]}


class ProducerPartialWrite(Archive):
    def test_real_partial_write_is_kept_apart(self):
        report = archive_audit.audit(self.root)
        diagnostics = self.files()["diagnostics.json"]
        self.assertEqual(diagnostics["state"], archive_audit.PRODUCER_PARTIAL)
        self.assertEqual((diagnostics["declared_bytes"], diagnostics["retained_bytes"]), (0, 17))
        self.assertEqual(diagnostics["producer_write"], "failed")
        self.assertTrue(diagnostics["available"])
        self.assertIs(diagnostics["json_readable"], False)
        self.assertEqual(len(diagnostics["sha256"]), 64)
        crash = self.files()["crash-metadata.json"]
        self.assertEqual((crash["state"], crash["producer_write"]), (archive_audit.INTACT, "succeeded"))
        self.assertEqual(report["archive"], {"declared_complete": False, "final": True,
                                             "termination_recorded": True, "cleanup_failures": []})
        self.assertFalse(report["evidence_complete"])
        self.assertIn("diagnostics.json: producer_declared_partial", report["findings"])
        self.assertNotIn("diagnostics.json: size_mismatch", report["findings"])

    def test_final_does_not_upgrade_the_partial_archive(self):
        self.assertTrue(self.manifest()["final"])
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])

    def test_failed_write_without_a_file(self):
        (self.root / "diagnostics.json").unlink()
        record = self.files()["diagnostics.json"]
        self.assertEqual(record["state"], archive_audit.PRODUCER_FAILED_ABSENT)
        self.assertFalse(record["available"])
        self.assertIsNone(record["sha256"])

    def test_retained_fragment_is_hashed_not_diagnosed_as_corruption(self):
        (self.root / "diagnostics.json").write_bytes(b"x" * 5000)
        record = self.files()["diagnostics.json"]
        self.assertEqual(record["state"], archive_audit.PRODUCER_PARTIAL)
        self.assertEqual(record["retained_bytes"], 5000)


class SuccessfulWrites(Archive):
    def test_exact_size_and_readable_json_is_intact(self):
        record = self.files()["crash-metadata.json"]
        self.assertEqual(record["state"], archive_audit.INTACT)
        self.assertEqual(record["retained_bytes"], record["declared_bytes"])

    def test_missing_successful_file(self):
        (self.root / "crash-metadata.json").unlink()
        self.assertEqual(self.files()["crash-metadata.json"]["state"], archive_audit.MISSING)

    def test_altered_size_is_a_mismatch(self):
        path = self.root / "crash-metadata.json"
        path.write_bytes(path.read_bytes() + b" ")
        self.assertEqual(self.files()["crash-metadata.json"]["state"], archive_audit.SIZE_MISMATCH)

    def test_same_size_but_unreadable_json(self):
        path = self.root / "crash-metadata.json"
        size = path.stat().st_size
        path.write_bytes(b"{" * size)
        record = self.files()["crash-metadata.json"]
        self.assertEqual(record["state"], archive_audit.UNREADABLE_JSON)
        self.assertEqual(record["retained_bytes"], size)

    def test_altered_bytes_change_the_receipt_hash(self):
        before = self.files()["crash-metadata.json"]["sha256"]
        path = self.root / "crash-metadata.json"
        path.write_bytes(path.read_bytes().replace(b"osd", b"mds", 1))
        self.assertNotEqual(self.files()["crash-metadata.json"]["sha256"], before)

    def test_complete_archive_is_complete_only_with_every_part(self):
        def complete(data):
            data["archive"]["complete"] = True
            data["files"][0] = {"name": "diagnostics.json", "bytes": 17}
        self.manifest(complete)
        (self.root / "diagnostics.json").write_text(json.dumps({"a": 1}))
        data = self.manifest(lambda d: d["files"][0].update(bytes=(self.root / "diagnostics.json").stat().st_size))
        report = archive_audit.audit(self.root)
        self.assertTrue(report["evidence_complete"], report)
        self.assertEqual(report["findings"], [])
        # Each requirement fails on its own.
        for name, change in {
            "declared incomplete": lambda d: d["archive"].update(complete=False),
            "not final": lambda d: d.update(final=False),
            "no termination": lambda d: d.pop("termination"),
        }.items():
            with self.subTest(name):
                self.manifest(change)
                self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])
                self.manifest(lambda d: d.update(archive=data["archive"], final=True, termination=data["termination"]))
        self.assertTrue(archive_audit.audit(self.root)["evidence_complete"])


class Safety(Archive):
    def test_names_cannot_escape_or_follow_links(self):
        outside = Path(self.directory.name) / "outside.json"
        outside.write_text("{}")
        for name in ["../outside.json", "sub/x.json", "..", ""]:
            with self.subTest(name):
                self.manifest(lambda d: d["files"].append({"name": name, "bytes": 2}))
                self.assertEqual(self.files()[name]["state"], archive_audit.UNSAFE)
                self.manifest(lambda d: d["files"].pop())
        (self.root / "link.json").symlink_to(outside)
        self.manifest(lambda d: d["files"].append({"name": "link.json", "bytes": 2}))
        record = self.files()["link.json"]
        self.assertEqual(record["state"], archive_audit.UNSAFE)
        self.assertIsNone(record["sha256"])

    def test_malformed_entries_are_not_evidence(self):
        for entry in [{"name": "crash-metadata.json"}, {"name": "crash-metadata.json", "bytes": "693"},
                      {"name": "crash-metadata.json", "bytes": True},
                      {"name": "crash-metadata.json", "bytes": 693, "error": None}]:
            with self.subTest(entry):
                self.manifest(lambda d: d["files"].__setitem__(1, entry))
                self.assertEqual(self.files()["crash-metadata.json"]["state"], archive_audit.UNSAFE)

    def test_invalid_path_names_are_findings_not_crashes(self):
        names = ["a\0b", "\0", "x" * 256, "\u00e9" * 128, "\ud800"]
        for name in names:
            with self.subTest(repr(name)):
                shutil.copy(FIXTURE / "manifest.json", self.root / "manifest.json")
                data = json.loads((self.root / "manifest.json").read_text())
                data["files"].append({"name": name, "bytes": 2})
                (self.root / "manifest.json").write_text(json.dumps(data))  # escapes NUL and surrogates
                report = archive_audit.audit(self.root)
                self.assertEqual(report["files"][-1]["state"], archive_audit.UNSAFE)
                self.assertFalse(report["evidence_complete"])
                output = Path(self.directory.name) / "receipt.json"
                result = subprocess.run([sys.executable, str(Path(archive_audit.__file__)), str(self.root),
                                         "--output", str(output)], capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(output.read_text())["files"][-1]["state"], archive_audit.UNSAFE)

    def test_non_string_names_are_findings_not_crashes(self):
        for name in [[], {}, 7, None, ["a"], {"a": 1}]:
            with self.subTest(name):
                shutil.copy(FIXTURE / "manifest.json", self.root / "manifest.json")
                self.manifest(lambda d: d["files"].append({"name": name, "bytes": 2}))
                report = archive_audit.audit(self.root)
                self.assertEqual(report["files"][-1]["state"], archive_audit.UNSAFE)
                self.assertIn("%s: unsafe_path" % (name,), report["findings"])
                self.assertFalse(report["evidence_complete"])
                output = Path(self.directory.name) / "receipt.json"
                archive_audit.main([str(self.root), "--output", str(output)])
                self.assertEqual(json.loads(output.read_text())["files"][-1]["state"], archive_audit.UNSAFE)

    def test_unreadable_or_untyped_manifest(self):
        (self.root / "manifest.json").write_text("{")
        report = archive_audit.audit(self.root)
        self.assertFalse(report["manifest"]["readable"])
        self.assertFalse(report["evidence_complete"])
        for change in [lambda d: d.pop("files"), lambda d: d["archive"].pop("complete"),
                       lambda d: d.update(final="true"), lambda d: d.update(archive=None)]:
            shutil.copy(FIXTURE / "manifest.json", self.root / "manifest.json")
            self.manifest(change)
            self.assertFalse(archive_audit.audit(self.root)["manifest"]["readable"])

    def test_unlisted_files_are_reported(self):
        (self.root / "extra.json").write_text("{}")
        self.assertEqual(archive_audit.audit(self.root)["unlisted_files"], ["extra.json"])

    def test_cleanup_failures_stay_separate(self):
        self.manifest(lambda d: d.update(cleanups=[{"name": "restore", "error": "boom"}]))
        report = archive_audit.audit(self.root)
        self.assertEqual(report["archive"]["cleanup_failures"], ["restore"])
        self.assertEqual(self.files()["crash-metadata.json"]["state"], archive_audit.INTACT)


class CaptureArchive(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "archive"
        shutil.copytree(CAPTURE_FIXTURE, self.root)

    def manifest(self, change):
        path = self.root / "manifest.json"
        data = json.loads(path.read_text())
        change(data)
        path.write_text(json.dumps(data))
        return data

    def record(self, name, change):
        def apply(data):
            change([r for r in data["capture_files"] if r["name"] == name][0])
        self.manifest(apply)

    def captures(self):
        return {r["name"]: r for r in archive_audit.audit(self.root)["capture_files"]}

    def state(self, name):
        return self.captures()[name]["state"]


class CaptureJoin(CaptureArchive):
    def test_real_producer_cases(self):
        report = archive_audit.audit(self.root)
        captures = self.captures()
        valid = captures[VALID]
        self.assertEqual((valid["state"], valid["producer_copy"], valid["downloaded"]),
                         (archive_audit.VERIFIED, "copied", True))
        self.assertEqual((valid["retained_bytes"], valid["reported_bytes"], valid["expected_bytes"]), (22, 22, 22))
        for name, error, reported in [(WRONG_DIGEST, "capture source size or digest changed", 22),
                                      (READ_ERROR, "capture source read failed", 22),
                                      (OVER_LIMIT, "capture file byte limit exceeded", 0)]:
            with self.subTest(name):
                record = captures[name]
                # An explained omission: the atomic copy failed, so no final file.
                self.assertEqual((record["state"], record["producer_copy"], record["downloaded"]),
                                 (archive_audit.PRODUCER_FAILED_ABSENT, "failed", False))
                self.assertEqual((record["producer_error"], record["reported_bytes"]), (error, reported))
        self.assertEqual(report["unrecorded_capture_files"], [])
        self.assertEqual(report["unlisted_files"], [])

    def test_raw_capture_is_not_parsed_as_json(self):
        files = {r["name"]: r for r in archive_audit.audit(self.root)["files"]}
        self.assertEqual(files[VALID]["state"], archive_audit.INTACT)
        self.assertIsNone(files[VALID]["json_readable"])
        # The metadata JSON beside it is still checked as JSON.
        self.assertIs(files["capture-1-valid.json"]["json_readable"], True)

    def test_successful_copy_does_not_upgrade_an_incomplete_archive(self):
        report = archive_audit.audit(self.root)
        self.assertTrue(report["archive"]["final"])
        self.assertFalse(report["archive"]["declared_complete"])
        self.assertFalse(report["evidence_complete"])

    def test_three_dimensions_stay_separate(self):
        # Producer outcome, downloaded availability and archive completeness
        # are independent: a failed copy can be absent in a complete-declared
        # archive and it still blocks evidence.
        self.manifest(lambda d: d["archive"].update(complete=True))
        report = archive_audit.audit(self.root)
        failed = self.captures()[WRONG_DIGEST]
        self.assertEqual((failed["producer_copy"], failed["downloaded"], report["archive"]["declared_complete"]),
                         ("failed", False, True))
        self.assertFalse(report["evidence_complete"])

    def test_complete_only_when_every_capture_is_verified(self):
        failed = {WRONG_DIGEST, READ_ERROR, OVER_LIMIT}

        def keep_valid_only(data):
            data["archive"]["complete"] = True
            data["capture_files"] = [c for c in data["capture_files"] if c["name"] not in failed]
            data["files"] = [f for f in data["files"] if f["name"] not in failed
                             and f["name"].replace(".json", ".pcap") not in failed]
        self.manifest(keep_valid_only)
        for name in failed:
            (self.root / name.replace(".pcap", ".json")).unlink()
        report = archive_audit.audit(self.root)
        self.assertTrue(report["evidence_complete"], report["findings"])
        (self.root / VALID).write_bytes(b"y" * 22)
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])
        shutil.copy(CAPTURE_FIXTURE / VALID, self.root / VALID)
        self.assertTrue(archive_audit.audit(self.root)["evidence_complete"])
        # An extra retained capture file alone blocks completeness.
        (self.root / "capture-9-extra.pcap").write_bytes(b"e")
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])
        (self.root / "capture-9-extra.pcap").unlink()
        # So does a capture record whose files entry is gone.
        self.manifest(lambda d: d.update(files=[f for f in d["files"] if f["name"] != VALID]))
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])

    def test_absent_failed_copy_with_staged_bytes_is_not_upload_loss(self):
        record = self.captures()[READ_ERROR]
        self.assertGreater(record["reported_bytes"], 0)
        self.assertEqual(record["state"], archive_audit.PRODUCER_FAILED_ABSENT)
        self.assertNotIn(archive_audit.MISSING, [r["state"] for r in archive_audit.audit(self.root)["files"]])


class CaptureIdentity(CaptureArchive):
    def test_missing_downloaded_file(self):
        (self.root / VALID).unlink()
        self.assertEqual(self.state(VALID), archive_audit.MISSING)
        self.assertFalse(self.captures()[VALID]["downloaded"])

    def test_size_disagreement(self):
        (self.root / VALID).write_bytes(b"short")
        self.assertEqual(self.state(VALID), archive_audit.SIZE_MISMATCH)

    def test_same_size_different_bytes(self):
        (self.root / VALID).write_bytes(b"y" * 22)
        self.assertEqual(self.state(VALID), archive_audit.DIGEST_MISMATCH)

    def test_copied_and_expected_size_must_agree(self):
        self.record(VALID, lambda r: r.update(expected_bytes=23))
        self.assertEqual(self.state(VALID), archive_audit.SIZE_MISMATCH)

    def test_copied_and_expected_digest_must_agree(self):
        self.record(VALID, lambda r: r.update(expected_sha256="0" * 64))
        self.assertEqual(self.state(VALID), archive_audit.DIGEST_MISMATCH)

    def test_files_entry_must_agree_with_the_capture_record(self):
        self.manifest(lambda d: [f.update(bytes=21) for f in d["files"] if f["name"] == VALID])
        self.assertEqual(self.state(VALID), archive_audit.INCONCLUSIVE)
        self.manifest(lambda d: [f.update(error="x") for f in d["files"] if f["name"] == VALID])
        self.assertEqual(self.state(VALID), archive_audit.INCONCLUSIVE)

    def test_failed_copy_with_a_final_file_is_reported_apart(self):
        (self.root / WRONG_DIGEST).write_bytes(b"x" * 22)
        record = self.captures()[WRONG_DIGEST]
        self.assertEqual((record["state"], record["downloaded"]), (archive_audit.UNEXPECTED_RETAINED, True))
        self.assertEqual(len(record["sha256"]), 64)
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])

    def test_unreadable_final_file_is_not_a_confirmed_absence(self):
        # A failed atomic copy whose final file exists but cannot be read, and
        # an injected PermissionError, must not look like the explained omission.
        path = self.root / WRONG_DIGEST
        path.write_bytes(b"x" * 22)
        original = Path.read_bytes

        def deny(self_path):
            if self_path.name == WRONG_DIGEST:
                raise PermissionError("denied")
            return original(self_path)
        with mock.patch.object(Path, "read_bytes", deny):
            record = self.captures()[WRONG_DIGEST]
            self.assertEqual(record["state"], archive_audit.UNREADABLE)
            self.assertIsNone(record["downloaded"])
            self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])
            files = {r["name"]: r for r in archive_audit.audit(self.root)["files"]}
            self.assertTrue(files[WRONG_DIGEST]["read_error"])

    @unittest.skipIf(hasattr(__import__("os"), "geteuid") and __import__("os").geteuid() == 0, "root reads mode 000")
    def test_mode_000_final_file(self):
        for name, copied in [(WRONG_DIGEST, False), (VALID, True)]:
            with self.subTest(name):
                self.setUp()
                if not copied:
                    (self.root / name).write_bytes(b"x" * 22)
                (self.root / name).chmod(0)
                self.addCleanup((self.root / name).chmod, 0o600)
                self.assertEqual(self.state(name), archive_audit.UNREADABLE)

    def test_capture_file_absent_from_files(self):
        self.manifest(lambda d: d.update(files=[f for f in d["files"] if f["name"] != VALID]))
        record = self.captures()[VALID]
        self.assertEqual(record["state"], archive_audit.NOT_IN_FILES)
        report = archive_audit.audit(self.root)
        self.assertIn(VALID, report["unlisted_files"])
        self.assertIn("capture %s: not_in_files" % VALID, report["findings"])

    def test_duplicate_names(self):
        self.manifest(lambda d: d["capture_files"].append(dict(d["capture_files"][0])))
        self.assertEqual(self.state(VALID), archive_audit.DUPLICATE)
        self.setUp()
        self.manifest(lambda d: d["files"].append(dict(d["files"][-1])))
        self.assertEqual(self.state(OVER_LIMIT), archive_audit.DUPLICATE)
        self.assertFalse(archive_audit.audit(self.root)["evidence_complete"])

    def test_path_escape_and_links(self):
        outside = Path(self.directory.name) / "outside.pcap"
        outside.write_bytes(b"z" * 22)
        for name in ["../outside.pcap", "sub/x.pcap", "..", "", "a\0b"]:
            with self.subTest(name):
                self.manifest(lambda d: d["capture_files"].append(
                    dict(d["capture_files"][0], name=name)))
                self.manifest(lambda d: d["files"].append({"name": name, "bytes": 22}))
                report = archive_audit.audit(self.root)
                self.assertEqual(report["capture_files"][-1]["state"], archive_audit.INCONCLUSIVE)
                self.assertIn("safe file name", report["capture_files"][-1]["reason"])
                self.assertIsNone(report["capture_files"][-1]["sha256"])
                self.manifest(lambda d: (d["capture_files"].pop(), d["files"].pop()))
        (self.root / VALID).unlink()
        (self.root / VALID).symlink_to(outside)
        self.assertEqual(self.state(VALID), archive_audit.INCONCLUSIVE)
        self.assertIsNone(self.captures()[VALID]["sha256"])

    def test_extra_retained_files_stay_separate(self):
        (self.root / "capture-9-extra.pcap").write_bytes(b"e")
        (self.root / ".capture-123").write_bytes(b"t")
        report = archive_audit.audit(self.root)
        self.assertEqual(report["unrecorded_capture_files"], [".capture-123", "capture-9-extra.pcap"])
        self.assertEqual(self.state(VALID), archive_audit.VERIFIED)
        self.assertFalse(report["evidence_complete"])


class CaptureMalformed(CaptureArchive):
    def test_missing_null_or_wrong_typed_fields_are_inconclusive(self):
        required = {"registration": [None, "1", True, 1.5], "copied": [None, "true", 1],
                    "bytes": [None, "22", True, -1, 22.0], "name": [None, 7, [], {}],
                    "expected_bytes": [None, "22", True, -1],
                    "expected_sha256": [None, 7, "ABC", "0" * 63, "6F" * 32],
                    "sha256": [None, 7, "ABC", "0" * 63, "6F" * 32]}
        for key, values in required.items():
            with self.subTest(key, value="<missing>"):
                self.setUp()
                self.manifest(lambda d: d["capture_files"][0].pop(key))
                self.assertEqual(self.captures()[VALID]["state"] if key != "name" else
                                 archive_audit.audit(self.root)["capture_files"][0]["state"],
                                 archive_audit.INCONCLUSIVE)
            for value in values:
                with self.subTest(key, value=repr(value)):
                    self.setUp()
                    self.manifest(lambda d: d["capture_files"][0].update({key: value}))
                    report = archive_audit.audit(self.root)
                    self.assertEqual(report["capture_files"][0]["state"], archive_audit.INCONCLUSIVE)
                    self.assertFalse(report["evidence_complete"])

    def test_error_contract(self):
        self.record(VALID, lambda r: r.update(error="boom"))
        self.assertEqual(self.state(VALID), archive_audit.INCONCLUSIVE)
        self.record(WRONG_DIGEST, lambda r: r.pop("error"))
        self.assertEqual(self.state(WRONG_DIGEST), archive_audit.INCONCLUSIVE)
        self.record(WRONG_DIGEST, lambda r: r.update(error=None))
        self.assertEqual(self.state(WRONG_DIGEST), archive_audit.INCONCLUSIVE)
        self.record(WRONG_DIGEST, lambda r: r.update(error=5))
        self.assertEqual(self.state(WRONG_DIGEST), archive_audit.INCONCLUSIVE)

    def test_wrong_typed_capture_files(self):
        for value in [None, {}, "x", 5, [1], [None], ["a"]]:
            with self.subTest(repr(value)):
                self.setUp()
                self.manifest(lambda d: d.update(capture_files=value))
                report = archive_audit.audit(self.root)
                self.assertFalse(report["evidence_complete"])
                self.assertIn("manifest: capture_files is not a list of objects", report["findings"])

    def test_absent_capture_files_is_an_archive_without_captures(self):
        shutil.rmtree(self.root)
        shutil.copytree(FIXTURE, self.root)
        report = archive_audit.audit(self.root)
        self.assertEqual((report["capture_files"], report["unrecorded_capture_files"]), ([], []))

    def test_cli_receipt_includes_captures(self):
        output = Path(self.directory.name) / "receipt.json"
        archive_audit.main([str(self.root), "--output", str(output)])
        receipt = json.loads(output.read_text())
        self.assertEqual([c["state"] for c in receipt["capture_files"]][0], archive_audit.VERIFIED)


if __name__ == "__main__":
    unittest.main()
