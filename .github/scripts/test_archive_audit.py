import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

import archive_audit

# The tree is the real output of the finalization producer with a writer that
# keeps the first 17 bytes of diagnostics.json and then returns a no space
# error (see ceph/finalization.go writeReport).
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "finalization-partial-write"


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


if __name__ == "__main__":
    unittest.main()
