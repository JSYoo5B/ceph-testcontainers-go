"""Docker-free collector controls; fake children model only command boundaries."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import structural_review as review


class DiscoveryTests(unittest.TestCase):
    def test_packages_exclude_fixtures_hidden_paths_and_nested_modules(self):
        files = ["ceph/a.go", "ceph/a_test.go", "new/b.go", "internal/tools/x/main.go", "testdata/x.go",
                 "vendor/x/a.go", ".hidden/b.go", "a/_hidden/b.go", "sub/go.mod", "sub/a.go", "sub/nested/b.go"]
        self.assertEqual(review.package_dirs(files), ["ceph", "internal/tools/x", "new"])

    def test_global_input_changes_expand_focus_without_losing_moves(self):
        files = {"base": ["old/a.go", "keep/b.go"], "head": ["new/a.go", "keep/b.go"]}
        changed = ["old/a.go", "new/a.go"]
        self.assertEqual(review.finding_focus(changed, files), (changed, False))
        for global_input in review.GLOBAL_INPUTS:
            self.assertEqual(review.finding_focus([global_input], files),
                             (["keep/b.go", "new/a.go", "old/a.go"], True))

    def test_non_go_change_never_builds_or_calls_lint(self):
        with mock.patch.object(review, "paths", return_value=["ceph/a.go"]), \
                mock.patch.object(review, "git", return_value=b"docs/guide.md\0"), \
                mock.patch.object(review.subprocess, "run") as run:
            text, status = review.collect("a"*40, "b"*40, Path("unused"), "lint")
        self.assertEqual(status, "not_requested")
        self.assertIn("No Go analysis inputs changed", text)
        run.assert_not_called()

    def test_failed_collection_writes_explicit_unavailable_receipt_not_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"report"
            # The child failure substitutes only lint/build failure: no fake
            # success is used as evidence of real collection or publication.
            with mock.patch("sys.argv", ["collector", "--base", "a"*40, "--head", "b"*40, "--output", str(output)]), \
                    mock.patch.object(review.shutil, "which", return_value="lint"), \
                    mock.patch.object(review, "collect", side_effect=subprocess.CalledProcessError(1, "lint")), \
                    mock.patch.object(review, "git", return_value=b"c"*40+b"\n"), \
                    mock.patch.dict(review.os.environ, {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_STEP_SUMMARY": ""}):
                self.assertEqual(review.main(), 1)
            receipt = json.loads((output/"receipt.json").read_text())
            self.assertEqual(receipt["status"], "unavailable")
            self.assertEqual((receipt["run_id"], receipt["attempt"]), (123, 2))
            self.assertEqual(set(receipt["files"]), {"summary.md"})
            self.assertIn("No clean result is inferred", (output/"summary.md").read_text())


class SummaryTests(unittest.TestCase):
    def test_moves_both_sides_and_largest_context_keep_counts_and_exclusions(self):
        def side(methods, fields):
            return {"exported_methods": methods, "private_methods": 2, "fields": fields, "declarations": 1, "locations": ["ceph/a.go:3"]}
        delta = {"changed_owners": [
            {"owner": "ceph.New", "status": "introduced", "base": None, "head": side(10, 3), "delta": {"exported_methods": 10, "private_methods": 2}},
            {"owner": "ceph.Old", "status": "removed", "base": side(10, 3), "head": None, "delta": {"exported_methods": -10, "private_methods": -2}}],
            "unmeasured_packages": [{"package": "new", "side": "base"}]}
        head = {"owners": [{"owner": "ceph.Large", **side(90, 50)}]}
        text = review.structure_summary({}, head, delta)
        for term in ("ceph.New", "ceph.Old", "90/2", "50", "promoted members are excluded", "All lexical build profiles", "absent owner", "new"):
            self.assertIn(term, text)
        self.assertLess(text.index("ceph.New"), text.index("ceph.Old"))

    def test_escaping(self):
        self.assertNotIn("<script>", review.cell("<script>|@user`[x]"))
        self.assertIn("&#64;", review.cell("@user"))
