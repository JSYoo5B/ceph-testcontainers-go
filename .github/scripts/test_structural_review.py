"""Docker-free collector controls; fake children model only command boundaries."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import structural_review as review


class DiscoveryTests(unittest.TestCase):
    def test_packages_exclude_fixtures_hidden_paths_and_nested_modules(self):
        files = ["ceph/a.go", "ceph/a_test.go", "new/b.go", "internal/tools/x/main.go", "testdata/x.go",
                 "vendor/x/a.go", ".hidden/b.go", "a/_hidden/b.go", "sub/go.mod", "sub/a.go", "sub/nested/b.go"]
        self.assertEqual(review.package_dirs(files), ["ceph", "new"])

    def test_repository_only_directories_are_outside_the_library(self):
        files = ["internal/watch/a.go", "internal/integration/harness/h.go", "internal/clustertest/c.go",
                 "internal/apiinventory/catalogue/c.go", "ceph/expect/e.go"]
        self.assertEqual(review.package_dirs(files), ["ceph/expect", "internal/watch"])

    def test_global_input_changes_expand_focus_without_losing_moves(self):
        files = {"base": ["old/a.go", "keep/b.go"], "head": ["new/a.go", "keep/b.go"]}
        changed = ["old/a.go", "new/a.go"]
        self.assertEqual(review.finding_focus(changed, files), (changed, False))
        for global_input in review.GLOBAL_INPUTS:
            self.assertEqual(review.finding_focus([global_input], files),
                             (["keep/b.go", "new/a.go", "old/a.go"], True))

    def test_ineligible_change_never_builds_or_calls_lint(self):
        with mock.patch.object(review, "eligibility", return_value=(False, "only documentation changed")), \
                mock.patch.object(review.subprocess, "run") as run:
            text, details, status = review.collect("a"*40, "b"*40, Path("unused"), "lint")
        self.assertEqual((status, details), ("not_requested", ""))
        self.assertIn("only documentation changed", text)
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
            self.assertEqual(set(receipt["files"]), {"summary.md", "details.md"})
            self.assertIn("No clean result is inferred", (output/"summary.md").read_text())


class EligibilityTests(unittest.TestCase):
    def decide(self, *pairs, nested=("internal/integration/goceph/probe",)):
        return review.classify(pairs, list(nested))[0]

    def test_tests_helpers_fixtures_and_docs_skip(self):
        for pair in (("ceph/a_test.go", "ceph/a_test.go"), (None, "internal/integration/x_test.go"),
                     ("internal/integration/harness/h.go", "internal/integration/harness/h.go"),
                     ("internal/clustertest/fake.go", None), ("ceph/testdata/x.json", "ceph/testdata/x.json"),
                     ("internal/integration/goceph/probe/main.go", "internal/integration/goceph/probe/main.go"),
                     ("docs/guides/a.md", "docs/guides/a.md"), ("rgw/README.md", "rgw/README.md")):
            with self.subTest(pair=pair):
                self.assertFalse(self.decide(pair))

    def test_library_code_public_test_conveniences_and_build_inputs_run(self):
        for pair in (("ceph/ceph.go", "ceph/ceph.go"), (None, "internal/watch/w.go"), ("ceph/expect/osd.go", None),
                     ("go.mod", "go.mod"), ("rgw/assets/x.json", "rgw/assets/x.json")):
            with self.subTest(pair=pair):
                self.assertTrue(self.decide(pair))

    def test_both_sides_of_a_move_count(self):
        # A library file moved into a test helper still changes the library,
        # and a helper moved into the library is a library change too.
        self.assertTrue(self.decide(("ceph/old.go", "internal/integration/harness/old.go")))
        self.assertTrue(self.decide(("internal/integration/harness/h.go", "ceph/h.go")))
        self.assertFalse(self.decide(("internal/integration/a.go", "internal/integration/harness/a.go")))

    def test_tools_alone_skip_and_unknown_paths_run(self):
        self.assertEqual(review.classify([(".github/scripts/x.py", ".github/scripts/x.py"), ("Makefile", "Makefile")], [])[0], False)
        self.assertIn("tools", review.classify([("internal/tools/structmetrics/main.go", None)], [])[1])
        eligible, reason = review.classify([("newtopdir/thing.go", "newtopdir/thing.go")], [])
        self.assertTrue(eligible)
        self.assertIn("newtopdir/thing.go", reason)

    def test_changed_pairs_keep_both_sides_of_renames(self):
        out = b"R100\0ceph/a.go\0internal/integration/a.go\0D\0ceph/b.go\0A\0ceph/c.go\0M\0ceph/d.go\0"
        with mock.patch.object(review, "git", return_value=out):
            self.assertEqual(review.changed_pairs("a"*40, "b"*40), [
                ("ceph/a.go", "internal/integration/a.go"), ("ceph/b.go", None), (None, "ceph/c.go"), ("ceph/d.go", "ceph/d.go")])

    @unittest.skipUnless(shutil.which("go"), "needs the Go toolchain")
    def test_repository_only_directories_match_the_library_imports(self):
        root = Path(__file__).resolve().parents[2]
        module = subprocess.check_output(["go", "list", "-m"], cwd=root, text=True).strip()
        public = [p for p in subprocess.check_output(["go", "list", "./..."], cwd=root, text=True).split()
                  if "/internal/" not in p]
        deps = subprocess.check_output(["go", "list", "-deps", *public], cwd=root, text=True).split()
        used = {d.removeprefix(module + "/") for d in deps if d.startswith(module + "/internal/")}
        internal = {p.removeprefix(module + "/") for p in subprocess.check_output(
            ["go", "list", "-tags", "all", "./internal/..."], cwd=root, text=True).split()}
        repository_only = review.TEST_ONLY_DIRS + review.TOOL_DIRS
        self.assertEqual({p for p in used if review.under(p, repository_only)}, set(),
                         "a library package imports a directory classified as repository-only")
        self.assertEqual({p for p in internal - used if not review.under(p, repository_only)}, set(),
                         "an internal directory no library package imports is not classified")


class RenderTests(unittest.TestCase):
    LINT = {"entries": [], "tool": {"golangci_lint_version": "2.15.0"}, "config": {"sha256": "c"*64},
            "base_evidence": {"raw_sha256": "a"*64}, "head_evidence": {"raw_sha256": "b"*64},
            "entries_per_state": {"reported_both": 3}}

    @staticmethod
    def side(methods, fields):
        return {"exported_methods": methods, "private_methods": 2, "fields": fields, "declarations": 1, "locations": ["ceph/a.go:3"]}

    def test_tables_show_packages_owners_and_changed_findings(self):
        base = {"packages": [{"package": "ceph", "lines": 100, "exported_declarations": 5}, {"package": "old", "lines": 7, "exported_declarations": 1}]}
        head = {"packages": [{"package": "ceph", "lines": 130, "exported_declarations": 6}, {"package": "new", "lines": 9, "exported_declarations": 2}]}
        delta = {"changed_owners": [
            {"owner": "ceph.New", "status": "introduced", "base": None, "head": self.side(3, 1), "delta": {"exported_methods": 3, "private_methods": 2}},
            {"owner": "ceph.Old", "status": "removed", "base": self.side(1, 2), "head": None, "delta": {"exported_methods": -1, "private_methods": -2}}],
            "unmeasured_packages": []}
        lint = dict(self.LINT, entries=[
            {"state": "reported_both", "linter": "cyclop", "package_dir": "ceph", "function": "Run",
             "base": {"file": "ceph/a.go", "line": 3, "test_file": False, "message": "calculated cyclomatic complexity for function Run is 14, max is 10"},
             "head": {"file": "ceph/a.go", "line": 3, "test_file": False, "message": "calculated cyclomatic complexity for function Run is 20, max is 10"}},
            {"state": "reported_both", "linter": "cyclop", "package_dir": "ceph", "function": "Same",
             "base": {"file": "ceph/a.go", "line": 9, "test_file": False, "message": "same"},
             "head": {"file": "ceph/a.go", "line": 9, "test_file": False, "message": "same"}},
            {"state": "new_above_threshold", "linter": "cyclop", "package_dir": "ceph", "function": "Test",
             "head": {"file": "ceph/a_test.go", "line": 1, "test_file": True, "message": "x is 30"}},
            {"state": "moved_or_unmatched", "reason": "declaration_absent_in_base", "linter": "dupl", "package_dir": "ceph",
             "head": {"file": "ceph/b.go", "line": 4, "test_file": False, "message": "4-20 lines are duplicate of `ceph/c.go:1-17`"}}])
        visible, details = review.render(base, head, delta, lint, {"ceph": 40, "new": 9, "old": 7}, {"ceph", "new", "old"})
        for want in ("| ceph | 100 → 130 (+30) | 5 → 6 (+1) | 40 (71%) |", "| new | absent → 9 (new) | absent → 2 (new) | 9 (16%) |",
                     "| old | 7 → absent (removed) |", "over 56 such lines",
                     "| ceph.New | absent → 3 (new) |", "| ceph.Old | 1 → absent (removed) |",
                     "| ceph.Run | cyclomatic complexity | 14 → 20 (+6) | changed |",
                     "duplicate block | see location | new or moved function | ceph/b.go:4"):
            self.assertIn(want, visible)
        self.assertNotIn("Same", visible)
        self.assertNotIn("a_test.go", visible)
        self.assertIn("raw SHA-256", details)
        self.assertNotIn("SHA-256", visible)

    def test_no_change_is_one_line_and_rows_are_bounded(self):
        same = {"packages": [{"package": "ceph", "lines": 1, "exported_declarations": 1}]}
        visible, _ = review.render(same, same, {"changed_owners": []}, self.LINT, {}, {"ceph"})
        self.assertEqual(visible.count("\n"), 1)
        self.assertIn("No library structural change", visible)
        owners = [{"owner": "ceph.T%02d" % i, "status": "introduced", "base": None, "head": self.side(1, 1),
                   "delta": {"exported_methods": 1, "private_methods": 2}} for i in range(20)]
        visible, _ = review.render(same, same, {"changed_owners": owners}, self.LINT, {}, {"ceph"})
        self.assertEqual(visible.count("| ceph.T"), review.ROW_LIMIT)
        self.assertIn("5 more changed owners are in structure-delta.json", visible)

    def test_escaping(self):
        self.assertNotIn("<script>", review.cell("<script>|@user`[x]"))
        self.assertIn("&#64;", review.cell("@user"))
