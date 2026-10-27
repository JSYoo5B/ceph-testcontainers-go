import io
import contextlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import docs_only

ROOT = Path(__file__).resolve().parents[2]

# (path, expected is_unread_doc). Every "read by a check" row names the check.
TABLE = [
    # Documentation that no check reads.
    ("README.md", True), ("CHANGELOG.md", True), ("AGENTS.md", True), ("LICENSE", True),
    ("docs/contributing/ci.md", True), ("docs/contributing/testing.md", True),
    ("docs/research/ceph-fuse-mount.md", True),
    ("docs/guides/errors.md", True), ("docs/guides/upgrading-to-v0.3.md", True),
    ("docs/design/fault-package.md", True), ("docs/design/messenger-evidence.md", True),
    # Read by internal/apiinventory (package pages name every export).
    ("docs/ceph.md", False), ("docs/fault.md", False), ("docs/rbd.md", False),
    ("docs/cephfs.md", False), ("docs/rgw.md", False), ("docs/capture.md", False),
    # Read by internal/apiinventory (package-scoped naming catalogues, receipts and the review note it stats).
    ("docs/design/api-naming/rgw/multisite/declarations.json", False),
    ("docs/design/api-naming/rgw/multisite/members.json", False),
    ("docs/design/api-naming/rgw/multisite/review.json", False),
    ("docs/design/api-naming/ceph/declarations.json", False), ("docs/design/api-naming/capture/review.json", False),
    ("docs/design/api-naming/review-note.md", False), ("docs/design/api-naming-audit.md", False),
    ("docs/design/api-naming.md", False),
    # Read by the messenger coverage fixture test.
    ("docs/guides/messenger-client-fixtures.md", False),
    # Read by tag_scenarios (registry) and its tests (catalogue).
    ("docs/guides/bug-compatibility-tags.json", False), ("docs/guides/bug-compatibility-tags.md", False),
    # Not on the allowlist: data files, new top-level pages, nested paths, other trees.
    ("docs/contributing/role-overhead-results.json", False), ("docs/new-package.md", False),
    ("docs/guides/sub/page.md", False), ("docs/guides/page.txt", False), ("docs/guides/.md", True),
    ("internal/integration/testdata/expected.md", False), ("internal/integration/goceph/README.md", False),
    ("ceph/notes.md", False), (".github/scripts/docs_only.py", False), (".github/workflows/code.yml", False),
    ("Makefile", False), ("go.mod", False), ("ceph/ceph.go", False), ("license", False),
    ("README.md.go", False), ("docs/contributing/ci.md/x", False),
    # Malformed or escaping paths.
    ("", False), ("/docs/guides/errors.md", False), ("docs/../ceph/ceph.go", False),
    ("docs//guides/errors.md", False), ("docs/guides/errors.md\n", False), (" README.md", False),
    ("docs\\guides\\errors.md", False), (None, False), (7, False),
]


def mismatches(classifier):
    return [(path, want) for path, want in TABLE if classifier(path) != want]


class ClassifierTests(unittest.TestCase):
    def test_table(self):
        self.assertEqual(mismatches(docs_only.is_unread_doc), [])


class NegativeControlTests(unittest.TestCase):
    """The table must reject each plausible wrong classifier."""

    @staticmethod
    def paths_ignore(path):
        # The runtime workflows' paths-ignore: docs/**, *.md, LICENSE.
        return isinstance(path, str) and (path.startswith("docs/") or "/" not in path and path.endswith(".md")
                                          or path == "LICENSE")

    @staticmethod
    def any_markdown(path):
        return isinstance(path, str) and (path.endswith(".md") or path == "LICENSE")

    @staticmethod
    def no_read_exceptions(path):
        return docs_only.is_unread_doc(path) or path in docs_only.READ_DOC_FILES

    @staticmethod
    def naming_receipts_allowed(path):
        return docs_only.is_unread_doc(path) or (isinstance(path, str) and path.startswith("docs/design/api-naming"))

    @staticmethod
    def package_pages_allowed(path):
        return docs_only.is_unread_doc(path) or path in ("docs/ceph.md", "docs/rgw.md")

    @staticmethod
    def never(path):
        return False

    def test_wrong_classifiers_fail_the_table(self):
        for name in ("paths_ignore", "any_markdown", "no_read_exceptions", "naming_receipts_allowed",
                     "package_pages_allowed", "never"):
            with self.subTest(name):
                self.assertNotEqual(mismatches(getattr(self, name)), [], name)

    def test_decide_fails_open_against_a_wrong_decider(self):
        # A decider that ignores the event would skip a push to main.
        wrong = lambda event, fork, base, changed: (all(map(docs_only.is_unread_doc, changed)), "")
        self.assertTrue(wrong("push", False, "", ["README.md"])[0])
        self.assertFalse(docs_only.decide("push", False, "", ["README.md"])[0])


class DecideTests(unittest.TestCase):
    def test_documentation_only_pull_request(self):
        only, _ = docs_only.decide("pull_request", False, "abc", ["README.md", "docs/guides/errors.md"])
        self.assertTrue(only)

    def test_one_code_file_runs_everything(self):
        for extra in ("ceph/ceph.go", "docs/ceph.md", "docs/design/api-naming/ceph/declarations.json",
                      ".github/workflows/code.yml"):
            with self.subTest(extra):
                self.assertFalse(docs_only.decide("pull_request", False, "abc", ["README.md", extra])[0])

    def test_fail_open_cases(self):
        docs = ["README.md"]
        cases = {
            "push": ("push", False, "abc", docs),
            "schedule": ("schedule", False, "", docs),
            "workflow_dispatch": ("workflow_dispatch", False, "abc", docs),
            "empty event": ("", False, "abc", docs),
            "fork": ("pull_request", True, "abc", docs),
            "no base": ("pull_request", False, "", docs),
            "unavailable diff": ("pull_request", False, "abc", None),
            "empty diff": ("pull_request", False, "abc", []),
        }
        for name, args in cases.items():
            with self.subTest(name):
                self.assertFalse(docs_only.decide(*args)[0])


def git(directory, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false",
                    *args], cwd=directory, check=True, capture_output=True)


class GitTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        git(self.root, "init", "-q", "-b", "main")
        self.write("README.md", "a\n")
        self.write("docs/guides/errors.md", "a\n")
        self.write("ceph/ceph.go", "package ceph\n")
        git(self.root, "add", "-A")
        git(self.root, "commit", "-q", "-m", "base")
        self.base = self.rev()
        self.old = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.old)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def rev(self):
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=self.root, check=True, capture_output=True,
                              text=True).stdout.strip()

    def commit(self):
        git(self.root, "add", "-A")
        git(self.root, "commit", "-q", "-m", "change")

    def run_main(self, *args):
        output = self.root / "out"
        output.unlink(missing_ok=True)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(docs_only.main([*args, "--github-output", str(output)]), 0)
        return output.read_text() if output.exists() else None

    def test_documentation_commit(self):
        self.write("README.md", "b\n")
        self.write("docs/guides/new.md", "b\n")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=true\n")

    def test_code_commit(self):
        self.write("README.md", "b\n")
        self.write("ceph/ceph.go", "package ceph // changed\n")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=false\n")

    def test_rename_out_of_code_is_seen(self):
        git(self.root, "mv", "ceph/ceph.go", "docs/guides/moved.md")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=false\n")

    def test_deleted_code_file_is_seen(self):
        (self.root / "ceph/ceph.go").unlink()
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=false\n")

    def test_read_doc_commit(self):
        self.write("docs/ceph.md", "b\n")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=false\n")

    def test_unknown_base_fails_open(self):
        self.write("README.md", "b\n")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", "0" * 40), "docs_only=false\n")
        self.assertEqual(self.run_main("--event", "pull_request", "--base", "not a commit"), "docs_only=false\n")

    def test_unchanged_tree_fails_open(self):
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base), "docs_only=false\n")

    def test_non_pull_request_events_run_everything(self):
        self.write("README.md", "b\n")
        self.commit()
        for event in ("push", "schedule", "workflow_dispatch", ""):
            with self.subTest(event):
                self.assertEqual(self.run_main("--event", event, "--base", self.base), "docs_only=false\n")

    def test_fork_and_missing_base(self):
        self.write("README.md", "b\n")
        self.commit()
        self.assertEqual(self.run_main("--event", "pull_request", "--base", self.base, "--fork", "true"),
                         "docs_only=false\n")
        self.assertEqual(self.run_main("--event", "pull_request"), "docs_only=false\n")

    def test_not_a_repository_fails_open(self):
        with tempfile.TemporaryDirectory() as empty:
            os.chdir(empty)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(docs_only.main(["--event", "pull_request", "--base", self.base]), 0)
            self.assertIn("docs_only=false", out.getvalue())

    def test_bad_arguments_fail_open(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(docs_only.main(["--no-such-flag"]), 0)
        self.assertIn("docs_only=false", out.getvalue())


class ReaderInventoryTests(unittest.TestCase):
    """A check that starts reading a documentation file must stop it being skipped."""

    # A Go source names a document file, or the whole package-scoped catalogue
    # directory that internal/apiinventory/catalogue reads.
    REFERENCE = re.compile(r"docs/[A-Za-z0-9_./-]*\.(?:md|json)|docs/design/api-naming/?")

    def sources(self):
        for path in sorted(ROOT.rglob("*.go")):
            relative = path.relative_to(ROOT)
            if "testdata" in relative.parts or relative.parts[0] in (".git", "docs", "artifacts"):
                continue
            yield relative
        yield Path(".github/scripts/tag_scenarios.py")

    def test_every_file_a_source_names_runs_the_checks(self):
        found = {}
        for relative in self.sources():
            for number, line in enumerate((ROOT / relative).read_text(encoding="utf-8").splitlines(), 1):
                if line.lstrip().startswith(("//", "#")):
                    continue
                for match in self.REFERENCE.findall(line):
                    found.setdefault(match, "%s:%d" % (relative, number))
        # The scan must keep seeing the known readers, or it proves nothing.
        for known in ("docs/design/api-naming", "docs/guides/messenger-client-fixtures.md",
                      "docs/guides/bug-compatibility-tags.json"):
            self.assertIn(known, found)
        for path, where in sorted(found.items()):
            self.assertFalse(docs_only.is_unread_doc(path), "%s is read at %s but is skipped as unread" % (path, where))

    def test_every_file_a_catalogue_receipt_names_runs_the_checks(self):
        # The package pages and review notes are named by data, not by a Go
        # literal, so the source scan cannot see them. Read the receipts.
        receipts = sorted((ROOT / "docs/design/api-naming").rglob("review.json"))
        self.assertGreaterEqual(len(receipts), 1)
        for receipt in receipts:
            data = json.loads(receipt.read_text(encoding="utf-8"))
            for path in (data["page"], "docs/design/" + data["review"]):
                self.assertTrue((ROOT / path).is_file(), "%s names a missing file %s" % (receipt.relative_to(ROOT), path))
                self.assertFalse(docs_only.is_unread_doc(path), "%s names %s, which is skipped as unread" % (receipt.relative_to(ROOT), path))

    def test_the_scan_catches_a_reader_of_an_allowlisted_file(self):
        self.assertTrue(docs_only.is_unread_doc("docs/guides/errors.md"))
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "reader_test.go"
            source.write_text('os.ReadFile("../../docs/guides/errors.md")\n')
            found = self.REFERENCE.findall(source.read_text())
        self.assertEqual(found, ["docs/guides/errors.md"])
        self.assertTrue(docs_only.is_unread_doc(found[0]))


class WorkflowWiringTests(unittest.TestCase):
    GATE = "needs.changes.outputs.docs_only != 'true'"

    def setUp(self):
        self.text = (ROOT / ".github/workflows/code.yml").read_text()

    def job(self, name):
        match = re.search(r"(?ms)^  %s:\n(.*?)(?=^  [a-z-]+:\n|\Z)" % re.escape(name), self.text)
        self.assertIsNotNone(match, name)
        return match.group(1)

    def test_detection_runs_the_script_with_the_event_and_base(self):
        block = self.job("changes")
        self.assertIn("python3 .github/scripts/docs_only.py", block)
        for needle in ("github.event_name", "github.event.pull_request.base.sha"):
            self.assertIn(needle, block)

    def test_only_an_explicit_true_skips_and_failure_does_not_skip(self):
        for name in ("unit", "race", "static", "tag-coverage"):
            condition = re.search(r"(?m)^    if: (.*)$", self.job(name)).group(1)
            self.assertIn("!cancelled()", condition, name)
            self.assertIn(self.GATE, condition, name)
        header = self.job("vulncheck").split("    steps:", 1)[0]
        self.assertIn("    if: ${{ !cancelled() }}\n", header)

    def test_every_vulncheck_step_is_gated(self):
        steps = re.split(r"(?m)^      - ", self.job("vulncheck").split("    steps:\n", 1)[1])[1:]
        self.assertGreaterEqual(len(steps), 6)
        for step in steps:
            self.assertIn(self.GATE, step, step.splitlines()[0])


if __name__ == "__main__":
    unittest.main()
