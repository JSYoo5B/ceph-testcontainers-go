"""Test the revision-to-revision bug binding comparator without Docker or a network."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import bug_binding_history as history
import tag_scenarios as engine

# Synthetic identities: test inputs only, never registered in the catalogue.
TAG, OTHER = "ceph_bug_synthetic_a", "ceph_bug_synthetic_b"
LINK = "https://tracker.example.invalid/issues/1"
MOVED = "https://tracker.example.invalid/moved/1"
FOREIGN = "https://tracker.example.invalid/issues/2"


def entry(name=TAG, status="supported", tracker=LINK):
    sources = ["rbd/" + name + ".go"] if status == "supported" else []
    return {"tag": name, "tracker": tracker, "status": status, "sources": sources}


def registry(*entries):
    return engine.bug_registry({"schema": engine.BUG_REGISTRY_SCHEMA, "tags": list(entries), "conflicts": []})


def decide(base, candidate, aliases=None):
    records = history.compare_bindings(registry(*base), registry(*candidate), aliases)
    return {record["tag"]: record for record in records}


class TransitionTests(unittest.TestCase):
    def assertRefused(self, record, *rules):
        self.assertEqual(record["decision"], history.REFUSE)
        self.assertEqual(record["rules"], list(rules))

    def assertPermitted(self, record):
        self.assertEqual(record["decision"], history.PERMIT)
        self.assertEqual(record["rules"], [])

    def test_supported_to_retired_with_the_same_defect_is_permitted(self):
        self.assertPermitted(decide([entry()], [entry(status="retired")])[TAG])

    def test_unchanged_entries_and_new_tags_are_permitted(self):
        for base, candidate in (([entry()], [entry()]), ([entry(status="retired")], [entry(status="retired")]),
                                ([], [entry()]), ([entry()], [entry(), entry(OTHER)])):
            for record in decide(base, candidate).values():
                self.assertPermitted(record)

    def test_deleting_a_retired_tombstone_is_refused(self):
        self.assertRefused(decide([entry(status="retired")], [])[TAG], history.RULE_RETIRED_DELETED)

    def test_deleting_a_supported_entry_without_retiring_it_is_refused(self):
        self.assertRefused(decide([entry()], [])[TAG], history.RULE_SUPPORTED_DELETED)

    def test_retired_to_supported_under_another_tracker_is_refused_as_reassignment(self):
        record = decide([entry(status="retired")], [entry(tracker=FOREIGN)])[TAG]
        self.assertRefused(record, history.RULE_TRACKER_REASSIGNED, history.RULE_REACTIVATION)

    def test_retired_to_retired_under_another_tracker_is_refused_as_reassignment(self):
        record = decide([entry(status="retired")], [entry(status="retired", tracker=FOREIGN)])[TAG]
        self.assertRefused(record, history.RULE_TRACKER_REASSIGNED)

    def test_same_defect_reactivation_stays_refused(self):
        self.assertRefused(decide([entry(status="retired")], [entry()])[TAG], history.RULE_REACTIVATION)

    def test_supported_tracker_change_needs_a_reviewed_alias(self):
        self.assertRefused(decide([entry()], [entry(tracker=MOVED)])[TAG], history.RULE_TRACKER_WITHOUT_ALIAS)

    def test_reviewed_alias_permits_the_link_edit(self):
        aliases = {TAG: [LINK]}
        self.assertPermitted(decide([entry()], [entry(tracker=MOVED)], aliases)[TAG])
        self.assertPermitted(decide([entry(status="retired")], [entry(status="retired", tracker=MOVED)], aliases)[TAG])

    def test_alias_history_of_another_tag_or_link_does_not_apply(self):
        for aliases in ({OTHER: [LINK]}, {TAG: [FOREIGN]}, {TAG: []}, {}):
            self.assertRefused(decide([entry()], [entry(tracker=MOVED)], aliases)[TAG],
                               history.RULE_TRACKER_WITHOUT_ALIAS)

    def test_alias_does_not_permit_reactivation(self):
        record = decide([entry(status="retired")], [entry(tracker=MOVED)], {TAG: [LINK]})[TAG]
        self.assertRefused(record, history.RULE_LINK_EDIT_CHANGES_STATUS, history.RULE_REACTIVATION)

    def test_alias_never_covers_a_link_edit_combined_with_a_status_change(self):
        aliases = {TAG: [LINK]}
        record = decide([entry()], [entry(status="retired", tracker=MOVED)], aliases)[TAG]
        self.assertRefused(record, history.RULE_LINK_EDIT_CHANGES_STATUS)
        # Independent controls: each edit alone is permitted.
        self.assertPermitted(decide([entry()], [entry(tracker=MOVED)], aliases)[TAG])
        self.assertPermitted(decide([entry()], [entry(status="retired")], aliases)[TAG])
        # Without the alias the same combined edit is refused for the missing alias.
        self.assertRefused(decide([entry()], [entry(status="retired", tracker=MOVED)])[TAG],
                           history.RULE_TRACKER_WITHOUT_ALIAS)

    def test_each_tag_is_decided_independently(self):
        records = decide([entry(), entry(OTHER, "retired")], [entry(status="retired")])
        self.assertPermitted(records[TAG])
        self.assertRefused(records[OTHER], history.RULE_RETIRED_DELETED)
        self.assertEqual(sorted(records), [TAG, OTHER])

    def test_source_list_edits_alone_do_not_change_the_binding(self):
        moved = entry()
        moved["sources"] = ["rbd/other.go"]
        self.assertPermitted(decide([entry()], [moved])[TAG])

    def test_malformed_alias_history_is_an_error_not_an_empty_history(self):
        for aliases in ({TAG: "https://x.invalid/"}, {TAG: [None]}, {TAG: ["http://x.invalid/"]}, {1: [LINK]}):
            with self.assertRaises(ValueError):
                decide([entry()], [entry()], aliases)

    def test_unvalidated_registry_is_an_error(self):
        for bad in (None, {}, {"tags": []}, {"tags": None}):
            with self.assertRaises(ValueError):
                history.compare_bindings(bad, registry())
            with self.assertRaises(ValueError):
                history.compare_bindings(registry(), bad)


class ReportTests(unittest.TestCase):
    def test_report_names_the_tag_both_states_and_every_rule(self):
        records = history.compare_bindings(registry(entry(status="retired")), registry(entry(tracker=FOREIGN)))
        self.assertEqual(history.report(records), [
            TAG + ": retired " + LINK + " -> supported " + FOREIGN
            + ": refused by tracker-reassigned, reactivation-undecided"])

    def test_report_names_an_absent_candidate(self):
        records = history.compare_bindings(registry(entry(status="retired")), registry())
        self.assertEqual(history.report(records), [TAG + ": retired " + LINK + " -> absent: refused by retired-entry-deleted"])

    def test_permitted_transitions_are_not_reported(self):
        records = history.compare_bindings(registry(entry()), registry(entry(status="retired")))
        self.assertEqual(history.report(records), [])
        history.check_bindings(registry(entry()), registry(entry(status="retired")))

    def test_check_raises_with_every_refused_transition(self):
        with self.assertRaisesRegex(ValueError, "retired-entry-deleted") as caught:
            history.check_bindings(registry(entry(status="retired"), entry(OTHER, "retired")),
                                   registry(entry(OTHER, "retired", FOREIGN)))
        self.assertIn(OTHER + ": retired", str(caught.exception))
        self.assertIn("tracker-reassigned", str(caught.exception))


def git(root, *arguments):
    subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                    "-c", "commit.gpgsign=false", *arguments], check=True, capture_output=True)


def write_registry(root, *entries, raw=None):
    path = Path(root) / history.REGISTRY_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw if raw is not None else json.dumps(
        {"schema": engine.BUG_REGISTRY_SCHEMA, "tags": list(entries), "conflicts": []}), encoding="utf-8")


class RevisionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        git(self.root, "init", "-q")

    def commit(self, message):
        git(self.root, "add", "-A")
        git(self.root, "commit", "-q", "-m", message)

    def run_main(self, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = history.main(["--root", str(self.root), *arguments])
        return code, out.getvalue(), err.getvalue()

    def test_base_registry_is_read_from_the_named_revision_not_the_worktree(self):
        write_registry(self.root, entry(status="retired"))
        self.commit("base")
        write_registry(self.root)
        self.assertEqual(history.load_revision_registry("HEAD", self.root)["tags"][TAG]["status"], "retired")

    def test_deleted_tombstone_is_refused_against_the_named_base(self):
        write_registry(self.root, entry(status="retired"))
        self.commit("base")
        write_registry(self.root)
        code, out, _ = self.run_main("--base", "HEAD")
        self.assertEqual(code, 1)
        self.assertEqual(out.strip(), TAG + ": retired " + LINK + " -> absent: refused by retired-entry-deleted")

    def test_permitted_retirement_exits_zero_and_prints_nothing(self):
        write_registry(self.root, entry())
        self.commit("base")
        write_registry(self.root, entry(status="retired"))
        self.assertEqual(self.run_main("--base", "HEAD"), (0, "", ""))

    def test_alias_option_permits_a_link_edit(self):
        write_registry(self.root, entry())
        self.commit("base")
        write_registry(self.root, entry(tracker=MOVED))
        self.assertEqual(self.run_main("--base", "HEAD")[0], 1)
        self.assertEqual(self.run_main("--base", "HEAD", "--alias", TAG + "=" + LINK), (0, "", ""))
        self.assertEqual(self.run_main("--base", "HEAD", "--alias", TAG)[0], 2)

    def test_unresolvable_base_is_inconclusive_not_an_empty_base(self):
        write_registry(self.root, entry(status="retired"))
        self.commit("base")
        write_registry(self.root)
        for revision in ("no-such-revision", "--upload-pack=x", ""):
            code, out, err = self.run_main("--base=" + revision)
            self.assertEqual((code, out), (2, ""), revision)
            self.assertIn("inconclusive", err)

    def test_base_without_a_registry_file_is_inconclusive(self):
        (self.root / "README").write_text("x")
        self.commit("base")
        write_registry(self.root)
        self.assertEqual(self.run_main("--base", "HEAD")[0], 2)

    def test_invalid_base_or_candidate_registry_is_inconclusive(self):
        write_registry(self.root, raw="{not json")
        self.commit("bad base")
        write_registry(self.root)
        code, _, err = self.run_main("--base", "HEAD")
        self.assertEqual(code, 2)
        self.assertIn("base revision registry", err)
        write_registry(self.root, entry())
        self.commit("good base")
        write_registry(self.root, raw="{not json")
        code, _, err = self.run_main("--base", "HEAD")
        self.assertEqual(code, 2)
        self.assertIn("registry is unreadable", err)

    def test_type_errors_from_a_malformed_entry_are_inconclusive_for_base_and_candidate(self):
        bad = entry()
        bad["sources"] = [[]]  # the validator reaches set(sources) and raises TypeError
        write_registry(self.root, bad)
        self.commit("malformed base")
        write_registry(self.root, entry())
        code, out, err = self.run_main("--base", "HEAD")
        self.assertEqual((code, out), (2, ""))
        self.assertIn("base revision registry is invalid", err)
        write_registry(self.root, entry())
        self.commit("good base")
        write_registry(self.root, bad)
        code, out, err = self.run_main("--base", "HEAD")
        self.assertEqual((code, out), (2, ""))
        self.assertIn("candidate registry is invalid", err)
        # A real policy refusal stays exit 1 with a report.
        write_registry(self.root)
        self.assertEqual(self.run_main("--base", "HEAD")[0], 1)

    def test_production_registry_is_unchanged_against_itself(self):
        base = engine.load_bug_registry()
        self.assertEqual(history.report(history.compare_bindings(base, base)), [])


if __name__ == "__main__":
    unittest.main()
