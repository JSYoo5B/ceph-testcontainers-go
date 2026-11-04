"""Publisher validation using explicit GitHub response fixtures, without writes.

The fake API below substitutes only remote JSON/zip responses to exercise the
write authorization and idempotence boundary. It does not model GitHub execution.
"""
import copy
import hashlib
import io
import json
from pathlib import Path
import stat
import unittest
from unittest import mock
import zipfile

import publish_structural_review as reporter

REPO = "owner/repository"
BASE, HEAD = "a"*40, "b"*40
LINKED = {"number": 7, "base": {"sha": BASE, "ref": "main", "repo": {"id": 1}},
          "head": {"sha": HEAD, "repo": {"id": 2}}}
RUN = {"id": 10, "run_attempt": 2, "workflow_id": 3, "event": "pull_request", "status": "completed", "conclusion": "success",
       "path": ".github/workflows/code.yml", "repository": {"id": 1, "full_name": REPO},
       "head_repository": {"id": 2}, "head_sha": HEAD, "pull_requests": [LINKED]}
PULL = {"state": "open", "head": {"sha": HEAD, "repo": {"id": 2}},
        "base": {"ref": "main", "repo": {"full_name": REPO}}}


def archive(receipt_changes=None, extra=None, summary="Safe report", status="unavailable"):
    files = {"summary.md": summary.encode()}
    if status == "complete":
        files.update({name: b"{}" for name in reporter.FILES-{"receipt.json", "summary.md"}})
    receipt = {"schema": reporter.SCHEMA, "base": BASE, "target_base": BASE, "head": HEAD, "analysis_sha": HEAD,
               "run_id": 10, "attempt": 2, "status": status,
               "files": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}}
    receipt.update(receipt_changes or {})
    files["receipt.json"] = json.dumps(receipt).encode()
    files.update(extra or {})
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as stream:
        for name, data in files.items():
            stream.writestr(name, data)
    return out.getvalue()


class EvidenceTests(unittest.TestCase):
    def test_complete_and_unavailable_are_distinct_valid_states(self):
        for status in ("complete", "unavailable", "not_requested"):
            receipt, text = reporter.evidence(archive(status=status), RUN, LINKED)
            self.assertEqual(receipt["status"], status)
            self.assertEqual(text, "Safe report")

    def test_mismatched_identity_and_missing_evidence_are_refused(self):
        for change in ({"base": "invalid"}, {"target_base": HEAD}, {"head": BASE}, {"run_id": 11}, {"attempt": 1}, {"schema": "other"},
                       {"status": "pass"}, {"files": {}}, {"status": "complete"}):
            with self.subTest(change=change), self.assertRaises(reporter.Refusal):
                reporter.evidence(archive(change), RUN, LINKED)

    def test_paths_links_and_duplicate_zip_members_cannot_reach_extraction(self):
        for name in ("../summary.md", "/summary.md", "a/b", "receipt.json/"):
            with self.subTest(name=name), self.assertRaises(reporter.Refusal):
                reporter.evidence(archive(extra={name: b"attack"}), RUN, LINKED)
        raw = io.BytesIO(archive())
        with zipfile.ZipFile(raw, "a") as stream:
            info = zipfile.ZipInfo("base-lint.json")
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            stream.writestr(info, "outside")
        with self.assertRaises(reporter.Refusal):
            reporter.evidence(raw.getvalue(), RUN, LINKED)
        raw = io.BytesIO(archive())
        with zipfile.ZipFile(raw, "a") as stream:
            with self.assertWarns(UserWarning):
                stream.writestr("summary.md", "replacement")
        with self.assertRaises(reporter.Refusal):
            reporter.evidence(raw.getvalue(), RUN, LINKED)

    def test_expansion_and_content_hash_limits(self):
        with mock.patch.object(reporter, "MAX_EXPANDED", 5), self.assertRaises(reporter.Refusal):
            reporter.evidence(archive(), RUN, LINKED)
        raw = archive({"files": {"summary.md": "0"*64}})
        with self.assertRaises(reporter.Refusal):
            reporter.evidence(raw, RUN, LINKED)

    def test_comment_data_cannot_escape_code_fence_or_mention(self):
        receipt, _ = reporter.evidence(archive(), RUN, LINKED)
        body = reporter.comment_body(REPO, RUN, receipt, "&#96;&#96;&#96;\n&#64;everyone\n<script>\n[x](https://example.org)\n\x00\u202e")
        self.assertIn("````text", body)
        self.assertNotIn("@everyone", body)
        self.assertNotIn("\x00", body)
        self.assertNotIn("\u202e", body)
        self.assertIn("not instructions", body)
        with mock.patch.object(reporter, "MAX_SUMMARY", 4):
            self.assertIn("Comment truncated", reporter.comment_body(REPO, RUN, receipt, "too long"))


class AssociationTests(unittest.TestCase):
    def test_github_association_is_the_only_destination(self):
        self.assertEqual(reporter.association(RUN, REPO)["number"], 7)
        for edit in ({"event": "push"}, {"path": ".github/workflows/other.yml"}, {"status": "in_progress"},
                     {"conclusion": "cancelled"}, {"pull_requests": []}, {"pull_requests": [LINKED, LINKED]},
                     {"head_sha": BASE}, {"head_repository": {"id": 3}}):
            with self.subTest(edit=edit), self.assertRaises(reporter.Refusal):
                reporter.association({**RUN, **edit}, REPO)

    def test_closed_moved_head_and_changed_repo_are_stale(self):
        self.assertTrue(reporter.current(PULL, LINKED, REPO))
        for edit in ({"state": "closed"}, {"head": {"sha": BASE, "repo": {"id": 2}}},
                     {"head": {"sha": HEAD, "repo": {"id": 3}}},
                     {"base": {"ref": "other", "repo": {"full_name": REPO}}}):
            self.assertFalse(reporter.current({**PULL, **edit}, LINKED, REPO))


class PublishTests(unittest.TestCase):
    def api_fixture(self, comments=(), pull=None, raw=None):
        # A strict URL-to-response fixture records every attempted API mutation.
        # Unexpected reads or writes fail this test instead of simulating success.
        raw = raw or archive()
        artifact = {"id": 20, "name": "structural-review-10-2", "expired": False,
                    "size_in_bytes": len(raw), "digest": "sha256:"+hashlib.sha256(raw).hexdigest()}
        reads = {"/actions/runs/10": RUN, "/actions/workflows/code.yml": {"id": 3},
                 "/pulls/7": pull or PULL, "/compare/"+BASE+"..."+HEAD: {"merge_base_commit": {"sha": BASE}}, "/actions/runs/10/artifacts?per_page=100&page=1": {"artifacts": [artifact]},
                 "/actions/artifacts/20/zip": raw, "/issues/7/comments?per_page=100&page=1": list(comments)}
        calls = []
        def api(path, method="GET", body=None, binary=False):
            suffix = path.removeprefix("repos/"+REPO)
            calls.append((suffix, method, body))
            if method != "GET":
                self.assertIn((suffix, method), (("/issues/7/comments", "POST"), ("/issues/comments/30", "PATCH")))
                return {"id": 30}
            self.assertIn(suffix, reads)
            return copy.deepcopy(reads[suffix])
        return api, calls, reads

    def test_initial_post_and_update_do_not_append_duplicate_comments(self):
        api, calls, _ = self.api_fixture()
        with mock.patch.object(reporter, "api", side_effect=api):
            reporter.publish(REPO, 10, 2)
        self.assertEqual([c[1] for c in calls if c[1] != "GET"], ["POST"])
        body = calls[-1][2]["body"]
        old = {"id": 30, "user": {"login": "github-actions[bot]"}, "body": body.replace("Safe report", "Old report")}
        api, calls, _ = self.api_fixture([old])
        with mock.patch.object(reporter, "api", side_effect=api):
            reporter.publish(REPO, 10, 2)
        self.assertEqual([c[1] for c in calls if c[1] != "GET"], ["PATCH"])
        old["body"] = body
        api, calls, _ = self.api_fixture([old])
        with mock.patch.object(reporter, "api", side_effect=api):
            self.assertIn("already current", reporter.publish(REPO, 10, 2))
        self.assertTrue(all(c[1] == "GET" for c in calls))

    def test_old_attempt_closed_pr_newer_report_and_late_head_never_write(self):
        api, calls, _ = self.api_fixture()
        with mock.patch.object(reporter, "api", side_effect=api):
            self.assertIn("superseded attempt", reporter.publish(REPO, 10, 1))
        self.assertTrue(all(c[1] == "GET" for c in calls))
        old = {"id": 30, "user": {"login": "github-actions[bot]"},
               "body": reporter.MARKER+"\n<!-- structural-review-run:11:1 -->"}
        for comments, pull in (([old], None), ([], {**PULL, "state": "closed"})):
            api, calls, _ = self.api_fixture(comments, pull)
            with mock.patch.object(reporter, "api", side_effect=api):
                reporter.publish(REPO, 10, 2)
            self.assertTrue(all(c[1] == "GET" for c in calls))
        api, calls, _ = self.api_fixture()
        n = 0
        def race(path, *args, **kwargs):
            nonlocal n
            if path.endswith("/pulls/7"):
                n += 1
                if n == 2:
                    return {**PULL, "head": {"sha": BASE, "repo": {"id": 2}}}
            return api(path, *args, **kwargs)
        with mock.patch.object(reporter, "api", side_effect=race):
            self.assertIn("changed during", reporter.publish(REPO, 10, 2))
        self.assertTrue(all(c[1] == "GET" for c in calls))

    def test_artifact_and_workflow_identity_and_human_comments(self):
        human = {"id": 99, "user": {"login": "contributor"}, "body": reporter.MARKER+"\nnot a bot report"}
        api, calls, reads = self.api_fixture([human])
        with mock.patch.object(reporter, "api", side_effect=api):
            reporter.publish(REPO, 10, 2)
        self.assertEqual(calls[-1][1], "POST")
        for key, value in (("/actions/workflows/code.yml", {"id": 4}),
                           ("/actions/runs/10/artifacts?per_page=100&page=1", {"artifacts": []})):
            api, calls, reads = self.api_fixture()
            reads[key] = value
            with mock.patch.object(reporter, "api", side_effect=api):
                try:
                    reporter.publish(REPO, 10, 2)
                except reporter.Refusal:
                    pass
            self.assertTrue(all(c[1] == "GET" for c in calls))

    def test_target_advancement_uses_github_merge_base_not_unrelated_removals(self):
        ancestor = "c"*40
        api, calls, reads = self.api_fixture(raw=archive({"base": ancestor}))
        with mock.patch.object(reporter, "api", side_effect=api), self.assertRaises(reporter.Refusal):
            reporter.publish(REPO, 10, 2)
        self.assertTrue(all(c[1] == "GET" for c in calls))
        api, calls, reads = self.api_fixture(raw=archive({"base": ancestor}))
        reads["/compare/"+BASE+"..."+HEAD] = {"merge_base_commit": {"sha": ancestor}}
        with mock.patch.object(reporter, "api", side_effect=api):
            reporter.publish(REPO, 10, 2)
        self.assertEqual(calls[-1][1], "POST")
        self.assertIn(ancestor, calls[-1][2]["body"])

    def test_analysis_merge_must_have_both_recorded_parents(self):
        merge = "d"*40
        for parents, accepted in (([BASE, HEAD], True), ([BASE], False), ([BASE, "c"*40], False)):
            api, calls, reads = self.api_fixture(raw=archive({"analysis_sha": merge}))
            reads["/git/commits/"+merge] = {"parents": [{"sha": p} for p in parents]}
            with mock.patch.object(reporter, "api", side_effect=api):
                if accepted:
                    reporter.publish(REPO, 10, 2)
                else:
                    with self.assertRaises(reporter.Refusal):
                        reporter.publish(REPO, 10, 2)
            self.assertEqual(any(c[1] != "GET" for c in calls), accepted)

    def test_dry_run_and_failed_write_are_never_retried(self):
        api, calls, _ = self.api_fixture()
        with mock.patch.object(reporter, "api", side_effect=api):
            self.assertIn("Safe report", reporter.publish(REPO, 10, 2, dry_run=True))
        self.assertTrue(all(c[1] == "GET" for c in calls))
        def failing_write(path, method="GET", body=None, **kwargs):
            if method != "GET":
                calls.append((path, method, None))
                raise OSError("lost response")
            return api(path, method, body, **kwargs)
        calls.clear()
        with mock.patch.object(reporter, "api", side_effect=failing_write), self.assertRaises(OSError):
            reporter.publish(REPO, 10, 2)
        self.assertEqual(sum(c[1] != "GET" for c in calls), 1)


class WorkflowTests(unittest.TestCase):
    def test_producer_reuses_code_job_and_publisher_runs_trusted_code_without_cache(self):
        root = Path(__file__).resolve().parents[2]
        code = (root/".github/workflows/code.yml").read_text()
        workflow = (root/".github/workflows/structural-review.yml").read_text()
        self.assertIn("Collect report-only structural review evidence", code)
        self.assertIn("continue-on-error: true", code)
        self.assertNotIn("pull-requests: write", code)
        self.assertIn("workflow_run:", workflow)
        self.assertIn("workflows: [Code checks]", workflow)
        self.assertIn("pull-requests: write", workflow)
        self.assertNotIn("pull_request_target", workflow)
        self.assertNotIn("actions/cache", workflow)
        self.assertNotIn("setup-go", workflow)
        self.assertNotIn("head.sha", workflow)
        self.assertIn("persist-credentials: false", workflow)
