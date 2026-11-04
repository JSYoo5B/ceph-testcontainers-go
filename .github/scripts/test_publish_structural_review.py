"""Publisher validation using explicit GitHub response fixtures, without writes.

The fake API below substitutes only remote JSON/zip responses to exercise the
write authorization and idempotence boundary. It does not model GitHub execution.
"""
import copy
import contextlib
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
       "path": ".github/workflows/structural-collect.yml", "repository": {"id": 1, "full_name": REPO},
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
            receipt, text, _ = reporter.evidence(archive(status=status), RUN, LINKED)
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

    def test_comment_renders_markdown_without_an_outer_code_block(self):
        receipt, _, _ = reporter.evidence(archive(), RUN, LINKED)
        text = "### Owners\n\n| Owner | Fields |\n| --- | --- |\n| ceph.Container | 21 |\n\nHead `abc`.\n"
        body = reporter.comment_body(REPO, RUN, receipt, text)
        self.assertIn(text, body)
        self.assertNotIn("```", body)
        self.assertIn("not instructions", body)

    def test_markdown_keeps_cell_entities_and_neutralizes_active_source_markup(self):
        text = "### Report\n\n| a&#124;b | &#96;name&#96; |\n\n```text\n~~~\n&#64;everyone @user <script> [x](https://example.org) ![img](url)\n\x00\u202e&#x202e;"
        clean = reporter.report_markdown(text)
        self.assertIn("a&#124;b", clean)
        self.assertIn("&#96;name&#96;", clean)
        self.assertIn("&#96;&#96;&#96;text", clean)
        self.assertIn("&#126;&#126;&#126;", clean)
        self.assertIn("&lt;script&gt;", clean)
        self.assertNotIn("@", clean)
        self.assertNotIn("[x]", clean)
        self.assertNotIn("![img]", clean)
        self.assertNotIn("\x00", clean)
        self.assertNotIn("\u202e", clean)
        self.assertNotIn("&#x202e;", clean)

    def test_details_are_collapsed_by_the_publisher_and_escaped(self):
        receipt, _, _ = reporter.evidence(archive(), RUN, LINKED)
        body = reporter.comment_body(REPO, RUN, receipt, "Visible\n", "Hidden <b>x</b> @user\n")
        self.assertIn("<details><summary>Collection details</summary>", body)
        self.assertIn("Hidden &lt;b&gt;x&lt;/b&gt;", body)
        self.assertNotIn("@user", body)
        self.assertLess(body.index("Visible"), body.index("<details>"))
        self.assertEqual(body.count("<details>"), 1)

    def test_comment_truncates_after_escaping_at_a_complete_line(self):
        receipt, _, _ = reporter.evidence(archive(), RUN, LINKED)
        with mock.patch.object(reporter, "MAX_SUMMARY", 8):
            body = reporter.comment_body(REPO, RUN, receipt, "ok\n<<<<")
        self.assertIn("ok\n", body)
        self.assertNotIn("&lt", body)
        self.assertIn("Comment truncated", body)


class AssociationTests(unittest.TestCase):
    def test_github_association_is_the_only_destination(self):
        self.assertEqual(reporter.association(RUN, REPO)["number"], 7)
        for edit in ({"event": "push"}, {"path": ".github/workflows/other.yml"}, {"status": "in_progress"},
                     {"conclusion": "cancelled"}, {"pull_requests": [LINKED, LINKED]},
                     {"head_sha": BASE}, {"head_repository": {"id": 3}}):
            with self.subTest(edit=edit), self.assertRaises(reporter.Refusal):
                reporter.association({**RUN, **edit}, REPO)
        # GitHub drops the association of a merged or closed PR: nothing to
        # publish, not a refusal that fails the publisher.
        self.assertIsNone(reporter.association({**RUN, "pull_requests": []}, REPO))

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
        reads = {"/actions/runs/10": RUN, "/actions/workflows/structural-collect.yml": {"id": 3},
                 "/pulls/7": pull or PULL, "/compare/"+BASE+"..."+HEAD: {"merge_base_commit": {"sha": BASE}}, "/actions/runs/10/artifacts?per_page=100&page=1": {"artifacts": [artifact]},
                 "/actions/artifacts/20/zip": raw, "/issues/7/comments?per_page=100&page=1": list(comments)}
        calls = []
        def api(path, method="GET", body=None, binary=False):
            suffix = path.removeprefix("repos/"+REPO)
            calls.append((suffix, method, body))
            if method != "GET":
                self.assertIn((suffix, method), (("/issues/7/comments", "POST"), ("/issues/comments/30", "PATCH"),
                                                 ("/issues/comments/30", "DELETE")))
                return {"id": 30}
            self.assertIn(suffix, reads)
            return copy.deepcopy(reads[suffix])
        return api, calls, reads

    def test_a_merged_pr_run_publishes_nothing_and_does_not_fail(self):
        api, calls, reads = self.api_fixture()
        reads["/actions/runs/10"] = {**RUN, "pull_requests": []}
        with mock.patch.object(reporter, "api", side_effect=api):
            self.assertIn("no open PR", reporter.publish(REPO, 10, 2))
        self.assertTrue(all(c[1] == "GET" for c in calls))

    def test_an_unmeasured_head_withdraws_only_an_older_bot_report(self):
        def stamped(run, attempt, login="github-actions[bot]"):
            return {"id": 30, "user": {"login": login},
                    "body": reporter.MARKER + "\n<!-- structural-review-run:%d:%d -->\nold head" % (run, attempt)}
        for comments, want, deleted in (([], "nothing published", False), ([stamped(9, 1)], "withdrawn", True),
                                        ([stamped(10, 2)], "newer report is kept", False),
                                        ([stamped(11, 1)], "newer report is kept", False),
                                        ([stamped(9, 1, "someone")], "nothing published", False)):
            with self.subTest(comments=comments):
                api, calls, reads = self.api_fixture(comments)
                reads["/actions/runs/10/artifacts?per_page=100&page=1"] = {"artifacts": []}
                with mock.patch.object(reporter, "api", side_effect=api):
                    self.assertIn(want, reporter.publish(REPO, 10, 2))
                self.assertEqual([c[:2] for c in calls if c[1] != "GET"], [("/issues/comments/30", "DELETE")] if deleted else [])
        # A head that moved during the run withdraws nothing.
        api, calls, reads = self.api_fixture([stamped(9, 1)])
        reads["/actions/runs/10/artifacts?per_page=100&page=1"] = {"artifacts": []}
        first = True
        def moving(path, method="GET", body=None, binary=False):
            nonlocal first
            if path.endswith("/pulls/7") and not first:
                return {**PULL, "head": {"sha": BASE, "repo": PULL["head"]["repo"]}}
            if path.endswith("/pulls/7"):
                first = False
            return api(path, method, body, binary)
        with mock.patch.object(reporter, "api", side_effect=moving):
            self.assertIn("nothing withdrawn", reporter.publish(REPO, 10, 2))
        self.assertTrue(all(c[1] == "GET" for c in calls))

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

    def test_late_rerun_and_ineligible_completion_prevent_post_patch_and_delete(self):
        old = {"id": 30, "user": {"login": "github-actions[bot]"},
               "body": reporter.MARKER+"\n<!-- structural-review-run:9:1 -->\nOld report"}
        late_states = [
            {"run_attempt": 3, "status": "in_progress", "conclusion": None},
            {"run_attempt": 3, "status": "completed", "conclusion": "success"},
            {"status": "in_progress", "conclusion": None},
            {"conclusion": "cancelled"},
            {"conclusion": "skipped"},
            {"workflow_id": 99},
            {"head_sha": BASE},
            {"pull_requests": [{**LINKED, "number": 8}]},
        ]
        # DELETE: no artifact for this attempt and an older bot report to withdraw.
        for operation, comments, outcome in (("POST", [], "nothing published"), ("PATCH", [old], "nothing published"),
                                             ("DELETE", [old], "nothing withdrawn")):
            for late in late_states:
                with self.subTest(operation=operation, late=late):
                    api, calls, reads = self.api_fixture(comments)
                    if operation == "DELETE":
                        reads["/actions/runs/10/artifacts?per_page=100&page=1"] = {"artifacts": []}
                    pull_reads = 0
                    def rerun(path, *args, **kwargs):
                        nonlocal pull_reads
                        if path.endswith("/pulls/7"):
                            pull_reads += 1
                            if pull_reads == 2:
                                reads["/actions/runs/10"] = {**RUN, **late}
                        return api(path, *args, **kwargs)
                    with mock.patch.object(reporter, "api", side_effect=rerun):
                        try:
                            result = reporter.publish(REPO, 10, 2)
                            self.assertIn(outcome, result)
                        except reporter.Refusal:
                            pass
                    self.assertEqual(sum(c[0] == "/actions/runs/10" for c in calls), 2)
                    self.assertTrue(all(c[1] == "GET" for c in calls), calls)

    def test_late_run_read_failure_does_not_write(self):
        old = {"id": 30, "user": {"login": "github-actions[bot]"},
               "body": reporter.MARKER+"\n<!-- structural-review-run:9:1 -->\nOld report"}
        for operation, comments in (("POST", []), ("PATCH", [old]), ("DELETE", [old])):
            with self.subTest(operation=operation):
                api, calls, fixture = self.api_fixture(comments)
                if operation == "DELETE":
                    fixture["/actions/runs/10/artifacts?per_page=100&page=1"] = {"artifacts": []}
                reads = 0
                def unavailable(path, *args, **kwargs):
                    nonlocal reads
                    if path.endswith("/actions/runs/10"):
                        reads += 1
                        if reads == 2:
                            raise OSError("source run read unavailable")
                    return api(path, *args, **kwargs)
                with mock.patch.object(reporter, "api", side_effect=unavailable), self.assertRaises(OSError):
                    reporter.publish(REPO, 10, 2)
                self.assertEqual(reads, 2)
                self.assertTrue(all(c[1] == "GET" for c in calls))

    def test_artifact_and_workflow_identity_and_human_comments(self):
        human = {"id": 99, "user": {"login": "owner"}, "body": reporter.MARKER+"\nnot a bot report"}
        api, calls, reads = self.api_fixture([human])
        with mock.patch.object(reporter, "api", side_effect=api):
            reporter.publish(REPO, 10, 2)
        self.assertEqual(calls[-1][1], "POST")
        for key, value in (("/actions/workflows/structural-collect.yml", {"id": 4}),
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

    def test_target_can_advance_between_event_and_ci_checkout(self):
        merge, engine_base, target, foreign = "d"*40, "c"*40, "e"*40, "f"*40
        for event_ancestor, target_ancestor, accepted in ((BASE, engine_base, True), (foreign, engine_base, False), (BASE, foreign, False)):
            api, calls, reads = self.api_fixture(raw=archive({"analysis_sha": merge}))
            reads["/git/commits/"+merge] = {"parents": [{"sha": engine_base}, {"sha": HEAD}]}
            reads["/compare/"+BASE+"..."+engine_base] = {"merge_base_commit": {"sha": event_ancestor}}
            reads["/git/ref/heads/main"] = {"object": {"sha": target}}
            reads["/compare/"+engine_base+"..."+target] = {"merge_base_commit": {"sha": target_ancestor}}
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


class CommandFailureTests(unittest.TestCase):
    def test_personal_cli_cannot_publish_a_human_report(self):
        with mock.patch("sys.argv", ["publisher", "--repository", REPO, "--run", "10", "--attempt", "2"]), \
                mock.patch.object(reporter, "publish") as publish, \
                mock.patch.dict(reporter.os.environ, {"GITHUB_ACTIONS": "false"}), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(reporter.main(), 1)
        publish.assert_not_called()

    def test_lost_write_response_never_claims_no_comment_was_created(self):
        output = io.StringIO()
        # A lost response after a write can leave a committed comment. The CLI
        # must preserve that uncertainty and must not expose transport details.
        with mock.patch("sys.argv", ["publisher", "--repository", REPO, "--run", "10", "--attempt", "2"]), \
                mock.patch.object(reporter, "publish", side_effect=OSError("private transport detail")), \
                mock.patch.dict(reporter.os.environ, {"GITHUB_ACTIONS": "true"}), \
                contextlib.redirect_stdout(output):
            self.assertEqual(reporter.main(), 1)
        self.assertIn("publication not confirmed", output.getvalue())
        self.assertNotIn("not published", output.getvalue())
        self.assertNotIn("private transport detail", output.getvalue())


class WorkflowTests(unittest.TestCase):
    def test_producer_is_its_own_read_only_workflow_and_publisher_runs_trusted_code_without_cache(self):
        root = Path(__file__).resolve().parents[2]
        code = (root/".github/workflows/code.yml").read_text()
        producer = (root/".github/workflows/structural-collect.yml").read_text()
        workflow = (root/".github/workflows/structural-review.yml").read_text()
        # The evidence starts with the pull request event, not after Code checks.
        self.assertNotIn("structural", code.lower())
        self.assertIn("name: Structural review evidence", producer)
        self.assertIn("on:\n  pull_request:\n\npermissions:\n  contents: read\n", producer)
        self.assertNotIn("write", producer)
        self.assertNotIn("pull_request_target", producer)
        self.assertIn("continue-on-error: true", producer)
        steps = producer.split("\n      - ")
        self.assertIn("name: Classify structural review eligibility", steps[2])
        for step in steps[3:-1]:
            self.assertIn("if: steps.scope.outputs.eligible == 'true'", step)
        self.assertIn("steps.structural.outcome != 'skipped'", steps[-1])
        self.assertIn(reporter.SOURCE_WORKFLOW, str(root/reporter.SOURCE_WORKFLOW))
        self.assertTrue((root/reporter.SOURCE_WORKFLOW).is_file())
        command = (root/".github/workflows/ci-command.yml").read_text()
        manual = command.split("\n  structural-report:\n", 1)[1]
        self.assertIn("if: github.event_name == 'workflow_dispatch'", manual)
        self.assertIn("ref: ${{ github.sha }}", manual)
        self.assertNotIn("setup-go", manual)
        self.assertNotIn("actions/cache", manual)
        self.assertIn("group: structural-report-${{ needs.structural-source.outputs.sha }}", manual)
        self.assertIn("group: structural-report-${{ github.event.workflow_run.head_sha }}", workflow)
        self.assertIn("workflow_run:", workflow)
        self.assertIn("workflows: [Structural review evidence]", workflow)
        self.assertIn("    types: [completed]\n", workflow)
        self.assertIn("    branches-ignore: [main]\n", workflow)
        self.assertIn("if: github.event.workflow_run.event == 'pull_request'", workflow)
        self.assertIn("pull-requests: write", workflow)
        self.assertNotIn("pull_request_target", workflow)
        self.assertNotIn("actions/cache", workflow)
        self.assertNotIn("setup-go", workflow)
        self.assertNotIn("head.sha", workflow)
        self.assertIn("persist-credentials: false", workflow)
