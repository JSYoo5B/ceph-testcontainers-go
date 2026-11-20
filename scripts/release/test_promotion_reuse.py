"""Exercise the promotion reuse decision against in-memory GitHub facts."""
from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import subprocess
import unittest
from unittest.mock import patch

import git_flow as flow
import promotion_reuse as reuse

REPO = flow.REPO
BASE, HEAD, TEST, MERGE, TREE = "a" * 40, "b" * 40, "c" * 40, "d" * 40, "e" * 40
DIGEST = "f" * 64


class Facts:
    """A merged promotion PR whose test merge and protected merge agree."""

    def __init__(self):
        self.commits = {MERGE: self.commit(MERGE, TREE, [BASE, HEAD]), TEST: self.commit(TEST, TREE, [BASE, HEAD])}
        self.pulls = [{"number": 7, "merged_at": "2026-11-20T12:35:52Z", "merge_commit_sha": MERGE,
                       "base": {"ref": "main", "repo": {"full_name": REPO}},
                       "head": {"ref": "release/1.0", "sha": HEAD, "repo": {"full_name": REPO}}}]
        self.runs = [self.run(40, 9, 2)]
        self.jobs = {40: [self.job(1, "Releases"), self.job(2, "Promotion reuse", "skipped"),
                          self.job(3, "Tentacle short / Plan"),
                          self.job(4, "Tentacle short / rbd_fixtures_locks", retry="success"),
                          self.job(5, "Environment / docker"), self.job(6, reuse.gate("main"))]}
        self.receipts = {40: reuse.candidate_receipt(
            REPO, 7, "main", HEAD, 40, 2, {"sha": TEST, "tree": TREE, "parents": [BASE, HEAD]},
            {"cells": 3, "callers": 2, "digest": DIGEST}, {"20.2.4": {"official": "image"}}, self.jobs[40])}
        self.artifacts = {40: [{"id": 400, "name": reuse.artifact_name(reuse.CANDIDATE, 2), "expired": False}]}

    @staticmethod
    def commit(sha, tree, parents):
        return {"sha": sha, "tree": {"sha": tree}, "parents": [{"sha": parent} for parent in parents]}

    @staticmethod
    def run(number, run_number, attempt, **fields):
        return dict({"id": number, "run_number": run_number, "run_attempt": attempt, "event": "pull_request",
                     "path": reuse.WORKFLOW, "head_sha": HEAD, "head_branch": "release/1.0",
                     "head_repository": {"full_name": REPO}, "status": "completed", "conclusion": "success",
                     "html_url": "https://example.org/runs/" + str(number)}, **fields)

    @staticmethod
    def job(number, name, conclusion="success", retry=None):
        steps = [{"name": "Run the native tests", "conclusion": "success"},
                 {"name": reuse.RETRY_STEP, "conclusion": retry or "skipped"}]
        return {"id": number, "name": name, "status": "completed", "conclusion": conclusion, "run_attempt": 2,
                "steps": steps}

    def api(self, path, *, pages=False):
        path = path.split("?")[0]
        if path.startswith("git/commits/"):
            sha = path.rsplit("/", 1)[1]
            if sha not in self.commits:
                raise subprocess.CalledProcessError(1, "gh", stderr="gh: Not Found (HTTP 404)")
            return self.commits[sha]
        if path == "commits/" + MERGE + "/pulls":
            return self.pulls
        if path == "actions/workflows/release-matrix.yml/runs":
            return self.runs
        parts = path.split("/")
        if parts[:2] == ["actions", "runs"] and parts[-1] == "jobs":
            return self.jobs[int(parts[2])]
        if parts[:2] == ["actions", "runs"] and parts[-1] == "artifacts":
            return self.artifacts.get(int(parts[2]), [])
        raise AssertionError("unexpected request " + path)

    def download(self, artifact):
        return self.receipts[artifact["id"] // 10]

    def decide(self, coverage=DIGEST, branch="main"):
        return reuse.decide(self.api, self.download, REPO, branch, MERGE, coverage)


class QualifyTests(unittest.TestCase):
    def assert_refused(self, facts, pattern, **arguments):
        receipt, reasons = facts.decide(**arguments)
        self.assertTrue(any(pattern in reason for reason in reasons), reasons)
        self.assertEqual(receipt["disposition"], "full-matrix")
        self.assertEqual(receipt["reasons"], reasons)
        self.assertNotIn("source", receipt)

    def test_identical_tree_and_parents_reuse_the_promotion_run(self):
        receipt, reasons = Facts().decide()
        self.assertEqual(reasons, [])
        self.assertEqual(receipt["disposition"], "reused")
        self.assertEqual(receipt["publication"], {"branch": "main", "sha": MERGE, "tree": TREE})
        self.assertEqual(receipt["tested"], {"sha": TEST, "tree": TREE, "parents": [BASE, HEAD]})
        self.assertEqual(receipt["pull_request"], 7)
        self.assertEqual(receipt["source"], {"run_id": 40, "run_attempt": 2, "url": "https://example.org/runs/40"})
        self.assertEqual(receipt["coverage"]["digest"], DIGEST)
        # The first failure that its retry passed stays on record.
        self.assertEqual(receipt["retried"], [{"id": 4, "name": "Tentacle short / rbd_fixtures_locks",
                                               "retry": "success"}])

    def test_a_changed_tree_or_merge_input_runs_the_full_matrix(self):
        facts = Facts()
        facts.commits[MERGE] = facts.commit(MERGE, "1" * 40, [BASE, HEAD])
        self.assert_refused(facts, "different trees")
        facts = Facts()
        facts.commits[MERGE] = facts.commit(MERGE, TREE, ["2" * 40, HEAD])
        self.assert_refused(facts, "different parents")
        # The branch moved before the merge: the test merge had another base.
        facts = Facts()
        facts.commits[TEST] = facts.commit(TEST, TREE, ["2" * 40, HEAD])
        facts.receipts[40]["tested"]["parents"] = ["2" * 40, HEAD]
        self.assert_refused(facts, "different parents")
        facts = Facts()
        facts.commits[MERGE] = facts.commit(MERGE, TREE, [BASE])
        self.assert_refused(facts, "not a merge of two parents")

    def test_a_stale_or_unrelated_pull_request_or_run_is_refused(self):
        facts = Facts()
        facts.pulls = []
        self.assert_refused(facts, "no merged PR")
        facts = Facts()
        facts.pulls[0]["merge_commit_sha"] = "1" * 40
        self.assert_refused(facts, "no merged PR")
        facts = Facts()
        facts.pulls.append(deepcopy(facts.pulls[0]))
        self.assert_refused(facts, "no merged PR")
        facts = Facts()
        facts.pulls[0]["head"]["repo"] = {"full_name": "fork/" + REPO.split("/")[1]}
        self.assert_refused(facts, "not a branch of this repository")
        facts = Facts()
        facts.pulls[0]["head"]["repo"] = None
        self.assert_refused(facts, "not a branch of this repository")
        # A run of an older head of the same PR.
        facts = Facts()
        facts.runs = [facts.run(40, 9, 2, head_sha="1" * 40)]
        self.assert_refused(facts, "not a pull request run")
        for fields, pattern in (({"event": "push"}, "not a pull request run"),
                                ({"path": ".github/workflows/other.yml"}, "not a pull request run"),
                                ({"head_branch": "feature"}, "did not belong"),
                                ({"head_repository": {"full_name": "fork/x"}}, "did not belong")):
            with self.subTest(fields=fields):
                facts = Facts()
                facts.runs = [facts.run(40, 9, 2, **fields)]
                self.assert_refused(facts, pattern)

    def test_an_incorrect_target_is_refused(self):
        facts = Facts()
        facts.pulls[0]["base"]["ref"] = "develop"
        self.assert_refused(facts, "another branch or repository")
        self.assert_refused(Facts(), "does not reuse a promotion run", branch="support/1.0")
        facts = Facts()
        facts.receipts[40]["base"] = "release/1.0"
        self.assert_refused(facts, "another PR, target or head")

    def test_a_recorded_head_without_checkout_proof_is_refused(self):
        # The run reports the merged head, but nothing proves what it tested.
        facts = Facts()
        facts.artifacts[40] = []
        self.assert_refused(facts, "no usable candidate receipt")
        # A receipt that names the PR head itself instead of a test merge.
        facts = Facts()
        facts.commits[HEAD] = facts.commit(HEAD, "1" * 40, ["3" * 40])
        facts.receipts[40]["tested"] = {"sha": HEAD}
        self.assert_refused(facts, "disagrees with its tested commit")
        # A receipt that names a commit the repository does not have.
        facts = Facts()
        del facts.commits[TEST]
        self.assert_refused(facts, "could not be read")
        # A receipt whose claimed tree is not the tested commit's.
        facts = Facts()
        facts.commits[TEST] = facts.commit(TEST, "1" * 40, [BASE, HEAD])
        self.assert_refused(facts, "disagrees with its tested commit")
        facts = Facts()
        facts.receipts[40]["tested"]["sha"] = "not a sha"
        self.assert_refused(facts, "names no tested commit")

    def test_incomplete_cancelled_or_failed_runs_are_refused(self):
        for conclusion in ("failure", "cancelled", "timed_out", None):
            with self.subTest(conclusion=conclusion):
                facts = Facts()
                facts.runs = [facts.run(40, 9, 2, conclusion=conclusion,
                                        status="completed" if conclusion else "in_progress")]
                self.assert_refused(facts, "has not passed")
        for conclusion in ("failure", "cancelled", "skipped"):
            with self.subTest(job=conclusion):
                facts = Facts()
                facts.jobs[40][3]["conclusion"] = conclusion
                self.assert_refused(facts, "did not succeed")
        facts = Facts()
        facts.jobs[40] = facts.jobs[40][:-1]
        self.assert_refused(facts, "missing or unsuccessful")
        facts = Facts()
        facts.jobs[40][-1]["run_attempt"] = 1
        self.assert_refused(facts, "missing or unsuccessful")
        # A retry that failed fails its job; the receipt alone cannot hide it.
        facts = Facts()
        facts.jobs[40][3] = facts.job(4, "Tentacle short / rbd_fixtures_locks", "failure", retry="failure")
        self.assert_refused(facts, "did not succeed")

    def test_expired_inconsistent_or_foreign_records_are_refused(self):
        facts = Facts()
        facts.artifacts[40][0]["expired"] = True
        self.assert_refused(facts, "no usable candidate receipt")
        # The receipt of an earlier attempt of the same run.
        facts = Facts()
        facts.artifacts[40][0]["name"] = reuse.artifact_name(reuse.CANDIDATE, 1)
        self.assert_refused(facts, "no usable candidate receipt")
        for field, value, pattern in (("run_id", 41, "another run or attempt"),
                                      ("run_attempt", 1, "another run or attempt"),
                                      ("pull_request", 8, "another PR, target or head"),
                                      ("head_sha", "1" * 40, "another PR, target or head"),
                                      ("repository", "o/r", "another repository or workflow"),
                                      ("workflow", "x.yml", "another repository or workflow"),
                                      ("disposition", "failed", "does not record a validated matrix"),
                                      ("schema", "other/v1", "no usable candidate receipt"),
                                      ("kind", "reuse", "no usable candidate receipt")):
            with self.subTest(field=field):
                facts = Facts()
                facts.receipts[40][field] = value
                self.assert_refused(facts, pattern)
        self.assert_refused(Facts(), "another matrix or image configuration", coverage="0" * 64)
        facts = Facts()
        facts.receipts[40]["coverage"] = {}
        self.assert_refused(facts, "another matrix or image configuration", coverage=None)

    def test_the_newest_successful_run_is_the_evidence(self):
        facts = Facts()
        facts.runs = [facts.run(41, 10, 1, conclusion="cancelled"), facts.run(40, 9, 2), facts.run(39, 8, 1)]
        receipt, reasons = facts.decide()
        self.assertEqual(reasons, [])
        self.assertEqual(receipt["source"]["run_id"], 40)
        # Without a successful run the newest one explains the refusal.
        facts.runs = [facts.run(41, 10, 1, conclusion="cancelled"), facts.run(40, 9, 2, conclusion="failure")]
        facts.jobs[41], facts.artifacts[41] = [], []
        self.assert_refused(facts, "has not passed")

    def test_an_unreadable_fact_is_a_reason_not_an_eligibility(self):
        facts = Facts()

        def broken(path, *, pages=False):
            if path.startswith("actions/workflows/"):
                raise subprocess.CalledProcessError(1, "gh", stderr="gh: Server Error (HTTP 502)")
            return facts.api(path, pages=pages)

        receipt, reasons = reuse.decide(broken, facts.download, REPO, "main", MERGE, DIGEST)
        self.assertEqual(receipt["disposition"], "full-matrix")
        self.assertEqual(len(reasons), 1)
        self.assertIn("could not be read", reasons[0])

    def test_retried_keeps_only_retries_that_ran(self):
        jobs = [Facts.job(1, "a"), Facts.job(3, "c", retry="failure"), Facts.job(2, "b", retry="success"),
                {"id": 4, "name": "d"}]
        self.assertEqual(reuse.retried(jobs), [{"id": 2, "name": "b", "retry": "success"},
                                               {"id": 3, "name": "c", "retry": "failure"}])


class EvidenceTests(unittest.TestCase):
    """git_flow evidence audits how the push run of main was qualified."""

    def audit(self, facts, jobs, recorded):
        run = {"id": 50, "run_attempt": 1}
        artifacts = [] if recorded is None else [{"id": 500, "name": reuse.artifact_name(reuse.REUSE, 1)}]

        def api(path, *, pages=False):
            if path.startswith("actions/runs/50/artifacts"):
                return artifacts
            return facts.api(path, pages=pages)

        def download(artifact):
            return recorded if artifact["id"] == 500 else facts.download(artifact)

        output = StringIO()
        with patch.object(flow, "api", side_effect=api), \
                patch.object(reuse, "download_json", side_effect=download), redirect_stdout(output):
            flow.promotion_evidence("main", MERGE, run, jobs)
        return output.getvalue()

    def job(self, name, conclusion="success"):
        return {"name": name, "conclusion": conclusion}

    def test_a_verified_reuse_names_its_source_run(self):
        facts = Facts()
        recorded, _ = facts.decide()
        printed = self.audit(facts, [self.job(reuse.REUSE_JOB), self.job(reuse.gate("main"))], recorded)
        self.assertIn("reuses PR #7 run https://example.org/runs/40/attempts/2", printed)
        self.assertIn("tested " + TEST, printed)
        self.assertIn("1 first failures passed their retry", printed)

    def test_a_recorded_reuse_that_no_longer_verifies_is_refused(self):
        facts = Facts()
        recorded, _ = facts.decide()
        jobs = [self.job(reuse.REUSE_JOB), self.job(reuse.gate("main"))]
        stale = Facts()
        stale.commits[MERGE] = stale.commit(MERGE, "1" * 40, [BASE, HEAD])
        with self.assertRaisesRegex(ValueError, "does not verify.*different trees"):
            self.audit(stale, jobs, recorded)
        forged = deepcopy(recorded)
        forged["source"]["run_id"] = 39
        with self.assertRaisesRegex(ValueError, "source differs"):
            self.audit(facts, jobs, forged)
        with self.assertRaisesRegex(ValueError, "executed matrix jobs"):
            self.audit(facts, [*jobs, self.job("Tentacle short / pools")], recorded)

    def test_a_full_matrix_is_reported_with_the_reason_reuse_was_refused(self):
        facts = Facts()
        facts.artifacts[40] = []
        refused, reasons = facts.decide()
        jobs = [self.job(reuse.REUSE_JOB), self.job("Tentacle short / pools"), self.job(reuse.gate("main"))]
        printed = self.audit(facts, jobs, refused)
        self.assertIn("validated by its own full matrix: " + reasons[0], printed)
        # No reuse job at all: an ordinary push matrix.
        self.assertIn("validated by its own full matrix", self.audit(facts, jobs[1:], None))

    def test_a_run_that_did_neither_is_refused(self):
        facts = Facts()
        with self.assertRaisesRegex(ValueError, "neither executed the matrix nor recorded"):
            self.audit(facts, [self.job(reuse.gate("main"))], None)
        # A reuse job that passed without leaving its receipt.
        with self.assertRaisesRegex(ValueError, "neither executed the matrix nor recorded"):
            self.audit(facts, [self.job(reuse.REUSE_JOB), self.job(reuse.gate("main"))], None)
        # Skipped matrix jobs are not an execution.
        refused, _ = Facts().decide(coverage="0" * 64)
        with self.assertRaisesRegex(ValueError, "neither executed the matrix nor recorded"):
            self.audit(facts, [self.job(reuse.REUSE_JOB), self.job("Tentacle short / pools", "skipped")], refused)


if __name__ == "__main__":
    unittest.main()
