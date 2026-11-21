"""Exercise the promotion reuse decision against in-memory GitHub facts."""
from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
from pathlib import Path
import subprocess
import sys
import tempfile
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
        # The runtime jobs: 4 passed its retry after a native failure.
        self.entries = [self.entry(4, retried=True), self.entry(5, ceph=False)]
        self.evidence = {40: reuse.encode_evidence(self.entries)}
        self.receipts = {40: reuse.candidate_receipt(
            REPO, 7, "main", HEAD, 40, 2, {"sha": TEST, "tree": TREE, "parents": [BASE, HEAD]},
            {"cells": 3, "callers": 2, "digest": DIGEST}, {"20.2.4": {"official": "image"}}, self.jobs[40],
            reuse.evidence_summary(self.evidence[40]))}
        self.artifacts = {40: [{"id": 400, "name": reuse.artifact_name(reuse.CANDIDATE, 2), "expired": False},
                               # The jobs' own artifacts are gone long before the receipt.
                               {"id": 401, "name": "tag-scenario-short-rbd_fixtures_locks-1-20.2.4", "expired": True},
                               {"id": 402, "name": "scenario-images-short-rbd_fixtures_locks-1-20.2.4", "expired": True}]}

    @staticmethod
    def entry(job, retried=False, ceph=True, **changes):
        """One job's evidence: its history row and, if it was retried, the
        retry's report. changes replace top-level fields of the row."""
        phases = ["modules", "compile", "storage", "docker", "images", "test_images", "storage_ready", "native",
                  "cleanup"]
        row = {"schema": reuse.ROW_SCHEMA,
               "execution_key": {"repository": REPO, "run_id": "40", "run_attempt": "2", "job_id": str(job),
                                 "profile_id": "short-rbd_fixtures_locks-1", "network": "host"},
               "source": {"checkout_commit": TEST, "checkout_tree": TREE},
               "profile": {"requires_ceph": ceph},
               "build": {"build_key": {"go_version": "go1.26.11"}},
               "evidence": {"files": {"scenario/native/native.log": "1" * 64}, "problems": {}},
               "comparability": {"status": "complete", "reasons": []},
               "phases": [{"phase": phase, "status": "failed" if retried and phase == "native" else "passed"}
                          for phase in phases],
               "outcome": {"status": "failed" if retried else "passed", "failed_phases": ["native"] if retried else [],
                           "cancelled_phases": [], "missing_phases": [], "skipped_phases": []},
               "runtime": ({"status": "recorded", "roles": {"essential": {
                   "reference": "registry/images:official-20.2.4-essential", "image_id": "sha256:" + "2" * 64,
                   "repo_digests": ["registry/images@sha256:" + "3" * 64]}}} if ceph else {"status": "not_applicable"})}
        row.update(changes)
        row["row_sha256"] = reuse.row_digest(row)
        retry = {"schema": reuse.RETRY_SCHEMA, "passed": True, "attempt_classification": "passed", "refused": None,
                 "retried_tests": ["TestRBDLocks"]} if retried else None
        return {"row": row, "retry": retry}

    def keep(self, entries):
        """Replace the preserved evidence and let the receipt record it."""
        self.entries = entries
        self.evidence[40] = reuse.encode_evidence(entries)
        self.receipts[40]["evidence"] = reuse.evidence_summary(self.evidence[40])

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
        if artifact["id"] % 10:
            raise AssertionError("an expired job artifact was downloaded")
        run = artifact["id"] // 10
        files = {reuse.RECEIPT_FILE[reuse.CANDIDATE]: json.dumps(self.receipts[run]).encode()}
        if self.evidence.get(run) is not None:
            files[reuse.EVIDENCE_FILE] = self.evidence[run]
        return files

    def decide(self, coverage=DIGEST, branch="main"):
        return reuse.decide(self.api, self.download, REPO, branch, MERGE, coverage)[:2]


class QualifyTests(unittest.TestCase):
    def assert_refused(self, facts, pattern, **arguments):
        receipt, reasons = facts.decide(**arguments)
        self.assertTrue(any(pattern in reason for reason in reasons), reasons)
        self.assertEqual(receipt["disposition"], "full-matrix")
        self.assertEqual(receipt["reasons"], reasons)
        self.assertNotIn("source", receipt)

    @staticmethod
    def rehearsal():
        """The same promotion, made to the rehearsal branch."""
        facts = Facts()
        facts.pulls[0]["base"]["ref"] = reuse.REHEARSAL
        facts.jobs[40][-1] = facts.job(6, reuse.gate(reuse.REHEARSAL))
        facts.receipts[40] = reuse.candidate_receipt(
            REPO, 7, reuse.REHEARSAL, HEAD, 40, 2, {"sha": TEST, "tree": TREE, "parents": [BASE, HEAD]},
            {"cells": 3, "callers": 2, "digest": DIGEST}, {"20.2.4": {"official": "image"}}, facts.jobs[40],
            reuse.evidence_summary(facts.evidence[40]))
        return facts

    def test_the_rehearsal_branch_reuses_by_the_same_rules(self):
        self.assertEqual(reuse.BRANCHES, ("main", reuse.REHEARSAL))
        receipt, reasons = self.rehearsal().decide(branch=reuse.REHEARSAL)
        self.assertEqual(reasons, [])
        self.assertEqual(receipt["disposition"], "reused")
        self.assertEqual(receipt["publication"]["branch"], reuse.REHEARSAL)
        # A rehearsal never qualifies a publication, nor the reverse.
        self.assert_refused(self.rehearsal(), "another branch or repository")
        self.assert_refused(Facts(), "another branch or repository", branch=reuse.REHEARSAL)
        facts = self.rehearsal()
        facts.pulls[0]["base"]["ref"] = "main"
        self.assert_refused(facts, "another PR, target or head")
        facts = self.rehearsal()
        facts.jobs[40][-1] = facts.job(6, reuse.gate("main"))
        self.assert_refused(facts, "is missing or unsuccessful", branch=reuse.REHEARSAL)

    def test_the_workflow_asks_for_a_reuse_on_exactly_the_reusing_branches(self):
        workflow = (Path(__file__).resolve().parents[2] / reuse.WORKFLOW).read_text()
        refs = " || ".join("github.ref == 'refs/heads/" + branch + "'" for branch in reuse.BRANCHES)
        bases = " || ".join("github.base_ref == '" + branch + "'" for branch in reuse.BRANCHES)
        self.assertIn("    if: github.event_name == 'push' && (" + refs + ")\n", workflow)
        self.assertIn("    if: github.event_name == 'pull_request' && (" + bases + ") && ", workflow)
        for trigger in ("  push:\n    branches: [", "  pull_request:\n    branches: ["):
            listed = workflow.split(trigger, 1)[1].split("]", 1)[0].split(", ")
            for branch in reuse.BRANCHES:
                self.assertIn(branch, listed)

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
        facts = Facts()
        self.assertEqual(receipt["evidence"], {"file": reuse.EVIDENCE_FILE, "entries": 2,
                                               "sha256": reuse.evidence_summary(facts.evidence[40])["sha256"]})
        self.assertEqual(receipt["observed"], {"go_versions": ["go1.26.11"], "images": {
            "registry/images:official-20.2.4-essential": {"image_ids": ["sha256:" + "2" * 64],
                                                          "repo_digests": ["registry/images@sha256:" + "3" * 64]}}})
        # The publication gets the evidence that qualified it.
        self.assertEqual(reuse.decide(facts.api, facts.download, REPO, "main", MERGE, DIGEST)[2], facts.evidence[40])

    def test_expired_job_artifacts_do_not_matter_while_their_records_are_preserved(self):
        """The receipt artifact carries the job records, so the run's
        shorter-lived artifacts are neither needed nor read."""
        facts = Facts()
        self.assertTrue(all(artifact["expired"] for artifact in facts.artifacts[40][1:]))
        self.assertEqual(facts.decide()[1], [])

    def test_a_live_receipt_without_usable_job_records_runs_the_full_matrix(self):
        """The receipt alone never qualifies a run: its evidence must
        account for every runtime job."""
        facts = Facts()
        facts.evidence[40] = None
        self.assert_refused(facts, "preserved no evidence")
        facts = Facts()
        facts.keep([])
        self.assert_refused(facts, "no preserved evidence")
        facts = Facts()
        facts.keep(facts.entries[:1])
        self.assert_refused(facts, "Environment / docker: no preserved evidence")
        facts = Facts()
        facts.keep([*facts.entries, facts.entry(9)])
        self.assert_refused(facts, "belong to no runtime job")
        facts = Facts()
        facts.keep([*facts.entries, facts.entries[1]])
        self.assert_refused(facts, "more than one evidence entry")
        # The file is not the one that the receipt records.
        facts = Facts()
        facts.evidence[40] = reuse.encode_evidence(facts.entries[:1])
        self.assert_refused(facts, "not the file that the candidate receipt records")
        facts = Facts()
        del facts.receipts[40]["evidence"]
        self.assert_refused(facts, "not the file that the candidate receipt records")
        facts = Facts()
        facts.evidence[40] = b"not json\n"
        facts.receipts[40]["evidence"] = reuse.evidence_summary(facts.evidence[40])
        self.assert_refused(facts, "not readable")
        # A run without any batch has nothing to reuse.
        facts = Facts()
        facts.jobs[40] = [job for job in facts.jobs[40] if " / " not in job["name"] or job["name"].endswith("Plan")]
        facts.keep([])
        self.assert_refused(facts, "no runtime job")

    def test_job_records_that_do_not_show_a_qualifying_execution_are_refused(self):
        def changed(job, pattern, retried=False, ceph=True, retry=..., **changes):
            facts = Facts()
            entry = facts.entry(job, retried=retried, ceph=ceph, **changes)
            if retry is not ...:
                entry["retry"] = retry
            facts.keep([entry if existing["row"]["execution_key"]["job_id"] == str(job) else existing
                        for existing in facts.entries])
            with self.subTest(pattern=pattern, changes=sorted(changes)):
                self.assert_refused(facts, pattern)

        key = Facts.entry(5)["row"]["execution_key"]
        changed(5, "of another run", ceph=False, execution_key=dict(key, run_id="39"))
        changed(5, "of another run", ceph=False, execution_key=dict(key, repository="o/r"))
        changed(5, "did not check out the tested commit", ceph=False,
                source={"checkout_commit": HEAD, "checkout_tree": TREE})
        changed(5, "did not check out the tested commit", ceph=False,
                source={"checkout_commit": TEST, "checkout_tree": "1" * 40})
        changed(5, "records are incomplete", ceph=False, evidence={"files": {}, "problems": {"native.log": "missing"}})
        changed(5, "records are incomplete", ceph=False, comparability={"status": "incomplete", "reasons": ["x"]})
        passed = Facts.entry(5)["row"]["phases"]
        for status in ("failed", "missing", "skipped", "not_required"):
            changed(5, "cleanup check did not pass", ceph=False,
                    phases=[dict(phase, status=status) if phase["phase"] == "cleanup" else phase for phase in passed])
        changed(5, "cleanup check did not pass", ceph=False, phases=passed[:-1])
        # A Ceph batch must say what it ran.
        changed(4, "did not record the images", retried=True, runtime={"status": "missing"})
        changed(4, "did not record the images", retried=True, runtime={"status": "recorded", "roles": {}})
        changed(4, "did not record the images", retried=True,
                runtime={"status": "recorded", "roles": {"essential": {"reference": "r", "image_id": ""}}})
        # Outcomes.
        failed = {"status": "failed", "failed_phases": ["native"], "cancelled_phases": [], "missing_phases": [],
                  "skipped_phases": []}
        changed(5, "neither passed nor passed its one retry", ceph=False, outcome=failed)
        changed(5, "neither passed nor passed its one retry", ceph=False,
                outcome=dict(failed, status="cancelled", failed_phases=[], cancelled_phases=["native"]))
        changed(5, "neither passed nor passed its one retry", ceph=False, outcome=dict(failed, status="skipped"))
        # A failure outside the native phase is never retried.
        changed(4, "neither passed nor passed its one retry", retried=True,
                outcome=dict(failed, failed_phases=["images"]))
        changed(4, "neither passed nor passed its one retry", retried=True,
                outcome=dict(failed, failed_phases=["native", "cleanup"]))
        # The retry's own report decides, and it must be there.
        report = Facts.entry(4, retried=True)["retry"]
        changed(4, "neither passed nor passed its one retry", retried=True, retry=None)
        for field, value in (("passed", False), ("attempt_classification", "native-test-failure"),
                             ("refused", "panic"), ("schema", "other")):
            changed(4, "neither passed nor passed its one retry", retried=True, retry=dict(report, **{field: value}))
        # A retry report beside a row that passed, or without the job's retry
        # step in the run, is not a consistent record.
        changed(5, "neither passed nor passed its one retry", ceph=False, retry=report)
        changed(5, "neither passed nor passed its one retry", retried=True, ceph=False)

    def test_an_altered_job_record_is_refused(self):
        facts = Facts()
        entry = deepcopy(facts.entries[1])
        entry["row"]["outcome"]["status"] = "passed"
        entry["row"]["phases"][-1]["status"] = "failed"
        facts.keep([facts.entries[0], entry])
        self.assert_refused(facts, "not an unaltered history row")
        for change in ({"schema": "other/v1"}, {"execution_key": None}):
            with self.subTest(change=change):
                facts = Facts()
                facts.keep([facts.entries[0], {"row": dict(facts.entries[1]["row"], **change), "retry": None}])
                self.assert_refused(facts, "not an unaltered history row")
        facts = Facts()
        facts.keep([facts.entries[0], "not an entry"])
        self.assert_refused(facts, "not an unaltered history row")

    def test_many_evidence_problems_are_summarized(self):
        facts = Facts()
        facts.jobs[40] += [facts.job(10 + number, "Squid short / batch" + str(number)) for number in range(8)]
        problems = facts.decide()[1]
        self.assertEqual(len(problems), 6)
        self.assertEqual(problems[-1], "and 3 more evidence problems")

    def test_the_row_digest_is_the_history_tools(self):
        """An evidence row is checked with the hash that the history tool
        gave it."""
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / ".github" / "scripts"))
        try:
            import scenario_history
        finally:
            sys.path.pop(0)
        row = Facts.entry(4, retried=True)["row"]
        self.assertEqual(reuse.row_digest(row), scenario_history.row_hash(row))
        self.assertEqual(scenario_history.SCHEMA, reuse.ROW_SCHEMA)

    def test_collect_evidence_pairs_each_row_with_its_retry_report(self):
        facts = Facts()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, entry in (("tag-scenario-b", facts.entries[1]), ("tag-scenario-a", facts.entries[0])):
                (root / name / "native-retry").mkdir(parents=True)
                (root / name / "history-row.json").write_text(json.dumps(entry["row"]))
                if entry["retry"]:
                    (root / name / "native-retry" / "report.json").write_text(json.dumps(entry["retry"]))
                (root / name / "native-retry" / "attempt").mkdir()
            (root / "tag-scenario-c").mkdir()
            collected = reuse.collect_evidence(root)
        self.assertEqual(collected, facts.entries)
        self.assertEqual(reuse.encode_evidence(collected), facts.evidence[40])
        self.assertEqual(reuse.runtime_jobs(facts.jobs[40]), [facts.jobs[40][3], facts.jobs[40][4]])

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
        facts.artifacts[40] = facts.artifacts[40][1:]
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

        receipt, reasons, evidence = reuse.decide(broken, facts.download, REPO, "main", MERGE, DIGEST)
        self.assertIsNone(evidence)
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

    def audit(self, facts, jobs, recorded, kept=...):
        """Audit push run 50, whose reuse artifact holds the receipt recorded
        and the evidence kept (by default the evidence that qualified it)."""
        run = {"id": 50, "run_attempt": 1}
        artifacts = [] if recorded is None else [{"id": 500, "name": reuse.artifact_name(reuse.REUSE, 1)}]
        kept = facts.evidence[40] if kept is ... else kept

        def api(path, *, pages=False):
            if path.startswith("actions/runs/50/artifacts"):
                return artifacts
            return facts.api(path, pages=pages)

        def download(artifact):
            if artifact["id"] != 500:
                return facts.download(artifact)
            files = {reuse.RECEIPT_FILE[reuse.REUSE]: json.dumps(recorded).encode()}
            if kept is not None and recorded.get("disposition") == "reused":
                files[reuse.EVIDENCE_FILE] = kept
            return files

        output = StringIO()
        with patch.object(flow, "api", side_effect=api), \
                patch.object(reuse, "download_files", side_effect=download), redirect_stdout(output):
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
        self.assertIn("2 preserved job records, 1 first failures passed their retry", printed)

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
        forged = deepcopy(recorded)
        forged["observed"]["go_versions"] = ["go1.0"]
        with self.assertRaisesRegex(ValueError, "observed differs"):
            self.audit(facts, jobs, forged)
        with self.assertRaisesRegex(ValueError, "executed matrix jobs"):
            self.audit(facts, [*jobs, self.job("Tentacle short / pools")], recorded)

    def test_a_reuse_whose_job_records_are_gone_or_changed_is_refused(self):
        facts = Facts()
        recorded, _ = facts.decide()
        jobs = [self.job(reuse.REUSE_JOB), self.job(reuse.gate("main"))]
        # The promotion run's receipt is still there, its evidence is not.
        gone = Facts()
        gone.evidence[40] = None
        with self.assertRaisesRegex(ValueError, "does not verify.*preserved no evidence"):
            self.audit(gone, jobs, recorded)
        # The publication run kept no copy, or another file.
        with self.assertRaisesRegex(ValueError, "did not keep the evidence"):
            self.audit(facts, jobs, recorded, kept=None)
        with self.assertRaisesRegex(ValueError, "did not keep the evidence"):
            self.audit(facts, jobs, recorded, kept=reuse.encode_evidence(facts.entries[:1]))

    def test_a_full_matrix_is_reported_with_the_reason_reuse_was_refused(self):
        facts = Facts()
        facts.evidence[40] = None
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
