"""Test the offline replay of the declaration selection without git, Go or GitHub."""

import unittest
from unittest.mock import patch

import replay_declaration_selection as replay

REPO = "o/r"


def record(selected, batches, fallback=(), error=None):
    item = {"fallback": list(fallback), "changed_declarations": ["a"], "selected": {p: "r" for p in selected},
            "batches": batches, "selected_profiles": len(selected), "total_profiles": len(batches)}
    if error:
        item["error"] = error
    return item


def explanation(batch, evidence):
    return {"pull_request": "#5", "batch": batch, "signature": "error", "evidence": evidence, "reference": "#6"}


class PullRequestTests(unittest.TestCase):
    def test_merges_and_squashes_name_their_base_and_head(self):
        log = ("m1\tb1 h1\tMerge pull request #119 from x/y\n"
               "s2\tb2\tfeat(ceph): add loss (#99)\n"
               "d3\tb3\tdirect commit\n"
               "m4\tb4 h4\tMerge pull request #117 from x/z\n").encode()
        with patch.object(replay, "git", return_value=log) as git:
            self.assertEqual(replay.pull_requests("origin/main", 2), [
                {"commit": "m1", "base": "b1", "head": "h1", "subject": "Merge pull request #119 from x/y",
                 "number": 119},
                {"commit": "s2", "base": "b2", "head": "s2", "subject": "feat(ceph): add loss (#99)", "number": 99}])
        self.assertEqual(git.call_args.args[:4], ("log", "--first-parent", "-n", "6"))


class JobTests(unittest.TestCase):
    def test_batch_jobs_map_to_category_and_batch(self):
        self.assertEqual(replay.job_batch("Ceph short", "Scenarios / rbd_namespaces"), ("short", "rbd_namespaces"))
        self.assertEqual(replay.job_batch("Ceph short", "Scenarios / rbd_namespaces (bridge)"),
                         ("short", "rbd_namespaces"))
        self.assertEqual(replay.job_batch("Ceph release matrix", "Squid topology / osd (host)"), ("topology", "osd"))
        self.assertEqual(replay.job_batch("Docker checks", "Scenarios / environment"), ("environment", "environment"))
        for workflow, job in (("Ceph short", "Scenarios / Plan"), ("Code checks", "Lint"),
                              ("Ceph short", "Gate"), ("Ceph release matrix", "Squid code / x")):
            self.assertIsNone(replay.job_batch(workflow, job), job)

    def test_covered_compares_category_and_batch(self):
        item = record(["short-a"], {"short-a": ["short", "a"], "topology-a": ["topology", "a"]})
        self.assertTrue(replay.covered(item, "short", "a"))
        self.assertFalse(replay.covered(item, "topology", "a"))
        self.assertIsNone(replay.covered(item, "short", "gone"))


class GitHubTests(unittest.TestCase):
    def test_runs_and_failures_of_every_attempt(self):
        pr = {"head": {"ref": "feat/x"}, "created_at": "2026-10-17T10:00:00Z", "merged_at": "2026-10-17T12:00:00Z"}
        branch = [{"id": n, "name": name, "created_at": at, "event": "pull_request", "head_sha": "h" + str(n),
                   "run_attempt": 1}
                  for n, name, at in ((1, "Ceph short", "2026-10-17T09:00:00Z"),
                                      (2, "Ceph short", "2026-10-17T11:00:00Z"),
                                      (3, "Code checks", "2026-10-17T11:00:00Z"))]
        push = [{"id": 4, "name": "Ceph short", "created_at": "2026-10-17T12:01:00Z", "event": "push",
                 "head_sha": "m", "run_attempt": 2}]
        jobs = {1: [{"id": 11, "name": "Scenarios / a", "conclusion": "failure"},
                    {"name": "Scenarios / b (bridge)", "conclusion": "cancelled"},
                    {"name": "Scenarios / Plan", "conclusion": "failure"}],
                2: [{"name": "Scenarios / a", "conclusion": "success"},
                    {"id": 21, "name": "Scenarios / b (bridge)", "conclusion": "timed_out"}]}
        responses = {
            "repos/o/r/pulls/7": pr,
            "repos/o/r/actions/runs?event=pull_request&branch=feat/x&per_page=100&page=1": {"workflow_runs": branch},
            "repos/o/r/actions/runs?event=push&head_sha=m&per_page=100&page=1": {"workflow_runs": push},
        }
        for attempt, rows in jobs.items():
            responses["repos/o/r/actions/runs/4/attempts/" + str(attempt) + "/jobs?per_page=100&page=1"] = {
                "jobs": rows}
        calls = []
        github = replay.GitHub(REPO, call=lambda path: calls.append(path) or responses[path])
        runs = github.runs(7, "m")
        self.assertEqual([run["id"] for run in runs], [2, 4])
        failures = github.failures(runs[1])
        self.assertEqual([(f["batch"], f["attempt"], f["conclusion"], f["rerun_passed"]) for f in failures],
                         [("a", 1, "failure", True), ("b", 2, "timed_out", False)])
        before = len(calls)
        github.runs(7, "m")
        self.assertEqual(len(calls), before, "responses are cached")

    def test_pages_until_a_short_page(self):
        pages = {1: [{"n": i} for i in range(100)], 2: [{"n": 100}]}
        github = replay.GitHub(REPO, call=lambda path: {"jobs": pages[int(path.rsplit("=", 1)[1])]})
        self.assertEqual(len(github.pages("x/jobs", "jobs")), 101)


class CheckTests(unittest.TestCase):
    def test_each_failure_meets_the_selection_of_the_diff_its_run_tested(self):
        batches = {"short-a": ["short", "a"], "short-b": ["short", "b"]}
        records = {"h": record(["short-a"], batches), "early": record(["short-b"], batches),
                   "gone": record([], {}, error="commit gone is not available")}
        pr = {"base": "b", "head": "h", "commit": "m", "number": 5}
        failure = {"category": "short", "event": "pull_request"}
        results = replay.check(pr, [
            {**failure, "batch": "a", "head_sha": "h"},
            {**failure, "batch": "a", "head_sha": "early"},
            {**failure, "batch": "b", "head_sha": "early"},
            {**failure, "batch": "b", "event": "push", "head_sha": "m"},
            {**failure, "batch": "c", "head_sha": "h"},
            {**failure, "batch": "a", "head_sha": "gone"},
            {**failure, "batch": "a", "head_sha": "h", "rerun_passed": True},
            {**failure, "batch": "b", "head_sha": "h", "job_id": 42},
            {**failure, "batch": "a", "head_sha": "h", "job_id": 42},
            {**failure, "batch": "b", "head_sha": "h", "job_id": 43},
        ], lambda base, head: records[head], explained={"42": explanation("short/b", "runner lost"),
                                                         "43": explanation("short/a", "another batch")})
        self.assertEqual([r["verdict"] for r in results], [
            "selected", "miss", "selected", "miss", "unverified: not in the head's plan",
            "unverified: commit gone is not available", "selected", "explained: runner lost", "selected", "miss"])

    def test_a_passing_retry_does_not_clear_an_excluded_failure(self):
        # An intermittent regression of the diff fails once and passes a
        # retry of the same commit: the miss stays, with the retry as evidence.
        batches = {"short-a": ["short", "a"], "short-b": ["short", "b"]}
        pr = {"base": "b", "head": "h", "commit": "m", "number": 5}
        failed = {"category": "short", "batch": "b", "event": "pull_request", "head_sha": "h", "job_id": 123,
                  "rerun_passed": True}
        results = replay.check(pr, [failed], lambda base, head: record(["short-a"], batches))
        self.assertEqual([r["verdict"] for r in results], ["miss"])
        item = replay.row({"commit": "m", "number": 5}, record(["short-a"], batches), results)
        self.assertEqual(item["misses"], ["short/b@h (passed on rerun)"])
        self.assertIn("unresolved misses: 1 (1 passed on a rerun of the same commit)", replay.summary([item]))
        explained = replay.check(pr, [failed], lambda base, head: record(["short-a"], batches),
                                 explained={"123": explanation("short/b", "runner lost before tests")})
        self.assertEqual([r["verdict"] for r in explained], ["explained: runner lost before tests"])

    def test_offline_reads_only_the_cache(self):
        github = replay.GitHub(REPO, call=replay.offline)
        github.cache["repos/o/r/pulls/1"] = {"number": 1}
        self.assertEqual(github.api("repos/o/r/pulls/1"), {"number": 1})
        with self.assertRaisesRegex(ValueError, "not in the cache"):
            github.api("repos/o/r/pulls/2")

    def test_explained_record_names_job_ids_and_evidence(self):
        entries = replay.load_explained(replay.EXPLAINED)
        for job, entry in entries.items():
            self.assertTrue(job.isdigit() and entry["pull_request"].startswith("#"), job)
            self.assertIn("/", entry["batch"], job)

    def test_an_explanation_needs_every_field(self):
        import json
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "explained.json"
            self.assertEqual(replay.load_explained(path), {})
            path.write_text(json.dumps({"7": explanation("short/a", "fine")}))
            self.assertEqual(list(replay.load_explained(path)), ["7"])
            for broken in ({"7": "a bare string"}, {"x": explanation("short/a", "fine")},
                           {"7": {**explanation("short/a", "fine"), "signature": " "}},
                           {"7": {key: value for key, value in explanation("short/a", "fine").items()
                                  if key != "reference"}}):
                path.write_text(json.dumps(broken))
                with self.assertRaises(ValueError, msg=broken):
                    replay.load_explained(path)


class TableTests(unittest.TestCase):
    def test_rows_table_and_summary(self):
        batches = {"short-x": ["short", "x"], "short-y": ["short", "y"], "topology-z": ["topology", "z"]}
        graph = record(["short-x"], batches)
        fallback = record(list(batches), batches, fallback=["go.mod changed", "x"])
        failed = replay.failed_record(ValueError("the head's planner failed\ndetail"))
        miss = [{"category": "short", "batch": "y", "head_sha": "abcdef12", "verdict": "miss"}]
        rows = [replay.row({"commit": "0123456789", "number": 12}, graph, miss),
                replay.row({"commit": "abcdef0123", "number": 13}, fallback, []),
                replay.row({"commit": "fedcba9876", "number": 14}, failed, [])]
        self.assertEqual({k: rows[0][k] for k in ("merge", "pull_request", "selected", "total", "selected_short",
                                                  "total_short", "why", "failed", "misses")},
                         {"merge": "0123456", "pull_request": "#12", "selected": 1, "total": 3,
                          "selected_short": 1, "total_short": 2, "why": "1 changed declarations",
                          "failed": ["short/y"], "misses": ["short/y@abcdef1"]})
        self.assertEqual(replay.table(rows).splitlines()[2:], [
            "| 0123456 | #12 | 1 of 3 | 1 of 2 | short/y | short/y@abcdef1 | 1 changed declarations: short-x |",
            "| abcdef0 | #13 | 3 of 3 | 2 of 2 | none | none | go.mod changed |",
            "| fedcba9 | #14 | 0 of 0 | 0 of 0 | none | none | the head's planner failed |"])
        self.assertEqual(replay.summary(rows).splitlines(), [
            "pull requests: 3 (1 not replayable)", "Ceph short jobs: 3 selected of 4",
            "pull requests below every batch: 1", "failed batch jobs: 1", "selected: 0",
            "explained as not the diff's: 0", "unresolved misses: 1 (0 passed on a rerun of the same commit)",
            "unverified: 0"])

    def test_table_without_github_has_no_failure_columns(self):
        item = replay.row({"commit": "0123456789", "number": 12}, record(["short-x"], {"short-x": ["short", "x"]}))
        self.assertEqual(replay.table([item]).splitlines()[0], "| Merge | PR | Selected | Short | Why |")


if __name__ == "__main__":
    unittest.main()
