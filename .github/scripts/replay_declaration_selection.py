#!/usr/bin/env python3
"""Replay the declaration selection over merged pull requests.

For each of the last pull requests on the first-parent history of a ref,
the merge's first parent is the base and its second parent the pull request
head; a squash merge is its own head. The head's tree is exported, its plan
discovered by the head's own planner, and internal/tools/batchselect reports which
batches the pull request's diff selects.

With --github, the runtime runs of each pull request are read with the gh
CLI: every attempt of every pull request run on the pull request's branch,
and the push run of the merge commit. A batch job that failed must be in the
selection of the diff that the run tested, or it is a miss (issue #116).
Cancelled jobs are ignored: the maintainer cancels runs on purpose. Only an
excluded failure whose job ID batch_selection_explained.json records, with
the batch, the failure's signature, the evidence that the diff did not cause
it and a reference, is not a miss. A later attempt of the same run that
passed the job is reported with the miss but does not clear it: an
intermittent regression of the diff passes a retry too.
With --offline the GitHub responses come from --cache only.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile

import tag_scenarios as engine

# Workflow name to the category of its batch jobs. The release matrix names
# the category in the calling job, as in "Squid short / rbd".
WORKFLOW_CATEGORIES = {"Ceph short": "short", "Ceph topology": "topology", "Ceph multicluster": "multicluster",
                       "Ceph recovery": "recovery", "Docker checks": "environment", "Ceph release matrix": ""}
FAILED = ("failure", "timed_out")
EXPLAINED = Path(__file__).with_name("batch_selection_explained.json")
NUMBER = re.compile(r"^Merge pull request #([0-9]+) |\(#([0-9]+)\)$")


def git(*args, root=engine.ROOT):
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("git " + " ".join(args) + " failed: " + result.stderr.decode(errors="replace").strip())
    return result.stdout


def pull_requests(ref, count, root=engine.ROOT):
    """The last count pull requests merged into ref, merged or squashed:
    dicts of commit, base, head, subject and number."""
    rows = []
    text = git("log", "--first-parent", "-n", str(count * 3), "--format=%H%x09%P%x09%s", ref, root=root)
    for row in text.decode().splitlines():
        commit, parents, subject = row.split("\t", 2)
        parents = parents.split()
        number = NUMBER.search(subject)
        if not number or len(parents) > 2:
            continue
        rows.append({"commit": commit, "base": parents[0], "head": parents[-1] if len(parents) == 2 else commit,
                     "subject": subject, "number": int(number[1] or number[2])})
        if len(rows) == count:
            break
    return rows


def export(commit, directory, root=engine.ROOT):
    """Write commit's tree into directory."""
    archive = Path(directory) / "tree.tar"
    archive.write_bytes(git("archive", "--format=tar", commit, root=root))
    tree = Path(directory) / "tree"
    with tarfile.open(archive) as source:
        source.extractall(tree, filter="data")
    return tree


def head_plan(tree):
    """The plan of the head, by the head's own planner: older source does
    not satisfy the current planner's rules."""
    output = Path(tree).parent / "plan.json"
    result = subprocess.run([sys.executable, str(Path(tree) / ".github/scripts/tag_scenarios.py"), "plan",
                             "--output", str(output)], cwd=tree, capture_output=True, text=True, timeout=600,
                            env=engine.go_environment())
    if result.returncode:
        raise ValueError("the head's planner failed: " + result.stderr.strip())
    return json.loads(output.read_text())


def replay_one(base, head, tool, root=engine.ROOT):
    """The selection of the diff from base's merge base to head, over the
    head's runtime profiles, with "batches" mapping each profile ID to its
    category and batch."""
    with tempfile.TemporaryDirectory() as directory:
        tree = export(head, directory, root)
        plan = head_plan(tree)
        profiles = [profile for profile in plan["include"] if profile["category"] != "code"]
        record = engine.declaration_selection(
            profiles, base, tree,
            runner=lambda batches, base, tree: engine.run_batchselect(batches, base, tree, head=head, tool=tool))
        record["batches"] = {profile["id"]: [profile["category"], profile["batch"]] for profile in profiles}
        return record


def failed_record(error):
    return {"error": str(error).splitlines()[0], "fallback": [], "changed_declarations": [], "selected": {},
            "excluded": {}, "batches": {}, "selected_profiles": 0, "total_profiles": 0}


def job_batch(workflow, job):
    """(category, batch) of a batch job, or None for any other job. A
    network mode other than the batch's default is appended in parentheses."""
    if workflow not in WORKFLOW_CATEGORIES:
        return None
    caller, separator, name = job.partition(" / ")
    category = WORKFLOW_CATEGORIES[workflow] or caller.split()[-1].lower()
    if not separator or name == "Plan" or category not in engine.RUNTIME_CATEGORIES:
        return None
    return category, re.sub(r" \((" + "|".join(engine.NETWORKS) + r")\)$", "", name)


def covered(record, category, batch):
    """Whether the selection runs that batch: True, False, or None when the
    head's plan has no such batch."""
    for profile, (kind, name) in record["batches"].items():
        if (kind, name) == (category, batch):
            return profile in record["selected"]
    return None


class GitHub:
    """Read-only GitHub API calls through gh, cached in a JSON file."""

    def __init__(self, repo, cache=None, call=None):
        self.repo, self.path = repo, cache
        self.cache = json.loads(cache.read_text()) if cache is not None and cache.exists() else {}
        self.call = call or self._gh

    @staticmethod
    def _gh(path):
        result = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise ValueError("gh api " + path + " failed: " + result.stderr.strip())
        return json.loads(result.stdout)

    def api(self, path):
        if path not in self.cache:
            self.cache[path] = self.call(path)
        return self.cache[path]

    def pages(self, path, key):
        items, page = [], 1
        while True:
            body = self.api(path + ("&" if "?" in path else "?") + "per_page=100&page=" + str(page))
            items += body[key]
            if len(body[key]) < 100:
                return items
            page += 1

    def save(self):
        if self.path is not None:
            self.path.write_text(json.dumps(self.cache, sort_keys=True) + "\n")

    def runs(self, number, commit):
        """Runtime runs of a merged pull request: its pull request runs
        while it was open, and the push runs of its merge commit."""
        pr = self.api("repos/" + self.repo + "/pulls/" + str(number))
        branch = pr["head"]["ref"]
        found = [run for run in self.pages("repos/" + self.repo + "/actions/runs?event=pull_request&branch=" + branch,
                                           "workflow_runs")
                 if pr["created_at"] <= run["created_at"] <= (pr["merged_at"] or "9")]
        found += self.pages("repos/" + self.repo + "/actions/runs?event=push&head_sha=" + commit, "workflow_runs")
        return [run for run in found if run["name"] in WORKFLOW_CATEGORIES]

    def failures(self, run):
        """Failed batch jobs of every attempt of a run. "rerun_passed" says
        whether a later attempt of the run passed the same job."""
        rows, passed = [], {}
        for attempt in range(1, run["run_attempt"] + 1):
            path = "repos/" + self.repo + "/actions/runs/" + str(run["id"]) + "/attempts/" + str(attempt) + "/jobs"
            for job in self.pages(path, "jobs"):
                batch = job_batch(run["name"], job["name"])
                if batch and job["conclusion"] == "success":
                    passed[job["name"]] = attempt
                if batch and job["conclusion"] in FAILED:
                    rows.append({"workflow": run["name"], "job": job["name"], "category": batch[0],
                                 "batch": batch[1], "event": run["event"], "head_sha": run["head_sha"],
                                 "run": run["id"], "attempt": attempt, "job_id": job["id"],
                                 "conclusion": job["conclusion"]})
        for item in rows:
            item["rerun_passed"] = passed.get(item["job"], 0) > item["attempt"]
        return rows


def offline(path):
    raise ValueError("gh api " + path + " is not in the cache")


def have_commit(sha, root=engine.ROOT):
    try:
        git("cat-file", "-e", sha + "^{commit}", root=root)
        return True
    except ValueError:
        pass
    try:
        git("fetch", "-q", "origin", sha, root=root)
        return True
    except ValueError:
        return False


EXPLANATION_FIELDS = ("pull_request", "batch", "signature", "evidence", "reference")


def load_explained(path):
    """The investigated failures of path: job ID to an entry with every
    field of EXPLANATION_FIELDS, each a non-empty string."""
    entries = json.loads(Path(path).read_text()) if Path(path).exists() else {}
    for job, entry in entries.items():
        if not job.isdigit() or not isinstance(entry, dict):
            raise ValueError(str(path) + ": " + job + " is not a job ID with an object")
        missing = [field for field in EXPLANATION_FIELDS
                   if not isinstance(entry.get(field), str) or not entry[field].strip()]
        if missing:
            raise ValueError(str(path) + ": job " + job + " lacks " + ", ".join(missing))
    return entries


def check(pr, failures, replay, explained=None):
    """Each failure with the selection of the diff its run tested: the pull
    request diff for a push run of the merge commit or a run of the final
    head, otherwise the diff of the head that the run tested. explained maps
    the job ID of an excluded failure that was investigated and found not to
    be the diff's to its entry; the entry must name the failure's batch.

    A passing retry does not clear a miss: an intermittent regression of the
    diff passes a retry of the same commit too. It stays in "rerun_passed"
    as evidence for whoever investigates the miss."""
    explained = explained or {}
    results = []
    for failure in failures:
        sha = pr["head"] if failure["event"] == "push" else failure["head_sha"]
        record = replay(pr["base"], sha)
        if record.get("error"):
            verdict = "unverified: " + record["error"]
        else:
            inside = covered(record, failure["category"], failure["batch"])
            verdict = {True: "selected", False: "miss", None: "unverified: not in the head's plan"}[inside]
            entry = explained.get(str(failure.get("job_id")))
            if inside is False and entry and entry["batch"] == failure["category"] + "/" + failure["batch"]:
                verdict = "explained: " + entry["evidence"]
        results.append({**failure, "verdict": verdict})
    return results


def short_counts(record):
    short = [profile for profile, (category, _) in record["batches"].items() if category == "short"]
    return sum(profile in record["selected"] for profile in short), len(short)


def row(pr, record, results=None):
    reason = record.get("error") or "; ".join(record["fallback"][:1]) or (
        str(len(record["changed_declarations"])) + " changed declarations")
    selected_short, total_short = short_counts(record)
    item = {"merge": pr["commit"][:7], "pull_request": "#" + str(pr["number"]),
            "selected": record["selected_profiles"], "total": record["total_profiles"],
            "selected_short": selected_short, "total_short": total_short, "why": reason,
            "selected_batches": sorted(record["selected"]) if not record["fallback"] else []}
    if results is not None:
        item["failed"] = sorted({r["category"] + "/" + r["batch"] for r in results})
        item["misses"] = sorted({r["category"] + "/" + r["batch"] + "@" + r["head_sha"][:7]
                                 + (" (passed on rerun)" if r.get("rerun_passed") else "")
                                 for r in results if r["verdict"] == "miss"})
        item["explained"] = sorted({r["category"] + "/" + r["batch"] + "@" + r["head_sha"][:7] + " (" + r["verdict"] + ")"
                                    for r in results if r["verdict"].startswith("explained")})
        item["unverified"] = sorted({r["category"] + "/" + r["batch"] + "@" + r["head_sha"][:7] + ": " + r["verdict"]
                                     for r in results if r["verdict"].startswith("unverified")})
        item["failures"] = results
    return item


def table(rows):
    validated = any("failed" in item for item in rows)
    lines = ["| Merge | PR | Selected | Short | " + ("Failed | Misses | " if validated else "") + "Why |",
             "| --- | --- | ---: | ---: | " + ("--- | --- | " if validated else "") + "--- |"]
    for item in rows:
        why = item["why"].replace("|", "\\|")
        if item["selected_batches"] and item["selected"] < item["total"]:
            why += ": " + ", ".join(item["selected_batches"])
        cells = [item["merge"], item["pull_request"], str(item["selected"]) + " of " + str(item["total"]),
                 str(item["selected_short"]) + " of " + str(item["total_short"])]
        if validated:
            cells += [", ".join(item["failed"]) or "none",
                      ", ".join(item["misses"] + item["explained"] + item["unverified"]) or "none"]
        lines.append("| " + " | ".join(cells + [why]) + " |")
    return "\n".join(lines)


def summary(rows):
    """Totals over the rows: Ceph short jobs with and without the
    selection, and the failures checked."""
    good = [item for item in rows if item["total"]]
    text = ["pull requests: " + str(len(rows)) + " (" + str(len(rows) - len(good)) + " not replayable)",
            "Ceph short jobs: " + str(sum(item["selected_short"] for item in good)) + " selected of "
            + str(sum(item["total_short"] for item in good)),
            "pull requests below every batch: " + str(sum(item["selected"] < item["total"] for item in good))]
    if any("failed" in item for item in rows):
        failures = [r for item in rows for r in item["failures"]]
        misses = [r for r in failures if r["verdict"] == "miss"]
        text += ["failed batch jobs: " + str(len(failures)),
                 "selected: " + str(sum(r["verdict"] == "selected" for r in failures)),
                 "explained as not the diff's: " + str(sum(r["verdict"].startswith("explained") for r in failures)),
                 "unresolved misses: " + str(len(misses)) + " (" + str(sum(bool(r.get("rerun_passed")) for r in misses))
                 + " passed on a rerun of the same commit)",
                 "unverified: " + str(sum(r["verdict"].startswith("unverified") for r in failures))]
    return "\n".join(text)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", default="origin/main")
    parser.add_argument("--merges", type=int, default=15, help="Number of merged pull requests")
    parser.add_argument("--github", metavar="OWNER/REPO",
                        help="Check each pull request's failed batch jobs against the selection")
    parser.add_argument("--cache", type=Path, help="JSON file that caches the GitHub API responses")
    parser.add_argument("--offline", action="store_true",
                        help="Read GitHub responses only from --cache, for a host without gh")
    parser.add_argument("--explained", type=Path, default=EXPLAINED,
                        help="JSON object of investigated failures: job ID to its batch, signature, evidence "
                             "and reference")
    parser.add_argument("--output", type=Path, help="Write the rows as JSON")
    args = parser.parse_args(argv)
    github = None
    if args.github:
        github = GitHub(args.github, args.cache, call=offline if args.offline else None)
    explained = load_explained(args.explained)
    with tempfile.TemporaryDirectory() as directory:
        tool = Path(directory) / "batchselect"
        subprocess.run(["go", "build", "-mod=readonly", "-o", str(tool), "./internal/tools/batchselect"], cwd=engine.ROOT,
                       check=True, env=engine.go_environment(), timeout=600)
        records = {}

        def replay(base, head):
            if (base, head) not in records:
                try:
                    if not have_commit(head):
                        raise ValueError("commit " + head[:7] + " is not available")
                    records[base, head] = replay_one(base, head, tool)
                except (ValueError, OSError, subprocess.SubprocessError) as error:
                    records[base, head] = failed_record(error)
            return records[base, head]

        rows = []
        for pr in pull_requests(args.ref, args.merges):
            results = None
            if github is not None:
                failures = [failure for run in github.runs(pr["number"], pr["commit"])
                            for failure in github.failures(run)]
                github.save()
                results = check(pr, failures, replay, explained)
            rows.append(row(pr, replay(pr["base"], pr["head"]), results))
            print(table(rows[-1:]).splitlines()[-1], file=sys.stderr, flush=True)
    if args.output is not None:
        args.output.write_text(json.dumps(rows, indent=2) + "\n")
    print(table(rows))
    print()
    print(summary(rows))
    misses = sum(len(item.get("misses", [])) for item in rows)
    return 1 if misses else 0


if __name__ == "__main__":
    raise SystemExit(main())
