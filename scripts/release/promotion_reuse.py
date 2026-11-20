#!/usr/bin/env python3
"""Qualify a publication-branch push with the Release matrix of its promotion PR.

A promotion PR runs the full Release matrix on GitHub's test merge. When the
protected merge that follows has the same tree and the same parents, a second
matrix on the push would test the same content again. record writes the
candidate receipt of a validated promotion run; verify decides, for a push,
whether that receipt qualifies the pushed commit and writes the reuse receipt
either way. Anything missing, expired, cancelled or inconsistent selects the
full matrix: verify never fails a reuse open.

What makes the receipt trustworthy: the run's head commit, as the Actions API
reports it, must be the merged PR's head, which is the second parent of the
pushed merge. The workflow that wrote the receipt therefore came from that
reviewed head merged into the protected branch. The receipt names the
test-merge commit that the workflow checked out, and verify reads that
commit from the repository itself and compares its tree and parents with the
pushed merge. A run's reported head alone proves nothing about what it tested.
"""

import argparse
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile

SCHEMA = "promotion-reuse/v1"
WORKFLOW = ".github/workflows/release-matrix.yml"
# Publication branches whose push may reuse a promotion run. A support line
# keeps full push validation until its protected path has been demonstrated.
BRANCHES = ("main",)
CANDIDATE = "release-candidate-receipt"
REUSE = "promotion-reuse-receipt"
REUSE_JOB = "Promotion reuse"
RETRY_STEP = "Retry the failed tests once"
SHA = re.compile(r"[a-f0-9]{40}\Z")


def gate(branch):
    return "Release candidate validated (" + branch + ")"


def artifact_name(kind, attempt):
    """One receipt per run attempt: a re-run writes its own."""
    return kind + "-" + str(attempt)


def retried(jobs):
    """The jobs whose first native run failed and whose retry ran, with the
    retry's conclusion: the first failures that the candidate keeps."""
    found = []
    for job in jobs:
        for step in job.get("steps") or ():
            if step.get("name") == RETRY_STEP and step.get("conclusion") in ("success", "failure"):
                found.append({"id": job["id"], "name": job["name"], "retry": step["conclusion"]})
    return sorted(found, key=lambda entry: entry["id"])


def candidate_receipt(repository, pull_request, base, head_sha, run_id, run_attempt, tested, coverage, images, jobs):
    """The receipt of a promotion run whose complete matrix passed.

    tested is the commit that the workflow checked out: its sha, tree and
    parents as git reports them in that checkout."""
    return {"schema": SCHEMA, "kind": "candidate", "repository": repository, "workflow": WORKFLOW,
            "pull_request": pull_request, "base": base, "head_sha": head_sha,
            "run_id": run_id, "run_attempt": run_attempt, "tested": tested,
            "coverage": coverage, "images": images, "retried": retried(jobs), "disposition": "validated"}


def unsuccessful(jobs):
    """Jobs of a promotion run that did not succeed. The reuse job is skipped
    on a pull request and is no part of the matrix."""
    return [job["name"] for job in jobs
            if job.get("conclusion") != "success"
            and not (job["name"] == REUSE_JOB and job.get("conclusion") == "skipped")]


def qualify(repository, branch, sha, merged, pull, run, jobs, receipt, tested, coverage=None):
    """Why the promotion run does not qualify the push of sha to branch, as a
    list of reasons; an empty list means that it does.

    merged and tested are the commit objects of the pushed merge and of the
    receipt's test merge as the repository returns them, pull the merged PR,
    run and jobs the promotion run's latest attempt, receipt its candidate
    receipt, and coverage the coverage digest of the pushed tree (None when
    the caller cannot compute it and compares receipts instead)."""
    reasons = []

    def check(condition, reason):
        if not condition:
            reasons.append(reason)
        return condition

    check(branch in BRANCHES, branch + " does not reuse a promotion run")
    parents = [parent["sha"] for parent in merged.get("parents", ())]
    check(merged.get("sha") == sha, "the pushed commit was not read")
    check(len(parents) == 2, "the pushed commit is not a merge of two parents")
    check(pull.get("merged_at") and pull.get("merge_commit_sha") == sha, "no merged PR has this merge commit")
    check(pull.get("base", {}).get("ref") == branch
          and pull.get("base", {}).get("repo", {}).get("full_name") == repository,
          "the merged PR targeted another branch or repository")
    head = pull.get("head", {})
    check(head.get("repo") and head["repo"].get("full_name") == repository,
          "the merged PR's head is not a branch of this repository")
    check(len(parents) == 2 and parents[1] == head.get("sha"),
          "the pushed merge's second parent is not the merged PR's head")
    check(run.get("event") == "pull_request" and run.get("path") == WORKFLOW,
          "the run is not a pull request run of the Release matrix")
    check(run.get("head_sha") == head.get("sha") and run.get("head_branch") == head.get("ref")
          and run.get("head_repository", {}).get("full_name") == repository,
          "the run did not belong to the merged PR's head")
    check(run.get("status") == "completed" and run.get("conclusion") == "success",
          "the promotion run's latest attempt has not passed")
    gates = [job for job in jobs if job["name"] == gate(branch)]
    check(len(gates) == 1 and gates[0].get("conclusion") == "success"
          and gates[0].get("run_attempt") == run.get("run_attempt"),
          gate(branch) + " is missing or unsuccessful in the promotion run")
    failed = unsuccessful(jobs)
    check(not failed, "jobs of the promotion run did not succeed: " + ", ".join(sorted(failed)[:5]))
    if not check(isinstance(receipt, dict) and receipt.get("schema") == SCHEMA and receipt.get("kind") == "candidate",
                 "the promotion run has no usable candidate receipt"):
        return reasons
    check(receipt.get("disposition") == "validated", "the candidate receipt does not record a validated matrix")
    check(receipt.get("repository") == repository and receipt.get("workflow") == WORKFLOW,
          "the candidate receipt is of another repository or workflow")
    check(receipt.get("run_id") == run.get("id") and receipt.get("run_attempt") == run.get("run_attempt"),
          "the candidate receipt is of another run or attempt")
    check(receipt.get("pull_request") == pull.get("number") and receipt.get("base") == branch
          and receipt.get("head_sha") == head.get("sha"),
          "the candidate receipt is of another PR, target or head")
    recorded = receipt.get("tested") or {}
    check(isinstance(recorded.get("sha"), str) and SHA.match(recorded.get("sha", "")),
          "the candidate receipt names no tested commit")
    check(tested.get("sha") == recorded.get("sha"), "the tested commit was not read from the repository")
    tested_parents = [parent["sha"] for parent in tested.get("parents", ())]
    check(tested.get("tree", {}).get("sha") == recorded.get("tree") and tested_parents == recorded.get("parents"),
          "the candidate receipt disagrees with its tested commit")
    check(tested.get("tree", {}).get("sha") and tested.get("tree", {}).get("sha") == merged.get("tree", {}).get("sha"),
          "the tested merge and the pushed merge have different trees")
    check(tested_parents == parents, "the tested merge and the pushed merge have different parents")
    digest = receipt.get("coverage", {}).get("digest")
    check(digest and (coverage is None or digest == coverage),
          "the promotion run covered another matrix or image configuration")
    return reasons


def reuse_receipt(repository, branch, sha, merged, pull, run, receipt, reasons):
    """The record of a push's decision: the reuse of a promotion run, or the
    reasons that the full matrix runs."""
    result = {"schema": SCHEMA, "kind": "reuse", "repository": repository,
              "publication": {"branch": branch, "sha": sha, "tree": merged.get("tree", {}).get("sha")},
              "disposition": "full-matrix" if reasons else "reused", "reasons": reasons}
    if not reasons:
        result |= {"pull_request": pull["number"], "tested": receipt["tested"],
                   "source": {"run_id": run["id"], "run_attempt": run["run_attempt"], "url": run.get("html_url")},
                   "coverage": receipt["coverage"], "images": receipt["images"], "retried": receipt["retried"]}
    return result


def promotion_run(runs, head):
    """The newest successful pull request run of the Release matrix for the
    PR head, or the newest run when none passed."""
    candidates = sorted((run for run in runs if run.get("event") == "pull_request" and run.get("head_sha") == head),
                        key=lambda run: run["run_number"], reverse=True)
    passed = [run for run in candidates if run.get("status") == "completed" and run.get("conclusion") == "success"]
    return (passed or candidates or [{}])[0]


def decide(api, download, repository, branch, sha, coverage=None):
    """Gather the facts of a push and qualify it. api(path, pages=...) reads
    this repository's REST API and download(artifact) returns an artifact's
    single JSON file. Returns (reuse receipt, reasons). A fact that cannot
    be read is a reason, never an eligibility."""
    merged, pull, run, jobs, receipt, tested = {}, {}, {}, [], None, {}
    try:
        merged = api("git/commits/" + sha)
        pulls = [candidate for candidate in api("commits/" + sha + "/pulls?per_page=100", pages=True)
                 if candidate.get("merge_commit_sha") == sha]
        if len(pulls) == 1:
            pull = pulls[0]
            head = pull.get("head", {}).get("sha", "")
            runs = api("actions/workflows/release-matrix.yml/runs?event=pull_request&head_sha=" + head
                       + "&per_page=100", pages="workflow_runs")
            run = promotion_run(runs, head)
        if run:
            attempt = "actions/runs/{}/attempts/{}".format(run["id"], run["run_attempt"])
            jobs = api(attempt + "/jobs?per_page=100", pages="jobs")
            name = artifact_name(CANDIDATE, run["run_attempt"])
            artifacts = [artifact for artifact in api("actions/runs/{}/artifacts?per_page=100".format(run["id"]),
                                                      pages="artifacts")
                         if artifact["name"] == name and not artifact.get("expired")]
            if len(artifacts) == 1:
                receipt = download(artifacts[0])
                recorded = (receipt.get("tested") or {}).get("sha", "") if isinstance(receipt, dict) else ""
                if isinstance(recorded, str) and SHA.match(recorded):
                    tested = api("git/commits/" + recorded)
        reasons = qualify(repository, branch, sha, merged, pull, run, jobs, receipt, tested, coverage)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile) as error:
        reasons = ["the promotion evidence could not be read: " + str(error).strip()[:300]]
    return reuse_receipt(repository, branch, sha, merged, pull, run, receipt, reasons), reasons


def download_json(artifact):
    """The single JSON file of an artifact, through the authenticated CLI."""
    result = subprocess.run(["gh", "api", artifact["archive_download_url"]], check=True, capture_output=True)
    with zipfile.ZipFile(io.BytesIO(result.stdout)) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError("artifact " + artifact["name"] + " does not hold exactly one file")
        return json.loads(archive.read(names[0]))


def checked_out():
    """The commit of the working tree: its sha, tree and parents."""
    def git(*args):
        return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.split()

    sha, tree = git("rev-parse", "HEAD", "HEAD^{tree}")
    return {"sha": sha, "tree": tree, "parents": git("rev-list", "--parents", "-n", "1", "HEAD")[1:]}


def write_output(values):
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write("".join(key + "=" + value + "\n" for key, value in values.items()))


def main(argv=None):
    import git_flow

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("record", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--coverage", required=True, type=Path,
                             help="The report of tag_scenarios.py releases for the checked-out tree")
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    report = json.loads(args.coverage.read_text())
    repository, run_id = os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.command == "record":
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
        jobs = git_flow.api("actions/runs/{}/attempts/{}/jobs?per_page=100".format(run_id, attempt), pages="jobs")
        receipt = candidate_receipt(repository, event["number"], event["base"]["ref"], event["head"]["sha"],
                                    run_id, attempt, checked_out(), report["coverage"], report["images"], jobs)
        args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0
    receipt, reasons = decide(git_flow.api, download_json, repository, os.environ["GITHUB_REF_NAME"],
                              os.environ["GITHUB_SHA"], report["coverage"]["digest"])
    args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    for reason in reasons:
        print("::notice::Full Release matrix: " + reason)
    write_output({"reused": "false" if reasons else "true"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
