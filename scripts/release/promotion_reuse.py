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

The receipt does not stand for the run's native work by itself. The jobs'
own artifacts expire weeks before the receipt does, so record keeps, beside
the receipt and for as long as it, the history row of every runtime job
(what it checked out, built and pulled, each phase's result, its cleanup
check and the hashes of its logs and reports) and the report of each retry.
verify reuses a run only when that evidence accounts for every runtime job
of the run: a missing, foreign, altered, failed or uncleaned entry selects
the full matrix, whether or not the receipt itself is still there.
"""

import argparse
import hashlib
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
# The branch on which the promotion path is rehearsed end to end. It is no
# publication branch: nothing is tagged or published from it.
REHEARSAL = "rehearsal/promotion"
# Branches whose push may reuse a promotion run: the publication branch and
# its rehearsal. A support line keeps full push validation until its
# protected path has been demonstrated.
BRANCHES = ("main", REHEARSAL)
CANDIDATE = "release-candidate-receipt"
REUSE = "promotion-reuse-receipt"
# The files of the two artifacts: a receipt and the evidence it accounts for.
RECEIPT_FILE = {CANDIDATE: CANDIDATE + ".json", REUSE: REUSE + ".json"}
EVIDENCE_FILE = "release-candidate-evidence.jsonl"
ROW_SCHEMA = "ceph-scenario-history/v1"
RETRY_SCHEMA = "ceph-tag-scenarios/v1/retry"
PLAN_JOB = "Plan"
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


def row_digest(row):
    """A history row's own hash, as scenario_history.row_hash computes it."""
    content = {key: value for key, value in row.items() if key != "row_sha256"}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def collect_evidence(directory):
    """The evidence entries of the downloaded job artifacts under directory:
    each job's history-row.json with the report of its retry, if it had one.
    Entries are ordered by job, so the same artifacts give the same bytes."""
    entries = []
    for path in sorted(Path(directory).rglob("history-row.json")):
        retry = path.parent / "native-retry" / "report.json"
        entries.append({"row": json.loads(path.read_text(encoding="utf-8")),
                        "retry": json.loads(retry.read_text(encoding="utf-8")) if retry.exists() else None})
    return sorted(entries, key=lambda entry: str((entry["row"].get("execution_key") or {}).get("job_id")))


def encode_evidence(entries):
    return "".join(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n" for entry in entries).encode()


def evidence_summary(content):
    """What a receipt records of its evidence file: its size in entries and
    its hash."""
    return {"file": EVIDENCE_FILE, "entries": len(content.splitlines()), "sha256": hashlib.sha256(content).hexdigest()}


def runtime_jobs(jobs):
    """The jobs of a run that executed a batch: jobs of a called runtime
    workflow other than its Plan job."""
    return [job for job in jobs if " / " in job["name"] and job["name"].rpartition(" / ")[2] != PLAN_JOB]


def evidence_problems(repository, run, jobs, receipt, tested, content):
    """Why the preserved evidence does not account for the promotion run's
    native work, as a list of reasons.

    content is the evidence file's bytes, or None when the artifact has
    none. Every runtime job of the run needs exactly one entry of its own
    whose row is unaltered, came from this run and the tested commit,
    recorded its images when it needed Ceph, passed its cleanup check and
    either passed, or failed only its native phase and passed its one retry."""
    recorded = receipt.get("evidence") or {}
    if content is None:
        return ["the promotion run preserved no evidence of its native jobs"]
    if evidence_summary(content) != recorded:
        return ["the preserved evidence is not the file that the candidate receipt records"]
    try:
        entries = [json.loads(line) for line in content.decode("utf-8").splitlines()]
    except (ValueError, UnicodeDecodeError):
        return ["the preserved evidence is not readable"]
    passed_retries = {entry["id"] for entry in receipt.get("retried") or () if entry.get("retry") == "success"}
    by_job, problems = {}, []
    for entry in entries:
        row = entry.get("row") if isinstance(entry, dict) else None
        key = row.get("execution_key") if isinstance(row, dict) else None
        if not isinstance(key, dict) or row.get("schema") != ROW_SCHEMA or row.get("row_sha256") != row_digest(row):
            problems.append("an evidence entry is not an unaltered history row")
            continue
        by_job.setdefault(str(key.get("job_id")), []).append(entry)
    expected = {str(job["id"]): job["name"] for job in runtime_jobs(jobs)}
    if not expected:
        problems.append("the promotion run has no runtime job")
    for job, name in sorted(expected.items(), key=lambda item: item[1]):
        found = by_job.pop(job, [])
        if len(found) != 1:
            problems.append(name + (": no preserved evidence" if not found else ": more than one evidence entry"))
            continue
        problem = entry_problem(repository, run, tested, found[0], int(job) in passed_retries)
        if problem:
            problems.append(name + ": " + problem)
    if by_job:
        problems.append(str(sum(len(found) for found in by_job.values())) + " evidence entries belong to no runtime job of the run")
    return problems[:5] + (["and " + str(len(problems) - 5) + " more evidence problems"] if len(problems) > 5 else [])


def entry_problem(repository, run, tested, entry, retry_passed):
    """Why one job's evidence entry does not show a qualifying execution, or
    None."""
    row, retry = entry["row"], entry.get("retry")
    key, source, outcome = row["execution_key"], row.get("source") or {}, row.get("outcome") or {}
    if key.get("repository") != repository or str(key.get("run_id")) != str(run.get("id")):
        return "its evidence is of another run"
    if source.get("checkout_commit") != tested.get("sha") or source.get("checkout_tree") != tested.get("tree", {}).get("sha"):
        return "it did not check out the tested commit"
    if (row.get("evidence") or {}).get("problems") or (row.get("comparability") or {}).get("status") != "complete":
        return "its records are incomplete"
    phases = {phase.get("phase"): phase.get("status") for phase in row.get("phases") or ()}
    if phases.get("cleanup") != "passed":
        return "its cleanup check did not pass"
    if (row.get("profile") or {}).get("requires_ceph"):
        runtime = row.get("runtime") or {}
        roles = runtime.get("roles") or {}
        if runtime.get("status") != "recorded" or not roles or not all(
                isinstance(role, dict) and role.get("image_id") and role.get("reference") for role in roles.values()):
            return "it did not record the images it ran"
    if outcome.get("status") == "passed" and retry is None and not retry_passed:
        return None
    retried_once = (isinstance(retry, dict) and retry.get("schema") == RETRY_SCHEMA and retry.get("passed") is True
                    and retry.get("attempt_classification") == "passed" and not retry.get("refused"))
    if (outcome.get("status") == "failed" and outcome.get("failed_phases") == ["native"]
            and not (outcome.get("cancelled_phases") or outcome.get("missing_phases") or outcome.get("skipped_phases"))
            and retried_once and retry_passed):
        return None
    return "it neither passed nor passed its one retry after a native failure"


def observed(content):
    """What the jobs of the evidence ran: each image reference with the
    image IDs and digests it resolved to, and each Go toolchain."""
    references, toolchains = {}, set()
    for line in content.decode("utf-8").splitlines():
        row = json.loads(line)["row"]
        version = ((row.get("build") or {}).get("build_key") or {}).get("go_version")
        if version:
            toolchains.add(version)
        for role in ((row.get("runtime") or {}).get("roles") or {}).values():
            seen = references.setdefault(role["reference"], {"image_ids": set(), "repo_digests": set()})
            seen["image_ids"].add(role["image_id"])
            seen["repo_digests"].update(role.get("repo_digests") or ())
    return {"images": {reference: {field: sorted(values) for field, values in seen.items()}
                       for reference, seen in sorted(references.items())},
            "go_versions": sorted(toolchains)}


def candidate_receipt(repository, pull_request, base, head_sha, run_id, run_attempt, tested, coverage, images, jobs,
                      evidence):
    """The receipt of a promotion run whose complete matrix passed.

    tested is the commit that the workflow checked out: its sha, tree and
    parents as git reports them in that checkout. evidence is the summary of
    the evidence file kept beside the receipt."""
    return {"schema": SCHEMA, "kind": "candidate", "repository": repository, "workflow": WORKFLOW,
            "pull_request": pull_request, "base": base, "head_sha": head_sha,
            "run_id": run_id, "run_attempt": run_attempt, "tested": tested,
            "coverage": coverage, "images": images, "retried": retried(jobs), "evidence": evidence,
            "disposition": "validated"}


def unsuccessful(jobs):
    """Jobs of a promotion run that did not succeed. The reuse job is skipped
    on a pull request and is no part of the matrix."""
    return [job["name"] for job in jobs
            if job.get("conclusion") != "success"
            and not (job["name"] == REUSE_JOB and job.get("conclusion") == "skipped")]


def qualify(repository, branch, sha, merged, pull, run, jobs, receipt, tested, coverage=None, evidence=None):
    """Why the promotion run does not qualify the push of sha to branch, as a
    list of reasons; an empty list means that it does.

    merged and tested are the commit objects of the pushed merge and of the
    receipt's test merge as the repository returns them, pull the merged PR,
    run and jobs the promotion run's latest attempt, receipt its candidate
    receipt, coverage the coverage digest of the pushed tree (None when the
    caller cannot compute it and compares receipts instead), and evidence
    the bytes of the evidence file kept beside the receipt, or None."""
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
    reasons.extend(evidence_problems(repository, run, jobs, receipt, tested, evidence))
    return reasons


def reuse_receipt(repository, branch, sha, merged, pull, run, receipt, reasons, evidence=None):
    """The record of a push's decision: the reuse of a promotion run, or the
    reasons that the full matrix runs."""
    result = {"schema": SCHEMA, "kind": "reuse", "repository": repository,
              "publication": {"branch": branch, "sha": sha, "tree": merged.get("tree", {}).get("sha")},
              "disposition": "full-matrix" if reasons else "reused", "reasons": reasons}
    if not reasons:
        result |= {"pull_request": pull["number"], "tested": receipt["tested"],
                   "source": {"run_id": run["id"], "run_attempt": run["run_attempt"], "url": run.get("html_url")},
                   "coverage": receipt["coverage"], "images": receipt["images"], "retried": receipt["retried"],
                   "evidence": receipt["evidence"], "observed": observed(evidence)}
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
    files as {name: bytes}. Returns (reuse receipt, reasons, evidence
    bytes or None). A fact that cannot be read is a reason, never an
    eligibility."""
    merged, pull, run, jobs, receipt, tested, evidence = {}, {}, {}, [], None, {}, None
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
                files = download(artifacts[0])
                receipt = json.loads(files[RECEIPT_FILE[CANDIDATE]]) if RECEIPT_FILE[CANDIDATE] in files else None
                evidence = files.get(EVIDENCE_FILE)
                recorded = (receipt.get("tested") or {}).get("sha", "") if isinstance(receipt, dict) else ""
                if isinstance(recorded, str) and SHA.match(recorded):
                    tested = api("git/commits/" + recorded)
        reasons = qualify(repository, branch, sha, merged, pull, run, jobs, receipt, tested, coverage, evidence)
        result = reuse_receipt(repository, branch, sha, merged, pull, run, receipt, reasons, evidence)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError,
            zipfile.BadZipFile) as error:
        reasons = ["the promotion evidence could not be read: " + str(error).strip()[:300]]
        result = reuse_receipt(repository, branch, sha, merged, pull, run, receipt, reasons)
    return result, reasons, None if reasons else evidence


def download_files(artifact):
    """The files of an artifact as {name: bytes}, through the authenticated
    CLI. A receipt artifact holds a receipt and at most its evidence file."""
    result = subprocess.run(["gh", "api", artifact["archive_download_url"]], check=True, capture_output=True)
    with zipfile.ZipFile(io.BytesIO(result.stdout)) as archive:
        names = archive.namelist()
        if not names or len(names) > 2:
            raise ValueError("artifact " + artifact["name"] + " does not hold a receipt and its evidence")
        return {name: archive.read(name) for name in names}


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
        command.add_argument("--output", required=True, type=Path,
                             help="Directory for the receipt and its evidence file")
    commands.choices["record"].add_argument("--rows", required=True, type=Path,
                                            help="Directory of this run's downloaded tag-scenario artifacts")
    args = parser.parse_args(argv)
    report = json.loads(args.coverage.read_text())
    repository, run_id = os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    args.output.mkdir(parents=True, exist_ok=True)
    if args.command == "record":
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
        jobs = git_flow.api("actions/runs/{}/attempts/{}/jobs?per_page=100".format(run_id, attempt), pages="jobs")
        content = encode_evidence(collect_evidence(args.rows))
        receipt = candidate_receipt(repository, event["number"], event["base"]["ref"], event["head"]["sha"],
                                    run_id, attempt, checked_out(), report["coverage"], report["images"], jobs,
                                    evidence_summary(content))
        (args.output / EVIDENCE_FILE).write_bytes(content)
        (args.output / RECEIPT_FILE[CANDIDATE]).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        print(json.dumps(receipt, indent=2, sort_keys=True))
        # The record is only usable if it accounts for the run; say so now
        # instead of at the publication push.
        tested = {"sha": receipt["tested"]["sha"], "tree": {"sha": receipt["tested"]["tree"]}}
        for problem in evidence_problems(repository, {"id": run_id}, jobs, receipt, tested, content):
            print("::warning::The candidate evidence will not qualify a reuse: " + problem)
        return 0
    receipt, reasons, evidence = decide(git_flow.api, download_files, repository, os.environ["GITHUB_REF_NAME"],
                                        os.environ["GITHUB_SHA"], report["coverage"]["digest"])
    (args.output / RECEIPT_FILE[REUSE]).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    if evidence is not None:
        # The publication keeps its own copy of what qualified it.
        (args.output / EVIDENCE_FILE).write_bytes(evidence)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    for reason in reasons:
        print("::notice::Full Release matrix: " + reason)
    write_output({"reused": "false" if reasons else "true"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
