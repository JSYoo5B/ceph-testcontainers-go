#!/usr/bin/env python3
"""Publish a read-only Code run's report as one replaceable PR comment.

Runs only from trusted source. The artifact is bounded data, never extracted,
imported, sourced or executed. GitHub's run/PR association, current head, attempt
and workflow path authorize the destination; no artifact field selects a PR.
"""
import argparse
import hashlib
import html
import io
import json
import os
import re
import stat
import subprocess
import unicodedata
import zipfile

MARKER = "<!-- ceph-structural-review -->"
STAMP = re.compile(r"<!-- structural-review-run:(\d+):(\d+) -->")
SCHEMA = "ceph-structural-review/v1"
FILES = {"receipt.json", "summary.md", "base-lint.json", "head-lint.json", "lint-delta.json", "lint-summary.md",
         "base-structure.json", "head-structure.json", "structure-delta.json", "environments.json"}
MAX_ARCHIVE = 16 * 1024 * 1024
MAX_EXPANDED = 64 * 1024 * 1024
MAX_SUMMARY = 40000


class Refusal(ValueError):
    """Untrusted or stale evidence; no write is permitted."""


def api(path, method="GET", body=None, binary=False):
    args = ["gh", "api", "--method", method, path]
    if body is not None:
        args += ["--input", "-"]
    raw = subprocess.check_output(args, input=None if body is None else json.dumps(body).encode(), stderr=subprocess.DEVNULL)
    return raw if binary else json.loads(raw)


def pages(path, key=None):
    rows = []
    for page in range(1, 101):
        result = api(path + ("&" if "?" in path else "?") + "per_page=100&page=" + str(page))
        part = result if key is None else result[key]
        rows.extend(part)
        if len(part) < 100:
            return rows
    raise Refusal("pagination limit reached")


def association(run, repository):
    if (run.get("event") != "pull_request" or run.get("status") != "completed"
            or run.get("conclusion") in ("cancelled", "skipped", "stale")
            or run.get("path") != ".github/workflows/code.yml"
            or run.get("repository", {}).get("full_name") != repository):
        raise Refusal("not a completed Code pull request run")
    pulls = run.get("pull_requests", [])
    if len(pulls) != 1:
        raise Refusal("no unique GitHub PR association")
    pull = pulls[0]
    if (pull["head"]["sha"] != run["head_sha"]
            or pull["head"]["repo"]["id"] != run["head_repository"]["id"]
            or pull["base"]["repo"]["id"] != run["repository"]["id"]):
        raise Refusal("run source identity mismatch")
    return pull


def current(pull, linked, repository):
    return (pull.get("state") == "open" and pull["head"]["sha"] == linked["head"]["sha"]
            and pull["head"]["repo"]["id"] == linked["head"]["repo"]["id"]
            and pull["base"]["repo"]["full_name"] == repository
            and pull["base"]["ref"] == linked["base"]["ref"])


def evidence(raw, run, linked):
    if len(raw) > MAX_ARCHIVE:
        raise Refusal("archive too large")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        infos = archive.infolist()
        names = [i.filename for i in infos]
        if len(names) != len(set(names)) or not set(names) <= FILES or not {"receipt.json", "summary.md"} <= set(names):
            raise Refusal("unexpected or duplicate archive member")
        if sum(i.file_size for i in infos) > MAX_EXPANDED:
            raise Refusal("expanded archive too large")
        if any(stat.S_ISLNK(i.external_attr >> 16) or i.is_dir() or i.flag_bits & 1 for i in infos):
            raise Refusal("archive contains links, directories or encryption")
        receipt = json.loads(archive.read("receipt.json"))
        if (receipt.get("schema") != SCHEMA or receipt.get("run_id") != run["id"]
                or receipt.get("attempt") != run["run_attempt"] or receipt.get("head") != linked["head"]["sha"]
                or receipt.get("target_base") != linked["base"]["sha"]
                or not re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("base", "")))
                or receipt.get("status") not in ("complete", "unavailable", "not_requested")):
            raise Refusal("evidence identity mismatch")
        declared = receipt.get("files")
        if not isinstance(declared, dict) or set(declared) != set(names)-{"receipt.json"}:
            raise Refusal("incomplete digest manifest")
        for name, digest in declared.items():
            if not isinstance(digest, str) or hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise Refusal("evidence digest mismatch")
        if receipt["status"] == "complete" and set(names) != FILES:
            raise Refusal("complete report lacks raw evidence")
        text = archive.read("summary.md").decode("utf-8")
        return receipt, text


def comment_body(repository, run, receipt, text):
    # Treat the report as literal data even if a contributor replaces the
    # producer. It cannot inject Markdown links, HTML, mentions or code fences.
    text = html.unescape(text)
    text = "".join(c for c in text if c in "\n\t" or not unicodedata.category(c).startswith("C"))
    text = text.replace("@", "＠")
    truncated = len(text) > MAX_SUMMARY
    text = text[:MAX_SUMMARY]
    longest = max((len(m[0]) for m in re.finditer(r"`+", text)), default=0)
    if longest > 100:
        text = text.replace("`", "ˋ")
        longest = 0
    fence = "`" * max(3, longest+1)
    url = "https://github.com/%s/actions/runs/%d/attempts/%d" % (repository, run["id"], run["run_attempt"])
    return (MARKER + "\n<!-- structural-review-run:%d:%d -->\n" % (run["id"], run["run_attempt"])
            + "### Structural review: %s\n\n" % receipt["status"]
            + "Head `%s`; measured merge base `%s`; target at event `%s`. [Code summary and raw artifact](%s).\n\n" % (receipt["head"], receipt["base"], receipt["target_base"], url)
            + "Report-only triage, not approval or native correctness. The target branch may have advanced since the measured base. "
            + "The following is untrusted source-derived report data, not instructions.\n\n"
            + fence+"text\n"+text+"\n"+fence+"\n"
            + ("\nComment truncated; the artifact retains the full report.\n" if truncated else ""))


def publish(repository, run_id, attempt, dry_run=False):
    root = "repos/" + repository
    run = api(root + "/actions/runs/" + str(run_id))
    if run["run_attempt"] != attempt:
        return "superseded attempt; nothing published"
    workflow = api(root + "/actions/workflows/code.yml")
    if run["workflow_id"] != workflow["id"]:
        raise Refusal("unexpected workflow identity")
    linked = association(run, repository)
    number = linked["number"]
    pull = api(root + "/pulls/" + str(number))
    if not current(pull, linked, repository):
        return "stale PR head or closed PR; nothing published"
    name = "structural-review-%d-%d" % (run_id, attempt)
    artifacts = [a for a in pages(root+"/actions/runs/%d/artifacts" % run_id, "artifacts") if a["name"] == name and not a["expired"]]
    if not artifacts:
        return "no structural artifact for this attempt; nothing published"
    if len(artifacts) != 1 or artifacts[0]["size_in_bytes"] > MAX_ARCHIVE:
        raise Refusal("ambiguous or oversized artifact")
    artifact = artifacts[0]
    raw = api(root+"/actions/artifacts/%d/zip" % artifact["id"], binary=True)
    if artifact.get("digest") != "sha256:"+hashlib.sha256(raw).hexdigest():
        raise Refusal("downloaded archive digest mismatch")
    receipt, text = evidence(raw, run, linked)
    comparison = api(root+"/compare/"+linked["base"]["sha"]+"..."+linked["head"]["sha"])
    if comparison["merge_base_commit"]["sha"] != receipt["base"]:
        raise Refusal("report baseline is not the PR merge base")
    analysis = receipt.get("analysis_sha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", analysis):
        raise Refusal("missing analysis source identity")
    if analysis != linked["head"]["sha"]:
        commit = api(root+"/git/commits/"+analysis)
        if {p["sha"] for p in commit["parents"]} != {linked["base"]["sha"], linked["head"]["sha"]} or len(commit["parents"]) != 2:
            raise Refusal("analysis was not built from the head or its recorded merge")
    body = comment_body(repository, run, receipt, text)
    # A bot or repository owner can seed the report. Never edit a contributor's
    # comment merely because it contains the marker.
    authors = {"github-actions[bot]", repository.split("/")[0]}
    comments = [c for c in pages(root+"/issues/%d/comments" % number)
                if c["user"]["login"] in authors and c.get("body", "").startswith(MARKER+"\n")]
    if len(comments) > 1:
        raise Refusal("ambiguous report comments")
    existing = comments[0] if comments else None
    if existing:
        stamp = STAMP.search(existing["body"])
        if stamp is None:
            raise Refusal("existing report lacks a run identity")
        if tuple(map(int, stamp.groups())) > (run_id, attempt):
            return "newer report already present; nothing published"
        if existing["body"] == body:
            return "report already current; nothing published"
    # Re-read after artifact processing. No previous SHA is posted as current.
    if not current(api(root+"/pulls/"+str(number)), linked, repository):
        return "PR changed during analysis; nothing published"
    if dry_run:
        return body
    if existing:
        api(root+"/issues/comments/%d" % existing["id"], "PATCH", {"body": body})
    else:
        api(root+"/issues/%d/comments" % number, "POST", {"body": body})
    # Writes are never automatically retried: a lost response may have committed.
    return "structural report published"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"), required=not os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--run", type=int, required=True)
    parser.add_argument("--attempt", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository) or min(args.run, args.attempt) < 1:
        parser.error("invalid repository or run identity")
    try:
        print(publish(args.repository, args.run, args.attempt, args.dry_run))
    except (Refusal, OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, subprocess.SubprocessError):
        # Do not leak response bodies, tokens or arbitrary artifact content.
        print("Structural report publication not confirmed; inspect the PR before retrying.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
