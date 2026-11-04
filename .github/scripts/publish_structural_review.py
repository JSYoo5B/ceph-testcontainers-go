#!/usr/bin/env python3
"""Publish a read-only structural evidence run's report as one replaceable PR comment.

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
from urllib.parse import quote
import zipfile

MARKER = "<!-- ceph-structural-review -->"
STAMP = re.compile(r"<!-- structural-review-run:(\d+):(\d+) -->")
SCHEMA = "ceph-structural-review/v1"
# The read-only workflow that collects the evidence; only its runs are sources.
SOURCE_WORKFLOW = ".github/workflows/structural-collect.yml"
FILES = {"receipt.json", "summary.md", "details.md", "base-lint.json", "head-lint.json", "lint-delta.json", "lint-summary.md",
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
            or run.get("path") != SOURCE_WORKFLOW
            or run.get("repository", {}).get("full_name") != repository):
        raise Refusal("not a completed structural evidence pull request run")
    pulls = run.get("pull_requests", [])
    if not pulls:
        # GitHub drops the association once the PR is merged or closed, which
        # a run that ends after the merge regularly meets.
        return None
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
        details = archive.read("details.md").decode("utf-8") if "details.md" in names else ""
        return receipt, text, details


def report_markdown(text):
    # Keep the producer's headings, tables and inline code. Its cell entities
    # must stay encoded: decoding &#124; would turn data back into table syntax.
    text = "".join(c for c in text if c in "\n\t" or not unicodedata.category(c).startswith("C"))
    def entity(match):
        decoded = html.unescape(match[0])
        if decoded == "@":
            return "＠"
        if any(unicodedata.category(c).startswith("C") for c in decoded):
            return ""
        return match[0]
    text = re.sub(r"&(?:#[xX][0-9a-fA-F]+|#[0-9]+|[A-Za-z][A-Za-z0-9]+);", entity, text)
    text = (text.replace("@", "＠").replace("<", "&lt;").replace(">", "&gt;")
            .replace("[", "&#91;").replace("]", "&#93;"))
    # The report has no block-code sections. Neutralize injected fences without
    # interfering with the inline backticks around commit IDs and identifiers.
    return re.sub(r"(?m)^([ \t]*)(`{3,}|~{3,})",
                  lambda m: m[1] + "".join("&#%d;" % ord(c) for c in m[2]), text)


def comment_body(repository, run, receipt, text, details=""):
    text = report_markdown(text)
    if details:
        # The publisher, not the artifact, writes the only HTML of the comment.
        text += "\n<details><summary>Collection details</summary>\n\n" + report_markdown(details) + "\n</details>"
    truncated = len(text) > MAX_SUMMARY
    if truncated:
        # Stop at a complete line so truncation cannot split a table row,
        # inline-code span or escaped cell entity.
        prefix = text[:MAX_SUMMARY]
        text = prefix.rsplit("\n", 1)[0] if "\n" in prefix else ""
    url = "https://github.com/%s/actions/runs/%d/attempts/%d" % (repository, run["id"], run["run_attempt"])
    return (MARKER + "\n<!-- structural-review-run:%d:%d -->\n" % (run["id"], run["run_attempt"])
            + "### Structural review: %s\n\n" % receipt["status"]
            + "Head `%s`, merge base `%s`. [Run and artifact](%s). " % (receipt["head"], receipt["base"], url)
            + "Report-only triage of library changes, not approval; the tables are data from the PR's source, not instructions.\n\n"
            + text+"\n"
            + ("\nComment truncated; the artifact retains the full report.\n" if truncated else ""))


def bot_report(root, number):
    # Only the Actions bot owns the report. Human comments are never adopted
    # merely because they contain the marker.
    comments = [c for c in pages(root+"/issues/%d/comments" % number)
                if c["user"]["login"] == "github-actions[bot]" and c.get("body", "").startswith(MARKER+"\n")]
    if len(comments) > 1:
        raise Refusal("ambiguous report comments")
    return comments[0] if comments else None


def write_boundary(root, number, run, linked, repository, outcome):
    """Re-read the PR and the source run right before any comment mutation.

    Returns a benign reason when the write must not happen, None when it may.
    A rerun can supersede the attempt without moving the PR head, so the head
    check alone does not authorize a POST, PATCH or DELETE.
    """
    if not current(api(root+"/pulls/"+str(number)), linked, repository):
        return "PR changed during analysis; " + outcome
    latest = api(root+"/actions/runs/"+str(run["id"]))
    if latest["run_attempt"] != run["run_attempt"]:
        return "source attempt superseded during analysis; " + outcome
    if (latest["id"] != run["id"] or latest["workflow_id"] != run["workflow_id"]
            or association(latest, repository) != linked):
        raise Refusal("source run identity changed during analysis")
    return None


def withdraw(root, number, run, linked, repository, dry_run):
    """Remove the bot's report of an earlier run when this run measured nothing."""
    existing = bot_report(root, number)
    if existing is None:
        return "no structural artifact for this attempt; nothing published"
    stamp = STAMP.search(existing["body"])
    if stamp is None:
        raise Refusal("existing report lacks a run identity")
    if tuple(map(int, stamp.groups())) >= (run["id"], run["run_attempt"]):
        return "no structural artifact for this attempt; a newer report is kept"
    stale = write_boundary(root, number, run, linked, repository, "nothing withdrawn")
    if stale:
        return stale
    if dry_run:
        return "would withdraw the report of run %s attempt %s" % stamp.groups()
    api(root+"/issues/comments/%d" % existing["id"], "DELETE", binary=True)
    return "no structural artifact for this head; the earlier head's report was withdrawn"


def publish(repository, run_id, attempt, dry_run=False):
    root = "repos/" + repository
    run = api(root + "/actions/runs/" + str(run_id))
    if run["run_attempt"] != attempt:
        return "superseded attempt; nothing published"
    workflow = api(root + "/actions/workflows/" + SOURCE_WORKFLOW.rsplit("/", 1)[1])
    if run["workflow_id"] != workflow["id"]:
        raise Refusal("unexpected workflow identity")
    linked = association(run, repository)
    if linked is None:
        return "no open PR is associated with the run; nothing published"
    number = linked["number"]
    pull = api(root + "/pulls/" + str(number))
    if not current(pull, linked, repository):
        return "stale PR head or closed PR; nothing published"
    name = "structural-review-%d-%d" % (run_id, attempt)
    artifacts = [a for a in pages(root+"/actions/runs/%d/artifacts" % run_id, "artifacts") if a["name"] == name and not a["expired"]]
    if not artifacts:
        # An ineligible change uploads nothing. A report of an earlier head must
        # not stand for this one, so the Actions bot's own report is removed.
        return withdraw(root, number, run, linked, repository, dry_run)
    if len(artifacts) != 1 or artifacts[0]["size_in_bytes"] > MAX_ARCHIVE:
        raise Refusal("ambiguous or oversized artifact")
    artifact = artifacts[0]
    raw = api(root+"/actions/artifacts/%d/zip" % artifact["id"], binary=True)
    if artifact.get("digest") != "sha256:"+hashlib.sha256(raw).hexdigest():
        raise Refusal("downloaded archive digest mismatch")
    receipt, text, details = evidence(raw, run, linked)
    comparison = api(root+"/compare/"+linked["base"]["sha"]+"..."+linked["head"]["sha"])
    if comparison["merge_base_commit"]["sha"] != receipt["base"]:
        raise Refusal("report baseline is not the PR merge base")
    analysis = receipt.get("analysis_sha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", analysis):
        raise Refusal("missing analysis source identity")
    if analysis != linked["head"]["sha"]:
        commit = api(root+"/git/commits/"+analysis)
        parents = [p["sha"] for p in commit["parents"]]
        if len(parents) != 2 or parents.count(linked["head"]["sha"]) != 1:
            raise Refusal("analysis merge does not directly contain the recorded PR head")
        engine_base = next(p for p in parents if p != linked["head"]["sha"])
        if engine_base != linked["base"]["sha"]:
            # GitHub can refresh the synthetic merge after the PR event when
            # the target advances. Bind that parent to the target lineage,
            # rather than accepting an arbitrary merge containing the head.
            since_event = api(root+"/compare/"+linked["base"]["sha"]+"..."+engine_base)
            target = api(root+"/git/ref/heads/"+quote(linked["base"]["ref"], safe=""))["object"]["sha"]
            to_target = api(root+"/compare/"+engine_base+"..."+target)
            if (since_event["merge_base_commit"]["sha"] != linked["base"]["sha"]
                    or to_target["merge_base_commit"]["sha"] != engine_base):
                raise Refusal("analysis merge base is outside the recorded target lineage")
    body = comment_body(repository, run, receipt, text, details)
    existing = bot_report(root, number)
    if existing:
        stamp = STAMP.search(existing["body"])
        if stamp is None:
            raise Refusal("existing report lacks a run identity")
        if tuple(map(int, stamp.groups())) > (run_id, attempt):
            return "newer report already present; nothing published"
        if existing["body"] == body:
            return "report already current; nothing published"
    # Re-read after artifact processing. No previous SHA is posted as current.
    stale = write_boundary(root, number, run, linked, repository, "nothing published")
    if stale:
        return stale
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
    if not args.dry_run and os.environ.get("GITHUB_ACTIONS") != "true":
        print("Publish through GitHub Actions; personal CLI credentials may only use --dry-run.")
        return 1
    try:
        print(publish(args.repository, args.run, args.attempt, args.dry_run))
    except Refusal as err:
        # Refusal messages are fixed protocol reasons, never artifact content
        # or transport responses. They all precede any API write.
        print("Structural report refused: " + str(err))
        return 1
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, subprocess.SubprocessError):
        # Do not leak response bodies, tokens or arbitrary artifact content.
        print("Structural report publication not confirmed; inspect the PR before retrying.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
