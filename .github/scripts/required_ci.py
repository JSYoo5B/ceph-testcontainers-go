#!/usr/bin/env python3
"""Keep the requested CI that a reviewer made a merge requirement, and publish
whether a pull request head has it.

The commit status "Required requested CI" is the one fixed context a branch
rule can require. It is set on the head commit of every open pull request:
success when the pull request requires nothing, or when every required
selection passed on that exact head.

The requirements of a pull request are a ledger of comments on it. A ledger
record is the first line of a comment written with the workflow token
(github-actions[bot]); ci_command.py defines the line. Records are only ever
added:

- require adds a selection, with the command that asked for it and the
  commit that command runs;
- retry and blocked make another command the selection's current attempt;
  blocked also says that the names no longer resolve on that commit;
- drop removes the selection;
- result is the outcome of one attempt's run.

A result counts only for the selection's current attempt and its commit, so
a run that finishes late, a re-run of an older command or a result on another
pull request never satisfies a requirement. A record whose comment was edited,
or that cannot be read, makes the pull request's status an error.

A commit status belongs to a commit, not to a pull request. Pull requests
that share a head commit share the status, which is the worst of theirs.

Two subcommands, both run only from the default branch, in the one
concurrency group that serializes every publication of the status:

- register applies an accepted require, retry or drop command. For a record
  that makes the head unsatisfied it sets the status pending before it writes
  the record, and the comment holding the record is the acknowledgment. It
  then evaluates.
- evaluate recomputes the status and the summary comment of every open pull
  request from the ledgers. A newer evaluation always supersedes an older one.

A failed job does not fail the status: a commit keeps the status it has until
another one is set. So whatever an evaluation cannot establish it publishes
as an error, never leaves as it was: a pull request whose comments cannot be
read makes its head an error. Both subcommands write settled=true to
GITHUB_OUTPUT only after every head was published; without it the workflow
sets the error itself on the commit that started the job. Only an evaluation
that cannot list the pull requests, or cannot set a status at all, leaves a
head as it was.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import ci_command

CONTEXT = "Required requested CI"
BOT = "github-actions[bot]"
SUMMARY = "<!-- required-requested-ci summary -->"
MAX_DESCRIPTION = 140
SHA = re.compile(r"[0-9a-f]{40}\Z")
ID = re.compile(r"[0-9a-f]{12}\Z")
# The fields of each record besides v and op.
FIELDS = {
    "require": {"pull", "id", "comment", "sha", "names", "selectors", "by", "run"},
    "retry": {"pull", "id", "comment", "sha", "by", "run"},
    "blocked": {"pull", "id", "comment", "sha", "by", "run"},
    "drop": {"pull", "id", "comment", "sha", "by"},
    "result": {"pull", "id", "comment", "sha", "run", "outcome"},
}
# Worst first: the status of a head is that of its worst requirement, and
# of the worst pull request that has the head.
SEVERITY = ("error", "failure", "pending", "success")
STATES = {"passed": "success", "running": "pending", "awaiting": "pending",
          "failed": "failure", "incomplete": "failure", "blocked": "failure"}
# What the reply to a repeated require says of the selection on a commit.
SENTENCES = {"passed": "passed on", "running": "is running on", "awaiting": "has no run on",
             "failed": "failed on", "incomplete": "did not complete its run on", "blocked": "no longer resolves on"}
LABELS = {"passed": "passed", "running": "running", "awaiting": "no run on this head",
          "failed": "failed", "incomplete": "run did not complete", "blocked": "names no longer resolve"}


class Unreadable(Exception):
    """A ledger record that cannot be trusted; the message says why."""


def positive(value):
    return type(value) is int and value > 0


def names(value, empty):
    return (isinstance(value, list) and (empty or value) and len(set(value)) == len(value)
            and all(isinstance(name, str) and ci_command.NAME.fullmatch(name) for name in value))


def check(record, pull):
    """Refuse a record that register or ci_command.report cannot have written
    for this pull request."""
    if not isinstance(record, dict) or record.get("v") != 1 or record.get("op") not in FIELDS:
        raise Unreadable("unknown record")
    if set(record) != FIELDS[record["op"]] | {"v", "op"}:
        raise Unreadable("unexpected fields")
    if record["pull"] != pull:
        raise Unreadable("written for pull request " + str(record["pull"])[:20])
    if not (positive(record["comment"]) and isinstance(record["id"], str) and ID.match(record["id"])
            and isinstance(record["sha"], str) and SHA.match(record["sha"])):
        raise Unreadable("malformed identity")
    for key in ("by", "run"):
        if key in record and not isinstance(record[key], str):
            raise Unreadable("malformed " + key)
    if "by" in record and not ci_command.LOGIN.fullmatch(record["by"]):
        raise Unreadable("malformed author")
    if "run" in record and not re.fullmatch(r"https://[^\s<>()\"']+", record["run"]):
        raise Unreadable("malformed run link")
    if record["op"] == "require" and not (
            names(record["names"], False) and names(record["selectors"], True)
            and record["id"] == ci_command.selection_id(record["names"], record["selectors"])):
        raise Unreadable("selection does not match its identity")
    if record["op"] == "result" and record["outcome"] not in ci_command.OUTCOMES:
        raise Unreadable("unknown outcome")
    return record


def ledger(comments, pull):
    """The records of a pull request's comments, oldest first, and the
    comments that look like records but cannot be trusted.

    Only a comment of the workflow token is a record: anyone can type the
    marker. The token's comments are never edited, so an edited one is not
    what the workflow wrote."""
    records, problems = [], []
    for comment in sorted(comments, key=lambda comment: comment["id"]):
        user = comment.get("user") or {}
        if user.get("login") != BOT or user.get("type") != "Bot":
            continue
        match = ci_command.LEDGER_LINE.fullmatch(((comment.get("body") or "").splitlines() or [""])[0].rstrip())
        if not match:
            continue
        try:
            if comment.get("created_at") != comment.get("updated_at"):
                raise Unreadable("edited after it was written")
            try:
                record = json.loads(match[1])
            except ValueError:
                raise Unreadable("not JSON") from None
            records.append({**check(record, pull), "url": comment.get("html_url", "")})
        except Unreadable as reason:
            problems.append({"url": comment.get("html_url", ""), "reason": reason.args[0]})
    return records, problems


def fold(records):
    """The requirements that the records leave, by selection identity, in
    the order they were first required.

    A repeated require changes nothing, so identical requests are idempotent.
    retry and blocked replace the attempt and forget its result. A result is
    kept only when it names the current attempt's command and commit; the
    last such result stands, which is a re-run of that same workflow run. A
    blocked attempt has no run, so no result replaces it."""
    required = {}
    for record in records:
        current = required.get(record["id"])
        attempt = {"comment": record["comment"], "sha": record["sha"], "run": record.get("run", "")}
        if record["op"] == "require":
            if current is None:
                required[record["id"]] = {"id": record["id"], "names": record["names"],
                                          "selectors": record["selectors"], "by": record["by"],
                                          "request": record["comment"], "attempt": attempt, "result": None}
        elif current is None:
            continue
        elif record["op"] == "drop":
            del required[record["id"]]
        elif record["op"] == "retry":
            current.update(attempt=attempt, result=None)
        elif record["op"] == "blocked":
            current.update(attempt=attempt, result={"outcome": "blocked", "url": record["url"]})
        elif ((record["comment"], record["sha"]) == (current["attempt"]["comment"], current["attempt"]["sha"])
              and (current["result"] or {}).get("outcome") != "blocked"):
            current["result"] = {"outcome": record["outcome"], "url": record["url"], "run": record["run"]}
    return required


def requirement_state(requirement, head):
    """What one requirement says of a head commit. Evidence of another
    commit is no evidence, and neither is a run without a recorded result."""
    if requirement["attempt"]["sha"] != head:
        return "awaiting"
    if requirement["result"] is None:
        return "running"
    return {"success": "passed", "failure": "failed", "blocked": "blocked"}.get(
        requirement["result"]["outcome"], "incomplete")


def evaluate(pull, head, records, problems):
    """The status of one pull request at its head commit."""
    rows = [{**requirement, "state": requirement_state(requirement, head)} for requirement in fold(records).values()]
    count = lambda *states: sum(row["state"] in states for row in rows)
    if problems:
        state, description = "error", str(len(problems)) + " ledger comment(s) of #" + str(pull) + " cannot be trusted"
    elif not rows:
        state, description = "success", "No requested CI is required"
    else:
        state = min((STATES[row["state"]] for row in rows), key=SEVERITY.index)
        description = str(count("passed")) + " of " + str(len(rows)) + " required selection(s) passed on " + head[:12]
        for states, label in ((("failed",), "failed"), (("incomplete",), "did not complete"),
                              (("blocked",), "no longer resolve"), (("running",), "running"),
                              (("awaiting",), "without a run on this head")):
            if count(*states):
                description += "; " + str(count(*states)) + " " + label
    return {"pull": pull, "head": head, "state": state, "description": description, "rows": rows,
            "problems": problems, "records": len(records), "last": records[-1]["url"] if records else ""}


def combine(views):
    """The status of a head commit that the given pull requests share."""
    worst = min(views, key=lambda view: (SEVERITY.index(view["state"]), view["pull"]))
    description = worst["description"]
    if len(views) > 1:
        description = ("#" + str(worst["pull"]) + ": " + description + "; head shared by "
                       + ", ".join("#" + str(view["pull"]) for view in sorted(views, key=lambda view: view["pull"])))
    if len(description) > MAX_DESCRIPTION:
        description = description[:MAX_DESCRIPTION - 3].rstrip() + "..."
    return worst, {"state": worst["state"], "description": description}


def comment_url(server, repository, pull, comment):
    return server + "/" + repository + "/pull/" + str(pull) + "#issuecomment-" + str(comment)


def summary(view, server, repository):
    """The summary comment of a pull request: every required selection with
    its state, the commit its evidence tested and the commands that change it."""
    link = lambda text, url: "[" + text + "](" + url + ")" if url else text
    lines = [SUMMARY, "### " + CONTEXT, "",
             "Head `" + view["head"] + "`: **" + view["state"] + "**. " + view["description"] + ".", ""]
    if view["rows"]:
        lines += ["| Selection | State | Tested commit | Evidence | Required by | Change it with |",
                  "| --- | --- | --- | --- | --- | --- |"]
    for row in view["rows"]:
        selection = ci_command.selection_text(row["names"], row["selectors"])
        attempt, result = row["attempt"], row["result"] or {}
        evidence = [link("command", comment_url(server, repository, view["pull"], attempt["comment"])),
                    link("run", attempt["run"])] + ([link("result", result["url"])] if result.get("url") else [])
        lines.append("| " + " | ".join([
            "`" + selection + "`", LABELS[row["state"]], "`" + attempt["sha"][:12] + "`", ", ".join(evidence),
            "`" + row["by"] + "` in " + link("comment", comment_url(server, repository, view["pull"], row["request"])),
            "`/ci retry " + selection + "` or `/ci drop " + selection + "`"]) + " |")
    if view["rows"]:
        lines += ["", "A selection passes only with a successful result of its latest accepted `require` or "
                  "`retry` command on the head above. `running` with a finished run means that no result was "
                  "recorded; retry it."]
    for problem in view["problems"]:
        lines.append("- " + link("This ledger comment", problem["url"]) + " cannot be trusted: " + problem["reason"]
                     + ". Delete it, then repeat the commands it stood for.")
    lines += ["", "Ledger: " + str(view["records"]) + " record(s)"
              + (", the last in " + link("this comment", view["last"]) if view["last"] else "")
              + ". An advisory `/ci run` is never listed here and never changes this status."]
    return "\n".join(lines)


class GitHub:
    """The REST calls of register and evaluate, through gh."""

    # A read is repeated after these pauses, in seconds: a pull request
    # that cannot be read becomes an error, which one lost request should
    # not cause. A write is never repeated: it may have arrived.
    RETRY = (2, 5)

    def __init__(self, repository, run=subprocess.run, sleep=time.sleep):
        self.repository, self.run, self.sleep = repository, run, sleep

    def api(self, path, method="GET", body=None):
        args = ["gh", "api", "--method", method, "repos/" + self.repository + "/" + path]
        if body is not None:
            args += ["--input", "-"]
        for pause in (*self.RETRY, None) if method == "GET" else (None,):
            try:
                done = self.run(args, input=None if body is None else json.dumps(body).encode(),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                return json.loads(done.stdout)
            except (subprocess.CalledProcessError, ValueError):
                if pause is None:
                    raise
                self.sleep(pause)

    def pages(self, path, key=None):
        rows = []
        for page in range(1, 101):
            result = self.api(path + ("&" if "?" in path else "?") + "per_page=100&page=" + str(page))
            part = result if key is None else result[key]
            rows.extend(part)
            if len(part) < 100:
                return rows
        raise RuntimeError("pagination limit reached: " + path)

    def open_pulls(self):
        return self.pages("pulls?state=open")

    def pull(self, number):
        return self.api("pulls/" + str(number))

    def comments(self, number):
        return self.pages("issues/" + str(number) + "/comments")

    def status(self, sha):
        """The current status of the context on a commit, or None."""
        for status in self.pages("commits/" + sha + "/status", key="statuses"):
            if status["context"] == CONTEXT:
                return {key: status.get(key) or "" for key in ("state", "description", "target_url")}
        return None

    def set_status(self, sha, status):
        self.api("statuses/" + sha, "POST", {**status, "context": CONTEXT})

    def create_comment(self, number, body):
        return self.api("issues/" + str(number) + "/comments", "POST", {"body": body})

    def update_comment(self, comment, body):
        return self.api("issues/comments/" + str(comment), "PATCH", {"body": body})


def publish(github, sha, status):
    """Set the status unless the commit already shows it. GitHub keeps a
    bounded number of statuses per commit and context. A status that cannot
    be read is set."""
    try:
        if github.status(sha) == status:
            return False
    except Exception:
        pass
    github.set_status(sha, status)
    return True


def unreadable(pull, head, error):
    """The view of a pull request whose requirements are unknown. They may
    be unmet, so its head must not stay or become green."""
    return {"pull": pull, "head": head, "state": "error", "unread": str(error), "rows": [], "problems": [],
            "description": "The requirements of #" + str(pull) + " could not be read", "summary": None}


def evaluate_all(github, server, repository, log=print):
    """Publish the status and the summary of every open pull request.

    Nothing that fails here leaves a head as it was when another status can
    be set. A pull request whose comments cannot be read, or cannot be
    evaluated, makes its head an error, for every pull request of that head.
    A head whose status cannot be set is reported, and so is a pull request
    without a readable head. The caller fails the job for any of them.

    The status goes first: a summary is what a reviewer reads as the state of
    the requirements, and it never runs ahead of the status of its head."""
    heads, unread, unpublished, unsummarized = {}, [], [], []
    for pull in sorted(github.open_pulls(), key=lambda pull: pull.get("number") or 0):
        number, head = pull.get("number"), (pull.get("head") or {}).get("sha")
        if not (positive(number) and isinstance(head, str) and SHA.match(head)):
            unpublished.append("pull request " + str(number)[:20])
            log("A pull request without a readable number or head commit: " + str(number)[:20])
            continue
        try:
            comments = github.comments(number)
            view = evaluate(number, head, *ledger(comments, number))
            view["summary"] = next((comment for comment in comments
                                    if (comment.get("user") or {}).get("login") == BOT
                                    and (comment.get("body") or "").startswith(SUMMARY)), None)
        except Exception as error:
            unread.append(number)
            view = unreadable(number, head, error)
            log("#" + str(number) + ": requirements could not be read: " + repr(error))
        heads.setdefault(head, []).append(view)

    def publish_heads():
        for head, views in heads.items():
            worst, status = combine(views)
            status["target_url"] = (worst["summary"] or {}).get("html_url") or (
                server + "/" + repository + "/pull/" + str(worst["pull"]))
            try:
                if publish(github, head, status):
                    log(head + ": " + status["state"] + ": " + status["description"])
            except Exception as error:
                if head not in unpublished:
                    unpublished.append(head)
                log(head + ": the status could not be set: " + repr(error))

    publish_heads()
    created = False
    for head, views in heads.items():
        for view in views:
            if "unread" in view or head in unpublished:
                continue
            try:
                body = summary(view, server, repository)
                if view["summary"] is None and (view["records"] or view["problems"]):
                    view["summary"], created = github.create_comment(view["pull"], body), True
                elif view["summary"] is not None and (
                        view["summary"].get("body") or "").replace("\r\n", "\n").strip() != body:
                    github.update_comment(view["summary"]["id"], body)
            except Exception as error:
                unsummarized.append(view["pull"])
                log("#" + str(view["pull"]) + ": the summary could not be written: " + repr(error))
    if created:
        # The status of a head links to its summary once that exists.
        publish_heads()
    return {"heads": {head: [{key: view[key] for key in ("pull", "state", "description")} for view in views]
                      for head, views in heads.items()},
            "unread": unread, "unpublished": unpublished, "unsummarized": unsummarized}


def register(github, request, resolved, run_url, server, repository, log=print):
    """Apply one accepted require, retry or drop command.

    resolved says whether Validate resolved every name on the request's
    commit. Returns whether the command became the selection's attempt and
    whether its run starts. Every outcome is answered with one comment; the
    comment of a change starts with its ledger record. The caller evaluates
    afterwards."""
    number, verb, key = request["pull"], request["verb"], request["id"]
    selection = "`" + ci_command.selection_text(request["names"], request["selectors"]) + "`"
    command = comment_url(server, repository, number, request["comment_id"])
    head = github.pull(number)["head"]["sha"]
    records, _ = ledger(github.comments(number), number)
    required = fold(records)
    record = {"v": 1, "pull": number, "id": key, "comment": request["comment_id"], "sha": request["sha"],
              "by": request["by"]}
    attempt = run = False
    if verb == "require" and not resolved:
        record, reply = None, "Nothing is required: the names of " + selection + " were not resolved."
    elif verb == "require" and key in required:
        record, reply = None, ("Already required: " + selection + " " + SENTENCES[requirement_state(required[key], head)]
                               + " " + head[:12] + ". Nothing changed and nothing runs; `/ci retry` runs a "
                               "required selection again.")
    elif verb != "require" and key not in required:
        record, reply = None, ("Nothing changed: " + selection + " is not a required selection of this pull request. "
                               + ("Required: " + ", ".join(
                                   "`" + ci_command.selection_text(entry["names"], entry["selectors"]) + "`"
                                   for entry in required.values()) + "." if required else "It requires none."))
    elif verb == "drop":
        record.update(op="drop")
        reply = ("No longer required: " + selection + ", removed by `" + request["by"] + "` in [this command]("
                 + command + "). The `" + CONTEXT + "` status is evaluated next without it.")
    elif not resolved:
        record.update(op="blocked", run=run_url)
        reply = ("Blocked: the names of the required selection " + selection + " no longer resolve on "
                 + request["sha"][:12] + " ([command](" + command + ")). The `" + CONTEXT + "` status fails until "
                 "the pull request restores them or the selection is dropped.")
    else:
        attempt = run = True
        record.update(op=verb, run=run_url)
        if verb == "require":
            record.update(names=request["names"], selectors=request["selectors"])
        reply = (("Required: " if verb == "require" else "Retrying the required selection: ") + selection
                 + ", by `" + request["by"] + "` in [this command](" + command + "). [This run](" + run_url
                 + ") tests " + request["sha"] + ". The `" + CONTEXT + "` status of the head was set pending before "
                 "this acknowledgment and passes only when the run succeeds on the head of the pull request.")
        if request["sha"] != head:
            reply += (" The head is now " + head[:12] + ", which this run does not test; retry the selection "
                      "on it.")
    if record is not None and record["op"] != "drop":
        # A green status must not outlive the acknowledgment below.
        github.set_status(head, {"state": "pending", "description": "Recording a change of the required selections",
                                 "target_url": run_url})
    github.create_comment(number, reply if record is None else ci_command.ledger_line(record) + "\n" + reply)
    log(reply)
    return {"attempt": attempt, "run": run}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("register")
    command.add_argument("--request", required=True, help="The request output of ci_command.py authorize, as JSON")
    command.add_argument("--resolved", required=True, choices=("true", "false"),
                         help="Whether ci_command.py select accepted the names on the request's commit")
    command.add_argument("--run-url", required=True)
    command.add_argument("--github-output", type=Path)
    command = commands.add_parser("evaluate")
    command.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    repository = os.environ["GITHUB_REPOSITORY"]
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    github = GitHub(repository)
    if args.command == "register":
        request = json.loads(args.request)
        if request.get("verb") not in ci_command.REQUIREMENT_VERBS:
            parser.error("not a requirement command")
        decision = register(github, request, args.resolved == "true", args.run_url, server, repository)
        # The record stands whatever the evaluation below does, so the run
        # it announced starts.
        if args.github_output is not None:
            ci_command.write_outputs(args.github_output, {key: str(value).lower() for key, value in decision.items()})
    decision = evaluate_all(github, server, repository)
    print(json.dumps(decision, indent=2, sort_keys=True))
    # Every head has the status this evaluation computed, an error where a
    # pull request could not be read. Without this output the workflow
    # withdraws the status of the commit that started the job.
    if args.github_output is not None and not decision["unpublished"]:
        ci_command.write_outputs(args.github_output, {"settled": "true"})
    return 1 if decision["unread"] or decision["unpublished"] or decision["unsummarized"] else 0


if __name__ == "__main__":
    sys.exit(main())
