#!/usr/bin/env python3
"""Parse, authorize and validate a pull request comment command (#116).

A comment line

    /ci run <batch|profile>... [on <release|image set>...]

runs the named runtime batches on the pull request's head commit through
tagged-runtime.yml. ci-command.yml calls three subcommands, each in its own
job:

- authorize reads the issue_comment event and the pull request, and decides
  whether the comment is a command at all, and whether it is accepted: from
  an OWNER, MEMBER or COLLABORATOR, on an open pull request whose head is a
  branch of this repository, with a well-formed command. It runs on the
  default branch and executes no code of the pull request.
- select resolves every name against the planner vocabulary of the head
  commit (tag_scenarios.py vocabulary). One unknown name refuses the whole
  command, so a typo never runs a partial selection.
- report writes the reply and the commit status of the finished run.

Each command has its own commit status context, named after the comment that
started it, so a repeated command never shows an earlier command's result.
Repeated commands still share one concurrency key, so they queue instead of
cancelling each other.

Every subcommand writes single-line JSON values to GITHUB_OUTPUT, so a reply
never reaches a workflow expression as text.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

AUTHORIZED = ("OWNER", "MEMBER", "COLLABORATOR")
# A command is a line that starts with "/ci" followed by a blank or the end
# of the line. Text before it on the same line, a quote ("> /ci") or a fenced
# code block makes the line an ordinary comment.
COMMAND_LINE = re.compile(r"/ci(?:[ \t]+(.*))?\Z")
FENCE = re.compile(r"[ ]{0,3}(```|~~~)")
# Batch names, profile names and IDs, releases and codenames; anything else
# is refused before it reaches a workflow.
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./-]{0,127}\Z")
ALL_RELEASES = "all"
USAGE = "`/ci run <batch|profile>... [on <release|codename|all>]`"
CONTEXT_PREFIX = "CI command"
MAX_CONTEXT = 100
STATES = {"success": "success", "failure": "failure", "cancelled": "error", "skipped": "error"}


class Refusal(Exception):
    """The comment is a command that is not run; the message is the reply."""


def command_lines(body):
    """The argument text of every command line of a comment body."""
    commands, fence = [], None
    for line in (body or "").splitlines():
        line = line.rstrip()
        marker = FENCE.match(line)
        if marker:
            fence = None if fence == marker[1] else fence or marker[1]
            continue
        if fence:
            continue
        match = COMMAND_LINE.match(line)
        if match:
            commands.append(match[1] or "")
    return commands


def parse(arguments):
    """Names and release selectors of one command's argument text.

    Names and selectors are separated by blanks or commas. A name or
    selector given twice is kept once and reported."""
    tokens = [token for token in re.split(r"[\s,]+", arguments.strip()) if token]
    if not tokens or tokens[0] != "run":
        raise Refusal("Unknown command. Usage: " + USAGE + ".")
    tokens = tokens[1:]
    if tokens.count("on") > 1:
        raise Refusal("`on` may appear only once. Usage: " + USAGE + ".")
    names, selectors = (tokens[:tokens.index("on")], tokens[tokens.index("on") + 1:]) if "on" in tokens else (tokens, [])
    if not names:
        raise Refusal("The command names no batch or profile. Usage: " + USAGE + ".")
    if "on" in tokens and not selectors:
        raise Refusal("`on` needs a release, a codename or `all`. Usage: " + USAGE + ".")
    invalid = [token for token in names + selectors if not NAME.fullmatch(token)]
    if invalid:
        # Quoted without backticks, so a token cannot leave the code span.
        raise Refusal("Invalid name(s): " + ", ".join("`" + token[:40].replace("`", "'") + "`" for token in invalid)
                      + ". Names use letters, digits, `_`, `.`, `/` and `-`.")
    unique = lambda values: list(dict.fromkeys(values))
    duplicates = sorted({token for token in names + selectors if (names + selectors).count(token) > 1})
    return {"names": unique(names), "selectors": unique(selectors), "duplicates": duplicates}


def authorize(event, pull):
    """Decide what to do with an issue_comment event.

    Returns command (whether the comment is a command at all), accepted, and
    either the request or the refusal reply. Comments that are not commands,
    edits, comments on issues and comments by bots are ignored silently."""
    ignored = {"command": False, "accepted": False}
    issue, comment = event.get("issue") or {}, event.get("comment") or {}
    if event.get("action") != "created" or not issue.get("pull_request"):
        return ignored
    if (comment.get("user") or {}).get("type") == "Bot":
        return ignored
    commands = command_lines(comment.get("body"))
    if not commands:
        return ignored
    decision = {"command": True, "accepted": False, "pull": issue.get("number"),
                "comment": comment.get("html_url", "")}
    try:
        association = comment.get("author_association", "")
        if association not in AUTHORIZED:
            raise Refusal("Only an owner, member or collaborator of this repository can run CI commands "
                          "(this comment's author association is " + (association or "unknown") + ").")
        if len(commands) > 1:
            raise Refusal("A comment may hold only one command; this one holds " + str(len(commands)) + ".")
        request = parse(commands[0])
        if not pull or pull.get("number") != issue.get("number"):
            raise Refusal("The pull request could not be read.")
        head_repo = ((pull.get("head") or {}).get("repo") or {}).get("full_name")
        base_repo = ((pull.get("base") or {}).get("repo") or {}).get("full_name")
        if not head_repo or head_repo != base_repo:
            raise Refusal("CI commands do not run pull requests from forks: the head repository is "
                          + (head_repo or "deleted") + ", not " + str(base_repo) + ".")
        if pull.get("state") != "open":
            raise Refusal("CI commands run only on open pull requests.")
        comment_id = comment.get("id")
        if type(comment_id) is not int or comment_id <= 0:
            raise Refusal("The comment could not be read.")
        sha = (pull.get("head") or {}).get("sha", "")
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise Refusal("The pull request's head commit could not be read.")
    except Refusal as refusal:
        decision["reply"] = refusal.args[0]
        return decision
    decision.update(accepted=True, sha=sha, request={**request, "sha": sha, "pull": issue["number"],
                                                        "comment_id": comment_id})
    return decision


def select(request, vocabulary):
    """Resolve the request's names and selectors against the planner
    vocabulary of the head commit.

    A name is a batch, category/batch, a profile name or a profile ID. Code
    batches run in Code checks, not here. Any unresolved name refuses the
    whole command."""
    profiles = vocabulary["profiles"]
    unknown, selected = [], {}
    for name in request["names"]:
        matches = [profile for profile in profiles
                   if name in (profile["id"], profile["profile"], profile["batch"],
                               profile["category"] + "/" + profile["batch"])]
        runnable = [profile for profile in matches if profile["category"] != "code"]
        if not runnable:
            unknown.append("`" + name + "`" + (" (a code batch; it runs in Code checks)" if matches else ""))
        for profile in runnable:
            selected[profile["id"]] = profile
    releases, codenames = [], {entry["codename"]: entry["release"] for entry in vocabulary["releases"]}
    supported = [entry["release"] for entry in vocabulary["releases"]]
    for selector in request["selectors"]:
        if selector == ALL_RELEASES:
            releases += supported
        elif selector in supported:
            releases.append(selector)
        elif selector in codenames:
            releases.append(codenames[selector])
        else:
            unknown.append("`" + selector + "` (supported: " + ", ".join(
                release + " " + codename for codename, release in codenames.items()) + ", all)")
    if unknown:
        return {"accepted": False, "reply": "Nothing runs. Unknown name(s) at " + request["sha"][:12] + ": "
                + ", ".join(unknown) + ". Names are batches, `category/batch`, profile names or profile IDs "
                "of `tag_scenarios.py vocabulary`."}
    releases = sorted(set(releases or [vocabulary["default_release"]]), key=supported.index)
    ids = sorted(selected)
    digest = hashlib.sha256(json.dumps([ids, releases]).encode()).hexdigest()[:12]
    context = status_context(request["comment_id"], request["names"], releases, digest)
    return {"accepted": True, "profiles": ",".join(ids), "releases": releases,
            "batches": sorted(profile["category"] + "/" + profile["batch"] for profile in selected.values()),
            "key": str(request["pull"]) + "-" + request["sha"] + "-" + digest, "context": context}


def status_context(comment, names, releases, digest):
    """The commit status context of one command.

    The comment ID comes first, so every command has its own context and
    truncation never removes it. A selection too long for GitHub's context
    limit is cut and ends with the selection digest instead."""
    context = CONTEXT_PREFIX + " " + str(comment) + ": " + " ".join(names) + " on " + " ".join(releases)
    if len(context) > MAX_CONTEXT:
        context = context[:MAX_CONTEXT - len(digest) - 4].rstrip() + "... " + digest
    return context


def report(command, validate, validate_result, runtime_result, run_url):
    """The reply and the commit status of a finished command run.

    command and validate are the outputs of those jobs; a status is set only
    for a request that passed validation."""
    run = " ([run](" + run_url + "))"
    if command.get("accepted") != "true":
        return {"reply": json.loads(command.get("reply") or '""') + run, "state": ""}
    if validate_result != "success":
        return {"reply": "Nothing runs: the names could not be validated (" + (validate_result or "not run")
                + ")." + run, "state": ""}
    if validate.get("accepted") != "true":
        return {"reply": json.loads(validate.get("reply") or '""') + run, "state": ""}
    request, sha = json.loads(command["request"]), command["sha"]
    batches, releases = json.loads(validate["batches"]), json.loads(validate["releases"])
    state = STATES.get(runtime_result, "error")
    outcome = {"success": "passed", "failure": "failed", "cancelled": "was cancelled"}.get(
        runtime_result, "ended as " + (runtime_result or "unknown"))
    lines = ["CI command " + outcome + " on " + sha + run + ".", "",
             "- Releases: " + ", ".join(releases),
             "- Batches (" + str(len(batches)) + "): " + ", ".join("`" + batch + "`" for batch in batches)]
    if request.get("duplicates"):
        lines.append("- Given more than once, run once: " + ", ".join("`" + name + "`" for name in request["duplicates"]))
    if runtime_result == "cancelled":
        lines.append("- A cancelled run is not re-run automatically.")
    return {"reply": "\n".join(lines), "state": state,
            "description": ("CI command " + outcome + ": " + str(len(batches)) + " batch(es)")[:140]}


def write_outputs(path, values):
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(key + "=" + (value if isinstance(value, str) else json.dumps(value, separators=(",", ":")))
                         + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("authorize")
    command.add_argument("--event", required=True, type=Path)
    command.add_argument("--pull", required=True, type=Path)
    command.add_argument("--github-output", type=Path)
    command = commands.add_parser("select")
    command.add_argument("--request", required=True, help="The request output of authorize, as JSON")
    command.add_argument("--vocabulary", required=True, type=Path)
    command.add_argument("--github-output", type=Path)
    command = commands.add_parser("report")
    command.add_argument("--command-outputs", required=True, help="toJSON(needs.command.outputs)")
    command.add_argument("--validate-outputs", default="{}", help="toJSON(needs.validate.outputs)")
    command.add_argument("--validate-result", default="")
    command.add_argument("--runtime-result", default="")
    command.add_argument("--run-url", required=True)
    command.add_argument("--body", required=True, type=Path)
    command.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    if args.command == "authorize":
        event = json.loads(args.event.read_text())
        pull = json.loads(args.pull.read_text()) if args.pull.exists() and args.pull.stat().st_size else None
        decision = authorize(event, pull)
        outputs = {"command": str(decision["command"]).lower(), "accepted": str(decision["accepted"]).lower(),
                   "reply": json.dumps(decision.get("reply", "")), "sha": decision.get("sha", ""),
                   "request": decision.get("request", {})}
    elif args.command == "select":
        if args.vocabulary.exists():
            decision = select(json.loads(args.request), json.loads(args.vocabulary.read_text()))
        else:
            decision = {"accepted": False, "reply": "Nothing runs: the planner of the head commit could not list "
                        "its batches. Rebase the pull request onto `main` if it predates CI commands."}
        outputs = {"accepted": str(decision["accepted"]).lower(), "reply": json.dumps(decision.get("reply", "")),
                   **{key: decision.get(key, "" if key in ("profiles", "key", "context") else [])
                      for key in ("profiles", "releases", "batches", "key", "context")}}
    else:
        decision = report(json.loads(args.command_outputs), json.loads(args.validate_outputs),
                          args.validate_result, args.runtime_result, args.run_url)
        args.body.write_text(decision["reply"] + "\n")
        outputs = {"state": decision["state"], "description": decision.get("description", "")}
    print(json.dumps(decision, indent=2, sort_keys=True))
    if args.github_output is not None:
        write_outputs(args.github_output, outputs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
