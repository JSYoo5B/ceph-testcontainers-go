#!/usr/bin/env python3
"""Join an expanded dispatch inventory to the jobs of one workflow run attempt.

The expected side is the `ceph-tag-dispatch/v1` document that
tag_scenarios.py persists next to the base plan (artifact
`plan-<category>-<release>`). The actual side is a job list of the same
repository, run and attempt, such as the GitHub jobs API response saved to a
file. Optional history-row receipts (scenario_history.py) add the identity of
what a job actually checked out.

The script is offline and pure. It reads files and writes one JSON report; it
never calls GitHub and starts no process.

A dispatch entry proves neither runner allocation nor native execution. An
entry that matches a job says only that a job of that name exists in the run
attempt. Every expected entry and every job in the list ends in exactly one
bucket, and the report checks that itself:

  entries: matched, conflicts, ambiguous, unmatched_expected, invalid_expected
  jobs:    matched, conflicts, ambiguous, unmatched_actual, invalid_actual,
           out_of_scope

A job matches an entry by name. Entries and jobs that share a name can only
be paired when exactly one of each exists; any other non-zero combination is
ambiguous and keeps all of its members. Missing, null or wrong-typed fields
make an entry, job or receipt invalid or inconclusive, never matched.

`context.sha` of the dispatch document is the declared workflow SHA. It is
reported as such and is never used as the checkout identity; that identity
comes from receipts only and is null when no receipt states it.

Not covered: runner cost, allocation, billing, native outcomes, empirical
cohorts, and any scheduling or selection policy.

Exit status: 0 for a conserved report (mismatches are findings in the report,
not failures), 2 when the inputs are inconclusive as a whole, 3 when the
conservation check of the report itself fails.
"""

import argparse
import json
from pathlib import Path
import re
import sys

import scenario_history

REPORT_SCHEMA = "ceph-dispatch-join/v1"
# Kept in step with tag_scenarios.DISPATCH_SCHEMA / DISPATCH_FIELDS by a test.
DISPATCH_SCHEMA = "ceph-tag-dispatch/v1"
ENTRY_STRINGS = ("id", "name", "category", "batch", "package", "network", "tests", "repetition", "artifact_suffix")
ENTRY_INTEGERS = ("job_timeout",)
# Strings that may legitimately be empty (default network, whole-batch tests,
# first repetition's artifact suffix).
MAY_BE_EMPTY = ("network", "tests", "artifact_suffix")
REQUIRED_CONTEXT = ("repository", "run_id", "run_attempt")
JOB_REPOSITORY = re.compile(r"/repos/([^/]+/[^/]+)/actions/jobs/")
NOT_ESTABLISHED = (
    "A dispatch entry proves neither runner allocation nor native execution.",
    "A name match does not prove that the job ran the entry's tests or used its images.",
    "No runner cost, billing, native outcome, empirical cohort or scheduling policy is computed.",
    "The declared workflow SHA is not the checkout identity; only receipts state a checkout.",
)


class Inconclusive(Exception):
    """The inputs as a whole cannot be joined."""


def is_text(value, empty=False):
    return isinstance(value, str) and (empty or value != "")


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def identifier(value):
    """A run, attempt or job ID as text, or None for a missing or wrong type."""
    if is_integer(value):
        return str(value)
    return value if is_text(value) and value.isdigit() else None


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Inconclusive(Path(path).name + ": not readable JSON (" + type(error).__name__ + ")") from None


def read_dispatch(document):
    """The validated envelope of a dispatch document and its raw entries."""
    if not isinstance(document, dict):
        raise Inconclusive("dispatch document is not an object")
    if document.get("schema") != DISPATCH_SCHEMA:
        raise Inconclusive("dispatch schema is not " + DISPATCH_SCHEMA)
    entries, context = document.get("entries"), document.get("context")
    if not isinstance(entries, list):
        raise Inconclusive("dispatch entries is not a list")
    if not is_integer(document.get("count")) or document["count"] != len(entries):
        raise Inconclusive("dispatch count is missing or differs from its entries")
    if not isinstance(context, dict):
        raise Inconclusive("dispatch context is not an object")
    missing = [name for name in REQUIRED_CONTEXT if not is_text(context.get(name))]
    if missing:
        raise Inconclusive("dispatch context lacks " + ", ".join(missing) + "; the run attempt is unknown")
    if identifier(context["run_id"]) is None or identifier(context["run_attempt"]) is None:
        raise Inconclusive("dispatch run_id or run_attempt is not numeric")
    return context, entries


def read_jobs(documents):
    """The jobs of one or more API pages. A total_count that disagrees with
    the jobs read means a truncated list, which cannot be joined."""
    jobs, totals = [], set()
    for document in documents:
        if isinstance(document, list):
            jobs.extend(document)
            continue
        if not isinstance(document, dict) or not isinstance(document.get("jobs"), list):
            raise Inconclusive("job list is neither a list nor an object with jobs")
        jobs.extend(document["jobs"])
        if "total_count" in document:
            if not is_integer(document["total_count"]):
                raise Inconclusive("job list total_count is not an integer")
            totals.add(document["total_count"])
    if len(totals) > 1:
        raise Inconclusive("job list pages disagree on total_count")
    if totals and totals != {len(jobs)}:
        raise Inconclusive("job list holds " + str(len(jobs)) + " jobs but total_count is " + str(next(iter(totals))))
    return jobs


def leaf(name):
    """The job's own name: a called workflow's jobs are "<caller> / <name>"."""
    caller, separator, tail = name.rpartition(" / ")
    return (caller if separator else None), tail


def entry_problems(entry):
    if not isinstance(entry, dict):
        return ["entry is not an object"]
    problems = []
    for field in ENTRY_STRINGS:
        if field not in entry or entry[field] is None:
            problems.append(field + " is missing or null")
        elif not is_text(entry[field], field in MAY_BE_EMPTY):
            problems.append(field + " is empty or not text")
    for field in ENTRY_INTEGERS:
        if field not in entry or entry[field] is None:
            problems.append(field + " is missing or null")
        elif not is_integer(entry[field]):
            problems.append(field + " is not an integer")
    return problems


def job_problems(job):
    if not isinstance(job, dict):
        return ["job is not an object"]
    problems = []
    if identifier(job.get("id")) is None:
        problems.append("id is missing or not an identifier")
    if not is_text(job.get("name")):
        problems.append("name is missing or not text")
    if identifier(job.get("run_id")) is None:
        problems.append("run_id is missing or not an identifier")
    if identifier(job.get("run_attempt")) is None:
        problems.append("run_attempt is missing or not an identifier")
    return problems


def entry_view(position, entry):
    return {"position": position, **{field: entry.get(field) for field in ("id", "name", "network", "repetition")}}


def job_view(position, job):
    view = {"position": position}
    for field in ("id", "name", "status", "conclusion"):
        view[field] = job.get(field) if isinstance(job, dict) else None
    return view


def load_receipts(directory):
    """History-row receipts under a directory as (rows, invalid), by relative path."""
    root, rows, invalid = Path(directory), [], []
    for path in sorted(root.rglob("history-row.json")):
        name = str(path.relative_to(root))
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
            scenario_history.validate_row(row)
        except (OSError, ValueError) as error:
            invalid.append({"path": name, "reason": type(error).__name__ + ": " + str(error)[:120]})
            continue
        rows.append((name, row))
    return rows, invalid


def receipt_key(row):
    key = row["execution_key"]
    parts = [key.get("repository"), identifier(key.get("run_id")), identifier(key.get("run_attempt")),
             identifier(key.get("job_id"))]
    return tuple(parts) if all(is_text(part) and part != scenario_history.MISSING for part in parts) else None


def known(value):
    return value if is_text(value) and value != scenario_history.MISSING else None


def read_receipts(candidates, entry):
    """What the receipts of one matched job say, as (state, details, reasons).

    state: not_provided | inconclusive | consistent | conflict. A concrete
    value that differs from the entry is a conflict; an absent, missing or
    wrong-typed one is inconclusive and never counts as agreement."""
    if candidates is None:
        return "not_provided", {"checkout_commit": None, "checkout_tree": None, "paths": []}, []
    details = {"checkout_commit": None, "checkout_tree": None, "paths": [path for path, _ in candidates]}
    if not candidates:
        return "inconclusive", details, ["no receipt names this job"]
    conflicts, gaps, commits, trees = [], [], set(), set()
    for path, row in candidates:
        key = row["execution_key"]
        for field, expected in (("profile_id", entry["id"]), ("repetition_id", entry["repetition"])):
            value = key.get(field)
            if not is_text(value) or value == scenario_history.MISSING:
                gaps.append(path + ": " + field + " is missing or not text")
            elif value != expected:
                conflicts.append(path + ": " + field + " differs from the dispatch entry")
        network = key.get("network")
        if not isinstance(network, str) or network == scenario_history.MISSING:
            # MISSING is the history format's explicit unknown marker: a gap,
            # neither agreement with an empty network nor a contradiction.
            gaps.append(path + ": network is missing or not text")
        elif entry["network"] and network != entry["network"]:
            # An empty entry network lets a batch run its default mode, which
            # the receipt then names; only a requested mode must agree.
            conflicts.append(path + ": network differs from the dispatch entry")
        source = row.get("source") if isinstance(row.get("source"), dict) else {}
        commit, tree = known(source.get("checkout_commit")), known(source.get("checkout_tree"))
        if commit is None:
            gaps.append(path + ": checkout_commit is missing")
        else:
            commits.add(commit)
        if tree is not None:
            trees.add(tree)
    if len(commits) > 1 or len(trees) > 1:
        conflicts.append("receipts disagree on the checkout identity")
    details["checkout_commit"] = next(iter(commits)) if len(commits) == 1 else None
    details["checkout_tree"] = next(iter(trees)) if len(trees) == 1 else None
    if conflicts:
        return "conflict", details, conflicts
    return ("inconclusive", details, gaps) if gaps else ("consistent", details, [])


def join(dispatch, job_documents, receipts=None, caller=None):
    context, entries = read_dispatch(dispatch)
    jobs = read_jobs(job_documents)
    run, attempt, repository = identifier(context["run_id"]), identifier(context["run_attempt"]), context["repository"]

    report = {"schema": REPORT_SCHEMA,
              "scope": {"repository": repository, "run_id": run, "run_attempt": attempt,
                        "category": context.get("category"), "release": context.get("release"),
                        "declared_workflow_sha": context.get("sha"), "caller": None,
                        "checkout_identity": "from receipts only; null when no receipt states it"},
              "matched": [], "conflicts": [], "ambiguous": [], "unmatched_expected": [], "invalid_expected": [],
              "unmatched_actual": [], "invalid_actual": [], "out_of_scope": [],
              "receipts_without_a_matched_job": [], "receipts_other_run": [], "invalid_receipts": [], "not_established": list(NOT_ESTABLISHED)}

    # Jobs first: only a well-formed job of this run attempt can be a candidate.
    seen = {}
    for position, job in enumerate(jobs):
        if not isinstance(job, dict) or identifier(job.get("id")) is None:
            continue
        seen.setdefault(identifier(job["id"]), []).append(position)
    candidates = []
    for position, job in enumerate(jobs):
        problems = job_problems(job)
        if problems:
            report["invalid_actual"].append({**job_view(position, job), "reasons": problems})
            continue
        found = JOB_REPOSITORY.search(job["url"]) if is_text(job.get("url")) else None
        reasons = []
        if len(seen[identifier(job["id"])]) > 1:
            reasons.append("job ID appears " + str(len(seen[identifier(job["id"])])) + " times in the list")
        if identifier(job["run_id"]) != run or identifier(job["run_attempt"]) != attempt:
            reasons.append("job belongs to run " + identifier(job["run_id"]) + " attempt "
                           + identifier(job["run_attempt"]) + ", not the dispatch's run attempt")
        if found and found.group(1) != repository:
            reasons.append("job URL names another repository")
        if reasons:
            report["conflicts"].append({"kind": "job", "job": job_view(position, job), "reasons": reasons})
        else:
            candidates.append((position, job))

    # The caller is the called workflow's job prefix. It is given, or derived
    # when the expected names are found under exactly one caller.
    expected_names = {entry["name"] for entry in entries if isinstance(entry, dict) and is_text(entry.get("name"))}
    if caller is None:
        callers = {leaf(job["name"])[0] for _, job in candidates if leaf(job["name"])[1] in expected_names}
        if len(callers) > 1:
            raise Inconclusive("the expected job names appear under several callers; pass --caller")
        caller = next(iter(callers)) if callers else None
    report["scope"]["caller"] = caller
    scoped = []
    for position, job in candidates:
        if caller is not None and leaf(job["name"])[0] != caller:
            report["out_of_scope"].append({**job_view(position, job), "reason": "job is not under the caller"})
        else:
            scoped.append((position, job))

    # Entries: well-formed, and unique by (id, repetition).
    identities = {}
    for position, entry in enumerate(entries):
        if not entry_problems(entry):
            identities.setdefault((entry["id"], entry["repetition"]), []).append(position)
    valid = []
    for position, entry in enumerate(entries):
        problems = entry_problems(entry)
        if not problems and len(identities[(entry["id"], entry["repetition"])]) > 1:
            problems = ["entry identity (id, repetition) is not unique"]
        if problems:
            report["invalid_expected"].append({**entry_view(position, entry if isinstance(entry, dict) else {}),
                                               "reasons": problems})
        else:
            valid.append((position, entry))

    by_name_entries, by_name_jobs = {}, {}
    for position, entry in valid:
        by_name_entries.setdefault(entry["name"], []).append((position, entry))
    for position, job in scoped:
        by_name_jobs.setdefault(leaf(job["name"])[1], []).append((position, job))

    by_job, receipt_paths = None, None
    if receipts is not None:
        by_job, receipt_paths = {}, [path for path, _ in receipts[0]] + [item["path"] for item in receipts[1]]
        report["invalid_receipts"] = list(receipts[1])
        for path, row in receipts[0]:
            key = receipt_key(row)
            if key is None:
                # The job association is not guessed: the receipt stays visible.
                report["invalid_receipts"].append({"path": path, "reason": "execution key lacks a usable "
                                                   "repository, run, attempt or job ID; no job association"})
            elif key[:3] != (repository, run, attempt):
                report["receipts_other_run"].append({"path": path, "job_id": key[3]})
            else:
                by_job.setdefault(key, []).append((path, row))
    matched_keys = set()
    for name in sorted(set(by_name_entries) | set(by_name_jobs)):
        wanted, actual = by_name_entries.get(name, []), by_name_jobs.get(name, [])
        if wanted and not actual:
            report["unmatched_expected"] += [{**entry_view(p, e), "reason": "no job of this name"} for p, e in wanted]
        elif actual and not wanted:
            report["unmatched_actual"] += [{**job_view(p, j), "reason": "no dispatch entry of this name"}
                                           for p, j in actual]
        elif len(wanted) == 1 and len(actual) == 1:
            (entry_position, entry), (job_position, job) = wanted[0], actual[0]
            key = (repository, run, attempt, identifier(job["id"]))
            state, details, reasons = read_receipts(None if by_job is None else by_job.get(key, []), entry)
            record = {"entry": entry_view(entry_position, entry), "job": job_view(job_position, job),
                      "receipt": {"state": state, "reasons": reasons, **details}}
            if state == "conflict":
                report["conflicts"].append({"kind": "receipt", **record, "reasons": reasons})
            else:
                report["matched"].append(record)
            matched_keys.add(key)
        else:
            report["ambiguous"].append({"name": name, "entries": [entry_view(p, e) for p, e in wanted],
                                        "jobs": [job_view(p, j) for p, j in actual],
                                        "reason": "names are shared by " + str(len(wanted)) + " entries and "
                                                  + str(len(actual)) + " jobs; no one-to-one pairing is provable"})
    if by_job is not None:
        for key, items in sorted(by_job.items()):
            if key not in matched_keys:
                report["receipts_without_a_matched_job"].append({"job_id": key[3], "paths": [p for p, _ in items]})

    report["conservation"] = conserve(report, len(entries), len(jobs), receipt_paths)
    return report


def conserve(report, entry_total, job_total, receipt_paths=None):
    """Count every input once through the report's own buckets. Every receipt
    read (receipt_paths, None when none were supplied) ends in exactly one of:
    attached to a matched or receipt-conflicted job, without a matched job,
    of another run attempt, or invalid."""
    entries, jobs = [], []
    for record in report["matched"]:
        entries.append(record["entry"]["position"])
        jobs.append(record["job"]["position"])
    for record in report["conflicts"]:
        if record["kind"] == "receipt":
            entries.append(record["entry"]["position"])
        jobs.append(record["job"]["position"])
    for record in report["ambiguous"]:
        entries += [item["position"] for item in record["entries"]]
        jobs += [item["position"] for item in record["jobs"]]
    entries += [item["position"] for key in ("unmatched_expected", "invalid_expected") for item in report[key]]
    jobs += [item["position"] for key in ("unmatched_actual", "invalid_actual", "out_of_scope") for item in report[key]]
    result = {"expected_entries": entry_total, "bucketed_entries": len(entries),
              "actual_jobs": job_total, "bucketed_jobs": len(jobs),
              "ok": sorted(entries) == list(range(entry_total)) and sorted(jobs) == list(range(job_total))}
    if receipt_paths is not None:
        placed = [path for record in report["matched"] for path in record["receipt"]["paths"]]
        placed += [path for record in report["conflicts"] if record["kind"] == "receipt"
                   for path in record["receipt"]["paths"]]
        placed += [path for record in report["receipts_without_a_matched_job"] for path in record["paths"]]
        placed += [record["path"] for key in ("receipts_other_run", "invalid_receipts") for record in report[key]]
        result.update(receipts_read=len(receipt_paths), bucketed_receipts=len(placed))
        result["ok"] = result["ok"] and sorted(placed) == sorted(receipt_paths)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dispatch", required=True, help="tag-dispatch.json of the plan artifact")
    parser.add_argument("--jobs", required=True, action="append",
                        help="saved jobs list of the same run attempt; repeat for API pages")
    parser.add_argument("--receipts", help="directory searched for history-row.json receipts")
    parser.add_argument("--caller", help="job-name prefix of the calling job, such as a release caller")
    parser.add_argument("--output", help="write the report here instead of standard output")
    args = parser.parse_args(argv)
    try:
        receipts = load_receipts(args.receipts) if args.receipts else None
        report = join(load_json(args.dispatch), [load_json(path) for path in args.jobs], receipts, args.caller)
    except Inconclusive as error:
        print("inconclusive: " + str(error), file=sys.stderr)
        return 2
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0 if report["conservation"]["ok"] else 3


if __name__ == "__main__":
    sys.exit(main())
