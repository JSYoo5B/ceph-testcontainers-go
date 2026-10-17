#!/usr/bin/env python3
"""Normalize one runtime job's saved receipts into a versioned history row.

normalize reads the receipts that a tagged runtime job already wrote
(module/compile/native reports, phases.json, the Docker cleanup reports and
the role image summary) and writes one JSON row. It never talks to Docker or
GitHub, never replays tests and never guesses a cause: a phase without its
receipt is recorded as missing, a skipped phase as skipped, and only a phase
whose workflow step succeeded and whose receipt passed counts as passed.

ingest appends rows to a JSONL history file. Re-ingesting an identical row
is a no-op; a different row under the same execution key is an error that
reports both row hashes and leaves the history unchanged.

summary counts rows per profile and release from a JSONL history file.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile

import tag_scenarios


SCHEMA = "ceph-scenario-history/v1"
MISSING = "missing"
# The workflow phases of report_workflow_phases.py, in execution order, and
# the receipt that is each phase's own evidence.
PHASES = ("modules", "compile", "docker", "images", "native", "cleanup")
WORKFLOW_OUTCOMES = {"success": "success", "failure": "failure", "cancelled": "cancelled",
                     "skipped": "skipped", "": "not_run"}
# The execution key fields that ingest needs to tell two attempts apart.
INGEST_KEY_FIELDS = ("repository", "run_id", "run_attempt", "profile_id")
ERROR_LIMIT = 500


class Evidence:
    """Reads receipts under named roots and records each file's sha256."""

    def __init__(self, roots):
        self.roots = {name: Path(path) if path else None for name, path in roots.items()}
        self.files = {}
        self.problems = {}

    def text(self, root, relative):
        base = self.roots.get(root)
        label = root + "/" + relative
        if base is None:
            return None
        path = base / relative
        try:
            content = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as error:
            self.problems[label] = "unreadable: " + type(error).__name__
            return None
        self.files[label] = hashlib.sha256(content).hexdigest()
        return content.decode("utf-8", errors="replace")

    def json(self, root, relative):
        """The receipt's object, None when absent, or "invalid"."""
        text = self.text(root, relative)
        if text is None:
            return None
        try:
            value = json.loads(text)
        except ValueError:
            value = None
        if not isinstance(value, dict):
            self.problems[root + "/" + relative] = "not a JSON object"
            return "invalid"
        return value


def bounded(text):
    text = str(text)
    return text if len(text) <= ERROR_LIMIT else text[:ERROR_LIMIT] + "..."


def first_error(report):
    if isinstance(report.get("errors"), list) and report["errors"]:
        return bounded(report["errors"][0])
    if isinstance(report.get("error"), str):
        return bounded(report["error"])
    completion = report.get("completion")
    if isinstance(completion, dict) and completion.get("errors"):
        return bounded(completion["errors"][0])
    return None


def receipt_evidence(report):
    if report is None:
        return MISSING
    if report == "invalid":
        return "invalid"
    return "passed" if report.get("passed") is True else "failed"


def combine(workflow, evidence, required=True):
    """One phase's status from its workflow outcome and its own receipt.

    A failure on either side wins; a cancelled or skipped step stays so; a
    pass needs both the step's success and a passing (or inapplicable)
    receipt."""
    if workflow == "failure" or evidence == "failed":
        return "failed"
    if workflow == "cancelled":
        return "cancelled"
    if not required:
        return "not_required"
    if workflow in ("skipped", "not_run"):
        return "skipped"
    if workflow == MISSING:
        return MISSING
    if evidence in (MISSING, "invalid"):
        return evidence
    return "passed"


def parse_tests(log):
    """RUN/PASS/FAIL/SKIP rows of a verbose native log, in start order.

    A test that started without a result line keeps result RUN."""
    rows, open_rows = [], {}
    for line in log.splitlines():
        run = tag_scenarios.RUN_LINE.fullmatch(line)
        if run:
            open_rows[run[1]] = len(rows)
            rows.append({"name": run[1], "result": "RUN", "seconds": None})
            continue
        result = tag_scenarios.RESULT_LINE.fullmatch(line)
        if result:
            index = open_rows.pop(result[2], None)
            if index is None:
                index = len(rows)
                rows.append({"name": result[2]})
            rows[index].update(result=result[1], seconds=float(result[3]))
    for row in rows:
        row["parent"] = row["name"].split("/", 1)[0]
    return rows


def environment_value(value, variable):
    if value is not None:
        return value
    return os.environ.get(variable, "") or MISSING


def matrix_id(profile, network):
    """The matrix entry ID of a profile's network mode (tag_scenarios.network_matrix)."""
    networks = profile.get("networks") or []
    if network and networks and network != networks[0]:
        return profile["id"] + "-" + network
    return profile["id"]


def normalize(scenario, cleanup=None, images=None, metadata=None):
    metadata = dict(metadata or {})
    evidence = Evidence({"scenario": scenario, "cleanup": cleanup, "images": images})
    modules = evidence.json("scenario", "modules/modules-report.json")
    compiled = evidence.json("scenario", "compile/compile-report.json")
    native = evidence.json("scenario", "native/report.json")
    log = evidence.text("scenario", "native/native.log")
    phases = evidence.json("scenario", "phases.json")
    before = evidence.json("cleanup", "before.json")
    after = evidence.json("cleanup", "after.json")
    summary = evidence.json("images", "summary.json")
    receipts = {name: value if isinstance(value, dict) else {}
                for name, value in (("modules", modules), ("compile", compiled), ("native", native),
                                    ("phases", phases), ("before", before), ("after", after),
                                    ("images", summary))}

    # Profile identity: every receipt that names one must agree.
    profile = receipts["compile"].get("profile") or receipts["native"].get("profile") or {}
    for other in (receipts["compile"].get("profile"), receipts["native"].get("profile")):
        if other and other.get("id") != profile.get("id"):
            raise ValueError("compile and native receipts belong to different profiles")
    # An empty matrix network lets a checker batch run its default mode,
    # which its native receipt then names.
    requested, reported = metadata.get("network") or "", receipts["native"].get("network")
    if requested and reported is not None and reported != requested:
        raise ValueError("native receipt ran another network mode than the matrix entry")
    network = reported if reported is not None else requested
    candidates = [metadata.get("profile"), receipts["phases"].get("profile"),
                  matrix_id(profile, network) if profile.get("id") else None]
    named = [value for value in candidates if value]
    if not named:
        raise ValueError("no receipt or argument identifies the profile")
    if len(set(named)) != 1:
        raise ValueError("profile IDs of the arguments and receipts differ: " + ", ".join(sorted(set(named))))
    profile_id = named[0]

    key = {"repository": environment_value(metadata.get("repository"), "GITHUB_REPOSITORY"),
           "workflow": environment_value(metadata.get("workflow"), "GITHUB_WORKFLOW_REF"),
           "run_id": environment_value(metadata.get("run_id"), "GITHUB_RUN_ID"),
           "run_attempt": environment_value(metadata.get("run_attempt"), "GITHUB_RUN_ATTEMPT"),
           "job_id": metadata.get("job_id") or MISSING,
           "repetition_id": metadata.get("repetition_id") or "1",
           "profile_id": profile_id,
           "checker": profile.get("checker", MISSING),
           "case": profile.get("case", MISSING),
           "network": network}

    source = {"requested_head": metadata.get("requested_head") or MISSING,
              "checkout_commit": metadata.get("checkout_commit") or MISSING,
              "checkout_tree": metadata.get("checkout_tree") or MISSING,
              "event": environment_value(metadata.get("event"), "GITHUB_EVENT_NAME"),
              "declared_github_revision": receipts["before"].get("declared_github_revision", MISSING)}

    binary = receipts["compile"].get("binary") if isinstance(receipts["compile"].get("binary"), dict) else None
    sources = binary.get("sources") if binary else None
    build = {"build_key": binary.get("build_key") if binary else MISSING,
             "binary_sha256": binary.get("sha256") if binary else MISSING,
             "sources_sha256": (hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(",", ":")).encode())
                                .hexdigest() if isinstance(sources, dict) else MISSING),
             "tests": binary.get("tests") if binary else MISSING,
             "tags": profile.get("tags", MISSING), "package": profile.get("package", MISSING)}
    ran = receipts["native"].get("binary")
    if not native or native == "invalid":
        build["native_binary"] = MISSING
    elif not isinstance(ran, dict):
        build["native_binary"] = "not_used"
    else:
        build["native_binary"] = "matches_compile" if binary and ran.get("sha256") == binary.get("sha256") else "differs"

    requires_ceph = profile.get("requires_ceph")
    workflow = {}
    if isinstance(phases, dict):
        for phase in phases.get("phases") or []:
            if isinstance(phase, dict) and phase.get("phase") in PHASES:
                workflow[phase["phase"]] = WORKFLOW_OUTCOMES.get(phase.get("outcome"), "invalid")
                if phase["phase"] == "images" and requires_ceph is None and phase.get("display") == "not required":
                    requires_ceph = False
    runtime = {"requested_release": metadata.get("release") or MISSING}
    if requires_ceph is False:
        runtime |= {"status": "not_applicable"}
    elif summary is None or summary == "invalid":
        runtime |= {"status": MISSING if summary is None else "invalid"}
    else:
        runtime |= {"status": "recorded", "release": summary.get("release", MISSING),
                    "variant": summary.get("variant", MISSING), "layout": summary.get("layout", MISSING),
                    "platform": summary.get("platform", MISSING),
                    "roles": {role: {"reference": image.get("reference", MISSING),
                                     "image_id": image.get("image_id", MISSING),
                                     "repo_digests": image.get("repo_digests", []),
                                     "platform": image.get("platform", MISSING)}
                              for role, image in sorted((summary.get("images") or {}).items())
                              if isinstance(image, dict)}}

    own = {"modules": modules, "compile": compiled, "docker": before, "images": summary,
           "native": native, "cleanup": after}
    rows = []
    for name in PHASES:
        report = own[name]
        phase = {"phase": name, "workflow": workflow.get(name, MISSING) if isinstance(phases, dict) else MISSING}
        required = not (name == "images" and requires_ceph is False)
        phase["evidence"] = receipt_evidence(report) if required else "not_applicable"
        if isinstance(report, dict):
            for field in ("classification", "elapsed_seconds", "exit_code", "timed_out"):
                if field in report:
                    phase[field] = report[field]
            if name == "images":
                phase["evidence"] = "passed" if report.get("preparation_passed") is True else "failed"
                if "image_preparation_seconds" in report:
                    phase["elapsed_seconds"] = report["image_preparation_seconds"]
            if name == "native":
                strict = report.get("strict_completion")
                phase["completion"] = "passed" if (report.get("completion") or {}).get("passed") is True else "failed"
                phase["strict_completion"] = ("not_applicable" if strict is None
                                              else "passed" if isinstance(strict, dict) and strict.get("passed") is True
                                              else "failed")
            if name == "cleanup" and isinstance(report.get("new_resources"), dict):
                phase["new_resources"] = {kind: len(value) for kind, value in sorted(report["new_resources"].items())
                                          if isinstance(value, list)}
            error = first_error(report)
            if error and phase["evidence"] != "passed":
                phase["error"] = error
        phase["status"] = combine(phase["workflow"], phase["evidence"], required)
        rows.append(phase)

    tests = parse_tests(log) if log is not None else MISSING
    statuses = [phase["status"] for phase in rows]
    if "failed" in statuses:
        status = "failed"
    elif "cancelled" in statuses:
        status = "cancelled"
    elif all(value in ("passed", "not_required") for value in statuses):
        status = "passed"
    else:
        status = "incomplete"
    first = next((phase for phase in rows if phase["status"] == "failed"), None)
    first_failure = None
    if first is not None:
        first_failure = {field: first[field] for field in ("phase", "workflow", "evidence", "classification", "error")
                         if field in first}
        if first["phase"] == "native" and isinstance(tests, list):
            failed = next((test for test in tests if test["result"] == "FAIL"), None)
            first_failure["test"] = failed["name"] if failed else None

    reasons = []
    if source["checkout_commit"] == MISSING:
        reasons.append("missing_checkout_identity")
    if build["build_key"] == MISSING:
        reasons.append("missing_build_identity")
    if runtime["status"] not in ("recorded", "not_applicable"):
        reasons.append("missing_image_identity")
    elif runtime["status"] == "recorded" and any(role["image_id"] == MISSING for role in runtime["roles"].values()):
        reasons.append("missing_image_identity")

    row = {"schema": SCHEMA, "execution_key": key, "source": source,
           "profile": {field: profile.get(field, MISSING) for field in
                       ("id", "profile", "category", "batch", "package", "tags", "checker", "case", "requires_ceph")},
           "build": build, "runtime": runtime, "phases": rows,
           "outcome": {"status": status,
                       "failed_phases": [phase["phase"] for phase in rows if phase["status"] == "failed"],
                       "cancelled_phases": [phase["phase"] for phase in rows if phase["status"] == "cancelled"],
                       "missing_phases": [phase["phase"] for phase in rows if phase["status"] in (MISSING, "invalid")],
                       "skipped_phases": [phase["phase"] for phase in rows if phase["status"] == "skipped"]},
           "first_failure": first_failure,
           "tests": tests,
           "comparability": {"status": "complete" if not reasons else "incomplete", "reasons": reasons},
           "evidence": {"files": dict(sorted(evidence.files.items())),
                        "problems": dict(sorted(evidence.problems.items()))}}
    row["row_sha256"] = row_hash(row)
    return row


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def row_hash(row):
    return hashlib.sha256(canonical({k: v for k, v in row.items() if k != "row_sha256"}).encode()).hexdigest()


def validate_row(row):
    if not isinstance(row, dict) or row.get("schema") != SCHEMA or not isinstance(row.get("execution_key"), dict):
        raise ValueError("not a " + SCHEMA + " row")
    if row.get("row_sha256") != row_hash(row):
        raise ValueError("row content differs from its row_sha256")
    missing = [field for field in INGEST_KEY_FIELDS if row["execution_key"].get(field) in (None, "", MISSING)]
    if missing:
        raise ValueError("execution key lacks " + ", ".join(missing) + "; such a row cannot be told apart")
    return canonical(row["execution_key"])


def read_history(path):
    rows = []
    if path.exists():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    raise ValueError(str(path) + ":" + str(number) + ": not JSON") from None
    return rows


def ingest(history, incoming):
    """Append rows to history; return (added, duplicates). Raise on conflicts."""
    history = Path(history)
    rows = read_history(history)
    known = {}
    for row in rows:
        known[validate_row(row)] = row
    added, duplicates, conflicts = [], 0, []
    for row in incoming:
        key = validate_row(row)
        existing = known.get(key)
        if existing is None:
            known[key] = row
            added.append(row)
        elif existing["row_sha256"] == row["row_sha256"]:
            duplicates += 1
        else:
            conflicts.append({"execution_key": row["execution_key"],
                              "existing": {"row_sha256": existing["row_sha256"], "evidence": existing.get("evidence")},
                              "incoming": {"row_sha256": row["row_sha256"], "evidence": row.get("evidence")}})
    if conflicts:
        raise ConflictError(conflicts)
    if added:
        history.parent.mkdir(parents=True, exist_ok=True)
        content = "".join(canonical(row) + "\n" for row in rows + added)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=history.parent, delete=False) as output:
            output.write(content)
        os.replace(output.name, history)
    return len(added), duplicates


class ConflictError(ValueError):
    def __init__(self, conflicts):
        super().__init__(str(len(conflicts)) + " execution key(s) already hold different content")
        self.conflicts = conflicts


def percentile(values, rank):
    """Nearest-rank percentile; None for no samples."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(rank / 100 * len(ordered)) - 1)]


def summarize(rows):
    """Per profile and release counts. Cancelled rows are counted but stay
    out of the failure denominator; native durations come only from rows
    whose native receipt exists and that were not cancelled."""
    groups = {}
    for row in rows:
        validate_row(row)
        release = row["runtime"].get("release") or row["runtime"].get("requested_release") or MISSING
        group = groups.setdefault(row["execution_key"]["profile_id"] + "@" + str(release), {
            "rows": 0, "cancelled": 0, "incomplete": 0, "passed": 0, "failed": 0,
            "failures_by_phase": {}, "failures_by_classification": {}, "_seconds": []})
        group["rows"] += 1
        status = row["outcome"]["status"]
        group[status] += 1
        if status == "failed":
            first = row["first_failure"] or {}
            phase = first.get("phase", MISSING)
            label = phase + ":" + str(first.get("classification") or "workflow-" + str(first.get("workflow")))
            group["failures_by_phase"][phase] = group["failures_by_phase"].get(phase, 0) + 1
            group["failures_by_classification"][label] = group["failures_by_classification"].get(label, 0) + 1
        native = next(phase for phase in row["phases"] if phase["phase"] == "native")
        if status != "cancelled" and native["evidence"] in ("passed", "failed") \
                and isinstance(native.get("elapsed_seconds"), (int, float)):
            group["_seconds"].append(native["elapsed_seconds"])
    result = {}
    for name in sorted(groups):
        group = groups[name]
        seconds = group.pop("_seconds")
        group["failure_denominator"] = group["rows"] - group["cancelled"]
        group["native_seconds"] = {"samples": len(seconds), "p50": percentile(seconds, 50),
                                   "p90": percentile(seconds, 90)}
        group["failures_by_phase"] = dict(sorted(group["failures_by_phase"].items()))
        group["failures_by_classification"] = dict(sorted(group["failures_by_classification"].items()))
        result[name] = group
    return {"schema": SCHEMA + "/summary", "profiles": result}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("normalize", help="Write one history row from a job's receipts")
    command.add_argument("--scenario", type=Path, required=True, help="artifacts/scenario of the job")
    command.add_argument("--cleanup", type=Path, help="Directory of before.json and after.json")
    command.add_argument("--images", type=Path, help="Directory of the role image summary.json")
    command.add_argument("--output", type=Path, help="Row file; standard output when omitted")
    for name in ("profile", "network", "repository", "workflow", "run-id", "run-attempt", "job-id",
                 "repetition-id", "requested-head", "checkout-commit", "checkout-tree", "event", "release"):
        command.add_argument("--" + name)
    command = commands.add_parser("ingest", help="Append rows to a JSONL history file")
    command.add_argument("--history", type=Path, required=True)
    command.add_argument("rows", type=Path, nargs="+")
    command = commands.add_parser("summary", help="Count rows of a JSONL history file")
    command.add_argument("--history", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "normalize":
            metadata = {name: getattr(args, name) for name in
                        ("profile", "network", "repository", "workflow", "run_id", "run_attempt", "job_id",
                         "repetition_id", "requested_head", "checkout_commit", "checkout_tree", "event", "release")}
            row = normalize(args.scenario, args.cleanup, args.images, metadata)
            content = json.dumps(row, indent=2, sort_keys=True) + "\n"
            if args.output is None:
                sys.stdout.write(content)
            else:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(content, encoding="utf-8")
                print("History row: " + row["outcome"]["status"] + "; " + str(args.output))
        elif args.command == "ingest":
            rows = [json.loads(path.read_text(encoding="utf-8")) for path in args.rows]
            added, duplicates = ingest(args.history, rows)
            print(json.dumps({"added": added, "duplicates": duplicates}))
        else:
            print(json.dumps(summarize(read_history(args.history)), indent=2, sort_keys=True))
        return 0
    except ConflictError as error:
        print(json.dumps({"error": str(error), "conflicts": error.conflicts}, indent=2, sort_keys=True),
              file=sys.stderr)
        return 1
    except (ValueError, OSError) as error:
        print("Scenario history failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
