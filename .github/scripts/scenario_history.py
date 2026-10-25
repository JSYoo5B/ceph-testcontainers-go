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

normalize --summary also appends the row's provenance to a Markdown job
summary: the bug compatibility selection beside the release, platform and
role image IDs.

summary counts rows per profile and release from a JSONL history file.

collect stores the rows of downloaded artifacts durably, one JSONL file per
workflow run attempt under a history directory (the scenario-history
branch), and rewrites that directory's summary.json: per profile and
release, the recent attempts and each configuration's same-config
classification. classify prints that summary for a history directory.
"""

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
from pathlib import Path
import sys
import tempfile

import tag_scenarios
import messenger_evidence


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


def bug_compatibility(profile, requested):
    """The bug compatibility selection that the receipts record.

    A profile's tags carry its selection (tag_scenarios.native_tags), so
    every selected tag must be one of them. requested, the workflow's own
    selection as a comma-separated string, must equal the receipts'; None
    skips that check. A receipt from before selections existed records
    none, which only the empty selection matches."""
    if not profile:
        return MISSING
    selected = profile.get("bug_compatibility", [])
    if not isinstance(selected, list) or selected != sorted(set(selected)) \
            or not set(selected) <= set(str(profile.get("tags", "")).split(",")):
        raise ValueError("the receipts' bug compatibility selection is not a sorted subset of their profile tags")
    if requested is not None and sorted({name.strip() for name in requested.split(",") if name.strip()}) != selected:
        raise ValueError("the receipts were built with another bug compatibility selection than this workflow's: "
                         + (",".join(selected) or "none"))
    return selected



def messenger_history(row, compiled, native, log, prepared, root=None):
    """Join existing case evidence; a job PASS alone never supplies a case.

    Image preparation precedes native execution. Its immutable IDs are the
    independent expected cohort, never a set reconstructed from case output.
    This optional additive field leaves legacy rows and scenario verdicts alone.
    """
    root = Path(root or messenger_evidence.BASE).resolve()
    result = {"status": "not_selected", "wire_coverage_claimed": False,
              "consumer_implementation_claimed": False}
    try:
        document = messenger_evidence.loads((root / messenger_evidence.BINDINGS).read_bytes())
        cases = {b["case"] for b in document["bindings"]}
        inventory = (compiled or {}).get("binary", {}).get("tests", [])
        observed = {test["name"] for test in row["tests"]} if isinstance(row["tests"], list) else set()
        if not cases.intersection(set(inventory) | observed) and "MSGR_CASE_RECEIPT " not in (log or ""):
            return result
        result["status"] = "unexecuted"
        # Inventory is retained even for failed/skipped jobs; no native case is
        # promoted unless every execution and owned-cleanup phase passed.
        mapping = messenger_evidence.report(root)
        result["report"] = mapping
        if row["outcome"]["status"] != "passed":
            result["reason"] = "execution_not_passed"
            return result
        result["status"] = "invalid"
        def need(value, reason):
            if not value:
                raise ValueError(reason)
        binary = compiled.get("binary", {})
        ran = native.get("binary", {})
        need(isinstance(inventory, list) and inventory
             and all(isinstance(case, str) and re.fullmatch(r"Test[A-Za-z0-9_]+", case) for case in inventory)
             and len(inventory) == len(set(inventory)), "invalid_compiled_inventory")
        completion = native.get("completion")
        strict = native.get("strict_completion")
        need(native.get("passed") is True and native.get("classification") == "passed"
             and type(native.get("exit_code")) is int and native["exit_code"] == 0
             and ("timed_out" not in native or native["timed_out"] is False)
             and isinstance(completion, dict) and completion.get("passed") is True
             and completion.get("errors") == []
             and "strict_completion" in native
             and (strict is None or isinstance(strict, dict) and strict.get("passed") is True
                  and strict.get("errors") == []), "native_terminal_metadata_refused")
        need(row["comparability"]["status"] == "complete", "incomplete_provenance")
        need(re.fullmatch(r"[0-9a-f]{40}", row["source"]["checkout_commit"]), "missing_checkout_identity")
        need(re.fullmatch(r"[0-9a-f]{64}", binary.get("sha256", ""))
             and binary.get("sha256") == ran.get("sha256")
             and binary.get("build_key") == ran.get("build_key"), "binary_identity_mismatch")
        sources = binary.get("sources")
        need(isinstance(sources, dict) and sources, "missing_source_snapshot")
        for name, expected in sources.items():
            path = root / name
            need(not Path(name).is_absolute() and ".." not in Path(name).parts
                 and path.resolve().is_relative_to(root) and path.is_file() and not path.is_symlink(),
                 "unsafe_source_snapshot")
            need(hashlib.sha256(path.read_bytes()).hexdigest() == expected, "source_snapshot_mismatch")
        need(set(messenger_evidence.SOURCES).issubset(sources), "incomplete_case_source_snapshot")
        need(isinstance(prepared, dict) and prepared.get("preparation_passed") is True,
             "missing_prepared_cohort")
        # The pinned reference owns cohort eligibility, not a runtime label.
        reference = messenger_evidence.loads((root / messenger_evidence.REFERENCE).read_bytes())
        if prepared.get("release") != reference["ceph_release"]:
            result["status"], result["reason"] = "unsupported_cohort", "unsupported_release"
            return result
        platform = prepared.get("platform")
        images = prepared.get("images")
        need(isinstance(images, dict) and images and isinstance(platform, str), "missing_prepared_cohort")
        need(all(isinstance(image, dict) and image.get("platform") == platform
                 and re.fullmatch(r"sha256:[0-9a-f]{64}", image.get("image_id", ""))
                 for image in images.values()), "invalid_prepared_cohort")
        key = binary.get("build_key", {})
        need(platform == str(key.get("goos")) + "/" + str(key.get("goarch")), "build_platform_mismatch")
        expected = {"release": prepared["release"], "platform": platform,
                    "images": sorted({image["image_id"] for image in images.values()})}
        need(isinstance(log, str) and len(log.encode()) <= 64 * 1024 * 1024, "missing_or_large_native_log")
        # Only the trusted Go tool reads a bounded retained log. No repository
        # test binary, native operation, or inspected-source program executes.
        with tempfile.TemporaryFile() as output:
            converted = subprocess.run(["go", "tool", "test2json", "-p", messenger_evidence.PACKAGE],
                                       input=log.encode(), stdout=output, stderr=subprocess.DEVNULL,
                                       timeout=30, check=False)
            need(converted.returncode == 0 and output.tell() <= 64 * 1024 * 1024,
                 "native_event_conversion_refused")
            output.seek(0)
            report = messenger_evidence.report(root, output.read(), expected)
        need(report["native_cases"], "no_native_case_accepted")
        need(all(case["case"] in inventory for case in report["native_cases"]),
             "case_absent_from_compiled_inventory")
        result.update(status="accepted", report=report)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.TimeoutExpired):
        result.update(status="invalid", reason="case_evidence_refused")
    return result

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
        if other and (other.get("tags"), other.get("bug_compatibility")) != (profile.get("tags"),
                                                                               profile.get("bug_compatibility")):
            raise ValueError("compile and native receipts carry different tags or bug compatibility selections")
    selection = bug_compatibility(profile, metadata.get("bug_compatibility"))
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
    # The executable that the native phase ran is kept in its own fields, so
    # that a row never binds a native result to an executable it did not run.
    ran = receipts["native"].get("binary")
    if not native or native == "invalid":
        build["native_binary"] = MISSING
    elif not isinstance(ran, dict):
        build["native_binary"] = "not_used"
    else:
        build["native_binary"] = "matches_compile" if binary and ran.get("sha256") == binary.get("sha256") else "differs"
        build["native_binary_sha256"] = ran.get("sha256") or MISSING
        build["native_build_key"] = ran.get("build_key", MISSING)

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
    if build["native_binary"] == "differs":
        reasons.append("native_binary_identity_mismatch")
    if runtime["status"] not in ("recorded", "not_applicable"):
        reasons.append("missing_image_identity")
    elif runtime["status"] == "recorded" and any(role["image_id"] == MISSING for role in runtime["roles"].values()):
        reasons.append("missing_image_identity")

    row = {"schema": SCHEMA, "execution_key": key, "source": source,
           "profile": {field: profile.get(field, MISSING) for field in
                       ("id", "profile", "category", "batch", "package", "tags", "checker", "case", "requires_ceph")}
           | {"bug_compatibility": selection},
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
    row["messenger"] = messenger_history(row, receipts["compile"], receipts["native"], log, summary)
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
            "failures_by_phase": {}, "failures_by_classification": {}, "messenger_attempts_by_status": {}, "_seconds": []})
        group["rows"] += 1
        messenger_status = row.get("messenger", {}).get("status", "not_recorded")
        counts = group["messenger_attempts_by_status"]
        counts[messenger_status] = counts.get(messenger_status, 0) + 1
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


# Same-configuration classification. A row's attempt kind says what its
# outcome is evidence of; only scenario outcomes (DECIDED) count towards a
# configuration's pass/fail record. Runner and image failures and cancelled
# or incomplete attempts are counted apart, never as scenario failures.
CLASSIFICATION_SCHEMA = SCHEMA + "/classification"
INFRASTRUCTURE_PHASES = ("modules", "compile", "docker")
DECIDED = ("passed", "assertion_failure", "timeout", "cleanup_failure")
SCENARIO_FAILURES = DECIDED[1:]
CLASSIFICATIONS = ("insufficient_samples", "confirmed_pass", "deterministic_failure", "confirmed_fail",
                   "suspected_intermittent")
# Rows of one profile and release that the recent window covers, and the
# configurations kept per profile, newest first.
RECENT_ATTEMPTS = 20
CONFIGS_KEPT = 10


def release_of(row):
    return row["runtime"].get("release") or row["runtime"].get("requested_release") or MISSING


def attempt_order(row):
    """Rows in execution order: run ID, run attempt, repetition ID."""
    key = row["execution_key"]
    return tuple(int(value) if str(value).isdigit() else 0
                 for value in (key.get("run_id"), key.get("run_attempt"), key.get("repetition_id")))


def attempt_kind(row):
    """What one attempt's outcome is evidence of."""
    status = row["outcome"]["status"]
    if status in ("passed", "cancelled", "incomplete"):
        return status
    first = row.get("first_failure") or {}
    failed_phase = first.get("phase")
    if failed_phase in INFRASTRUCTURE_PHASES:
        return "runner_failure"
    if failed_phase == "images":
        return "image_failure"
    if failed_phase == "cleanup":
        return "cleanup_failure"
    native = next((item for item in row["phases"] if item["phase"] == "native"), {})
    if native.get("timed_out") is True:
        return "timeout"
    return "assertion_failure"


def config_identity(row):
    """Everything that two attempts must share to be the same configuration:
    profile, network mode, release, checked-out tree, build key, platform and
    every role image ID. None for a row without complete identities.

    The build key holds the profile's tags, so two bug compatibility
    selections are already two configurations; profile.bug_compatibility is
    not added, which keeps the keys of earlier rows unchanged."""
    if row["comparability"]["status"] != "complete":
        return None
    runtime = row["runtime"]
    return {"profile_id": row["execution_key"]["profile_id"], "network": row["execution_key"].get("network", ""),
            "release": release_of(row), "checkout_tree": row["source"]["checkout_tree"],
            "build_key": row["build"]["build_key"], "platform": runtime.get("platform", runtime["status"]),
            "images": {role: image["image_id"] for role, image in sorted((runtime.get("roles") or {}).items())}}


def config_key(row):
    identity = config_identity(row)
    if identity is None:
        return None
    return hashlib.sha256(canonical(identity).encode()).hexdigest()[:16]


def failure_signature(row):
    first = row.get("first_failure") or {}
    return (first.get("phase"), first.get("test"), first.get("classification"))


def classify_config(rows):
    """One configuration's record and classification; rows in execution order."""
    kinds = [attempt_kind(row) for row in rows]
    decided = [(row, kind) for row, kind in zip(rows, kinds) if kind in DECIDED]
    passed = sum(1 for _, kind in decided if kind == "passed")
    failing = [row for row, kind in decided if kind != "passed"]
    if len(decided) < 2:
        classification = "insufficient_samples"
    elif not failing:
        classification = "confirmed_pass"
    elif not passed:
        signatures = {failure_signature(row) for row in failing}
        classification = "deterministic_failure" if len(signatures) == 1 else "confirmed_fail"
    else:
        classification = "suspected_intermittent"
    # Per test, only the attempts whose log has its PASS or FAIL line. A
    # scenario attempt is not evidence that a test ran: a timeout, panic or
    # early stop can end the run before a later test starts, and a test that
    # started without a result has no outcome.
    tests = {}
    for row, _ in decided:
        for test in row["tests"] if isinstance(row["tests"], list) else []:
            if test.get("result") in ("PASS", "FAIL"):
                record = tests.setdefault(test["name"], {"failed": 0, "ran": 0})
                record["ran"] += 1
                record["failed"] += test["result"] == "FAIL"
    first = None
    if failing:
        # The first failure stays exposed; a later pass does not erase it.
        key = failing[0]["execution_key"]
        first = {field: key.get(field) for field in ("run_id", "run_attempt", "job_id", "repetition_id")}
        first.update({field: value for field, value in (failing[0].get("first_failure") or {}).items()
                      if field in ("phase", "test", "classification")})
        first["kind"] = attempt_kind(failing[0])
    excluded = {}
    for kind in kinds:
        if kind not in DECIDED:
            excluded[kind] = excluded.get(kind, 0) + 1
    return {"identity": config_identity(rows[0]), "attempts": len(decided), "passed": passed,
            "failed": len(failing), "excluded": dict(sorted(excluded.items())),
            "classification": classification, "first_failure": first,
            "last_run_id": rows[-1]["execution_key"]["run_id"],
            "tests": dict(sorted(tests.items()))}


def classify(rows):
    """Per profile and release: the recent window of attempts and each
    recent configuration's same-config classification."""
    groups = {}
    for row in rows:
        validate_row(row)
        groups.setdefault(row["execution_key"]["profile_id"] + "@" + str(release_of(row)), []).append(row)
    profiles = {}
    for name in sorted(groups):
        ordered = sorted(groups[name], key=attempt_order)
        recent = ordered[-RECENT_ATTEMPTS:]
        kinds = {}
        for row in recent:
            kind = attempt_kind(row)
            kinds[kind] = kinds.get(kind, 0) + 1
        by_config, incomparable = {}, 0
        for row in ordered:
            key = config_key(row)
            if key is None:
                incomparable += 1
            else:
                by_config.setdefault(key, []).append(row)
        newest = sorted(by_config, key=lambda key: attempt_order(by_config[key][-1]), reverse=True)[:CONFIGS_KEPT]
        messenger_counts = {}
        for row in recent:
            state = row.get("messenger", {}).get("status", "not_recorded")
            messenger_counts[state] = messenger_counts.get(state, 0) + 1
        profiles[name] = {
            "rows": len(ordered), "incomparable_rows": incomparable,
            "recent_messenger_attempts_by_status": dict(sorted(messenger_counts.items())),
            "recent": {"rows": len(recent), "attempts": sum(kinds.get(kind, 0) for kind in DECIDED),
                       "failed": sum(kinds.get(kind, 0) for kind in SCENARIO_FAILURES),
                       "kinds": dict(sorted(kinds.items()))},
            "configs": {key: classify_config(by_config[key]) for key in newest}}
    return {"schema": CLASSIFICATION_SCHEMA, "rows": len(rows), "profiles": profiles}


def collect(history_dir, rows_dir):
    """Ingest every history-row.json under rows_dir into history_dir/rows,
    one JSONL file per workflow run attempt, then rewrite summary.json.

    A run's file is only ever appended to, so the stored rows of earlier
    runs never change. A row that cannot be told apart (no run identity) is
    reported and skipped; a conflicting row fails the collection."""
    history_dir, rows_dir = Path(history_dir), Path(rows_dir)
    incoming, rejected = {}, []
    for path in sorted(rows_dir.rglob("history-row.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
            validate_row(row)
        except (ValueError, OSError) as error:
            rejected.append({"path": str(path.relative_to(rows_dir)), "error": bounded(error)})
            continue
        key = row["execution_key"]
        name = "-".join(str(key[field]) for field in ("run_id", "run_attempt"))
        if not all(part.isalnum() for part in name.split("-")):
            rejected.append({"path": str(path.relative_to(rows_dir)), "error": "run identity is not alphanumeric"})
            continue
        incoming.setdefault(name, []).append(row)
    added = duplicates = 0
    for name, rows in sorted(incoming.items()):
        result = ingest(history_dir / "rows" / (name + ".jsonl"), rows)
        added, duplicates = added + result[0], duplicates + result[1]
    summary = classify(stored_rows(history_dir))
    write_atomic(history_dir / "summary.json", json.dumps(summary, indent=1, sort_keys=True) + "\n")
    return {"added": added, "duplicates": duplicates, "rejected": rejected, "rows": summary["rows"]}


def stored_rows(history_dir):
    rows = []
    for path in sorted((Path(history_dir) / "rows").glob("*.jsonl")):
        rows.extend(read_history(path))
    return rows


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as output:
        output.write(content)
    os.replace(output.name, path)


def history_notes(summary, row, tests):
    """Plain sentences that put a failing job's tests beside the history of
    earlier attempts: per test in the same configuration, and the profile's
    recent window. The summary is data from the history branch; anything
    malformed yields no note rather than a guess."""
    if not isinstance(summary, dict) or summary.get("schema") != CLASSIFICATION_SCHEMA:
        return []
    release = release_of(row)
    profile = (summary.get("profiles") or {}).get(row["execution_key"]["profile_id"] + "@" + str(release))
    if not isinstance(profile, dict):
        return ["no earlier attempt of this profile on " + str(release) + " is in the scenario history"]
    notes = []
    recent = profile.get("recent") or {}
    if isinstance(recent.get("failed"), int) and isinstance(recent.get("attempts"), int):
        notes.append("the profile failed {}/{} recent attempts on {}".format(recent["failed"], recent["attempts"],
                                                                            release))
    config = (profile.get("configs") or {}).get(config_key(row) or "")
    if not isinstance(config, dict) or not isinstance(config.get("attempts"), int):
        notes.append("no earlier same-config attempt")
        return notes
    classification = config.get("classification")
    if classification not in CLASSIFICATIONS:
        return notes
    records = config.get("tests") if isinstance(config.get("tests"), dict) else {}
    for test in tests:
        record = records.get(test)
        if record is None or (isinstance(record, dict) and record.get("ran") == 0):
            notes.append("{} has no observed outcome in {} earlier same-config attempts".format(
                test, config["attempts"]))
        elif isinstance(record, dict) and isinstance(record.get("failed"), int) and isinstance(record.get("ran"), int):
            notes.append("{} failed {}/{} earlier same-config attempts that ran it".format(
                test, record["failed"], record["ran"]))
    notes.append("same-config record: {} passed, {} failed ({})".format(config["passed"], config["failed"],
                                                                        classification))
    return notes


def markdown_cell(value):
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def provenance_summary(row):
    """The row's selected paths beside its image identities, as Markdown."""
    selection = row["profile"].get("bug_compatibility", MISSING)
    if isinstance(selection, list):
        selection = ", ".join("`" + name + "`" for name in selection) or "none (ordinary paths)"
    runtime = row["runtime"]
    lines = ["#### Provenance of " + markdown_cell(row["execution_key"]["profile_id"]), "",
             "| Field | Value |", "| --- | --- |",
             "| Bug compatibility selection | " + markdown_cell(selection) + " |",
             "| Tags | " + markdown_cell(row["build"]["tags"]) + " |",
             "| Checkout tree | " + markdown_cell(row["source"]["checkout_tree"]) + " |",
             "| Release | " + markdown_cell(runtime.get("release", runtime["requested_release"])) + " |",
             "| Platform | " + markdown_cell(runtime.get("platform", runtime["status"])) + " |"]
    messenger = row.get("messenger", {"status": "not_recorded"})
    lines.append("| Messenger native-state evidence | " + markdown_cell(messenger["status"]) + " |")
    report = messenger.get("report", {})
    if report:
        lines.append("| Messenger declarations mapped / total | " + str(report["mapped_declarations"]) + " / " + str(report["source_declarations"]) + " |")
        lines.append("| Messenger accepted cases / unexecuted bindings | " + str(len(report["native_cases"])) + " / " + str(len(report["unexecuted_bindings"])) + " |")
    for role, image in sorted((runtime.get("roles") or {}).items()):
        lines.append("| Image `" + markdown_cell(role) + "` | " + markdown_cell(image["image_id"]) + " |")
    if runtime["status"] != "recorded":
        lines.append("| Images | " + markdown_cell(runtime["status"]) + " |")
    return "\n".join(lines + ["", ""])


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
    command.add_argument("--bug-compatibility", metavar="TAGS",
                         help="The workflow's bug compatibility selection; receipts of another one write no row")
    command.add_argument("--summary", type=Path, help="Append the row's provenance, such as $GITHUB_STEP_SUMMARY")
    command = commands.add_parser("ingest", help="Append rows to a JSONL history file")
    command.add_argument("--history", type=Path, required=True)
    command.add_argument("rows", type=Path, nargs="+")
    command = commands.add_parser("summary", help="Count rows of a JSONL history file")
    command.add_argument("--history", type=Path, required=True)
    command = commands.add_parser("collect", help="Store downloaded rows and rewrite the classification")
    command.add_argument("--history-dir", type=Path, required=True, help="Checkout of the history branch")
    command.add_argument("--rows", type=Path, required=True, help="Directory searched for history-row.json")
    command = commands.add_parser("classify", help="Print the classification of a history directory")
    command.add_argument("--history-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "normalize":
            metadata = {name: getattr(args, name) for name in
                        ("profile", "network", "repository", "workflow", "run_id", "run_attempt", "job_id",
                         "repetition_id", "requested_head", "checkout_commit", "checkout_tree", "event", "release",
                         "bug_compatibility")}
            row = normalize(args.scenario, args.cleanup, args.images, metadata)
            if args.summary is not None:
                with args.summary.open("a", encoding="utf-8") as summary:
                    summary.write(provenance_summary(row))
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
        elif args.command == "collect":
            result = collect(args.history_dir, args.rows)
            print(json.dumps(result, indent=2, sort_keys=True))
            for item in result["rejected"]:
                print("::warning title=Scenario history::Skipped " + item["path"] + ": " + item["error"])
        elif args.command == "classify":
            print(json.dumps(classify(stored_rows(args.history_dir)), indent=2, sort_keys=True))
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
