#!/usr/bin/env python3
"""Offline allocation-cost ledger over the existing dispatch and cohort joins.

Job windows are measured runner seconds, never billing. Incomplete, failed,
cancelled or conflicting inputs retain their cost and cannot supply a matched
attempt-total comparison. This script fetches nothing and executes no workload.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import sys

import ci_measurements as measurements
import dispatch_join
import scenario_history

SCHEMA = "ceph-dispatch-cost/v2"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp(value):
    try:
        return measurements.parse_time(value) if isinstance(value, str) else None
    except ValueError:
        return None


def window(job):
    """Allocation and timing evidence, independently of a native step marker."""
    steps = job.get("steps")
    assigned = ((type(job.get("runner_id")) is int and job["runner_id"] > 0)
                or isinstance(job.get("runner_name"), str) and bool(job["runner_name"].strip()))
    executed = isinstance(steps, list) and any(
        isinstance(step, dict) and step.get("conclusion") in ("success", "failure", "cancelled", "timed_out", "neutral", "action_required")
        and timestamp(step.get("started_at")) is not None for step in steps)
    if not assigned and not executed:
        return {"allocation": "unknown", "runner_seconds": None, "reason": "no_positive_allocation_evidence"}
    start, end, created = [timestamp(job.get(field)) for field in ("started_at", "completed_at", "created_at")]
    out = {"allocation": "allocated", "runner_seconds": None, "acquisition_seconds": None,
           "step_seconds": None, "unattributed_seconds": None}
    if start is None or end is None or end < start:
        return out | {"reason": "missing_or_invalid_job_window"}
    out["runner_seconds"] = (end - start).total_seconds()
    if created is not None and created <= start:
        out["acquisition_seconds"] = (start - created).total_seconds()
    if not isinstance(steps, list):
        return out | {"reason": "missing_steps"}
    intervals = []
    for step in steps:
        if not isinstance(step, dict) or not isinstance(step.get("name"), str):
            return out | {"reason": "invalid_step"}
        a, b = timestamp(step.get("started_at")), timestamp(step.get("completed_at"))
        if a is None and b is None and step.get("conclusion") == "skipped":
            continue
        if a is None or b is None or not start <= a <= b <= end:
            return out | {"reason": "missing_or_invalid_step_window"}
        intervals.append((a, b))
    intervals.sort()
    if any(a[1] > b[0] for a, b in zip(intervals, intervals[1:])):
        return out | {"reason": "overlapping_step_windows"}
    out["step_seconds"] = measurements.job_phases(job)
    out["unattributed_seconds"] = out["runner_seconds"] - sum(out["step_seconds"].values())
    return out


def positions(join):
    """Reuse the conserved association buckets; each input position owns one."""
    out, matched = {}, {}
    for name in ("matched", "conflicts", "ambiguous", "unmatched_actual", "invalid_actual", "out_of_scope"):
        for record in join[name]:
            jobs = record.get("jobs", []) if name == "ambiguous" else [record.get("job", record)]
            for job in jobs:
                if "position" in job:
                    out[job["position"]] = name
                    if name == "matched":
                        matched[job["position"]] = record
    return out, matched


def complete_config(identity):
    """The initial matched-total scope needs explicit immutable Ceph roles."""
    if not isinstance(identity, dict) or not re.fullmatch(r"[0-9a-f]{40}", str(identity.get("checkout_tree", ""))):
        return False
    key, platform, images = identity.get("build_key"), identity.get("platform"), identity.get("images")
    return (isinstance(identity.get("release"), str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", identity["release"])
            and isinstance(key, dict)
            and all(isinstance(key.get(name), str) and key[name] and key[name] != scenario_history.MISSING
                    for name in ("package", "tags", "go_version", "goos", "goarch"))
            and key["go_version"].startswith("go")
            and isinstance(key.get("go_environment"), dict) and bool(key["go_environment"])
            and all(isinstance(name, str) and isinstance(value, str) for name, value in key["go_environment"].items())
            and platform == str(key.get("goos")) + "/" + str(key.get("goarch"))
            and isinstance(images, dict) and bool(images)
            and all(isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) for value in images.values()))


def cache_state(job, ledger, repository):
    relevant = [row for row in ledger if all(str(row.get(field)) == str(job.get(field))
                                           for field in ("run_id", "run_attempt"))
                and str(row.get("job_id")) == str(job.get("id"))]
    if any(row.get("repository") != repository or not isinstance(row.get("outcome"), str)
           or row["outcome"] == "unknown" or (row.get("operation") in ("restore", "import")
                                               and row.get("evidence_source") != "log") for row in relevant):
        return None
    steps = job.get("steps")
    if not isinstance(steps, list) or any(not isinstance(step, dict) or not isinstance(step.get("name"), str) for step in steps):
        return None
    cache_steps = [step for step in steps if measurements.CACHE_STEP.match(step["name"])]
    # A successful save cannot stand in for a missing restore. Reconcile
    # each observed operation with its own producer-bound row before reducing
    # the ledger to a cache condition; duplicate steps consume distinct rows.
    remaining = list(relevant)
    for step in cache_steps:
        name = step["name"]
        operation = "save" if name.lower().startswith(("save", "post")) else "restore"
        kind = measurements.step_kind(name)
        match = next((index for index, row in enumerate(remaining)
                      if row.get("step") == name and row.get("operation") == operation
                      and (row.get("kind") == kind or "other" in (kind, row.get("kind")))), None)
        if match is None:
            return None
        remaining.pop(match)
    try:
        return measurements.cache_condition(relevant)
    except (KeyError, TypeError):
        return None


def assess(dispatch, job_documents, run, receipts=None, caller=None, cache_ledger=None):
    context, entries = dispatch_join.read_dispatch(dispatch)
    jobs = dispatch_join.read_jobs(job_documents)
    repository, run_id, attempt = context["repository"], str(context["run_id"]), str(context["run_attempt"])
    if (not isinstance(run, dict) or str(run.get("id")) != run_id or str(run.get("run_attempt")) != attempt
            or (run.get("repository") or {}).get("full_name") != repository):
        raise ValueError("run metadata does not belong to the dispatch repository/run/attempt")
    joined = dispatch_join.join(dispatch, job_documents, receipts, caller)
    if not joined["conservation"]["ok"]:
        raise ValueError("dispatch association conservation failed")
    bucket_of, matched = positions(joined)
    groups = defaultdict(list)
    for position, job in enumerate(jobs):
        identifier = dispatch_join.identifier(job.get("id")) if isinstance(job, dict) else None
        groups[("job", identifier) if identifier else ("unidentified", str(position))].append((position, job))
    canonical_jobs, records = [], []
    for (kind, identifier), copies in groups.items():
        raw = [value for _, value in copies]
        same = all(value == raw[0] for value in raw)
        job = raw[0] if isinstance(raw[0], dict) else {}
        timing = window(job) if same else {"allocation": "unknown", "runner_seconds": None,
                                          "reason": "conflicting_duplicate_job_records"}
        if kind == "unidentified":
            timing = {"allocation": "unknown", "runner_seconds": None, "reason": "unidentified_job_record"}
        bucket = bucket_of.get(copies[0][0], "invalid_actual") if len(copies) == 1 else "conflicts"
        record = {"job_id": identifier if kind == "job" else None, "name": job.get("name"),
                  "positions": [p for p, _ in copies], "copies": len(copies), "bucket": bucket,
                  "conclusion": job.get("conclusion"), **timing}
        if not same:
            record["candidate_windows"] = [{"position": p, **window(value)} if isinstance(value, dict)
                                           else {"position": p, "allocation": "unknown", "runner_seconds": None}
                                           for p, value in copies]
        if (same and kind == "job" and not dispatch_join.job_problems(job) and timing.get("step_seconds") is not None
                and timing.get("acquisition_seconds") is not None):
            # Existing cohort logic is reused only for jobs with valid timing;
            # every malformed record remains in the cost ledger independently.
            canonical_jobs.append(job)
        records.append(record)
    valid_rows, invalid_rows = [], list(receipts[1]) if receipts is not None else []
    for path, row in receipts[0] if receipts is not None else []:
        try:
            scenario_history.validate_row(row)
            scenario_history.config_identity(row)
            scenario_history.attempt_kind(row)
            valid_rows.append(row)
        except (ValueError, KeyError, TypeError, AttributeError):
            invalid_rows.append({"path": path, "reason": "incomplete_history_contract"})
    cohorts = measurements.cohorts([run], {run_id: canonical_jobs}, valid_rows, repository)
    source_stamps = defaultdict(set)
    for row in valid_rows:
        key, build, runtime = row["execution_key"], row["build"], row["runtime"]
        if (key.get("repository") == repository and str(key.get("run_id")) == run_id
                and str(key.get("run_attempt")) == attempt):
            stamp = build.get("sources_sha256")
            bound = (build.get("native_binary") == "matches_compile"
                     and build.get("build_key") == build.get("native_build_key")
                     and isinstance(build.get("binary_sha256"), str)
                     and re.fullmatch(r"[0-9a-f]{64}", build["binary_sha256"])
                     and build["binary_sha256"] == build.get("native_binary_sha256")
                     and isinstance(stamp, str) and re.fullmatch(r"[0-9a-f]{64}", stamp)
                     and all(role.get("platform") == runtime.get("platform")
                             for role in runtime.get("roles", {}).values() if isinstance(role, dict)))
            source_stamps[str(key.get("job_id"))].add(stamp if bound else None)
    configs, outcomes = {}, {}
    for config in cohorts["configs"]:
        for sample in config["samples"]:
            configs[str(sample["job_id"])] = config["identity"]
            outcomes[str(sample["job_id"])] = sample["kind"]
    for sample in cohorts["incomparable"]:
        outcomes[str(sample["job_id"])] = sample["kind"]
    reasons, runtime_identity, overhead = [], [], []
    ledger = cache_ledger or []
    for record in records:
        pos, identifier = record["positions"][0], record["job_id"]
        job = jobs[pos] if isinstance(jobs[pos], dict) else {}
        record["history_kind"] = outcomes.get(identifier)
        if record["bucket"] == "matched":
            conclusion = record["conclusion"]
            if conclusion in ("failure", "cancelled", "skipped", "timed_out"):
                record["bucket"] = "matched_" + conclusion
            elif outcomes.get(identifier) == "passed" and identifier in configs:
                record["bucket"] = "matched_passed"
            else:
                record["bucket"] = "matched_incomplete"
        cache = cache_state(job, ledger, repository) if record["allocation"] == "allocated" else None
        record["cache_condition"] = cache
        labels = job.get("labels")
        timing_complete = (record["allocation"] == "allocated" and record.get("runner_seconds") is not None
                           and record.get("acquisition_seconds") is not None and record.get("step_seconds") is not None)
        if not timing_complete:
            reasons.append("incomplete_allocation_or_timing")
        if cache is None:
            reasons.append("unknown_cache_condition")
        if not isinstance(labels, list) or not labels or not all(isinstance(label, str) for label in labels):
            reasons.append("unknown_runner_labels")
        if record["bucket"] == "matched_passed":
            association = matched.get(pos)
            config = configs.get(identifier)
            stamps = source_stamps.get(identifier, set())
            if (association is None or association["receipt"]["state"] != "consistent" or not complete_config(config)
                    or len(stamps) != 1 or None in stamps
                    or config.get("release") != context.get("release")
                    or record["conclusion"] != "success" or job.get("status") != "completed"):
                reasons.append("incomplete_or_inconsistent_runtime_identity")
            else:
                entry = entries[association["entry"]["position"]]
                runtime_identity.append({"entry": entry, "configuration": config, "compiled_sources_sha256": next(iter(stamps)),
                                         "cache_condition": cache, "runner_labels": labels})
        elif record["bucket"] == "unmatched_actual" and dispatch_join.leaf(job.get("name", ""))[1] == "Plan":
            if record["conclusion"] != "success" or job.get("status") != "completed":
                reasons.append("plan_not_successful")
            overhead.append({"name": job.get("name"), "cache_condition": cache, "runner_labels": labels})
        else:
            reasons.append("non_comparable_job_bucket")
    for name in ("conflicts", "ambiguous", "unmatched_expected", "invalid_expected", "invalid_actual", "out_of_scope",
                 "invalid_receipts", "receipts_without_a_matched_job", "receipts_other_run"):
        if joined[name]:
            reasons.append("dispatch_" + name)
    if invalid_rows:
        reasons.append("invalid_history_rows")
    if not entries or len(runtime_identity) != len(entries):
        reasons.append("incomplete_dispatched_execution_set")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        reasons.append("attempt_not_successful")
    if (not isinstance(run.get("name"), str) or not run["name"]
            or not isinstance(run.get("event"), str) or not run["event"]
            or not isinstance(context.get("category"), str) or not context["category"]
            or dispatch.get("networks") not in ("default", "all")
            or type(dispatch.get("repetitions")) is not int or dispatch["repetitions"] < 1):
        reasons.append("unknown_workflow_context")
    buckets = {}
    for record in records:
        group = buckets.setdefault(record["bucket"], {"jobs": 0, "known_runner_seconds": 0.0, "unknown_cost_jobs": 0,
                                                      "allocations": Counter()})
        group["jobs"] += 1
        group["allocations"][record["allocation"]] += 1
        if record["runner_seconds"] is None:
            group["unknown_cost_jobs"] += 1
        else:
            group["known_runner_seconds"] += record["runner_seconds"]
    known = sum(record["runner_seconds"] or 0 for record in records)
    conserved = sum(group["known_runner_seconds"] for group in buckets.values()) == known
    assert conserved and sum(group["jobs"] for group in buckets.values()) == len(records)
    held = [record for record in records if record["allocation"] == "allocated"]
    phases = {name: sum((record.get("step_seconds") or {}).get(name, 0) for record in held)
              for name in measurements.PHASE_NAMES}
    created = timestamp(run.get("created_at"))
    ends = []
    for record in records:
        job = jobs[record["positions"][0]]
        if (isinstance(job, dict) and record.get("reason") != "conflicting_duplicate_job_records"
                and str(job.get("run_id")) == run_id and str(job.get("run_attempt")) == attempt
                and job.get("status") == "completed"):
            ended = timestamp(job.get("completed_at"))
            if ended is not None:
                ends.append(ended)
    wall = (max(ends) - created).total_seconds() if created is not None and ends and created <= min(ends) else None
    if wall is None:
        reasons.append("unknown_observed_wall")
    identity = {"repository": repository, "workflow": run.get("name"), "event": run.get("event"), "category": context.get("category"),
                "release": context.get("release"), "networks": dispatch.get("networks"),
                "repetitions": dispatch.get("repetitions"),
                "runtime": sorted(runtime_identity, key=canonical), "overhead": sorted(overhead, key=canonical)}
    report = {"schema": SCHEMA, "scope": joined["scope"], "run": {key: run.get(key) for key in
            ("id", "run_attempt", "name", "event", "head_sha", "created_at", "status", "conclusion")},
            "jobs": records, "buckets": buckets, "dispatch_join": joined, "cohorts": cohorts,
            "invalid_history_rows": invalid_rows,
            "conservation": {"ok": conserved, "input_job_records": len(jobs), "unique_job_groups": len(records),
                             "duplicate_records": len(jobs) - len(records), "known_runner_seconds": known,
                             "unknown_cost_jobs": sum(record["runner_seconds"] is None for record in records)},
            "metrics": {"known_runner_seconds": known, "observed_wall_seconds": wall,
                        "acquisition_seconds": measurements.span([record.get("acquisition_seconds") for record in held]),
                        "known_step_seconds": phases, "step_timing_unknown_jobs": sum(record.get("step_seconds") is None for record in held),
                        "known_unattributed_seconds": sum(record.get("unattributed_seconds") or 0 for record in held)},
            "comparison": {"eligible": not reasons, "reasons": sorted(set(reasons)), "identity": identity,
                           "fingerprint": digest(identity) if not reasons else None},
            "claims": {"billing": False, "causal_savings": False, "dag_critical_path": False,
                       "coverage": "dispatched entries only"}}
    report["report_sha256"] = digest(report)
    return report


def compare(reports):
    reasons = []
    if len(reports) < 2:
        reasons.append("at_least_two_complete_attempts_required")
    seen = set()
    for report in reports:
        if (report.get("schema") != SCHEMA or report.get("report_sha256") !=
                digest({key: value for key, value in report.items() if key != "report_sha256"})):
            raise ValueError("not an intact dispatch cost report")
        key = tuple(report["scope"].get(field) for field in ("repository", "run_id", "run_attempt"))
        if key in seen:
            reasons.append("duplicate_attempt")
        seen.add(key)
        admission = report["comparison"]
        if (admission["eligible"] is not True or admission["reasons"] != []
                or report["conservation"]["ok"] is not True or report["conservation"]["unknown_cost_jobs"] != 0
                or admission["fingerprint"] != digest(admission["identity"])):
            reasons.append("attempt_not_eligible")
    if len({report["comparison"]["fingerprint"] for report in reports}) != 1:
        reasons.append("configuration_dispatch_or_cache_differs")
    result = {"schema": SCHEMA + "/comparison", "status": "uncomparable" if reasons else "matched",
              "samples": len(reports), "reasons": sorted(set(reasons)),
              "attempts": [{"scope": report["scope"], "eligibility": report["comparison"]["reasons"]} for report in reports],
              "claims": {"billing": False, "causal_savings": False}}
    if not reasons:
        result["matched_ranges"] = {name: measurements.span([report["metrics"][name] for report in reports])
                                    for name in ("known_runner_seconds", "observed_wall_seconds")}
        result["step_seconds"] = {name: measurements.span([report["metrics"]["known_step_seconds"][name] for report in reports])
                                  for name in measurements.PHASE_NAMES}
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("assess")
    for name in ("dispatch", "run"):
        command.add_argument("--" + name, type=Path, required=True)
    command.add_argument("--jobs", type=Path, action="append", required=True)
    command.add_argument("--receipts", type=Path)
    command.add_argument("--caller")
    command.add_argument("--cache-ledger", type=Path, help="ci_measurements caches report with rows")
    command = commands.add_parser("compare")
    command.add_argument("reports", nargs="+", type=Path)
    for command in commands.choices.values():
        command.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "assess":
            ledger = dispatch_join.load_json(args.cache_ledger).get("rows", []) if args.cache_ledger else None
            result = assess(dispatch_join.load_json(args.dispatch), [dispatch_join.load_json(path) for path in args.jobs],
                            dispatch_join.load_json(args.run), dispatch_join.load_receipts(args.receipts) if args.receipts else None,
                            args.caller, ledger)
        else:
            result = compare([dispatch_join.load_json(path) for path in args.reports])
        text = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(text)
        else:
            print(text, end="")
        return 0
    except (ValueError, KeyError, TypeError, AttributeError, OSError, dispatch_join.Inconclusive) as error:
        print("dispatch cost: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
