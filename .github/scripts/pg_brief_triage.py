#!/usr/bin/env python3
"""Offline triage of the retained `ceph pg dump pgs_brief` diagnostic output.

This reads one already retained text (the `pgs` artifact of a diagnostics
report) and its recorded truncation flag. It never queries a cluster and never
changes a test verdict. It answers only what the brief output can answer:

- whether the bytes are usable evidence: complete, truncated, unparseable or
  inconclusive. A clipped or malformed text is never decoded into an empty or
  healthy PG list, and an empty or missing `pg_stats` is not evidence of health;
- which PGs are not in a settled `active+clean` state, grouped by pool, and a
  capped list of PGs a targeted read-only expansion (for example `pg <pgid>
  query`) could start from. The cap keeps the expansion bounded; omitted
  candidates are counted, not dropped silently;
- how many rows of this shape fit the output limit. This is arithmetic on the
  sample, not a measurement of any cluster or of query time.

At the pinned Ceph source (7f793731f1b39eb4f465e960113d2363c311b964),
`pg_stat_t::dump_brief` emits only state, up, acting, up_primary and
acting_primary (plus the pgid written by `PGMap::dump_pg_stats`). Report epoch,
scrub and active/clean timestamps, object counts and scrub error counts are
therefore listed under `not_answerable_from_brief`: a missing or stale-stat
question needs a different, explicit read-only capture, not this output.
"""
import argparse
import json
from pathlib import Path
import re
import sys

COMPLETE = "complete"
TRUNCATED = "truncated"
UNPARSEABLE = "unparseable"
INCONCLUSIVE = "inconclusive"

# diagnostics/diagnostics.go clips each query to MaxOutputBytes (default 64 KiB).
DEFAULT_LIMIT = 64 << 10
DEFAULT_MAX_TARGETS = 8
SETTLED_TOKENS = frozenset({"active", "clean", "scrubbing", "deep"})
NOT_ANSWERABLE_FROM_BRIEF = (
    "reported_epoch", "last_active", "last_clean", "last_scrub", "last_deep_scrub",
    "stat_sum", "scrub_errors", "log_start", "last_fresh",
)
PGID = re.compile(r"^(0|[1-9][0-9]*)\.[0-9a-f]+$")


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def pg_key(pgid):
    pool, seed = pgid.split(".")
    return int(pool), int(seed, 16)


def row_problem(row):
    """Return why a pg_stats row is not usable, or None."""
    if not isinstance(row, dict):
        return "row is not an object"
    pgid = row.get("pgid")
    if not isinstance(pgid, str) or not PGID.match(pgid):
        return "pgid missing or malformed"
    if not isinstance(row.get("state"), str) or not row["state"]:
        return "state missing or empty"
    for field in ("up", "acting"):
        value = row.get(field)
        if not isinstance(value, list) or not all(is_int(v) for v in value):
            return field + " missing or not an integer list"
    for field in ("up_primary", "acting_primary"):
        if not is_int(row.get(field)):
            return field + " missing or not an integer"
    return None


def triage(text, truncated, expected_pgs=None, limit=DEFAULT_LIMIT, max_targets=DEFAULT_MAX_TARGETS):
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    if not isinstance(max_targets, int) or isinstance(max_targets, bool) or max_targets < 0:
        raise ValueError("max_targets must be a non-negative integer")
    report = {
        "status": INCONCLUSIVE, "settled": None, "reasons": [], "pg_count": None,
        "bytes": None, "truncation_recorded": truncated if isinstance(truncated, bool) else None,
        "states": {}, "unsettled_by_pool": {}, "targets": [], "targets_omitted": 0,
        "rows_fitting_limit": None, "not_answerable_from_brief": list(NOT_ANSWERABLE_FROM_BRIEF),
    }
    reasons = report["reasons"]
    if not isinstance(text, str):
        reasons.append("output text is missing or not a string")
        return report
    report["bytes"] = len(text.encode("utf-8"))
    if not isinstance(truncated, bool):
        reasons.append("truncation flag is missing or not a boolean")
        return report
    if expected_pgs is not None and (not is_int(expected_pgs) or expected_pgs < 0):
        raise ValueError("expected_pgs must be a non-negative integer")
    try:
        document = json.loads(text)
    except ValueError:
        report["status"] = TRUNCATED if truncated else UNPARSEABLE
        reasons.append("output is clipped and not valid JSON" if truncated
                       else "output is not valid JSON")
        return report
    if truncated:
        reasons.append("output was recorded as truncated yet parses; not trusted as complete")
        report["status"] = TRUNCATED
        return report
    rows = document.get("pg_stats") if isinstance(document, dict) else None
    if not isinstance(rows, list):
        reasons.append("pg_stats is missing or not a list")
        return report
    if not rows:
        reasons.append("pg_stats is empty; an empty list is not evidence of health")
        return report
    problems = [p for p in map(row_problem, rows) if p]
    if problems:
        reasons.append("%d malformed rows, first: %s" % (len(problems), problems[0]))
        return report
    ids = [row["pgid"] for row in rows]
    if len(set(ids)) != len(ids):
        reasons.append("duplicate pgid")
        return report
    report["status"] = COMPLETE
    report["pg_count"] = len(rows)
    if expected_pgs is not None and expected_pgs != len(rows):
        report["status"] = INCONCLUSIVE
        reasons.append("expected %d PGs but the output lists %d" % (expected_pgs, len(rows)))
        return report
    unsettled = []
    for row in rows:
        report["states"][row["state"]] = report["states"].get(row["state"], 0) + 1
        if not (set(row["state"].split("+")) <= SETTLED_TOKENS and {"active", "clean"} <= set(row["state"].split("+"))):
            unsettled.append(row["pgid"])
    unsettled.sort(key=pg_key)
    for pgid in unsettled:
        pool = pgid.split(".")[0]
        report["unsettled_by_pool"][pool] = report["unsettled_by_pool"].get(pool, 0) + 1
    report["settled"] = not unsettled
    report["targets"] = unsettled[:max_targets]
    report["targets_omitted"] = len(unsettled) - len(report["targets"])
    # Mean bytes per row of this sample, framing included.
    report["rows_fitting_limit"] = limit * len(rows) // report["bytes"]
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("output", type=Path, help="retained pgs_brief output text")
    parser.add_argument("--truncated", choices=("true", "false"), required=True,
                        help="the diagnostics artifact's recorded truncation flag")
    parser.add_argument("--expected-pgs", type=int)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--max-targets", type=int, default=DEFAULT_MAX_TARGETS)
    args = parser.parse_args(argv)
    result = triage(args.output.read_text(encoding="utf-8"), args.truncated == "true",
                    args.expected_pgs, args.limit, args.max_targets)
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
