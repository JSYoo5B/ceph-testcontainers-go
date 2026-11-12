#!/usr/bin/env python3
"""Expected reproduction verdicts of known-defect experiment cases.

A row of .github/reproduction-rows.json names one reproduction unit (the
case, or case/subtest, that a harness.StartReproduction call in the case's
package starts), the optional batch that runs it, the release whose pinned
role images it runs on, the platform and the expected verdict. The Go test
receives the expectation as CEPH_TEST_EXPECTED_VERDICT=<unit>=<verdict> and
compares it; the tag runner also requires the unit's REPRODUCTION_VERDICT
line, so a unit that never reports cannot satisfy its row.
"""

import argparse
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
ROWS = ROOT / ".github/reproduction-rows.json"
ROLE_IMAGES = ROOT / ".github/ceph-role-images.json"
SCHEMA = "ceph-reproduction-rows/v1"
ENV = "CEPH_TEST_EXPECTED_VERDICT"
VERDICTS = ("REPRODUCED", "NOT_REPRODUCED")
ACTUAL = VERDICTS + ("INCONCLUSIVE",)
# The same unit syntax as harness.ParseReproductionExpectation.
UNIT = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*(/[a-z0-9]+([_-][a-z0-9]+)*)?\Z")
ROW_ID = re.compile(r"[a-z0-9]+([_.-][a-z0-9]+)*\Z")
ROW_FIELDS = {"id", "unit", "batch", "release", "platform", "expected", "evidence"}
START = re.compile(r'StartReproduction\(\s*t\s*,\s*"([^"]+)"')
LINE = re.compile(r"REPRODUCTION_VERDICT case=(\S+) verdict=(\S+) expected=(\S+) attempts=(\d+) (.*)$")


def parse_expectation(value):
    """(unit, verdict) of a CEPH_TEST_EXPECTED_VERDICT value, or None."""
    if not value:
        return None
    unit, sep, verdict = value.partition("=")
    if not sep or not UNIT.match(unit) or verdict not in VERDICTS:
        raise ValueError("%s=%r: want <case>[/<subtest>]=<REPRODUCED|NOT_REPRODUCED>" % (ENV, value))
    return unit, verdict


def package_units(package, root=ROOT):
    """The reproduction units that the package's test files start."""
    directory = Path(root) / package
    units = set()
    for path in sorted(directory.glob("*_test.go")):
        units.update(START.findall(path.read_text(encoding="utf-8")))
    return units


def require_unit(expectation, package, root=ROOT):
    """Refuse an expectation for a unit that the package does not start."""
    if expectation is not None and expectation[0] not in package_units(package, root):
        raise ValueError("%s names unit %s, which %s does not start" % (ENV, expectation[0], package))


def verdict_lines(text):
    """Every REPRODUCTION_VERDICT summary line of a verbose test log."""
    lines = []
    for raw in text.splitlines():
        index = raw.find("REPRODUCTION_VERDICT ")
        if index < 0:
            continue
        match = LINE.match(raw[index:])
        if match:
            lines.append({"unit": match[1], "verdict": match[2], "expected": match[3],
                          "attempts": int(match[4]), "detail": match[5]})
    return lines


def check(text, expectation):
    """The verdicts a log reports and whether they meet the expectation.

    The expected unit must report exactly one line, with the expected
    verdict: a skip, a missing line, INCONCLUSIVE or another verdict fails."""
    lines = verdict_lines(text)
    result = {"expected": None, "verdicts": lines, "met": True, "reason": ""}
    if expectation is None:
        return result
    unit, verdict = expectation
    result["expected"] = {"unit": unit, "verdict": verdict}
    own = [line for line in lines if line["unit"] == unit]
    if not own:
        result.update(met=False, reason="no REPRODUCTION_VERDICT line of %s" % unit)
    elif len(own) != 1:
        result.update(met=False, reason="%d REPRODUCTION_VERDICT lines of %s" % (len(own), unit))
    elif own[0]["verdict"] != verdict or own[0]["expected"] != verdict:
        result.update(met=False, reason="expected %s of %s, actual %s" % (verdict, unit, own[0]["verdict"]))
    return result


def role_releases(path=ROLE_IMAGES):
    return set(json.loads(Path(path).read_text())["images"])


def validate(document, optional_plan, releases, root=ROOT):
    """The rows, after refusing any row that could not run or be compared."""
    if not isinstance(document, dict) or document.get("schema") != SCHEMA or set(document) != {"schema", "rows"}:
        raise ValueError("reproduction rows need schema %s and rows" % SCHEMA)
    rows = document["rows"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("reproduction rows: rows must be a non-empty list")
    seen, keys = set(), set()
    for row in rows:
        if not isinstance(row, dict) or not set(row) <= ROW_FIELDS or not {"id", "unit", "batch", "release", "platform", "evidence"} <= set(row):
            raise ValueError("reproduction row %r: fields must be %s, expected optional" % (row, sorted(ROW_FIELDS)))
        name = row["id"]
        if not isinstance(name, str) or not ROW_ID.match(name) or name in seen:
            raise ValueError("reproduction row %r: id must be unique and lowercase" % (name,))
        seen.add(name)
        if "expected" in row:
            parse_expectation(row["unit"] + "=" + str(row["expected"]))
        elif not UNIT.match(row["unit"]):
            raise ValueError("reproduction row %s: invalid unit %r" % (name, row["unit"]))
        profiles = [profile for profile in optional_plan["include"] if profile["batch"] == row["batch"]]
        if not profiles:
            raise ValueError("reproduction row %s: %s is not an optional batch" % (name, row["batch"]))
        if not any(row["unit"] in package_units(profile["package"], root) for profile in profiles):
            raise ValueError("reproduction row %s: batch %s starts no unit %s" % (name, row["batch"], row["unit"]))
        if row["release"] not in releases:
            raise ValueError("reproduction row %s: release %s has no pinned role images" % (name, row["release"]))
        if row["platform"] != "linux/amd64":
            raise ValueError("reproduction row %s: platform %s is not linux/amd64" % (name, row["platform"]))
        if not isinstance(row["evidence"], str) or not row["evidence"].strip():
            raise ValueError("reproduction row %s: evidence must name the native observation" % name)
        key = (row["unit"], row["release"], row["platform"])
        if key in keys:
            raise ValueError("reproduction row %s: another row runs %s on %s %s" % ((name,) + key))
        keys.add(key)
    return rows


def resolve(rows, name):
    """The workflow inputs of one row."""
    for row in rows:
        if row["id"] == name:
            expected = row["unit"] + "=" + row["expected"] if "expected" in row else ""
            return {"batch": row["batch"], "release": row["release"], "expected_verdict": expected,
                    "goarch": row["platform"].split("/", 1)[1]}
    raise ValueError("no reproduction row %s" % name)


def optional_plan(root=ROOT):
    import tag_scenarios
    catalog = tag_scenarios.load_catalog(root)
    bugs = tag_scenarios.bug_selection(tag_scenarios.load_bug_registry(), "")
    return tag_scenarios.discover_optional(catalog, bugs)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("verify")
    command = commands.add_parser("resolve")
    command.add_argument("--row", required=True)
    command.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    try:
        rows = validate(json.loads(ROWS.read_text()), optional_plan(), role_releases())
        if args.command == "verify":
            print(json.dumps({"schema": SCHEMA, "rows": len(rows)}))
            return 0
        outputs = resolve(rows, args.row)
        print(json.dumps(outputs, sort_keys=True))
        if args.github_output:
            with args.github_output.open("a", encoding="utf-8") as output:
                for key, value in sorted(outputs.items()):
                    output.write("%s=%s\n" % (key, value))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(json.dumps({"schema": SCHEMA, "errors": [str(error)]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
