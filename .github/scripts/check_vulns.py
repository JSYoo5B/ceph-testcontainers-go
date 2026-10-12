#!/usr/bin/env python3
"""Apply the vulnerability policy to govulncheck's JSON output.

govulncheck -format json exits 0 even when it reports vulnerabilities, so
its exit status alone cannot gate CI. This reads the JSON stream, takes the
most precise level reported for each vulnerability, and fails when one is
reached at symbol level (the code calls a vulnerable function) or package
level (the code imports a vulnerable package). A vulnerability found only
in a required module is listed but does not fail. A stream without the
scanner's config, with malformed JSON or with unknown finding shapes fails,
so that a broken scan cannot pass as a clean one.
"""
import argparse
import json
from pathlib import Path
import sys

LEVELS = ("module", "package", "symbol")
FAILING = ("package", "symbol")


class ScanError(Exception):
    pass


def messages(text):
    """The JSON objects of a govulncheck stream, in order."""
    decoder = json.JSONDecoder()
    index, result = 0, []
    while True:
        while index < len(text) and text[index].isspace():
            index += 1
        if index == len(text):
            return result
        try:
            value, index = decoder.raw_decode(text, index)
        except ValueError as error:
            raise ScanError("malformed govulncheck output at offset %d: %s" % (index, error)) from error
        if not isinstance(value, dict) or len(value) != 1:
            raise ScanError("unexpected govulncheck message: %.200s" % json.dumps(value))
        result.append(value)


def level(finding):
    """The level of one finding, from its innermost trace frame."""
    trace = finding.get("trace")
    if not isinstance(trace, list) or not trace or not isinstance(trace[0], dict) or not trace[0].get("module"):
        raise ScanError("finding %s has no module trace" % finding.get("osv"))
    frame = trace[0]
    if frame.get("function"):
        return "symbol"
    if frame.get("package"):
        return "package"
    return "module"


def evaluate(text):
    """The scan's config and the most precise finding of each vulnerability."""
    config, summaries, results = None, {}, {}
    for message in messages(text):
        kind, value = next(iter(message.items()))
        if kind == "config":
            config = value
        elif kind == "osv":
            summaries[value.get("id")] = value.get("summary", "")
        elif kind == "finding":
            osv = value.get("osv")
            if not osv:
                raise ScanError("finding without a vulnerability ID")
            found = level(value)
            current = results.get(osv)
            if current is None or LEVELS.index(found) > LEVELS.index(current["level"]):
                frame = value["trace"][0]
                results[osv] = {"osv": osv, "level": found, "module": frame["module"],
                                "version": frame.get("version", ""), "fixed": value.get("fixed_version", ""),
                                "trace": [" ".join(filter(None, (f.get("package"), f.get("receiver"), f.get("function"))))
                                          or f.get("module", "") for f in value["trace"]]}
    if not config or config.get("scanner_name") != "govulncheck" or not config.get("db_last_modified"):
        raise ScanError("the stream has no govulncheck config; the scan did not complete")
    for result in results.values():
        result["summary"] = summaries.get(result["osv"], "")
    return config, sorted(results.values(), key=lambda r: (-LEVELS.index(r["level"]), r["osv"]))


def report(config, results):
    lines = ["govulncheck %s, %s, database %s, scan level %s" % (
        config.get("scanner_version"), config.get("go_version"), config.get("db_last_modified"), config.get("scan_level"))]
    for result in results:
        verdict = "FAIL" if result["level"] in FAILING else "info"
        fixed = result["fixed"] or "no fix"
        lines.append("%s %s (%s level) in %s@%s, fixed in %s: %s" % (
            verdict, result["osv"], result["level"], result["module"], result["version"], fixed, result["summary"]))
        if result["level"] == "symbol":
            lines.append("    called through " + " <- ".join(result["trace"][:4]))
    if not results:
        lines.append("no vulnerabilities")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stream", type=Path, help="govulncheck -format json output")
    args = parser.parse_args(argv)
    try:
        config, results = evaluate(args.stream.read_text())
    except (OSError, ScanError) as error:
        print("vulnerability check failed: %s" % error, file=sys.stderr)
        return 2
    print(report(config, results))
    failing = [result["osv"] for result in results if result["level"] in FAILING]
    if failing:
        print("reachable vulnerabilities: " + ", ".join(failing), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
