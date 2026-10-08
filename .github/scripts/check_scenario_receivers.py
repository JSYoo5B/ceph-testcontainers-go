"""Require the complete receiver network fixture in a retained Go test log."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re


PARENT = "TestMultiClusterRBDReceiverReadiness"
NETWORKS = ("bridge", "host")
SCOPES = ("scope-0", "scope-1", "scope-2", "scope-3", "scope-4")
SELECTORS = {network: "^" + PARENT + "$/^" + network + "$" for network in NETWORKS}
PACKAGE = "github.com/jsyoo5b/ceph-testcontainers-go/internal/integration"


def required_leaves(network):
    if network not in NETWORKS:
        raise ValueError("unknown receiver network")
    return tuple(PARENT + "/" + network + "/" + scope for scope in SCOPES)


def validate(log, network):
    """Validate one own job; the same parent may run in the other network job."""
    leaves = required_leaves(network)
    expected_runs = (PARENT, PARENT + "/" + network, *leaves)
    # Go buffers subtest results and reports their completed tree in preorder:
    # parent, network, then scopes. Completion still precedes package PASS.
    expected_passes = expected_runs
    runs, passes, failures, skipped, package_passes, package_success = [], [], [], [], [], []
    run_positions, pass_positions = {}, {}
    for index, line in enumerate(log.splitlines()):
        run = re.fullmatch(r"=== RUN\s+(\S+)", line)
        if run:
            runs.append(run[1])
            run_positions[run[1]] = index
        result = re.fullmatch(r"\s*--- (PASS|FAIL|SKIP): (\S+) \([0-9.]+s\)", line)
        if result:
            {"PASS": passes, "FAIL": failures, "SKIP": skipped}[result[1]].append(result[2])
            if result[1] == "PASS":
                pass_positions[result[2]] = index
        if re.match(r"^FAIL(?:\s|$)|^panic:", line):
            failures.append("package failure")
        if line == "PASS":
            package_passes.append(index)
        if re.fullmatch(r"ok\s+" + re.escape(PACKAGE) + r"\s+[0-9.]+s", line):
            package_success.append(index)
    errors = []
    if tuple(runs) != expected_runs:
        errors.append("receiver RUN inventory/order differs from the selected network and five scopes")
    if tuple(passes) != expected_passes:
        errors.append("receiver PASS inventory/order differs from the selected network and five scopes")
    if failures:
        errors.append("receiver log contains failed tests or a package failure")
    if skipped:
        errors.append("receiver log contains skipped tests")
    if len(package_passes) != 1 or len(package_success) != 1 or package_passes[0] >= package_success[0]:
        errors.append("receiver log lacks exactly one completed successful Go package")
    if tuple(runs) == expected_runs and tuple(passes) == expected_passes:
        if any(run_positions[name] >= pass_positions[name] for name in expected_runs) or (
                package_passes and any(pass_positions[name] >= package_passes[0]
                                       for name in expected_runs)):
            errors.append("receiver tests did not finish after their RUN and before package completion")
    run_counts, pass_counts = Counter(runs), Counter(passes)
    return {
        "schema": "ceph-rbd-receiver-coverage/v1",
        "parent": PARENT,
        "network": network,
        "selector": SELECTORS[network],
        "required_leaves": list(leaves),
        "required_tests": list(expected_runs),
        "observed_runs": runs,
        "observed_passes": passes,
        "counts": {name: {"run": run_counts[name], "pass": pass_counts[name]}
                   for name in sorted(set(expected_runs) | set(runs) | set(passes))},
        "failed_tests": failures,
        "skipped_tests": skipped,
        "package_success": len(package_passes) == len(package_success) == 1,
        "passed": not errors,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--network", required=True, choices=NETWORKS)
    args = parser.parse_args()
    try:
        report = validate(args.log.read_text(encoding="utf-8"), args.network)
    except (OSError, UnicodeError) as error:
        report = {"schema": "ceph-rbd-receiver-coverage/v1", "network": args.network,
                  "passed": False, "errors": ["receiver log could not be read: " + str(error)]}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
