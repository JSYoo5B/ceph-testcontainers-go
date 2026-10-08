"""Require one complete CephFS original-process fixture in its own Go log."""

import argparse
from collections import Counter
import datetime
import hashlib
import json
from pathlib import Path
import re


PARENT = "TestMultiClusterCephFSOriginalProcessQuiescence"
NETWORKS = ("bridge", "host")
KINDS = ("peer", "directory")
CASES = {"process-quiescence-" + network + "-" + kind: (network, kind)
         for network in NETWORKS for kind in KINDS}
SELECTORS = {case: "^" + PARENT + "$/^" + network + "$/^" + kind + "$"
             for case, (network, kind) in CASES.items()}
PACKAGE = "github.com/jsyoo5b/ceph-testcontainers-go/internal/integration"
STAGES = ("exited", "later-run", "container-removed")
DIRECTORY = "/daemon-a"
ORIGINAL_SNAPSHOT = "before-original-process-removal"
BACKLOG_SNAPSHOT = "after-original-process-quiescence"
BYTES_MARKER = "CEPHFS_ORIGINAL_PROCESS_BYTES "
ABSENCE_MARKER = "CEPHFS_ORIGINAL_PROCESS_ABSENCE "
EVIDENCE_MARKER = "original process evidence="
EVIDENCE = re.compile(
    r"original process evidence=(exited|later-run|container-removed) "
    r"receipt=(peer|directory) engine=(\S+) CID=([0-9a-f]{64}) "
    r"StartedAt=(\S+) GID=([1-9][0-9]*); "
    r"pending gate retained and snapshot bytes frozen$")
EXPECTED_BYTES = (ORIGINAL_SNAPSHOT + ":" + DIRECTORY + ":").encode() * 1024 + bytes(range(256))
EXPECTED_SHA256 = hashlib.sha256(EXPECTED_BYTES).hexdigest()


def required_leaf(case):
    if case not in CASES:
        raise ValueError("unknown quiescence fixture")
    return PARENT + "/" + "/".join(CASES[case])


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate native proof field")
        result[key] = value
    return result


def invalid_constant(_):
    raise ValueError("non-finite native proof number")


def valid_started_at(value):
    match = re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"(?:\.([0-9]{1,9}))?(?:Z|[+-]([0-9]{2}):([0-9]{2}))", value)
    if match is None or (match[2] is not None and (int(match[2]) > 23 or int(match[3]) > 59)):
        return False
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        # Python keeps microseconds; Go RFC3339Nano also permits the final
        # three nonzero nanoseconds. Preserve those for the zero-time check.
        return (parsed != datetime.datetime(1, 1, 1, tzinfo=datetime.timezone.utc)
                or any(digit != "0" for digit in (match[1] or "")))
    except ValueError:
        return False


def validate(log, case):
    """Validate exact own-leaf completion and its retained three-stage history.

    The original unchanged Go assertions still prove full native FSID/pool/peer,
    Docker/watchers and gate state. These log records corroborate that executed
    history; they do not independently reconstruct every native status field.
    """
    leaf = required_leaf(case)
    network, kind = CASES[case]
    expected_tests = (PARENT, PARENT + "/" + network, leaf)
    # Go's verbose reporter emits a parent before its buffered child results.
    # This is the order in the retained native log, including one-leaf selectors.
    expected_passes = expected_tests
    expected_events = [("bytes", "initial", role) for role in ("source", "destination")]
    for stage in STAGES:
        expected_events.extend(("bytes", stage, role) for role in ("source", "destination"))
        expected_events.extend((("absence", stage), ("evidence", stage)))
    runs, passes, failures, skipped, package_passes, packages = [], [], [], [], [], []
    run_positions, pass_positions, events, event_positions = {}, {}, [], []
    bytes_proofs, absence_proofs, evidence_proofs, errors = [], [], [], []
    filesystems = {}
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
        if re.match(r"^ok(?:\s|$)", line):
            packages.append((index, line))
        for marker, rows, fields in (
                (BYTES_MARKER, bytes_proofs,
                 {"case", "role", "stage", "filesystem", "directory", "snapshot", "bytes", "sha256"}),
                (ABSENCE_MARKER, absence_proofs,
                 {"case", "role", "stage", "filesystem", "directory", "snapshot", "absent", "count", "duration_ms"})):
            if marker not in line:
                continue
            try:
                proof = json.loads(line.split(marker, 1)[1], object_pairs_hook=unique_object,
                                   parse_constant=invalid_constant)
                if not isinstance(proof, dict) or set(proof) != fields:
                    raise ValueError("native proof shape differs")
                if (proof["case"] != leaf or proof["directory"] != DIRECTORY
                        or not isinstance(proof["filesystem"], str) or not proof["filesystem"]):
                    raise ValueError("native proof fixture identity differs")
                if marker == BYTES_MARKER:
                    if (proof["stage"] not in ("initial", *STAGES)
                            or proof["role"] not in ("source", "destination")
                            or proof["snapshot"] != ORIGINAL_SNAPSHOT
                            or type(proof["bytes"]) is not int
                            or proof["bytes"] != len(EXPECTED_BYTES)
                            or proof["sha256"] != EXPECTED_SHA256):
                        raise ValueError("positive original byte/hash proof differs")
                    if proof["stage"] == "initial":
                        filesystems.setdefault(proof["role"], proof["filesystem"])
                    if filesystems.get(proof["role"]) != proof["filesystem"]:
                        raise ValueError("original filesystem name changed")
                    events.append(("bytes", proof["stage"], proof["role"]))
                else:
                    if (proof["stage"] not in STAGES or proof["role"] != "destination"
                            or proof["snapshot"] != BACKLOG_SNAPSHOT or proof["absent"] is not True
                            or type(proof["count"]) is not int or proof["count"] < 2
                            or type(proof["duration_ms"]) is not int or proof["duration_ms"] < 10000
                            or filesystems.get("destination") != proof["filesystem"]):
                        raise ValueError("measured original backlog absence proof differs")
                    events.append(("absence", proof["stage"]))
                rows.append(proof)
                event_positions.append(index)
            except (ValueError, TypeError, KeyError):
                errors.append("quiescence native byte/absence proof is malformed or differs")
        if EVIDENCE_MARKER in line:
            match = EVIDENCE.search(line)
            if (match is None or match[2] != kind or not valid_started_at(match[5])
                    or len(match[6]) > 20 or int(match[6]) > (1 << 64) - 1):
                errors.append("quiescence original process evidence is malformed or differs")
            else:
                events.append(("evidence", match[1]))
                event_positions.append(index)
                evidence_proofs.append({"stage": match[1], "receipt": match[2], "engine": match[3],
                                        "CID": match[4], "StartedAt": match[5], "GID": match[6]})
    if tuple(runs) != expected_tests:
        errors.append("quiescence RUN inventory/order differs from the selected leaf")
    if tuple(passes) != expected_passes:
        errors.append("quiescence PASS inventory/order differs from the selected leaf")
    if failures:
        errors.append("quiescence log contains a failed test or package failure")
    if skipped:
        errors.append("quiescence log contains a skipped test")
    package_success = (len(package_passes) == len(packages) == 1
                       and package_passes[0] < packages[0][0]
                       and re.fullmatch(r"ok\s+" + re.escape(PACKAGE) + r"\s+[0-9.]+s", packages[0][1]) is not None)
    if not package_success:
        errors.append("quiescence log lacks exactly one completed successful Go package")
    if tuple(runs) == expected_tests and tuple(passes) == expected_passes:
        if (any(run_positions[name] >= pass_positions[name] for name in expected_tests)
                or (package_passes and max(pass_positions.values()) >= package_passes[0])):
            errors.append("quiescence tests did not finish after RUN and before package completion")
        if event_positions and (min(event_positions) <= run_positions[leaf]
                                or max(event_positions) >= min(pass_positions.values())):
            errors.append("quiescence native proof is outside the selected running fixture")
    if events != expected_events or len(bytes_proofs) != 8 or len(absence_proofs) != 3 or len(evidence_proofs) != 3:
        errors.append("quiescence native proof inventory/order lacks the complete original history")
    if evidence_proofs and len({tuple(proof[key] for key in ("receipt", "engine", "CID", "StartedAt", "GID"))
                               for proof in evidence_proofs}) != 1:
        errors.append("quiescence process evidence changed the retained original binding")
    run_counts, pass_counts = Counter(runs), Counter(passes)
    return {
        "schema": "ceph-cephfs-quiescence-coverage/v1", "parent": PARENT,
        "case": case, "network": network, "receipt_kind": kind,
        "selector": SELECTORS[case], "required_leaf": leaf,
        "required_tests": list(expected_tests), "observed_runs": runs, "observed_passes": passes,
        "counts": {name: {"run": run_counts[name], "pass": pass_counts[name]}
                   for name in sorted(set(expected_tests) | set(runs) | set(passes))},
        "byte_proofs": bytes_proofs, "absence_proofs": absence_proofs,
        "original_process_evidence": evidence_proofs,
        "failed_tests": failures, "skipped_tests": skipped,
        "package_success": package_success, "passed": not errors, "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--case", required=True, choices=CASES)
    args = parser.parse_args()
    try:
        report = validate(args.log.read_text(encoding="utf-8"), args.case)
    except (OSError, UnicodeError) as error:
        report = {"schema": "ceph-cephfs-quiescence-coverage/v1", "case": args.case,
                  "passed": False, "errors": ["quiescence log could not be read: " + str(error)]}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
