"""Require one complete CephFS process-recovery fixture in its own Go log."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re

import check_scenario_quiescence as original


PARENT = "TestMultiClusterCephFSOriginalProcessQuiescenceRecovery"
CASES = {"process-recovery-" + network + "-" + kind: (network, kind)
         for network in original.NETWORKS for kind in original.KINDS}
SELECTORS = {case: "^" + PARENT + "$/^" + network + "$/^" + kind + "$"
             for case, (network, kind) in CASES.items()}
BYTES_MARKER, ABSENCE_MARKER = original.BYTES_MARKER, original.ABSENCE_MARKER
LOST_MARKER = "CEPHFS_PROCESS_QUIESCENCE_LOST_REPLY "
ACK_MARKER = "CEPHFS_PROCESS_QUIESCENCE_ACK "
REPLAY_MARKER = "CEPHFS_PROCESS_QUIESCENCE_NO_REMOVE_REPLAY "
RECOVERED_MARKER = "CEPHFS_PROCESS_QUIESCENCE_RECOVERED "
MARKERS = (BYTES_MARKER, ABSENCE_MARKER, LOST_MARKER, ACK_MARKER, REPLAY_MARKER, RECOVERED_MARKER)
SNAPSHOTS = {"initial": original.ORIGINAL_SNAPSHOT,
             **{stage: original.ORIGINAL_SNAPSHOT for stage in original.STAGES},
             "recovered-backlog": original.BACKLOG_SNAPSHOT,
             "recovered-new": "after-explicit-process-recovery",
             "retained-original": original.ORIGINAL_SNAPSHOT}
ACK_STAGES = ("exited", "later-run", "accepted", "accepted-repeat",
              "replacement-refuses-reaffirmation")
REPLAY_STAGES = ("empty-inventory", "replacement-inventory")


def required_leaf(case):
    if case not in CASES:
        raise ValueError("unknown process recovery fixture")
    return PARENT + "/" + "/".join(CASES[case])


def require(condition, message):
    if not condition:
        raise ValueError(message)


def exact_fields(proof, fields):
    require(isinstance(proof, dict) and set(proof) == set(fields), "native proof shape differs")


def integer(value, minimum=0):
    return type(value) is int and minimum <= value <= (1 << 64) - 1


def gid(value):
    return (isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None
            and int(value) <= (1 << 64) - 1)


def cid(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def expected_payload(snapshot):
    data = (snapshot + ":" + original.DIRECTORY + ":").encode() * 1024 + bytes(range(256))
    return len(data), hashlib.sha256(data).hexdigest()


def expected_events():
    events = [(BYTES_MARKER, "initial", role) for role in ("source", "destination")]
    events.append((LOST_MARKER,))
    for stage in original.STAGES:
        if stage != "container-removed":
            events.append((ACK_MARKER, stage))
        events.extend((BYTES_MARKER, stage, role) for role in ("source", "destination"))
        events.append((ABSENCE_MARKER, stage))
    events.extend(((ACK_MARKER, "accepted"), (ACK_MARKER, "accepted-repeat"),
                   (REPLAY_MARKER, "empty-inventory"),
                   (ACK_MARKER, "replacement-refuses-reaffirmation"),
                   (REPLAY_MARKER, "replacement-inventory")))
    for stage in ("recovered-backlog", "recovered-new", "retained-original"):
        events.extend((BYTES_MARKER, stage, role) for role in ("source", "destination"))
    events.append((RECOVERED_MARKER,))
    return events


def validate(log, case, pass_order="preorder"):
    """Corroborate the executed native history and exact selected leaf.

    Unchanged Go assertions remain responsible for raw Docker/task/watcher,
    source FSID/pool and checkpoint observation fields not serialized here.
    The retained log binds byte recovery, explicit ACK and process generation;
    it does not reconstruct every native assertion from terminal text.
    """
    leaf = required_leaf(case)
    if pass_order not in ("preorder", "postorder"):
        raise ValueError("unknown recovery PASS reporting order")
    network, kind = CASES[case]
    tests = [PARENT, PARENT + "/" + network, leaf]
    expected_passes = tests if pass_order == "preorder" else list(reversed(tests))
    runs, passes, failures, skipped, events, positions, errors = [], [], [], [], [], [], []
    run_positions, pass_positions, package_passes, packages = {}, {}, [], []
    proofs = {marker: [] for marker in MARKERS}
    filesystems, binding, observation_identity = {}, None, None
    for index, line in enumerate(log.splitlines()):
        run = re.fullmatch(r"=== RUN\s+(\S+)", line)
        if run:
            runs.append(run[1]); run_positions[run[1]] = index
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
        present = [marker for marker in MARKERS if marker in line]
        if not present:
            continue
        if len(present) != 1:
            errors.append("recovery line combines multiple native proofs")
            continue
        marker = present[0]
        try:
            proof = json.loads(line.split(marker, 1)[1], object_pairs_hook=original.unique_object,
                               parse_constant=original.invalid_constant)
            require(isinstance(proof, dict) and proof.get("case") == leaf,
                    "native recovery fixture identity differs")
            if marker == BYTES_MARKER:
                exact_fields(proof, ("case", "role", "stage", "filesystem", "directory", "snapshot", "bytes", "sha256"))
                stage, role = proof["stage"], proof["role"]
                require(stage in SNAPSHOTS and role in ("source", "destination")
                        and proof["directory"] == original.DIRECTORY
                        and proof["snapshot"] == SNAPSHOTS[stage]
                        and isinstance(proof["filesystem"], str) and proof["filesystem"], "byte identity differs")
                size, digest = expected_payload(proof["snapshot"])
                require(integer(proof["bytes"]) and proof["bytes"] == size and proof["sha256"] == digest,
                        "native snapshot payload differs")
                if stage == "initial":
                    filesystems.setdefault(role, proof["filesystem"])
                require(filesystems.get(role) == proof["filesystem"], "original filesystem name changed")
                event = (marker, stage, role)
            elif marker == ABSENCE_MARKER:
                exact_fields(proof, ("case", "role", "stage", "filesystem", "directory", "snapshot", "absent", "count", "duration_ms"))
                require(proof["stage"] in original.STAGES and proof["role"] == "destination"
                        and proof["directory"] == original.DIRECTORY and proof["snapshot"] == original.BACKLOG_SNAPSHOT
                        and proof["filesystem"] == filesystems.get("destination") and proof["absent"] is True
                        and integer(proof["count"], 2) and integer(proof["duration_ms"], 10000),
                        "measured backlog absence differs")
                event = (marker, proof["stage"])
            elif marker == LOST_MARKER:
                exact_fields(proof, ("case", "kind", "receipt_returned", "cause_preserved", "original_intent_lost_reply", "reply_losses", "original_removals"))
                require(proof["kind"] == kind and all(proof[key] is True for key in
                        ("receipt_returned", "cause_preserved", "original_intent_lost_reply"))
                        and integer(proof["reply_losses"]) and proof["reply_losses"] == 1
                        and integer(proof["original_removals"]) and proof["original_removals"] == 1,
                        "one real lost original removal reply differs")
                event = (marker,)
            elif marker == ACK_MARKER:
                exact_fields(proof, ("case", "stage", "acknowledged", "native_commands", "observation", "peer_removals", "directory_removals", "reply_losses"))
                stage = proof["stage"]
                require(stage in ACK_STAGES, "ACK stage differs")
                accepted = stage in ("accepted", "accepted-repeat")
                require(proof["acknowledged"] is accepted and integer(proof["native_commands"])
                        and (proof["native_commands"] > 0 if accepted else proof["native_commands"] == 0),
                        "ACK native command boundary differs")
                for name, expected in (("peer_removals", int(kind == "peer")),
                                       ("directory_removals", int(kind == "directory")), ("reply_losses", 1)):
                    require(integer(proof[name]) and proof[name] == expected, "ACK replayed original removal")
                observation = proof["observation"]
                exact_fields(observation, ("PeerID", "Directory", "SourceFilesystem", "DestinationFilesystem", "SourceFilesystemID", "DestinationFilesystemID", "PolicyRemoved", "OriginalQuiescent", "Daemons"))
                require(isinstance(observation["PeerID"], str) and observation["PeerID"]
                        and observation["Directory"] == (original.DIRECTORY if kind == "directory" else "")
                        and observation["SourceFilesystem"] == filesystems.get("source")
                        and observation["DestinationFilesystem"] == filesystems.get("destination")
                        and integer(observation["SourceFilesystemID"], 1)
                        and integer(observation["DestinationFilesystemID"], 1)
                        and observation["PolicyRemoved"] is accepted and observation["OriginalQuiescent"] is accepted,
                        "ACK original observation differs")
                identity = tuple(observation[key] for key in ("PeerID", "Directory", "SourceFilesystem", "DestinationFilesystem", "SourceFilesystemID", "DestinationFilesystemID"))
                if observation_identity is None:
                    observation_identity = identity
                require(observation_identity == identity, "ACK adopted another policy generation")
                daemons = observation["Daemons"]
                require(isinstance(daemons, dict) and len(daemons) == 1, "ACK original daemon inventory differs")
                name, daemon = next(iter(daemons.items()))
                exact_fields(daemon, ("EngineID", "ContainerID", "StartedAt", "InstanceID", "OriginalTaskEnded", "OriginalWatcherRetired", "OriginalQuiescent", "Evidence", "Problem"))
                require(isinstance(name, str) and name and isinstance(daemon["EngineID"], str) and daemon["EngineID"]
                        and cid(daemon["ContainerID"]) and isinstance(daemon["StartedAt"], str)
                        and original.valid_started_at(daemon["StartedAt"]) and gid(daemon["InstanceID"])
                        and all(daemon[key] is accepted for key in ("OriginalTaskEnded", "OriginalWatcherRetired", "OriginalQuiescent"))
                        and daemon["Evidence"] == ("container-removed" if accepted else "unobserved")
                        and daemon["Problem"] == "", "ACK original process witness differs")
                current = (name, daemon["EngineID"], daemon["ContainerID"], daemon["StartedAt"], daemon["InstanceID"])
                if binding is None:
                    binding = current
                require(binding == current, "ACK changed retained original process binding")
                event = (marker, stage)
            elif marker == REPLAY_MARKER:
                exact_fields(proof, ("case", "stage", "kind", "original_removals"))
                require(proof["stage"] in REPLAY_STAGES and proof["kind"] == kind
                        and integer(proof["original_removals"]) and proof["original_removals"] == 1,
                        "same-generation retry replayed removal")
                event = (marker, proof["stage"])
            else:
                exact_fields(proof, ("case", "kind", "original_peer", "recovered_peer", "original_cid", "replacement_cid", "original_gid", "replacement_gid", "original_removals", "reply_losses", "original_intent_lost_reply", "initial", "backlog", "fresh"))
                require(binding is not None and observation_identity is not None and proof["kind"] == kind
                        and proof["original_peer"] == observation_identity[0]
                        and isinstance(proof["recovered_peer"], str) and proof["recovered_peer"]
                        and (proof["recovered_peer"] != proof["original_peer"] if kind == "peer"
                             else proof["recovered_peer"] == proof["original_peer"])
                        and proof["original_cid"] == binding[2] and cid(proof["replacement_cid"])
                        and proof["replacement_cid"] != proof["original_cid"]
                        and proof["original_gid"] == binding[4] and gid(proof["replacement_gid"])
                        and proof["replacement_gid"] != proof["original_gid"]
                        and integer(proof["original_removals"]) and proof["original_removals"] == 1
                        and integer(proof["reply_losses"]) and proof["reply_losses"] == 1
                        and proof["original_intent_lost_reply"] is True, "recovered original/replacement binding differs")
                ids = []
                for key, snapshot in (("initial", original.ORIGINAL_SNAPSHOT), ("backlog", original.BACKLOG_SNAPSHOT), ("fresh", SNAPSHOTS["recovered-new"])):
                    exact_fields(proof[key], ("ID", "Name"))
                    require(integer(proof[key]["ID"], 1) and proof[key]["Name"] == snapshot, "recovered checkpoint identity differs")
                    ids.append(proof[key]["ID"])
                require(len(set(ids)) == 3, "recovery reused a source checkpoint")
                event = (marker,)
            events.append(event); positions.append(index); proofs[marker].append(proof)
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            errors.append("recovery native proof is malformed or differs: " + str(error))
    if runs != tests or passes != expected_passes:
        errors.append("recovery RUN/PASS inventory/order differs from the selected leaf")
    if failures or skipped:
        errors.append("recovery log contains a failed/skipped test or package failure")
    package_success = (len(package_passes) == len(packages) == 1
                       and package_passes[0] < packages[0][0]
                       and re.fullmatch(r"ok\s+" + re.escape(original.PACKAGE) + r"\s+[0-9.]+s", packages[0][1]) is not None)
    if not package_success:
        errors.append("recovery lacks exactly one completed successful Go package")
    if runs == tests and passes == expected_passes:
        if (any(run_positions[name] >= pass_positions[name] for name in tests)
                or (package_passes and max(pass_positions.values()) >= package_passes[0])):
            errors.append("recovery tests did not finish after RUN and before package completion")
        if positions and (min(positions) <= run_positions[leaf] or max(positions) >= min(pass_positions.values())):
            errors.append("recovery native proof is outside the selected running fixture")
    if events != expected_events():
        errors.append("recovery lacks the complete ordered byte/absence/ACK/replacement history")
    return {"schema": "ceph-cephfs-recovery-coverage/v1", "parent": PARENT, "case": case,
            "network": network, "receipt_kind": kind, "selector": SELECTORS[case],
            "required_leaf": leaf, "required_tests": tests, "pass_order": pass_order, "observed_runs": runs,
            "observed_passes": passes, "byte_proofs": proofs[BYTES_MARKER],
            "absence_proofs": proofs[ABSENCE_MARKER], "lost_reply_proofs": proofs[LOST_MARKER],
            "acknowledgments": proofs[ACK_MARKER], "no_remove_replay_proofs": proofs[REPLAY_MARKER],
            "recovered_proofs": proofs[RECOVERED_MARKER], "failed_tests": failures,
            "skipped_tests": skipped, "package_success": package_success,
            "passed": not errors, "errors": errors}


def validate_json(raw, case):
    """Validate actual go test -json events and their reconstructed Output.

    test2json streams terminal child PASS before network/parent PASS. The plain
    verbose CI reporter buffers children and uses preorder instead; each mode
    has one explicit order. Structured lifecycle and text must both complete.
    """
    leaf = required_leaf(case)
    tests = [PARENT, leaf.rsplit("/", 1)[0], leaf]
    runs, passes, output, errors = [], [], [], []
    run_positions, pass_positions, starts, package_passes = {}, {}, [], []
    proof_positions = []
    output_runs, output_passes, output_package_passes = {}, {}, []
    for index, line in enumerate(raw.splitlines()):
        try:
            row = json.loads(line, object_pairs_hook=original.unique_object,
                             parse_constant=original.invalid_constant)
            require(isinstance(row, dict) and {"Time", "Action", "Package"} <= set(row)
                    and set(row) <= {"Time", "Action", "Package", "Test", "Elapsed", "Output", "OutputType"}
                    and row["Package"] == original.PACKAGE
                    and isinstance(row["Time"], str) and original.valid_started_at(row["Time"]),
                    "JSON event identity/shape differs")
            action = row["Action"]
            require(action in ("start", "run", "output", "pass"), "JSON contains a failed/skipped or unexpected event")
            test = row.get("Test")
            require("Test" not in row or test in tests, "JSON event selected another test")
            if action == "output":
                require(isinstance(row.get("Output"), str) and "Elapsed" not in row
                        and ("OutputType" not in row or isinstance(row["OutputType"], str)),
                        "JSON Output shape differs")
                text = row["Output"]
                if any(marker in text for marker in MARKERS):
                    require(test == leaf, "JSON native proof belongs to another test")
                    proof_positions.append(index)
                for body in text.splitlines():
                    run = re.fullmatch(r"=== RUN\s+(\S+)", body)
                    passed = re.fullmatch(r"\s*--- PASS: (\S+) \([0-9.]+s\)", body)
                    if run:
                        require(test == run[1], "JSON RUN Output test attribution differs")
                        output_runs.setdefault(test, []).append(index)
                    if passed:
                        require(test == passed[1], "JSON PASS Output test attribution differs")
                        output_passes.setdefault(test, []).append(index)
                    if body == "PASS":
                        require(test is None, "JSON package PASS Output has a test identity")
                        output_package_passes.append(index)
                    if re.match(r"^ok(?:\s|$)", body):
                        require(test is None, "JSON package completion Output has a test identity")
                output.append(text)
            else:
                require("Output" not in row and "OutputType" not in row, "JSON lifecycle includes substituted Output")
                if action == "start":
                    require(test is None and "Elapsed" not in row, "JSON start differs")
                    starts.append(index)
                elif action == "run":
                    require(test in tests and "Elapsed" not in row, "JSON test RUN differs")
                    runs.append(test); run_positions[test] = index
                else:
                    elapsed = row.get("Elapsed")
                    require(type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0,
                            "JSON PASS elapsed differs")
                    if test is None:
                        package_passes.append(index)
                    else:
                        passes.append(test); pass_positions[test] = index
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            errors.append("recovery JSON event is malformed or differs: " + str(error))
    report = validate("".join(output), case, pass_order="postorder")
    if starts != [0] or runs != tests or passes != list(reversed(tests)) or len(package_passes) != 1:
        errors.append("recovery JSON lifecycle inventory/order differs from the selected leaf")
    if runs == tests and passes == list(reversed(tests)) and len(package_passes) == 1:
        if (any(run_positions[test] >= pass_positions[test] for test in tests)
                or max(pass_positions.values()) >= package_passes[0]
                or package_passes[0] != len(raw.splitlines()) - 1):
            errors.append("recovery JSON tests did not finish before final package PASS")
        if proof_positions and (min(proof_positions) <= run_positions[leaf]
                                or max(proof_positions) >= min(pass_positions.values())):
            errors.append("recovery JSON native proof is outside the selected running fixture")
        for test in tests:
            if (len(output_runs.get(test, [])) != 1 or len(output_passes.get(test, [])) != 1
                    or not run_positions[test] < output_runs[test][0] < output_passes[test][0] < pass_positions[test]):
                errors.append("recovery JSON lifecycle and terminal Output disagree")
        if (len(output_package_passes) != 1
                or not max(pass_positions.values()) < output_package_passes[0] < package_passes[0]):
            errors.append("recovery JSON package Output precedes complete test PASS history")
    report.update({"format": "go-json", "json_observed_runs": runs, "json_observed_passes": passes,
                   "json_package_pass_count": len(package_passes)})
    report["errors"].extend(errors)
    report["passed"] = not report["errors"]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--case", required=True, choices=CASES)
    parser.add_argument("--format", choices=("plain", "go-json"), default="plain")
    args = parser.parse_args()
    try:
        raw = args.log.read_text(encoding="utf-8")
        report = validate_json(raw, args.case) if args.format == "go-json" else validate(raw, args.case)
    except (OSError, UnicodeError) as error:
        report = {"schema": "ceph-cephfs-recovery-coverage/v1", "case": args.case,
                  "passed": False, "errors": ["recovery log could not be read: " + str(error)]}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
