#!/usr/bin/env python3
"""Verify a downloaded, deliberately FAILED failure_archive runtime artifact.

This offline check never changes the native/job verdict, recollects diagnostics
or reruns a test. Download scenario, image-preparation and cleanup artifacts from
one known CI attempt first. Independently supply that run's head and run ID.
An exit status of zero means the failed execution's archives were verified; it
never means that the native tests or workflow passed. Metadata is evidence, not
cryptographic CI attestation.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

import archive_audit
import scenario_history

TEST = "TestFailureArchiveProof"
MODES = ("assertion", "verification")


class ProofError(ValueError):
    pass


def need(value, message):
    if not value:
        raise ProofError(message)


def read(path):
    need(path.is_file() and not path.is_symlink(), "missing or unsafe evidence file: " + path.name)
    return json.loads(path.read_bytes())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify(scenario, images, cleanup, root, revision, run_id, attempt):
    scenario, images, cleanup, root = [Path(p).resolve() for p in (scenario, images, cleanup, root)]
    need(re.fullmatch(r"[0-9a-f]{40}", revision), "expected revision must be a full source commit")
    need(str(run_id).isdigit() and str(attempt).isdigit(), "expected CI run/attempt must be numeric")
    compiled = read(scenario / "compile/compile-report.json")
    native = read(scenario / "native/report.json")
    log = (scenario / "native/native.log").read_text()
    history = read(scenario / "history-row.json")
    scenario_history.validate_row(history)
    need(history["source"]["checkout_commit"] == revision, "another checkout revision")
    key = history["execution_key"]
    need(str(key["run_id"]) == str(run_id) and str(key["run_attempt"]) == str(attempt), "another CI attempt")
    need(history["outcome"]["status"] == "failed" and history["outcome"]["failed_phases"] == ["native"], "failure outside native assertions")
    need(history["comparability"]["status"] == "complete", "incomplete execution provenance")
    need(len(history["phases"]) == len(scenario_history.PHASES) and {p["phase"] for p in history["phases"]} == set(scenario_history.PHASES), "missing or duplicate workflow phase")
    need(all(p["status"] == ("failed" if p["phase"] == "native" else "passed") for p in history["phases"]), "missing or unconfirmed phase")
    need(compiled.get("passed") is True and native.get("passed") is False and native.get("exit_code") not in (None, 0), "native execution was not genuinely failed")
    need(native.get("classification") == "native-test-failure", "not a native assertion failure")
    profile = compiled["profile"]
    need(profile["category"] == "optional" and profile["batch"] == "failure_archive" and profile["bug_compatibility"] == [], "wrong optional profile")
    need(profile == native["profile"], "compile/native profile mismatch")
    binary = compiled["binary"]
    need(re.fullmatch(r"[0-9a-f]{64}", binary["sha256"]), "invalid binary identity")
    need(binary["sha256"] == native["binary"]["sha256"] and binary["build_key"] == native["binary"]["build_key"], "compile/native binary mismatch")
    need(history["build"]["native_binary"] == "matches_compile" and history["build"]["binary_sha256"] == binary["sha256"], "history binary identity mismatch")
    need(binary["tests"] == [TEST] and native["compiled_tests"] == [TEST], "wrong compiled inventory")
    need(binary["sources"] and all(isinstance(p, str) and not Path(p).is_absolute() and ".." not in Path(p).parts for p in binary["sources"]), "unsafe or absent source snapshot")
    for name, expected in binary["sources"].items():
        path = root / name
        need(path.resolve().is_relative_to(root) and path.is_file() and not path.is_symlink(), "source input is missing or unsafe")
        need(digest(path.read_bytes()) == expected, "different compiled source: " + name)
    completion = native["completion"]
    cases = {TEST, *(TEST + "/" + mode for mode in MODES)}
    need(set(completion["observed_runs"]) == cases and len(completion["observed_runs"]) == len(cases), "unreached or duplicate native case")
    need(not completion["observed_passes"] and not completion["skipped_tests"], "case passed or skipped")
    need(set(completion["failed_tests"]) == cases | {"package failure"} and len(completion["failed_tests"]) == len(cases) + 1, "unexpected or duplicate failed case")
    need(log.count("CONTROLLED_ARCHIVE_ASSERTION:") == 1, "original assertion marker missing or repeated")
    ready = [json.loads(v) for v in re.findall(r"FAILURE_ARCHIVE_PROOF_READY (\{[^\n]+\})", log)]
    need(len(ready) == 2 and {r["mode"] for r in ready} == set(MODES), "native readiness evidence missing")
    ready = {r["mode"]: r for r in ready}
    prepared = read(images / "summary.json")
    need(prepared.get("preparation_passed") is True and set(prepared["images"]) == {"essential"}, "image preparation incomplete")
    image_ids = {i["image_id"] for i in prepared["images"].values()}
    need(all(re.fullmatch(r"sha256:[0-9a-f]{64}", i) for i in image_ids), "immutable image identity missing")
    platform = binary["build_key"]["goos"] + "/" + binary["build_key"]["goarch"]
    need(prepared["platform"] == platform == history["runtime"]["platform"], "platform mismatch")
    before, after = read(cleanup / "before.json"), read(cleanup / "after.json")
    need(before.get("passed") is True and after.get("passed") is True and before["engine"] == after["engine"], "cleanup engine/result mismatch")
    need(after.get("new_resources") == {"containers": [], "networks": [], "volumes": []}, "new owned resources remain or are unobserved")
    manifests = sorted((scenario / "native/test-artifacts").rglob("manifest.json"))
    need(len(manifests) == 2, "expected two separately retained failure archives")
    verified = []
    executions = set()
    listed = {item["path"]: item["bytes"] for item in native["test_artifacts"]["files"]}
    for path in manifests:
        m = read(path)
        mode = m["test"].removeprefix(TEST + "/")
        need(mode in MODES and m["scenario"] == "failure-archive-proof/" + mode, "wrong archive case/scenario")
        need(m["phase"] == "before-cleanup" and m["final"] is True and m["archive"]["retained"] is True and m["archive"]["cluster_closed"] is False, "wrong archive lifecycle boundary")
        execution = m["execution"]
        need(str(execution["run_id"]) == str(run_id) and str(execution["run_attempt"]) == str(attempt), "archive belongs to another CI attempt")
        need(execution["platform"] == platform, "archive platform mismatch")
        need(execution.get("revision") in (None, "", revision), "archive source revision mismatch")
        need(execution.get("revision") or "source revision" in m["missing_provenance"], "unknown source was not declared missing")
        need(execution["id"] not in executions, "archive execution identity overwritten")
        executions.add(execution["id"])
        need(m["trigger"]["test_failed"] is (mode == "assertion") and m["trigger"]["check_failed"] is (mode == "verification"), "original failure trigger was replaced")
        need(not m["trigger"]["cleanup_failed"] and not m["trigger"]["cleanup_reported_failure"], "late cleanup failed")
        need(m.get("termination") is not None and not m["termination"].get("error"), "termination unconfirmed")
        need(len(m["cleanups"]) == 1 and m["cleanups"][0]["name"] == "restore controlled debug window" and not m["cleanups"][0].get("error"), "debug cleanup unconfirmed")
        audit = archive_audit.audit(path.parent)
        need(audit["evidence_complete"] and not audit["findings"], "retained report write/availability failure")
        for file in (path, *(path.parent / r["name"] for r in audit["files"])):
            name = file.relative_to(scenario / "native/test-artifacts").as_posix()
            need(listed.get(name) == file.stat().st_size, "downloaded archive differs from native file inventory")
        diagnostics = read(path.parent / "diagnostics.json")
        crash = read(path.parent / "crash-metadata.json")
        need(diagnostics["complete"] is True and crash["complete"] is True, "incomplete native collection")
        inspected = [json.loads(a["data"]) for a in diagnostics["artifacts"] if a["kind"] == "container-inspect"]
        need(inspected and {i["image_id"] for i in inspected} == image_ids, "diagnostic image cohort mismatch")
        status = [json.loads(a["data"]) for a in diagnostics["artifacts"] if a["kind"] == "ceph" and a["name"] == "status"]
        need(len(status) == 1 and status[0]["fsid"] == ready[mode]["fsid"], "native FSID differs from the archived cluster")
        need({i["image_id"] for i in crash["containers"]} == image_ids, "crash image cohort mismatch")
        need(ready[mode]["osd_container"] in {i["id"] for i in inspected} and ready[mode]["osds"] == [0] and ready[mode]["ceph_version"] == prepared["release"], "native readiness identity mismatch")
        windows = m["debug_windows"]
        need(len(windows) == 1 and not m.get("debug_windows_omitted"), "debug invocation missing or omitted")
        window = windows[0]
        archive, final = window["archive"], window["final"]
        need(window["sequence"] == 1 and archive["present"] is True and final["present"] is True, "debug snapshots absent")
        a, f = archive["receipt"], final["receipt"]
        need(archive["phase"] == "before-cleanup" and final["phase"] == "after-cleanup" and a["state"] == "applied-verified" and f["state"] == "restored-verified", "debug snapshots aliased or restoration unverified")
        need(a["generation"] == f["generation"] and a["prior"] == f["prior"] == f["restored"] and a["applied"] == "1/1", "debug generation/prior value changed")
        target = next(i for i in inspected if i["id"] == ready[mode]["osd_container"])
        need(a["generation"] == target["id"] + "@" + target["state"]["started_at"] and a["daemon"] == f["daemon"] == "osd.0" and a["name"] == f["name"] == "debug_osd", "debug receipt names another native target generation")
        if mode == "verification":
            v = read(path.parent / "verification-1-absent-crash-control.json")
            need(v["outcome"] == "unexpected" and v["crashes"]["outcome"] == "unexpected", "post-check failure missing")
            need(any(e["name"] == "required-but-absent" and e["satisfied"] is False for e in v["crashes"]["expectations"]), "required-absence negative control missing")
        verified.append({"test": m["test"], "execution": execution["id"], "archive_manifest_sha256": digest(path.read_bytes()),
                         "missing_manifest_provenance": m["missing_provenance"], "files": audit["files"]})
    need({item["test"] for item in verified} == {TEST + "/" + mode for mode in MODES}, "duplicate archive case")
    return {"schema": 1, "archive_verification": "verified", "native_outcome": "failed", "workflow_pass_claimed": False,
            "revision": revision, "run_id": str(run_id), "run_attempt": str(attempt), "binary_sha256": binary["sha256"],
            "source_files_verified": len(binary["sources"]), "images": sorted(image_ids), "platform": platform, "archives": verified}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("scenario", "images", "cleanup", "root"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--revision", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--attempt", required=True)
    p.add_argument("--output", type=Path)
    args = vars(p.parse_args(argv))
    output = args.pop("output")
    try:
        report = verify(**args)
        text = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if output:
            output.write_text(text)
        else:
            print(text, end="")
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("failed archive proof: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
