#!/usr/bin/env python3
r"""Join pinned declarations, requirements and post-cleanup native case receipts.

A receipt is native-state evidence, never proof of consumer implementation or
per-message wire coverage. Inputs are local source and a complete go test -json
stream. This validator performs no network access and never retries a mutation.

Scope: of the 357 reference declarations, MSG_COMMAND and MSG_COMMAND_REPLY map
to targeted-command/osd-pg-mds (TestTargetedCommandRecipe); all others stay
explicitly unmapped. A mapping, an available recipe or a passing parser is not
native execution, wire coverage or consumer implementation.

Binding catalogue (internal/integration/testdata/messenger_evidence.json,
schema ceph-messenger-evidence/v2; a v1 catalogue is refused). Each binding has:
  - (kind, symbol) declarations and a requirement ID;
  - an exact top-level test selector and a repository-relative source;
  - a code-owned assertion contract and parent/case receipt markers;
  - evidence class (only native-state; never elevated to a wire claim);
  - execution context: roles, plane, authentication, placement and path.
Rules:
  - one binding per exact top-level Go test; subtest/regexp selectors and
    multiple records per test are refused;
  - several declarations in one binding only when its contract supports every
    edge; sharing a parent PASS is insufficient;
  - numeric aliases are never merged across namespaces;
  - unknown declarations, unrelated requirement symbols, missing cases, stale
    markers, duplicate links/IDs and a changed Ceph reference are errors;
  - source must be a canonical, nonsymlink internal/integration/*_test.go file
    whose test is registered by the AST indexer (see the package doc of
    internal/tools/messengercases). That helper is trusted validator source;
    --root selects data to inspect, not Go code to run.
Contracts are a closed Python dispatch. Only targeted-command/v1 exists: the
OSD/PG/MDS generation, placement and refusal validator, limited to the
MSG_COMMAND/MSG_COMMAND_REPLY edges. JSON cannot import or invent a contract;
a new declaration family needs a reviewed validator first.

Runtime receipt (ceph-messenger-case/v1). TestTargetedCommandRecipe keeps its
TARGETED_COMMAND_RECEIPT parent marker and registers before creating resources.
Its MSGR_CASE_RECEIPT is emitted only by the last cleanup callback, after owned
cleanup; any test or cleanup failure suppresses it. It records:
  - binding/case, unique run ID and cluster generation (FSID);
  - exact parent SHA-256;
  - source snapshot: SHA-256 of the embedded reference, coverage and binding
    JSON, plus one aggregate over internal/integration/*.go and one over
    internal/integration/harness/*.go (the shared code the cases call), each
    computed as SHA-256 of sorted rows "path NUL file-sha256 newline"
    (inclusion does not mean all build-tag alternatives ran);
  - native Ceph version, Go build and the inspected immutable image IDs of
    every executor container (cluster/daemon, client toolkit, limited client;
    an aggregate image once). Inspection failure blocks the receipt; an unused
    client is never started for metadata.
Source digests bind the exact catalogue bytes, so an older receipt never counts
as current-source execution. JSON validation is not CI attestation; keep the CI
head/attempt or local build record with the report.

Cohort file (--expected-cohort), declared before execution from engine-resolved
images, never derived from a receipt:
    {"release": "20.2.4", "platform": "linux/amd64",
     "images": ["sha256:<64 hex>", ...]}
Images are sorted and unique. Mismatched IDs, release, platform or pinned native
build are refused. Only the manifest-pinned 20.2.4 cohort is supported.

Usage (Go 1.26+ and Python):
    # Source mapping only; every case stays unexecuted.
    python3 .github/scripts/messenger_evidence.py --output mapping.json

    # Run the case on an authorized Linux runner with the selected images.
    go test -tags=all -count=1 -timeout=25m -json \
      -run '^TestTargetedCommandRecipe$' ./internal/integration > native-events.jsonl
    python3 .github/scripts/messenger_evidence.py \
      --events native-events.jsonl --expected-cohort cohort.json \
      --require-execution --output evidence.json

    python3 -m unittest discover -s .github/scripts -p test_messenger_evidence.py
A retained verbose log may be converted with go tool test2json -p <package>;
keep its exit status and log. Never rerun an uncertain mutation to obtain a
receipt.

The validator checks:
  - duplicate JSON keys, source identity and complete, SHA-256-bound markers;
  - unique execution with a pending/running/finished lifecycle: output must
    follow the unique run and precede terminal pass/skip; split parent and case
    chunks are reassembled only within that interval; exactly one test pass and
    package pass are required;
  - matching FSID/OSD UUIDs, PG/acting and MDS generation, native/auth refusal
    outcomes and cleanup;
  - a skipped case never supplies a receipt; missing, repeated, stale,
    truncated or mixed evidence fails; streams over 64 MiB are refused;
    unknown case receipts are never counted as another case.
Reports keep unexecuted_bindings when nothing ran, always list unmapped
declarations, and set wire and consumer implementation claims to false. The
unit tests are synthetic offline controls, not stored native receipts.

Scenario history join (scenario_history.py). Tagged-runtime normalization adds a
messenger field when the compiled inventory or retained log selects a mapped
case; other profiles record not_selected without indexing or conversion. It runs
no native command: trusted go tool test2json converts the saved log (30 s limit,
64 MiB input/output bounds), then this validator checks the events. Acceptance
requires:
  - successful job phases and owned cleanup;
  - matching compile/native binary and build key, checked-out source file
    hashes and the case's embedded source snapshot;
  - each accepted selector in the validated nonempty, unique compiled test
    inventory;
  - receipt completion and any strict-completion check passing, exit code zero
    and terminal classification passed; strict_completion is always recorded
    (null when not required); timed_out, if supplied, must be false; missing
    terminal fields are not success, and a valid log cannot override
    contradictory native metadata;
  - release, platform and image set from the separately saved pre-native image
    preparation summary, with compiled Go and image platforms agreeing.
States:
  - accepted: validated native-state cases;
  - unexecuted: failed, canceled, skipped or incomplete jobs (mapping kept);
  - unsupported_cohort: unsupported pinned release;
  - invalid: inconsistent or missing case/source/build/cohort evidence;
  - not_recorded: older rows without the field (never backfilled).
A passing scenario never makes a missing receipt accepted, and case states never
rewrite the scenario outcome or failure category. Summaries count attempts by
case-evidence status, not unique declaration coverage or a new release/platform
qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import uuid

BASE = Path(__file__).resolve().parents[2]
REFERENCE = "internal/integration/testdata/messenger_reference.json"
COVERAGE = "internal/integration/testdata/messenger_coverage.json"
BINDINGS = "internal/integration/testdata/messenger_evidence.json"
EMITTER = "internal/integration/messenger_case_evidence_integration_helpers_test.go"
# The case package and the harness it calls; both are compiled into a receipt.
SOURCE_DIRECTORIES = ("internal/integration", "internal/integration/harness")
SOURCES = (REFERENCE, COVERAGE, BINDINGS) + tuple(str(p.relative_to(BASE)) for directory in SOURCE_DIRECTORIES
                                                  for p in sorted((BASE / directory).glob("*.go")))
PACKAGE = "github.com/JSYoo5B/ceph-testcontainers-go/internal/integration"


class EvidenceError(ValueError):
    pass


def need(condition, message):
    if not condition:
        raise EvidenceError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def loads(data):
    try:
        return json.loads(data, object_pairs_hook=unique_object,
                          parse_constant=lambda x: (_ for _ in ()).throw(EvidenceError("non-finite JSON: " + x)))
    except (ValueError, TypeError) as exc:
        raise EvidenceError(str(exc)) from exc


def digest(data):
    return hashlib.sha256(data).hexdigest()


def source_digests(root):
    result = {p: digest((root / p).read_bytes()) for p in (REFERENCE, COVERAGE, BINDINGS)}
    for directory in SOURCE_DIRECTORIES:
        paths = sorted((root / directory).glob("*.go"))
        need(paths, "missing integration source snapshot of " + directory)
        snapshot = "".join(str(p.relative_to(root)) + "\0" + digest(p.read_bytes()) + "\n" for p in paths)
        result[directory + "/*.go"] = digest(snapshot.encode())
    return result


def fields(obj, names):
    need(isinstance(obj, dict) and set(obj) == set(names.split()), "missing or unknown fields: " + names)


def positive(value):
    return type(value) is int and value > 0


def valid_uuid(value):
    try:
        return isinstance(value, str) and uuid.UUID(value).int != 0 and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def case_index(root):
    # Run only this validator's trusted, standard-library AST reader. The
    # inspected root is data; none of its Go packages are imported or run.
    env = dict(os.environ, GOWORK="off", GOFLAGS="-mod=readonly")
    try:
        result = subprocess.run(["go", "run", str(BASE / "internal/tools/messengercases/main.go"), str(root.resolve())],
                                cwd=BASE, env=env, capture_output=True, text=True, timeout=90)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise EvidenceError("cannot index Go test declarations: " + str(exc)) from exc
    need(result.returncode == 0, "cannot index Go test declarations: " + result.stderr.strip())
    return loads(result.stdout)


def binding_source(root, binding, cases):
    source, case = binding["source"], binding["case"]
    need(isinstance(source, str) and "\\" not in source, "invalid case source path")
    path = PurePosixPath(source)
    need(str(path) == source and len(path.parts) == 3 and path.parts[:2] == ("internal", "integration")
         and path.name.endswith("_test.go"), "case source must be a canonical integration test path")
    selected = root / source
    need(selected.is_file() and not selected.is_symlink()
         and selected.resolve().is_relative_to(root.resolve()), "case source missing or outside repository")
    need(isinstance(case, str) and re.fullmatch(r"Test[A-Za-z0-9_]+", case), "only exact top-level test selectors are supported")
    definitions = [entry for entry in cases if entry["name"] == case]
    need(len(definitions) == 1 and definitions[0]["source"] == source, "case source missing, stale or ambiguous")
    need(binding["parent_marker"] in definitions[0]["markers"], "parent marker is not a literal log call in the selected case")


def catalogue(root=BASE):
    reference, coverage, bindings = [loads((root / p).read_bytes()) for p in (REFERENCE, COVERAGE, BINDINGS)]
    fields(bindings, "schema ceph_commit bindings")
    need(bindings["schema"] == "ceph-messenger-evidence/v2", "unsupported bindings schema")
    need(reference["ceph_commit"] == coverage["ceph_commit"] == bindings["ceph_commit"], "stale Ceph reference")
    declarations = {(e["kind"], e["symbol"]) for e in reference["entries"]}
    need(len(declarations) == len(reference["entries"]), "duplicate source declaration")
    rows = {r["id"]: r for r in coverage["rows"]}
    need(len(rows) == len(coverage["rows"]), "duplicate requirement")
    indexed, selected_cases = {}, set()
    cases = case_index(root)
    for binding in bindings["bindings"]:
        fields(binding, "id declarations requirement case source contract parent_marker marker evidence context")
        ident = binding["id"]
        need(isinstance(ident, str) and ident and ident not in indexed, "duplicate or empty binding")
        row = rows.get(binding["requirement"])
        need(row is not None, "unknown requirement")
        need(any(c["test"] == binding["case"] and c.get("receipt") == binding["parent_marker"] for c in row["cases"]), "stale case or parent marker")
        need(binding["evidence"] == "native-state", "this first schema supports native-state evidence only")
        need(binding["marker"] == "MSGR_CASE_RECEIPT", "unsupported case marker")
        need(isinstance(binding["contract"], str) and binding["contract"] in CONTRACTS, "unknown assertion contract")
        contract = CONTRACTS[binding["contract"]]
        need(binding["parent_marker"] == contract["parent_marker"], "parent marker belongs to another assertion contract")
        need(isinstance(binding["case"], str), "invalid Go case selector")
        need(binding["case"] not in selected_cases, "only one binding per Go case is supported")
        fields(binding["context"], "roles plane authentication placement path")
        need(all(binding["context"].values()), "missing execution context")
        keys = []
        for declaration in binding["declarations"]:
            fields(declaration, "kind symbol")
            key = (declaration["kind"], declaration["symbol"])
            need(key in declarations and key[1] in row["symbols"], "unknown or unrelated declaration")
            need(key in contract["declarations"], "declaration is not supported by the assertion contract")
            keys.append(key)
        need(keys and len(keys) == len(set(keys)), "empty or duplicate declaration link")
        binding_source(root, binding, cases)
        need(binding["marker"] in (root / EMITTER).read_text(), "case marker source missing")
        indexed[ident] = binding
        selected_cases.add(binding["case"])
    need(indexed, "no bindings")
    return reference, declarations, indexed


def validate_parent(parent, generation):
    need(valid_uuid(generation) and parent.get("fsid") == generation, "mixed cluster generation")
    need(positive(parent.get("osdmap_epoch")), "missing OSDMap epoch")
    need(re.fullmatch(r"[0-9]+\.[0-9a-f]+", parent.get("pgid", "")), "invalid PG identity")
    acting = parent.get("acting")
    need(isinstance(acting, list) and acting and all(type(i) is int and i >= 0 for i in acting) and len(acting) == len(set(acting)), "invalid acting set")
    pg = parent.get("pg", {})
    need(pg.get("pgid") == parent["pgid"] and pg.get("acting") == acting and positive(pg.get("epoch")), "mixed placement receipt")
    for osd in acting:
        native = parent.get("osd." + str(osd), {})
        need(native.get("cluster_fsid") == generation and native.get("whoami") == osd and valid_uuid(native.get("osd_fsid")) and positive(native.get("newest_map")), "mixed OSD identity")
    mds = parent.get("mds", {})
    need(mds.get("cluster_fsid") == generation and positive(mds.get("id")) and positive(mds.get("mdsmap_epoch")) and mds.get("state") == "up:active" and mds.get("fs_name"), "invalid MDS generation")
    need(parent.get("client_refusals") == {"osd.99": -2, "pg 99.0": -2}, "missing client refusals")
    need(parent.get("unknown_command", {}).get("ret") == -22 and parent.get("paused", {}).get("ret") == -110 and parent.get("stopped", {}).get("ret") == -6, "missing native failure outcomes")
    denied = parent.get("server_refusals", {})
    need(set(denied.get("librados", {})) == {"osd0", "pg"} and all(r.get("ret") == -13 for r in denied["librados"].values()), "missing librados auth refusals")
    need(len(denied.get("cli", [])) == 2 and all(r.get("exit") == 13 for r in denied["cli"]), "missing CLI auth refusals")


# This reviewed dispatch is code-owned, never an import or callable from JSON.
# New families need their own validator and explicit declaration edges.
CONTRACTS = {"targeted-command/v1": {
    "parent_marker": "TARGETED_COMMAND_RECEIPT",
    "declarations": {("message", "MSG_COMMAND"), ("message", "MSG_COMMAND_REPLY")},
    "validate": validate_parent,
}}


def validate_stream(raw, root, reference, bindings, expected):
    need(len(raw) <= 64 * 1024 * 1024, "event stream exceeds 64 MiB")
    fields(expected, "release platform images")
    need(expected["release"] == reference["ceph_release"], "unsupported release cohort")
    output, runs, passes = {}, {}, {}
    package_pass = 0
    finished = set()
    lifecycle = {binding["case"]: "pending" for binding in bindings.values()}
    for line in raw.splitlines():
        event = loads(line)
        need(isinstance(event, dict), "event must be an object")
        need(event.get("Action") not in {"fail", "build-fail"}, "failed test/build stream")
        if event.get("Package") != PACKAGE:
            continue
        need(package_pass == 0, "events after package completion")
        case, action = event.get("Test"), event.get("Action")
        need(case not in finished or action not in {"output", "run"}, "events after test completion")
        if case in lifecycle:
            # Bind all chunks to this unique execution window, including chunks
            # without a complete marker. Later run/pass counts cannot make
            # pre-run bytes into evidence from the actual execution.
            if action == "run":
                need(lifecycle[case] == "pending", "bound test did not run exactly once")
                lifecycle[case] = "running"
            else:
                need(lifecycle[case] == "running", "event outside bound test execution")
                if action in {"pass", "skip"}:
                    lifecycle[case] = "finished"
        if action == "run":
            runs[case] = runs.get(case, 0) + 1
        if action == "output":
            output[case] = output.get(case, "") + event.get("Output", "")
        if action == "pass":
            if case:
                passes[case] = passes.get(case, 0) + 1
                finished.add(case)
            else:
                package_pass += 1
    need(package_pass == 1, "missing or duplicate package completion")
    expected_sources = source_digests(root)
    accepted, run_ids = [], set()
    for ident, binding in bindings.items():
        case = binding["case"]
        if case not in runs:
            continue
        need(runs[case] == passes.get(case) == 1, "case did not pass exactly once")
        lines = output.get(case, "").splitlines()
        def marker_values(marker):
            pattern = re.compile(r"(?:^|\s)" + re.escape(marker) + r" (.*)$")
            return [m.group(1) for line in lines if (m := pattern.search(line))]
        parents, receipts = marker_values(binding["parent_marker"]), marker_values(binding["marker"])
        need(len(parents) == len(receipts) == 1, "missing/duplicate/truncated case receipts")
        parent, receipt = loads(parents[0]), loads(receipts[0])
        fields(receipt, "schema binding case run_id generation parent_sha256 sources cohort cleanup")
        need(receipt["schema"] == "ceph-messenger-case/v1" and receipt["binding"] == ident and receipt["case"] == case, "wrong receipt identity")
        need(valid_uuid(receipt["run_id"]) and receipt["run_id"] not in run_ids, "duplicate/invalid run ID")
        run_ids.add(receipt["run_id"])
        need(receipt["sources"] == expected_sources, "stale compiled source or inventory")
        need(receipt["cleanup"] == "passed", "cleanup incomplete")
        need(receipt["parent_sha256"] == digest(parents[0].encode()), "mixed or truncated parent")
        CONTRACTS[binding["contract"]]["validate"](parent, receipt["generation"])
        cohort = receipt["cohort"]
        fields(cohort, "release platform images ceph_version go_version")
        need(all(cohort[k] == expected[k] for k in expected), "unexpected image/release/platform cohort")
        need(cohort["images"] and cohort["images"] == sorted(set(cohort["images"])) and all(re.fullmatch(r"sha256:[0-9a-f]{64}", i) for i in cohort["images"]), "invalid immutable image IDs")
        need(cohort["ceph_version"].startswith("ceph version " + expected["release"] + " ") and "(" + reference["ceph_commit"] + ")" in cohort["ceph_version"] and cohort["go_version"].startswith("go"), "unexpected native build")
        # A post-cleanup marker must follow the assertion receipt.
        need(output[case].index(binding["parent_marker"] + " ") < output[case].index(binding["marker"] + " "), "case receipt preceded assertions")
        accepted.append({"binding": ident, "case": case, "source": binding["source"], "contract": binding["contract"],
                         "run_id": receipt["run_id"], "generation": receipt["generation"], "evidence": binding["evidence"], "cohort": cohort})
    # Unknown case receipts may not be silently discarded as unrelated output.
    markers = sum(text.count("MSGR_CASE_RECEIPT ") for text in output.values())
    need(markers == len(accepted), "unmatched case receipts")
    return accepted


def report(root=BASE, events=None, expected=None):
    reference, declarations, bindings = catalogue(root)
    mapped = {(d["kind"], d["symbol"]) for b in bindings.values() for d in b["declarations"]}
    accepted = validate_stream(events, root, reference, bindings, expected) if events is not None else []
    return {"schema": "ceph-messenger-evidence-report/v1", "ceph_commit": reference["ceph_commit"],
            "source_declarations": len(declarations), "mapped_declarations": len(mapped),
            "unmapped_declarations": [{"kind": k, "symbol": s} for k, s in sorted(declarations - mapped)],
            "unexecuted_bindings": sorted(set(bindings) - {r["binding"] for r in accepted}), "native_cases": accepted,
            "wire_coverage_claimed": False, "consumer_implementation_claimed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=BASE)
    parser.add_argument("--events", type=Path, help="complete go test -json output")
    parser.add_argument("--expected-cohort", type=Path, help="predeclared release/platform/immutable image IDs")
    parser.add_argument("--require-execution", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        need(bool(args.events) == bool(args.expected_cohort), "events and expected-cohort are required together")
        result = report(args.root, args.events.read_bytes() if args.events else None,
                        loads(args.expected_cohort.read_bytes()) if args.expected_cohort else None)
        need(not args.require_execution or result["native_cases"], "no native execution accepted")
        encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(encoded)
        else:
            print(encoded, end="")
        return 0
    except (EvidenceError, OSError, KeyError, TypeError, AttributeError) as exc:
        print("messenger evidence: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
