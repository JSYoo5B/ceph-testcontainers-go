#!/usr/bin/env python3
"""Offline audit of a retained finalization archive (manifest.json plus the
reports it lists).

The audit only reads an already retained directory. It does not recollect
anything and does not change any test verdict. It keeps these facts apart, per
file and for the archive:

- availability: the listed file is a regular file inside the archive root;
- producer write outcome: whether the producer recorded a write error;
- JSON readability of the retained bytes;
- size agreement with the manifest, asserted only for a successful write;
- archive completeness as the producer declared it (archive.complete);
- final cleanup status (manifest final flag and recorded termination).

A producer-declared failed write sets `bytes: 0` before any retained fragment
is known, so a fragment of such a file is preserved and hashed and reported as
a producer-declared partial file. It is never a download-corruption finding.
`final` records that the late cleanup and termination outcomes were written; it
never upgrades an archive whose producer declared it incomplete.

Each `capture_files` record (a raw packet-capture copy) is joined to its named
`files` entry. The copy is atomic: it is published only after the complete size
and SHA256 match. So `copied: true` requires the downloaded file with exactly
the copied and expected sizes and the full digest, while `copied: false` with
its producer error means no final file is expected. Its `bytes` are only staged
bytes, so an absent file is an explained omission and not evidence of upload
loss. A file that is present anyway is reported apart as an unexpected retained
file. The producer outcome, downloaded availability and archive completeness
stay independent fields. A successful copy never makes an incomplete archive
complete. Missing, null or wrong-typed fields are inconclusive and never match.
Raw captures are not JSON, so they are not parsed as JSON.
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

SCHEMA = 1
MANIFEST = "manifest.json"

# File states.
INTACT = "intact"
MISSING = "missing"
UNSAFE = "unsafe_path"
SIZE_MISMATCH = "size_mismatch"
UNREADABLE_JSON = "unreadable_json"
PRODUCER_PARTIAL = "producer_declared_partial"
PRODUCER_FAILED_ABSENT = "producer_declared_failed_absent"
DIGEST_MISMATCH = "digest_mismatch"
INCONCLUSIVE = "inconclusive"
UNEXPECTED_RETAINED = "failed_copy_file_present"
PROBLEM_STATES = {MISSING, UNSAFE, SIZE_MISMATCH, UNREADABLE_JSON, DIGEST_MISMATCH, INCONCLUSIVE,
                  UNEXPECTED_RETAINED}

# Capture record states reuse the file states and add the omission and join ones.
VERIFIED = "verified"
NOT_IN_FILES = "not_in_files"
DUPLICATE = "duplicate_name"
FULL_SHA256 = re.compile(r"[0-9a-f]{64}")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def load_manifest(root):
    path = root / MANIFEST
    if path.is_symlink() or not path.is_file():
        return None, "manifest.json is not a regular file"
    try:
        manifest = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        return None, "manifest.json is not readable JSON: %s" % type(error).__name__
    if not isinstance(manifest, dict):
        return None, "manifest.json is not a JSON object"
    return manifest, None


def valid_component(name):
    """A name the filesystem can be asked about: encodable and not overlong."""
    try:
        return len(name.encode("utf-8")) <= 255
    except UnicodeEncodeError:
        return False


def safe_name(name):
    return isinstance(name, str) and bool(name) and "/" not in name and "\\" not in name \
        and name not in (".", "..") and "\0" not in name and valid_component(name)


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def audit_file(root, entry, raw=False):
    """Audit one manifest file entry; entry must be a dict. A raw entry is not
    JSON and its readability is not asked."""
    name = entry.get("name")
    declared = entry.get("bytes")
    error = entry.get("error", "")
    record = {"name": name, "declared_bytes": declared, "producer_error": error or None,
              "producer_write": "failed" if error else "succeeded",
              "available": False, "retained_bytes": None, "sha256": None, "json_readable": None}
    if not safe_name(name) or not is_int(declared) or not isinstance(error, str):
        record["state"] = UNSAFE
        return record
    path = root / name
    try:
        if path.is_symlink() or (path.exists() and not path.is_file()):
            record["state"] = UNSAFE
            return record
        data = path.read_bytes()
    except FileNotFoundError:
        data = None
    except ValueError:
        record["state"] = UNSAFE
        return record
    except OSError:
        record["state"] = MISSING
        return record
    if data is not None:
        record["available"] = True
        record["retained_bytes"] = len(data)
        record["sha256"] = sha256(data)
        if not raw:
            try:
                json.loads(data)
                record["json_readable"] = True
            except ValueError:
                record["json_readable"] = False
    if error:
        # The producer declared this write failed. Its bytes field is not an
        # assertion about the retained file, so no size finding is made.
        record["state"] = PRODUCER_FAILED_ABSENT if data is None else PRODUCER_PARTIAL
    elif data is None:
        record["state"] = MISSING
    elif len(data) != declared:
        record["state"] = SIZE_MISMATCH
    elif record["json_readable"] is False:
        record["state"] = UNREADABLE_JSON
    else:
        record["state"] = INTACT
    return record


def capture_names(records):
    """Names of well-typed capture records, used to avoid parsing raw files as JSON."""
    return {r.get("name") for r in records if isinstance(r, dict) and safe_name(r.get("name"))}


def capture_inconclusive(record):
    """Why a capture record cannot be evaluated, or None. Nothing is defaulted:
    an absent, null or wrong-typed required field never counts as a match."""
    if not safe_name(record.get("name")):
        return "name is missing, not a string or not a safe file name"
    if not is_int(record.get("registration")):
        return "registration is missing or not an integer"
    if not isinstance(record.get("copied"), bool):
        return "copied is missing or not a boolean"
    if not is_int(record.get("bytes")) or record["bytes"] < 0:
        return "bytes is missing or not a non-negative integer"
    error = record.get("error", "")
    if not isinstance(error, str):
        return "error is not a string"
    for key in ("expected_bytes", "expected_sha256", "sha256"):
        if key in record and record[key] is None:
            return "%s is null" % key
    if "expected_bytes" in record and (not is_int(record["expected_bytes"]) or record["expected_bytes"] < 0):
        return "expected_bytes is not a non-negative integer"
    for key in ("expected_sha256", "sha256"):
        if key in record and not isinstance(record[key], str):
            return "%s is not a string" % key
    if record["copied"]:
        if error:
            return "copied is true but a producer error is recorded"
        if not is_int(record.get("expected_bytes")):
            return "a successful copy lacks expected_bytes"
        for key in ("expected_sha256", "sha256"):
            if not isinstance(record.get(key), str) or not FULL_SHA256.fullmatch(record[key]):
                return "a successful copy lacks a full lowercase %s" % key
    elif not error:
        return "copied is false without a producer error"
    return None


def join_capture(root, record, entries):
    """Join one capture record to the audit records of the files entries with
    its name. Returns the capture record; the caller maps it onto the file."""
    name = record.get("name") if isinstance(record, dict) else None
    out = {"name": name if isinstance(name, str) else None, "registration": None, "producer_copy": None,
           "producer_error": None, "reported_bytes": None, "expected_bytes": None,
           "downloaded": None, "retained_bytes": None, "sha256": None, "reason": None}
    if not isinstance(record, dict):
        out.update(state=INCONCLUSIVE, reason="capture_files record is not an object")
        return out
    if is_int(record.get("registration")):
        out["registration"] = record["registration"]
    if isinstance(record.get("copied"), bool):
        out["producer_copy"] = "copied" if record["copied"] else "failed"
    if isinstance(record.get("error"), str) and record["error"]:
        out["producer_error"] = record["error"]
    if is_int(record.get("bytes")):
        out["reported_bytes"] = record["bytes"]
    if is_int(record.get("expected_bytes")):
        out["expected_bytes"] = record["expected_bytes"]
    reason = capture_inconclusive(record)
    if reason:
        out.update(state=INCONCLUSIVE, reason=reason)
        return out
    if len(entries) == 0:
        out.update(state=NOT_IN_FILES, reason="no files entry has this name")
        return out
    if len(entries) > 1:
        out.update(state=DUPLICATE, reason="several files entries have this name")
        return out
    entry = entries[0]
    if entry["state"] == UNSAFE:
        out.update(state=INCONCLUSIVE, reason="the files entry is malformed")
        return out
    if entry["declared_bytes"] != record["bytes"] or (entry["producer_error"] or "") != record.get("error", ""):
        out.update(state=INCONCLUSIVE, reason="the files entry disagrees with the capture record")
        return out
    out["downloaded"] = entry["available"]
    out["retained_bytes"] = entry["retained_bytes"]
    out["sha256"] = entry["sha256"]
    if not record["copied"]:
        # A failed atomic copy publishes no final file. Staged bytes do not
        # make an absent file a loss.
        out["state"] = PRODUCER_FAILED_ABSENT if not entry["available"] else UNEXPECTED_RETAINED
    elif not entry["available"]:
        out["state"] = MISSING
    elif not (entry["retained_bytes"] == record["bytes"] == record["expected_bytes"]):
        out["state"] = SIZE_MISMATCH
    elif not (entry["sha256"] == record["sha256"] == record["expected_sha256"]):
        out["state"] = DIGEST_MISMATCH
    else:
        out["state"] = VERIFIED
    return out


def audit(root):
    root = Path(root)
    report = {"schema": SCHEMA, "manifest": {"readable": False}, "files": [], "capture_files": [],
              "unrecorded_capture_files": [], "unlisted_files": [],
              "archive": {}, "findings": [], "evidence_complete": False}
    if root.is_symlink() or not root.is_dir():
        report["manifest"]["error"] = "archive root is not a directory"
        return report
    manifest, problem = load_manifest(root)
    if manifest is None:
        report["manifest"]["error"] = problem
        return report
    report["manifest"] = {"readable": True, "sha256": sha256((root / MANIFEST).read_bytes())}
    entries = manifest.get("files")
    archive = manifest.get("archive")
    if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries) \
            or not isinstance(archive, dict) or not isinstance(archive.get("complete"), bool) \
            or not isinstance(manifest.get("final"), bool):
        report["manifest"].update(readable=False)
        report["manifest"]["error"] = "manifest lacks files, archive.complete or final of the expected type"
        return report
    captures = manifest.get("capture_files", [])
    capture_malformed = "capture_files" in manifest and (
        not isinstance(captures, list) or not all(isinstance(r, dict) for r in captures))
    if not isinstance(captures, list):
        captures = []
    raw_names = capture_names(captures)
    report["files"] = [audit_file(root, entry, raw=isinstance(entry.get("name"), str) and entry["name"] in raw_names) for entry in entries]
    listed = {record["name"] for record in report["files"] if isinstance(record["name"], str)}
    report["unlisted_files"] = sorted(path.name for path in root.iterdir()
                                      if path.name not in listed and path.name != MANIFEST)
    termination = manifest.get("termination")
    report["archive"] = {"declared_complete": archive["complete"], "final": manifest["final"],
                         "termination_recorded": isinstance(termination, dict),
                         "cleanup_failures": [c.get("name") for c in manifest.get("cleanups") or []
                                              if isinstance(c, dict) and c.get("error")]}
    findings = report["findings"]
    by_name = {}
    for record in report["files"]:
        if isinstance(record["name"], str):
            by_name.setdefault(record["name"], []).append(record)
    seen = {}
    for record in captures:
        if isinstance(record, dict) and isinstance(record.get("name"), str):
            seen[record["name"]] = seen.get(record["name"], 0) + 1
    for record in captures:
        name = record.get("name") if isinstance(record, dict) else None
        audited = by_name.get(name, []) if isinstance(name, str) else []
        result = join_capture(root, record, audited)
        if isinstance(name, str) and seen[name] > 1:
            result.update(state=DUPLICATE, reason="several capture_files records have this name")
        report["capture_files"].append(result)
        for entry in audited:
            # The capture join, not the generic file rules, judges a raw copy.
            entry["capture_record"] = True
            if result["state"] == VERIFIED:
                entry["state"] = INTACT
            elif result["state"] in (INCONCLUSIVE, DUPLICATE):
                entry["state"] = INCONCLUSIVE
            elif result["state"] != NOT_IN_FILES:
                entry["state"] = result["state"]
    report["unrecorded_capture_files"] = sorted(
        path.name for path in root.iterdir()
        if (path.name.endswith(".pcap") or path.name.startswith(".capture-")) and path.name not in seen)
    for result in report["capture_files"]:
        if result["state"] != VERIFIED:
            findings.append("capture %s: %s" % (result["name"], result["state"]))
    if capture_malformed:
        findings.append("manifest: capture_files is not a list of objects")
    for name in report["unrecorded_capture_files"]:
        findings.append("capture %s: retained without a capture_files record" % name)
    for record in report["files"]:
        if record.get("capture_record"):
            continue
        if record["state"] in PROBLEM_STATES:
            findings.append("%s: %s" % (record["name"], record["state"]))
        elif record["state"] in (PRODUCER_PARTIAL, PRODUCER_FAILED_ABSENT):
            findings.append("%s: %s" % (record["name"], record["state"]))
    if not archive["complete"]:
        findings.append("archive: producer declared the archive incomplete")
    if not manifest["final"]:
        findings.append("archive: manifest is not final")
    elif not report["archive"]["termination_recorded"]:
        findings.append("archive: final manifest records no termination")
    # Completeness is the producer's declaration AND every listed file intact.
    # `final` is a separate fact and only adds a requirement.
    report["evidence_complete"] = (archive["complete"] and manifest["final"]
                                   and report["archive"]["termination_recorded"]
                                   and all(r["state"] == INTACT for r in report["files"])
                                   and not capture_malformed
                                   and all(c["state"] == VERIFIED for c in report["capture_files"])
                                   and not report["unrecorded_capture_files"])
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    text = json.dumps(audit(args.directory), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
