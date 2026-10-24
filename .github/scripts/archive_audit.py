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
"""
import argparse
import hashlib
import json
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
PROBLEM_STATES = {MISSING, UNSAFE, SIZE_MISMATCH, UNREADABLE_JSON}


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


def audit_file(root, entry):
    """Audit one manifest file entry; entry must be a dict."""
    name = entry.get("name")
    declared = entry.get("bytes")
    error = entry.get("error", "")
    record = {"name": name, "declared_bytes": declared, "producer_error": error or None,
              "producer_write": "failed" if error else "succeeded",
              "available": False, "retained_bytes": None, "sha256": None, "json_readable": None}
    if not isinstance(name, str) or not name or "/" in name or "\\" in name or name in (".", "..") \
            or not isinstance(declared, int) or isinstance(declared, bool) or not isinstance(error, str):
        record["state"] = UNSAFE
        return record
    path = root / name
    if path.is_symlink() or (path.exists() and not path.is_file()):
        record["state"] = UNSAFE
        return record
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        data = None
    except OSError:
        record["state"] = MISSING
        return record
    if data is not None:
        record["available"] = True
        record["retained_bytes"] = len(data)
        record["sha256"] = sha256(data)
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
    elif not record["json_readable"]:
        record["state"] = UNREADABLE_JSON
    else:
        record["state"] = INTACT
    return record


def audit(root):
    root = Path(root)
    report = {"schema": SCHEMA, "manifest": {"readable": False}, "files": [], "unlisted_files": [],
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
    report["files"] = [audit_file(root, entry) for entry in entries]
    listed = {record["name"] for record in report["files"]}
    report["unlisted_files"] = sorted(path.name for path in root.iterdir()
                                      if path.name not in listed and path.name != MANIFEST)
    termination = manifest.get("termination")
    report["archive"] = {"declared_complete": archive["complete"], "final": manifest["final"],
                         "termination_recorded": isinstance(termination, dict),
                         "cleanup_failures": [c.get("name") for c in manifest.get("cleanups") or []
                                              if isinstance(c, dict) and c.get("error")]}
    findings = report["findings"]
    for record in report["files"]:
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
                                   and all(r["state"] == INTACT for r in report["files"]))
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
