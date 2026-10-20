#!/usr/bin/env python3
"""Extract the Messenger reference declarations of a pinned Ceph commit.

The Messenger coverage manifest (internal/integration/testdata/
messenger_coverage.json) maps requirements to the OSD operations, message
types and feature bits that Ceph declares. This script reads those
declarations from the Ceph headers and writes them, with their source line,
to internal/integration/testdata/messenger_reference.json, which the
manifest's validator reads offline.

Each kind keeps its own namespace and every declaration stays a separate
entry: feature names that share a bit and incarnation, retired names and
message types that share a value are not merged. Only declarations with a
literal value are extracted; the script fails on a declaration line that it
cannot parse, so a changed header cannot silently shrink the inventory.

    messenger_reference.py generate --source DIR --commit SHA --release X.Y.Z --output FILE
    messenger_reference.py check --source DIR --reference FILE

DIR is a Ceph checkout (or a directory with the same relative paths) at the
pinned commit. Neither command uses the network.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

SOURCES = {
    "osd_op": ["src/include/rados.h"],
    "message": ["src/include/ceph_fs.h", "src/msg/Message.h"],
    "feature": ["src/include/ceph_features.h"],
    "msgr2_feature": ["src/include/msgr.h"],
}

OSD_OP = re.compile(r'^\s*f\(\s*(\w+)\s*,\s*__CEPH_OSD_OP(1?)\(([^)]*)\)\s*,\s*"([^"]+)"\s*\)')
OSD_OP_LINE = re.compile(r'^\s*f\(')
MESSAGE = re.compile(r'^\s*#define\s+((?:CEPH_)?MSG_\w+)\s+(0x[0-9a-fA-F]+|\d+)\s*(?:/[/*].*)?$')
MESSAGE_LINE = re.compile(r'^\s*#define\s+(?:CEPH_)?MSG_\w+\s')
FEATURE = re.compile(r'^DEFINE_CEPH_FEATURE(_DEPRECATED|_RETIRED)?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\w+)\s*((?:,\s*\w+\s*)*)\)\s*;?\s*(?://.*)?$')
FEATURE_LINE = re.compile(r'^DEFINE_CEPH_FEATURE')
MSGR2_FEATURE = re.compile(r'^DEFINE_MSGR2_FEATURE\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\w+)\s*\)\s*;?\s*(?://.*)?$')
MSGR2_FEATURE_LINE = re.compile(r'^DEFINE_MSGR2_FEATURE\(')

# Message definitions without a numeric type, such as CRC flags.
MESSAGE_FLAGS = ("MSG_CRC_",)


class ReferenceError(Exception):
    pass


def osd_ops(path, text):
    result = []
    for number, line in enumerate(text.splitlines(), 1):
        if not OSD_OP_LINE.match(line):
            continue
        match = OSD_OP.match(line)
        if not match:
            raise ReferenceError("%s:%d: unparsed OSD operation: %s" % (path, number, line.strip()))
        name, single, args, wire = match.groups()
        parts = [part.strip() for part in args.split(",")]
        if single:
            if len(parts) != 2:
                raise ReferenceError("%s:%d: unparsed __CEPH_OSD_OP1: %s" % (path, number, line.strip()))
            mode, kind, code = parts[0], "", parts[1]
        else:
            if len(parts) != 3:
                raise ReferenceError("%s:%d: unparsed __CEPH_OSD_OP: %s" % (path, number, line.strip()))
            mode, kind, code = parts
        result.append({"kind": "osd_op", "symbol": "CEPH_OSD_OP_" + name, "source": path, "line": number,
                       "mode": mode, "type": kind, "code": int(code, 0), "name": wire})
    return result


def messages(path, text):
    result = []
    for number, line in enumerate(text.splitlines(), 1):
        if not MESSAGE_LINE.match(line):
            continue
        symbol = line.split()[1]
        if symbol.startswith(MESSAGE_FLAGS):
            continue
        match = MESSAGE.match(line)
        if not match:
            raise ReferenceError("%s:%d: unparsed message type: %s" % (path, number, line.strip()))
        result.append({"kind": "message", "symbol": match.group(1), "source": path, "line": number,
                       "value": int(match.group(2), 0)})
    return result


def features(path, text):
    result = []
    for number, line in enumerate(text.splitlines(), 1):
        if not FEATURE_LINE.match(line):
            continue
        match = FEATURE.match(line)
        if not match:
            raise ReferenceError("%s:%d: unparsed feature: %s" % (path, number, line.strip()))
        variant, bit, incarnation, name, extra = match.groups()
        releases = [part.strip() for part in extra.split(",") if part.strip()]
        entry = {"kind": "feature", "symbol": "CEPH_FEATURE_" + name, "source": path, "line": number,
                 "bit": int(bit), "incarnation": int(incarnation), "lifecycle": "active"}
        if variant == "_DEPRECATED":
            if len(releases) != 1:
                raise ReferenceError("%s:%d: deprecated feature without one release" % (path, number))
            entry.update(lifecycle="deprecated", deprecated=releases[0])
        elif variant == "_RETIRED":
            if len(releases) != 2:
                raise ReferenceError("%s:%d: retired feature without two releases" % (path, number))
            entry.update(lifecycle="retired", deprecated=releases[0], retired=releases[1])
        elif releases:
            raise ReferenceError("%s:%d: active feature with extra arguments" % (path, number))
        result.append(entry)
    return result


def msgr2_features(path, text):
    result = []
    for number, line in enumerate(text.splitlines(), 1):
        if not MSGR2_FEATURE_LINE.match(line):
            continue
        match = MSGR2_FEATURE.match(line)
        if not match:
            raise ReferenceError("%s:%d: unparsed msgr2 feature: %s" % (path, number, line.strip()))
        bit, incarnation, name = match.groups()
        result.append({"kind": "msgr2_feature", "symbol": "CEPH_MSGR2_FEATURE_" + name, "source": path,
                       "line": number, "bit": int(bit), "incarnation": int(incarnation)})
    return result


PARSERS = {"osd_op": osd_ops, "message": messages, "feature": features, "msgr2_feature": msgr2_features}


def extract(source):
    """The reference entries and the SHA-256 of each header under source."""
    entries, digests = [], {}
    for kind, paths in SOURCES.items():
        found = []
        for path in paths:
            data = (Path(source) / path).read_bytes()
            digests[path] = hashlib.sha256(data).hexdigest()
            found.extend(PARSERS[kind](path, data.decode("utf-8")))
        if not found:
            raise ReferenceError("no %s declarations in %s" % (kind, ", ".join(paths)))
        entries.extend(found)
    seen = {}
    for entry in entries:
        key = (entry["kind"], entry["symbol"])
        if key in seen:
            raise ReferenceError("%s %s is declared at %s:%d and %s:%d" % (
                entry["kind"], entry["symbol"], seen[key]["source"], seen[key]["line"], entry["source"], entry["line"]))
        seen[key] = entry
    return entries, digests


def document(source, commit, release):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ReferenceError("commit must be a full 40-hex SHA: %r" % commit)
    entries, digests = extract(source)
    counts = {}
    for entry in entries:
        counts[entry["kind"]] = counts.get(entry["kind"], 0) + 1
    return {"ceph_commit": commit, "ceph_release": release, "sources": digests, "counts": counts,
            "entries": entries}


def render(doc):
    lines = ["{"]
    for key in ("ceph_commit", "ceph_release", "sources", "counts"):
        lines.append("  %s: %s," % (json.dumps(key), json.dumps(doc[key], sort_keys=True)))
    lines.append('  "entries": [')
    rows = [json.dumps(entry, sort_keys=True) for entry in doc["entries"]]
    lines.extend("    " + row + ("," if i < len(rows) - 1 else "") for i, row in enumerate(rows))
    lines.append("  ]")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate")
    generate.add_argument("--source", required=True)
    generate.add_argument("--commit", required=True)
    generate.add_argument("--release", required=True)
    generate.add_argument("--output", required=True)
    check = commands.add_parser("check")
    check.add_argument("--source", required=True)
    check.add_argument("--reference", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            Path(args.output).write_text(render(document(args.source, args.commit, args.release)))
            return 0
        recorded = json.loads(Path(args.reference).read_text())
        current = document(args.source, recorded["ceph_commit"], recorded["ceph_release"])
        if render(current) != render(recorded):
            print("%s does not match the headers under %s" % (args.reference, args.source), file=sys.stderr)
            return 1
        return 0
    except (OSError, ReferenceError, KeyError, ValueError) as error:
        print("messenger_reference: %s" % error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
