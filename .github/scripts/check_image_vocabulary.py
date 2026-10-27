#!/usr/bin/env python3
"""Keep the retired control-image vocabulary out of live code and documentation.

The image project's role contract names five dependency roles (essential,
client, cephfs, rbd-aux, rgw) and the explicit aggregate all. The retired
vocabulary described one broad "control" image and daemon-role tag families.
This check fails when a tracked text file reintroduces it, except in the
paths of ALLOWED, which are historical records. It does not ban the control
plane vocabulary: ControlContainer, "control container" and "control plane"
name a real executor and stay valid.

Run it from the repository root; it reads the files that Git tracks.
"""
import argparse
import os
import re
import subprocess
import sys

# One entry per retired spelling: (name, regular expression).
PATTERNS = (
    ("ControlImage identifier", re.compile(r"[Cc]ontrolImage")),
    ("control image prose", re.compile(r"control[ -]images?\b", re.I)),
    ("control or all role", re.compile(r"`?control`?\s*(?:or|/|and)\s*`?all`?\s+(?:role|image)", re.I)),
    ("control role list", re.compile(r"control,\s*OSD,\s*MDS", re.I)),
    ("control role tag", re.compile(r"official-[A-Za-z0-9.<>_-]*-control\b")),
)

# Historical records and the files that must name the retired spellings to
# check them. A path ending in "/" is a prefix.
ALLOWED = {
    "CHANGELOG.md": "published release history",
    "docs/guides/upgrading-to-v0.2.md": "migration guide of a published release",
    "docs/guides/upgrading-to-v0.3.md": "migration guide of a published release",
    "docs/research/ceph-fuse-mount.md": "recorded measurement with the tag that ran",
    "docs/design/image-role-terminology.md": "design record that quotes the retired names",
    ".github/scripts/fixtures/cephfs-quiescence-native.source.go.txt": "retained producer source",
    ".github/scripts/fixtures/cephfs-recovery-native.source.go.txt": "retained producer source",
    ".github/scripts/fixtures/package-split.json": "rewrite rule matching the retained producers",
    ".github/scripts/check_image_vocabulary.py": "this check",
    ".github/scripts/test_check_image_vocabulary.py": "this check's tests",
}
SUFFIXES = (".go", ".md", ".py", ".json", ".yml", ".yaml", ".txt", ".sh", ".mod")


def allowed(path):
    return path in ALLOWED or any(key.endswith("/") and path.startswith(key) for key in ALLOWED)


def tracked(root):
    output = subprocess.check_output(["git", "-C", root, "ls-files", "-z"])
    return [p for p in output.decode().split("\0") if p and p.endswith(SUFFIXES)]


def scan_text(path, text):
    """Return (path, line number, pattern name, line) for each retired spelling."""
    found = []
    for number, line in enumerate(text.split("\n"), 1):
        for name, pattern in PATTERNS:
            if pattern.search(line):
                found.append((path, number, name, line.strip()[:120]))
    return found


def scan(root, paths=None):
    problems = []
    for path in paths if paths is not None else tracked(root):
        if allowed(path):
            continue
        full = os.path.join(root, path)
        if os.path.islink(full) or not os.path.isfile(full):
            continue
        with open(full, encoding="utf-8", errors="replace") as handle:
            problems.extend(scan_text(path, handle.read()))
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=".")
    args = parser.parse_args(argv)
    problems = scan(args.root)
    for path, number, name, line in problems:
        print("%s:%d: %s: %s" % (path, number, name, line), file=sys.stderr)
    if problems:
        print("%d retired image-role spelling(s); use essential, client, cephfs, rbd-aux, rgw or all, "
              "or add a historical record to ALLOWED" % len(problems), file=sys.stderr)
        return 1
    print("image role vocabulary: no retired spelling outside the historical records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
