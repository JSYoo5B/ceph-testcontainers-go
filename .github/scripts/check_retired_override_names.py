#!/usr/bin/env python3
"""Keep the retired override entry points and handle types out of live code and docs.

The seven reversible override operations (#146) left `ceph.Container` for the
package `override`: AddBlocklist, SetFullRatios, SetMGRModule, SetOSDFlag,
SetConfig, SetMessengerPolicy and SetDebugLevel, each returning a
`*override.*Handle`. 0.x: the old `Container` methods and the `ceph` handle types
were removed without aliases. This check fails when a tracked text file names
one of them again (or a removed observation receiver such as
`(*Container).BlocklistEntries`), except in the paths of ALLOWED and PREFIXES, which are
historical records (published release notes, upgrade guides, move records, the
design records that quote the old names) and the files that must spell the
names to check them.

Run it from the repository root; it reads the files that Git tracks.
"""
import argparse
import os
import re
import subprocess
import sys

METHODS = (
    "OverrideConfig",
    "OverrideMessengerPolicy",
    "OverrideOSDFlag",
    "OverrideMGRModule",
    "OverrideFullRatios",
    "OverrideDebugLevel",
    "TemporaryBlocklist",
)
HANDLES = (
    "ConfigOverrideHandle",
    "MessengerPolicyOverrideHandle",
    "OSDFlagOverrideHandle",
    "MGRModuleOverrideHandle",
    "DebugLevelOverrideHandle",
    "FullRatiosOverride",
    "BlocklistOverride",
)
# Observation methods and static-fact accessors that left Container
# (they are methods of ceph.Observer or fields of ceph.ClusterFacts now).
RECEIVER_METHODS = (
    "AddressFamily", "BlocklistEntries", "CephVersion", "ClusterNetworkName", "Configuration",
    "DaemonAddresses", "EssentialImage", "FullRatios", "HasSeparateClusterNetwork", "HealthDetails",
    "Images", "MGRModules", "ManagerMetrics", "ManagerServices", "ManagerStatus", "MessengerMode",
    "MonitorBootstrapAddresses", "NetworkName", "OSDFlags", "OSDStates", "ObjectWatchers",
    "PGInconsistencies", "Pools", "PublicAddress", "QuorumStatus", "Status", "UsesHostNetwork",
)
# Word boundaries keep unrelated identifiers (such as the registry kind
# faultConfigOverride or a test named TestOverrideConfigX) from matching.
PATTERNS = (
    ("retired Container method", re.compile(r"\b(?:%s)\b" % "|".join(METHODS))),
    ("retired ceph handle type", re.compile(r"\b(?:%s)\b" % "|".join(HANDLES))),
    # Only receiver-qualified spellings: the bare names are valid on Observer
    # and ClusterFacts (and Status is also a method of Pool).
    ("retired Container receiver",
     re.compile(r"(?:\(\s*\*?\s*(?:ceph\.)?Container\s*\)|\bContainer)\.(?:%s)\b"
                % "|".join(RECEIVER_METHODS))),
)

# Historical records and the files that must name the retired spellings to
# check them.
ALLOWED = {
    "CHANGELOG.md": "published release history",
    "docs/design/api-naming.md": "naming inventory of the retired names, marked as a historical record",
    "docs/design/api-naming-audit.md": "naming audit of the retired names, marked as a historical record",
    "docs/design/override-package-boundary.md": "design record that quotes the retired names and maps them",
    "docs/design/structural-metrics-sample.md": "historical sample of the structure metrics tool on the override move",
    ".github/scripts/check_retired_override_names.py": "this check",
    ".github/scripts/test_check_retired_override_names.py": "this check's tests",
}
# A path that starts with one of these is a historical record.
PREFIXES = {
    "docs/changes/": "release-note fragments",
    "docs/guides/upgrading-": "migration guides",
    "docs/design/api-naming/moves/": "move records",
}
SUFFIXES = (".go", ".md", ".py", ".json", ".yml", ".yaml", ".txt", ".sh", ".mod")


def allowed(path):
    return path in ALLOWED or any(path.startswith(prefix) for prefix in PREFIXES)


def tracked(root):
    output = subprocess.check_output(["git", "-C", root, "ls-files", "-z"])
    return [p for p in output.decode().split("\0") if p and p.endswith(SUFFIXES)]


def scan_text(path, text):
    """Return (path, line number, pattern name, line) for each retired name."""
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
        print("%d retired override name(s); use the override package functions and handles "
              "(see docs/override.md), or add a historical record to ALLOWED" % len(problems), file=sys.stderr)
        return 1
    print("override names: no retired name outside the historical records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
