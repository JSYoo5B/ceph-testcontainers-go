#!/usr/bin/env python3
"""Compare a candidate bug compatibility ledger with a named base revision.

The single-snapshot validator in tag_scenarios.py cannot see a deleted
tombstone or a reassigned spelling, because both snapshots are individually
valid. This module compares the candidate registry with the registry as it
existed in a named maintained-base revision and reports each refused
transition of an existing tag, following the transition table in
docs/contributing/bug-compatibility.md.

It never talks to the network or Docker. A base revision that cannot be
resolved, or that holds no readable registry, is an error, never an empty base
(an empty base would permit every deletion). Reactivation stays refused until
the policy decides it. Reviewed URL aliases are an explicit input: this module
does not parse the catalogue page and derives no defect key from a tracker.

Exit status: 0 no refused transition, 1 at least one refused transition,
2 the comparison itself was inconclusive.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import tag_scenarios as engine

PERMIT, REFUSE = "permit", "refuse"
RULE_RETIRED_DELETED = "retired-entry-deleted"
RULE_SUPPORTED_DELETED = "supported-entry-deleted-without-retirement"
RULE_TRACKER_REASSIGNED = "tracker-reassigned"
RULE_TRACKER_WITHOUT_ALIAS = "tracker-changed-without-reviewed-alias"
RULE_LINK_EDIT_CHANGES_STATUS = "link-edit-changes-status"
RULE_REACTIVATION = "reactivation-undecided"
REGISTRY_PATH = "docs/guides/bug-compatibility-tags.json"


def _entries(registry, label):
    tags = registry.get("tags") if isinstance(registry, dict) else None
    if not isinstance(tags, dict):
        raise ValueError(label + " registry is not a validated bug compatibility registry")
    return tags


def reviewed_aliases(aliases):
    """Normalize {tag: [prior link, ...]} into {tag: frozenset(links)}."""
    result = {}
    for name, links in (aliases or {}).items():
        if not isinstance(name, str) or not isinstance(links, (list, tuple, set, frozenset)) or any(
                not isinstance(link, str) or not link.startswith("https://") for link in links):
            raise ValueError("reviewed alias history needs a tag mapped to https links: " + repr(name))
        result[name] = frozenset(links)
    return result


def compare_bindings(base, candidate, aliases=None):
    """One record per tag present in either registry, sorted by tag.

    A record holds the tag, its base and candidate (status, tracker) or None,
    the decision and the names of every rule that refuses it. A tracker edit is
    a permitted link edit only when the candidate's reviewed alias history
    records the base tracker and the status is unchanged; the validator cannot tell an alias from another
    defect's link on its own."""
    before, after = _entries(base, "base"), _entries(candidate, "candidate")
    history = reviewed_aliases(aliases)
    records = []
    for name in sorted(set(before) | set(after)):
        old, new = before.get(name), after.get(name)
        rules = []
        if old is not None and new is None:
            rules.append(RULE_RETIRED_DELETED if old["status"] == "retired" else RULE_SUPPORTED_DELETED)
        elif old is not None and old["tracker"] != new["tracker"]:
            if old["tracker"] not in history.get(name, frozenset()):
                rules.append(RULE_TRACKER_REASSIGNED if old["status"] == "retired" else RULE_TRACKER_WITHOUT_ALIAS)
            elif old["status"] != new["status"]:
                # A link edit preserves status and a retirement preserves the
                # tracker, so a reviewed alias never covers a combined edit.
                rules.append(RULE_LINK_EDIT_CHANGES_STATUS)
        if old is not None and new is not None and old["status"] == "retired" and new["status"] == "supported":
            rules.append(RULE_REACTIVATION)
        records.append({
            "tag": name,
            "base": None if old is None else {"status": old["status"], "tracker": old["tracker"]},
            "candidate": None if new is None else {"status": new["status"], "tracker": new["tracker"]},
            "decision": REFUSE if rules else PERMIT,
            "rules": rules,
        })
    return records


def refused_transitions(records):
    return [record for record in records if record["decision"] == REFUSE]


def describe(record):
    def side(entry):
        return "absent" if entry is None else entry["status"] + " " + entry["tracker"]
    return "{}: {} -> {}: refused by {}".format(
        record["tag"], side(record["base"]), side(record["candidate"]), ", ".join(record["rules"]))


def report(records):
    """The refused transitions as text lines, empty when none is refused."""
    return [describe(record) for record in refused_transitions(records)]


def check_bindings(base, candidate, aliases=None):
    refused = report(compare_bindings(base, candidate, aliases))
    if refused:
        raise ValueError("bug compatibility binding history refused:\n" + "\n".join(refused))


def load_revision_registry(revision, root, path=REGISTRY_PATH):
    """The validated registry as committed at a named revision of the repository."""
    if not isinstance(revision, str) or not revision or revision.startswith("-"):
        raise ValueError("base revision needs a name")
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "show", revision + "^{commit}:" + path],
            capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ValueError("base revision registry is unreadable: " + str(error)) from None
    if completed.returncode != 0:
        raise ValueError("base revision registry is unreadable: " + revision)
    try:
        return engine.bug_registry(json.loads(completed.stdout))
    except (ValueError, TypeError) as error:
        raise ValueError("base revision registry is invalid: " + str(error)) from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", required=True, help="named maintained-base revision")
    parser.add_argument("--root", default=str(engine.ROOT), help="repository root")
    parser.add_argument("--alias", action="append", default=[], metavar="TAG=URL",
                        help="reviewed prior tracker link recorded for a tag (repeatable)")
    args = parser.parse_args(argv)
    try:
        aliases = {}
        for item in args.alias:
            name, separator, link = item.partition("=")
            if not separator:
                raise ValueError("alias needs TAG=URL: " + item)
            aliases.setdefault(name, []).append(link)
        root = Path(args.root)
        base = load_revision_registry(args.base, root)
        try:
            candidate = engine.load_bug_registry(root / REGISTRY_PATH)
        except TypeError as error:  # the validator can fail on a malformed entry
            raise ValueError("candidate registry is invalid: " + str(error)) from None
        refused = report(compare_bindings(base, candidate, aliases))
    except ValueError as error:
        print("inconclusive: " + str(error), file=sys.stderr)
        return 2
    for line in refused:
        print(line)
    return 1 if refused else 0


if __name__ == "__main__":
    sys.exit(main())
