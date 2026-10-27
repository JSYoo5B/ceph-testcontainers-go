#!/usr/bin/env python3
"""Validate the release-note fragments under docs/changes.

Each release-worthy change owns one file, docs/changes/<change-id>.md, so that
concurrent pull requests never edit a shared changelog section. This script
checks the directory as a whole and nothing else: it does not assemble a
changelog and it does not decide whether a change needs a fragment. See
docs/contributing/release-fragments.md for the format.

A fragment is Markdown with a strict front matter block:

    ---
    change: 216-example
    kind: feature
    migration: docs/guides/upgrading-example.md
    supersedes: [215-older-change]
    reason: only with kind none
    ---
    User-facing text.

The script reads files with lstat and never follows a symbolic link. It exits
non-zero and prints every problem it finds when the directory is invalid. A
missing directory is valid: Git cannot keep an empty directory, so a tree with
no unreleased fragment has none.
"""
import argparse
import os
import re
import stat
import sys

CHANGES_DIR = "docs/changes"
GUIDES_DIR = "docs/guides"
KINDS = ("api-break", "feature", "behavior", "compatibility", "scope", "none")
KEYS = ("change", "kind", "migration", "supersedes", "reason")
CHANGE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
FILE_NAME = re.compile(r"([a-z0-9][a-z0-9-]{0,63})\.md")
MIGRATION = re.compile(r"(docs/guides/[A-Za-z0-9_.-]+\.md)(#[A-Za-z0-9_-]+)?")
MAX_BYTES = 64 * 1024
MAX_VALUE = 500


class FragmentError(Exception):
    pass


def fragment_stem(name):
    """The change identifier of a file name, or None unless it is exactly <change-id>.md."""
    match = FILE_NAME.fullmatch(name)
    return match.group(1) if match else None


def parse(text):
    """Return (fields, body) for one fragment, or raise FragmentError."""
    if "\x00" in text or "\r" in text:
        raise FragmentError("contains a NUL or carriage return character")
    lines = text.split("\n")
    if lines[0] != "---":
        raise FragmentError("the first line must be exactly '---' (front matter is required)")
    try:
        end = lines.index("---", 1)
    except ValueError:
        raise FragmentError("front matter is not closed by a '---' line") from None
    fields = {}
    for number, line in enumerate(lines[1:end], 2):
        match = re.fullmatch(r"([a-z]+): (\S.*)", line)
        if not match:
            raise FragmentError("line %d is not 'key: value': %r" % (number, line[:60]))
        key, value = match.group(1), match.group(2)
        if key not in KEYS:
            raise FragmentError("line %d: unknown key %r" % (number, key))
        if key in fields:
            raise FragmentError("line %d: duplicate key %r" % (number, key))
        if value != value.rstrip() or len(value) > MAX_VALUE:
            raise FragmentError("line %d: %s has trailing whitespace or is longer than %d characters"
                                % (number, key, MAX_VALUE))
        fields[key] = value
    return fields, "\n".join(lines[end + 1:])


def parse_supersedes(value):
    match = re.fullmatch(r"\[(.*)\]", value)
    if not match:
        raise FragmentError("supersedes must be a list such as [a-change, b-change]")
    items = [item.strip() for item in match.group(1).split(",")]
    if any(not CHANGE_ID.fullmatch(item) for item in items):
        raise FragmentError("supersedes holds an item that is not a valid change identifier")
    return items


def check_fields(name, fields, body, root, problems):
    def bad(message):
        problems.append("%s: %s" % (name, message))

    stem = fragment_stem(name)
    for key in ("change", "kind"):
        if key not in fields:
            bad("missing required field %r" % key)
    change = fields.get("change")
    if change is not None:
        if not CHANGE_ID.fullmatch(change):
            bad("change %r must be lowercase [a-z0-9-], start with a letter or digit, at most 64 characters" % change)
        elif change != stem:
            bad("change %r does not match the file name %r" % (change, stem))
    kind = fields.get("kind")
    if kind is not None and kind not in KINDS:
        bad("unknown kind %r (one of %s)" % (kind, ", ".join(KINDS)))
    if kind == "none":
        if "reason" not in fields:
            bad("kind none requires a reason")
    elif "reason" in fields and kind in KINDS:
        bad("reason is only allowed with kind none")
    if kind in KINDS and kind != "none" and not body.strip():
        bad("kind %s requires user-facing text after the front matter" % kind)
    migration = fields.get("migration")
    if kind == "api-break" and migration is None:
        bad("kind api-break requires a migration link")
    if migration is not None:
        match = MIGRATION.fullmatch(migration)
        if not match:
            bad("migration %r must be docs/guides/<file>.md with an optional #anchor" % migration)
        else:
            target = os.path.join(root, match.group(1))
            try:
                regular = stat.S_ISREG(os.lstat(target).st_mode)
            except OSError:
                regular = False
            if not regular:
                bad("migration target %s is not an existing regular file" % match.group(1))
    if "supersedes" in fields:
        try:
            items = parse_supersedes(fields["supersedes"])
        except FragmentError as error:
            bad(str(error))
        else:
            if len(set(items)) != len(items):
                bad("supersedes lists a change twice")
            if change in items:
                bad("a change cannot supersede itself")
            return items
    return []


def validate(root="."):
    """Return the list of problems found in root/docs/changes (empty when valid)."""
    problems = []
    directory = os.path.join(root, CHANGES_DIR)
    try:
        mode = os.lstat(directory).st_mode
    except FileNotFoundError:
        return problems
    except OSError as error:
        return ["%s: cannot be read: %s" % (CHANGES_DIR, error)]
    if not stat.S_ISDIR(mode):
        return ["%s: must be a real directory, not a symbolic link or file" % CHANGES_DIR]
    try:
        names = sorted(os.listdir(directory))
    except OSError as error:
        return ["%s: cannot be listed: %s" % (CHANGES_DIR, error)]

    claimed = {}
    supersedes = {}
    for name in names:
        path = os.path.join(directory, name)
        try:
            mode = os.lstat(path).st_mode
        except OSError as error:
            problems.append("%s: cannot be read: %s" % (name, error))
            continue
        if stat.S_ISLNK(mode):
            problems.append("%s: a symbolic link is not allowed" % name)
            continue
        if stat.S_ISDIR(mode):
            problems.append("%s: nested directories are not allowed" % name)
            continue
        if not stat.S_ISREG(mode):
            problems.append("%s: not a regular file" % name)
            continue
        if fragment_stem(name) is None:
            problems.append("%s: only <change-id>.md files are allowed (lowercase [a-z0-9-], at most 64 "
                            "characters before .md)" % name)
            continue
        if os.path.getsize(path) > MAX_BYTES:
            problems.append("%s: larger than %d bytes" % (name, MAX_BYTES))
            continue
        try:
            with open(path, "rb") as handle:
                text = handle.read().decode("utf-8")
            fields, body = parse(text)
        except UnicodeDecodeError:
            problems.append("%s: not valid UTF-8" % name)
            continue
        except OSError as error:
            problems.append("%s: cannot be read: %s" % (name, error))
            continue
        except FragmentError as error:
            problems.append("%s: %s" % (name, error))
            continue
        supersedes[fragment_stem(name)] = check_fields(name, fields, body, root, problems)
        if "change" in fields:
            claimed.setdefault(fields["change"].lower(), []).append(name)

    for change, files in sorted(claimed.items()):
        if len(files) > 1:
            problems.append("duplicate change identifier %r in %s" % (change, ", ".join(files)))
    known = {fragment_stem(name) for name in names if fragment_stem(name)}
    for stem, items in sorted(supersedes.items()):
        for item in items:
            if item not in known:
                problems.append("%s.md: supersedes %r, which is not a fragment in the tree" % (stem, item))
    for stem in sorted(supersedes):
        seen, current = set(), stem
        # Follow the first superseded edge of each node; a cycle is any repeat.
        stack = [stem]
        while stack:
            current = stack.pop()
            for item in supersedes.get(current, []):
                if item == stem:
                    problems.append("%s.md: supersedes forms a cycle" % stem)
                    stack = []
                    break
                if item not in seen and item in supersedes:
                    seen.add(item)
                    stack.append(item)
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=".", help="repository root (default: current directory)")
    args = parser.parse_args(argv)
    problems = validate(args.root)
    for problem in problems:
        print("%s/%s" % (CHANGES_DIR, problem) if not problem.startswith(CHANGES_DIR) else problem,
              file=sys.stderr)
    if problems:
        print("%d problem(s) in %s" % (len(problems), CHANGES_DIR), file=sys.stderr)
        return 1
    count = 0
    directory = os.path.join(args.root, CHANGES_DIR)
    if os.path.isdir(directory):
        count = len(os.listdir(directory))
    print("%s: %d fragment(s) valid" % (CHANGES_DIR, count))
    return 0


if __name__ == "__main__":
    sys.exit(main())
