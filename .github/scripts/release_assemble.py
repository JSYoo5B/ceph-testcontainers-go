#!/usr/bin/env python3
"""Assemble release-note fragments into a CHANGELOG draft and check released history.

Two subcommands, both Docker-free and offline:

    release_assemble.py assemble --version v0.4.0 --date 2026-11-01 [--output FILE]
    release_assemble.py check [--base REV]... [--require-tags]

assemble reads docs/changes (validated by release_fragments.py) and CHANGELOG.md,
and prints one release section. It never edits CHANGELOG.md, never deletes a
fragment and never reads the clock: the release preparer reviews and edits the
draft, pastes it in place of the legacy Unreleased block and deletes the consumed
fragments in the release pull request. Feature pull requests do not run it and
commit no aggregate.

Output order is fixed: the section heading, the legacy Unreleased block verbatim
(when it has text), then one subsection per kind (api-break, feature, behavior,
compatibility, scope) with entries sorted by change identifier. A superseded
fragment and a kind none fragment are left out of the text. Every consumed
fragment leaves a marker that records its identifier and the first 16 hex digits
of the SHA-256 of the file:

    - Entry text.
      <!-- change: 216-example sha256:0123456789abcdef -->
    <!-- omitted: 216-note sha256:0123456789abcdef -->

check reads the released sections of CHANGELOG.md (a heading such as
"## v0.3.0 (2026-10-16)"). It refuses a fragment that is still in the tree under
an identifier already assembled into a released section (not deleted, edited
after release, or re-used), a repeated marker, and a malformed marker. With git
available it also requires every released section of a v<major>.<minor>.<patch>
tag's CHANGELOG.md, and of each --base revision, to be byte for byte unchanged
(an edited, removed or deleted entry fails). A clone without those tags or
revisions cannot verify this: the report says SKIPPED or lists the versions that
were not verified, and --require-tags turns that into a failure. It is never
reported as a pass.
"""
import argparse
import collections
import datetime
import hashlib
import os
import re
import stat
import subprocess
import sys

import release_fragments

CHANGELOG = "CHANGELOG.md"
KIND_ORDER = ("api-break", "feature", "behavior", "compatibility", "scope")
KIND_HEADINGS = {
    "api-break": "Breaking changes",
    "feature": "Features",
    "behavior": "Behavior",
    "compatibility": "Compatibility",
    "scope": "Supported scope",
}
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SECTION = re.compile(r"## (v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)) \(([0-9]{4}-[0-9]{2}-[0-9]{2})\)")
ENTRY_MARKER = re.compile(r"  <!-- change: ([a-z0-9][a-z0-9-]{0,63}) sha256:([0-9a-f]{16}) -->")
OMITTED_MARKER = re.compile(r"<!-- omitted: ([a-z0-9][a-z0-9-]{0,63}) sha256:([0-9a-f]{16}) -->")
REVISION = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._/@~^-]{0,199}")

# Corrections that reached main after the tag, reviewed before this check existed
# (they replaced links to repository milestone pages). The released section text
# that main holds today is accepted for these versions, keyed by the full SHA-256
# of the section. Any other text still has to equal the tag. Do not add entries
# for a new release: published sections are immutable from here on.
ACCEPTED_DRIFT = {
    "v0.2.0": "d9bcdf453bbcf68af7b8af133daca46f98a09a78e9bfbdb96b83c7027a937cd3",
    "v0.3.0": "39fdb9ba67501d515b2ba29398e3b31e7c3614b7c970bdcc09bb907c876bd70f",
}

Fragment = collections.namedtuple("Fragment", "change kind migration supersedes body digest")
Ledger = collections.namedtuple("Ledger", "version digest role")


class AssembleError(Exception):
    def __init__(self, problems):
        super().__init__("\n".join(problems))
        self.problems = list(problems)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()[:16]


def load_fragments(root):
    """Return the validated fragments of root/docs/changes, or raise AssembleError."""
    problems = release_fragments.validate(root)
    if problems:
        raise AssembleError(["docs/changes/" + item for item in problems])
    directory = os.path.join(root, release_fragments.CHANGES_DIR)
    if not os.path.isdir(directory):
        return []
    fragments = []
    for name in sorted(os.listdir(directory)):
        with open(os.path.join(directory, name), "rb") as handle:
            raw = handle.read()
        fields, body = release_fragments.parse(raw.decode("utf-8"))
        supersedes = release_fragments.parse_supersedes(fields["supersedes"]) if "supersedes" in fields else []
        fragments.append(Fragment(fields["change"], fields["kind"], fields.get("migration"), supersedes,
                                  body, digest(raw)))
    return fragments


def read_changelog(root):
    path = os.path.join(root, CHANGELOG)
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise AssembleError([CHANGELOG + ": must be a regular file"])
        with open(path, "rb") as handle:
            return handle.read().decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise AssembleError(["%s: cannot be read: %s" % (CHANGELOG, error)]) from None


def split_sections(text):
    """Return (legacy_lines, released, problems).

    released maps a version to its section text (heading line included, no trailing
    blank line). legacy_lines are the lines of the "## Unreleased" block.
    """
    legacy, released, problems = [], {}, []
    unreleased_seen = False
    current, buffer = None, []  # current is None, "unreleased" or a version

    def close():
        if current == "unreleased":
            legacy.extend(buffer)
        elif current is not None:
            released[current] = "\n".join(buffer).rstrip("\n")

    for line in text.split("\n"):
        if not line.startswith("## "):
            buffer.append(line)
            continue
        close()
        buffer = []
        if line == "## Unreleased":
            if unreleased_seen:
                problems.append("%s: more than one '## Unreleased' heading" % CHANGELOG)
            unreleased_seen = True
            current = "unreleased"
            continue
        match = SECTION.fullmatch(line)
        if not match:
            problems.append("%s: unrecognized second-level heading %r (expected '## Unreleased' or "
                            "'## vX.Y.Z (YYYY-MM-DD)')" % (CHANGELOG, line[:60]))
            current = None
            continue
        if match.group(1) in released:
            problems.append("%s: version %s has more than one section" % (CHANGELOG, match.group(1)))
        current = match.group(1)
        buffer = [line]
    close()
    return legacy, released, problems


def trim_blank(lines):
    start, end = 0, len(lines)
    while start < end and lines[start].strip() == "":
        start += 1
    while end > start and lines[end - 1].strip() == "":
        end -= 1
    return lines[start:end]


def build_ledger(released):
    """Return (ledger, problems) from the markers inside the released sections."""
    ledger, problems = {}, []
    for version in sorted(released):
        for line in released[version].split("\n"):
            entry = ENTRY_MARKER.fullmatch(line)
            omitted = OMITTED_MARKER.fullmatch(line)
            match, role = (entry, "change") if entry else (omitted, "omitted")
            if match is None:
                if "<!-- change:" in line or "<!-- omitted:" in line:
                    problems.append("%s section %s: malformed identifier marker %r" % (CHANGELOG, version, line[:80]))
                continue
            change = match.group(1)
            if change in ledger:
                problems.append("%s: identifier %r is recorded in both %s and %s"
                                % (CHANGELOG, change, ledger[change].version, version))
                continue
            ledger[change] = Ledger(version, match.group(2), role)
    return ledger, problems


def check_tree(root, ledger):
    """Refuse a fragment in the tree whose identifier is already released."""
    problems = []
    directory = os.path.join(root, release_fragments.CHANGES_DIR)
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return problems
    for name in names:
        stem = release_fragments.fragment_stem(name)
        path = os.path.join(directory, name)
        if stem is None or stem not in ledger:
            continue
        try:
            if not stat.S_ISREG(os.lstat(path).st_mode):
                continue
            with open(path, "rb") as handle:
                raw = handle.read()
        except OSError:
            continue
        released = ledger[stem]
        if digest(raw) == released.digest:
            problems.append("docs/changes/%s: already assembled into %s; delete it in the release pull request"
                            % (name, released.version))
        else:
            problems.append("docs/changes/%s: identifier %r was released in %s and the file differs from the "
                            "released one (edited after release or re-used); choose a new identifier"
                            % (name, stem, released.version))
    return problems


def compare_released(old_text, new_text, label, accepted=ACCEPTED_DRIFT):
    """Every released section of old_text must exist, byte for byte, in new_text.

    A section whose current text has the SHA-256 recorded in accepted for its
    version is the one reviewed pre-existing drift and is not reported.
    """
    _, old, _ = split_sections(old_text)
    _, new, _ = split_sections(new_text)
    problems = []
    for version in sorted(old):
        if version not in new:
            problems.append("released section %s of %s is missing from %s" % (version, label, CHANGELOG))
        elif old[version] != new[version] and \
                hashlib.sha256(new[version].encode("utf-8")).hexdigest() != accepted.get(version):
            problems.append("released section %s differs from %s; published history is immutable" % (version, label))
    return problems


def run_git(root, *args):
    """Return (returncode, stdout bytes), or None when git cannot run."""
    try:
        done = subprocess.run(["git", "-C", root] + list(args), capture_output=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.returncode, done.stdout


def changelog_at(root, ref):
    result = run_git(root, "show", "%s:%s" % (ref, CHANGELOG))
    if result is None or result[0] != 0:
        return None
    try:
        return result[1].decode("utf-8")
    except UnicodeDecodeError:
        return None


def release_tags(root):
    result = run_git(root, "tag", "--list", "v*")
    if result is None or result[0] != 0:
        return []
    names = result[1].decode("utf-8", "replace").split("\n")
    return sorted(name for name in names if VERSION.fullmatch(name))


Report = collections.namedtuple("Report", "failures verified drifted unverified bases ledger")


def check(root, bases=(), require_tags=False):
    """Run the released-range checks and return a Report."""
    failures = []
    text = read_changelog(root)
    _, released, problems = split_sections(text)
    failures += problems
    ledger, problems = build_ledger(released)
    failures += problems
    failures += check_tree(root, ledger)

    verified, drifted, unverified = [], [], []
    tags = set(release_tags(root))
    for version in sorted(released):
        if version not in tags:
            unverified.append(version)
            continue
        old = changelog_at(root, "refs/tags/" + version)
        if old is None:
            unverified.append(version)
            continue
        _, old_sections, _ = split_sections(old)
        if version not in old_sections:
            unverified.append(version)
            continue
        failures += compare_released(old, text, "tag " + version)
        (verified if old_sections[version] == released[version] else drifted).append(version)
    for tag in sorted(tags - set(released)):
        failures.append("tag %s has no released section in %s" % (tag, CHANGELOG))

    checked_bases = []
    for base in bases:
        if not REVISION.fullmatch(base):
            failures.append("base %r is not an acceptable revision name" % base)
            continue
        resolved = run_git(root, "rev-parse", "--verify", "--quiet", base + "^{commit}")
        if resolved is None or resolved[0] != 0:
            failures.append("base %r cannot be resolved (is the history fetched?)" % base)
            continue
        old = changelog_at(root, resolved[1].decode("ascii", "replace").strip())
        if old is None:
            failures.append("base %r has no readable %s" % (base, CHANGELOG))
            continue
        failures += compare_released(old, text, "base " + base)
        checked_bases.append(base)
    if require_tags and (unverified or not tags):
        failures.append("--require-tags: released versions not verified against a tag: %s"
                        % (", ".join(unverified) or "none have a tag"))
    return Report(failures, verified, drifted, unverified, checked_bases, ledger)


def render_entry(fragment):
    lines = fragment.body.strip().split("\n")
    out = ["- " + lines[0].rstrip()]
    out += [("  " + line.rstrip()) if line.strip() else "" for line in lines[1:]]
    if fragment.migration:
        out += ["", "  Migration: [%s](%s)" % (fragment.migration, fragment.migration)]
    out.append("  <!-- change: %s sha256:%s -->" % (fragment.change, fragment.digest))
    return "\n".join(out)


def assemble(root, version, date):
    """Return the draft release section text. Raises AssembleError."""
    problems = []
    if not VERSION.fullmatch(version):
        problems.append("version %r must look like v1.2.3" % version)
    try:
        if not DATE.fullmatch(date):
            raise ValueError(date)
        datetime.date.fromisoformat(date)
    except ValueError:
        problems.append("date %r must be a real YYYY-MM-DD date" % date)
    if problems:
        raise AssembleError(problems)

    report = check(root)
    if report.failures:
        raise AssembleError(report.failures)
    text = read_changelog(root)
    legacy, released, _ = split_sections(text)
    if version in released:
        raise AssembleError(["%s already has a section for %s" % (CHANGELOG, version)])
    fragments = load_fragments(root)

    legacy = trim_blank(legacy)
    for line in legacy:
        if "<!-- change:" in line or "<!-- omitted:" in line:
            raise AssembleError(["the legacy Unreleased block holds a line that looks like an identifier marker"])

    superseded = {item for fragment in fragments for item in fragment.supersedes}
    kept = [f for f in fragments if f.kind != "none" and f.change not in superseded]
    omitted = [f for f in fragments if f.kind == "none" or f.change in superseded]
    if not legacy and not kept:
        raise AssembleError(["nothing to assemble: no legacy Unreleased text and no fragment with release text"])

    parts = ["## %s (%s)" % (version, date)]
    if legacy:
        parts.append("\n".join(legacy))
    for kind in KIND_ORDER:
        entries = sorted((f for f in kept if f.kind == kind), key=lambda f: f.change)
        if entries:
            parts.append("### " + KIND_HEADINGS[kind])
            parts.append("\n\n".join(render_entry(f) for f in entries))
    if omitted:
        parts.append("\n".join("<!-- omitted: %s sha256:%s -->" % (f.change, f.digest)
                               for f in sorted(omitted, key=lambda f: f.change)))
    return "\n\n".join(parts) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=".", help="repository root (default: current directory)")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("assemble", help="print a draft release section")
    build.add_argument("--version", required=True)
    build.add_argument("--date", required=True, help="release date, YYYY-MM-DD")
    build.add_argument("--output", help="write the draft to this new file instead of standard output")
    verify = commands.add_parser("check", help="check the released range of CHANGELOG.md")
    verify.add_argument("--base", action="append", default=[], help="revision whose released sections must "
                        "be unchanged (repeatable)")
    verify.add_argument("--require-tags", action="store_true", help="fail when a released version has no tag "
                        "to compare with")
    args = parser.parse_args(argv)

    try:
        if args.command == "assemble":
            draft = assemble(args.root, args.version, args.date)
            if args.output:
                with open(args.output, "x", encoding="utf-8", newline="") as handle:
                    handle.write(draft)
                print("wrote the draft for %s to %s" % (args.version, args.output), file=sys.stderr)
            else:
                sys.stdout.write(draft)
            return 0
        report = check(args.root, args.base, args.require_tags)
    except AssembleError as error:
        for problem in error.problems:
            print(problem, file=sys.stderr)
        print("%d problem(s)" % len(error.problems), file=sys.stderr)
        return 1
    except OSError as error:
        print("cannot write the draft: %s" % error, file=sys.stderr)
        return 1

    for problem in report.failures:
        print(problem, file=sys.stderr)
    print("released identifiers: %d recorded" % len(report.ledger))
    if report.verified:
        print("tag comparison: verified %s" % ", ".join(report.verified))
    elif not report.drifted:
        print("tag comparison: SKIPPED (no release tag with a comparable %s in this clone)" % CHANGELOG)
    if report.drifted:
        print("tag comparison: %s differ from their own tag but match the one accepted reviewed correction"
              % ", ".join(report.drifted))
    if (report.verified or report.drifted) and report.unverified:
        print("tag comparison: NOT verified for %s" % ", ".join(report.unverified))
    for base in report.bases:
        print("base comparison: verified against %s" % base)
    if report.failures:
        print("%d problem(s)" % len(report.failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
