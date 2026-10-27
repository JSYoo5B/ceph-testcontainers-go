#!/usr/bin/env python3
"""Decide whether a pull request changes only documentation that no check reads.

The Code checks workflow keeps triggering on every pull request, because its
jobs are required status checks and a workflow that never starts leaves them
pending. A small job runs this script and the heavy jobs skip themselves when
it reports docs_only=true. A skipped job reports success.

Documentation counts as unread only when it is on the allowlist below. Every
other path, including documentation that a Go test or CI tool reads, runs the
full checks. Any doubt or error also runs them: the answer is docs_only=true
only when the changed files were computed and every one is on the allowlist.
"""
import argparse
import os
import subprocess
import sys

# docs/ directories whose direct Markdown children are not read by any check.
# Exceptions inside them are the files that a check reads.
UNREAD_DOC_DIRS = ("docs/contributing", "docs/design", "docs/guides", "docs/research")
READ_DOC_FILES = frozenset((
    # internal/integration messenger coverage test cross-checks this guide.
    "docs/guides/messenger-client-fixtures.md",
    # tag_scenarios tests compare the catalogue with the tag registry.
    "docs/guides/bug-compatibility-tags.md",
))
# internal/apiinventory reads the audit, the review notes and the package-scoped
# declaration, member and review files below docs/design/api-naming/.
READ_DOC_PREFIXES = ("docs/design/api-naming",)


def is_unread_doc(path):
    """True when path is documentation that no check reads."""
    if not isinstance(path, str) or not path or path != path.strip():
        return False
    if path.startswith("/") or "\\" in path or "\x00" in path or "\n" in path:
        return False
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        return False
    if len(parts) == 1:
        # Top-level Markdown files and the license are read by no check.
        return path == "LICENSE" or path.endswith(".md")
    if not path.endswith(".md"):
        return False
    if path in READ_DOC_FILES or path.startswith(READ_DOC_PREFIXES):
        return False
    return "/".join(parts[:-1]) in UNREAD_DOC_DIRS


def decide(event, fork, base, changed):
    """Return (docs_only, reason). changed is a list of paths or None."""
    if event != "pull_request":
        return False, "event %r always runs every check" % (event,)
    if fork:
        return False, "pull request from a fork runs every check"
    if not base:
        return False, "no base commit: running every check"
    if changed is None:
        return False, "changed files unavailable: running every check"
    if not changed:
        return False, "empty change list: running every check"
    code = [path for path in changed if not is_unread_doc(path)]
    if code:
        return False, "%d changed file(s) need the checks, first: %s" % (len(code), code[0])
    return True, "all %d changed file(s) are documentation that no check reads" % len(changed)


def changed_files(base, head="HEAD"):
    """Paths that differ between base and head, or None on any error.

    Renames are listed as a deletion and an addition so that moving a file
    out of the allowlist is seen. Only the two trees are compared, so a
    shallow checkout of a merge commit is enough once base is fetched.
    """
    try:
        subprocess.run(["git", "cat-file", "-e", base + "^{commit}"], check=True, capture_output=True)
        result = subprocess.run(["git", "diff", "--name-only", "--no-renames", "-z", base, head],
                                check=True, capture_output=True)
        return [item.decode("utf-8") for item in result.stdout.split(b"\0") if item]
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError, ValueError):
        return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--event", default="")
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--fork", default="false", help="true for a pull request from a fork")
    parser.add_argument("--github-output", default="")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        docs_only, reason = False, "unusable arguments: running every check"
        args = None
    if args is not None:
        try:
            changed = changed_files(args.base, args.head) if args.event == "pull_request" and args.base else None
            docs_only, reason = decide(args.event, args.fork == "true", args.base, changed)
        except Exception as error:  # fail open on any error
            docs_only, reason = False, "detection failed (%s): running every check" % type(error).__name__
    print("docs_only=%s: %s" % ("true" if docs_only else "false", reason))
    target = (args.github_output if args else "") or os.environ.get("GITHUB_OUTPUT", "")
    if target:
        try:
            with open(target, "a", encoding="utf-8") as output:
                output.write("docs_only=%s\n" % ("true" if docs_only else "false"))
        except OSError as error:
            print("cannot write the step output: %s" % error, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
