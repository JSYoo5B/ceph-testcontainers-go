#!/usr/bin/env python3
"""Publish completed Go failure names without exposing their diagnostic bodies.

Usage: report_test_failures.py LOG --profile scenario-topology-extensions
This reporter always exits successfully; the test step owns its failure status.
Annotation protocol: https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands
"""

import argparse
from pathlib import Path
import re
from typing import Sequence


# Limit annotations to static Go test identifiers and ordinary subtest labels.
# In particular, do not capture a diagnostic suffix, control bytes, or config.
FAILURE = re.compile(
    r"^[ \t]*--- FAIL: "
    r"(Test[A-Za-z0-9_]+(?:/[A-Za-z0-9_.#=+\-]+)*)"
    r" \([0-9]+(?:\.[0-9]+)?s\)[ \t]*$"
)
PROFILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,79}\Z")
PACKAGE_PASS = re.compile(
    r"^ok[ \t]+[A-Za-z0-9_.~/\-]+[ \t]+"
    r"(?:[0-9]+(?:\.[0-9]+)?s|\(cached\))"
    r"(?:[ \t]+\[no tests to run\])?[ \t]*$"
)
PACKAGE_FAIL = re.compile(r"^FAIL(?:[ \t]+.*)?$")


class ReporterParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse's default error exposes arguments and exits 2. Neither is
        # appropriate for a reporter that must preserve the test step's status.
        raise ValueError("invalid reporter arguments")


def annotate(level: str, message: str) -> None:
    # GitHub workflow command data escaping. The title is a fixed literal, so
    # untrusted values never enter command properties.
    escaped = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::{level} title=Go test result::{escaped}")


def unknown(profile: str) -> None:
    annotate("notice", f"{profile}: failed testcase not identified; log unavailable or Go completion missing.")


def completed_failures(path: Path) -> tuple[list[str], bool]:
    failures: list[str] = []
    seen: set[str] = set()
    passed = False
    package_failed = False
    with path.open(encoding="utf-8", errors="replace") as log:
        for raw in log:
            line = raw.rstrip("\r\n")
            failure = FAILURE.fullmatch(line)
            if failure is not None:
                name = failure.group(1)
                if name not in seen:
                    seen.add(name)
                    failures.append(name)
            if PACKAGE_PASS.fullmatch(line) is not None:
                passed = True
            if PACKAGE_FAIL.fullmatch(line) is not None:
                package_failed = True
    return failures, passed and not package_failed


def main(argv: Sequence[str] | None = None) -> int:
    profile = "Go test"
    try:
        parser = ReporterParser(description=__doc__)
        parser.add_argument("log", type=Path, help="Go verbose log file")
        parser.add_argument("--profile", required=True, help="public test profile label")
        args = parser.parse_args(argv)
        if PROFILE.fullmatch(args.profile) is None:
            raise ValueError("invalid profile label")
        profile = args.profile
        failures, passed = completed_failures(args.log)
        for name in failures:
            annotate("error", f"{profile}: {name}")
        if not failures and not passed:
            unknown(profile)
    except (OSError, ValueError):
        # Do not print paths, exception bodies, raw log lines, keys, or config.
        unknown(profile)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
