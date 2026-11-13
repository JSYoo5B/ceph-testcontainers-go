#!/usr/bin/env python3
"""Run one known-defect experiment case with go test and check its result.

A case prints one EXPERIMENT_RESULT line per independently evaluated defect,
REPRODUCED or NOT_REPRODUCED, and passes either way; only its given and when
steps fail it. The case always runs on its default image first, which
reproduces the defect, so every line must be REPRODUCED. With an image (a
fixed build) the case then runs again with that image injected into its
package-level image variable, and every defect the default run reproduced
must be printed again as NOT_REPRODUCED. A defect the fixed run evaluates but
the default run did not (one that is reachable only once the first is fixed)
is printed and not judged: the dispatch claims nothing about it. A run also
fails when go test fails or when no line is printed.
"""
import argparse
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
MODULE = "github.com/JSYoo5B/ceph-testcontainers-go"
EXPERIMENTS = "internal/integration/experiments"
CASE = re.compile(r"[a-z0-9]+(?:_[a-z0-9]+)*\Z")
IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9./:@_-]*\Z")
RESULT = re.compile(r"EXPERIMENT_RESULT case=(\S+) result=(REPRODUCED|NOT_REPRODUCED)\b")


def command(case, image=""):
    """The go test command of case, with image injected when given."""
    if not CASE.match(case) or not (ROOT / EXPERIMENTS / case).is_dir():
        raise ValueError("unknown experiment case %r" % case)
    if image and not IMAGE.match(image):
        raise ValueError("invalid image reference %r" % image)
    package = "%s/%s/%s" % (MODULE, EXPERIMENTS, case)
    flags = ["-ldflags", "-X %s_test.image=%s" % (package, image)] if image else []
    return ["go", "test", "-tags=all,native_regression", "-count=1", "-v", "-timeout", "110m", *flags, package]


def results(log):
    """The (name, result) pairs of the log's EXPERIMENT_RESULT lines, in order."""
    return RESULT.findall(log)


def check_default(log):
    """The reasons a default-image log is not a reproduction; empty when it is."""
    found = results(log)
    if not found:
        return ["the case printed no EXPERIMENT_RESULT line"]
    return ["%s: %s, want REPRODUCED" % pair for pair in found if pair[1] != "REPRODUCED"]


def check_fixed(log, reproduced):
    """The reasons a fixed-image log does not resolve the reproduced names."""
    found = results(log)
    if not found:
        return ["the case printed no EXPERIMENT_RESULT line"]
    reasons = []
    for name in reproduced:
        got = [result for other, result in found if other == name]
        if not got:
            reasons.append("%s: no result, want NOT_REPRODUCED" % name)
        reasons += ["%s: %s, want NOT_REPRODUCED" % (name, result) for result in got if result != "NOT_REPRODUCED"]
    return reasons


def run(case, image=""):
    """Run case once, streaming its output; go test's exit code and the log."""
    print("== %s on %s" % (case, image or "its default image"), flush=True)
    process = subprocess.Popen(command(case, image), cwd=ROOT, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines = []
    for line in process.stdout:
        sys.stdout.write(line)
        lines.append(line)
    code = process.wait()
    log = "".join(lines)
    for name, result in results(log):
        print("result %s %s" % (name, result))
    return code, log


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", required=True)
    parser.add_argument("--image", default="")
    args = parser.parse_args(argv)
    command(args.case, args.image)  # refuse a bad case or image before the first run
    code, log = run(args.case)
    reasons = ["default image: " + reason for reason in (["go test exited %d" % code] if code else []) + check_default(log)]
    if args.image:
        reproduced = list(dict.fromkeys(name for name, result in results(log) if result == "REPRODUCED"))
        code, log = run(args.case, args.image)
        reasons += ["fixed image: " + reason
                    for reason in (["go test exited %d" % code] if code else []) + check_fixed(log, reproduced)]
    for reason in reasons:
        print("::error::" + reason)
    return 1 if reasons else 0


if __name__ == "__main__":
    sys.exit(main())
