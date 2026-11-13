#!/usr/bin/env python3
"""Run one known-defect experiment case with go test and check its result.

A case prints one EXPERIMENT_RESULT line per independently evaluated defect,
REPRODUCED or NOT_REPRODUCED, and passes either way; only its given and when
steps fail it. Without an image the case runs on its default image, which
reproduces the defect, so every line must be REPRODUCED. With an image (a
fixed build) injected into the case's package-level image variable, every
line must be NOT_REPRODUCED. The run fails when go test fails, when no line
is printed, or when a line has the other result.
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


def check(log, image=""):
    """The reasons the log does not meet the expectation; empty when it does."""
    want = "NOT_REPRODUCED" if image else "REPRODUCED"
    results = RESULT.findall(log)
    if not results:
        return ["the case printed no EXPERIMENT_RESULT line"]
    return ["%s: %s, want %s" % (case, result, want) for case, result in results if result != want]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", required=True)
    parser.add_argument("--image", default="")
    args = parser.parse_args(argv)
    process = subprocess.Popen(command(args.case, args.image), cwd=ROOT, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines = []
    for line in process.stdout:
        sys.stdout.write(line)
        lines.append(line)
    code = process.wait()
    reasons = check("".join(lines), args.image)
    for case, result in RESULT.findall("".join(lines)):
        print("result %s %s" % (case, result))
    if code:
        reasons.insert(0, "go test exited %d" % code)
    for reason in reasons:
        print("::error::" + reason)
    return 1 if reasons else 0


if __name__ == "__main__":
    sys.exit(main())
