#!/usr/bin/env python3
"""Pin the role images of a representative campaign to registry digests.

A campaign repeats the representative batches several times in one run.
Every attempt must run the same commit and the same role images, so the
campaign resolves each published role tag of the release to its manifest
digest once, before any attempt starts, and every attempt pulls tag@digest.

pin writes digests=<role>=sha256:<hex>,... (and release=) to GITHUB_OUTPUT.
It reads the registry only through `docker buildx imagetools inspect`; it
pulls, builds and runs nothing.
"""

import argparse
import json
import subprocess
import sys

import run_image_matrix


def resolve(reference, run=None):
    """The manifest (index) digest that a tag names now."""
    command = ["docker", "buildx", "imagetools", "inspect", reference, "--format", "{{json .Manifest}}"]
    result = (run or subprocess.run)(command, text=True, capture_output=True, timeout=120, check=False)
    if result.returncode:
        raise RuntimeError("cannot resolve " + reference + ": " + result.stderr.strip()[:300])
    try:
        digest = json.loads(result.stdout).get("digest")
    except (ValueError, AttributeError):
        digest = None
    if not isinstance(digest, str) or run_image_matrix.DIGEST.fullmatch(digest) is None:
        raise RuntimeError(reference + " has no sha256 manifest digest")
    return digest


def pin(release, run=None):
    """role -> digest for every published role image of the release."""
    references = run_image_matrix.image_references("official", "roles", release)
    return {role: resolve(reference, run) for role, reference in references.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("pin", help="Resolve the role tags of a release to digests")
    command.add_argument("--release", required=True, choices=tuple(run_image_matrix.RELEASES))
    command.add_argument("--github-output", help="Append release= and digests= here")
    args = parser.parse_args(argv)
    try:
        digests = pin(args.release)
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print("Campaign pinning failed: " + str(error), file=sys.stderr)
        return 1
    value = ",".join(role + "=" + digest for role, digest in digests.items())
    print("Pinned role images of " + args.release + ": " + value)
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as output:
            output.write("release=" + args.release + "\ndigests=" + value + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
