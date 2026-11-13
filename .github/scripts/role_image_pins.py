#!/usr/bin/env python3
"""Pin the role images of one CI run to the registry digests of their tags.

The role images are the published <variant>-<release>-<role> tags of
ceph-testcontainers-images, and this repository records no digest of them: a
compatible rebuild replaces the image under its tag and CI picks it up with
no change here. One run must still use one image per role. Its jobs start at
different times, a failed job can be run again hours later, and a campaign
repeats its batches, so the run resolves each role tag to its manifest digest
once, before any runtime job starts, and every job and attempt pulls that
digest. The digests live in the run's outputs and artifacts only.

pin writes digests=<role>=sha256:<hex>,... (and release=) to GITHUB_OUTPUT.
A caller that already pinned the images passes its digests, which are
validated and written as they are; a plan without a Ceph batch pins nothing.
It reads the registry only through `docker buildx imagetools inspect`, with
registry_pull's bounded retry of transport failures; it pulls, builds and
runs nothing.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import registry_pull
import run_image_matrix


def resolve(reference, run=None, sleep=None):
    """The manifest (index) digest that a tag names now."""
    command = ["docker", "buildx", "imagetools", "inspect", reference, "--format", "{{json .Manifest}}"]
    results = []

    def attempt(number):
        result = (run or subprocess.run)(command, text=True, capture_output=True, timeout=120, check=False)
        results.append(result)
        return result.returncode, result.stderr or ""

    passed, attempts = registry_pull.pull(attempt, sleep=sleep)
    result = results[-1]
    if not passed:
        raise RuntimeError("cannot resolve " + reference + " after " + str(len(attempts)) + " attempt(s): "
                           + (result.stderr or "").strip()[:300])
    try:
        digest = json.loads(result.stdout).get("digest")
    except (ValueError, AttributeError):
        digest = None
    if not isinstance(digest, str) or run_image_matrix.DIGEST.fullmatch(digest) is None:
        raise RuntimeError(reference + " has no sha256 manifest digest")
    return digest


def pin(release, run=None, sleep=None):
    """role -> digest for every published role image of the release,
    optional roles included."""
    references = run_image_matrix.image_references("official", "roles", release,
                                                   run_image_matrix.published_roles("official"))
    return {role: resolve(reference, run, sleep) for role, reference in references.items()}


def needs_role_images(plan):
    """Whether any entry of a tag_scenarios.py plan declares role images. A
    plan of Docker-only batches needs no registry answer."""
    data = json.loads(Path(plan).read_text(encoding="utf-8"))
    entries = data.get("include") if isinstance(data, dict) else None
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        raise ValueError("the plan lists no matrix entries")
    return any(entry.get("image_roles") for entry in entries)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("pin", help="Resolve the role tags of a release to digests")
    command.add_argument("--release", required=True, choices=tuple(run_image_matrix.RELEASES))
    command.add_argument("--digests", default="",
                         help="The caller's role=sha256:<digest> pins; not empty, they are kept and no tag is resolved")
    command.add_argument("--plan", help="A tag_scenarios.py plan; without an entry that declares role images, pin nothing")
    command.add_argument("--github-output", help="Append release= and digests= here")
    args = parser.parse_args(argv)
    try:
        if args.digests:
            digests, source = run_image_matrix.parse_digests(args.digests), "the caller's pins"
        elif args.plan and not needs_role_images(args.plan):
            digests, source = {}, "no batch of the plan declares role images"
        else:
            digests, source = pin(args.release), "resolved from the role tags"
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
        print("Role image pinning failed: " + str(error), file=sys.stderr)
        return 1
    value = ",".join(role + "=" + digest for role, digest in digests.items())
    print("Pinned role images of " + args.release + " (" + source + "): " + (value or "none"))
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as output:
            output.write("release=" + args.release + "\ndigests=" + value + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
