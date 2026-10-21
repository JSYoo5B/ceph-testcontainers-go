#!/usr/bin/env python3
"""Choose bounded PR smoke coverage or the complete roles-v2 campaign."""

import argparse
import fnmatch
import json
import os
from pathlib import Path
import re
import subprocess

from run_image_matrix import RELEASE, SUPPORTED_RELEASES


RELEASES = tuple(row[0] for row in SUPPORTED_RELEASES)
RUNNERS = {"amd64": "ubuntu-24.04", "arm64": "ubuntu-24.04-arm"}
# These paths define the image contract or the campaign that validates it.
FULL_PATHS = (
    "ceph/images*.go", "internal/integration/*images*fixture*.go",
    "internal/integration/role_images_v2*", "rbd/*role_images_v2*",
    ".github/workflows/roles-v2.yml", ".github/scripts/*roles_v2*",
    ".github/scripts/check_runtime_cleanup.py", ".github/scripts/run_image_matrix.py",
    "Makefile", "go.mod", "go.sum",
)
RUNTIME_ROOTS = ("ceph/", "cephfs/", "rbd/", "rgw/", "fault/", "internal/")
EXCLUDED_ROOTS = ("internal/tools/", "internal/integration/goceph/")


def select_scope(event, changed, *, mode="full", release_phase=False, labels=()):
    if event == "workflow_dispatch":
        if mode not in ("smoke", "full"):
            raise ValueError("manual scope must be smoke or full")
        return mode, "manual " + mode
    if event == "schedule":
        return "full", "weekly full campaign"
    if event != "pull_request":
        raise ValueError("unsupported roles-v2 event: " + event)
    if release_phase or "ci:full" in labels:
        return "full", "release phase or ci:full override"
    if any(fnmatch.fnmatchcase(path, pattern) for path in changed for pattern in FULL_PATHS):
        return "full", "image contract or campaign changed"
    if any(path.startswith(RUNTIME_ROOTS) and not path.startswith(EXCLUDED_ROOTS)
           for path in changed):
        return "smoke", "runtime change: representative role coverage"
    return "none", "no roles-v2 runtime inputs changed"


def plan(event, changed=(), **options):
    scope, reason = select_scope(event, changed, **options)
    cells = [(RELEASE, "amd64")] if scope == "smoke" else (
        [(release, arch) for release in RELEASES for arch in RUNNERS] if scope == "full" else [])
    return {"scope": scope, "reason": reason, "changed_paths": sorted(changed),
            "count": len(cells), "matrix": {"include": [
                {"release": release, "architecture": arch, "runner": RUNNERS[arch], "scope": scope}
                for release, arch in cells]}}


def changed_paths(event):
    pull = event["pull_request"]
    base, head = pull["base"]["sha"], pull["head"]["sha"]
    if any(not re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("PR base/head must be complete commit SHAs")
    # Three dots compare with the merge base, as PR diffs do. Include both sides
    # of renames, so moving an input out of a runtime path still selects it.
    output = subprocess.check_output([
        "git", "diff", "--name-only", "--no-renames", "-z", base + "..." + head, "--"])
    return [path.decode("utf-8") for path in output.split(b"\0") if path]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    event_name = os.environ["GITHUB_EVENT_NAME"]
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    result = plan(event_name, changed_paths(event) if event_name == "pull_request" else (),
                  mode=event.get("inputs", {}).get("scope", "full"),
                  release_phase=os.environ.get("CI_RELEASE_PHASE") == "true",
                  labels=[item["name"] for item in event.get("pull_request", {}).get("labels", [])])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key in ("scope", "count", "matrix"):
            value = json.dumps(result[key], separators=(",", ":")) if key == "matrix" else str(result[key])
            output.write(key + "=" + value + "\n")
    summary = f"roles-v2: {result['scope']} ({result['count']} native jobs); {result['reason']}"
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(summary + "\n\nSee the roles-v2-plan artifact for the changed inputs.\n")


if __name__ == "__main__":
    main()
