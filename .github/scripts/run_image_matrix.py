#!/usr/bin/env python3
"""Run the nine supplied-image compatibility tests without building images.

An absent image is explicitly pulled for the Docker engine's native platform.
Existing references are resolved once to local image IDs before invoking make.
This is module compatibility evidence, not the separate image contract checker.
"""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
RELEASE = "20.2.4"
OFFICIAL_IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
REGISTRY = "ghcr.io/jsyoo5b/ceph-testcontainers-images"
ROLE_ENV = {
    "control": "CEPH_TEST_IMAGE",
    "osd": "CEPH_TEST_OSD_IMAGE",
    "rgw": "CEPH_TEST_RGW_IMAGE",
    "mds": "CEPH_TEST_MDS_IMAGE",
    "mirror": "CEPH_TEST_MIRROR_IMAGE",
}
EXPECTED_TESTS = (
    "TestClusterLifecycle",
    "TestRBDLifecycle",
    "TestCephFSFilesystem",
    "TestRGWS3",
    "TestManagerLifecycle",
    "TestMultiClusterRBDBackup",
    "TestMultiClusterRBDSnapshotMirror",
    "TestMultiClusterCephFSSnapshotMirrorAndBackup",
    "TestMultiClusterRGWMultisite",
)
IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")
RUN = re.compile(r"^=== RUN[ \t]+(Test[A-Za-z0-9_]+)(?:[ \t]*)$")
RESULT = re.compile(r"^[ \t]*--- (PASS|FAIL|SKIP): (Test[^ \t]+) \([0-9]+(?:\.[0-9]+)?s\)[ \t]*$")


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("official", "debian", "ubuntu"), default="official")
    parser.add_argument("--layout", choices=("all", "roles"), default="all")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"),
                        help="Must match the Docker engine; omitted means its native platform")
    parser.add_argument("--release", choices=(RELEASE,), default=RELEASE)
    parser.add_argument("--output-dir", type=Path,
                        help="New directory only; default artifacts/image-matrix-<cell>-<UTC>-<id>")
    return parser.parse_args(argv)


def image_references(variant, layout, release):
    if release != RELEASE or variant not in ("official", "debian", "ubuntu") or layout not in ("all", "roles"):
        raise ValueError("unsupported image matrix cell")
    if layout == "all":
        reference = OFFICIAL_IMAGE if variant == "official" else REGISTRY + ":" + variant + "-" + release + "-all"
        return {"all": reference}
    return {role: REGISTRY + ":" + variant + "-" + release + "-" + role
            for role in ("control", "osd", "rgw", "mds")}


def native_platform(info):
    if not isinstance(info, dict) or info.get("OSType") != "linux":
        raise RuntimeError("image compatibility requires a Docker Linux engine")
    architecture = info.get("Architecture")
    arch = {"amd64": "amd64", "x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(
        architecture if isinstance(architecture, str) else "")
    if arch is None:
        raise RuntimeError("Docker engine architecture must be amd64 or arm64")
    return "linux/" + arch


def source_manifest(root):
    # Hash the actual checkout inputs rather than treating a caller's revision
    # label as proof of unmodified source. Generated output is never included.
    paths = {root / "Makefile", root / "go.mod", root / "go.sum",
             root / ".github/scripts/run_image_matrix.py"}
    for directory in ("ceph", "multicluster", "internal/dockerbridge", "internal/integration"):
        paths.update((root / directory).rglob("*.go"))
    paths.update(path for path in (root / "ceph/internal/scripts").rglob("*") if path.is_file())
    files = []
    for path in sorted(paths, key=lambda value: value.relative_to(root).as_posix()):
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("source input is missing or is a symlink: " + path.relative_to(root).as_posix())
        content = path.read_bytes()
        files.append({"path": path.relative_to(root).as_posix(), "bytes": len(content),
                      "sha256": hashlib.sha256(content).hexdigest()})
    module = re.search(r"^module[ \t]+([^ \t\r\n]+)[ \t]*$", (root / "go.mod").read_text(), re.MULTILINE)
    if module is None:
        raise RuntimeError("go.mod has no module declaration")
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"files": files, "sha256": digest, "module": module.group(1)}


def completed_tests(path, module):
    runs = []
    results = {}
    duplicates = []
    failures = []
    skips = []
    package_passes = 0
    package_failure = False
    package = re.compile(r"^ok[ \t]+" + re.escape(module + "/internal/integration")
                         + r"[ \t]+[0-9]+(?:\.[0-9]+)?s[ \t]*$")
    with path.open(encoding="utf-8", errors="replace") as log:
        for raw in log:
            line = raw.rstrip("\r\n")
            started = RUN.fullmatch(line)
            if started:
                runs.append(started.group(1))
            result = RESULT.fullmatch(line)
            if result:
                status, name = result.groups()
                if status == "FAIL":
                    failures.append(name)
                if status == "SKIP":
                    skips.append(name)
                if "/" not in name:
                    if name in results:
                        duplicates.append(name)
                    results[name] = status
            if package.fullmatch(line):
                package_passes += 1
            if re.fullmatch(r"FAIL(?:[ \t]+.*)?", line):
                package_failure = True
    expected = set(EXPECTED_TESTS)
    missing = sorted(expected.difference(results))
    unexpected = sorted((set(results) | set(runs)).difference(expected))
    passed = (not missing and not unexpected and not duplicates and not failures and not skips
              and set(runs) == expected and len(runs) == len(expected)
              and all(status == "PASS" for status in results.values())
              and package_passes == 1 and not package_failure)
    return {"expected": list(EXPECTED_TESTS), "started": runs, "results": results,
            "missing": missing, "unexpected": unexpected, "duplicates": duplicates,
            "failed": failures, "skipped": skips, "package_passes": package_passes,
            "package_failure": package_failure, "passed": passed}


def main(argv=None):
    args = parse_arguments(argv)
    output = args.output_dir or ROOT / "artifacts" / (
        "image-matrix-" + args.variant + "-" + args.layout + "-"
        + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    output = output.resolve()
    try:
        output.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        print("cannot create a new result directory: " + str(error), file=sys.stderr)
        return 2
    summary = {"schema": 1, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "variant": args.variant, "layout": args.layout, "release": args.release,
               "requested_platform": args.platform, "image_builds": 0,
               "cleanup_owner": "Go test fixtures (this runner never prunes Docker resources)",
               "commands": [], "images": {}, "test_command": ["make", "image-compatibility"],
               "test_exit_code": None, "runtime_result": "not_run", "passed": False}
    code = 1

    def capture(command):
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=60, check=False)
        summary["commands"].append({"argv": command, "exit_code": result.returncode})
        return result

    def stream(command, name, env=None):
        started = time.monotonic()
        record = {"argv": command, "log": name, "exit_code": None}
        summary["commands"].append(record)
        with (output / name).open("x", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in process.stdout:
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
            record["exit_code"] = process.wait()
            record["seconds"] = round(time.monotonic() - started, 3)
        return record["exit_code"]

    try:
        summary["source"] = source_manifest(ROOT)
        revision = os.environ.get("GITHUB_SHA")
        if revision and re.fullmatch(r"[a-fA-F0-9]{40}", revision):
            summary["source"]["declared_github_revision"] = revision.lower()
        for executable in ("go", "make", "docker"):
            if shutil.which(executable) is None:
                raise RuntimeError(executable + " is required on the caller's PATH")
        go = capture(["go", "version"])
        if go.returncode:
            raise RuntimeError("caller Go is unavailable")
        summary["go_version"] = go.stdout.strip()
        info_result = capture(["docker", "info", "--format", "{{json .}}"])
        if info_result.returncode:
            raise RuntimeError("Docker engine info failed")
        info = json.loads(info_result.stdout)
        platform = native_platform(info)
        summary["docker"] = {name: info.get(name) for name in
                             ("ServerVersion", "OSType", "Architecture", "NCPU", "MemTotal")}
        summary["platform"] = platform
        if args.platform and args.platform != platform:
            raise RuntimeError("requested platform differs from Docker engine native platform; emulation is not validation")
        for role, reference in image_references(args.variant, args.layout, args.release).items():
            # Ordinary inspection keeps this native-only runner compatible
            # with engines predating the inspect --platform API (1.49).
            inspect = ["docker", "image", "inspect", "--", reference]
            result = capture(inspect)
            if result.returncode:
                # Permission/daemon/API errors are not evidence of an absent
                # image and must not cause an unrelated registry pull.
                if not re.search(r"\bNo such (?:image|object):", result.stderr, re.IGNORECASE):
                    raise RuntimeError(role + " image inspection failed; image absence was not confirmed")
                pull_code = stream(["docker", "pull", "--platform", platform, reference], "pull-" + role + ".log")
                if pull_code:
                    raise RuntimeError(role + " image pull failed (exit " + str(pull_code) + ")")
                result = capture(inspect)
                if result.returncode:
                    raise RuntimeError(role + " image is unavailable after pull")
            inspected = json.loads(result.stdout)
            if not isinstance(inspected, list) or len(inspected) != 1 or not isinstance(inspected[0], dict):
                raise RuntimeError(role + " image inspection has an invalid shape")
            image = inspected[0]
            image_id = image.get("Id")
            if not isinstance(image_id, str) or IMAGE_ID.fullmatch(image_id) is None:
                raise RuntimeError(role + " image has no immutable local image ID")
            if image.get("Os") != "linux" or image.get("Architecture") != platform.split("/")[1]:
                raise RuntimeError(role + " image platform differs from the native Docker engine")
            repo_digests = image.get("RepoDigests")
            if repo_digests is None:
                repo_digests = []
            if (not isinstance(repo_digests, list) or any(not isinstance(value, str)
                    or re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", value) is None for value in repo_digests)):
                raise RuntimeError(role + " image has invalid registry digest metadata")
            summary["images"][role] = {"reference": reference, "image_id": image_id,
                                      "repo_digests": repo_digests, "platform": platform}
        images = summary["images"]
        env = os.environ.copy()
        # Inherited make options can request dry runs or extra goals. This
        # runner invokes exactly the checked-in compatibility target.
        for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEFILES", "MAKEOVERRIDES", "GOFLAGS"):
            env.pop(name, None)
        selected = {role: images["all" if args.layout == "all" else
                                 ("control" if role == "mirror" else role)]["image_id"]
                    for role in ROLE_ENV}
        summary["test_environment"] = {ROLE_ENV[role]: image for role, image in selected.items()}
        # A nonempty neutral flag overrides persisted GOENV flags such as
        # -overlay; preserve normal cache/proxy and Docker connectivity config.
        summary["test_environment"].update({"CGO_ENABLED": "0", "GOWORK": "off", "GOFLAGS": "-mod=readonly"})
        env.update(summary["test_environment"])
        summary["test_exit_code"] = stream(summary["test_command"], "integration.log", env)
        summary["tests"] = completed_tests(output / "integration.log", summary["source"]["module"])
        summary["passed"] = summary["test_exit_code"] == 0 and summary["tests"]["passed"]
        summary["runtime_result"] = "passed" if summary["passed"] else "failed"
        code = summary["test_exit_code"]
        if code < 0:
            code = 128 - code
        if not summary["passed"]:
            if not code:
                code = 1
            summary["error"] = "compatibility tests did not complete all nine cases without failure or skip"
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        summary["error"] = str(error)
        print(str(error), file=sys.stderr)
    finally:
        summary["exit_code"] = code
        summary["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with (output / "summary.json").open("x", encoding="utf-8") as report:
            json.dump(summary, report, indent=2)
            report.write("\n")
        print("Image matrix result: " + ("PASS" if summary["passed"] else "FAIL") + "; " + str(output), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
