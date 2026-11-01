#!/usr/bin/env python3
"""Run the nine supplied-image compatibility tests without building images.

An absent image is explicitly pulled for the Docker engine's native platform.
Existing references are resolved once to local image IDs before invoking make.
This is module compatibility evidence, not the separate image contract checker.
Preparation mode resolves those same images without executing compatibility tests,
or, with --roles, only the role images that a batch declares. A pull that
fails on a registry transport error is retried a bounded number of times
(registry_pull.py); the summary records every attempt.
"""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid

import registry_pull


ROOT = Path(__file__).resolve().parents[2]
# Releases that ceph-testcontainers-images checks and publishes, with their
# codenames and unmodified official images. This table is the supported
# release set of CI: Ceph periodic and Ceph release matrix run every category
# on each row (tag_scenarios.py releases), so a new release is one row here
# once ceph-testcontainers-images publishes its role images. A release or
# patch that is not a row is refused, never mapped to a neighbouring one.
# RELEASE is the default Ceph version; DEFAULT_IMAGE matches ceph.DefaultImage.
SUPPORTED_RELEASES = (
    ("20.2.4", "tentacle",
     "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"),
    ("19.2.5", "squid",
     "quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b"),
)
RELEASES = {release: image for release, _, image in SUPPORTED_RELEASES}
CODENAMES = {release: codename for release, codename, _ in SUPPORTED_RELEASES}
RELEASE = "20.2.4"
OFFICIAL_IMAGE = RELEASES[RELEASE]
ROLE_ENV = {"essential":"CEPH_TEST_ESSENTIAL_IMAGE", "client":"CEPH_TEST_CLIENT_IMAGE", "cephfs":"CEPH_TEST_CEPHFS_IMAGE", "rbd-aux":"CEPH_TEST_RBD_AUX_IMAGE", "rgw":"CEPH_TEST_RGW_IMAGE"}
REGISTRY = "ghcr.io/jsyoo5b/ceph-testcontainers-images"
DEFAULT_IMAGE = REGISTRY + "@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"
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
# The integration test packages that hold EXPECTED_TESTS; make
# image-compatibility runs ./internal/integration/..., and each must pass once.
EXPECTED_PACKAGES = (
    "internal/integration",
    "internal/integration/cephfs",
    "internal/integration/rbd",
    "internal/integration/rgw",
)
IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")
DIGEST = IMAGE_ID
RUN = re.compile(r"^=== RUN[ \t]+(Test[A-Za-z0-9_]+)(?:[ \t]*)$")
RESULT = re.compile(r"^[ \t]*--- (PASS|FAIL|SKIP): (Test[^ \t]+) \([0-9]+(?:\.[0-9]+)?s\)[ \t]*$")


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("official", "debian", "ubuntu"), default="official")
    parser.add_argument("--layout", choices=("all", "roles"), default="all")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"),
                        help="Must match the Docker engine; omitted means its native platform")
    parser.add_argument("--release", choices=tuple(RELEASES), default=RELEASE)
    parser.add_argument("--output-dir", type=Path,
                        help="New directory only; default artifacts/image-matrix-<cell>-<UTC>-<id>")
    parser.add_argument("--prepare-only", action="store_true",
                        help="Resolve supplied images without running Go or compatibility tests")
    parser.add_argument("--github-env", type=Path,
                        help="Append the resolved role IDs to this file; requires --prepare-only")
    parser.add_argument("--roles",
                        help="Comma-separated roles to prepare, essential included; default all five dependency roles. "
                             "Requires --prepare-only and --layout roles")
    parser.add_argument("--digests", default="",
                        help="Comma-separated role=sha256:<digest> pins of the role tags; empty pins none. "
                             "Requires --layout roles")
    args = parser.parse_args(argv)
    if args.github_env is not None and not args.prepare_only:
        parser.error("--github-env requires --prepare-only")
    if args.roles is None:
        args.roles = tuple(ROLE_ENV)
    else:
        if not args.prepare_only or args.layout != "roles":
            parser.error("--roles requires --prepare-only and --layout roles")
        try:
            args.roles = parse_roles(args.roles)
        except ValueError as error:
            parser.error(str(error))
    try:
        args.digests = parse_digests(args.digests)
    except ValueError as error:
        parser.error(str(error))
    if args.digests and args.layout != "roles":
        parser.error("--digests requires --layout roles")
    return args


def parse_roles(text):
    """Return declared roles in contract order, including its core role."""
    roles = text.split(",")
    if any(role not in ROLE_ENV for role in roles) or len(roles) != len(set(roles)) or "essential" not in roles:
        raise ValueError("--roles must list distinct roles of " + ",".join(ROLE_ENV)
                         + ", " + "essential" + " included")
    return tuple(role for role in ROLE_ENV if role in roles)


def parse_digests(text):
    """role=sha256:<hex> pins, comma-separated; an empty text pins none."""
    digests = {}
    for item in filter(None, (text or "").split(",")):
        role, _, digest = item.partition("=")
        if role not in ROLE_ENV or role in digests or DIGEST.fullmatch(digest) is None:
            raise ValueError("--digests takes distinct role=sha256:<64 hex> pins of " + ",".join(ROLE_ENV))
        digests[role] = digest
    return digests


def pin_references(references, digests):
    """Pin each role tag to its registry digest, so that every attempt of a
    campaign pulls the same manifest even when the tag moves. A pinned role
    that this run does not prepare is ignored."""
    return {role: reference.split("@", 1)[0] + "@" + digests[role] if role in digests else reference
            for role, reference in references.items()}


def published_role_pins(release, variant="official"):
    """Immutable indexes of the published role images."""
    data = json.loads((ROOT / ".github/ceph-role-images.json").read_text())
    pins = data.get("images", {}).get(release, {}).get(variant, {})
    if (data.get("schema") != 1 or set(pins) != set(ROLE_ENV) | {"all"}
            or any(not isinstance(value, str) or DIGEST.fullmatch(value) is None for value in pins.values())):
        raise ValueError("published role pins are incomplete or invalid for " + release + "/" + variant)
    return pins


def image_references(variant, layout, release, roles=None):
    roles = tuple(ROLE_ENV) if roles is None else tuple(roles)
    if release not in RELEASES or variant not in ("official", "debian", "ubuntu") or layout not in ("all", "roles"):
        raise ValueError("unsupported image matrix cell")
    if layout == "all" and roles != tuple(ROLE_ENV):
        raise ValueError("the all layout serves every role")
    pins = published_role_pins(release, variant)
    return {role: REGISTRY + "@" + pins[role] for role in (("all",) if layout == "all" else roles)}


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
             root / ".github/scripts/run_image_matrix.py",
             root / ".github/scripts/registry_pull.py",
             root / ".github/ceph-role-images.json"}
    for directory in ("ceph", "cephfs", "rgw", "rbd", "internal"):
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
    package_passes = []
    package_failure = False
    package = re.compile(r"^ok[ \t]+(" + "|".join(re.escape(module + "/" + name) for name in EXPECTED_PACKAGES)
                         + r")[ \t]+[0-9]+(?:\.[0-9]+)?s[ \t]*$")
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
            passed_package = package.fullmatch(line)
            if passed_package:
                package_passes.append(passed_package.group(1)[len(module) + 1:])
            if re.fullmatch(r"FAIL(?:[ \t]+.*)?", line):
                package_failure = True
    expected = set(EXPECTED_TESTS)
    missing = sorted(expected.difference(results))
    unexpected = sorted((set(results) | set(runs)).difference(expected))
    passed = (not missing and not unexpected and not duplicates and not failures and not skips
              and set(runs) == expected and len(runs) == len(expected)
              and all(status == "PASS" for status in results.values())
              and sorted(package_passes) == sorted(EXPECTED_PACKAGES) and not package_failure)
    return {"expected": list(EXPECTED_TESTS), "started": runs, "results": results,
            "missing": missing, "unexpected": unexpected, "duplicates": duplicates,
            "failed": failures, "skipped": skips, "package_passes": package_passes,
            "package_failure": package_failure, "passed": passed}


def append_role_environment(path, environment, roles=None):
    roles = tuple(ROLE_ENV) if roles is None else roles
    image_names = [ROLE_ENV[role] for role in ROLE_ENV if role in roles]
    names = image_names
    core = "essential"
    if (core not in roles or set(environment) != set(names)
            or any(not isinstance(environment[name], str)
                   or IMAGE_ID.fullmatch(environment[name]) is None for name in image_names)):
        raise RuntimeError("prepared environment must contain exactly the immutable image IDs of the declared roles")
    content = "".join(name + "=" + environment[name] + "\n" for name in names).encode("utf-8")
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NONBLOCK", 0), 0o600)
    try:
        original = os.fstat(descriptor)
        if not stat.S_ISREG(original.st_mode):
            raise RuntimeError("GitHub environment output must be a regular file")
        try:
            if os.write(descriptor, content) != len(content):
                raise OSError("incomplete GitHub role environment append")
        except BaseException:
            os.ftruncate(descriptor, original.st_size)
            raise
    finally:
        os.close(descriptor)


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
               "roles": list(args.roles),
               "requested_platform": args.platform, "image_builds": 0,
               "cleanup_owner": "Go test fixtures (this runner never prunes Docker resources)",
               "commands": [], "images": {}, "test_command": ["make", "image-compatibility"],
               "test_exit_code": None, "runtime_result": "not_run", "passed": False}
    if args.prepare_only:
        summary.update({"scope": "image_preparation", "preparation_passed": False,
                        "test_command": None,
                        "cleanup_owner": "caller (image preparation creates no containers or networks)"})
    code = 1

    def capture(command):
        started = time.monotonic()
        record = {"argv": command, "exit_code": None}
        summary["commands"].append(record)
        try:
            result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=60, check=False)
            record["exit_code"] = result.returncode
            return result
        finally:
            record["seconds"] = round(time.monotonic() - started, 3)

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
        for executable in (("docker",) if args.prepare_only else ("go", "make", "docker")):
            if shutil.which(executable) is None:
                raise RuntimeError(executable + " is required on the caller's PATH")
        if not args.prepare_only:
            go = capture(["go", "version"])
            if go.returncode:
                raise RuntimeError("caller Go is unavailable")
            summary["go_version"] = go.stdout.strip()
        preparation_started = time.monotonic()
        info_result = capture(["docker", "info", "--format", "{{json .}}"])
        if info_result.returncode:
            raise RuntimeError("Docker engine info failed")
        info = json.loads(info_result.stdout)
        platform = native_platform(info)
        summary["docker"] = {name: info.get(name) for name in
                             ("ServerVersion", "OSType", "Architecture", "NCPU", "MemTotal")}
        if args.prepare_only:
            summary["docker"]["ID"] = info.get("ID")
        summary["platform"] = platform
        if args.platform and args.platform != platform:
            raise RuntimeError("requested platform differs from Docker engine native platform; emulation is not validation")
        references = pin_references(image_references(args.variant, args.layout, args.release, args.roles),
                                    args.digests)
        if args.digests:
            summary["pinned_digests"] = args.digests

        for role, reference in references.items():
            def attempt(number, role=role, reference=reference):
                name = "pull-" + role + (".log" if number == 1 else "-" + str(number) + ".log")
                code = stream(["docker", "pull", "--platform", platform, reference], name)
                return code, (output / name).read_text(encoding="utf-8", errors="replace")

            try:
                summary["images"][role] = registry_pull.resolve(
                    role, reference, platform, lambda value: capture(["docker", "image", "inspect", "--", value]),
                    attempt, summary.setdefault("pulls", {}))
            except registry_pull.PreparationError as error:
                if error.failure:
                    summary["classification"] = "registry-transport"
                    summary["registry_failure"] = error.failure
                raise
        if not summary["pulls"]:
            del summary["pulls"]
        images = summary["images"]
        summary["image_preparation_seconds"] = round(time.monotonic() - preparation_started, 3)
        selected = {role: images["all" if args.layout == "all" else role]["image_id"]
                    for role in args.roles}
        environment = {ROLE_ENV[role]: image for role, image in selected.items()}
        if args.prepare_only:
            summary["prepared_environment"] = environment
            if args.github_env is not None:
                append_role_environment(args.github_env, environment, args.roles)
                summary["github_env"] = {"path": str(args.github_env), "appended_keys": list(environment)}
            summary["preparation_passed"] = True
            code = 0
        else:
            env = os.environ.copy()
            # Inherited make options can request dry runs or extra goals. This
            # runner invokes exactly the checked-in compatibility target.
            for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEFILES", "MAKEOVERRIDES", "GOFLAGS"):
                env.pop(name, None)
            summary["test_environment"] = environment
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
        label = "Image preparation" if args.prepare_only else "Image matrix"
        passed = summary["preparation_passed"] if args.prepare_only else summary["passed"]
        print(label + " result: " + ("PASS" if passed else "FAIL") + "; " + str(output), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
