#!/usr/bin/env python3
"""Pull the images that a runtime batch's tests would pull themselves.

testcontainers-go pulls an absent image when a test creates its container:
Ryuk for the reaper, and every image that a test names besides its role
images, such as an older official release, a netem helper or busybox. Docker
reports a registry transport error of such a pull as HTTP 500, which
testcontainers-go does not retry, so the test fails before its own assertions
(issue 381). This script pulls those images before the tests start, through
registry_pull's bounded retry of transport failures, and records them.

The images come from the source, not from a list: batchselect -images walks
the declarations reachable from the batch's compiled tests and reports each
image reference among their constants, a reference prefix that a test
completes with the Ceph version at run time, and a read of the pinned role
image file. Ryuk is the ReaperDefaultImage of the testcontainers-go version in
go.mod. Nothing here creates a container or removes an image.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import registry_pull
import run_image_matrix as matrix
import tag_scenarios


ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 1
TESTCONTAINERS = "github.com/testcontainers/testcontainers-go"
# testcontainers-go names its Ryuk image in this file and constant.
REAPER_SOURCE = "internal/config/config.go"
REAPER_CONSTANT = re.compile(r'^\s*const\s+ReaperDefaultImage\s*=\s*"([a-z0-9][a-z0-9./_-]*:[A-Za-z0-9._-]+)"',
                             re.MULTILINE)
ANALYSIS_SCHEMA = "ceph-test-images/v1"
ANALYSIS_SECONDS = 600
COMMAND_SECONDS = 60


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(command, timeout=COMMAND_SECONDS):
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False)


def reaper_reference(listed):
    """The Ryuk image of the testcontainers-go module directory that
    `go list -m -f '{{.Dir}}'` printed as listed."""
    directory = Path(listed.strip())
    if not directory.is_absolute():
        raise ValueError("go list printed no module directory for " + TESTCONTAINERS)
    try:
        text = (directory / REAPER_SOURCE).read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError("cannot read the Ryuk image of " + TESTCONTAINERS + ": " + type(error).__name__) from None
    found = REAPER_CONSTANT.findall(text)
    if len(found) != 1:
        raise ValueError(TESTCONTAINERS + " names no single ReaperDefaultImage in " + REAPER_SOURCE)
    return found[0]


def compiled_profile(directory, selection):
    """Package, tags and the tests that this job runs, from the compile
    receipt; selection, when not empty, is the matrix entry's subset."""
    report = json.loads((Path(directory) / "compile-report.json").read_text(encoding="utf-8"))
    binary = report.get("binary") if isinstance(report, dict) else None
    if not isinstance(binary, dict) or report.get("passed") is not True:
        raise ValueError("the compile receipt records no built binary")
    profile, tests = report.get("profile") or {}, binary.get("tests")
    if not isinstance(profile.get("package"), str) or not isinstance(profile.get("tags"), str) \
            or not isinstance(tests, list) or not tests:
        raise ValueError("the compile receipt names no package, tags or tests")
    if selection:
        chosen = selection.split(",")
        if not set(chosen) <= set(tests):
            raise ValueError("the test selection names tests that were not compiled")
        tests = chosen
    return profile["package"], profile["tags"], sorted(tests)


def analysis_command(package, tags, tests):
    command = ["go", "run", "-mod=readonly", "./internal/tools/batchselect", "-root", str(ROOT),
               "-package", package, "-images", tags, "-tests", ",".join(tests)]
    if tag_scenarios.integration_package(package):
        command += ["-with", ",".join(tag_scenarios.INTEGRATION_HARNESS)]
    return command


def validate_analysis(analysis, package, tests):
    if (not isinstance(analysis, dict) or analysis.get("schema") != ANALYSIS_SCHEMA
            or analysis.get("package") != package or analysis.get("tests") != tests
            or not isinstance(analysis.get("images"), list)):
        raise ValueError("the image analysis does not describe this package and these tests")
    for use in analysis["images"]:
        if not isinstance(use, dict) or use.get("kind") not in ("image", "prefix", "role-pins") \
                or not isinstance(use.get("tests"), list):
            raise ValueError("the image analysis has an invalid entry")
    return analysis["images"]


def published_role_digests():
    digests = set()
    for release in matrix.RELEASES:
        for variant in ("official", "debian", "ubuntu"):
            try:
                digests.update(matrix.published_role_pins(release, variant).values())
            except ValueError:
                continue
    return digests


def plan_images(uses, release, roles, roles_prepared):
    """(planned, skipped): the references to prepare, each with the
    analysis entries that named it, and the entries that need none."""
    planned, skipped = {}, []
    published = published_role_digests() if roles_prepared else set()

    def add(reference, use, note=None):
        entry = planned.setdefault(reference, {"reference": reference, "kinds": [], "tests": [], "positions": []})
        for field, value in (("kinds", use["kind"]), ("positions", use.get("position"))):
            if value and value not in entry[field]:
                entry[field].append(value)
        entry["tests"] = sorted(set(entry["tests"]) | set(use["tests"]))
        if note:
            entry["note"] = note

    for use in uses:
        reference = use.get("reference", "")
        if use["kind"] == "image":
            repository, _, digest = reference.partition("@")
            if roles_prepared and repository.split(":")[0] == matrix.REGISTRY and digest in published:
                # With prepared role images a test refuses this fallback
                # (harness.IntegrationImage); the role preparation owns them.
                skipped.append({"reference": reference, "position": use.get("position"),
                                "reason": "published role image; the job's role images replace it"})
                continue
            add(reference, use)
        elif use["kind"] == "prefix":
            official = matrix.RELEASES[release]
            tagged, _, digest = official.partition("@")
            if reference + release != tagged:
                raise ValueError("a test completes the image prefix %r at run time (%s), which this preparation "
                                 "cannot name" % (reference, use.get("position")))
            # The test names the tag, so the tag is pulled; the pin says
            # which manifest that tag should resolve to.
            add(tagged, use, "official image of " + release + " by tag; pinned " + digest)
        else:
            for each in matrix.RELEASES:
                pins = matrix.published_role_pins(each)
                for role in roles:
                    add(matrix.REGISTRY + "@" + pins[role], use, "pinned " + role + " role image of " + each)
    return list(planned.values()), skipped


def prepare(arguments, environ=os.environ, runner=run, sleep=None):
    directory = Path(arguments.directory)
    directory.mkdir(parents=True, exist_ok=False)
    summary = {"schema": SCHEMA, "started_at": timestamp(), "release": arguments.release, "passed": False,
               "commands": [], "pulls": {}, "images": [], "skipped": []}

    def capture(command, timeout=COMMAND_SECONDS):
        record = {"argv": command, "exit_code": None}
        summary["commands"].append(record)
        result = runner(command, timeout)
        record["exit_code"] = result.returncode
        return result

    code = 1
    try:
        if arguments.release not in matrix.RELEASES:
            raise ValueError("unsupported release " + arguments.release)
        roles = matrix.parse_roles(arguments.roles) if arguments.roles else ()
        roles_prepared = environ.get("SCENARIO_IMAGE_LAYOUT") == "roles"
        package, tags, tests = compiled_profile(arguments.compile, arguments.tests)
        summary |= {"package": package, "tags": tags, "tests": tests, "roles_prepared": roles_prepared}
        analysed = capture(analysis_command(package, tags, tests), ANALYSIS_SECONDS)
        if analysed.returncode:
            raise ValueError("the image analysis failed: " + analysed.stderr.strip()[-500:])
        uses = validate_analysis(json.loads(analysed.stdout), package, tests)
        summary["analysis"] = uses
        planned, summary["skipped"] = plan_images(uses, arguments.release, roles, roles_prepared)
        listed = capture(["go", "list", "-m", "-f", "{{.Dir}}", TESTCONTAINERS])
        if listed.returncode:
            raise ValueError("go list cannot locate " + TESTCONTAINERS)
        reaper = reaper_reference(listed.stdout)
        info = capture(["docker", "info", "--format", "{{json .}}"])
        if info.returncode:
            raise ValueError("Docker engine info failed")
        platform = matrix.native_platform(json.loads(info.stdout))
        summary["platform"] = platform

        def resolve(name, reference):
            def attempt(number):
                log = "pull-" + name + (".log" if number == 1 else "-" + str(number) + ".log")
                result = capture(["docker", "pull", "--platform", platform, reference], None)
                text = (result.stdout or "") + (result.stderr or "")
                (directory / log).write_text(text, encoding="utf-8")
                print(text, end="", flush=True)
                return result.returncode, text

            return registry_pull.resolve(name, reference, platform,
                                         lambda value: capture(["docker", "image", "inspect", "--", value]),
                                         attempt, summary["pulls"], sleep=sleep)

        # Ryuk is no test's image: every testcontainers-go session starts it.
        summary["reaper"] = resolve("reaper", reaper)
        for index, entry in enumerate(planned, 1):
            summary["images"].append(entry | resolve("image-" + str(index), entry["reference"]))
        summary["passed"] = True
        code = 0
    except registry_pull.PreparationError as error:
        summary["error"] = str(error)
        if error.failure:
            summary["classification"] = "registry-transport"
            summary["registry_failure"] = error.failure
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        summary["error"] = str(error)
    finally:
        summary["finished_at"] = timestamp()
        with (directory / "summary.json").open("x", encoding="utf-8") as output:
            json.dump(summary, output, indent=2)
            output.write("\n")
    if code:
        print("Test image preparation FAIL: " + summary["error"], file=sys.stderr)
    else:
        print("Test image preparation PASS: Ryuk and %d test image(s), %d skipped; %s"
              % (len(summary["images"]), len(summary["skipped"]), directory / "summary.json"), flush=True)
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compile", type=Path, required=True, help="The compile directory with compile-report.json")
    parser.add_argument("--tests", default="", help="The matrix entry's comma-separated test selection")
    parser.add_argument("--release", required=True)
    parser.add_argument("--roles", default="", help="The batch's declared role images, for the pinned role file")
    parser.add_argument("--directory", type=Path, required=True, help="New directory for the receipt and pull logs")
    return prepare(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
