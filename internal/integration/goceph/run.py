#!/usr/bin/env python3
"""Run supplied go-ceph client and test-runner images entirely on Linux.

The caller prepares the probe and checkout-specific runner images.
The harness neither builds images nor pulls missing client/runner images.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid


SOURCE_IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--client-image", default=os.environ.get("CEPH_TEST_GOCEPH_CLIENT_IMAGE"),
                        help="Existing Linux image containing /usr/local/bin/go-ceph-probe")
    parser.add_argument("--runner-image", default=os.environ.get("CEPH_TEST_GOCEPH_RUNNER_IMAGE"),
                        help="Existing Linux image containing this checkout's integration test binary")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--test-run", default="^TestGoCephLinux$")
    args = parser.parse_args(argv)
    for option in ("client_image", "runner_image"):
        if not getattr(args, option) or not getattr(args, option).strip():
            parser.error("--" + option.replace("_", "-") + " is required (or set CEPH_TEST_GOCEPH_" +
                         option.upper() + "); supply existing Linux client and runner images")
    return args


def main(argv=None):
    args = parse_arguments(argv)
    fixture = Path(__file__).resolve().parent
    root = fixture.parents[2]
    output = args.output_dir or root / "artifacts" / (
        "go-ceph-linux-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-" + uuid.uuid4().hex[:8])
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "source_image": os.environ.get("CEPH_TEST_IMAGE") or SOURCE_IMAGE,
               "client_image_reference": args.client_image, "runner_image_reference": args.runner_image,
               "expected_go_ceph": "v0.41.0", "expected_native_build_tag": "tentacle",
               "image_builds": 0, "commands": [], "passed": False}

    def capture(arguments):
        return subprocess.check_output([args.docker, *arguments], text=True).strip()

    def run(arguments, logfile):
        command = [args.docker, *arguments]
        started = time.monotonic()
        with (output / logfile).open("w") as log:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, bufsize=1)
            for line in process.stdout:
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
            code = process.wait()
        summary["commands"].append({"argv": command, "exit_code": code,
                                    "seconds": round(time.monotonic() - started, 3), "log": logfile})
        if code:
            raise subprocess.CalledProcessError(code, command)

    try:
        info = json.loads(capture(["info", "--format", "{{json .}}"] ))
        if info.get("OSType") != "linux":
            raise RuntimeError("this fixture requires a Docker Linux engine")
        summary["docker"] = {key: info.get(key) for key in
                             ("ServerVersion", "OSType", "Architecture", "NCPU", "MemTotal")}
        images = {}
        for target, image in (("client", args.client_image), ("runner", args.runner_image)):
            try:
                inspected = json.loads(capture(["image", "inspect", "--", image]))[0]
            except subprocess.CalledProcessError as error:
                raise RuntimeError(target + " image is unavailable locally; prepare or explicitly pull it " +
                                   "before running this harness") from error
            if inspected.get("Os") != "linux" or not re.fullmatch(r"sha256:[a-f0-9]{64}", inspected.get("Id", "")):
                raise RuntimeError(target + " image must be an existing Linux image with an immutable image ID")
            summary[target + "_image"] = inspected
            images[target] = inspected["Id"]
        env = {"DOCKER_HOST": "unix:///var/run/docker.sock",
               "TESTCONTAINERS_HOST_OVERRIDE": "127.0.0.1",
               "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE": "/var/run/docker.sock",
               "CEPH_TEST_GOCEPH_CLIENT_IMAGE": images["client"]}
        for name in ("CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE", "CEPH_TEST_RGW_IMAGE", "CEPH_TEST_MDS_IMAGE"):
            if os.environ.get(name):
                env[name] = os.environ[name]
        cidfile = output / "runner.cid"
        arguments = ["run", "--pull=never", "--rm", "--network=host", "--cidfile", str(cidfile),
                     "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock"]
        for name, value in env.items():
            arguments.extend(["-e", name + "=" + value])
        run([*arguments, images["runner"], "-test.v", "-test.failfast", "-test.timeout=40m", "-test.run=" + args.test_run],
            "integration.log")
        summary["passed"] = True
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        summary["error"] = str(error)
        print(str(error), file=sys.stderr)
    finally:
        # Inspect only containers created by this run; never prune other work.
        logpath = output / "integration.log"
        ids = set(re.findall(r"Container (?:created|started|stopped|terminated): ([a-f0-9]{12,64})",
                             logpath.read_text() if logpath.exists() else ""))
        cidfile = output / "runner.cid"
        if cidfile.exists():
            ids.add(cidfile.read_text().strip())
        summary["owned_container_ids"] = sorted(ids)
        try:
            # Ryuk exits after its reconnect grace period. Allow that normal
            # shutdown to finish before interpreting containers as leaks.
            deadline = time.monotonic() + 30
            while True:
                live = capture(["ps", "-aq"]).splitlines()
                remaining = [value for value in live
                             if any(value.startswith(owned) or owned.startswith(value) for owned in ids)]
                if not remaining or time.monotonic() >= deadline:
                    break
                time.sleep(1)
            summary["remaining_owned_container_ids"] = remaining
            if summary["remaining_owned_container_ids"]:
                summary["passed"] = False
        except (OSError, subprocess.CalledProcessError) as error:
            summary["cleanup_inspection_error"] = str(error)
            summary["passed"] = False
        summary["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print("Linux go-ceph result: " + ("PASS" if summary["passed"] else "FAIL") + "; " + str(output), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
