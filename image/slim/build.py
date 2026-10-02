#!/usr/bin/env python3
"""Build five local Ceph role images from an RPM image or local Debian packages.

Only Docker and Python 3.9+ are required on the host. Local .deb installation may
fetch distro dependencies; inspection and assembly then run offline in Linux.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import uuid


ROLES = ("control", "osd", "rgw", "mds", "all")
HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent.parent


class BuildError(RuntimeError):
    pass


def run(args, log=None, stdin=None, capture=False, env=None, cwd=None):
    """Never interpolate image names or paths into a host shell."""
    if capture:
        result = subprocess.run(args, check=False, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=env, cwd=cwd)
        if result.returncode:
            raise BuildError("Command failed: " + " ".join(args) + "\n" + result.stderr.strip())
        return result.stdout.strip()
    with open(log, "w") if log else tempfile.TemporaryFile(mode="w+") as output:
        with open(stdin, "rb") if stdin else tempfile.TemporaryFile() as input_file:
            with subprocess.Popen(args, stdin=input_file, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, errors="replace",
                                  env=env, cwd=cwd) as proc:
                try:
                    for line in proc.stdout:
                        print(line, end="", flush=True)
                        output.write(line)
                    code = proc.wait()
                except BaseException:
                    proc.terminate()
                    try:
                        proc.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    raise
            if code:
                raise BuildError("Command failed (%d): %s; log: %s" % (code, " ".join(args), log))
    return ""


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def inspect_image(image, platform=None):
    args = ["docker", "image", "inspect"]
    if platform:
        args += ["--platform", platform]
    return json.loads(run(args + [image], capture=True))[0]


def pull_image(image, args, log):
    if not args.skip_pull:
        command = ["docker", "pull"]
        if args.platform:
            command += ["--platform", args.platform]
        run(command + [image], log=log)


def deb_inputs(args):
    files = args.deb_packages or sorted(args.deb_directory.glob("*.deb"))
    if not files:
        raise BuildError("No .deb packages were supplied")
    inputs = []
    seen = set()
    for supplied in files:
        path = supplied.resolve(strict=True)
        if not path.is_file() or path.suffix != ".deb":
            raise BuildError("Expected a regular .deb file: " + str(supplied))
        if path in seen:
            raise BuildError("Package file supplied twice: " + str(supplied))
        seen.add(path)
        digest = hashlib.sha256()
        with path.open("rb") as package:
            for block in iter(lambda: package.read(1024 * 1024), b""):
                digest.update(block)
        inputs.append((path, {"filename": supplied.name,
                              "staged_filename": "%04d.deb" % len(inputs),
                              "sha256": digest.hexdigest()}))
    return inputs


def prepare_deb_source(args, output, timestamp, staging_tag, report):
    inputs = deb_inputs(args)
    pull_image(args.base_image, args, output / "base-pull.log")
    base = inspect_image(args.base_image, args.platform)
    if base["Os"] != "linux":
        raise BuildError("Debian packages require a Linux Debian/Ubuntu base image")
    platform = args.platform or "linux/" + base["Architecture"]
    save_json(output / "base-image.json", base)
    provenance = {"base_image": args.base_image, "base_image_id": base["Id"],
                  "base_image_digests": base.get("RepoDigests", []), "platform": platform,
                  "packages": [metadata for _, metadata in inputs]}
    save_json(output / "deb-input.json", provenance)
    base_tag = "ceph-testcontainers-deb-base:" + uuid.uuid4().hex
    # Containerd's platform-specific inspect ID may identify an untaggable
    # child manifest. Pin the locally addressable index/digest instead.
    base_reference = next(iter(base.get("RepoDigests", [])), None) or inspect_image(args.base_image)["Id"]
    run(["docker", "tag", base_reference, base_tag])
    try:
        with tempfile.TemporaryDirectory(prefix="ceph-deb-source-") as directory:
            context = Path(directory)
            (context / "debs").mkdir()
            for path, metadata in inputs:
                shutil.copyfile(path, context / "debs" / metadata["staged_filename"])
            shutil.copyfile(HERE / "install_debs.py", context / "install_debs.py")
            save_json(context / "input.json", provenance)
            generated = "\n".join([
                "# Generated local package installation stage; excluded from role images.",
                "ARG BASE_IMAGE=scratch", "FROM ${BASE_IMAGE}",
                "ENV DEBIAN_FRONTEND=noninteractive",
                "RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates gawk",
                'COPY ["install_debs.py", "/tmp/install_debs.py"]',
                'COPY ["debs/", "/tmp/ceph-debs/"]',
                'COPY ["input.json", "/tmp/ceph-input.json"]',
                "RUN python3 /tmp/install_debs.py --directory /tmp/ceph-debs --input /tmp/ceph-input.json --output /usr/share/ceph-testcontainers/source-packages.json && rm -rf /tmp/ceph-debs /tmp/ceph-input.json /tmp/install_debs.py /var/lib/apt/lists/*",
                'LABEL io.ceph-testcontainers.package-manager="dpkg"',
                "LABEL io.ceph-testcontainers.slim-build=" + json.dumps(timestamp),
            ]) + "\n"
            (context / "Dockerfile").write_text(generated)
            (output / "Dockerfile.source.generated").write_text(generated)
            run(["docker", "buildx", "build", "--load", "--no-cache", "--platform", platform,
                 "--provenance=false", "--build-arg", "BASE_IMAGE=" + base_tag,
                 "-t", staging_tag, str(context)], log=output / "deb-install.log")
    finally:
        cleanup = subprocess.run(["docker", "image", "rm", base_tag], text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output / "base-tag-cleanup.log").write_text(cleanup.stdout + cleanup.stderr)
        if cleanup.returncode:
            report["base_tag_cleanup_error"] = cleanup.stderr.strip()
    return inspect_image(staging_tag, platform), platform


def archive_path(value, prefix):
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or len(path.parts) != 2:
        raise BuildError("Invalid archive path in assembly plan: " + value)
    if path.parts[0] != prefix or path.suffix != ".tar":
        raise BuildError("Invalid archive path in assembly plan: " + value)
    return path.as_posix()


def dockerfile(plan):
    """Reuse identical independent tar layers across every target, including all."""
    groups = plan["groups"]
    ordered = plan["ordered_groups"]
    if not ordered or ordered[0] != "common" or set(ordered) != set(groups):
        raise BuildError("Assembly plan must start with a single common group")
    for name in ordered:
        if not re.fullmatch(r"[a-z][a-z0-9-]*", name):
            raise BuildError("Invalid group name: " + name)
    common_tar = archive_path(groups["common"]["archive"], "groups")
    lines = [
        "# Generated by image/slim/build.py; input paths are Linux-created tar archives.",
        "FROM scratch AS common",
        "ADD --link %s /" % common_tar,
        'ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin LANG=C.UTF-8',
        'CMD ["/bin/bash"]',
        "LABEL org.opencontainers.image.base.name=" + json.dumps(plan["source_image"]),
    ]
    for role in ROLES:
        selected = plan["roles"][role]["groups"]
        if selected != [group for group in ordered if group in selected] or selected[0] != "common":
            raise BuildError("Invalid group ordering for role " + role)
        lines += ["", "FROM common AS " + role]
        for group in selected[1:]:
            lines.append("ADD --link %s /" % archive_path(groups[group]["archive"], "groups"))
        lines.append("ADD --link %s /" % archive_path(plan["roles"][role]["manifest_archive"], "manifests"))
        lines += [
            "LABEL org.opencontainers.image.title=" + json.dumps("Ceph testcontainers " + role),
            "LABEL org.opencontainers.image.version=" + json.dumps(plan["ceph_version"]),
            "LABEL io.ceph-testcontainers.role=" + json.dumps(role),
        ]
    return "\n".join(lines) + "\n"


def check_layers(plan, images):
    by_group = {}
    uses = {}
    for role in ROLES:
        selected = plan["roles"][role]["groups"]
        layers = images[role]["rootfs_diff_ids"]
        if len(layers) != len(selected) + 1:
            raise BuildError("Unexpected filesystem layer count for " + role)
        for group, diff_id in zip(selected, layers):
            if group in by_group and by_group[group] != diff_id:
                raise BuildError("Common payload layer was not reused: " + group)
            by_group[group] = diff_id
        for diff_id in layers:
            uses.setdefault(diff_id, []).append(role)
    return {
        "group_diff_ids": by_group,
        "diff_id_roles": uses,
        "unique_diff_id_count": len(uses),
        "shared_payload_group_count": sum(len(g["members"]) > 1 for g in plan["groups"].values()),
        "note": "DiffID verifies identical unpacked layer content; local snapshot usage and registry transfer are not measured",
    }


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-image", help="Quay Ceph RPM tag or immutable digest")
    source.add_argument("--deb-packages", nargs="+", type=Path, help="Local Ceph .deb files; shell globs are accepted")
    source.add_argument("--deb-directory", type=Path, help="Directory containing local Ceph .deb files (not recursive)")
    parser.add_argument("--base-image", help="Matching Debian/Ubuntu base image; required for .deb inputs")
    parser.add_argument("--repository", default="ceph-testcontainers", help="Local output repository; no push is performed")
    parser.add_argument("--tag", help="Output tag prefix; defaults to actual Ceph version")
    parser.add_argument("--platform", help="One platform per run, e.g. linux/arm64 or linux/amd64")
    parser.add_argument("--output-dir", type=Path, help="Empty directory for manifests, build/smoke logs and report")
    parser.add_argument("--skip-pull", action="store_true", help="Use an already cached source or base image")
    parser.add_argument("--skip-smoke", action="store_true", help="Record smoke validation as skipped")
    parser.add_argument("--integration", action="store_true", help="Run Go integration suite with mixed role images and then all")
    parser.add_argument("--multicluster", action="store_true", help="Run multi-cluster Go tests with mixed roles and control mirror image")
    parser.add_argument("--go-command", default="go", help="Go executable used only for integration checks")
    parser.add_argument("--keep-context", action="store_true", help="Keep generated tar build context in the output directory")
    args = parser.parse_args(argv)
    if bool(args.base_image) != bool(args.deb_packages or args.deb_directory):
        parser.error("--base-image is required with .deb inputs and cannot be used with --source-image")
    return args


def main():
    args = arguments()
    for name in ("docker",):
        if not shutil.which(name):
            raise BuildError(name + " is required")
    if (args.integration or args.multicluster) and not shutil.which(args.go_command):
        raise BuildError("Go executable not found: " + args.go_command)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output_dir or PROJECT / "artifacts" / ("slim-" + timestamp + "-" + uuid.uuid4().hex[:8])).resolve()
    if output.exists() and any(output.iterdir()):
        raise BuildError("Output directory must be empty: " + str(output))
    output.mkdir(parents=True, exist_ok=True)
    report = {"status": "running", "source_input": args.source_image or args.base_image,
              "package_manager": "rpm" if args.source_image else "dpkg", "started_at_utc": timestamp,
              "output_directory": str(output), "checks": {
                  "smoke": "skipped" if args.skip_smoke else "pending",
                  "mixed_integration": "pending" if args.integration else "not_requested",
                  "all_integration": "pending" if args.integration else "not_requested",
                  "multicluster_integration": "pending" if args.multicluster else "not_requested",
              }}
    save_json(output / "build-report.json", report)
    container_id = None
    staging_tag = None
    try:
        run(["docker", "version"], log=output / "docker-version.log")
        run(["docker", "buildx", "version"], log=output / "buildx-version.log")
        if args.source_image:
            pull_image(args.source_image, args, output / "source-pull.log")
            source = inspect_image(args.source_image, args.platform)
            immutable = args.source_image if "@sha256:" in args.source_image else next(iter(source.get("RepoDigests", [])), source["Id"])
            platform = args.platform or "linux/" + source["Architecture"]
        else:
            staging_tag = "ceph-testcontainers-deb-source:" + uuid.uuid4().hex
            source, platform = prepare_deb_source(args, output, timestamp, staging_tag, report)
            immutable = source["Id"]
        if source["Os"] != "linux":
            raise BuildError("The source must be a Linux Ceph image")
        report["source"] = {"resolved_image": immutable, "image_id": source["Id"], "platform": platform,
                            "architecture": source["Architecture"], "local_size_bytes": source["Size"]}
        save_json(output / "source-image.json", source)
        with tempfile.TemporaryDirectory(prefix="ceph-slim-") as work:
            work = Path(work)
            context = work / "context"
            context.mkdir()
            scripts = work / "ceph-slim"
            scripts.mkdir()
            for name in ("assemble.py", "analyze.py", "package_roles.py", "debian_packages.py"):
                shutil.copy2(HERE / name, scripts / name)
            create = ["docker", "create", "--network=none", "--platform", platform,
                      "--label", "io.ceph-testcontainers.slim-build=" + timestamp,
                      "-e", "SOURCE_IMAGE=" + immutable, "-e", "SOURCE_IMAGE_ID=" + source["Id"]]
            if not args.source_image:
                create += ["-e", "PACKAGE_BACKEND=dpkg", "-e",
                           "LOCAL_PACKAGE_PROVENANCE=/usr/share/ceph-testcontainers/source-packages.json"]
            create += ["--entrypoint", "python3", immutable, "/tmp/ceph-slim/package_roles.py"]
            container_id = run(create, capture=True)
            run(["docker", "cp", str(scripts), container_id + ":/tmp/ceph-slim"], log=output / "source-copy.log")
            print("Assembling package closures and shared file groups...", flush=True)
            run(["docker", "start", "-a", container_id], log=output / "assembly.log")
            exit_code = run(["docker", "inspect", "--format", "{{.State.ExitCode}}", container_id], capture=True)
            if exit_code != "0":
                raise BuildError("Source assembly failed with exit code " + exit_code)
            run(["docker", "cp", container_id + ":/role-output/plan.json", str(context / "plan.json")])
            plan = json.loads((context / "plan.json").read_text())
            if plan["source_image"] != immutable or plan["oci_architecture"] != source["Architecture"]:
                raise BuildError("Assembly source/architecture differs from the inspected source image")
            release = re.match(r"ceph version (\S+)", plan["ceph_version"])
            if not release:
                raise BuildError("Unexpected Ceph version output: " + plan["ceph_version"])
            tag = args.tag or release.group(1)
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,119}", tag):
                raise BuildError("Invalid output tag prefix; specify --tag")
            tags = {role: "%s:%s-%s" % (args.repository, tag, role) for role in ROLES}
            report.update({"ceph_version": plan["ceph_version"], "tags": tags})
            if not args.source_image:
                report["local_package_provenance"] = plan["local_package_provenance"]
                save_json(output / "source-packages.json", plan["local_package_provenance"])
            save_json(output / "plan.json", plan)
            archives = {archive_path(g["archive"], "groups") for g in plan["groups"].values()}
            archives.update(archive_path(plan["roles"][role]["manifest_archive"], "manifests") for role in ROLES)
            for archive in sorted(archives):
                target = context / archive
                target.parent.mkdir(exist_ok=True)
                run(["docker", "cp", container_id + ":/role-output/" + archive, str(target)])
            run(["docker", "rm", container_id])
            container_id = None
            generated = dockerfile(plan)
            (context / "Dockerfile").write_text(generated)
            (output / "Dockerfile.generated").write_text(generated)
            report["images"] = {}
            for role in ROLES:
                print("Building " + tags[role], flush=True)
                run(["docker", "buildx", "build", "--load", "--network=none", "--platform", platform,
                     "--provenance=false", "--target", role, "-t", tags[role], str(context)], log=output / ("build-" + role + ".log"))
                built = inspect_image(tags[role], platform)
                report["images"][role] = {"tag": tags[role], "image_id": built["Id"], "architecture": built["Architecture"],
                                           "local_size_bytes": built["Size"], "rootfs_diff_ids": built["RootFS"]["Layers"],
                                           "logical_regular_file_bytes": plan["roles"][role]["logical_regular_file_bytes"]}
                save_json(output / "build-report.json", report)
            report["layer_sharing"] = check_layers(plan, report["images"])
            if args.keep_context:
                shutil.copytree(context, output / "context")
        if args.skip_smoke:
            report["checks"]["smoke"] = "skipped"
        else:
            report["checks"]["smoke"] = "running"
            save_json(output / "build-report.json", report)
            for role in ROLES:
                print("Smoke testing " + role, flush=True)
                run(["docker", "run", "--rm", "-i", "--network=none", "--platform", platform,
                     "--entrypoint", "/bin/sh", tags[role], "-s", "--", role],
                    stdin=HERE / "smoke-role.sh", log=output / ("smoke-" + role + ".log"))
            report["checks"]["smoke"] = "passed"
        if args.integration:
            env = dict(os.environ, CGO_ENABLED="0", CEPH_TEST_IMAGE=tags["control"],
                       CEPH_TEST_OSD_IMAGE=tags["osd"], CEPH_TEST_RGW_IMAGE=tags["rgw"], CEPH_TEST_MDS_IMAGE=tags["mds"])
            test = [args.go_command, "test", "-tags=integration", "-count=1", "-v", "-timeout=20m", "./internal/integration"]
            print("Testing mixed control/osd/rgw/mds images...", flush=True)
            report["checks"]["mixed_integration"] = "running"
            save_json(output / "build-report.json", report)
            run(test, env=env, cwd=PROJECT, log=output / "integration-mixed.log")
            report["checks"]["mixed_integration"] = "passed"
            env.update({name: tags["all"] for name in ("CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE", "CEPH_TEST_RGW_IMAGE", "CEPH_TEST_MDS_IMAGE")})
            print("Testing all image...", flush=True)
            report["checks"]["all_integration"] = "running"
            save_json(output / "build-report.json", report)
            run(test, env=env, cwd=PROJECT, log=output / "integration-all.log")
            report["checks"]["all_integration"] = "passed"
        if args.multicluster:
            env = dict(os.environ, CGO_ENABLED="0", CEPH_TEST_IMAGE=tags["control"],
                       CEPH_TEST_OSD_IMAGE=tags["osd"], CEPH_TEST_RGW_IMAGE=tags["rgw"],
                       CEPH_TEST_MDS_IMAGE=tags["mds"], CEPH_TEST_MIRROR_IMAGE=tags["control"])
            test = [args.go_command, "test", "-tags=integration,multicluster", "-run", "^TestMultiCluster",
                    "-count=1", "-v", "-timeout=60m", "./internal/integration"]
            print("Testing multiple clusters with mixed roles and control mirror image...", flush=True)
            report["checks"]["multicluster_integration"] = "running"
            save_json(output / "build-report.json", report)
            run(test, env=env, cwd=PROJECT, log=output / "integration-multicluster.log")
            report["checks"]["multicluster_integration"] = "passed"
        report["status"] = "passed"
        print("Built five images; report: " + str(output / "build-report.json"), flush=True)
        print(json.dumps(tags, indent=2), flush=True)
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = str(error)
        for check, state in report["checks"].items():
            if state in ("running", "pending"):
                report["checks"][check] = "failed" if state == "running" else "not_run"
        raise
    finally:
        if container_id:
            cleanup = subprocess.run(["docker", "rm", "-f", container_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if cleanup.returncode:
                report["assembly_container_cleanup_error"] = cleanup.stderr.strip()
        if staging_tag:
            cleanup = subprocess.run(["docker", "image", "rm", staging_tag], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if cleanup.returncode:
                report["source_stage_cleanup_error"] = cleanup.stderr.strip()
        save_json(output / "build-report.json", report)


if __name__ == "__main__":
    try:
        main()
    except (BuildError, OSError, ValueError, KeyError) as error:
        print("Ceph slim build failed: " + str(error), file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Ceph slim build interrupted", file=sys.stderr)
        sys.exit(130)
