#!/usr/bin/env python3
"""Discover, compile-check and execute source-owned Go test tag profiles.

Only build tags select runtime tests. Source metadata owns process and job
budgets; command-line and workflow inputs cannot replace test selection.
"""

import argparse
from collections import Counter, defaultdict
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

import run_image_matrix as images


ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ("code", "environment", "short", "topology", "multicluster", "recovery")
RUNTIME_CATEGORIES = tuple(category for category in CATEGORIES if category != "code")
CAPABILITY_TAGS = ("integration", "auth", "features", "topology", "hostnetwork", "multicluster", "diagnostics")
CHECKERS = {
    "quiescence": ("check_scenario_quiescence", "TestMultiClusterCephFSOriginalProcessQuiescence"),
    "recovery": ("check_scenario_recovery", "TestMultiClusterCephFSOriginalProcessQuiescenceRecovery"),
    "receivers": ("check_scenario_receivers", "TestMultiClusterRBDReceiverReadiness"),
}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*\Z")
# Published role images, in the order of their CEPH_TEST_* variables. A Ceph
# batch that declares no image-roles gets all five dependency roles.
IMAGE_ROLES = ("essential", "client", "cephfs", "rbd-aux", "rgw")
# Docker network modes a test can declare with networkModes(t, ...).
NETWORKS = ("host", "bridge")
NETWORK_SCOPES = ("default", "all")
# Themes of the representative set (//ci: representative=<theme>) that the
# periodic campaign repeats; the set must cover every theme.
REPRESENTATIVE_THEMES = ("topology", "auth", "mirroring", "recovery", "flaky")
# Bounds of one representative campaign: repetitions of each batch, matrix
# jobs (GitHub allows 256 per matrix) and summed job-timeout minutes.
MAX_REPETITIONS = 10
MAX_CAMPAIGN_JOBS = 100
MAX_CAMPAIGN_MINUTES = 3000
DURATION = re.compile(r"([1-9][0-9]*)(ms|s|m|h)\Z")
SCHEMA = "ceph-tag-scenarios/v1"
# Bounds of the two preparation phases. Module preparation is the only phase
# that reaches the network; the compile then reads the module cache.
MODULES_TIMEOUT_SECONDS = 300
COMPILE_TIMEOUT_SECONDS = 180
GO_EXECUTION_ENV = {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"}
# Native execution formats: the go test driver, or the compile phase's test
# binary run directly (run --binary).
GO_TEST, TEST_BINARY = "go-test", "test-binary"
EXECUTIONS = (GO_TEST, TEST_BINARY)
# The package whose batches internal/tools/batchselect analyses, and its bound: the
# first run compiles the export data of every dependency.
DECLARATION_PACKAGE = "./internal/integration"
DECLARATION_TIMEOUT_SECONDS = 600
# The files that a build reads from each package that go list -deps -test
# reports. The test variants of the profile's package already carry its test
# files; the Test* fields of a dependency are not part of this build.
BUILD_FILE_FIELDS = ("GoFiles", "CgoFiles", "CFiles", "CXXFiles", "MFiles", "HFiles", "FFiles", "SFiles",
                     "SwigFiles", "SwigCXXFiles", "SysoFiles", "EmbedFiles")
GO_PLATFORMS = frozenset((
    "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios", "js", "linux", "netbsd",
    "openbsd", "plan9", "solaris", "wasip1", "windows", "386", "amd64", "arm", "arm64", "loong64",
    "mips", "mipsle", "mips64", "mips64le", "ppc64", "ppc64le", "riscv64", "s390x", "wasm",
))
GO_TOOL_TAGS = frozenset(("cgo", "gc", "gccgo", "unix", "race", "msan", "asan", "boringcrypto"))
# The verbose result lines of one test or subtest; scenario_history.py reads
# the same lines for per-test durations.
RUN_LINE = re.compile(r"=== RUN\s+(Test\S+)")
RESULT_LINE = re.compile(r"\s*--- (PASS|FAIL|SKIP): (Test\S+) \(([0-9.]+)s\)")


def go_environment():
    # A nonempty GOFLAGS also overrides persisted GOENV flags such as -overlay.
    # Keep proxy/cache settings, Docker connectivity and supplied role images.
    return os.environ | GO_EXECUTION_ENV


def reserved_go_tag(value):
    return (value in GO_PLATFORMS or value in GO_TOOL_TAGS or re.fullmatch(r"go1\.[0-9]+", value) is not None
            or any(value.startswith(architecture + ".") for architecture in GO_PLATFORMS))


def evaluate(expression, tags):
    if expression is None:
        return True
    op = expression["op"]
    children = expression.get("children", [])
    if op == "tag":
        return expression["tag"] in tags
    if op == "not" and len(children) == 1:
        return not evaluate(children[0], tags)
    if op in ("and", "or") and len(children) == 2:
        left, right = (evaluate(child, tags) for child in children)
        return (left and right) if op == "and" else (left or right)
    raise ValueError("invalid Go build expression catalog")


def parse_metadata(file):
    rows = file.get("metadata", [])
    if len(rows) != 1:
        raise ValueError(file["path"] + ": runtime test needs exactly one //ci: metadata line")
    values = {}
    for word in rows[0].split():
        if word.count("=") != 1:
            raise ValueError(file["path"] + ": malformed CI metadata")
        key, value = word.split("=", 1)
        if (key not in ("timeout", "job-timeout", "checker", "case", "image-roles", "representative")
                or key in values or not value):
            raise ValueError(file["path"] + ": unknown, duplicate or empty CI metadata")
        values[key] = value
    if not {"timeout", "job-timeout"} <= values.keys():
        raise ValueError(file["path"] + ": missing CI execution budget")
    duration = DURATION.fullmatch(values["timeout"])
    if duration is None:
        raise ValueError(file["path"] + ": invalid Go process timeout")
    seconds = int(duration[1]) * {"ms": .001, "s": 1, "m": 60, "h": 3600}[duration[2]]
    if not 1 <= seconds <= 6 * 3600:
        raise ValueError(file["path"] + ": process timeout is outside the runner budget")
    if not re.fullmatch(r"[1-9][0-9]{0,2}", values["job-timeout"]):
        raise ValueError(file["path"] + ": invalid job timeout")
    job_timeout = int(values["job-timeout"])
    if not 1 <= job_timeout <= 360 or seconds >= job_timeout * 60:
        raise ValueError(file["path"] + ": job timeout must exceed the process timeout")
    checker = values.get("checker", "none")
    case = values.get("case", "")
    if checker == "none":
        if case:
            raise ValueError(file["path"] + ": checker case requires a checker")
    else:
        if checker not in CHECKERS:
            raise ValueError(file["path"] + ": unknown completion checker")
        if not file.get("networks"):
            raise ValueError(file["path"] + ": a checker test takes its network mode from networkModes")
        for network in file["networks"]:
            checker_case(checker, network, case, file["path"])
        if file["tests"] != [CHECKERS[checker][1]]:
            raise ValueError(file["path"] + ": isolated checker must own only its original parent")
    representative = values.get("representative", "")
    if representative and representative not in REPRESENTATIVE_THEMES:
        raise ValueError(file["path"] + ": representative takes one of " + ", ".join(REPRESENTATIVE_THEMES))
    image_roles = None
    if "image-roles" in values:
        roles = values["image-roles"].split(",")
        if (any(role not in IMAGE_ROLES for role in roles) or len(roles) != len(set(roles))
                or "essential" not in roles):
            raise ValueError(file["path"] + ": image-roles must list distinct roles of "
                             + ",".join(IMAGE_ROLES) + ", essential included")
        image_roles = ",".join(role for role in IMAGE_ROLES if role in roles)
    return {"timeout": values["timeout"], "job_timeout": job_timeout,
            "checker": checker, "case": case, "image_roles": image_roles, "representative": representative}


def checker_case(checker, network, kind, path="checker"):
    """The checker's case for one network mode and receipt kind of a batch.

    A checker validates one mode's run of its parent. The batch declares the
    receipt kind, if the checker has kinds, and its test declares the modes,
    so each matrix entry of the batch checks its own mode.
    """
    module = importlib.import_module(CHECKERS[checker][0])
    if checker == "receivers":
        case = network if not kind else ""
    else:
        case = "process-" + checker + "-" + network + "-" + kind
    if case not in (module.NETWORKS if checker == "receivers" else module.CASES):
        raise ValueError(path + ": unknown completion checker case")
    return case


def resolve_image_roles(profile):
    """Give a Ceph profile its declared roles, or all five; others get none."""
    declared = profile["image_roles"]
    if not profile["requires_ceph"]:
        if declared is not None:
            raise ValueError(profile["profile"] + ": image-roles needs a Ceph runtime profile")
        profile["image_roles"] = ""
    elif declared is None:
        profile["image_roles"] = ",".join(IMAGE_ROLES)


def package_path(file):
    path = Path(file["path"])
    if path.is_absolute() or ".." in path.parts or not file["path"].endswith("_test.go"):
        raise ValueError("invalid source package path")
    parent = path.parent.as_posix()
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", parent):
        raise ValueError("unsupported source package path")
    return "./" + parent if parent != "." else "."


def capability_tags(catalog):
    # Capability tags come from source too. Adding a new capability does not
    # require changing this runner, a Make selector or a workflow matrix.
    tags = set(CAPABILITY_TAGS)
    for file in catalog:
        if not file["tests"] or "ci_optional" in file["tags"] or "ci_sdk" in file["tags"]:
            continue
        if any(reserved_go_tag(tag) for tag in file["tags"]):
            raise ValueError(file["path"] + ": platform/toolchain tags cannot be forced as CI capability tags")
        suffixes = Path(file["path"]).name[:-len("_test.go")].split("_")[-2:]
        if file.get("metadata") and any(suffix in GO_PLATFORMS for suffix in suffixes):
            raise ValueError(file["path"] + ": platform-specific runtime files need a separate runner policy")
        tags.update(tag for tag in file["tags"] if tag not in ("all", "ci", "native_regression", "goceph")
                    and not tag.startswith("ci_"))
    return tuple(sorted(tags))


def native_tags(category, batch, capabilities=CAPABILITY_TAGS):
    return ",".join((*capabilities, "ci", "ci_" + category, "ci_batch", "ci_batch_" + batch))


def discover(catalog):
    """Build profiles and reject missing/overlapping ownership before execution."""
    profiles, owners, aggregates, all_parents = {}, defaultdict(list), {}, defaultdict(set)
    sources, networks, network_tests = defaultdict(set), defaultdict(set), defaultdict(set)
    capabilities = capability_tags(catalog)
    for file in catalog:
        if not file["tests"]:
            if file.get("metadata"):
                raise ValueError(file["path"] + ": helper file must not declare CI test metadata")
            if file.get("networks"):
                raise ValueError(file["path"] + ": networkModes belongs in the file that declares the test")
            continue
        tags = set(file["tags"])
        package = package_path(file)
        if "ci_optional" in tags or "ci_sdk" in tags:
            if evaluate(file["constraint"], {"all"}):
                raise ValueError(file["path"] + ": optional or SDK test must require an explicit extra tag")
            continue
        category_tags = {tag for tag in tags if tag.startswith("ci_") and not tag.startswith("ci_batch_")}
        category_tags.discard("ci_batch")
        if category_tags - {"ci_" + category for category in CATEGORIES}:
            raise ValueError(file["path"] + ": unknown CI category tag")
        # Ordinary untagged tests remain in code-check and may execute cheaply
        # beside Dockerbridge fixtures; they do not own runtime batches.
        if not tags and not file.get("metadata"):
            continue
        if not category_tags:
            if evaluate(file["constraint"], set(capabilities) | {"ci"}):
                raise ValueError(file["path"] + ": required test is missing a CI category")
            if evaluate(file["constraint"], {"all"}):
                for test in file["tests"]:
                    key = (package, test)
                    if key in aggregates:
                        raise ValueError("duplicate all-tag aggregate parent: " + str(key))
                    aggregates[key] = file
                    all_parents[package].add(test)
            continue
        if len(category_tags) != 1:
            raise ValueError(file["path"] + ": a test file must have one CI category")
        batches = {tag[len("ci_batch_"):] for tag in tags if tag.startswith("ci_batch_")}
        if not batches and not file.get("metadata") and evaluate(file["constraint"], {"all"}):
            for test in file["tests"]:
                key = (package, test)
                if key in aggregates:
                    raise ValueError("duplicate all-tag aggregate parent: " + str(key))
                aggregates[key] = file
                all_parents[package].add(test)
            continue
        if len(batches) != 1 or not IDENTIFIER.fullmatch(next(iter(batches), "")):
            raise ValueError(file["path"] + ": a test file must have one valid CI batch")
        category, batch = next(iter(category_tags))[3:], next(iter(batches))
        if not evaluate(file["constraint"], set(native_tags(category, batch, capabilities).split(","))):
            raise ValueError(file["path"] + ": declared profile excludes its test file")
        metadata = parse_metadata(file)
        key = (category, batch, package)
        profile = {"category": category, "batch": batch, "package": package,
                   "tags": native_tags(category, batch, capabilities), **metadata}
        if key in profiles and profiles[key] != profile:
            raise ValueError(file["path"] + ": one batch has conflicting budgets, checker scope or image roles")
        profiles[key] = profile
        sources[key].add(file["path"])
        if file.get("networks"):
            networks[key].add(tuple(file["networks"]))
            network_tests[key].update(file.get("network_tests", ()))
        for test in file["tests"]:
            owners[(package, test)].append((key, file, metadata))
            if evaluate(file["constraint"], {"all"}):
                all_parents[package].add(test)
    if not profiles:
        raise ValueError("no required tagged runtime profiles were found")
    for key, assignments in owners.items():
        if len(assignments) == 1 and key not in aggregates:
            if not evaluate(assignments[0][1]["constraint"], {"all"}):
                raise ValueError(assignments[0][1]["path"] + ": required test is missing from the all tag")
            continue
        checker_names = {assignment[2]["checker"] for assignment in assignments}
        if len(checker_names) != 1 or "none" in checker_names or key not in aggregates:
            raise ValueError("overlapping runtime parent ownership: " + str(key))
        checker = next(iter(checker_names))
        module = importlib.import_module(CHECKERS[checker][0])
        expected = set(module.NETWORKS if checker == "receivers" else module.CASES)
        actual = [checker_case(checker, network, assignment[2]["case"])
                  for assignment in assignments for network in assignment[1]["networks"]]
        if len(actual) != len(set(actual)) or set(actual) != expected:
            raise ValueError("isolated checker scopes do not cover the original aggregate: " + str(key))
    if set(aggregates) - set(owners):
        raise ValueError("all-tag aggregate has no required runtime owner: " + str(sorted(set(aggregates) - set(owners))))
    # Evaluate every profile against every source file. Metadata is an assertion
    # about compiled ownership, not a second manual test-name selector.
    for profile in profiles.values():
        selected = set(profile["tags"].split(","))
        for file in catalog:
            if not file["tests"] or package_path(file) != profile["package"]:
                continue
            if not evaluate(file["constraint"], selected):
                continue
            if "ci_optional" in file["tags"] or "ci_sdk" in file["tags"]:
                raise ValueError(file["path"] + ": optional test leaked into a required profile")
            if file["tags"]:
                own = (profile["category"], profile["batch"], profile["package"])
                if any(not any(assignment[0] == own and assignment[1]["path"] == file["path"]
                               for assignment in owners[(profile["package"], test)]) for test in file["tests"]):
                    raise ValueError(file["path"] + ": test tags overlap another runtime profile")
    result = [profiles[key] for key in sorted(profiles)]
    for key in sorted(profiles):
        # The files that declare the batch's tests; shared helpers are not
        # listed because every batch compiles them.
        profiles[key]["sources"] = sorted(sources[key])
        profiles[key]["networks"] = batch_networks(key, networks[key])
        profiles[key]["network_tests"] = sorted(network_tests[key])
    for profile in result:
        profile["profile"] = profile["category"] + "/" + profile["batch"] + "/" + profile["package"][2:].replace("/", "-")
        suffix = hashlib.sha256(profile["profile"].encode()).hexdigest()[:8]
        profile["id"] = (profile["category"] + "-" + profile["batch"])[:70] + "-" + suffix
        profile["requires_ceph"] = profile["package"] == "./internal/integration" and profile["category"] != "code"
        resolve_image_roles(profile)
    return {"schema": SCHEMA, "coverage_scope": "required", "include": result,
            "required_parents": {package: sorted(names) for package, names in sorted(all_parents.items())},
            "required_executions": sum(len(assignments) for assignments in owners.values()),
            "required_profiles": len(result), "capability_tags": list(capabilities)}


def batch_networks(key, declared):
    """The network modes of a batch: its test files that declare modes
    declare the same ones, so the batch has one default mode."""
    if not declared:
        return []
    if len(declared) != 1:
        raise ValueError("/".join(key[:2]) + ": the test files of a batch declare different networkModes")
    modes = list(next(iter(declared)))
    if any(mode not in NETWORKS for mode in modes) or len(modes) != len(set(modes)):
        raise ValueError("/".join(key[:2]) + ": networkModes takes distinct modes of " + ", ".join(NETWORKS))
    return modes


def network_matrix(profiles, scope):
    """One matrix entry per profile and network mode of the scope.

    The default scope runs a batch's first mode, as pull requests do; the all
    scope also runs the others. The default entry runs the whole batch and
    keeps the profile's name and ID, so its job and artifact names do not
    change; another mode's entry runs only the tests that declare modes.
    """
    if scope not in NETWORK_SCOPES:
        raise ValueError("unknown network scope")
    entries = []
    for profile in profiles:
        modes = profile.get("networks") or [""]
        for index, mode in enumerate(modes if scope == "all" else modes[:1]):
            entry = {**profile, "network": mode, "name": profile["batch"], "tests": "",
                     "repetition": "1", "artifact_suffix": ""}
            if index:
                entry["name"] += " (" + mode + ")"
                entry["id"] = profile["id"] + "-" + mode
                entry["tests"] = ",".join(profile["network_tests"])
            entries.append(entry)
    return entries


def select_required_plan(plan, category=None):
    """Select one workflow's matrix without narrowing required coverage facts."""
    if category is None:
        return dict(plan)
    if category not in RUNTIME_CATEGORIES:
        raise ValueError("unsupported required runtime category")
    selected = [profile for profile in plan["include"] if profile["category"] == category]
    if not selected:
        raise ValueError("requested required runtime category is empty")
    return {**plan, "include": selected, "required_include": plan["include"],
            "selected_category": category, "selected_profiles": len(selected)}


def select_representative_plan(plan):
    """The batches that declare a representative theme, across categories.

    The periodic campaign repeats only these; full coverage stays with the
    category workflows and the periodic matrix."""
    selected = [profile for profile in plan["include"] if profile.get("representative")]
    missing = sorted(set(REPRESENTATIVE_THEMES) - {profile["representative"] for profile in selected})
    if missing:
        raise ValueError("the representative set lacks the theme(s) " + ", ".join(missing))
    if any(profile["category"] == "code" for profile in selected):
        raise ValueError("a code batch cannot be representative")
    return {**plan, "include": selected, "required_include": plan["include"], "selected_scope": "representative",
            "selected_profiles": len(selected)}


def repeat_matrix(entries, repetitions):
    """Each matrix entry once per repetition, with its repetition ID.

    Every attempt is its own job on a fresh runner with its own artifact
    names; the profile ID stays the entry's, so history rows of one batch
    differ only by repetition ID. The campaign is bounded in jobs and in
    summed job budgets."""
    if not isinstance(repetitions, int) or not 1 <= repetitions <= MAX_REPETITIONS:
        raise ValueError("repetitions must be 1 to " + str(MAX_REPETITIONS))
    if repetitions == 1:
        return entries
    # Repetition-major order spreads one batch's attempts over the run.
    repeated = [{**entry, "repetition": str(index), "artifact_suffix": "-r" + str(index),
                 "name": entry["name"] + " #" + str(index)}
                for index in range(1, repetitions + 1) for entry in entries]
    if len(repeated) > MAX_CAMPAIGN_JOBS:
        raise ValueError("the campaign would run " + str(len(repeated)) + " jobs; the bound is "
                         + str(MAX_CAMPAIGN_JOBS))
    minutes = sum(entry["job_timeout"] for entry in repeated)
    if minutes > MAX_CAMPAIGN_MINUTES:
        raise ValueError("the campaign's job budgets sum to " + str(minutes) + " minutes; the bound is "
                         + str(MAX_CAMPAIGN_MINUTES))
    return repeated


RELEASE_SCOPES = ("all", "others")
# Runner budgets, measured with ci_measurements.py (docs/contributing/ci.md,
# "Runner budget"). A job that waited for a runner started when 39 of this
# account's jobs ran, so 40 hosted jobs run at once. A pull request
# execution keeps PULL_REQUEST_RUNNERS of them free, so Ceph periodic holds
# at most the rest: its release callers, its environment caller and its
# representative campaign together. Each release caller's runtime matrix
# runs at most its category's number of jobs at once. The split follows each
# category's measured runner time per release: short needs about twice the
# runner minutes of the others.
#
# The full matrix and the representative campaign run in separate
# concurrency groups (PERIODIC_GROUP_SUFFIX), so that neither replaces the
# other's waiting run; a run of each can therefore be in progress at once.
# The campaign's max-parallel is what the full matrix leaves of the periodic
# share, and periodic_runner_budget adds the groups up.
RUNNER_LIMIT = 40
PULL_REQUEST_RUNNERS = 16
RELEASE_MAX_PARALLEL = {"short": 4, "topology": 2, "multicluster": 2, "recovery": 2}
ENVIRONMENT_MAX_PARALLEL = 1
CAMPAIGN_MAX_PARALLEL = 3
PERIODIC_GROUP_SUFFIX = {"full": "", "representative": "-campaign"}


def release_matrix(scope, check_prefix, current=None):
    """The release callers of a release-matrix workflow, one per supported
    release and release-bound category.

    The supported releases are run_image_matrix.SUPPORTED_RELEASES, so a new
    release is one row there. Scope all is Ceph periodic; scope others leaves
    out the current release, which the pull request entry points already run.
    The environment category needs no Ceph release and stays a separate job.
    """
    if scope not in RELEASE_SCOPES:
        raise ValueError("unknown release scope")
    current = images.RELEASE if current is None else current
    if current not in images.RELEASES:
        raise ValueError("current release " + current + " is not a supported release")
    entries = []
    for release, codename, _ in images.SUPPORTED_RELEASES:
        if scope == "others" and release == current:
            continue
        for category in RUNTIME_CATEGORIES:
            if category != "environment":
                entries.append({"release": release, "codename": codename, "category": category,
                                "title": codename.capitalize() + " " + category,
                                "check_name": check_prefix + " " + codename + " " + category,
                                "max_parallel": RELEASE_MAX_PARALLEL[category]})
    return entries


def runner_budget(entries):
    """The runners that the release callers entries and the environment
    caller hold at most at once, against those that pull requests leave."""
    used = sum(entry["max_parallel"] for entry in entries) + ENVIRONMENT_MAX_PARALLEL
    available = RUNNER_LIMIT - PULL_REQUEST_RUNNERS
    return {"runners": used, "available": available, "passed": used <= available}


def periodic_runner_budget(entries, campaign=None, groups=None):
    """The runners that Ceph periodic holds at most at once, over all its
    concurrency groups, against those that pull requests leave.

    Each mode (the full matrix of release callers entries with the
    environment caller, and the representative campaign with max-parallel
    campaign) runs in the group that its suffix in groups names. A group
    runs one run at a time and lets the next wait, so modes that share a
    group hold the larger of their budgets, and separate groups add up."""
    campaign = CAMPAIGN_MAX_PARALLEL if campaign is None else campaign
    groups = PERIODIC_GROUP_SUFFIX if groups is None else groups
    modes = {"full": runner_budget(entries)["runners"], "representative": campaign}
    held = {}
    for mode, runners in modes.items():
        held[groups[mode]] = max(held.get(groups[mode], 0), runners)
    used = sum(held.values())
    available = RUNNER_LIMIT - PULL_REQUEST_RUNNERS
    return {"runners": used, "available": available, "modes": modes,
            "groups": {"periodic" + suffix: runners for suffix, runners in held.items()}, "passed": used <= available}


def releases_report(plan, scope, check_prefix):
    """The release matrix of scope with proof that it keeps every required
    cell: with the environment job of Ceph periodic, scope all must run each
    required profile on every supported release exactly once."""
    entries = release_matrix(scope, check_prefix)
    releases = [release for release, _, _ in images.SUPPORTED_RELEASES
                if scope == "all" or release != images.RELEASE]
    callers = [(entry["release"], entry["category"]) for entry in entries]
    environment = [(None, "environment")] if scope == "all" else []
    expected = required_cells(plan, releases)
    if scope != "all":
        expected = expected - runtime_cells(plan, [(None, "environment")])
    difference = coverage_difference(expected, runtime_cells(plan, callers + environment))
    budget = periodic_runner_budget(entries) if scope == "all" else runner_budget(entries)
    return {"schema": SCHEMA, "scope": scope, "include": entries, "releases": releases,
            "cells": sum(expected.values()), "runner_budget": budget,
            "passed": not difference["missing"] and not difference["extra"] and budget["passed"], **difference}


def runtime_cells(plan, callers):
    """The multiset of (release, profile ID) runtime cells that callers run.

    Each caller is (release, category) as a tagged-runtime caller passes it.
    An environment profile needs no Ceph release, so its cell has none.
    """
    cells = Counter()
    for release, category in callers:
        if category != "environment" and release not in images.RELEASES:
            raise ValueError("caller release " + str(release) + " is not a supported release")
        for profile in select_required_plan(plan, category)["include"]:
            cells[(None if category == "environment" else release, profile["id"])] += 1
    return cells


def required_cells(plan, releases):
    """Every release-bound required profile on each release, and every
    environment profile once."""
    callers = [(release, category) for release in releases
               for category in RUNTIME_CATEGORIES if category != "environment"]
    return runtime_cells(plan, callers + [(None, "environment")])


def coverage_difference(required, actual):
    """Missing and extra cells of actual against required, as sorted lists.
    Both are multisets, so a duplicated cell is extra."""
    key = lambda cell: (cell[0] or "", cell[1])
    return {"missing": sorted((required - actual).elements(), key=key),
            "extra": sorted((actual - required).elements(), key=key)}


def select_changed_plan(plan, changed):
    """Keep only the batches whose own test files a change touched.

    This is the pull request scope of the categories that do not gate every
    merge. A change to a shared helper, a public package or anything else
    selects nothing here; the periodic matrix runs those batches on main.
    """
    changed = {path.strip() for path in changed if path.strip()}
    selected = [profile for profile in plan["include"] if changed & set(profile["sources"])]
    return {**plan, "include": selected, "selected_scope": "changed",
            "changed_files": sorted(changed), "selected_profiles": len(selected)}


def changed_paths(base, root=ROOT):
    """Paths that the checked-out commit changed since its merge base with base."""
    result = subprocess.run(["git", "diff", "--name-only", "--no-renames", base + "...HEAD"], cwd=root,
                            text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise ValueError("git diff since " + base + " failed: " + result.stderr.strip())
    return result.stdout.splitlines()


def run_batchselect(batches, base, root=ROOT, head=None, tool=None):
    """Run internal/tools/batchselect for batches of the declaration package.

    Without head, root is a checkout and the diff ends at its working tree;
    with head, root holds that commit's exported tree and ROOT is the
    repository."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "batches.json"
        path.write_text(json.dumps(batches))
        command = [str(tool)] if tool else ["go", "run", "-mod=readonly", "./internal/tools/batchselect"]
        command += ["-root", str(root), "-base", base, "-package", DECLARATION_PACKAGE, "-batches", str(path)]
        if head:
            command += ["-head", head, "-repo", str(ROOT)]
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True,
                                timeout=DECLARATION_TIMEOUT_SECONDS, env=go_environment())
    if result.returncode:
        raise ValueError("declaration selection failed: " + result.stderr.strip())
    return json.loads(result.stdout)


def declaration_selection(profiles, base, root=ROOT, runner=None):
    """Advisory selection of profiles by declaration reachability (#116).

    It is recorded beside the plan and never changes which profiles run.
    A profile outside the analysed package, or any failure of the analysis,
    counts as selected with the reason."""
    runner = runner or run_batchselect
    analysed = [profile for profile in profiles if profile["package"] == DECLARATION_PACKAGE]
    record = {"advisory": True, "base": base, "package": DECLARATION_PACKAGE, "fallback": [],
              "changed_declarations": [], "selected": {}, "excluded": {}}
    batches, unavailable = {}, ""
    if analysed:
        try:
            result = runner([{"id": profile["id"], "tags": profile["tags"]} for profile in analysed], base, root)
            batches = {batch["id"]: batch for batch in result.get("batches", [])}
            if sorted(batches) != sorted(profile["id"] for profile in analysed):
                raise ValueError("declaration selection does not report every analysed profile")
            record.update(merge_base=result.get("merge_base", ""), fallback=result.get("fallback", []),
                          changed_declarations=result.get("changed_declarations", []))
        except (ValueError, OSError, subprocess.SubprocessError, KeyError, TypeError) as error:
            batches, unavailable = {}, str(error)
            record["error"] = unavailable
    for profile in profiles:
        if profile["package"] != DECLARATION_PACKAGE:
            record["selected"][profile["id"]] = "package " + profile["package"] + " is not analysed"
        elif unavailable:
            record["selected"][profile["id"]] = "analysis unavailable: " + unavailable
        elif batches[profile["id"]]["selected"]:
            record["selected"][profile["id"]] = batches[profile["id"]]["reason"]
        else:
            record["excluded"][profile["id"]] = batches[profile["id"]]["reason"]
    record["selected_profiles"], record["total_profiles"] = len(record["selected"]), len(profiles)
    return record


def apply_declaration_selection(plan, selection):
    """Keep only the profiles that the declaration selection selected: the
    pull request scope of a gating category (#116). Pushes to main run the
    full plan, so a miss surfaces at the next merge."""
    selected = [profile for profile in plan["include"] if profile["id"] in selection["selected"]]
    return {**plan, "include": selected, "selected_scope": "declarations", "selected_profiles": len(selected),
            "declaration_selection": {**selection, "advisory": False}}


def declaration_summary(selection, category):
    """Markdown for the job summary: every selected and excluded profile
    with its reason."""
    def cell(text):
        return text.replace("|", "\\|").replace("\n", " ")
    lines = ["### Declaration selection: " + category, "",
             str(selection["selected_profiles"]) + " of " + str(selection["total_profiles"])
             + " batches selected since " + cell(selection.get("merge_base") or selection["base"]) + "."]
    if not selection.get("advisory", True):
        lines += ["", "Only the selected batches run here, by opt-in. The selection is advisory: "
                      "pushes to main run every batch."]
    if selection.get("error"):
        lines += ["", "Every batch runs: the analysis failed: " + cell(selection["error"])]
    for reason in selection["fallback"]:
        lines += ["", "Every batch runs: " + cell(reason)]
    lines += ["", "| Batch | Runs | Reason |", "| --- | --- | --- |"]
    for verdict, items in (("yes", selection["selected"]), ("no", selection["excluded"])):
        lines += ["| " + profile + " | " + verdict + " | " + cell(reason) + " |" for profile, reason in sorted(items.items())]
    return "\n".join(lines) + "\n"


def load_catalog(root=ROOT):
    result = subprocess.run(["go", "run", "-mod=readonly", str(ROOT / "internal/tools/tagcatalog/main.go"), str(root)],
                            cwd=root, text=True, capture_output=True, timeout=180,
                            env=go_environment())
    if result.returncode:
        raise ValueError("Go AST test catalog failed: " + result.stderr.strip())
    return json.loads(result.stdout)


def discover_optional(catalog):
    """Keep native defect regressions explicitly outside required coverage."""
    profiles = {}
    capabilities = (*capability_tags(catalog), "native_regression", "role_images")
    for file in catalog:
        if not file["tests"] or "ci_optional" not in file["tags"]:
            continue
        if any(reserved_go_tag(tag) for tag in file["tags"]):
            raise ValueError(file["path"] + ": optional profile cannot force platform/toolchain tags")
        batches = {tag[len("ci_batch_"):] for tag in file["tags"] if tag.startswith("ci_batch_")}
        if len(batches) != 1 or not IDENTIFIER.fullmatch(next(iter(batches), "")):
            raise ValueError(file["path"] + ": optional test must declare one valid batch")
        batch = next(iter(batches))
        package = package_path(file)
        profile = {"category": "optional", "batch": batch, "package": package,
                   "tags": native_tags("optional", batch, capabilities), **parse_metadata(file)}
        if profile["representative"]:
            raise ValueError(file["path"] + ": an optional batch cannot be representative")
        if not evaluate(file["constraint"], set(profile["tags"].split(","))):
            raise ValueError(file["path"] + ": declared optional profile excludes its file")
        if evaluate(file["constraint"], {"all"}):
            raise ValueError(file["path"] + ": optional test leaked into the normal all tag")
        key = (batch, package)
        if key in profiles and profiles[key] != profile:
            raise ValueError(file["path"] + ": optional batch has conflicting execution metadata")
        profiles[key] = profile
    for profile in profiles.values():
        selected = set(profile["tags"].split(","))
        for file in catalog:
            if not file["tests"] or package_path(file) != profile["package"] or not evaluate(file["constraint"], selected):
                continue
            if file["tags"] and ("ci_optional" not in file["tags"]
                                 or "ci_batch_" + profile["batch"] not in file["tags"]):
                raise ValueError(file["path"] + ": optional tags overlap a different source profile")
        profile["profile"] = "optional/" + profile["batch"] + "/" + profile["package"][2:].replace("/", "-")
        suffix = hashlib.sha256(profile["profile"].encode()).hexdigest()[:8]
        profile["id"] = ("optional-" + profile["batch"])[:70] + "-" + suffix
        profile["requires_ceph"] = bool(profile["image_roles"]) or profile["package"] == "./internal/integration"
        resolve_image_roles(profile)
    return {"schema": SCHEMA, "coverage_scope": "optional",
            "include": [profiles[key] for key in sorted(profiles)]}


def select_optional_plan(plan, batch):
    """An explicit source-owned optional batch never claims required coverage."""
    if not batch or not IDENTIFIER.fullmatch(batch):
        raise ValueError("optional plan needs a valid explicit batch")
    selected = [profile for profile in plan["include"] if profile["batch"] == batch]
    if not selected:
        raise ValueError("requested optional batch is unsupported or empty")
    return {**plan, "include": selected, "selected_batch": batch, "selected_profiles": len(selected)}


def select_profiles_plan(plan, optional, identifiers):
    """The runtime profiles named by ID, of any category or optional: the
    plan of a pull request comment command (ci_command.py, #116). It claims
    no coverage; an unknown, duplicate or code profile ID refuses the plan."""
    names = [name.strip() for name in identifiers.split(",") if name.strip()]
    if not names:
        raise ValueError("plan --profiles needs at least one profile ID")
    if len(names) != len(set(names)):
        raise ValueError("plan --profiles names a profile ID twice")
    available = [profile for profile in plan["include"] + optional["include"] if profile["category"] != "code"]
    unknown = sorted(set(names) - {profile["id"] for profile in available})
    if unknown:
        raise ValueError("unknown runtime profile ID(s): " + ", ".join(unknown))
    selected = [profile for profile in available if profile["id"] in names]
    return {**plan, "coverage_scope": "selected", "include": selected, "required_include": plan["include"],
            "selected_scope": "profiles", "selected_profiles": len(selected)}


def vocabulary(plan, optional):
    """The names a pull request comment command may use: every profile with
    its category, batch and package, and the supported releases."""
    fields = ("id", "profile", "category", "batch", "package")
    return {"schema": SCHEMA,
            "profiles": [{field: profile[field] for field in fields}
                         for profile in plan["include"] + optional["include"]],
            "releases": [{"release": release, "codename": codename}
                         for release, codename, _ in images.SUPPORTED_RELEASES],
            "default_release": images.RELEASE}


def selected_tests(catalog, package, tags):
    tests = []
    for file in catalog:
        if package_path(file) == package and evaluate(file["constraint"], set(tags.split(","))):
            tests.extend(file["tests"])
    if len(tests) != len(set(tests)):
        raise ValueError("duplicate test declaration in compiled tag profile")
    return sorted(tests)


def compile_inventory(package, tags, root=ROOT):
    command = ["go", "test", "-mod=readonly", "-tags=" + tags, "-list=.", package]
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=180,
                            env=go_environment())
    if result.returncode:
        raise ValueError("tag profile did not compile: " + tags + "\n" + result.stdout + result.stderr)
    tests = re.findall(r"^Test[^\s/]+$", result.stdout, re.MULTILINE)
    if not tests or len(tests) != len(set(tests)):
        raise ValueError("compiled tag profile is empty or has duplicate tests")
    return sorted(tests)


def verify(catalog, plan, root=ROOT):
    observations = []
    packages = sorted({profile["package"] for profile in plan["include"]})
    for package in packages:
        actual = compile_inventory(package, "all", root)
        expected = selected_tests(catalog, package, "all")
        if actual != expected:
            raise ValueError("all-tag compiled inventory differs from its AST source: " + package)
        observations.append({"package": package, "tags": "all", "tests": actual})
    for category in CATEGORIES:
        for package in packages:
            if not any(profile["category"] == category and profile["package"] == package for profile in plan["include"]):
                continue
            tags = ",".join((*plan["capability_tags"], "ci", "ci_" + category))
            actual = compile_inventory(package, tags, root)
            expected = selected_tests(catalog, package, tags)
            if actual != expected:
                raise ValueError("category compiled inventory differs from its AST source: " + category)
            observations.append({"package": package, "tags": tags, "tests": actual})
    for profile in plan["include"]:
        actual = compile_inventory(profile["package"], profile["tags"], root)
        expected = selected_tests(catalog, profile["package"], profile["tags"])
        if actual != expected:
            raise ValueError("compiled profile differs from its AST source: " + profile["profile"])
        observations.append({"package": profile["package"], "tags": profile["tags"], "tests": actual})
    return {"schema": SCHEMA, "passed": True, "profiles": observations}


def completion(log, expected, package, execution=GO_TEST):
    """Check the RUN/PASS proof of one native log.

    A go test log ends its package with PASS and then the driver's
    "ok <package>" line. A test binary run directly prints the same results
    and PASS, but no ok line: its PASS must be the last line, and the caller
    proves the package binding and the process exit status separately."""
    if execution not in EXECUTIONS:
        raise ValueError("unknown native execution format")
    runs, passes, failures, skips = [], [], [], []
    package_passes, packages = [], []
    run_positions, pass_positions = {}, {}
    lines = log.splitlines()
    for index, line in enumerate(lines):
        run = RUN_LINE.fullmatch(line)
        if run:
            runs.append(run[1])
            run_positions[run[1]] = index
        result = RESULT_LINE.fullmatch(line)
        if result:
            {"PASS": passes, "FAIL": failures, "SKIP": skips}[result[1]].append(result[2])
            if result[1] == "PASS":
                pass_positions[result[2]] = index
        if re.match(r"^FAIL(?:\s|$)|^panic:", line):
            failures.append("package failure")
        if line == "PASS":
            package_passes.append(index)
        if re.fullmatch(r"ok\s+" + re.escape(package) + r"\s+[0-9.]+s", line):
            packages.append(index)
    errors = []
    parent_runs = Counter(name for name in runs if "/" not in name)
    parent_passes = Counter(name for name in passes if "/" not in name)
    if parent_runs != Counter(expected) or parent_passes != Counter(expected):
        errors.append("native parent RUN/PASS inventory differs from the compiled profile")
    if Counter(runs) != Counter(passes) or any(count != 1 for count in Counter(runs).values()):
        errors.append("a started native test or leaf lacks exactly one completed PASS")
    if failures:
        errors.append("native log contains failed tests or package failure")
    if skips:
        errors.append("native log contains skipped tests")
    if not package_completed(lines, package_passes, packages, execution):
        errors.append("native log lacks exactly one successful Go package completion")
    elif any(run_positions[name] >= pass_positions.get(name, -1)
             or pass_positions.get(name, package_passes[0]) >= package_passes[0] for name in runs):
        errors.append("native tests did not finish after RUN and before package completion")
    return {"passed": not errors, "errors": errors, "execution": execution, "observed_runs": runs,
            "observed_passes": passes, "failed_tests": failures, "skipped_tests": skips}


def package_completed(lines, package_passes, packages, execution):
    """One PASS line, then exactly one ok line (go test), or one PASS line as
    the last line and no ok line (test binary)."""
    if len(package_passes) != 1:
        return False
    if execution == GO_TEST:
        return len(packages) == 1 and package_passes[0] < packages[0]
    last = max((index for index, line in enumerate(lines) if line.strip()), default=-1)
    return not packages and package_passes[0] == last


def strict_completion(log, profile, network, execution=GO_TEST):
    if profile["checker"] == "none":
        return None
    module = importlib.import_module(CHECKERS[profile["checker"]][0])
    case = checker_case(profile["checker"], network, profile["case"])
    if execution == GO_TEST:
        return module.validate(log, case)
    return module.validate(log, case, execution=execution)


def failure_kind(exit_code, report, strict):
    if exit_code == 0 and report["passed"] and (strict is None or strict["passed"]):
        return "passed"
    if exit_code != 0 and not report["failed_tests"]:
        return "process-or-build-failure"
    if report["skipped_tests"]:
        return "incomplete-skipped-fixture"
    if report["failed_tests"]:
        return "native-test-failure"
    return "incomplete-native-proof"


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def binary_name(package):
    """The compiled binary's file name in a compile directory."""
    return (package[2:].replace("/", "-") if package != "." else "root") + ".test"


def build_key(profile, root=ROOT):
    """What besides the sources decides the compiled binary."""
    result = subprocess.run(["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH"], cwd=root, text=True,
                            capture_output=True, timeout=60, env=go_environment())
    if result.returncode:
        raise ValueError("go env failed: " + result.stderr.strip())
    values = json.loads(result.stdout)
    return {"package": profile["package"], "tags": profile["tags"], "go_environment": dict(GO_EXECUTION_ENV),
            "go_version": values["GOVERSION"], "goos": values["GOOS"], "goarch": values["GOARCH"]}


def build_sources(profile, root=ROOT):
    """SHA-256 of every main-module file that the profile's test build reads,
    with go.mod and go.sum, by path relative to root."""
    command = ["go", "list", "-mod=readonly", "-tags=" + profile["tags"], "-deps", "-test", "-json", profile["package"]]
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=180, env=go_environment())
    if result.returncode:
        raise ValueError("go list of the build inputs failed: " + result.stderr.strip())
    root = Path(root).resolve()
    paths = {root / "go.mod", root / "go.sum"}
    decoder, text, index = json.JSONDecoder(), result.stdout.strip(), 0
    while index < len(text):
        package, index = decoder.raw_decode(text, index)
        index = len(text) - len(text[index:].lstrip())
        if not (package.get("Module") or {}).get("Main"):
            continue
        for field in BUILD_FILE_FIELDS:
            for name in package.get(field) or ():
                path = (Path(package["Dir"]) / name).resolve()
                # The generated test main lives in the build cache.
                if path.is_relative_to(root):
                    paths.add(path)
    return {path.relative_to(root).as_posix(): file_sha256(path) for path in sorted(paths) if path.is_file()}


def binary_inventory(binary, cwd):
    """The top-level tests that a test binary lists; it runs none of them."""
    result = subprocess.run([str(binary), "-test.list=."], cwd=cwd, text=True, capture_output=True, timeout=60,
                            env=go_environment())
    if result.returncode:
        raise ValueError("compiled test binary could not list its tests\n" + result.stdout + result.stderr)
    tests = re.findall(r"^Test[^\s/]+$", result.stdout, re.MULTILINE)
    if not tests or len(tests) != len(set(tests)):
        raise ValueError("compiled test binary is empty or lists duplicate tests")
    return sorted(tests)


def binary_receipt(profile, binary, root=ROOT):
    """The record that lets a later phase run this binary instead of building."""
    return {"binary": binary.name, "sha256": file_sha256(binary), "build_key": build_key(profile, root),
            "sources": build_sources(profile, root),
            "tests": binary_inventory(binary, Path(root) / profile["package"])}


def verified_binary(directory, profile, expected, root=ROOT):
    """The compile phase's binary for profile, after proving it still is.

    Every check fails closed: a missing or failed receipt, another profile,
    a changed binary, build key, source or inventory never falls back to a
    new build."""
    directory = Path(directory)
    try:
        report = json.loads((directory / "compile-report.json").read_text())
    except (OSError, ValueError) as error:
        raise ValueError("compiled binary receipt is missing or unreadable: " + str(error)) from None
    if not isinstance(report, dict):
        report = {}
    receipt = report.get("binary")
    if report.get("schema") != SCHEMA or report.get("passed") is not True or not isinstance(receipt, dict):
        raise ValueError("compile receipt did not record a successful binary")
    compiled = report.get("profile") or {}
    if compiled.get("id") != profile["id"] or compiled.get("tags") != profile["tags"]:
        raise ValueError("compiled binary belongs to another profile")
    name = receipt.get("binary")
    if name != binary_name(profile["package"]):
        raise ValueError("compile receipt names an unexpected binary")
    binary = (directory / name).resolve()
    if not binary.is_file() or file_sha256(binary) != receipt.get("sha256"):
        raise ValueError("compiled binary is missing or its sha256 differs from its receipt")
    if build_key(profile, root) != receipt.get("build_key"):
        raise ValueError("compiled binary's build key differs from this toolchain, platform or profile")
    if build_sources(profile, root) != receipt.get("sources"):
        raise ValueError("compiled binary was built from different source files")
    if receipt.get("tests") != expected:
        raise ValueError("compiled binary's recorded inventory differs from its AST source")
    if binary_inventory(binary, Path(root) / profile["package"]) != expected:
        raise ValueError("compiled binary's own inventory differs from its AST source")
    return binary, receipt


def run_profile(catalog, plan, category, batch, package, directory, root=ROOT, tests="", binary=None):
    """Run one profile, or with tests only those of its tests that declare
    network modes, as a matrix entry of a non-default mode does.

    With binary, a compile directory, run the test binary that the compile
    phase built there instead of building with go test again."""
    matches = [profile for profile in plan["include"] if profile["category"] == category
               and profile["batch"] == batch and (package is None or profile["package"] == package)]
    if len(matches) != 1:
        raise ValueError("requested category/batch/package is unsupported or ambiguous")
    profile = matches[0]
    expected = selected_tests(catalog, profile["package"], profile["tags"])
    if not expected:
        raise ValueError("requested runtime profile is empty")
    selection = tests.split(",") if tests else []
    if selection and (len(selection) != len(set(selection)) or not set(selection) <= set(profile.get("network_tests", ()))
                      or (profile["checker"] != "none" and set(selection) != set(expected))):
        raise ValueError("a test selection names only tests of the profile that declare network modes")
    # A checker validates one mode's run, so its batch runs in one mode: the
    # matrix entry's, or the default when no entry selected one.
    network = os.environ.get("CEPH_TEST_NETWORK", "")
    if profile["checker"] != "none":
        network = network or profile["networks"][0]
        if network not in profile["networks"]:
            raise ValueError("CEPH_TEST_NETWORK names a network mode that the checker batch does not declare")
    # Reject malformed source metadata and empty profile before allocating an
    # artifact or starting Go. Reconfirm the actual compiled inventory next.
    if binary is None:
        execution, receipt, cwd = GO_TEST, None, root
        actual = compile_inventory(profile["package"], profile["tags"], root)
        if actual != expected:
            raise ValueError("compiled profile differs from its AST source")
        command = ["go", "test", "-mod=readonly", "-tags=" + profile["tags"], "-count=1", "-v",
                   "-timeout=" + profile["timeout"], profile["package"]]
    else:
        # go test runs a test binary in its package directory.
        execution, cwd = TEST_BINARY, Path(root) / profile["package"]
        path, receipt = verified_binary(binary, profile, expected, root)
        command = [str(path), "-test.v", "-test.count=1", "-test.timeout=" + profile["timeout"]]
    directory.mkdir(parents=True, exist_ok=False)
    if selection:
        expected = sorted(selection)
        pattern = "^(" + "|".join(expected) + ")$"
        if execution == GO_TEST:
            command[-1:-1] = ["-run", pattern]
        else:
            command.append("-test.run=" + pattern)
    started = time.time()
    with (directory / "native.log").open("w", encoding="utf-8") as log:
        environment = go_environment()
        environment.pop("CEPH_TEST_RBD_CLIENT_IMAGE", None)
        if network:
            environment["CEPH_TEST_NETWORK"] = network
        child = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 text=True, encoding="utf-8", errors="replace",
                                 env=environment)
        with child.stdout:
            for line in child.stdout:
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
        exit_code = child.wait()
    text = (directory / "native.log").read_text(encoding="utf-8")
    module = re.search(r"^module\s+(\S+)\s*$", (root / "go.mod").read_text(), re.MULTILINE)
    if module is None:
        raise ValueError("source go.mod has no module declaration")
    native_package = module[1] + ("/" + profile["package"][2:] if profile["package"] != "." else "")
    completed = completion(text, expected, native_package, execution)
    strict = strict_completion(text, profile, network, execution)
    result = {"schema": SCHEMA, "profile": profile, "command": command, "execution": execution,
              "network": network, "elapsed_seconds": round(time.time() - started, 3), "exit_code": exit_code,
              "go_environment": dict(GO_EXECUTION_ENV),
              "compiled_tests": expected, "completion": completed, "strict_completion": strict,
              "classification": failure_kind(exit_code, completed, strict),
              "source": [{"path": file["path"], "sha256": file["sha256"]} for file in catalog]}
    if receipt is not None:
        result["binary"] = {"directory": str(binary), "binary": receipt["binary"], "sha256": receipt["sha256"],
                            "build_key": receipt["build_key"]}
    result["passed"] = result["classification"] == "passed"
    (directory / "report.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def bounded_go(command, root, timeout):
    """Run a go command in its own process group within timeout seconds.

    At the deadline the whole group is killed, so no compiler or download
    process outlives the phase, and the output written until then is kept.
    exit_code is None for a command that timed out."""
    started = time.monotonic()
    child = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             encoding="utf-8", errors="replace", env=go_environment(), start_new_session=True)
    try:
        stdout, stderr = child.communicate(timeout=timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = child.communicate()
        timed_out = True
    outcome = {"exit_code": None if timed_out else child.returncode, "timed_out": timed_out,
               "timeout_seconds": timeout, "elapsed_seconds": round(time.monotonic() - started, 3)}
    return outcome, (stdout or "") + (stderr or "")


def prepare_modules(directory, root=ROOT):
    """Download every module that the build list needs into the module cache."""
    directory.mkdir(parents=True, exist_ok=False)
    command = ["go", "mod", "download"]
    outcome, output = bounded_go(command, root, MODULES_TIMEOUT_SECONDS)
    (directory / "modules.log").write_text(output)
    passed = outcome["exit_code"] == 0
    report = {"schema": SCHEMA, "command": command, **outcome, "go_environment": dict(GO_EXECUTION_ENV),
              "module_cache_restored": os.environ.get("SCENARIO_MODULE_CACHE_HIT", "") == "true",
              "passed": passed,
              "classification": "prepared" if passed else "modules-timeout" if outcome["timed_out"] else "modules-failure"}
    (directory / "modules-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def compile_profile(plan, category, batch, package, directory, root=ROOT):
    matches = [profile for profile in plan["include"] if profile["category"] == category
               and profile["batch"] == batch and (package is None or profile["package"] == package)]
    if len(matches) != 1:
        raise ValueError("requested category/batch/package is unsupported or ambiguous")
    profile = matches[0]
    directory.mkdir(parents=True, exist_ok=False)
    binary = (directory / binary_name(profile["package"])).resolve()
    command = ["go", "test", "-mod=readonly", "-tags=" + profile["tags"], "-c", "-o", str(binary), profile["package"]]
    outcome, output = bounded_go(command, root, COMPILE_TIMEOUT_SECONDS)
    (directory / "compile.log").write_text(output)
    passed = outcome["exit_code"] == 0
    classification = "compiled" if passed else "compile-timeout" if outcome["timed_out"] else "compile-failure"
    report = {"schema": SCHEMA, "profile": profile, "command": command, **outcome,
              "go_environment": dict(GO_EXECUTION_ENV)}
    if passed:
        # The receipt lets run --binary execute this binary instead of
        # building again; without it the binary is not reusable.
        try:
            report["binary"] = binary_receipt(profile, binary, root)
        except (ValueError, OSError, subprocess.SubprocessError) as error:
            passed, classification = False, "compile-receipt-failure"
            report["errors"] = [str(error)]
    report |= {"passed": passed, "classification": classification}
    (directory / "compile-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def warm_build_cache(plan, directory, root=ROOT):
    """Fill the Go build cache with one runtime profile of each package.

    Every profile of a package builds the same dependencies, so one compile
    leaves only the package's own tests to compile for the others: on four
    CPUs, ./internal/integration compiled in 18 s with an empty cache and in
    3.4 s with the cache of another tag set. The binaries are discarded;
    each runtime job still compiles, records and verifies its own."""
    directory.mkdir(parents=True, exist_ok=False)
    packages = {}
    for profile in plan["include"]:
        if profile["category"] != "code":
            packages.setdefault(profile["package"], profile)
    if not packages:
        raise ValueError("no runtime profile was found to warm the build cache")
    results = []
    with tempfile.TemporaryDirectory() as scratch:
        for package, profile in sorted(packages.items()):
            binary = Path(scratch) / binary_name(package)
            command = ["go", "test", "-mod=readonly", "-tags=" + profile["tags"], "-c", "-o", str(binary), package]
            outcome, output = bounded_go(command, root, COMPILE_TIMEOUT_SECONDS)
            (directory / (binary_name(package)[:-len(".test")] + ".log")).write_text(output)
            results.append({"package": package, "profile": profile["id"], "command": command, **outcome,
                            "passed": outcome["exit_code"] == 0})
    report = {"schema": SCHEMA, "packages": results, "go_environment": dict(GO_EXECUTION_ENV),
              "passed": all(result["passed"] for result in results)}
    (directory / "buildcache-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--output", type=Path)
        if name == "plan":
            command.add_argument("--github-output", type=Path)
            command.add_argument("--verify", action="store_true")
            command.add_argument("--category", choices=RUNTIME_CATEGORIES)
            command.add_argument("--optional", action="store_true")
            command.add_argument("--representative", action="store_true",
                                 help="Select the representative batches of every category")
            command.add_argument("--repetitions", type=int, default=1,
                                 help="Run each matrix entry this many times (representative only)")
            command.add_argument("--batch")
            command.add_argument("--profiles", default="", metavar="IDS",
                                 help="Comma-separated runtime profile IDs of any category or optional batch")
            command.add_argument("--changed-files", type=Path,
                                 help="File with one changed path per line; keep only batches whose test files changed")
            command.add_argument("--changed-since", default="",
                                 help="Base commit; keep only batches whose test files changed since it. Empty keeps all")
            command.add_argument("--select-declarations", default="", metavar="BASE",
                                 help="Base commit; record which batches reach a declaration changed since it. "
                                      "Advisory unless --apply-declarations")
            command.add_argument("--apply-declarations", action="store_true",
                                 help="Keep only the batches that --select-declarations selects")
            command.add_argument("--summary", type=Path,
                                 help="Append the declaration selection as Markdown, such as $GITHUB_STEP_SUMMARY")
            command.add_argument("--networks", choices=NETWORK_SCOPES, default="default",
                                 help="Run each batch in its default network mode, or in every mode it declares")
    for name in ("compile", "run"):
        command = commands.add_parser(name)
        command.add_argument("--category", required=True,
                             choices=(*CATEGORIES, "optional") if name == "compile" else CATEGORIES)
        command.add_argument("--batch", required=True)
        command.add_argument("--package")
        command.add_argument("--directory", required=True, type=Path)
        if name == "run":
            command.add_argument("--tests", default="",
                                 help="Comma-separated tests that declare network modes; run only these")
            command.add_argument("--binary", type=Path,
                                 help="Compile directory; run its verified test binary instead of go test")
    command = commands.add_parser("releases")
    command.add_argument("--scope", choices=RELEASE_SCOPES, required=True)
    command.add_argument("--check-prefix", required=True)
    command.add_argument("--github-output", type=Path)
    command = commands.add_parser("vocabulary")
    command.add_argument("--output", type=Path)
    command = commands.add_parser("code")
    command.add_argument("--directory", required=True, type=Path)
    command = commands.add_parser("modules")
    command.add_argument("--directory", required=True, type=Path)
    command = commands.add_parser("buildcache")
    command.add_argument("--directory", required=True, type=Path)
    command = commands.add_parser("optional")
    command.add_argument("--batch", required=True)
    command.add_argument("--package")
    command.add_argument("--directory", required=True, type=Path)
    command.add_argument("--binary", type=Path,
                         help="Compile directory; run its verified test binary instead of go test")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            if args.optional and (not args.batch or args.category is not None or args.verify):
                raise ValueError("optional plan requires --batch and excludes --category and --verify")
            if args.representative and (args.optional or args.category is not None or args.verify
                                        or args.changed_files is not None or args.changed_since
                                        or args.select_declarations):
                raise ValueError("plan --representative excludes --category, --optional, --verify and selections")
            if args.profiles and (args.optional or args.representative or args.category is not None or args.verify
                                  or args.changed_files is not None or args.changed_since
                                  or args.select_declarations):
                raise ValueError("plan --profiles excludes --category, --optional, --representative, --verify "
                                 "and selections")
            if args.repetitions != 1 and not args.representative:
                raise ValueError("plan --repetitions requires --representative")
            if not 1 <= args.repetitions <= MAX_REPETITIONS:
                raise ValueError("repetitions must be 1 to " + str(MAX_REPETITIONS))
            if args.batch is not None and not args.optional:
                raise ValueError("plan --batch requires explicit --optional")
            if args.changed_files is not None and args.changed_since:
                raise ValueError("plan takes --changed-files or --changed-since, not both")
            if (args.changed_files is not None or args.changed_since) and (args.optional or args.verify or args.category is None):
                raise ValueError("plan --changed-files requires --category and excludes --optional and --verify")
            if args.select_declarations and args.optional:
                raise ValueError("plan --select-declarations excludes --optional")
            if args.apply_declarations and (not args.select_declarations or args.category is None or args.verify
                                            or args.changed_files is not None or args.changed_since):
                raise ValueError("plan --apply-declarations requires --select-declarations and --category, "
                                 "and excludes --verify and a changed-file selection")
        if args.command == "modules":
            report = prepare_modules(args.directory)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["passed"] else 1
        catalog = load_catalog()
        plan = discover(catalog)
        if args.command == "releases":
            report = releases_report(plan, args.scope, args.check_prefix)
            if args.github_output is not None:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write("matrix=" + json.dumps({"include": report["include"]}, separators=(",", ":")) + "\n")
                    output.write("count=" + str(len(report["include"])) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["passed"] else 1
        if args.command == "vocabulary":
            report = vocabulary(plan, discover_optional(catalog))
            if args.output is not None:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0
        if args.command == "buildcache":
            report = warm_build_cache(plan, args.directory)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["passed"] else 1
        if args.command == "run":
            report = run_profile(catalog, plan, args.category, args.batch, args.package, args.directory,
                                 tests=args.tests, binary=args.binary)
        elif args.command == "compile":
            selected_plan = discover_optional(catalog) if args.category == "optional" else plan
            report = compile_profile(selected_plan, args.category, args.batch, args.package, args.directory)
        elif args.command == "code":
            profiles = [profile for profile in plan["include"] if profile["category"] == "code"]
            if not profiles:
                raise ValueError("no code tag profile was found")
            args.directory.mkdir(parents=True, exist_ok=False)
            results = [run_profile(catalog, plan, "code", profile["batch"], profile["package"],
                                   args.directory / profile["id"]) for profile in profiles]
            report = {"schema": SCHEMA, "passed": all(result["passed"] for result in results), "profiles": results}
            (args.directory / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        elif args.command == "optional":
            optional = discover_optional(catalog)
            report = run_profile(catalog, optional, "optional", args.batch, args.package, args.directory,
                                 binary=args.binary)
        else:
            if args.command == "plan":
                if args.optional:
                    report = select_optional_plan(discover_optional(catalog), args.batch)
                elif args.profiles:
                    report = select_profiles_plan(plan, discover_optional(catalog), args.profiles)
                elif args.representative:
                    report = select_representative_plan(plan)
                    report["repetitions"] = args.repetitions
                else:
                    report = select_required_plan(plan, args.category)
                    if args.select_declarations:
                        # Advisory unless applied: recorded, the matrix stays as is.
                        selection = declaration_selection(report["include"], args.select_declarations)
                        report["declaration_selection"] = selection
                        if args.apply_declarations:
                            report = apply_declaration_selection(report, selection)
                        if args.summary is not None:
                            with args.summary.open("a", encoding="utf-8") as summary:
                                summary.write(declaration_summary(report["declaration_selection"], args.category))
                    if args.changed_files is not None:
                        report = select_changed_plan(report, args.changed_files.read_text().splitlines())
                    elif args.changed_since:
                        report = select_changed_plan(report, changed_paths(args.changed_since))
                    if args.verify:
                        # The source filter affects workflow execution only;
                        # verification always proves the whole required suite.
                        report["compiled_coverage"] = verify(catalog, plan)
            else:
                report = verify(catalog, plan)
            if args.output is not None:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            if args.command == "plan" and args.github_output is not None:
                runtime = network_matrix([profile for profile in report["include"] if profile["category"] != "code"],
                                         args.networks)
                runtime = repeat_matrix(runtime, args.repetitions)
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write("matrix=" + json.dumps({"include": runtime}, separators=(",", ":")) + "\n")
                    output.write("count=" + str(len(runtime)) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("passed", True) else 1
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"schema": SCHEMA, "passed": False, "errors": [str(error)]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
