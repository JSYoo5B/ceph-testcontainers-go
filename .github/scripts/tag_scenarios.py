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
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ("code", "environment", "short", "topology", "multicluster", "recovery")
RUNTIME_CATEGORIES = tuple(category for category in CATEGORIES if category != "code")
LEGACY_TAGS = ("integration", "auth", "features", "topology", "hostnetwork", "multicluster", "diagnostics")
CHECKERS = {
    "quiescence": ("check_scenario_quiescence", "TestMultiClusterCephFSOriginalProcessQuiescence"),
    "recovery": ("check_scenario_recovery", "TestMultiClusterCephFSOriginalProcessQuiescenceRecovery"),
    "receivers": ("check_scenario_receivers", "TestMultiClusterRBDReceiverReadiness"),
}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*\Z")
DURATION = re.compile(r"([1-9][0-9]*)(ms|s|m|h)\Z")
SCHEMA = "ceph-tag-scenarios/v1"
GO_EXECUTION_ENV = {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off"}
GO_PLATFORMS = frozenset((
    "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios", "js", "linux", "netbsd",
    "openbsd", "plan9", "solaris", "wasip1", "windows", "386", "amd64", "arm", "arm64", "loong64",
    "mips", "mipsle", "mips64", "mips64le", "ppc64", "ppc64le", "riscv64", "s390x", "wasm",
))
GO_TOOL_TAGS = frozenset(("cgo", "gc", "gccgo", "unix", "race", "msan", "asan", "boringcrypto"))


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
        if key not in ("timeout", "job-timeout", "checker", "case") or key in values or not value:
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
        module = importlib.import_module(CHECKERS[checker][0])
        accepted = module.NETWORKS if checker == "receivers" else module.CASES
        if case not in accepted:
            raise ValueError(file["path"] + ": unknown completion checker case")
        if file["tests"] != [CHECKERS[checker][1]]:
            raise ValueError(file["path"] + ": isolated checker must own only its original parent")
    return {"timeout": values["timeout"], "job_timeout": job_timeout,
            "checker": checker, "case": case}


def package_path(file):
    path = Path(file["path"])
    if path.is_absolute() or ".." in path.parts or not file["path"].endswith("_test.go"):
        raise ValueError("invalid source package path")
    parent = path.parent.as_posix()
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", parent):
        raise ValueError("unsupported source package path")
    return "./" + parent if parent != "." else "."


def legacy_tags(catalog):
    # Capability tags come from source too. Adding a new capability does not
    # require changing this runner, a Make selector or a workflow matrix.
    tags = set(LEGACY_TAGS)
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


def native_tags(category, batch, capabilities=LEGACY_TAGS):
    return ",".join((*capabilities, "ci", "ci_" + category, "ci_batch", "ci_batch_" + batch))


def discover(catalog):
    """Build profiles and reject missing/overlapping ownership before execution."""
    profiles, owners, aggregates, all_parents = {}, defaultdict(list), {}, defaultdict(set)
    capabilities = legacy_tags(catalog)
    for file in catalog:
        if not file["tests"]:
            if file.get("metadata"):
                raise ValueError(file["path"] + ": helper file must not declare CI test metadata")
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
            raise ValueError(file["path"] + ": one batch has conflicting budgets or checker scope")
        profiles[key] = profile
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
        actual = [assignment[2]["case"] for assignment in assignments]
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
    for profile in result:
        profile["profile"] = profile["category"] + "/" + profile["batch"] + "/" + profile["package"][2:].replace("/", "-")
        suffix = hashlib.sha256(profile["profile"].encode()).hexdigest()[:8]
        profile["id"] = (profile["category"] + "-" + profile["batch"])[:70] + "-" + suffix
        profile["requires_ceph"] = profile["package"] == "./internal/integration" and profile["category"] != "code"
    return {"schema": SCHEMA, "coverage_scope": "required", "include": result,
            "required_parents": {package: sorted(names) for package, names in sorted(all_parents.items())},
            "required_executions": sum(len(assignments) for assignments in owners.values()),
            "required_profiles": len(result), "capability_tags": list(capabilities)}


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


def load_catalog(root=ROOT):
    result = subprocess.run(["go", "run", "-mod=readonly", str(ROOT / "tools/tagcatalog/main.go"), str(root)],
                            cwd=root, text=True, capture_output=True, timeout=180,
                            env=go_environment())
    if result.returncode:
        raise ValueError("Go AST test catalog failed: " + result.stderr.strip())
    return json.loads(result.stdout)


def discover_optional(catalog):
    """Keep native defect regressions explicitly outside required coverage."""
    profiles = {}
    capabilities = (*legacy_tags(catalog), "native_regression")
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
        profile["requires_ceph"] = profile["package"] == "./internal/integration"
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


def completion(log, expected, package):
    runs, passes, failures, skips = [], [], [], []
    package_passes, packages = [], []
    run_positions, pass_positions = {}, {}
    for index, line in enumerate(log.splitlines()):
        run = re.fullmatch(r"=== RUN\s+(Test\S+)", line)
        if run:
            runs.append(run[1])
            run_positions[run[1]] = index
        result = re.fullmatch(r"\s*--- (PASS|FAIL|SKIP): (Test\S+) \([0-9.]+s\)", line)
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
    if len(package_passes) != 1 or len(packages) != 1 or package_passes[0] >= packages[0]:
        errors.append("native log lacks exactly one successful Go package completion")
    elif any(run_positions[name] >= pass_positions.get(name, -1)
             or pass_positions.get(name, package_passes[0]) >= package_passes[0] for name in runs):
        errors.append("native tests did not finish after RUN and before package completion")
    return {"passed": not errors, "errors": errors, "observed_runs": runs,
            "observed_passes": passes, "failed_tests": failures, "skipped_tests": skips}


def strict_completion(log, profile):
    if profile["checker"] == "none":
        return None
    module = importlib.import_module(CHECKERS[profile["checker"]][0])
    return module.validate(log, profile["case"])


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


def run_profile(catalog, plan, category, batch, package, directory, root=ROOT):
    matches = [profile for profile in plan["include"] if profile["category"] == category
               and profile["batch"] == batch and (package is None or profile["package"] == package)]
    if len(matches) != 1:
        raise ValueError("requested category/batch/package is unsupported or ambiguous")
    profile = matches[0]
    expected = selected_tests(catalog, profile["package"], profile["tags"])
    if not expected:
        raise ValueError("requested runtime profile is empty")
    # Reject malformed source metadata and empty profile before allocating an
    # artifact or starting Go. Reconfirm the actual compiled inventory next.
    actual = compile_inventory(profile["package"], profile["tags"], root)
    if actual != expected:
        raise ValueError("compiled profile differs from its AST source")
    directory.mkdir(parents=True, exist_ok=False)
    command = ["go", "test", "-mod=readonly", "-tags=" + profile["tags"], "-count=1", "-v",
               "-timeout=" + profile["timeout"], profile["package"]]
    started = time.time()
    with (directory / "native.log").open("w", encoding="utf-8") as log:
        environment = go_environment()
        environment.pop("CEPH_TEST_RBD_CLIENT_IMAGE", None)
        child = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
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
    completed = completion(text, expected, native_package)
    strict = strict_completion(text, profile)
    result = {"schema": SCHEMA, "profile": profile, "command": command,
              "elapsed_seconds": round(time.time() - started, 3), "exit_code": exit_code,
              "go_environment": dict(GO_EXECUTION_ENV),
              "compiled_tests": expected, "completion": completed, "strict_completion": strict,
              "classification": failure_kind(exit_code, completed, strict),
              "source": [{"path": file["path"], "sha256": file["sha256"]} for file in catalog]}
    result["passed"] = result["classification"] == "passed"
    (directory / "report.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def compile_profile(plan, category, batch, package, directory, root=ROOT):
    matches = [profile for profile in plan["include"] if profile["category"] == category
               and profile["batch"] == batch and (package is None or profile["package"] == package)]
    if len(matches) != 1:
        raise ValueError("requested category/batch/package is unsupported or ambiguous")
    profile = matches[0]
    directory.mkdir(parents=True, exist_ok=False)
    command = ["go", "test", "-mod=readonly", "-tags=" + profile["tags"], "-c", "-o",
               str((directory / "test-binary").resolve()), profile["package"]]
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=180,
                            env=go_environment())
    (directory / "compile.log").write_text(result.stdout + result.stderr)
    report = {"schema": SCHEMA, "profile": profile, "command": command, "exit_code": result.returncode,
              "go_environment": dict(GO_EXECUTION_ENV),
              "passed": result.returncode == 0,
              "classification": "compiled" if result.returncode == 0 else "compile-failure"}
    (directory / "compile-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
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
            command.add_argument("--batch")
    for name in ("compile", "run"):
        command = commands.add_parser(name)
        command.add_argument("--category", required=True,
                             choices=(*CATEGORIES, "optional") if name == "compile" else CATEGORIES)
        command.add_argument("--batch", required=True)
        command.add_argument("--package")
        command.add_argument("--directory", required=True, type=Path)
    command = commands.add_parser("code")
    command.add_argument("--directory", required=True, type=Path)
    command = commands.add_parser("optional")
    command.add_argument("--batch", required=True)
    command.add_argument("--package")
    command.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            if args.optional and (not args.batch or args.category is not None or args.verify):
                raise ValueError("optional plan requires --batch and excludes --category and --verify")
            if args.batch is not None and not args.optional:
                raise ValueError("plan --batch requires explicit --optional")
        catalog = load_catalog()
        plan = discover(catalog)
        if args.command == "run":
            report = run_profile(catalog, plan, args.category, args.batch, args.package, args.directory)
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
            report = run_profile(catalog, optional, "optional", args.batch, args.package, args.directory)
        else:
            if args.command == "plan":
                if args.optional:
                    report = select_optional_plan(discover_optional(catalog), args.batch)
                else:
                    report = select_required_plan(plan, args.category)
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
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write("matrix=" + json.dumps({"include": [profile for profile in report["include"] if profile["category"] != "code"]}, separators=(",", ":")) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("passed", True) else 1
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"schema": SCHEMA, "passed": False, "errors": [str(error)]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
