#!/usr/bin/env python3
"""Collect report-only structural review evidence in the read-only Code job.

Build the existing tools once, inspect exact base/head archives, and retain raw
measurements alongside a bounded summary. Nothing runs containers or changes a
lint policy. The publisher consumes data only, never this job's executables.
"""
import argparse
import hashlib
import html
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import shutil
import subprocess
import tarfile
import tempfile
import time

SCHEMA = "ceph-structural-review/v1"
SHA = re.compile(r"[0-9a-f]{40}\Z")
GLOBAL_INPUTS = {"go.mod", "go.sum", ".golangci.yml", "internal/tools/lintcalibration/golangci.yml"}
OUTPUTS = ("base-lint.json", "head-lint.json", "lint-delta.json", "lint-summary.md",
           "base-structure.json", "head-structure.json", "structure-delta.json", "environments.json", "summary.md")


def cell(value):
    text = html.escape(str(value)).replace("\n", " ").replace("\r", " ")
    return re.sub(r"[|`*\[\]@\\]", lambda m: "&#%d;" % ord(m[0]), text)


def git(*args):
    return subprocess.check_output(["git", *args])


def paths(ref):
    return [p.decode("utf-8") for p in git("ls-tree", "-rz", "--name-only", ref).split(b"\0") if p]


def package_dirs(files):
    """Root-module production directories, without nested modules or fixtures."""
    nested = [str(PurePosixPath(p).parent) for p in files if p.endswith("/go.mod")]
    result = set()
    for name in files:
        p = PurePosixPath(name)
        if not name.endswith(".go") or name.endswith("_test.go"):
            continue
        if any(c.startswith((".", "_")) or c in ("testdata", "vendor") for c in p.parts):
            continue
        if any(name.startswith(n + "/") for n in nested):
            continue
        if str(p.parent) != ".":
            result.add(str(p.parent))
    return sorted(result)


def finding_focus(changed, files):
    global_change = bool(set(changed) & GLOBAL_INPUTS)
    focused = sorted(set(files["base"]+files["head"])) if global_change else changed
    return focused, global_change


def run_to(command, output, cwd=None):
    with output.open("wb") as stream:
        subprocess.run(command, cwd=cwd, stdout=stream, stderr=subprocess.DEVNULL, check=True, timeout=240)


def structure_summary(base, head, delta, limit=10):
    lines = ["### Concrete struct responsibilities", "",
             "Declared exported/private methods and direct fields only; promoted members are excluded. "
             "All lexical build profiles are combined; tests and generated files are excluded. "
             "A removed/introduced owner can be a move; neither means improvement.", "",
             "| Owner / status | Base methods E/P; fields | Head methods E/P; fields | Declaration locations |",
             "| --- | --- | --- | --- |"]
    rows = delta["changed_owners"]
    # Growth first is an inspection order, not a size gate. Preserve deterministic
    # owner ordering for ties, and show both halves of moves in the full delta.
    rows = sorted(rows, key=lambda r: (-(r["delta"]["exported_methods"] + r["delta"]["private_methods"]), r["owner"]))
    def counts(side):
        if side is None:
            return "not measured / absent owner"
        return "%d/%d; %d (declarations=%d)" % (side["exported_methods"], side["private_methods"], side["fields"], side["declarations"])
    for row in rows[:limit]:
        locations = [s + ": " + ", ".join(row[s]["locations"]) for s in ("base", "head") if row[s] is not None]
        lines.append("| %s | %s | %s | %s |" % (cell(row["owner"] + " / " + row["status"]),
                     counts(row["base"]), counts(row["head"]), cell("; ".join(locations))))
    lines += ["", "%d changed owners; %d beyond row limit. Unmeasured package/sides: %s." %
              (len(rows), max(0, len(rows)-limit), cell(json.dumps(delta["unmeasured_packages"]))), "",
              "Largest current owners (context, not new regressions):", "",
              "| Owner | Declared methods E/P | Direct fields | Locations |", "| --- | --- | --- | --- |"]
    largest = sorted(head["owners"], key=lambda r: (-(r["exported_methods"]+r["private_methods"]), r["owner"]))
    for row in largest[:5]:
        lines.append("| %s | %d/%d | %d | %s |" % (cell(row["owner"]), row["exported_methods"],
                     row["private_methods"], row["fields"], cell(", ".join(row["locations"]))))
    lines += ["", "Package totals and every changed owner remain in structure-delta.json; "
              "base-structure.json and head-structure.json retain full distributions and exclusions."]
    return "\n".join(lines)+"\n"


def collect(base, head, output, lint):
    files = {"base": paths(base), "head": paths(head)}
    packages = sorted(set(package_dirs(files["base"])) | set(package_dirs(files["head"])))
    changed = [p.decode("utf-8") for p in git("diff", "--name-only", "--no-renames", "-z", base, head).split(b"\0") if p]
    if not any(p.endswith(".go") or p in GLOBAL_INPUTS for p in changed):
        return "No Go analysis inputs changed; structural measurement was not requested.\n", "not_requested"
    if lint is None:
        raise ValueError("linter unavailable")
    with tempfile.TemporaryDirectory(prefix="structural-review-") as tmp:
        root = Path(tmp)
        tools = {}
        for name in ("structmetrics", "lintcalibration"):
            tools[name] = str(root / name)
            subprocess.run(["go", "build", "-o", tools[name], "./internal/tools/"+name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=120)
        snapshots = {}
        measures = {}
        environments = {}
        for side, ref in (("base", base), ("head", head)):
            snapshot = root / side
            snapshot.mkdir()
            # No archive path, link or permission may escape its own snapshot.
            with tarfile.open(fileobj=io.BytesIO(git("archive", ref))) as archive:
                archive.extractall(snapshot, filter="data")
            snapshots[side] = snapshot
            environments[side] = json.loads(subprocess.check_output(
                ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED"], cwd=snapshot, stderr=subprocess.DEVNULL))
            (output/"environments.json").write_text(json.dumps(environments, sort_keys=True, indent=2)+"\n")
            run_to([tools["structmetrics"], "measure", "-packages", ",".join(packages), "-dir", str(snapshot)], output / (side+"-structure.json"))
            measures[side] = json.loads((output / (side+"-structure.json")).read_text())
            run_to([tools["lintcalibration"], "report", "-golangci-lint", lint, "-dir", str(snapshot), "./..."], output / (side+"-lint.json"))
        if environments["base"] != environments["head"]:
            return "Go toolchain/platform environments differ between snapshots; no paired quality inference. See environments.json.\n", "unavailable"
        unmeasured_both = set(measures["base"]["unmeasured_packages"]) & set(measures["head"]["unmeasured_packages"])
        measured = [p for p in packages if p not in unmeasured_both]
        run_to([tools["structmetrics"], "delta", "-packages", ",".join(measured), "-base-dir", str(snapshots["base"]),
                "-head-dir", str(snapshots["head"])], output / "structure-delta.json")
        pair = [tools["lintcalibration"], "pair", "-base", str(output/"base-lint.json"), "-head", str(output/"head-lint.json"),
                "-base-ref", base, "-head-ref", head, "-base-dir", str(snapshots["base"]), "-head-dir", str(snapshots["head"])]
        run_to(pair, output / "lint-delta.json")
        focus = root / "changed.json"
        focused, global_change = finding_focus(changed, files)
        focus.write_text(json.dumps(focused))
        run_to(pair+["-format", "markdown", "-limit", "20", "-changed-files", str(focus)], output / "lint-summary.md")
        delta = json.loads((output / "structure-delta.json").read_text())
        text = structure_summary(measures["base"], measures["head"], delta)
        if global_change:
            text += "\nGlobal analysis inputs changed; findings focus expands to all module files.\n"
        text += "\n"+(output / "lint-summary.md").read_text()
        if unmeasured_both:
            text += "\nNo non-generated production files on either side: "+cell(", ".join(sorted(unmeasured_both)))+".\n"
        return text, "complete"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--target-base", help="target branch SHA at the PR event; defaults to base")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--golangci-lint", default="golangci-lint")
    args = parser.parse_args()
    if args.target_base is None:
        args.target_base = args.base
    if not all(SHA.fullmatch(ref) for ref in (args.base, args.head, args.target_base)):
        parser.error("base and head must be full commit SHAs")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    status = "unavailable"
    text = "Structural measurement unavailable; inspect the Code job. No clean result is inferred.\n"
    try:
        binary = shutil.which(args.golangci_lint)
        text, status = collect(args.base, args.head, output, binary)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, tarfile.TarError):
        print("::warning::Structural measurement unavailable; no quality verdict was produced.")
    elapsed = round(time.monotonic()-started, 3)
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    text = "# Structural review\n\nBase `%s`; head `%s`. Status: **%s**.\n\n" % (args.base, args.head, status)+text
    text += "\nMeasurement wall time: %.3f s; maximum child-process RSS: %d KiB (Linux). " % (elapsed, peak)
    text += "Includes tool builds and both collections; cache state affects cost. No review-time or token saving is inferred.\n"
    (output/"summary.md").write_text(text, encoding="utf-8")
    receipt = {"schema": SCHEMA, "base": args.base, "head": args.head, "target_base": args.target_base, "status": status,
               "run_id": int(os.environ.get("GITHUB_RUN_ID", "0")), "attempt": int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")),
               "analysis_sha": git("rev-parse", "HEAD").decode().strip(), "elapsed_seconds": elapsed,
               "maximum_child_rss_kib": peak, "go_version": subprocess.check_output(["go", "version"]).decode().strip(),
               "go_environment": json.loads(subprocess.check_output(["go", "env", "-json", "GOOS", "GOARCH", "CGO_ENABLED"])),
               "build_tags_source": "internal/tools/lintcalibration/golangci.yml", "files": {}}
    for name in OUTPUTS:
        path = output / name
        if path.exists():
            receipt["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (output/"receipt.json").write_text(json.dumps(receipt, sort_keys=True, indent=2)+"\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(text)
    return 0 if status != "unavailable" else 1


if __name__ == "__main__":
    raise SystemExit(main())
