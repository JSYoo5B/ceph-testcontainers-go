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
# Repository-only Go code: integration tests with their harness and fixtures,
# test helpers, the consumer example that an integration test runs, and
# development/analysis tools. No library package imports
# them; test_structural_review.py checks that against `go list -deps`.
TEST_ONLY_DIRS = ("internal/integration", "internal/clusteraccess", "internal/clustertest",
                  "internal/testhelper", "internal/debugoverhead", "internal/consumerevidence")
TOOL_DIRS = ("internal/tools", "internal/apiinventory", ".github")
TOOL_FILES = {".golangci.yml", "Makefile", ".gitignore"}
# Inputs that change how the library itself builds or which packages exist.
LIBRARY_FILES = {"go.mod", "go.sum"}
OUTPUTS = ("base-lint.json", "head-lint.json", "lint-delta.json", "lint-summary.md",
           "base-structure.json", "head-structure.json", "structure-delta.json", "environments.json", "summary.md", "details.md")


def cell(value):
    text = html.escape(str(value)).replace("\n", " ").replace("\r", " ")
    return re.sub(r"[|`*\[\]@\\]", lambda m: "&#%d;" % ord(m[0]), text)


def git(*args):
    return subprocess.check_output(["git", *args])


def paths(ref):
    return [p.decode("utf-8") for p in git("ls-tree", "-rz", "--name-only", ref).split(b"\0") if p]


def under(name, dirs):
    return any(name == d or name.startswith(d + "/") for d in dirs)


def package_dirs(files):
    """Library package directories of the root module: production Go files
    outside nested modules, fixtures and the repository-only directories."""
    nested = [str(PurePosixPath(p).parent) for p in files if p.endswith("/go.mod")]
    result = set()
    for name in files:
        p = PurePosixPath(name)
        if not name.endswith(".go") or name.endswith("_test.go"):
            continue
        if any(c.startswith((".", "_")) or c in ("testdata", "vendor") for c in p.parts):
            continue
        if under(name, nested) or under(name, TEST_ONLY_DIRS + TOOL_DIRS):
            continue
        if str(p.parent) != ".":
            result.add(str(p.parent))
    return sorted(result)


def classify_path(name, nested):
    """The kind of one changed path: library, test, docs, tool or unknown.
    Unknown is never taken for test-only; it makes the change eligible."""
    p = PurePosixPath(name)
    if name in LIBRARY_FILES:
        return "library"
    if name in TOOL_FILES or under(name, TOOL_DIRS):
        return "tool"
    if name.endswith(".md") or under(name, ("docs",)) or name == "LICENSE":
        return "docs"
    if (name.endswith("_test.go") or "testdata" in p.parts or under(name, TEST_ONLY_DIRS)
            or under(name, nested)):
        return "test"
    if p.parts and p.parts[0] in ("capture", "ceph", "cephfs", "fault", "override", "rbd", "rgw", "internal"):
        # A library package directory: Go source and anything it may embed.
        return "library"
    return "unknown"


def classify(changes, nested):
    """Decide from both sides of every changed path whether to measure.
    changes holds (old, new) path pairs; a deletion or addition has one side."""
    kinds = {}
    for pair in changes:
        for name in pair:
            if name:
                kinds.setdefault(classify_path(name, nested), set()).add(name)
    if "library" in kinds:
        return True, "library implementation or API changed"
    if "unknown" in kinds:
        return True, "unclassified paths changed: " + ", ".join(sorted(kinds["unknown"])[:5])
    if "tool" in kinds:
        return False, "only analysis tools or configuration changed; their own tests cover them"
    return False, "only tests, test helpers, fixtures or documentation changed"


def changed_pairs(base, head):
    """(old, new) pairs of git's rename-aware name-status diff."""
    fields = [f.decode("utf-8") for f in git("diff", "--name-status", "-M", "-z", base, head).split(b"\0") if f]
    pairs, i = [], 0
    while i < len(fields):
        status = fields[i]
        if status[0] in "RC":
            pairs.append((fields[i+1], fields[i+2]))
            i += 3
        else:
            old = fields[i+1] if status[0] != "A" else None
            new = fields[i+1] if status[0] != "D" else None
            pairs.append((old, new))
            i += 2
    return pairs


def eligibility(base, head):
    nested = sorted({str(PurePosixPath(p).parent) for side in (base, head) for p in paths(side) if p.endswith("/go.mod")})
    return classify(changed_pairs(base, head), nested)


def finding_focus(changed, files):
    global_change = bool(set(changed) & GLOBAL_INPUTS)
    focused = sorted(set(files["base"]+files["head"])) if global_change else changed
    return focused, global_change


def run_to(command, output, cwd=None):
    with output.open("wb") as stream:
        subprocess.run(command, cwd=cwd, stdout=stream, stderr=subprocess.DEVNULL, check=True, timeout=240)


ROW_LIMIT = 15
GENERATED = re.compile(rb"(?m)^// Code generated .* DO NOT EDIT\.$")
METRIC_VALUE = {"cyclop": re.compile(r"complexity for function \S+ is (\d+)"),
                "gocognit": re.compile(r"cognitive complexity (\d+)"),
                "funlen": re.compile(r"\((\d+) > \d+\)")}
# lintcalibration's states and reasons, as a reader of the table needs them.
STATE_TEXT = {"new_above_threshold": "newly above threshold", "reported_both": "changed",
              "not_reported_above_threshold": "no longer above threshold"}
REASON_TEXT = {"declaration_absent_in_base": "new or moved function", "declaration_absent_in_head": "removed or moved function"}
METRIC_NAME = {"cyclop": "cyclomatic complexity", "gocognit": "cognitive complexity", "dupl": "duplicate block"}


def arrow(base, head):
    """base -> head (delta); an absent side is named, never zero."""
    if base is None and head is None:
        return "unavailable"
    if base is None:
        return "absent → %d (new)" % head
    if head is None:
        return "%d → absent (removed)" % base
    if base == head:
        return "%d" % head
    return "%d → %d (%+d)" % (base, head, head - base)


def changed_lines(base, head, packages):
    """Added plus deleted lines per library package, over the production,
    non-generated Go files of the measured package directories."""
    out = git("diff", "--numstat", "--no-renames", "-z", base, head)
    per = {}
    for record in out.split(b"\0"):
        fields = record.split(b"\t")
        if len(fields) != 3 or fields[0] == b"-":
            continue
        name = fields[2].decode("utf-8")
        directory = str(PurePosixPath(name).parent)
        if directory not in packages or not name.endswith(".go") or name.endswith("_test.go"):
            continue
        blob = b""
        for ref in (head, base):
            try:
                blob = git("cat-file", "blob", "%s:%s" % (ref, name))
                break
            except subprocess.CalledProcessError:
                continue
        if GENERATED.search(blob[:4096]):
            continue
        per[directory] = per.get(directory, 0) + int(fields[0]) + int(fields[1])
    return per


def package_table(base, head, churn):
    rows = {}
    for side, report in (("base", base), ("head", head)):
        for row in report["packages"]:
            rows.setdefault(row["package"], {})[side] = row
    total = sum(churn.values())
    changed = []
    for package, sides in rows.items():
        b, h = sides.get("base"), sides.get("head")
        lines = [side and side.get("lines") for side in (b, h)]
        decls = [side and side.get("exported_declarations") for side in (b, h)]
        if churn.get(package, 0) == 0 and lines[0] == lines[1] and decls[0] == decls[1]:
            continue
        changed.append((package, lines, decls, churn.get(package, 0)))
    changed.sort(key=lambda r: (-r[3], r[0]))
    if not changed:
        return [], 0
    out = ["#### Changed packages", "",
           "Lines and exported declarations count production, non-generated files. Share is this package's added plus "
           "deleted lines over %d such lines changed in all library packages; a high share is where to look, not a verdict." % total, "",
           "| Package | Lines | Exported declarations | Changed lines (share) |", "| --- | --- | --- | --- |"]
    for package, lines, decls, churned in changed[:ROW_LIMIT]:
        share = "%d (%.0f%%)" % (churned, 100.0 * churned / total) if total else "0"
        out.append("| %s | %s | %s | %s |" % (cell(package), arrow(*lines), arrow(*decls), share))
    if len(changed) > ROW_LIMIT:
        out.append("")
        out.append("%d more changed packages are in base-structure.json and head-structure.json." % (len(changed) - ROW_LIMIT))
    return out + [""], len(changed)


def owner_table(delta):
    rows = sorted(delta["changed_owners"], key=lambda r: (-abs(r["delta"]["exported_methods"]) - abs(r["delta"]["private_methods"]), r["owner"]))
    if not rows:
        return [], 0
    out = ["#### Changed owners", "",
           "Methods declared on the struct and its direct fields; promoted members are excluded and build profiles are merged. "
           "A moved or renamed owner shows as one removed and one introduced row.", "",
           "| Owner | Public methods | Private methods | Fields | Declared at |", "| --- | --- | --- | --- | --- |"]
    for row in rows[:ROW_LIMIT]:
        def value(key):
            return arrow(row["base"][key] if row["base"] else None, row["head"][key] if row["head"] else None)
        where = (row["head"] or row["base"])["locations"]
        out.append("| %s | %s | %s | %s | %s |" % (cell(row["owner"]), value("exported_methods"), value("private_methods"),
                                                   value("fields"), cell(", ".join(where[:2]))))
    if len(rows) > ROW_LIMIT:
        out += ["", "%d more changed owners are in structure-delta.json." % (len(rows) - ROW_LIMIT)]
    return out + [""], len(rows)


def metric_value(entry, side):
    site = entry.get(side)
    if site is None:
        return None
    pattern = METRIC_VALUE.get(entry["linter"])
    match = pattern.search(site["message"]) if pattern else None
    return int(match[1]) if match else None


def signal_table(lint, packages):
    """Changed complexity, length and duplication findings of library code."""
    rows = []
    for entry in lint["entries"]:
        sites = [entry.get(side) for side in ("base", "head")]
        files = [site["file"] for site in sites if site]
        if not files or any(site["test_file"] for site in sites if site):
            continue
        if not any(str(PurePosixPath(f).parent) in packages for f in files):
            continue
        if entry["state"] == "reported_both" and sites[0]["message"] == sites[1]["message"]:
            continue
        rows.append(entry)
    order = {"new_above_threshold": 0, "reported_both": 1, "moved_or_unmatched": 2, "not_reported_above_threshold": 3}
    rows.sort(key=lambda e: (order.get(e["state"], 4), e.get("package_dir", ""), e.get("function", "")))
    if not rows:
        return [], 0
    out = ["#### Changed complexity, length and duplication findings", "",
           "Only findings above the linters' default thresholds exist; a function below them has no value, which is not zero. "
           "Unmatched findings, duplication included, keep their own locations.", "",
           "| Function | Signal | Base → head | State | Head location |", "| --- | --- | --- | --- | --- |"]
    for entry in rows[:ROW_LIMIT]:
        name = ".".join(part for part in (entry.get("package_dir"), entry.get("receiver"), entry.get("function")) if part)
        site = entry.get("head") or entry.get("base")
        signal = METRIC_NAME.get(entry["linter"], entry["linter"])
        if entry["linter"] == "funlen":
            signal = "length (%s)" % ("statements" if "statements" in site["message"] else "lines")
        base, head = metric_value(entry, "base"), metric_value(entry, "head")
        if entry["linter"] == "dupl" or (base is None and head is None):
            values = "see location"
        else:
            values = arrow(base, head)
            values = values.replace("absent", "below threshold")
        state = STATE_TEXT.get(entry["state"]) or REASON_TEXT.get(entry.get("reason", ""), "unmatched: %s" % entry.get("reason", ""))
        out.append("| %s | %s | %s | %s | %s |" % (cell(name or "unidentified"), cell(signal), values, cell(state),
                                                   cell("%s:%d" % (site["file"], site["line"]))))
    if len(rows) > ROW_LIMIT:
        out += ["", "%d more changed findings are in lint-delta.json." % (len(rows) - ROW_LIMIT)]
    return out + [""], len(rows)


def render(base, head, delta, lint, churn, packages):
    """The visible report and its collapsed details."""
    sections, counts = [], {}
    for name, (lines, count) in (("packages", package_table(base, head, churn)), ("owners", owner_table(delta)),
                                 ("findings", signal_table(lint, packages))):
        sections += lines
        counts[name] = count
    if not any(counts.values()):
        visible = "No library structural change: package lines, exported declarations, owners and findings are unchanged.\n"
    else:
        visible = "%d changed packages, %d changed owners, %d changed findings.\n\n" % (
            counts["packages"], counts["owners"], counts["findings"]) + "\n".join(sections)
    unmeasured = delta.get("unmeasured_packages", [])
    details = ["Measured %d library packages; repository-only test helpers (%s) and tools (%s) are excluded, as are "
               "_test.go and generated files." % (len(packages), ", ".join(TEST_ONLY_DIRS), ", ".join(TOOL_DIRS)), "",
               "Tools: structmetrics and lintcalibration from the analyzed source; golangci-lint %s with "
               "internal/tools/lintcalibration/golangci.yml (config SHA-256 %s)." % (
                   cell(lint["tool"]["golangci_lint_version"]), cell(lint["config"]["sha256"])), "",
               "Lint evidence: base raw SHA-256 %s, head raw SHA-256 %s; entries per state: %s." % (
                   cell(lint["base_evidence"]["raw_sha256"]), cell(lint["head_evidence"]["raw_sha256"]),
                   cell(", ".join("%s %d" % item for item in sorted(lint.get("entries_per_state", {}).items())))), ""]
    if unmeasured:
        details += ["Packages without production files on one side: %s." % cell(json.dumps(unmeasured)), ""]
    details += ["The full package distributions, every changed owner and every finding are in the artifact: "
                "base-structure.json, head-structure.json, structure-delta.json and lint-delta.json."]
    return visible, "\n".join(details) + "\n"


def collect(base, head, output, lint):
    eligible, reason = eligibility(base, head)
    if not eligible:
        return "Not measured: %s.\n" % reason, "", "not_requested"
    files = {"base": paths(base), "head": paths(head)}
    packages = sorted(set(package_dirs(files["base"])) | set(package_dirs(files["head"])))
    changed = [p.decode("utf-8") for p in git("diff", "--name-only", "--no-renames", "-z", base, head).split(b"\0") if p]
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
            return "Go toolchain or platform differs between the snapshots; no paired comparison. See environments.json.\n", "", "unavailable"
        unmeasured_both = set(measures["base"]["unmeasured_packages"]) & set(measures["head"]["unmeasured_packages"])
        measured = [p for p in packages if p not in unmeasured_both]
        run_to([tools["structmetrics"], "delta", "-packages", ",".join(measured), "-base-dir", str(snapshots["base"]),
                "-head-dir", str(snapshots["head"])], output / "structure-delta.json")
        pair = [tools["lintcalibration"], "pair", "-base", str(output/"base-lint.json"), "-head", str(output/"head-lint.json"),
                "-base-ref", base, "-head-ref", head, "-base-dir", str(snapshots["base"]), "-head-dir", str(snapshots["head"])]
        run_to(pair, output / "lint-delta.json")
        focus = root / "changed.json"
        focused, _ = finding_focus(changed, files)
        focus.write_text(json.dumps(focused))
        run_to(pair+["-format", "markdown", "-limit", "20", "-changed-files", str(focus)], output / "lint-summary.md")
        delta = json.loads((output / "structure-delta.json").read_text())
        lint_delta = json.loads((output / "lint-delta.json").read_text())
        visible, details = render(measures["base"], measures["head"], delta, lint_delta,
                                  changed_lines(base, head, set(measured)), set(measured))
        return visible, details, "complete"


def classify_main(args):
    eligible, reason = eligibility(args.base, args.head)
    print("Structural review %s: %s." % ("eligible" if eligible else "skipped", reason))
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as stream:
            stream.write("eligible=%s\n" % str(eligible).lower())
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--target-base", help="target branch SHA at the PR event; defaults to base")
    parser.add_argument("--output", type=Path, help="report directory; required unless --classify")
    parser.add_argument("--classify", action="store_true", help="only decide whether the change is measured")
    parser.add_argument("--golangci-lint", default="golangci-lint")
    args = parser.parse_args()
    if args.target_base is None:
        args.target_base = args.base
    if not all(SHA.fullmatch(ref) for ref in (args.base, args.head, args.target_base)):
        parser.error("base and head must be full commit SHAs")
    if args.classify:
        return classify_main(args)
    if args.output is None:
        parser.error("--output is required")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    status = "unavailable"
    text, details = "Structural measurement unavailable; inspect the Code job. No clean result is inferred.\n", ""
    try:
        binary = shutil.which(args.golangci_lint)
        text, details, status = collect(args.base, args.head, output, binary)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, tarfile.TarError):
        print("::warning::Structural measurement unavailable; no quality verdict was produced.")
    elapsed = round(time.monotonic()-started, 3)
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    details += ("\nMeasurement took %.3f s, tool builds and both collections included; maximum child-process RSS %d "
                "(KiB on Linux). Cache state affects both.\n" % (elapsed, peak))
    (output/"summary.md").write_text(text, encoding="utf-8")
    (output/"details.md").write_text(details, encoding="utf-8")
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
            stream.write("# Structural review: %s\n\nBase `%s`; head `%s`.\n\n%s\n%s" % (status, args.base, args.head, text, details))
    return 0 if status != "unavailable" else 1


if __name__ == "__main__":
    raise SystemExit(main())
