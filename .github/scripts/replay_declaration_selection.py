#!/usr/bin/env python3
"""Replay the advisory declaration selection over merged pull requests.

For each of the last merges on the first-parent history of a ref, the
merge's second parent is the pull request head and its first parent the
base. The head's tree is exported, its plan discovered from its own source,
and tools/batchselect reports which batches the pull request's diff
selects. Nothing here calls the GitHub API: compare the table with each
pull request's full run by hand (issue #116).
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile

import tag_scenarios as engine


def git(*args, root=engine.ROOT):
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("git " + " ".join(args) + " failed: " + result.stderr.decode(errors="replace").strip())
    return result.stdout


def merges(ref, count, root=engine.ROOT):
    """(merge, base, head, subject) of the last count merges of ref."""
    rows = []
    text = git("log", "--merges", "--first-parent", "-n", str(count), "--format=%H%x09%P%x09%s", ref, root=root)
    for row in text.decode().splitlines():
        commit, parents, subject = row.split("\t", 2)
        parents = parents.split()
        if len(parents) == 2:
            rows.append((commit, parents[0], parents[1], subject))
    return rows


def export(commit, directory, root=engine.ROOT):
    """Write commit's tree into directory."""
    archive = Path(directory) / "tree.tar"
    archive.write_bytes(git("archive", "--format=tar", commit, root=root))
    tree = Path(directory) / "tree"
    with tarfile.open(archive) as source:
        source.extractall(tree, filter="data")
    return tree


def head_plan(tree):
    """The plan of the head, by the head's own planner: older source does
    not satisfy the current planner's rules."""
    output = Path(tree).parent / "plan.json"
    result = subprocess.run([sys.executable, str(Path(tree) / ".github/scripts/tag_scenarios.py"), "plan",
                             "--output", str(output)], cwd=tree, capture_output=True, text=True, timeout=600,
                            env=engine.go_environment())
    if result.returncode:
        raise ValueError("the head's planner failed: " + result.stderr.strip())
    return json.loads(output.read_text())


def replay_one(base, head, tool, root=engine.ROOT):
    with tempfile.TemporaryDirectory() as directory:
        tree = export(head, directory, root)
        plan = head_plan(tree)
        profiles = [profile for profile in plan["include"] if profile["package"] == engine.DECLARATION_PACKAGE]
        return engine.declaration_selection(
            profiles, base, tree,
            runner=lambda batches, base, tree: engine.run_batchselect(batches, base, tree, head=head, tool=tool))


def row(merge, subject, record):
    number = re.search(r"#([0-9]+)", subject)
    reason = record.get("error") or "; ".join(record["fallback"][:1]) or (
        str(len(record["changed_declarations"])) + " changed declarations")
    return {"merge": merge[:7], "pull_request": "#" + number[1] if number else "",
            "selected": record["selected_profiles"], "total": record["total_profiles"], "why": reason,
            "selected_batches": sorted(record["selected"]) if not record["fallback"] else []}


def table(rows):
    lines = ["| Merge | PR | Selected | Why |", "| --- | --- | ---: | --- |"]
    for item in rows:
        why = item["why"].replace("|", "\\|")
        if item["selected_batches"] and item["selected"] < item["total"]:
            why += ": " + ", ".join(item["selected_batches"])
        lines.append("| " + item["merge"] + " | " + item["pull_request"] + " | " + str(item["selected"]) + " of "
                     + str(item["total"]) + " | " + why + " |")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", default="origin/main")
    parser.add_argument("--merges", type=int, default=15)
    parser.add_argument("--output", type=Path, help="Write the rows as JSON")
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory() as directory:
        tool = Path(directory) / "batchselect"
        subprocess.run(["go", "build", "-mod=readonly", "-o", str(tool), "./tools/batchselect"], cwd=engine.ROOT,
                       check=True, env=engine.go_environment(), timeout=600)
        rows = []
        for merge, base, head, subject in merges(args.ref, args.merges):
            try:
                record = replay_one(base, head, tool)
            except (ValueError, OSError, subprocess.SubprocessError) as error:
                record = {"error": str(error).splitlines()[0], "fallback": [], "changed_declarations": [],
                          "selected": {}, "selected_profiles": 0, "total_profiles": 0}
            rows.append(row(merge, subject, record))
            print(table(rows[-1:]).splitlines()[-1], file=sys.stderr, flush=True)
    if args.output is not None:
        args.output.write_text(json.dumps(rows, indent=2) + "\n")
    print(table(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
