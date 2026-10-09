#!/usr/bin/env python3
"""Expose actual workflow step outcomes without replacing their exit status."""

import json
import os
from pathlib import Path
import re
import sys


PHASES = (
    ("compile", "Compilation", "SCENARIO_COMPILE_OUTCOME"),
    ("docker", "Docker baseline", "SCENARIO_DOCKER_OUTCOME"),
    ("images", "Published image preparation", "SCENARIO_IMAGES_OUTCOME"),
    ("native", "Native assertions and completion", "SCENARIO_NATIVE_OUTCOME"),
    ("cleanup", "Docker resource cleanup", "SCENARIO_CLEANUP_OUTCOME"),
)
OUTCOMES = frozenset(("success", "failure", "cancelled", "skipped", ""))
CHECK_NAME = re.compile(r"[A-Za-z][A-Za-z0-9 /._-]{0,63}\Z")
PROFILE = re.compile(r"[a-z][a-z0-9_-]{0,79}\Z")


def report(environment):
    check = environment.get("SCENARIO_CHECK_NAME", "")
    profile = environment.get("SCENARIO_PROFILE", "")
    requires_ceph = environment.get("SCENARIO_REQUIRES_CEPH", "")
    if not CHECK_NAME.fullmatch(check) or not PROFILE.fullmatch(profile):
        raise ValueError("workflow check name and profile must be bounded safe identifiers")
    if requires_ceph not in ("true", "false"):
        raise ValueError("workflow must declare whether Ceph images are required")
    phases = []
    for key, label, variable in PHASES:
        outcome = environment.get(variable, "")
        if outcome not in OUTCOMES:
            raise ValueError("workflow step outcome is invalid")
        status = "not required" if key == "images" and requires_ceph == "false" and outcome == "skipped" else outcome or "not run"
        phases.append({"phase": key, "label": label, "outcome": outcome, "display": status})
    return {"check": check, "profile": profile, "phases": phases,
            "failed_phases": [phase["phase"] for phase in phases if phase["outcome"] == "failure"],
            "cancelled_phases": [phase["phase"] for phase in phases if phase["outcome"] == "cancelled"]}


def summary(result):
    rows = ["### " + result["check"] + " / " + result["profile"], "",
            "| Phase | Actual step outcome |", "| --- | --- |"]
    rows.extend("| " + phase["label"] + " | " + phase["display"] + " |" for phase in result["phases"])
    rows += ["", "Step outcomes identify where execution failed; they do not establish its root cause.", ""]
    return "\n".join(rows)


def main():
    try:
        result = report(os.environ)
        directory = Path("artifacts/scenario")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "phases.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as output:
                output.write(summary(result))
        for phase in result["phases"]:
            if phase["outcome"] in ("failure", "cancelled"):
                level = "error" if phase["outcome"] == "failure" else "warning"
                title = result["check"] + " / " + phase["label"]
                print("::" + level + " title=" + title + "::" + phase["label"] + " " + phase["outcome"] + ". Profile: " + result["profile"] + ". See phase summary and saved receipts.")
        return 0
    except (ValueError, OSError) as error:
        print("Workflow phase reporting failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
