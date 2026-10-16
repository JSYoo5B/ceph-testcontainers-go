#!/usr/bin/env python3
"""Decide whether a scheduled Ceph periodic run executes its matrix.

The schedule fires every few hours. The matrix runs when:
- the run was dispatched by hand;
- the repository is in a release phase (CI_RELEASE_PHASE is "true"), so one
  commit is run again and again to expose flaky profiles;
- main has a commit that no earlier periodic run executed; or
- the last execution of this commit is older than REPEAT_AFTER, so even an
  unchanged main is run again regularly.

Otherwise the run ends after this gate. Writes run=true|false and reason to
GITHUB_OUTPUT and prints the decision.
"""

import argparse
import datetime
import json
import os
import subprocess
import sys

GATE_JOB = "Gate"
REPEAT_AFTER = datetime.timedelta(days=7)
# Runs older than this are not inspected; the repeat rule covers them.
LOOKBACK = 50


def parse_time(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def executed(jobs):
    """Whether a run started any matrix job, not only the gate."""
    return any(job["name"] != GATE_JOB and job.get("conclusion") != "skipped" for job in jobs)


def decide(event, release_phase, sha, now, runs, jobs_of):
    """Return (run, reason). runs are this workflow's earlier runs, newest
    first; jobs_of(run) lists a run's jobs."""
    if event == "workflow_dispatch":
        return True, "dispatched by hand"
    if release_phase:
        return True, "release phase"
    for run in runs:
        if run.get("status") not in ("completed", "in_progress", "queued", "waiting", "pending"):
            continue
        if not executed(jobs_of(run)):
            continue
        if run["head_sha"] != sha:
            return True, "main changed since the last executed run (" + run["head_sha"][:12] + ")"
        age = now - parse_time(run["created_at"])
        if age >= REPEAT_AFTER:
            return True, "repeat of an unchanged commit after " + str(age.days) + " days"
        return False, "commit already executed at " + run["created_at"]
    return True, "no earlier executed run"


def github(path):
    result = subprocess.run(["gh", "api", path], text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError("gh api " + path + " failed: " + result.stderr.strip())
    return json.loads(result.stdout)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", required=True, help="Workflow file name, such as ceph-periodic.yml")
    args = parser.parse_args(argv)
    repository, run_id = os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_RUN_ID"])
    event, sha = os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_SHA"]
    release_phase = os.environ.get("CI_RELEASE_PHASE", "") == "true"
    runs = [run for run in github("repos/{}/actions/workflows/{}/runs?branch=main&per_page={}".format(
        repository, args.workflow, LOOKBACK))["workflow_runs"] if run["id"] != run_id]

    def jobs_of(run):
        return github("repos/{}/actions/runs/{}/jobs?per_page=100".format(repository, run["id"]))["jobs"]

    run, reason = decide(event, release_phase, sha, datetime.datetime.now(datetime.timezone.utc), runs, jobs_of)
    print(("Running" if run else "Skipping") + " the periodic matrix: " + reason)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write("run=" + ("true" if run else "false") + "\nreason=" + reason + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
