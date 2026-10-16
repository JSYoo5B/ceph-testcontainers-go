#!/usr/bin/env python3
"""Decide whether a scheduled Ceph periodic run executes its matrix.

The schedule fires every few hours. The matrix runs when:
- the run was dispatched by hand;
- the repository is in a release phase (CI_RELEASE_PHASE is "true"), so one
  commit is run again and again to expose flaky profiles;
- main has a commit that no earlier periodic run executed; or
- no run executed main within REPEAT_AFTER, so even an unchanged main is
  run again regularly.

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
# Statuses of a run that has not finished yet.
UNFINISHED = ("in_progress", "queued", "waiting", "pending", "requested")
# Conclusions of a job whose native work ran to an end.
FINISHED_WORK = ("success", "failure", "timed_out")


def parse_time(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def runtime_job(job):
    """A matrix batch job: not the gate and not a category's Plan job."""
    return job["name"] != GATE_JOB and not job["name"].endswith("/ Plan")


def executed(run, jobs):
    """Whether a run executed native matrix work.

    A finished run counts only when it was not cancelled and a batch job ran
    to an end, passing or failing: a failure stays the commit's recorded
    result and is not repeated to get a pass. A run that is still going counts
    once a batch job has started. A run cancelled before or during its
    batches, or one that ended after Gate and Plan alone, is not an
    execution, so the next firing runs the commit again.
    """
    batches = [job for job in jobs if runtime_job(job)]
    if run.get("status") in UNFINISHED:
        return any(job.get("status") in ("in_progress", "completed") and job.get("conclusion") != "skipped"
                   for job in batches)
    if run.get("status") != "completed" or run.get("conclusion") == "cancelled":
        return False
    return any(job.get("conclusion") in FINISHED_WORK for job in batches)


def decide(event, release_phase, sha, now, runs, jobs_of):
    """Return (run, reason). runs must be every earlier run of this workflow
    created within REPEAT_AFTER before now, newest first; jobs_of(run) lists
    a run's jobs."""
    if event == "workflow_dispatch":
        return True, "dispatched by hand"
    if release_phase:
        return True, "release phase"
    for run in runs:
        if now - parse_time(run["created_at"]) >= REPEAT_AFTER:
            continue
        if not executed(run, jobs_of(run)):
            continue
        if run["head_sha"] != sha:
            return True, "main changed since the last executed run (" + run["head_sha"][:12] + ")"
        return False, "commit already executed at " + run["created_at"]
    # Nothing executed main in the repeat interval: a new commit, or an
    # unchanged one that is due for its repeat.
    return True, "no execution of main in the last " + str(REPEAT_AFTER.days) + " days"


def github(path):
    """Every page of a GitHub API list, as a list of page objects."""
    result = subprocess.run(["gh", "api", "--paginate", "--slurp", path], text=True, capture_output=True, timeout=120)
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
    now = datetime.datetime.now(datetime.timezone.utc)
    # Every run of the repeat interval, across all pages: a fixed count would
    # forget an execution behind enough skipped gates.
    since = (now - REPEAT_AFTER).strftime("%Y-%m-%dT%H:%M:%SZ")
    pages = github("repos/{}/actions/workflows/{}/runs?branch=main&per_page=100&created=%3E%3D{}".format(
        repository, args.workflow, since))
    runs = sorted((run for page in pages for run in page["workflow_runs"] if run["id"] != run_id),
                  key=lambda run: run["created_at"], reverse=True)

    def jobs_of(run):
        pages = github("repos/{}/actions/runs/{}/jobs?per_page=100&filter=latest".format(repository, run["id"]))
        return [job for page in pages for job in page["jobs"]]

    run, reason = decide(event, release_phase, sha, now, runs, jobs_of)
    print(("Running" if run else "Skipping") + " the periodic matrix: " + reason)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write("run=" + ("true" if run else "false") + "\nreason=" + reason + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
