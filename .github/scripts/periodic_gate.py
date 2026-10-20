#!/usr/bin/env python3
"""Decide whether a scheduled Ceph periodic run executes its matrix.

The schedule fires every few hours. The matrix runs when:
- the run was dispatched by hand;
- the repository is in a release phase (CI_RELEASE_PHASE is "true"), so one
  commit is run again and again to expose flaky profiles;
- main has a commit that no earlier periodic run executed; or
- no run executed main within REPEAT_AFTER, so even an unchanged main is
  run again regularly.

Otherwise the run ends after this gate.

The campaign schedule, or a dispatch with mode "representative", runs the
representative campaign instead of the matrix: the source-owned
representative batches, repeated on one pinned commit and pinned role image
digests. A campaign run is never an execution of the matrix.

Writes run=true|false, reason and campaign=true|false to GITHUB_OUTPUT and
prints the decision.
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time

# Actions names a job of a called workflow "<caller> / <job>". The matrix
# batches are such jobs, of the runtime callers; every job that
# ceph-periodic.yml defines itself (Gate, the Releases planner, the
# campaign's Pin, History and any later planning or bookkeeping job) has a
# plain name and is never a batch. The called workflow's own planner is its
# Plan job, and the Representative caller's jobs are campaign attempts.
CALLED_JOB = " / "
PLAN_JOB = "Plan"
CAMPAIGN_CALLER = "Representative"
REPEAT_AFTER = datetime.timedelta(days=7)
# Statuses of a run that has not finished yet.
UNFINISHED = ("in_progress", "queued", "waiting", "pending", "requested")
# Conclusions of a job whose native work ran to an end.
FINISHED_WORK = ("success", "failure", "timed_out")


def parse_time(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def runtime_job(job):
    """A matrix batch job: a job of a called runtime workflow other than its
    Plan job, and not a campaign attempt. A job of the periodic workflow
    itself, such as Gate or Releases, plans or records and never counts."""
    caller, separator, called = job["name"].partition(CALLED_JOB)
    return bool(separator) and called != PLAN_JOB and caller != CAMPAIGN_CALLER


def campaign(event, schedule, campaign_schedule, mode):
    """Whether this run is the representative campaign, and why."""
    if event == "schedule" and schedule and schedule == campaign_schedule:
        return True, "campaign schedule"
    if event == "workflow_dispatch" and mode == "representative":
        return True, "campaign dispatched by hand"
    return False, ""


def executed(run, jobs):
    """Whether a run executed native matrix work.

    A finished run counts only when it was not cancelled and a batch job ran
    to an end, passing or failing: a failure stays the commit's recorded
    result and is not repeated to get a pass. A run that is still going counts
    once a batch job has started. A run cancelled before or during its
    batches, or one that ended in its planning jobs (Gate, Releases, a Plan
    job), is not an execution, so the next firing runs the commit again.
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


# The Gate job has five minutes. Its history reads together get at most
# READ_BUDGET of them, each attempt at most ATTEMPT_TIMEOUT; a transient
# failure is retried after RETRY_DELAYS, and the last error is reported when
# the budget or the delays run out.
READ_BUDGET = datetime.timedelta(minutes=3)
ATTEMPT_TIMEOUT = 60
RETRY_DELAYS = (2, 5, 10, 20, 30)
# Errors of the transport or of GitHub's side, which a later attempt can pass:
# HTTP 5xx, an HTTP/2 stream reset or cancel, a dropped connection, a timeout.
TRANSIENT = re.compile(r"HTTP 5\d\d|stream error|connection reset|unexpected EOF|i/o timeout|"
                       r"TLS handshake timeout|timed out|Bad Gateway|Service Unavailable", re.I)


class Reader:
    """Reads GitHub API lists for the gate within one shared time budget."""

    def __init__(self, run=subprocess.run, sleep=time.sleep, clock=time.monotonic, budget=READ_BUDGET):
        self.run, self.sleep, self.clock = run, sleep, clock
        self.deadline = clock() + budget.total_seconds()

    def __call__(self, path):
        """Every page of a GitHub API list, as a list of page objects. A read
        that cannot complete raises RuntimeError with the last error; it is
        never turned into an empty history."""
        delays = list(RETRY_DELAYS)
        while True:
            remaining = self.deadline - self.clock()
            if remaining <= 0:
                raise RuntimeError("gh api " + path + ": the read budget of the gate is spent")
            try:
                result = self.run(["gh", "api", "--paginate", "--slurp", path], text=True, capture_output=True,
                                  timeout=min(ATTEMPT_TIMEOUT, remaining))
                # The exit status decides; stderr is only the diagnostic. A
                # failed process never yields its stdout, even if it parses.
                failed = result.returncode != 0
                error = (result.stderr.strip() or "exit status " + str(result.returncode)) if failed else ""
            except subprocess.TimeoutExpired:
                failed, error = True, "timed out after " + str(int(min(ATTEMPT_TIMEOUT, remaining))) + " s"
            if not failed:
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError as decode:
                    raise RuntimeError("gh api " + path + " returned malformed JSON: " + str(decode)) from None
            if not TRANSIENT.search(error) or not delays or self.clock() + delays[0] >= self.deadline:
                raise RuntimeError("gh api " + path + " failed: " + error)
            print("gh api " + path + " failed, trying again: " + error)
            self.sleep(delays.pop(0))


github = Reader()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", required=True, help="Workflow file name, such as ceph-periodic.yml")
    parser.add_argument("--campaign-schedule", default="",
                        help="Cron of the representative campaign; a firing of it runs the campaign")
    args = parser.parse_args(argv)
    repository, run_id = os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_RUN_ID"])
    event, sha = os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_SHA"]
    output = os.environ.get("GITHUB_OUTPUT")
    is_campaign, why = campaign(event, os.environ.get("PERIODIC_SCHEDULE", ""), args.campaign_schedule,
                                os.environ.get("PERIODIC_MODE", ""))
    if is_campaign:
        print("Running the representative campaign instead of the matrix: " + why)
        if output:
            with open(output, "a", encoding="utf-8") as handle:
                handle.write("run=false\nreason=" + why + "\ncampaign=true\n")
        return 0
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
        pages = github("repos/{}/actions/runs/{}/jobs?per_page=25&filter=latest".format(repository, run["id"]))
        return [job for page in pages for job in page["jobs"]]

    run, reason = decide(event, release_phase, sha, now, runs, jobs_of)
    print(("Running" if run else "Skipping") + " the periodic matrix: " + reason)
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write("run=" + ("true" if run else "false") + "\nreason=" + reason + "\ncampaign=false\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
