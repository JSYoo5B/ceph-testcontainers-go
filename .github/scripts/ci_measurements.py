#!/usr/bin/env python3
"""Measure the runner cost and wall time of pull request and main executions.

fetch reads workflow runs, their jobs with step timings and, for each head
commit, which of the given marker commits it contains, from the GitHub API
(through gh) into a cache directory. A completed run's jobs and a head's
marker answers never change, so a later fetch reads only what is missing and
report works offline from the cache.

report groups the runs of one head commit and event (pull_request or push)
into an execution and measures, per cohort of executions that contain the
same markers:

- job count, runner time (sum of job start-to-completion) and per-job
  rounded-up runner minutes;
- runner acquisition delay: a job's creation to its start. A matrix job is
  created once its needs are met, so this is queue time, not plan time;
- critical-path wall time: the first run's creation to the last job's
  completion;
- per phase (setup, compile, Docker baseline, role images, native, cleanup,
  reporting) runner seconds of the runtime jobs;
- per profile and release, the distribution of job and native durations;
- the peak number of jobs that ran at the same time in the repository.

Cancelled runs are left out: a run cancelled by hand or superseded by a
newer push is not a comparable execution, and an execution that lost one of
its runs to a cancellation is left out as a whole. Only first attempts
count, so a rerun does not double an execution.

The packing estimate answers what running several runtime profiles in one
job would save: per member after the first, the phases that a shared job
runs once (setup, Docker baseline, role images, reporting) and the
acquisition delay, against the serialized compile, native and cleanup time
that the shared job adds to the wall time.
"""

import argparse
import bisect
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import sys

import scenario_history
import tag_scenarios


SCHEMA = "ceph-ci-measurements/v1"
# The workflows that a pull request or a push to main starts. Ceph periodic
# and the optional regressions are measured as jobs (peak concurrency) only.
FAMILY = ("Code checks", "Docker checks", "Ceph short", "Ceph topology", "Ceph multicluster",
          "Ceph recovery", "Ceph release matrix")
EVENTS = ("pull_request", "push")
# Step name prefixes of each phase of a tagged runtime job, in job order.
# Every other step of a runtime job is reporting (summaries, history rows,
# artifact upload and the post steps).
PHASES = (
    ("setup", ("Set up job", "Run actions/checkout", "Run actions/setup-go", "Restore the Go module cache",
               "Restore the Go build cache", "Prepare Go modules")),
    ("compile", ("Compile selected tagged suite",)),
    ("docker", ("Docker baseline",)),
    ("images", ("Resolve native published Ceph roles", "Resolve published Ceph roles")),
    ("native", ("Native assertions",)),
    ("cleanup", ("Verify owned Docker resource cleanup",)),
)
PHASE_NAMES = tuple(name for name, _ in PHASES) + ("reporting",)
# The phases that one shared job of several profiles would run once.
SHARED_PHASES = ("setup", "docker", "images", "reporting")
# Each member of a shared job still compiles its own tag set, runs as its
# own process and proves its own cleanup.
MEMBER_PHASES = ("compile", "native", "cleanup")
RELEASE_CODENAMES = ("squid", "tentacle")
CURRENT_CODENAME = "tentacle"
RUNTIME_STEP = "Native assertions"


def parse_time(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def seconds(start, end):
    start, end = parse_time(start), parse_time(end)
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds())


def percentile(values, rank):
    """Nearest-rank percentile; None for no values."""
    values = sorted(values)
    if not values:
        return None
    index = max(0, math.ceil(rank / 100 * len(values)) - 1)
    return values[index]


def distribution(values):
    values = [value for value in values if value is not None]
    if not values:
        return {"count": 0, "median": None, "p90": None, "max": None, "sum": 0}
    return {"count": len(values), "median": percentile(values, 50), "p90": percentile(values, 90),
            "max": max(values), "sum": round(sum(values), 1)}


def ran(job):
    """Whether a job held a runner. A skipped job never starts one, and a
    job cancelled while queued has timestamps but no step, not even "Set up
    job"."""
    return bool(job.get("conclusion") != "skipped" and job.get("started_at") and job.get("completed_at")
                and job.get("steps"))


def is_runtime(job):
    return any(step.get("name", "").startswith(RUNTIME_STEP) for step in job.get("steps") or ())


def job_phases(job):
    """Seconds of each phase of a runtime job, by its step timings."""
    phases = dict.fromkeys(PHASE_NAMES, 0.0)
    for step in job.get("steps") or ():
        duration = seconds(step.get("started_at"), step.get("completed_at"))
        if duration is None:
            continue
        name = step.get("name", "")
        phase = next((phase for phase, prefixes in PHASES if name.startswith(prefixes)), "reporting")
        phases[phase] += duration
    return phases


def profile_key(run_name, job_name):
    """(release codename, batch) of a runtime job.

    A release caller's job is "<Codename> <category> / <batch>"; the entry
    workflows' jobs are "Scenarios / <batch>" on the current release, and
    Docker checks need no release. A repetition suffix is dropped so that
    attempts of one batch share a key."""
    caller, _, batch = job_name.rpartition(" / ")
    batch = batch.split(" #")[0]
    word = caller.split(" ")[0].lower() if caller else ""
    if run_name == "Docker checks":
        return "none", batch
    if word in RELEASE_CODENAMES:
        return word, batch
    return CURRENT_CODENAME, batch


def cohort_of(markers, contained):
    """The name of the newest marker that a head contains, in marker order.

    Markers are ordered by when they landed; a head that lacks an older
    marker but has a newer one is still classified by the newest."""
    name = "baseline"
    for marker in markers:
        if contained.get(marker["sha"]):
            name = marker["name"]
    return name


def executions(runs, jobs, contains, markers):
    """Executions: the first-attempt family runs of one head and event.

    jobs maps a run ID (as a string) to its job list; contains maps a head
    SHA to {marker SHA: bool}. Returns a list of execution dicts, and the
    count of executions left out with the reason."""
    groups = defaultdict(list)
    for run in runs:
        if run.get("name") in FAMILY and run.get("event") in EVENTS:
            groups[(run["event"], run["head_sha"])].append(run)
    result, skipped = [], Counter()
    for (event, sha), group in sorted(groups.items(), key=lambda item: min(r["created_at"] for r in item[1])):
        # One head can start a workflow twice (a reopened pull request or a
        # label event); keep the first run of each workflow.
        first = {}
        for run in sorted(group, key=lambda run: (run["created_at"], run["id"])):
            first.setdefault(run["name"], run)
        group = list(first.values())
        if not any(run["name"] == "Ceph short" for run in group):
            skipped["no runtime workflow"] += 1
            continue
        if any(run.get("status") != "completed" for run in group):
            skipped["in progress"] += 1
            continue
        if any(run.get("conclusion") == "cancelled" for run in group):
            skipped["cancelled run"] += 1
            continue
        if any(str(run["id"]) not in jobs for run in group):
            skipped["jobs not fetched"] += 1
            continue
        if sha not in contains:
            skipped["markers not fetched"] += 1
            continue
        execution_jobs = []
        if not any(ran(job) for run in group for job in jobs[str(run["id"])] if job.get("run_attempt", 1) == 1):
            skipped["no job held a runner"] += 1
            continue
        for run in group:
            for job in jobs[str(run["id"])]:
                if job.get("run_attempt", 1) == 1:
                    execution_jobs.append({**job, "workflow": run["name"]})
        result.append({"event": event, "head_sha": sha, "cohort": cohort_of(markers, contains[sha]),
                       "created_at": min(run["created_at"] for run in group),
                       # A workflow whose jobs all skipped (a scoped category
                       # without changed batches) concludes "skipped".
                       "conclusion": "success" if all(run.get("conclusion") in ("success", "skipped")
                                                      for run in group) else "failure",
                       "runs": {run["name"]: run["id"] for run in group}, "jobs": execution_jobs})
    return result, dict(skipped)


def execution_metrics(execution):
    held = [job for job in execution["jobs"] if ran(job)]
    runtime = [job for job in held if is_runtime(job)]
    durations = [seconds(job["started_at"], job["completed_at"]) for job in held]
    ends = [parse_time(job["completed_at"]) for job in held]
    wall = (max(ends) - parse_time(execution["created_at"])).total_seconds() if ends else 0.0
    phases = dict.fromkeys(PHASE_NAMES, 0.0)
    for job in runtime:
        for phase, value in job_phases(job).items():
            phases[phase] += value
    return {"event": execution["event"], "head_sha": execution["head_sha"], "cohort": execution["cohort"],
            "conclusion": execution["conclusion"], "created_at": execution["created_at"],
            "jobs": len(held), "runtime_jobs": len(runtime),
            "skipped_jobs": len(execution["jobs"]) - len(held),
            "runner_minutes": round(sum(durations) / 60, 2),
            "billed_minutes": sum(math.ceil(value / 60) if value else 0 for value in durations),
            "wall_minutes": round(wall / 60, 2),
            "acquisition_seconds": [seconds(job["created_at"], job["started_at"]) for job in held],
            "phase_minutes": {phase: round(value / 60, 2) for phase, value in phases.items()}}


def cohort_summary(metrics):
    """Distributions per (cohort, event) of the execution metrics."""
    groups = defaultdict(list)
    for metric in metrics:
        groups[(metric["cohort"], metric["event"])].append(metric)
    summary = []
    for (cohort, event), group in sorted(groups.items()):
        acquisition = [value for metric in group for value in metric["acquisition_seconds"]]
        summary.append({
            "cohort": cohort, "event": event, "executions": len(group),
            "succeeded": sum(metric["conclusion"] == "success" for metric in group),
            "jobs": distribution([metric["jobs"] for metric in group]),
            "runtime_jobs": distribution([metric["runtime_jobs"] for metric in group]),
            "runner_minutes": distribution([metric["runner_minutes"] for metric in group]),
            "billed_minutes": distribution([metric["billed_minutes"] for metric in group]),
            "wall_minutes": distribution([metric["wall_minutes"] for metric in group]),
            "acquisition_seconds": distribution(acquisition),
            "phase_minutes": {phase: distribution([metric["phase_minutes"][phase] for metric in group])
                              for phase in PHASE_NAMES},
        })
    return summary


def runtime_jobs(runs, jobs, markers=None, contains=None):
    """Every successful first-attempt runtime job of a non-cancelled run,
    with its profile key, cohort and phases."""
    result = []
    for run in runs:
        if run.get("conclusion") == "cancelled" or str(run["id"]) not in jobs:
            continue
        cohort = None
        if markers is not None and contains is not None and run["head_sha"] in contains:
            cohort = cohort_of(markers, contains[run["head_sha"]])
        for job in jobs[str(run["id"])]:
            if job.get("run_attempt", 1) != 1 or job.get("conclusion") != "success" or not is_runtime(job):
                continue
            release, batch = profile_key(run["name"], job["name"])
            result.append({"workflow": run["name"], "event": run["event"], "cohort": cohort,
                           "release": release, "batch": batch,
                           "duration": seconds(job["started_at"], job["completed_at"]),
                           "acquisition": seconds(job["created_at"], job["started_at"]),
                           "phases": job_phases(job)})
    return result


def profile_distributions(jobs, cohort=None):
    groups = defaultdict(list)
    for job in jobs:
        if cohort is None or job["cohort"] == cohort:
            groups[(job["release"], job["batch"])].append(job)
    result = []
    for (release, batch), group in sorted(groups.items()):
        entry = {"release": release, "batch": batch, "jobs": len(group),
                 "duration": distribution([job["duration"] for job in group])}
        for phase in PHASE_NAMES:
            entry[phase] = distribution([job["phases"][phase] for job in group])
        result.append(entry)
    return result


def phase_comparison(jobs, cohorts, release=CURRENT_CODENAME):
    """Per cohort, the per-job phase distributions of the batches that ran
    in every cohort on release, so that the cohorts compare the same set."""
    batches = None
    for cohort in cohorts:
        present = {job["batch"] for job in jobs if job["cohort"] == cohort and job["release"] == release}
        batches = present if batches is None else batches & present
    batches = batches or set()
    result = []
    for cohort in cohorts:
        group = [job for job in jobs if job["cohort"] == cohort and job["release"] == release
                 and job["batch"] in batches]
        entry = {"cohort": cohort, "batches": len(batches), "jobs": len(group),
                 "duration": distribution([job["duration"] for job in group])}
        for phase in PHASE_NAMES:
            entry[phase] = distribution([job["phases"][phase] for job in group])
        result.append(entry)
    return result


def peak_concurrency(jobs):
    """The most jobs that held a runner at the same time, and when."""
    events = []
    for job in jobs:
        if ran(job):
            events.append((parse_time(job["started_at"]), 1))
            events.append((parse_time(job["completed_at"]), -1))
    # A job that ends at the second another starts did not overlap it.
    events.sort(key=lambda event: (event[0], event[1]))
    running, peak, when = 0, 0, None
    for moment, change in events:
        running += change
        if running > peak:
            peak, when = running, moment
    return {"peak": peak, "at": when.strftime("%Y-%m-%dT%H:%M:%SZ") if when else None}


def packing_estimate(profiles, pack_size, native_limit):
    """What packing pack_size profiles per job would change, from per-profile
    medians.

    Candidates are profiles whose native p90 is at most native_limit
    seconds. Packing saves, per member after the first, the shared phases
    and that member's acquisition delay (acquisition is given per profile as
    the median); it adds to the pack's wall time every member's compile,
    native and cleanup time except the longest member's."""
    candidates = [profile for profile in profiles if profile["native"]["p90"] is not None
                  and profile["native"]["p90"] <= native_limit]
    candidates.sort(key=lambda profile: profile["native"]["median"])
    packs = [candidates[index:index + pack_size] for index in range(0, len(candidates), pack_size)]
    saved, added_wall = 0.0, []
    for pack in packs:
        if len(pack) < 2:
            continue
        shared = [sum(member[phase]["median"] or 0 for phase in SHARED_PHASES) for member in pack]
        saved += sum(shared) - max(shared)
        serial = [sum(member[phase]["median"] or 0 for phase in MEMBER_PHASES) for member in pack]
        added_wall.append(sum(serial) - max(serial))
    return {"pack_size": pack_size, "native_limit_seconds": native_limit, "candidates": len(candidates),
            "jobs_before": len(candidates), "jobs_after": len(packs),
            "saved_runner_minutes": round(saved / 60, 2),
            "added_wall_seconds_max": max(added_wall) if added_wall else 0}


def running_counts(jobs):
    """Sorted moments and the number of held runners from each moment on."""
    events = []
    for job in jobs:
        if ran(job):
            events.append((parse_time(job["started_at"]), 1))
            events.append((parse_time(job["completed_at"]), -1))
    events.sort(key=lambda event: (event[0], event[1]))
    moments, counts, running = [], [], 0
    for moment, change in events:
        running += change
        moments.append(moment)
        counts.append(running)
    return moments, counts


def runner_limit(jobs, queued_seconds=120):
    """The runner concurrency that a queued job waited for.

    For every job that waited more than queued_seconds for a runner, the
    number of this repository's other jobs that held a runner when it
    started. A job that waits for an account-wide limit starts when the
    running count drops below that limit, so the most frequent count is
    the limit minus one; lower counts mean runners held elsewhere (another
    repository of the account). Jobs of every workflow count."""
    moments, counts = running_counts(jobs)
    observed = []
    for job in jobs:
        if not ran(job) or seconds(job["created_at"], job["started_at"]) <= queued_seconds:
            continue
        # After every start and end at that second, less the job itself.
        index = bisect.bisect_right(moments, parse_time(job["started_at"])) - 1
        observed.append(counts[index] - 1)
    if not observed:
        return {"queued_jobs": 0, "mode": None, "mode_share": None, "limit": None, "running": distribution([])}
    mode, frequency = Counter(observed).most_common(1)[0]
    return {"queued_jobs": len(observed), "mode": mode, "mode_share": round(frequency / len(observed), 3),
            "limit": mode + 1, "running": distribution(observed)}


def busy_intervals(runs, jobs, workflow, minimum_jobs):
    """(first start, last completion) of the runs of workflow that held at
    least minimum_jobs runners, such as a periodic matrix run."""
    intervals = []
    for run in runs:
        held = [job for job in jobs.get(str(run["id"]), ()) if ran(job)]
        if run.get("name") == workflow and len(held) >= minimum_jobs:
            intervals.append((min(parse_time(job["started_at"]) for job in held),
                              max(parse_time(job["completed_at"]) for job in held)))
    return intervals


def contention_summary(metrics, executions_by_sha, intervals, cohort):
    """Wall time and acquisition delay of cohort's executions, split by
    whether a busy interval (a periodic matrix run) overlapped them."""
    groups = {True: [], False: []}
    for metric in metrics:
        if metric["cohort"] != cohort:
            continue
        execution = executions_by_sha[(metric["event"], metric["head_sha"])]
        held = [job for job in execution["jobs"] if ran(job)]
        start = parse_time(execution["created_at"])
        end = max(parse_time(job["completed_at"]) for job in held)
        overlap = any(first < end and start < last for first, last in intervals)
        groups[overlap].append(metric)
    return [{"cohort": cohort, "periodic_overlap": overlap, "executions": len(group),
             "wall_minutes": distribution([metric["wall_minutes"] for metric in group]),
             "acquisition_seconds": distribution([value for metric in group
                                                  for value in metric["acquisition_seconds"]])}
            for overlap, group in sorted(groups.items(), reverse=True)]


def matrix_runs(runs, jobs, workflow, minimum_jobs):
    """Per run of workflow with at least minimum_jobs runtime jobs: jobs,
    runner time, compile time, and how many compiles a reuse of one binary
    per tag set would leave (one per batch, whatever its release or network
    mode)."""
    result = []
    for run in runs:
        if run.get("name") != workflow:
            continue
        held = [job for job in jobs.get(str(run["id"]), ()) if ran(job)]
        runtime = [job for job in held if is_runtime(job)]
        if len(runtime) < minimum_jobs:
            continue
        keys = [profile_key(run["name"], job["name"]) for job in runtime]
        tag_sets = {batch.split(" (")[0] for _, batch in keys}
        compile_seconds = [job_phases(job)["compile"] for job in runtime]
        start = parse_time(run["created_at"])
        end = max(parse_time(job["completed_at"]) for job in held)
        result.append({"run_id": run["id"], "created_at": run["created_at"], "jobs": len(held),
                       "runtime_jobs": len(runtime), "tag_sets": len(tag_sets),
                       "runner_minutes": round(sum(seconds(job["started_at"], job["completed_at"])
                                                   for job in held) / 60, 1),
                       "compile_minutes": round(sum(compile_seconds) / 60, 1),
                       "compile_median_seconds": percentile(compile_seconds, 50),
                       "wall_minutes": round((end - start).total_seconds() / 60, 1),
                       "acquisition_seconds": distribution([seconds(job["created_at"], job["started_at"])
                                                            for job in runtime])})
    return result


def caller_groups(run_jobs):
    """A matrix run's jobs as callers: {caller: (plan seconds, [job seconds])}
    in matrix order. A job is "<caller> / <job>"; the caller's Plan job runs
    first and its other jobs after it."""
    groups = {}
    for job in sorted(run_jobs, key=lambda job: job["id"]):
        if not ran(job) or " / " not in job["name"]:
            continue
        caller, _, name = job["name"].partition(" / ")
        plan, durations = groups.setdefault(caller, [0.0, []])
        duration = seconds(job["started_at"], job["completed_at"])
        if name == "Plan":
            groups[caller][0] = duration
        else:
            durations.append(duration)
    return {caller: (plan, durations) for caller, (plan, durations) in groups.items()}


def simulate(groups, runners, job_limit=None, caller_limit=None):
    """Makespan of callers that share runners free runners.

    groups maps a caller to (plan seconds, [job seconds]) in matrix order.
    A caller runs its plan, then queues its jobs in order with at most
    job_limit queued or running at once (its runtime matrix's max-parallel;
    a dict gives each caller its own, by caller name);
    at most caller_limit callers run at once (the caller matrix's
    max-parallel). Free runners take queued jobs first come, first served,
    then by caller and job order. Returns the makespan in minutes and the
    peak number of runners held."""
    names, callers = list(groups), list(groups.values())
    limits = [(job_limit.get(name) if isinstance(job_limit, dict) else job_limit) or math.inf for name in names]
    caller_limit = caller_limit or math.inf
    admitted, pending, finished = 0, [0] * len(callers), [False] * len(callers)
    active = [0] * len(callers)
    now, queue, running, peak = 0.0, [], [], 0

    def admit():
        nonlocal admitted
        while admitted < len(callers) and sum(1 for index in range(admitted) if not finished[index]) < caller_limit:
            queue.append((now, admitted, -1, callers[admitted][0]))
            admitted += 1

    def release(index):
        """Queue the caller's next jobs; called once its plan has finished."""
        durations = callers[index][1]
        while pending[index] < len(durations) and active[index] < limits[index]:
            queue.append((now, index, pending[index], durations[pending[index]]))
            pending[index] += 1
            active[index] += 1

    admit()
    while queue or running:
        queue.sort()
        while queue and len(running) < runners:
            _, index, job, duration = queue.pop(0)
            running.append((now + duration, index, job))
        peak = max(peak, len(running))
        running.sort()
        now, index, job = running.pop(0)
        if job >= 0:
            active[index] -= 1
        release(index)
        if pending[index] == len(callers[index][1]) and active[index] == 0:
            finished[index] = True
            admit()
    return {"makespan_minutes": round(now / 60, 1), "peak_runners": peak}


def execution_groups(execution, jobs):
    """An execution's runs as simulate() callers, named workflow/caller."""
    groups = {}
    for workflow, run_id in execution["runs"].items():
        for caller, value in caller_groups(jobs[str(run_id)]).items():
            groups[workflow + "/" + caller] = value
    return groups


def category_limits(groups, limits):
    """Per caller the limit of its category, the last word of its name
    ("Tentacle short" is short); callers of other categories are unbounded."""
    return {name: limits.get(name.split(" ")[-1]) for name in groups}


def schedule_summary(periodic_runs, pull_requests, runners, limits, reserve):
    """Simulated makespans that a runtime bound would give.

    periodic_runs are caller groups of full periodic matrix runs; they are
    simulated on every runner, unbounded and with the category limits.
    pull_requests are caller groups of pull request executions, simulated
    on every runner and on only the reserve that the bounded periodic
    matrix leaves them."""
    periodic = []
    for groups in periodic_runs:
        unbounded = simulate(groups, runners)
        bounded = simulate(groups, runners, category_limits(groups, limits))
        periodic.append({"unbounded": unbounded, "bounded": bounded})
    requests = {"all_runners": distribution([simulate(groups, runners)["makespan_minutes"]
                                             for groups in pull_requests]),
                "reserve": distribution([simulate(groups, reserve)["makespan_minutes"]
                                         for groups in pull_requests])}
    return {"runners": runners, "reserve": reserve, "limits": limits, "periodic": periodic,
            "pull_requests": requests}


def report(cache, markers, limits=None, reserve=None, periodic_minimum=100):
    runs = load_runs(cache)
    jobs = load_jobs(cache)
    contains = load_contains(cache, markers)
    found, skipped = executions(runs, jobs, contains, markers)
    metrics = [execution_metrics(execution) for execution in found]
    cohorts = ["baseline"] + [marker["name"] for marker in markers]
    measured = runtime_jobs(runs, jobs, markers, contains)
    all_jobs = [job for run in runs for job in jobs.get(str(run["id"]), ())]
    limit = runner_limit(all_jobs)
    intervals = busy_intervals(runs, jobs, "Ceph periodic", periodic_minimum)
    by_key = {(execution["event"], execution["head_sha"]): execution for execution in found}
    result = {"schema": SCHEMA, "markers": markers, "runs": len(runs), "skipped_executions": skipped,
              "cohorts": cohort_summary(metrics), "executions": metrics,
              "phase_comparison": phase_comparison(measured, [cohort for cohort in cohorts
                                                              if any(job["cohort"] == cohort for job in measured)]),
              "profiles": profile_distributions(measured, cohorts[-1]),
              "peak_concurrency": peak_concurrency(all_jobs), "runner_limit": limit,
              "contention": contention_summary(metrics, by_key, intervals, cohorts[-1]),
              "periodic_runs": matrix_runs(runs, jobs, "Ceph periodic", periodic_minimum)}
    if limits is not None and limit["limit"]:
        periodic = [caller_groups(jobs[str(run["run_id"])]) for run in result["periodic_runs"]]
        requests = [execution_groups(execution, jobs) for execution in found
                    if execution["cohort"] == cohorts[-1] and execution["event"] == "pull_request"]
        result["schedule"] = schedule_summary(periodic, requests, limit["limit"], limits, reserve)
    return result


def minutes_text(value):
    return "-" if value is None else "{:.1f}".format(value)


def markdown(result):
    lines = ["| Cohort | Event | Executions (ok) | Jobs median/p90 | Runtime jobs median | "
             "Runner min median/p90 | Billed min median | Wall min median/p90 | Acquisition s median/p90/max |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for entry in result["cohorts"]:
        acquisition = entry["acquisition_seconds"]
        lines.append("| {} | {} | {} ({}) | {}/{} | {} | {}/{} | {} | {}/{} | {}/{}/{} |".format(
            entry["cohort"], entry["event"], entry["executions"], entry["succeeded"],
            entry["jobs"]["median"], entry["jobs"]["p90"], entry["runtime_jobs"]["median"],
            minutes_text(entry["runner_minutes"]["median"]), minutes_text(entry["runner_minutes"]["p90"]),
            entry["billed_minutes"]["median"],
            minutes_text(entry["wall_minutes"]["median"]), minutes_text(entry["wall_minutes"]["p90"]),
            acquisition["median"], acquisition["p90"], acquisition["max"]))
    lines += ["", "| Cohort | Batches | Jobs | " + " | ".join(PHASE_NAMES) + " | job |",
              "| --- | ---: | ---: |" + " ---: |" * (len(PHASE_NAMES) + 1)]
    for entry in result["phase_comparison"]:
        cells = ["{}/{}".format(entry[phase]["median"], entry[phase]["p90"]) for phase in PHASE_NAMES]
        lines.append("| {} | {} | {} | {} | {}/{} |".format(entry["cohort"], entry["batches"], entry["jobs"],
                                                             " | ".join(cells), entry["duration"]["median"],
                                                             entry["duration"]["p90"]))
    limit = result["runner_limit"]
    lines += ["", "Peak concurrent jobs: {} at {}. Of {} jobs that waited over 2 minutes for a runner, {:.0%} "
              "started when {} others ran: runner limit {}.".format(
                  result["peak_concurrency"]["peak"], result["peak_concurrency"]["at"], limit["queued_jobs"],
                  limit["mode_share"] or 0, limit["mode"], limit["limit"])]
    lines += ["", "| Cohort | Periodic overlap | Executions | Wall min median/p90 | Acquisition s median/p90 |",
              "| --- | --- | ---: | ---: | ---: |"]
    for entry in result["contention"]:
        lines.append("| {} | {} | {} | {}/{} | {}/{} |".format(
            entry["cohort"], "yes" if entry["periodic_overlap"] else "no", entry["executions"],
            minutes_text(entry["wall_minutes"]["median"]), minutes_text(entry["wall_minutes"]["p90"]),
            entry["acquisition_seconds"]["median"], entry["acquisition_seconds"]["p90"]))
    lines += ["", "| Periodic run | Runtime jobs | Tag sets | Runner min | Compile min | Wall min |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for run in result["periodic_runs"]:
        lines.append("| {} | {} | {} | {} | {} | {} |".format(run["created_at"], run["runtime_jobs"], run["tag_sets"],
                                                             run["runner_minutes"], run["compile_minutes"],
                                                             run["wall_minutes"]))
    schedule = result.get("schedule")
    if schedule:
        lines += ["", "Simulated on {} runners; category limits {}:".format(schedule["runners"], schedule["limits"]),
                  "", "| Periodic run | Unbounded min (peak) | Bounded min (peak) |", "| --- | ---: | ---: |"]
        for run, simulated in zip(result["periodic_runs"], schedule["periodic"]):
            lines.append("| {} | {} ({}) | {} ({}) |".format(
                run["created_at"], simulated["unbounded"]["makespan_minutes"], simulated["unbounded"]["peak_runners"],
                simulated["bounded"]["makespan_minutes"], simulated["bounded"]["peak_runners"]))
        requests = schedule["pull_requests"]
        lines += ["", "Pull request executions ({}): median/p90 {}/{} min on {} runners, {}/{} min on {}.".format(
            requests["all_runners"]["count"], requests["all_runners"]["median"], requests["all_runners"]["p90"],
            schedule["runners"], requests["reserve"]["median"], requests["reserve"]["p90"], schedule["reserve"])]
    for estimate in result.get("packing", ()):
        lines += ["", "Packing {pack_size} profiles with native p90 <= {native_limit_seconds:.0f} s: "
                      "{jobs_before} jobs -> {jobs_after}, saves {saved_runner_minutes} runner min, "
                      "adds up to {added_wall_seconds_max:.0f} s to a pack.".format(**estimate)]
    return "\n".join(lines) + "\n"


# Configuration cohorts. A runtime job's saved history row
# (scenario_history.py) says what it ran: profile, release, checked-out tree,
# build key, platform and role image IDs. Joining the measured job to that
# row by execution identity lets repetitions of exactly one configuration be
# compared, which marker ancestry alone cannot establish.

COHORT_METRICS = ("duration", "acquisition") + PHASE_NAMES


def span(values):
    """Sample count and range; no percentile is claimed for a few samples."""
    values = sorted(value for value in values if value is not None)
    if not values:
        return {"count": 0, "min": None, "median": None, "max": None}
    return {"count": len(values), "min": round(values[0], 1), "median": round(percentile(values, 50), 1),
            "max": round(values[-1], 1)}


def load_history_rows(path):
    """History rows of a scenario-history directory (rows/*.jsonl) and of
    downloaded artifacts (history-row.json), each row once."""
    path = Path(path)
    rows = {}
    for row in scenario_history.stored_rows(path):
        rows[row["row_sha256"]] = row
    for item in sorted(path.rglob("history-row.json")):
        row = json.loads(item.read_text(encoding="utf-8"))
        scenario_history.validate_row(row)
        rows[row["row_sha256"]] = row
    return list(rows.values())


def cohorts(runs, jobs, rows, repository, ledger=None):
    """Join history rows to their measured jobs and group the joined attempts
    by exact configuration (scenario_history.config_key).

    A row joins the job of its repository, run ID, run attempt and job ID.
    Nothing that does not join is dropped: rows without a measured job,
    measured runtime jobs without a row and rows without a complete
    configuration identity are listed with the reason. Every outcome stays in
    the configuration's ledger with its runner time; the timing ranges are of
    passed attempts only, so a failure, cancellation or timeout cannot shorten
    or lengthen them.

    With a cache ledger (cache_ledger), each sample carries its job's cache
    condition and each configuration is also split by condition, with the
    same outcome ledger and passed-only ranges per part."""
    names = {str(run["id"]): run.get("name") for run in runs}
    conditions = ledger_by_job(ledger) if ledger is not None else None
    measured = {}
    for run_id, run_jobs in jobs.items():
        for job in run_jobs:
            measured[(run_id, str(job.get("run_attempt", 1)), str(job.get("id")))] = job
    attempts, unmatched_rows, joined = [], [], {}
    for row in rows:
        scenario_history.validate_row(row)
        key = row["execution_key"]
        identity = {field: key.get(field) for field in ("run_id", "run_attempt", "job_id", "repetition_id",
                                                        "profile_id", "network")}
        lookup = (str(key["run_id"]), str(key["run_attempt"]), str(key.get("job_id")))
        reason = None
        if key.get("repository") != repository:
            reason = "row of another repository"
        elif key.get("job_id") in (None, "", scenario_history.MISSING):
            reason = "row has no job ID"
        elif lookup not in measured:
            reason = "job is not in the measurement cache"
        elif lookup in joined:
            reason = "another row names the same job"
        elif not ran(measured[lookup]):
            reason = "job held no runner"
        if reason:
            unmatched_rows.append({**identity, "reason": reason})
            continue
        job = measured[lookup]
        joined[lookup] = row
        attempts.append({**identity, "workflow": names.get(lookup[0]), "job": job.get("name"),
                         "release": scenario_history.release_of(row), "kind": scenario_history.attempt_kind(row),
                         "config_key": scenario_history.config_key(row),
                         "duration": seconds(job["started_at"], job["completed_at"]),
                         "acquisition": seconds(job.get("created_at"), job["started_at"]), **job_phases(job)})
        if conditions is not None:
            attempts[-1]["cache_condition"] = cache_condition(conditions.get(lookup, ()))
    unmatched_jobs = [{"run_id": run_id, "run_attempt": attempt, "job_id": job_id, "job": job.get("name"),
                       "workflow": names.get(run_id)}
                      for (run_id, attempt, job_id), job in sorted(measured.items())
                      if ran(job) and is_runtime(job) and (run_id, attempt, job_id) not in joined]
    groups, incomparable = defaultdict(list), []
    for attempt in attempts:
        if attempt["config_key"] is None:
            incomparable.append({field: attempt[field] for field in ("run_id", "run_attempt", "job_id",
                                                                     "profile_id", "kind")})
        else:
            groups[attempt["config_key"]].append(attempt)
    configs = []
    for config, group in sorted(groups.items(), key=lambda item: (item[1][0]["profile_id"], item[1][0]["release"],
                                                                   item[1][0]["network"] or "", item[0])):
        identity = scenario_history.config_identity(joined[(str(group[0]["run_id"]), str(group[0]["run_attempt"]),
                                                            str(group[0]["job_id"]))])
        fields = ("run_id", "run_attempt", "job_id", "repetition_id", "kind")
        if conditions is not None:
            fields += ("cache_condition",)
        entry = {"config_key": config, "identity": identity, "profile_id": group[0]["profile_id"],
                 "release": group[0]["release"], "network": group[0]["network"] or "", "attempts": len(group),
                 **attempt_summary(group),
                 "samples": [{field: attempt[field] for field in fields} for attempt in group]}
        if conditions is not None:
            parts = defaultdict(list)
            for attempt in group:
                parts[attempt["cache_condition"]].append(attempt)
            entry["by_cache"] = {condition: {"attempts": len(part), **attempt_summary(part)}
                                 for condition, part in sorted(parts.items())}
        configs.append(entry)
    return {"schema": SCHEMA, "repository": repository, "rows": len(rows), "joined": len(attempts),
            "configs": configs, "incomparable": incomparable, "unmatched_rows": unmatched_rows,
            "unmatched_jobs": unmatched_jobs}


def attempt_summary(group):
    """The outcome ledger of attempts and the ranges of the passed ones."""
    ledger = {}
    for attempt in group:
        entry = ledger.setdefault(attempt["kind"], {"attempts": 0, "runner_seconds": 0.0})
        entry["attempts"] += 1
        entry["runner_seconds"] = round(entry["runner_seconds"] + (attempt["duration"] or 0), 1)
    passed = [attempt for attempt in group if attempt["kind"] == "passed"]
    return {"outcomes": dict(sorted(ledger.items())),
            "passed": {metric: span([attempt[metric] for attempt in passed]) for metric in COHORT_METRICS}}


def cohorts_markdown(result):
    lines = ["Joined {} of {} history rows; {} rows and {} measured runtime jobs did not join, {} joined rows "
             "have no complete configuration.".format(result["joined"], result["rows"],
                                                      len(result["unmatched_rows"]), len(result["unmatched_jobs"]),
                                                      len(result["incomparable"])),
             "", "| Profile | Release | Network | Config | Attempts | Outcomes | Passed job s min/median/max | "
                 "Passed native s min/median/max |",
             "| --- | --- | --- | --- | ---: | --- | ---: | ---: |"]
    for config in result["configs"]:
        outcomes = ", ".join("{} {}".format(kind, entry["attempts"]) for kind, entry in config["outcomes"].items())
        cells = []
        for metric in ("duration", "native"):
            value = config["passed"][metric]
            cells.append("-" if not value["count"] else "{}/{}/{}".format(value["min"], value["median"], value["max"]))
        lines.append("| {} | {} | {} | {} | {} | {} | {} | {} |".format(
            config["profile_id"], config["release"], config["network"] or "-", config["config_key"],
            config["attempts"], outcomes, *cells))
    for config in result["configs"]:
        if not config.get("by_cache"):
            continue
        lines += ["", "{} ({}, {}) by cache condition:".format(config["config_key"], config["profile_id"],
                                                                config["release"]),
                  "", "| Cache condition | Attempts | Outcomes | Passed job s min/median/max |",
                  "| --- | ---: | --- | ---: |"]
        for condition, entry in config["by_cache"].items():
            outcomes = ", ".join("{} {}".format(kind, value["attempts"]) for kind, value in entry["outcomes"].items())
            value = entry["passed"]["duration"]
            lines.append("| {} | {} | {} | {} |".format(
                condition, entry["attempts"], outcomes,
                "-" if not value["count"] else "{}/{}/{}".format(value["min"], value["median"], value["max"])))
    return "\n".join(lines) + "\n"


# Cache-state ledger. Which cache a measured job restored or saved decides
# what its setup and compile time mean, and a green job proves no cache
# write: a save step can be skipped, continue on error, or log a failure
# that the job ignores. The ledger reads only what a log line states. A job
# log is fetched once, opt-in and bounded (fetch --cache-logs), and only its
# cache lines are kept (cachelogs/<job id>.json); without that excerpt a
# cache step that ran is "unknown", and a skipped save step is
# "save-skipped" by the step's own conclusion.

CACHE_OUTCOMES = ("hit", "partial-restore", "miss", "restore-failed", "saved", "save-skipped", "save-failed",
                  "unknown")
CACHE_EXCERPT_LINES = 400
CACHE_EXCERPT_WIDTH = 500
LOG_TIMESTAMP = re.compile(r"^\ufeff?\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z ?")
GO_VERSION = re.compile(r"^go version (go\S+) (\S+/\S+)$")
# A BuildKit plain-progress line: "#<vertex> <text>".
BUILDX_LINE = re.compile(r"^#(\d+) (.*)$")
BUILDX_CACHE = re.compile(r"cache manifest|writing cache|exporting cache|sending cache export|ERROR|WARNING")
CACHE_STEP = re.compile(r"^(Restore|Save)\b.*\bcache\b|^(Run|Post Run) actions/cache", re.IGNORECASE)
# An explicit registry cache destination: --cache-to / cache-to with a
# literal ref (one that names a shell or expression variable is not one).
CACHE_TO = re.compile(r"cache-to[=:]\s*\S*?\bref=([^,\s\"']+)")


def cache_excerpt(text):
    """The lines of a job log that the ledger reads, without timestamps:
    every step header, each actions/cache step section (inputs and results),
    the Go toolchain line, the runner's cache mode, lines naming a cache-to
    destination and BuildKit cache lines. Returns (lines, truncated)."""
    lines, section = [], False
    for raw in text.splitlines():
        line = LOG_TIMESTAMP.sub("", raw, count=1).rstrip()
        header = line.startswith("##[group]Run ")
        if header:
            section = line.startswith("##[group]Run actions/cache")
        buildx = BUILDX_LINE.match(line)
        if (header or section or line.startswith("Cache mode: ") or GO_VERSION.match(line) or "cache-to" in line
                or (buildx and BUILDX_CACHE.search(buildx.group(2)))):
            lines.append(line[:CACHE_EXCERPT_WIDTH])
    return lines[:CACHE_EXCERPT_LINES], len(lines) > CACHE_EXCERPT_LINES


def cache_kind(key, path=None):
    text = (key or "") + " " + (path or "")
    if (key or "").startswith("go-modules-") or "pkg/mod" in text:
        return "go-module"
    if (key or "").startswith("go-build-") or "go-build" in text:
        return "go-build"
    return "other"


def cache_entries(lines):
    """Cache operations that an excerpt states, in log order.

    actions/cache: "Cache restored from key" is a hit, or a partial restore
    when the restored key is not the input key; "Cache not found" is a miss;
    "Failed to restore" a failed restore; only "Cache saved with key" is a
    save, and "Failed to save" or a refused reservation a failed one.
    BuildKit: an imported cache manifest is a hit (of the manifest; which
    stages it served is another question), an import ERROR naming "not
    found" a miss and any other a failed import; only a completed "writing
    cache ... manifest" is a save, and an ERROR or WARNING of the export
    vertex a failed one, also when ignore-error kept the build green. A
    requested operation that states none of these stays "unknown".

    "exporting cache to registry" names no destination, and a build may
    import several references and export to another one. An export's key is
    the literal cache-to ref of its step when the step names exactly one,
    and None otherwise; it is never taken from an import."""
    entries, mode, toolchain, platform = [], None, None, None
    current, in_inputs, imports, exports, destinations = None, False, {}, {}, set()

    def settle(entry, outcome, evidence):
        if entry["outcome"] == "unknown":
            entry["outcome"], entry["evidence"] = outcome, evidence

    for line in lines:
        version = GO_VERSION.match(line)
        if version:
            toolchain, platform = version.groups()
        if line.startswith("Cache mode: "):
            mode = line.split(": ", 1)[1]
        for reference in CACHE_TO.findall(line):
            if "$" not in reference:
                destinations.add(reference)
        if line.startswith("##[group]Run "):
            current, destinations = None, {reference for reference in CACHE_TO.findall(line) if "$" not in reference}
            if line.startswith("##[group]Run actions/cache"):
                current = {"source": "actions/cache", "operation": "save" if "/save@" in line else "restore",
                           "key": None, "path": None, "toolchain": toolchain, "platform": platform,
                           "cache_mode": mode, "requested": True, "outcome": "unknown", "evidence": None}
                entries.append(current)
                in_inputs = True
            continue
        if current is not None:
            text = line.replace("##[warning]", "").replace("##[error]", "")
            if line.startswith("##[endgroup]"):
                in_inputs = False
            elif in_inputs and line.startswith("  key: "):
                current["key"] = line[len("  key: "):]
            elif in_inputs and line.startswith("  path: "):
                current["path"] = line[len("  path: "):]
            elif text.startswith("Cache restored from key: "):
                restored = text[len("Cache restored from key: "):]
                settle(current, "hit" if restored == current["key"] else "partial-restore", text)
            elif text.startswith("Cache not found for input keys"):
                settle(current, "miss", text)
            elif text.startswith("Failed to restore"):
                settle(current, "restore-failed", text)
            elif text.startswith("Cache saved with key: "):
                settle(current, "saved", text)
            elif text.startswith("Failed to save") or "Unable to reserve cache" in text:
                settle(current, "save-failed", text)
            elif line.startswith("##[error]"):
                settle(current, current["operation"] + "-failed", text)
            continue
        buildx = BUILDX_LINE.match(line)
        if not buildx:
            continue
        vertex, text = buildx.groups()
        if text.startswith("importing cache manifest from "):
            imports[vertex] = {"source": "buildx", "operation": "import",
                               "key": text[len("importing cache manifest from "):].strip(), "path": None,
                               "toolchain": None, "platform": None, "cache_mode": mode, "requested": True,
                               "outcome": "unknown", "evidence": None}
            exports.pop(vertex, None)
            entries.append(imports[vertex])
        elif text.startswith("exporting cache to registry"):
            exports[vertex] = {"source": "buildx", "operation": "export",
                               "key": next(iter(destinations)) if len(destinations) == 1 else None, "path": None,
                               "toolchain": None, "platform": None, "cache_mode": mode, "requested": True,
                               "outcome": "unknown", "evidence": None}
            imports.pop(vertex, None)
            entries.append(exports[vertex])
        elif vertex in imports:
            if text.startswith("ERROR"):
                settle(imports[vertex], "miss" if "not found" in text else "restore-failed", text)
            elif text.startswith("inferred cache manifest type") and text.endswith("done"):
                settle(imports[vertex], "hit", text)
        elif vertex in exports:
            if text.startswith(("ERROR", "WARNING")):
                settle(exports[vertex], "save-failed", text)
            elif text.startswith("writing cache") and "manifest" in text and text.endswith("done"):
                settle(exports[vertex], "saved", text)
    for entry in entries:
        entry["kind"] = "registry" if entry["source"] == "buildx" else cache_kind(entry["key"], entry["path"])
    return entries


def step_kind(name):
    lowered = name.lower()
    return "go-module" if "module" in lowered else "go-build" if "build" in lowered else "other"


def cache_ledger(runs, jobs, logs, repository):
    """One row per cache operation of every job that held a runner, keyed
    by repository, run ID, run attempt and job ID.

    logs maps a job ID (as a string) to its stored excerpt. Every cache step
    that ran keeps a row: an excerpt's actions/cache sections are matched to
    those steps in order, by operation and by kind where the step name and
    the section's key both give one, and a step without a section is
    "unknown" with the reason (no log, an unavailable log, or no section in
    the excerpt, which may be truncated). A section without such a step and
    every BuildKit operation keep their own row. A skipped save step is
    "save-skipped" with requested false, and a skipped restore step
    "unknown"."""
    run_of = {str(run["id"]): run for run in runs}
    rows = []
    for run_id, run_jobs in sorted(jobs.items(), key=lambda item: int(item[0]) if item[0].isdigit() else 0):
        run = run_of.get(run_id, {})
        for job in run_jobs:
            if not ran(job):
                continue
            steps = [step for step in job["steps"] if CACHE_STEP.match(step.get("name", ""))]
            log = logs.get(str(job.get("id")))
            base = {"repository": repository, "run_id": run_id, "run_attempt": str(job.get("run_attempt", 1)),
                    "job_id": str(job.get("id")), "workflow": run.get("name"), "job": job.get("name"),
                    "event": run.get("event"), "head_branch": run.get("head_branch")}
            fetched = log is not None and "lines" in log
            truncated = bool(fetched and log.get("truncated"))
            base["excerpt_truncated"] = truncated
            if not fetched:
                missing = "log not fetched" if log is None else "log unavailable: " + log.get("unavailable", "")
            else:
                missing = "no cache section in the fetched log" + (" excerpt, which is truncated" if truncated else "")
            parsed = [{**base, **entry, "evidence_source": "log"} for entry in cache_entries(log["lines"])] \
                if fetched else []
            sections = [entry for entry in parsed if entry["source"] == "actions/cache"]
            found, position = [], 0
            for step in steps:
                if step.get("conclusion") == "skipped":
                    continue
                operation = "save" if step["name"].lower().startswith(("save", "post")) else "restore"
                kind = step_kind(step["name"])
                match = next((index for index in range(position, len(sections))
                              if sections[index]["operation"] == operation
                              and (sections[index]["kind"] == kind or "other" in (kind, sections[index]["kind"]))),
                             None)
                if match is None:
                    found.append({**base, "source": "step", "operation": operation, "kind": step_kind(step["name"]),
                                  "key": None, "path": None, "toolchain": None, "platform": None, "cache_mode": None,
                                  "requested": True, "outcome": "unknown", "evidence": missing,
                                  "evidence_source": "none", "step": step["name"]})
                    continue
                # Sections before the match have no step of this name; they
                # keep their own rows.
                found += sections[position:match]
                found.append({**sections[match], "step": step["name"]})
                position = match + 1
            found += sections[position:]
            found += [entry for entry in parsed if entry["source"] != "actions/cache"]
            for step in steps:
                if step.get("conclusion") != "skipped":
                    continue
                save = step["name"].lower().startswith("save")
                found.append({**base, "source": "step", "operation": "save" if save else "restore",
                              "kind": step_kind(step["name"]), "key": None, "path": None, "toolchain": None,
                              "platform": None, "cache_mode": None, "requested": False,
                              "outcome": "save-skipped" if save else "unknown", "evidence": "step skipped",
                              "evidence_source": "step", "step": step["name"]})
            rows += found
    return rows


def cache_condition(rows):
    """What a job restored, as "kind=outcome" per restore or import; the
    condition that its setup and compile time ran under."""
    restores = sorted({(row["kind"], row["outcome"]) for row in rows if row["operation"] in ("restore", "import")})
    return ",".join("{}={}".format(kind, outcome) for kind, outcome in restores) or "no cache restore"


def ledger_by_job(ledger):
    result = defaultdict(list)
    for row in ledger:
        result[(row["run_id"], row["run_attempt"], row["job_id"])].append(row)
    return result


def caches(runs, jobs, logs, repository):
    ledger = cache_ledger(runs, jobs, logs, repository)
    counts = Counter((row["kind"], row["operation"], row["outcome"]) for row in ledger)
    return {"schema": SCHEMA, "repository": repository, "rows": ledger,
            "jobs": len(ledger_by_job(ledger)),
            "logs": sum(1 for row in ledger_by_job(ledger).values() if row[0]["evidence_source"] == "log"),
            "summary": [{"kind": kind, "operation": operation, "outcome": outcome, "rows": count}
                        for (kind, operation, outcome), count in sorted(counts.items())]}


def caches_markdown(result):
    lines = ["{} cache operations of {} jobs; {} jobs have a fetched log excerpt.".format(
                 len(result["rows"]), result["jobs"], result["logs"]),
             "", "| Kind | Operation | Outcome | Rows |", "| --- | --- | --- | ---: |"]
    for entry in result["summary"]:
        lines.append("| {kind} | {operation} | {outcome} | {rows} |".format(**entry))
    lines += ["", "| Run | Attempt | Job | Event | Kind | Operation | Key | Toolchain | Outcome |",
              "| --- | ---: | --- | --- | --- | --- | --- | --- | --- |"]
    for row in result["rows"]:
        lines.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            row["run_id"], row["run_attempt"], row["job"], row["event"] or "-", row["kind"], row["operation"],
            row["key"] or "-", row["toolchain"] or "-", row["outcome"]))
    return "\n".join(lines) + "\n"


# Cache access. The cache holds runs/<date>.json (the runs created that UTC
# day), jobs/<run id>.json, contains/<head sha>.json and, when fetched,
# cachelogs/<job id>.json.

def load_runs(cache):
    runs = {}
    for path in sorted((Path(cache) / "runs").glob("*.json")):
        for run in json.loads(path.read_text()):
            runs[run["id"]] = run
    return sorted(runs.values(), key=lambda run: (run["created_at"], run["id"]))


def load_jobs(cache):
    return {path.stem: json.loads(path.read_text()) for path in sorted((Path(cache) / "jobs").glob("*.json"))}


def load_contains(cache, markers):
    result = {}
    for path in sorted((Path(cache) / "contains").glob("*.json")):
        answers = json.loads(path.read_text())
        if all(marker["sha"] in answers for marker in markers):
            result[path.stem] = answers
    return result


def load_cache_logs(cache):
    return {path.stem: json.loads(path.read_text()) for path in sorted((Path(cache) / "cachelogs").glob("*.json"))}


RUN_FIELDS = ("id", "name", "event", "status", "conclusion", "created_at", "run_started_at", "updated_at",
              "head_sha", "head_branch", "run_attempt")
JOB_FIELDS = ("id", "name", "run_attempt", "status", "conclusion", "created_at", "started_at", "completed_at")
STEP_FIELDS = ("name", "number", "conclusion", "started_at", "completed_at")


def gh_json_lines(path, jq):
    result = subprocess.run(["gh", "api", "--paginate", path, "--jq", jq], text=True, capture_output=True,
                            timeout=300)
    if result.returncode:
        raise ValueError("gh api " + path + " failed: " + result.stderr.strip())
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def compare_status(repo, base, head):
    result = subprocess.run(["gh", "api", "repos/{}/compare/{}...{}".format(repo, base, head), "--jq", ".status"],
                            text=True, capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("compare " + base + "..." + head + " failed: " + result.stderr.strip())
    return result.stdout.strip()


def job_log(repo, job_id):
    """A job's whole log text; GitHub serves no single step's log. Raises
    LookupError when the log is gone (expired or deleted), so that the
    answer is stored instead of asked again."""
    result = subprocess.run(["gh", "api", "--allow-escape-sequences",
                             "repos/{}/actions/jobs/{}/logs".format(repo, job_id)],
                            text=True, capture_output=True, timeout=300)
    if result.returncode:
        message = result.stderr.strip()
        if "HTTP 404" in message or "HTTP 410" in message:
            raise LookupError(message)
        raise ValueError("job log " + str(job_id) + " failed: " + message)
    return result.stdout


def cache_log_candidates(runs, jobs, cache, job_pattern=None):
    """Held-runner jobs of completed runs, oldest first (their logs expire
    first), that have a cache step or whose name matches job_pattern and
    have no stored excerpt."""
    candidates = []
    for run in sorted(runs, key=lambda run: (run["created_at"], run["id"])):
        if run.get("status") != "completed":
            continue
        for job in jobs.get(str(run["id"]), ()):
            if not ran(job) or (Path(cache) / "cachelogs" / (str(job["id"]) + ".json")).exists():
                continue
            if any(CACHE_STEP.match(step.get("name", "")) for step in job["steps"]) or (
                    job_pattern and re.search(job_pattern, job.get("name", ""))):
                candidates.append((run, job))
    return candidates


def fetch_cache_logs(cache, repo, runs, jobs, limit, job_pattern=None, log=job_log, workers=8):
    """Store the cache excerpt of at most limit job logs. A gone log is
    stored as unavailable; any other failure leaves it for the next call."""
    (Path(cache) / "cachelogs").mkdir(parents=True, exist_ok=True)
    fetched = Counter()

    def excerpt_of(run, job):
        path = Path(cache) / "cachelogs" / (str(job["id"]) + ".json")
        record = {"run_id": str(run["id"]), "run_attempt": str(job.get("run_attempt", 1)), "job_id": str(job["id"])}
        try:
            lines, truncated = cache_excerpt(log(repo, job["id"]))
            record.update(lines=lines, truncated=truncated)
            result = "cache logs"
        except LookupError as error:
            record["unavailable"] = str(error)
            result = "unavailable cache logs"
        path.write_text(json.dumps(record, sort_keys=True) + "\n")
        return result

    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks = cache_log_candidates(runs, jobs, cache, job_pattern)[:limit]
        for future in [pool.submit(excerpt_of, run, job) for run, job in tasks]:
            try:
                fetched[future.result()] += 1
            except (ValueError, OSError, subprocess.SubprocessError):
                fetched["failed requests"] += 1
    return fetched


def fetch(cache, repo, since, until, markers, today=None, api=gh_json_lines, compare=compare_status, workers=8,
          cache_logs=0, cache_log_jobs=None, log=job_log):
    """Fill the cache for the runs created on the UTC days since..until.

    A finished day's run list, a completed run's jobs and a head's marker
    answers are fetched once; today's run list and unfinished runs are
    fetched again on the next call. Job lists and marker answers are fetched
    by workers concurrent requests; a failed request leaves its record
    missing for the next call and is counted.

    With cache_logs, the cache excerpts of at most that many job logs of
    these runs are stored as well (fetch_cache_logs); no log is read
    otherwise."""
    cache = Path(cache)
    for name in ("runs", "jobs", "contains"):
        (cache / name).mkdir(parents=True, exist_ok=True)
    today = today or datetime.now(timezone.utc).date()
    day, fetched, runs = since, Counter(), []
    while day <= until:
        path = cache / "runs" / (day.isoformat() + ".json")
        if not path.exists() or day >= today:
            keys = ",".join(field + ": ." + field for field in RUN_FIELDS)
            listed = api("repos/{}/actions/runs?created={}&per_page=100".format(repo, day.isoformat()),
                         ".workflow_runs[] | {" + keys + "}")
            path.write_text(json.dumps(listed, sort_keys=True) + "\n")
            fetched["run lists"] += 1
        runs += json.loads(path.read_text())
        day += timedelta(days=1)
    keys = ",".join(field + ": ." + field for field in JOB_FIELDS)
    step_keys = ",".join(field + ": ." + field for field in STEP_FIELDS)
    jq = ".jobs[] | {" + keys + ", steps: [.steps[] | {" + step_keys + "}]}"

    def jobs_of(run):
        jobs = api("repos/{}/actions/runs/{}/jobs?filter=all&per_page=100".format(repo, run["id"]), jq)
        (cache / "jobs" / (str(run["id"]) + ".json")).write_text(json.dumps(jobs, sort_keys=True) + "\n")
        return "job lists"

    def answers_of(sha):
        path = cache / "contains" / (sha + ".json")
        answers = json.loads(path.read_text()) if path.exists() else {}
        for marker in markers:
            if marker["sha"] not in answers:
                answers[marker["sha"]] = compare(repo, marker["sha"], sha) in ("ahead", "identical")
        path.write_text(json.dumps(answers, sort_keys=True) + "\n")
        return "marker answers"

    completed = [run for run in runs if run.get("status") == "completed" and run.get("conclusion") != "cancelled"]
    tasks = [(jobs_of, run) for run in completed if not (cache / "jobs" / (str(run["id"]) + ".json")).exists()]
    heads = sorted({run["head_sha"] for run in completed if run.get("name") in FAMILY})
    for sha in heads:
        path = cache / "contains" / (sha + ".json")
        if not path.exists() or any(marker["sha"] not in json.loads(path.read_text()) for marker in markers):
            tasks.append((answers_of, sha))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in [pool.submit(function, argument) for function, argument in tasks]:
            try:
                fetched[future.result()] += 1
            except (ValueError, OSError, subprocess.SubprocessError):
                fetched["failed requests"] += 1
    if cache_logs > 0:
        stored = {str(run["id"]): json.loads((cache / "jobs" / (str(run["id"]) + ".json")).read_text())
                  for run in completed if (cache / "jobs" / (str(run["id"]) + ".json")).exists()}
        fetched.update(fetch_cache_logs(cache, repo, completed, stored, cache_logs, cache_log_jobs, log, workers))
    return dict(fetched)


def parse_markers(values):
    """name=sha arguments, oldest first."""
    markers = []
    for value in values:
        name, separator, sha = value.partition("=")
        if not separator or not name or len(sha) < 7 or name == "baseline":
            raise ValueError("a marker is name=<commit sha>, and baseline is reserved: " + value)
        markers.append({"name": name, "sha": sha})
    if len({marker["name"] for marker in markers}) != len(markers):
        raise ValueError("marker names must be distinct")
    return markers


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("fetch", "report"):
        command = commands.add_parser(name)
        command.add_argument("--cache", type=Path, required=True)
        command.add_argument("--marker", action="append", default=[],
                             help="name=<full commit SHA> of a CI change, oldest first")
    command = commands.choices["fetch"]
    command.add_argument("--repo", required=True)
    command.add_argument("--since", required=True, type=lambda value: datetime.strptime(value, "%Y-%m-%d").date())
    command.add_argument("--until", type=lambda value: datetime.strptime(value, "%Y-%m-%d").date())
    command.add_argument("--cache-logs", type=int, default=0, metavar="N",
                         help="Also store the cache lines of at most N job logs (default 0: read no log)")
    command.add_argument("--cache-log-jobs", metavar="REGEX",
                         help="Also read the logs of jobs whose name matches, such as BuildKit image builds")
    command = commands.add_parser("cohorts", help="Join history rows to measured jobs by configuration")
    command.add_argument("--cache", type=Path, required=True)
    command.add_argument("--history", type=Path, required=True,
                         help="A scenario-history directory (rows/*.jsonl) or downloaded history-row.json artifacts")
    command.add_argument("--repo", required=True)
    command.add_argument("--output", type=Path)
    command.add_argument("--markdown", type=Path)
    command = commands.add_parser("caches", help="List the cache-state ledger of the measured jobs")
    command.add_argument("--cache", type=Path, required=True)
    command.add_argument("--repo", required=True)
    command.add_argument("--output", type=Path)
    command.add_argument("--markdown", type=Path)
    command = commands.choices["report"]
    command.add_argument("--output", type=Path)
    command.add_argument("--markdown", type=Path)
    command.add_argument("--pack-size", type=int, action="append", default=[])
    command.add_argument("--native-limit", type=float, default=60.0,
                         help="Native p90 seconds up to which a profile is a packing candidate")
    command.add_argument("--limit", action="append", default=[], metavar="CATEGORY=N",
                         help="Runtime max-parallel of a release caller's category to simulate; "
                              "default tag_scenarios.RELEASE_MAX_PARALLEL")
    command.add_argument("--reserve", type=int,
                         help="Runners left for pull requests; default tag_scenarios.PULL_REQUEST_RUNNERS")
    args = parser.parse_args(argv)
    try:
        if args.command == "caches":
            result = caches(load_runs(args.cache), load_jobs(args.cache), load_cache_logs(args.cache), args.repo)
            if args.output is not None:
                args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            text = caches_markdown(result)
            if args.markdown is not None:
                args.markdown.write_text(text)
            print(text, end="")
            return 0
        if args.command == "cohorts":
            runs, jobs = load_runs(args.cache), load_jobs(args.cache)
            ledger = cache_ledger(runs, jobs, load_cache_logs(args.cache), args.repo)
            result = cohorts(runs, jobs, load_history_rows(args.history), args.repo, ledger)
            if args.output is not None:
                args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            text = cohorts_markdown(result)
            if args.markdown is not None:
                args.markdown.write_text(text)
            print(text, end="")
            return 0
        markers = parse_markers(args.marker)
        if args.command == "fetch":
            until = args.until or datetime.now(timezone.utc).date()
            if args.cache_logs < 0:
                raise ValueError("--cache-logs is a number of job logs, 0 or more")
            print(json.dumps(fetch(args.cache, args.repo, args.since, until, markers, cache_logs=args.cache_logs,
                                   cache_log_jobs=args.cache_log_jobs), sort_keys=True))
            return 0
        limits = dict(tag_scenarios.RELEASE_MAX_PARALLEL)
        for value in args.limit:
            category, separator, number = value.partition("=")
            if not separator or not number.isdigit() or int(number) < 1:
                raise ValueError("a limit is category=<positive number>: " + value)
            limits[category] = int(number)
        reserve = tag_scenarios.PULL_REQUEST_RUNNERS if args.reserve is None else args.reserve
        result = report(args.cache, markers, limits, reserve)
        # Packing candidates are the batches of a pull request execution:
        # the current release, each batch in its default network mode.
        current = [profile for profile in result["profiles"]
                   if profile["release"] == CURRENT_CODENAME and " (" not in profile["batch"]]
        result["packing"] = [packing_estimate(current, size, args.native_limit) for size in args.pack_size]
        if args.output is not None:
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        text = markdown(result)
        if args.markdown is not None:
            args.markdown.write_text(text)
        print(text, end="")
        return 0
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"schema": SCHEMA, "passed": False, "errors": [str(error)]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
