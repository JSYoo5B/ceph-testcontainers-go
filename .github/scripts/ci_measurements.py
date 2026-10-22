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
    # fetch keeps cancelled runs' jobs for the cohorts outcome ledger; the
    # report leaves them out, so a cache with or without them reports alike.
    cancelled = {str(run["id"]) for run in runs if run.get("conclusion") == "cancelled"}
    jobs = {run_id: run_jobs for run_id, run_jobs in load_jobs(cache).items() if run_id not in cancelled}
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


def cohorts(runs, jobs, rows, repository):
    """Join history rows to their measured jobs and group the joined attempts
    by exact configuration (scenario_history.config_key).

    A row joins the job of its repository, run ID, run attempt and job ID.
    Nothing that does not join is dropped: rows without a measured job,
    measured runtime jobs without a row and rows without a complete
    configuration identity are listed with the reason. Every outcome stays in
    the configuration's ledger with its runner time; the timing ranges are of
    passed attempts only, so a failure, cancellation or timeout cannot shorten
    or lengthen them."""
    names = {str(run["id"]): run.get("name") for run in runs}
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
        ledger = {}
        for attempt in group:
            entry = ledger.setdefault(attempt["kind"], {"attempts": 0, "runner_seconds": 0.0})
            entry["attempts"] += 1
            entry["runner_seconds"] = round(entry["runner_seconds"] + (attempt["duration"] or 0), 1)
        passed = [attempt for attempt in group if attempt["kind"] == "passed"]
        configs.append({"config_key": config, "identity": identity, "profile_id": group[0]["profile_id"],
                        "release": group[0]["release"], "network": group[0]["network"] or "",
                        "attempts": len(group), "outcomes": dict(sorted(ledger.items())),
                        "passed": {metric: span([attempt[metric] for attempt in passed])
                                   for metric in COHORT_METRICS},
                        "samples": [{field: attempt[field] for field in ("run_id", "run_attempt", "job_id",
                                                                         "repetition_id", "kind")}
                                    for attempt in group]})
    return {"schema": SCHEMA, "repository": repository, "rows": len(rows), "joined": len(attempts),
            "configs": configs, "incomparable": incomparable, "unmatched_rows": unmatched_rows,
            "unmatched_jobs": unmatched_jobs}


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
    return "\n".join(lines) + "\n"


# Cache access. The cache holds runs/<date>.json (the runs created that UTC
# day), jobs/<run id>.json and contains/<head sha>.json.

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


def fetch(cache, repo, since, until, markers, today=None, api=gh_json_lines, compare=compare_status, workers=8):
    """Fill the cache for the runs created on the UTC days since..until.

    A finished day's run list, a completed run's jobs and a head's marker
    answers are fetched once; today's run list and unfinished runs are
    fetched again on the next call. Job lists and marker answers are fetched
    by workers concurrent requests; a failed request leaves its record
    missing for the next call and is counted.

    Jobs are fetched for every completed run, cancelled ones included: the
    cohorts ledger counts a cancelled attempt's runner time, and a cancelled
    workflow can hold successful sibling jobs. The report leaves cancelled
    runs out on its own. Marker answers are fetched only for heads of
    uncancelled runs, the only heads the report places in a cohort."""
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

    completed = [run for run in runs if run.get("status") == "completed"]
    tasks = [(jobs_of, run) for run in completed if not (cache / "jobs" / (str(run["id"]) + ".json")).exists()]
    heads = sorted({run["head_sha"] for run in completed
                    if run.get("name") in FAMILY and run.get("conclusion") != "cancelled"})
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
    command = commands.add_parser("cohorts", help="Join history rows to measured jobs by configuration")
    command.add_argument("--cache", type=Path, required=True)
    command.add_argument("--history", type=Path, required=True,
                         help="A scenario-history directory (rows/*.jsonl) or downloaded history-row.json artifacts")
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
        if args.command == "cohorts":
            result = cohorts(load_runs(args.cache), load_jobs(args.cache), load_history_rows(args.history), args.repo)
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
            print(json.dumps(fetch(args.cache, args.repo, args.since, until, markers), sort_keys=True))
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
