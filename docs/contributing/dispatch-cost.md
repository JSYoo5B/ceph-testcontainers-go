# Dispatch cost assessment

`dispatch_cost.py` connects the existing [dispatch join](ci.md#dispatch-join)
and [configuration cohorts](ci.md#runner-budget) to measured job windows. It
reads saved JSON only, calls neither GitHub nor a native workload, and changes
no workflow, selection or coverage policy.

## One attempt

Retain the expanded `tag-dispatch.json`, complete jobs API pages, workflow-run
API object and corresponding `history-row.json` artifacts from one repository,
run and attempt. Preserve artifact identities and ZIP digests separately; the
assessment consumes these inputs but is not an artifact-authenticity verifier.

```sh
python3 .github/scripts/dispatch_cost.py assess \
  --dispatch tag-dispatch.json --run run.json --jobs jobs.json \
  --receipts downloaded-artifacts --caller 'Scenarios' --output cost.json
```

Repeat `--jobs` for paginated responses. An incomplete jobs envelope or wrong
run identity is inconclusive, not a complete ledger. `--cache-ledger caches.json`
optionally reads the existing `ci_measurements.py caches` output. Missing cache
restore/import evidence remains unknown; a supplied restore/import observation
must have log evidence and belong to this repository/run/attempt/job.

Each identifiable job has exactly one cost bucket. Dispatch association uses
`dispatch_join`; source/profile/network/immutable-image grouping uses
`ci_measurements.cohorts`. Successful, failed, timed-out, cancelled, incomplete,
unmatched, ambiguous and conflicting inputs remain visible. A matched job name
alone supplies neither native success nor a comparable configuration.

Allocation needs positive runner-assignment or executed-step evidence. Empty
steps, a skipped/cancelled conclusion, or a timestamp alone cannot establish
that no runner was assigned. An assigned runner cancelled before its first step
keeps its measurable start/end window; missing or contradictory timing remains
unknown. Unknown/unobserved windows are never converted to zero-cost jobs.
Malformed/unidentified records remain unresolved. Identical repeated job IDs
count one job window and retain their copy positions; contradictory copies keep
all candidate windows without choosing one as truth.

`known_runner_seconds` sums the known job windows once. Its sum over mutually
exclusive buckets must equal the ledger sum. Unknown-cost jobs are counted
separately, so this known sum is not a fabricated complete total. The phase
breakdown reuses the existing step-name taxonomy, with unknown phase counts and
unattributed job-window time separate. A failed job that never reached its
native step still retains its allocation cost.

`observed_wall_seconds` is workflow creation to the last observed completed job
in the supplied run/attempt, not a dependency-DAG critical path. Acquisition is
job creation to runner start, not dependency wait before job creation. API
second-resolution timestamps, runner variation and unknown cache conditions
limit interpretation. Runner seconds are not billing or charged minutes.

## Matched descriptive comparisons

```sh
python3 .github/scripts/dispatch_cost.py compare before.json after.json \
  --output comparison.json
```

A comparison needs two distinct complete successful attempts. Every dispatched
entry must join consistently to a passed execution receipt with a complete
configuration: exact checkout tree/build key, release, profile, resolved network,
platform and immutable Ceph-role image IDs. The expanded dispatch multiset,
repetitions, workflow/event, recorded runner labels and known cache conditions
must also match. Only a successful `Plan` overhead job may remain outside the
runtime entries; other unmatched/out-of-scope allocations, missing entries,
conflicting receipts, unknown timing/cache or cancelled attempts refuse total
comparison. The initial matched-total contract requires explicit Ceph-role image
cohorts; image-free histories do not silently qualify other runtime images.

The result is `uncomparable` with reasons when these conditions are absent.
Comparable reports show sample counts and min/median/max descriptive ranges,
not causal optimization savings. Duplicate attempts cannot inflate sample
counts. Report content hashes detect accidental edits, not forgery or CI
attestation. Dispatched-set reconciliation does not prove complete required-suite
coverage or replace the periodic-plus-campaign coverage contract.
