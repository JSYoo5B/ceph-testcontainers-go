# EC primary loss and admission thresholds

`TestECPrimaryAdmissionBoundaries` is a bounded 2+1 Jerasure
`reed_sol_van` scenario under issue #59. It adds primary-loss and non-primary-loss
controls to the existing non-primary EC outcome and recovered-index recipes.
Each of the four subcases owns a fresh three-OSD cluster and one overwrite-enabled
PG. It does not change the existing scenarios or their explicit permissive policy.

| Selector suffix | Configured `MinSize` | Native threshold | One stopped holder |
| --- | --- | --- | --- |
| `primary/default` | omitted | K+1 = 3 | observed inactive/peered PG; bounded client timeouts |
| `primary/permissive` | 2 | K = 2 | new primary; acknowledged overwrite and preserved snapshot |
| `nonprimary/default` | omitted | K+1 = 3 | original primary; bounded client timeouts |
| `nonprimary/permissive` | 2 | K = 2 | original primary; acknowledged overwrite and preserved snapshot |

The test reads the effective native k/m, plugin/technique, overwrite flag and
min_size. It chooses a holder by the reported acting primary and its position
in the full acting vector, rather than treating an OSD ID as a fragment index.
It retains the missing slot (`CRUSH_ITEM_NONE`), the replacement primary, map
epochs, FSID, pool identity and policy before/after I/O. A requested Stop or an
old active PG report cannot supply this admission observation.

Before installing noout, the scenario captures the healthy holder's actual
container ID, PID and start time. Immediately before Stop it inspects the same
holder again and requires that matching, running, non-OOM generation (also not
paused, restarting or dead). An earlier unexpected exit or generation change
fails before the planned Stop; it cannot be credited as the injected fault.
`EC_ADMISSION_STOP_REQUEST` retains both observations and the request time;
`EC_ADMISSION_STOP_RESULT` separately retains the resulting stopped generation,
exit code and OOM state. The same stopped/non-OOM generation must still be
present when the PG fault is recorded. Inspection and Stop remain separate
engine operations, not an atomic compare-and-stop guarantee. Cleanup attempts a
restart only after this scenario actually requested its admitted Stop.

The permissive case uses separate writer and reader librados connections to
acknowledge an overwrite under a self-managed snapshot context. After the
original OSD returns, fresh connections check exact head/snapshot hashes and
versions against that acknowledgement. The default case never lowers min_size:
it performs one read and submits one write to a different, initially absent
object with a bounded client timeout. A timed-out write is **unknown**, not
reported as absent, refused or acknowledged. Both clients shut down before
restoration, and a fresh reader subsequently reports whether that write became
visible; a visible object must contain its complete requested bytes. It is never
resubmitted to manufacture success. The acknowledged seed and snapshot have
their own independent readback and remain separate from this unknown operation.

Run the complete native case on an authorized Linux engine with the selected
essential and client images:

```sh
CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=30m \
  -run '^TestECPrimaryAdmissionBoundaries$' ./internal/integration
```

The source-owned `recovery/ec_admission` batch supplies those two roles in CI.
`EC_ADMISSION_POOL`, `EC_ADMISSION_FAULT`, `EC_ADMISSION_CLIENT` and
`EC_ADMISSION_RECEIPT` retain configuration, placement, native outcomes and
restoration independently. Cleanup restores only the owned stopped holder and
the scoped noout policy, then terminates the fixture even after an assertion
failure. Source fixtures reject shifted shard positions, stale epochs,
unchanged/wrong primaries and changed admission policy; these are model controls,
not native coverage.

This slice does not establish below-K availability, 4+2 layouts, an interruption
during reconstruction, general per-object eligible-fragment classification or
permanent data loss. Missing OSD stores remain owned and recoverable. The
existing recovered-index contribution recipe keeps its own geometry and native
recovery evidence; a final clean state here is not a new contribution witness.
Release/image/platform support is limited to actually exercised native cohorts.
