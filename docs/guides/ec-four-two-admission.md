# Native 4+2 admission boundaries

`TestECFourTwoAdmissionBoundaries` exercises two fresh erasure-coded pools:
Ceph's default `min_size=5` and explicit `min_size=4`. Both use Jerasure
`reed_sol_van`, six OSDs, one PG, overwrites and self-managed snapshots.

The test uses six distinct logical CRUSH hosts on one Docker engine. It reads
back each OSD's native CRUSH location and the pool's host failure-domain rule;
this does not represent six physical machines. Insufficient or mismatched
topology fails separately from the I/O admission assertions. The two cases run
sequentially. Six OSD memory targets and the controller/client containers require
more resources than the smaller 2+1 scenarios; check the engine's available
memory before running this batch.

A retained Python process connects one writer and one independent reader before
fault injection. Its seed spans two native stripe widths plus a partial stripe.
The test records actual k/m, stripe geometry, feature configuration, min_size,
acting positions, primary, map epochs and container process generations.

1. Stop the initial primary only after a fresh healthy, non-OOM identity check.
   Require the exact five-holder mapping and a new primary, then acknowledge a
   distinct overwrite through the original clients. Check exact bytes, object
   version and the original snapshot.
2. Stop a surviving non-primary while retaining the replacement primary. Require
   the exact four-holder mapping and a later reported epoch. The explicit
   `min_size=4` case acknowledges another overwrite. The default case remains
   peered and records one timed-out read and one timed-out write to a separate
   object as unknown outcomes. Neither operation is replayed.
3. Close command input, require delivery accounting and complete native process
   exit, and require shutdown receipts from both original clients before any
   stopped OSD starts. Restore the same stores and wait for clean placement.
4. A fresh independent reader checks the last acknowledged head's bytes and
   version, the original snapshot's bytes and version, and whether the uncertain
   object is absent or contains the whole submitted value. Absence is an
   observation, not proof that the earlier write was refused.

Failure cleanup does not start stores while an actor's shutdown remains
unconfirmed. Cluster termination owns the remaining supervised process and
resources. No operation with uncertain delivery or completion is retried.

The fixture batch includes counterexamples for changed holder generations,
shifted fragment slots, stale epochs, incorrect admission states, replacement
clients, changed snapshots, incomplete shutdown and torn uncertain writes.
The native batch is `recovery/ec_four_two`; it requires the `essential` and
`client` images. See [Testing](../contributing/testing.md) for batch commands.

This scenario covers the paired 4+2 admission threshold. Below-K behavior,
interruption during reconstruction, and evidence that each recovered fragment
contributes to decoding are separate scenarios.
