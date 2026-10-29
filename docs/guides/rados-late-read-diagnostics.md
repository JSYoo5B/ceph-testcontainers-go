# Preserving late RADOS readback failures

`TestRADOSMutationContracts` submits a truncate, a remove and a guarded
compound write through a two-second client. Its canonical ten-second reader
then observes their eventual effects while the late writer stays connected.
A native `TimedOut` in the final `late-compound` stat must not suppress the
single final receipt, which carries the earlier mutation results and reader
progress.

The test preserves mutation order and exactly one call for each
mutation, their two-second timeout, canonical reader order/ten-second timeout,
90-second convergence bound and three-minute supervised command. The OSD
historic-operation and final byte/version assertions remain the acceptance
oracles. No mutation is retried, and no native timeout is increased.

## Owned partial checkpoints

The late script writes an atomic checkpoint identified by a fresh invocation
token, phase, pool, namespace, Python PID and Linux process start observation.
It keeps the writer, reader and late-writer client IDs, timeout settings and
locally assigned ioctx identities. Each native operation has a started event
flushed before the risky call, then a returned/error event with wall and
monotonic times. Observations contain sizes, byte counts, last-ioctx versions,
exception class and errno, without object bytes, xattr values or credentials.

A started event establishes admission to the wrapper, not that the native
call reached an OSD. If the process is stopped there, the call remains unknown.
A returned timeout does not prove that a mutation was applied. Versions belong
to the preceding ioctx operation; this trace does not make a multi-call view
atomic or prove an uninterrupted Messenger session.

The three mutation results and completion timestamps are retained separately
from the rolling reader trace. At most 48 events and 32 KiB are retained;
`seq`, `dropped_events`, byte/event limits and the captured byte count make
loss explicit. The final mutation receipt format stays unchanged. The
checkpoint is not a substitute for its missing final assertions.

A checkpoint-write failure is reported as incomplete and does not prevent a
native call or trigger a retry. A reader exception is recorded and re-raised
unchanged. Cleanup errors are retained without replacing an already active
primary exception; if cleanup alone fails, the command still fails.

## Failure capture and settlement

On late-phase failure, Go attempts the owned checkpoint before the OSD delay
configuration is restored. It uses a fresh eight-second capture context even
when the operation's context expired. It copies the file through the pinned
container archive API, reads at most 32 KiB plus a sentinel, and closes/joins
the owned reader on expiry. A valid-looking body returned after the deadline
is refused. Missing, oversized, mismatched-token or incomplete observations
remain unavailable; the original native command failure is still reported.

The pinned `testcontainers-go` v0.44.0 archive copy delegates its context to
Moby client v0.6.2's HTTP request and exposes the response body through a
closable TAR reader. No extra native command is launched for failure capture:
its independent stop/inspection allowance could outlive the capture budget.
The requested context budget is not a universal hard wall for an uncooperative
transport or reader-close settlement.

Automatic independent-reader/permutation probes are deliberately absent.
They would change observation conditions and need their own supervision and
budget design. The existing canonical reader's per-operation identity/order
trace is preserved without adding more RADOS reads after its failure.

Normal native completion separately requires a complete, valid checkpoint:
three distinct registered clients with the unchanged timeout settings, three
ordered mutation timeouts, coherent event/drop counts and no write/cleanup
failure. A diagnostic qualification failure is distinguished from the native
operation's own outcome. `stage=complete` alone never establishes successful
process cleanup or proves that the whole Go contract passed.

## Validation boundaries

`TestRadosMutation*` runs the actual embedded Python script with a stub-rados
module. It covers stat/read/xattr timeouts, a state-gated unreturned-call control whose
checkpoint remains at `started`, checkpoint-write failure, cleanup/primary
error ordering, exactly three ordered mutations, explicit rolling loss and
stale/oversized receipt refusal. Object-not-found remains the existing absent
object observation; missing xattr remains distinct from a read failure.
Archive-reader controls cover unavailable reads, valid JSON with a failed
stream, deadline-driven close/join, late matching output and the byte cap.

```sh
CGO_ENABLED=0 go test -mod=readonly -tags=integration,features \
  -run '^TestRadosMutation' -count=1 -timeout=2m ./internal/integration
```

The normal source-owned native batch is:

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch rados_mutation_contracts \
  --package ./internal/integration --directory artifacts/rados-mutation
```

A passing native qualification exercises the canonical checkpoint path, not
a naturally failing post-operation capture. Those failure boundaries remain
fixture evidence until an actual failing native attempt emits them. The
intermittent `late-compound` cause remains unclassified.
