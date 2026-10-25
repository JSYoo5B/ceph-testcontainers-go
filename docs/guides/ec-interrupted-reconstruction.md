# Interrupting an EC reconstruction

`TestECInterruptedReconstruction` exercises one 2+1 Jerasure `reed_sol_van`
pool, one PG, `min_size=2` and four logical OSD domains on a single engine.
It shares the compound-recovery writer ledger, supervision, backfill cursor
parser, configuration handles and owned cleanup with
`TestOSDSecondFaultDuringBackfill`. The replicated scenario keeps its original
pool policy and omission-shaped acting vectors; EC keeps fragment-indexed
vectors with `CRUSH_ITEM_NONE` in the missing slot.

A non-primary holder is stopped with its store retained and marked out while
`nobackfill` holds transfer to the spare. The primary's native query must show
that one indexed target at `MIN`, with the PG interval retained. After release,
a same-interval query must show its non-MIN/non-MAX cursor advanced and work
still in flight or scanned ahead. The EC case checks this predicate again
immediately before requesting the spare's Stop. A changed interval, completed
transfer or unavailable progress is an unreached phase, never a passing skip.
The observation and engine Stop are separate operations, not atomic interruption
of a particular object or byte. This is graceful container Stop of a retained
OSD, not power loss, storage corruption or permanent shard loss.

After the blocking pre-Stop PG query returns, a fresh engine inspection must
still match the captured CID/PID/start and healthy running/non-OOM state. A prior
exit, OOM, pause or replacement during that query refuses Stop without acquiring
planned-restart ownership. `EC_RECONSTRUCTION_STOP_REQUEST` retains this fresh
post-query admission separately from the resulting stopped-state receipt.
The original CID/PID/start and running/non-OOM state admit both EC stops; their
resulting stopped generations are checked independently. Surviving fragment
positions, primary and advancing map epochs must match the original two
survivors. The threshold never changes. A live librados client writes distinct
objects once, logging intent and acknowledgement/error separately. Only writes
acknowledged after the second outage is confirmed establish continued I/O.
Errors and outstanding calls remain classified by the shared ledger and are
never replayed. Fresh clients verify all acknowledged objects independently;
unknown operations, if any, have their effects inspected without being
reclassified as pre-application refusals.

All stopped stores return, scoped WPQ/recovery pacing and flags restore, and the
PG must become clean with its configured redundancy. Independent known-byte
readback and the live ledger close the data oracle. The receipt retains the
PG/primary, lost holder/spare, held interval/cursor, reached progress and writer
watermarks. Pool facts separately record the native EC profile, overwrite
permission and unchanged size/min_size. This does not prove a physical-host
outage, an additional k/m layout or another release/platform cohort.

Run on an authorized Linux engine with explicit essential/client role images:

```sh
CGO_ENABLED=0 go test -tags=all -count=1 -timeout=25m -v \
  -run '^TestECInterruptedReconstruction$' ./internal/integration
```

The source-owned topology batch is `ec_reconstruction`. `EC_RECONSTRUCTION_PRE_STOP`
retains the last actual unfinished query; `EC_RECONSTRUCTION_RECEIPT` retains the
shared ledger/phase result. The tiny shard-peer, policy and malformed-placement
fixtures are synthetic model controls, separate from native execution.
