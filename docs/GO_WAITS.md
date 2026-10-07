# Wait with context and select

The synchronous `Wait(ctx, ...)` APIs work with caller-owned goroutines. A
buffered channel can carry one terminal result to one consumer; it is neither a
broadcast nor a progress stream. No library Async/Watch or callback API is needed.

For an existing owned `*multicluster.RBDMirrorNamespace` named `view`, retain
both its partial status and error:

```go
type replayResult struct {
    Status multicluster.RBDMirrorImageStatus
    Err    error
}

waitCtx, cancel := context.WithTimeout(parent, 30*time.Second)
resultCh := make(chan replayResult, 1)
var results <-chan replayResult = resultCh
joined := make(chan struct{})
defer func() { cancel(); <-joined }() // Register before launching the worker.
go func() {
    defer close(joined)
    status, err := view.WaitReplayReady(waitCtx, "volume")
    resultCh <- replayResult{Status: status, Err: err}
    close(resultCh)
}()

var result replayResult
var open bool
select {
case result, open = <-results:
case <-waitCtx.Done():
    cancel()
    <-joined
    result, open = <-results
}
cancel()
<-joined
if !open {
    panic("wait closed without its terminal result")
}
if _, open := <-results; open {
    panic("wait sent more than one terminal result")
}
// Inspect result.Status and result.Err, then run any caller callback.
// Fixture cleanup follows with its own bounded context.
```

The producer sends once and closes the channel. Capacity one lets it publish the
actual result even if the caller first selects cancellation. The closed
two-value receive distinguishes channel completion from a zero-valued result.
Keep canonical cancellation/deadline causes through `errors.Is`; a useful partial
status may accompany the error. An error-only API such as `fs.WaitReady` uses
`chan error` with the same lifecycle.

Cancel signals that work should stop; join confirms the worker has returned.
On cancellation, this example joins before receiving the buffered result. Both
must complete before a callback or fixture cleanup. For tests,
register the worker's cancel/join cleanup before launch and after fixture cleanup
registration, so LIFO teardown stops the worker first. Use the test goroutine to
report failures. [Go context](https://pkg.go.dev/context)

If result and cancellation are both ready, `select` may choose either. Preserve
the method's returned result; a cleanup `cancel()` making `ctx.Err()` non-nil
does not reclassify an already completed success. [Go select specification](https://go.dev/ref/spec#Select_statements)

A callback belongs in caller code after join, outside fixture/member gates. A
subsequent `fs.MDSStatus(parent)` should use the still-live parent context, not
the canceled wait context. `context.AfterFunc` runs on cancellation and does not
join an already started callback; it is not a completion hook for cluster
cleanup. [Go context.AfterFunc](https://pkg.go.dev/context#AfterFunc)

Launching a goroutine is not a witness-admission barrier. Scoped RBD waits pin
the cohort on their first admitted read; Add/Remove/replacement requires a new
wait. Ready still does not prove particular bytes or snapshot completion. A
channel wrapper does not make fixture methods universally concurrent: current
RGW waits retain the topology gate, so avoid blocking in same-fixture
context-free inventory access before reaching cancellation.

[The executable external Example](../ceph/wait_example_test.go) exercises the
real `WaitReady` API's unavailable-descriptor error and one-shot lifecycle without
Docker. It demonstrates error transport, not a usable or ready filesystem.
[The existing cold-MDS native fixture](../internal/integration/no_initial_mds_integration_test.go)
uses an actual owned filesystem for a completed deadline and later readiness,
then re-enters `MDSStatus` from the consumer before its existing byte/identity/
health/cleanup assertions. Its live phase also directly checks quorum, PG clean,
and the supported `rbd_support`/`volumes` module readiness commands under the
same operation context. These remain distinct readiness predicates.

## Verified execution

At the current 266 runtime inputs / 252 Go files (source manifest
`3d6dfbc3b2727f499dcd5a6b972b777183a415b4609aa919b7b75247e0ef0eef`),
`make check` passed unit tests, full-package race tests, vet and the eight-tag
compile. The external Example executed successfully without Docker. New
regressions reproduce the original late-success and queued-admission failures;
the fixes retain useful partial reports and canonical cancellation causes.

`make scenario-mds-bootstrap` passed on the pinned original Quay Ceph 20.2.4
image on Linux ARM64, with separate bridge and host cases (198.468 seconds for
the package). Each case received one deadline and one successful channel result,
confirmed channel closure and worker join, and re-entered `MDSStatus` from the
consumer. Both cases directly passed quorum, PG-clean and the supported
`rbd_support`/`volumes` module waits. The original 10 full-reader records across
four independent 128 KiB datasets, 14 filesystem-map checks, six strict health
and six module checks remained in place. The run's own cleanup check found no
new containers or networks.

Raw logs, source manifests and actual terminal receipts are under
`artifacts/go-wait-context-20261008/` (locally generated and Git-ignored). This is
one current native profile, not a new result for the full CI matrix, other
images/platforms or every multicluster wait. The fixed image requirements did
not change.
