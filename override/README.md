# Reversible overrides

Import `github.com/JSYoo5B/ceph-testcontainers-go/override` alongside `ceph` to
put a running cluster into a policy or admission state that a test needs for a
while, and to put it back: fence one client session, hold an OSD flag, enable
or disable an MGR module, lower the full ratios, raise one OSD's debug level,
set a central configuration entry or change the messenger policy. Overrides
build test conditions; they are not a cluster administration tool. See
[API purpose and purpose tags](../docs/guides/api-purpose.md).

## Order of use

1. **Start a cluster** with `ceph.Run`.
2. **Apply the override** the scenario needs:

   | Condition | Function | Handle |
   | --- | --- | --- |
   | Fence one client session | `AddBlocklist` | `BlocklistHandle` |
   | Hold a global OSD flag (`noout`, `nodown`, ...) | `SetOSDFlag` | `OSDFlagHandle` |
   | Enable or disable an MGR module | `SetMGRModule` | `MGRModuleHandle` |
   | Make the cluster (near)full with little data | `SetFullRatios` | `FullRatiosHandle` |
   | Raise one OSD's runtime debug level | `SetDebugLevel` | `DebugLevelHandle` |
   | Set one central configuration entry | `SetConfig` | `ConfigHandle` |
   | Change accepted client connection modes | `SetMessengerPolicy` | `MessengerPolicyHandle` |

3. **Run the SUT** under the condition and check its behavior.
4. **Observe** the cluster side with the read-only APIs that stay in `ceph`
   (`Observer.BlocklistEntries`, `Observer.OSDFlags`, `Observer.MGRModules`,
   `Observer.FullRatios`, `Observer.Configuration`, `Observer.HealthDetails`).
5. **Restore** with the handle when the scenario needs the cluster back.

```golang
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
// hold is never nil: after a refusal its Restore does nothing.
defer hold.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
// Stop an OSD and check the SUT while data is not rebalanced away.
```

## Concepts and limits

- Every function returns a non-nil handle, so it can be deferred before the
  error is checked. A refused call returns a handle that owns nothing; an
  uncertain one (the change may have been applied) returns a real handle
  whose `Restore` still undoes it. `Restore` undoes only what the call did, is
  shared by copies of the handle and is retryable with a fresh context.
- `Terminate` does not undo overrides: the change ends with the disposable
  cluster. There is no expiry or scope-exit restoration.
- One override per target: a second handle for the same session, flag,
  module, threshold set, daemon option or configuration key is refused.
- Ceph offers no compare-and-swap for these settings: other writers must not
  race an override and its restoration.
- Overrides verify the stored state, not its effect: a configuration entry can
  be shadowed by local configuration, some options apply only at daemon start,
  and a full-ratio change takes effect only when the OSDs report it.
- Debug levels are a narrow diagnostic window for one OSD and option around one
  operation, not a logging default; restore them with a context of their own
  and register them with `finalize.Finalization.TrackDebugLevel`.

## Reference

The full contract of every function and handle, including each refusal and
uncertain outcome, is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/override)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/override`. Scenarios
that combine overrides with faults are in
[Fault injection](../docs/guides/fault-injection.md); failure archives in
[finalize](../finalize/README.md).
