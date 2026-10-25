# A retained reopen whose completed runner reply is withheld

The optional `retained_uncertain_reply` batch in `./ceph` qualifies one bounded
uncertain-reopen outcome. It uses the existing private `osdRunner` test seam in
the same package, without changing a public API or production implementation.
The hook forwards the real image and customizers to the ordinary runner. Only
when that runner returns a real ready container successfully does the hook
retain an observation handle and return `nil` with an injected client-side error.
Actual runtime failures pass through unchanged.

This is client-side injected uncertainty **after real creation and readiness**.
It is not an observed Docker network failure or raw create-RPC reply loss. It
cannot establish behavior when a request creates a container after a cleanup
scan, or when native startup never settles.

The caller must receive an owned new OSD descriptor with no embedded container.
The detached handle is consumed. Repeated reopen and detach must refuse without
another runner dispatch. The old removed Docker instance keeps its native fence.
Never call container methods on the new descriptor's nil embedded container.
The unknown replacement ID is not asserted to be fenced: native-command admission
also intentionally accepts caller-owned containers attached to the cluster.

The independent Docker inspection and owner-filtered creation events must show
one different running container, the retained owned volume and the cluster's
ownership label. Healthy control/client paths verify the original OSD UUID and
FSID, up/in registration and all 1,048,583 known bytes in a single-replica pool.
No second replica can supply those bytes through backfill.

Normal module `Terminate` must then find and remove the withheld instance using
its owner-label reconciliation and release the retained volume. The test never
calls `Terminate` on the observation handle on its success path. Raw Docker
absence checks and an independent run-resource cleanup audit establish teardown;
a manual emergency cleanup cannot turn a failed qualification into a pass.

The forwarding fixture is Docker-free and separate from this native result.
Because this batch targets `./ceph`, its source-owned profile also selects that
package's ordinary unit tests. Compilation, those unit outcomes, the native
scenario and cleanup are distinct evidence. The batch requires explicit opt-in
and is excluded from `all` and ordinary required runtime profiles.
