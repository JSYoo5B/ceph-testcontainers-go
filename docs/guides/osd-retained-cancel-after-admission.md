# A retained reopen canceled after its container was created

The optional `retained_cancel_after_admission` batch in `./ceph` qualifies one
bounded cancellation boundary of `DetachedOSD.Reopen`: the caller's context ends after
the creation request was dispatched and the runner returned a ready container.
It uses the existing private `osdRunner` test seam in the same package and
changes no public API or production code.

The hook forwards the real image and customizers to the ordinary runner. Only
when that runner returns a container without an error does the hook cancel the
caller's context and return the unchanged result. Actual runtime failures and
nil results pass through without a cancellation. This is a source-injected
cancellation at a chosen point, not a signal delivered at an arbitrary moment,
and not a cancellation during container creation or readiness.

Expected outcome of the canceled call:

- `DetachedOSD.Reopen` returns an error, and the new descriptor owns the created
  container. The detached handle is consumed, and a second `DetachedOSD.Reopen` refuses
  with no further runner dispatch.
- Owner-filtered Docker create events bounded by daemon timestamps show exactly
  one creation, and independent inspection shows that container running.
- The daemon keeps booting after the cancellation: a fresh context waits for
  it to be up/in. Detach only a booted candidate: a still-booting daemon can be
  killed after the 10 s stop grace period before it marks itself down, and in a
  one-OSD cluster no peer reports it, so `OSDContainer.Detach` can wait out its
  3 minute budget.
- A fresh context then settles the candidate through the ordinary lifecycle:
  `OSDContainer.Detach` stops and removes the canceled candidate, and a second `DetachedOSD.Reopen`
  creates a distinct container with the same ID, registration UUID and cluster
  FSID.
- An independent client reads all 1,048,583 known bytes from a single-replica,
  one-PG pool, so no other replica can supply them through backfill.
- `Terminate` removes the final container and the owned volume. Raw Docker
  absence checks establish teardown.

Not covered: cancellation during container creation, during readiness or at
any other point of detach, an unsettled or lost creation reply (see the
separate uncertain-reply guide), failed volume release or failed container
removal, other releases, images and platforms, abrupt exit and durability. The
forwarding fixture and the Docker-free reopen controls are separate from the
native result. The batch requires explicit opt-in and is excluded from `all`
and from the ordinary required runtime profiles.
