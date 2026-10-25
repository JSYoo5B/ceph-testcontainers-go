# A retained reopen canceled after its container was created

The optional `retained_cancel_after_admission` batch in `./ceph` qualifies one
bounded cancellation boundary of `ReopenOSD`: the caller's context ends after
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

- `ReopenOSD` returns an error, and the new descriptor owns the created
  container. The detached handle is consumed, and a second `ReopenOSD` refuses
  with no further runner dispatch.
- Owner-filtered Docker create events bounded by daemon timestamps show exactly
  one creation, and independent inspection shows that container running.
- A fresh context settles the candidate through the ordinary lifecycle:
  `DetachOSD` stops and removes the canceled candidate, and a second `ReopenOSD`
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
