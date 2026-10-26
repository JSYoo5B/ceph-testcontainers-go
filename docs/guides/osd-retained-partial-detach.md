# Settling a retained OSD detach after container removal

`OSDContainer.Detach` can remove the old container and then fail to admit the retained
store. The OSD descriptor remains owned and marked as detaching; this is not a
published `DetachedOSD`. `OSDContainer.Remove` refuses that descriptor. Resolve the known
store-admission failure, then call `OSDContainer.Detach` again with a fresh context, or
terminate the cluster. A retry after confirmed removal reads the store and
registration without stopping or removing that instance again.

The `topology/osd_retained_partial_detach` native batch qualifies this specific
boundary on a cluster-owned ordinary volume. An isolated, caller-owned observer
atomically moves the regular `ready` marker to another name on that same volume.
This is a metadata-admission fault: it does not change BlueStore data, daemon
identity files, keys, devices or Docker responses. The bootstrap must never run
while the marker is absent; the test restores it before attempting reopen.

A bounded restoration cleanup is registered before dispatching the marker move,
so an early assertion failure or lost observer reply settles the marker before
the observer and cluster are removed. Marker commands use the existing process
supervisor; an unconfirmed command must settle before restoration is attempted.
A failed settlement or restoration is reported as cleanup failure, never as a
successful restored store.

The first detach must return no detached handle and a missing-regular-ready
error **after** the old Docker instance has been removed. The original descriptor
must remain owned, the MON registration must stay down/in with the same UUID,
and purge must be refused. A transparent forwarding recorder counts the actual
`Stop` and `Terminate` calls made through that descriptor; it changes no runtime
argument, response, error or deadline. Owner-filtered Docker stop/destroy/create
events provide a separate observation of real container transitions.

After the observer restores the original marker, its inode, mode, size,
modification time and SHA-256 must match. Retrying detach must then publish the
same registration without another `Stop` or `Terminate` call, or any lifecycle
or creation event. Reopen must create exactly one different container on the
same volume, preserving the OSD UUID and cluster FSID. A single-replica, one-PG
pool has no backfill source: all 1,048,583 known bytes must still match. Finally,
the replacement instance and owned volume must be gone after termination.

The forwarding and marker-operation fixtures run without Docker. They establish
that recording preserves runtime calls and that the marker move preserves its
identity; they are separate from native execution. Each native claim needs its
own source, image/platform and cleanup receipts.

This scenario does not inject a failed Docker removal or an uncertain create
reply. It does not qualify cancellation after admission, a lost reply, failed
volume release, abrupt exit or durability. Those remain separate failure
boundaries in the [retained lifecycle design](../design/osd-retained-reopen.md).
