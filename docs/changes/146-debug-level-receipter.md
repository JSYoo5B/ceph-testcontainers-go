---
change: 146-debug-level-receipter
kind: feature
---
`ceph.Finalization.TrackDebugLevel` now takes the new interface `ceph.DebugLevelReceipter` (`Receipt() DebugLevelReceipt`) instead of `*ceph.DebugLevelOverrideHandle`; the handle implements it, so existing calls compile unchanged. A nil interface, a typed nil pointer, a zero-value handle and any receipter whose receipt has an empty state are recorded as absent (`present: false`), decided by the receipt and not by a nil comparison. `ceph.DebugLevelReceipt` gains `Reason`, and `ceph.DebugLevelNotApplied` is a new state for a window that was refused before any native step; the failure manifest records such a receipt as present, with its `reason` redacted and bounded to 2048 bytes. 0.x: old names are removed without aliases. `OverrideDebugLevel` behavior is unchanged.
