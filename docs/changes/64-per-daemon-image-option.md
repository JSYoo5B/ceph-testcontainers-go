---
change: 64-per-daemon-image-option
kind: feature
---
`AddMonitor`, `AddManager`, `AddOSD` and `AddOSDWithConfig` take an optional `ceph.WithDaemonImage(image)` that starts that one new daemon from another image; the default stays the cluster's essential image. `MonitorContainer`, `ManagerContainer`, `OSDContainer` and `DetachedOSD` report the image with `Image()`, an OSD's `DetachedOSD.Reopen` starts from the same image again, and the daemon handles have a `Version(ctx)` that reads the release the running daemon reports through its admin socket. `EssentialImage`, `Images`, `ClusterFacts` and `CephVersion` keep describing the cluster only, and the client release check is unchanged. Existing calls compile unchanged; code that declares an interface with the old `AddMonitor`, `AddManager`, `AddOSD` or `AddOSDWithConfig` signatures must add the variadic `...ceph.DaemonOption`. This is not a mixed-version or upgrade support claim.
