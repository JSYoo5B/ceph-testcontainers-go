# Upgrading: AddOSD configuration

`Container.AddOSD` takes typed `ceph.OSDOptions` values. `AddOSDWithConfig` is
removed without an alias, and an OSD image is selected with `OSDOptions.WithImage`
instead of `WithDaemonImage`. `AddMonitor` and `AddManager` keep `WithDaemonImage`.

| Before | After |
| --- | --- |
| `cluster.AddOSD(ctx)` | unchanged |
| `cluster.AddOSDWithConfig(ctx, cfg)` | `cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(cfg))` |
| `cluster.AddOSD(ctx, ceph.WithDaemonImage(img))` | `cluster.AddOSD(ctx, ceph.OSDOptions{}.WithImage(img))` |
| `cluster.AddOSDWithConfig(ctx, cfg, ceph.WithDaemonImage(img))` | `cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(cfg).WithImage(img))` |

Several values may be passed (`AddOSD(ctx, placementOptions, imageOptions)`).
Each field may be set by one value only: placement or image set in two values is
refused with `ErrInvalidRequest`, also when both agree, before the topology lock
and before any OSD is registered. An empty image is an explicit invalid
selection. An explicit `WithPlacement(ceph.OSDConfig{})` requests the default
placement. See [composition rules](../design/value-configuration-composition.md).

Entry-point count: `AddOSD` and `AddOSDWithConfig` become one `AddOSD`; the
settings are discoverable under `OSDOptions` (`WithPlacement`, `WithImage`).
