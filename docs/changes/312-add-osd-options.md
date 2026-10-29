---
change: 312-add-osd-options
kind: api-break
migration: docs/guides/upgrading-osd-options.md
---
`Container.AddOSD` takes `...ceph.OSDOptions` instead of `...DaemonOption`, and
`Container.AddOSDWithConfig` is removed. `OSDOptions{}.WithPlacement(config)`
selects the logical CRUSH location and `OSDOptions{}.WithImage(image)` starts
the one OSD from another image. Several values compose; a field set by two values
is refused before the topology lock and before any registration. `WithDaemonImage`
remains the option of `AddMonitor` and `AddManager`. There is no deprecated
alias or forwarding layer.
