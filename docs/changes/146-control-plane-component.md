---
change: 146-control-plane-component
kind: none
reason: internal move of the control-plane container handle and its lock (the former controlMu) from Container fields into controlPlaneState, with no new lock and no lock-order change; no exported API or behavior changes
---
