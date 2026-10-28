---
change: 146-extension-store-component
kind: none
reason: internal move of the per-cluster service state map and its lock (the former extensionsMu) from Container fields into extensionStore, and removal of the NetworkRemoved pointer from the unit-test state view in favor of two hooks; no new lock, no lock-order change, no exported API or behavior change
---
