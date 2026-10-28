---
change: 146-bootstrap-record-component
kind: none
reason: internal move of the bootstrap configuration, admin keyring and Ceph version, with their lock (the former configMu), from Container fields into bootstrapRecord, and removal of the Config, Keyring and CephVersion pointers from the unit-test state view in favor of setters; no new lock, no lock-order change, no exported API or behavior change
---
