---
change: 313-admission-lifetime
kind: none
reason: internal move of the shared setupMu, topology-mutex, timeout and release lifetime of the three begin*Operation flows into one function; each flow keeps its own predicate and refusal text, no new lock, no lock-order change, no exported API or behavior change
---
