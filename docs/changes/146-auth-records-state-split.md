---
change: 146-auth-records-state-split
kind: none
reason: internal move of the auth records (fences, client keys and secrets, stray inputs, revocations) into a named state struct guarded by the existing cluster mutex, with caller-lock guards; no exported API or behavior changes
---
