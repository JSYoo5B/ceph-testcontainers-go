---
change: 146-osd-registry-state-split
kind: none
reason: internal move of the OSD handle registry (the owned and detached OSD maps) into a named state struct, and replacement of an unexported test-hook pointer to the owned map by two narrow internal test functions; no exported API or behavior changes
---
