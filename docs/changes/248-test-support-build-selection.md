---
change: 248-test-support-build-selection
kind: none
reason: unit-test-only code (synthetic cluster constructor, state accessors, hook methods, shared fakes) now compiles only with the test_support build tag and test commands select it; no exported API or runtime behavior changes
---
