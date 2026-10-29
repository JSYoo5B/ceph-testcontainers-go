# Filesystem identity (F1) and the unlocked name read (F5)

Issue #313. Source reading and Docker-free unit tests only; nothing here is
native evidence.

## Premise

A filesystem handle has exactly one identity, fixed when the handle is created.
Which native filesystem an operation targets must not depend on a field a caller
can edit or on a struct another goroutine replaces.

## Decision (F1)

`Filesystem.name` is an unexported string captured from the validated `Config`
in `startFilesystem` before the handle is published. It is never written again,
so it needs no lock. Every registry key, ownership predicate, native command
target and daemon environment reads it. The exported `FilesystemName` stays as a
descriptive copy; editing it changes no operation's target.

Rationale: the code shows no reason for two sources. The previous split (the
`begin*` flows used `config.Name`, `ScaleMDS`, `MDSStatus`, `WaitReady` and the
session readers used the exported copy) was not a designed difference. Removing
the exported field is a public-handle change and is not part of this slice.

Retained on purpose: the four descriptor-integrity refusals that compare the
exported copy with the identity (`beginSubvolumeOperation`,
`beginSessionOperation`, `checkStoppedMDSOwner`, `scaleColdMDS`). They are
unchanged, and an AST guard fixes the list.

## Behavior change

After the exported `FilesystemName` is edited to another name:

| Operation | Before | After |
| --- | --- | --- |
| `MDSStatus`, `WaitReady` | parsed the sibling's FSMap entry (daemon reported unowned) | report the created filesystem |
| `ScaleMDS`, removal, MDS start | refused only because the registry key no longer matched | target the created filesystem |

The refusals listed above and the data-pool admission are unchanged.

## Decision (F5)

The snapshot-schedule methods read the name without `setupMu` or the cluster
mutex while `ScaleMDS` assigns the whole `config` struct. They now read
`fs.name`, and no code reads the name through `config`. Other `config` fields
keep their documented lock contract.

## Controls (`cephfs/filesystem_name_test.go`, `cephfs/admission_test.go`)

- `MDSStatus`, `WaitReady` and `ScaleMDS` ownership with an edited descriptor
  (each fails against the previous sources).
- A `-race` overlap of the schedule status read with a `config` replacement
  under the cluster mutex (reports a data race against the previous sources).
- An AST guard: no `.config.Name` reads, and an exact list of exported-copy
  readers.

## Not covered

No native run. The relocation of operations onto the handles they act on, the
embedded `Container`, F6 and the remaining admission sites are unchanged.
