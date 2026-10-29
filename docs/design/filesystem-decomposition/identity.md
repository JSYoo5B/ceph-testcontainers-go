# Filesystem identity

A filesystem handle has exactly one identity, fixed when the handle is created.
Which native filesystem an operation targets must not depend on a field a caller
can edit or on a struct another goroutine replaces.

## Invariant

`Filesystem.name` is an unexported string captured from the validated `Config`
in `startFilesystem` before the handle is published. It is never written again,
so it needs no lock. Every registry key, ownership predicate, native command
target and daemon environment reads it. No code reads the name through
`config`.

The exported `FilesystemName` is a descriptive copy. Editing it changes no
operation's target.

## Edited descriptor

Four admission paths still compare the exported copy with the identity and
refuse an edited descriptor: `beginSubvolumeOperation`,
`beginSessionOperation`, `checkStoppedMDSOwner` and `scaleColdMDS`. An AST guard
fixes this list. Every other reader uses `name`.

## Snapshot-schedule readers

The snapshot-schedule methods run without `setupMu` or the cluster mutex while
`ScaleMDS` assigns the whole `config` struct. They must read `name`, never
`config.Name`, or the read races with the replacement. Other `config` fields
keep their documented lock contract.

## Not covered

Nothing here is native evidence. Moving operations onto the handles they act on,
the embedded `Container`, and the remaining admission sites are separate work.
