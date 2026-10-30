# Classified errors

## Introduction

A fixture call can fail for different reasons, and a test usually needs to
react differently to each:

- the request was wrong, or the state of the fixture forbids it;
- the Ceph release on the target does not have the feature;
- the image does not have a tool that the operation needs, or an optional
  extension was not selected;
- the Docker engine, host or kernel cannot provide what the operation needs;
- the native command ran and Ceph refused it or failed.

Every package of the module reports these cases with five error categories,
which you match with `errors.Is`, and with typed details, which you read with
`errors.As`. For classified failures, match these rather than error text.
This guide explains the categories, the details they carry and how to write
tests that hold on every supported release. Parse/readback failures and
unknown outcomes can still be unclassified; the categories do not cover every
error.

## The categories

| Category | Meaning | Examples |
| --- | --- | --- |
| `ceph.ErrInvalidRequest` | Refused before any change: malformed arguments, or an operation that the state of the cluster or of the selected resource forbids | an empty pool name, a terminated cluster, a container that the cluster neither owns nor has attached, an override that is already held, a resource that was replaced outside the fixture |
| `ceph.ErrUnsupported` | The Ceph of the relevant target lacks the feature | an `aes256k` key on an essential image whose `ceph-authtool` has no `--key-type`, RBD namespace mapping to a Squid cluster |
| `ceph.ErrImageCapability` | The image lacks an executable or runtime, or an optional extension was not selected or does not meet its contract | `RunNativeCommand` on a container without `python3`, a netem helper image without `tc` |
| `ceph.ErrEnvironmentUnavailable` | The engine, host, kernel or container permissions cannot provide what the operation needs; the image may be valid | host networking disabled on Docker Desktop, a helper without `NET_ADMIN`, a kernel without netem, an engine that cannot be reached |
| `ceph.ErrNativeFailure` | A native command ran and failed | `ceph osd pool get missing size` exits with `ENOENT` |

The categories are disjoint for a single failure. An error joined from
several failures, such as a failed change and a failed restoration, can match
several.

`ErrInvalidRequest` is a validation refusal before any change. Documented
feature preflights, such as `WithAuth` key-type validation and `rbdmirror.Run`
namespace mapping checks, also refuse unsupported features before changes.
Extension capability checks run before the fault is applied. These guarantees
belong to the documented operation or preflight; an image or environment
error during a later step is not a promise to roll back the entire fixture.
Always preserve a non-nil partial handle returned with an error.
Once a native command has been sent, a lost reply leaves its effect unknown:
the command may have run, as when Docker answers the final inspection of an
exec after the command finished. Such a failure keeps Docker's error, and is
`ErrEnvironmentUnavailable` for a failure of the engine or matches no
category for a container that disappeared or stopped; neither says that the
command did not run.

The narrower errors of the module belong to a category:
`ceph.ErrHostNetworkUnavailable`, `fault.ErrNetemPermissionDenied` and
`fault.ErrNetemKernelUnsupported` to `ErrEnvironmentUnavailable`, and
`fault.ErrNetemImageUnsupported` to `ErrImageCapability`. Both forms match:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithHostNetwork())
testcontainers.CleanupContainer(t, cluster)
switch {
case errors.Is(err, ceph.ErrHostNetworkUnavailable): // also ErrEnvironmentUnavailable
	t.Skip("host networking is not usable here:", err)
case err != nil:
	t.Fatal(err)
}
```

## Reading the details

`*ceph.OperationError` carries a category in `Kind` and what identifies the
failure:

| Field | Content |
| --- | --- |
| `Op` | The operation, when the cause's text does not already name it |
| `Target` | The container or cluster, such as `control` (the control container), `destination cluster`, `source setup client` or `zone destination` |
| `Version` | The Ceph release observed on `Target`, when it matters |
| `Image` | The immutable ID of `Target`'s image, when the fixture read it |
| `Capability` | The missing feature or tool, such as `ceph-authtool --key-type` or `python3` |
| `Err` | The cause, which `errors.Unwrap` returns |

```golang
_, err := cluster.CreateClient(ctx, "modern", ceph.ClientCaps{Mon: "allow r"},
	ceph.ClientOptions{}.WithKeyType(ceph.CipherAES256K))
var failure *ceph.OperationError
if errors.As(err, &failure) && failure.Kind == ceph.ErrUnsupported {
	t.Logf("%s (Ceph %s) lacks %s", failure.Target, failure.Version, failure.Capability)
}
```

Unset fields were not known or do not apply. The error's text contains the
operation, the cause and the target, version and image, but never command
arguments, raw native output or credentials.

## Native failures

A module command that runs and exits nonzero returns a `*ceph.NativeFailure`:
`cluster.Ceph`, the RBD, RGW and CephFS operations, `rbdmirror.Mirror.SourceRBD`,
`multisite.Fixture.ZoneAdmin` and the other helpers that run Ceph's tools.

| Field | Content |
| --- | --- |
| `Program` | The program, such as `ceph` or `rbd`, without its arguments |
| `ExitCode` | The exit code |
| `Errno` | The error code that the tool printed, such as `ENOENT`, or empty |
| `Reason` | The tool's own explanation, at most `ceph.MaxNativeFailureReason` bytes |

`Errno` comes from what the tool printed: the `Error ENOENT:` that starts a
line of the `ceph` CLI, or the last `(2) No such file or directory` of a line
of `rbd`, `rados` and `radosgw-admin`, which print the errno after the names
of the request. A number is taken only with its own errno text. It is never
guessed from the exit code, because not every tool exits with its errno. `errors.Is` also matches the standard library's
file errors:

| Errno | Also matches |
| --- | --- |
| `ENOENT` | `fs.ErrNotExist` |
| `EACCES`, `EPERM` | `fs.ErrPermission` |
| `EEXIST` | `fs.ErrExist` |

```golang
_, err := cluster.Ceph(ctx, "osd", "pool", "get", "missing", "size")
if !errors.Is(err, fs.ErrNotExist) {
	t.Fatalf("a missing pool returned %v", err)
}
```

`Reason` is the tool's error output without Ceph's log lines, which start
with a timestamp, joined with `; `: the tools often print the cause on one
line and a generic summary on the last, as in `start snapshot 'baseline' does
not exist in the image, aborting; rbd: import-diff failed: (22) Invalid
argument`. Keys and the secrets that `RedactSecrets` recognizes are replaced,
and control characters are removed.
It can name resources of the request, such as a pool. Some operations hide
native output entirely, such as Cephx identity changes, configuration
overrides and RGW administration; their errors keep a `*NativeFailure`
without `Reason`, so `ExitCode`, `Errno` and the category stay available.

`RunNativeCommand` is different: it returns a nonzero exit as
`NativeCommandResult.ExitCode`, not as an error, so that your own oracle
decides what an exit means. Its errors are about running the command:

| Failure | Category |
| --- | --- |
| A malformed `NativeCommand`, a container that the cluster neither owns nor has attached, a stopped or paused container | `ErrInvalidRequest` |
| A container without `python3`, which runs the supervisor, or a program that does not exist or is not executable | `ErrImageCapability` |
| A program that could not be started for want of a process or memory, or because a security policy refused it | `ErrEnvironmentUnavailable` |
| Another failure of Docker | `ErrEnvironmentUnavailable` |
| A container that disappeared or stopped after the command was sent | none: the command may have run |
| A timeout or truncated output (`ErrNativeCommandIncomplete`), a canceled call, a command that could not be confirmed as stopped | none; see below |

## Unsupported features

`ErrUnsupported` is decided before any change, from evidence about the
target that the request needs:

- **The target's own tools**, when they reveal the feature. The control
  image's `ceph-authtool` decides whether `WithAuth` and `CreateClient` can
  generate `aes256k` keys, whatever its release number says: a patched older
  image with `--key-type` works, and a stripped newer one without it is
  refused. For RBD namespace mapping, `rbd` must list `--remote-namespace`
  both in each setup client and in each cluster's client toolkit, whose
  release the cluster's daemons run; a backported Squid image maps
  namespaces.
- **A documented limit of the release** that no tool reveals. Squid RGW
  resolves no source for a user-mode sync pipe of an account principal.

Each side of a link is judged on its own: a mirror between a Tentacle source
and a Squid destination is refused with `Target` `destination cluster` and
`Version` `19.2.5`, and a multisite pipe with the release of the zone that
lacks the feature. The refused request changes nothing on either side.

An absent optional command is not an unsupported release. A command that
Ceph rejects is a `*NativeFailure`, also when the reason is a known bug of a
release: the module never turns a failed request into a version refusal by
assumption.

## Image capabilities and optional extensions

The default features use only what the official `quay.io/ceph/ceph` image
ships. When a module command finds that a program is missing from the image,
Docker cannot start it, and the error is `ErrImageCapability` with the
program in `Capability`.

Optional extensions, such as the netem helper of `ApplyNetworkProfile`,
are selected explicitly and checked before anything changes. Their errors
separate the four cases that matter:

| Case | Category | Narrower error |
| --- | --- | --- |
| The extension is not selected (`NetworkProfile.HelperImage` is empty) | `ErrImageCapability` | |
| The selected image does not meet the extension's contract | `ErrImageCapability` | `ErrNetemImageUnsupported` |
| The tools work, but the engine denies a permission or the kernel lacks a module | `ErrEnvironmentUnavailable` | `ErrNetemPermissionDenied`, `ErrNetemKernelUnsupported` |
| The extension's native command ran and failed | `ErrNativeFailure` | `*NativeFailure` of `tc` |

Nothing installs a missing tool or falls back to another mechanism.

## Errors outside the categories

Some errors keep their own contracts and match no category:

- `context.Canceled` and `context.DeadlineExceeded`: a call that `ctx` ended
  is never classified by what it could not finish. A Docker failure during a
  canceled call stays a context error.
- `ceph.ErrNativeCommandIncomplete`: the command timed out or its output was
  truncated; the partial result is returned.
- `*ceph.NativeCommandUnstoppedError`: a command may still run; call its
  `Stop`.
- `ceph.ErrNativeInputClosed`: the input was closed, or its command ended or
  stopped taking input. A write beyond `MaxInput`, in contrast, matches
  `ceph.ErrInvalidRequest` and leaves the input open. A write ended by its
  context keeps the context error; some bytes may already have been sent.
  See [Standard input](native-commands.md#standard-input).
- Failed checks of the fixture's own, such as native output that it could
  not parse, or a readback that differs from the requested change.
- A failure whose outcome is unknown, such as a container that disappeared
  or stopped after a native command was sent to it. The command may have
  run; inspect the native state before you repeat a mutation.

A non-nil handle returned with an error keeps its meaning whatever the
category: restore it, because the change may have been applied. See
[Contexts, ownership and cleanup](contexts-and-cleanup.md#handles-returned-with-errors).

## Retrying

The module does not replay mutations after an uncertain send or lost reply,
and a classified error does not mean that retrying the same call is safe.
`Ceph` and the fixture's own CLI commands do reconnect after the CLI's
connection timeout before it sends a command, bounded by `ctx` and a minute;
see [The ceph CLI](native-commands.md#the-ceph-cli). That pre-send retry is not
mutation replay. As a rule:

- `ErrInvalidRequest`, `ErrUnsupported` and `ErrImageCapability` do not
  resolve merely by repeating an unchanged request. Correct the arguments,
  restore a blocking override or resource state, or select a capable image.
- `ErrEnvironmentUnavailable` does not change until the environment does.
- `ErrNativeFailure` depends on the native status. `EAGAIN` and `EBUSY`
  often pass, while `ENOENT` and `EINVAL` do not. Retry only an idempotent
  request, and only when `Errno` says that the state can change.

A state that is not ready yet is not an error: the `Wait` methods keep
observing it until their condition holds or `ctx` ends, and then return an
error that matches the context's error. A condition that cannot resolve
itself, such as a resource replaced outside the fixture, ends a wait at once
where the method's page says so.

## Tests across releases

Assert the classified outcome instead of branching on the version string.
The same test then holds on Squid, on Tentacle and on patched images:

```golang
_, err := cluster.CreateClient(ctx, "modern", ceph.ClientCaps{Mon: "allow r"},
	ceph.ClientOptions{}.WithKeyType(ceph.CipherAES256K))
var unsupported *ceph.OperationError
switch {
case errors.As(err, &unsupported) && unsupported.Kind == ceph.ErrUnsupported:
	// The essential image cannot generate the key; nothing was created.
	if unsupported.Capability != "ceph-authtool --key-type" {
		t.Fatalf("unexpected refusal: %v", err)
	}
case err != nil:
	t.Fatal(err)
default:
	// Check the key type from the daemon side.
}
```

An unsupported combination is a result to assert, not a reason to skip: the
refusal before any change is the contract on that release.

## Related APIs

- [`ceph`](#the-categories): the categories,
  `OperationError`, `NativeFailure` and `MaxNativeFailureReason`.
- [Running native commands](native-commands.md): `Ceph`, `RunNativeCommand`
  and reading native output.
- [Releases and images](releases-and-images.md): release differences and
  image requirements.
- [Fault injection](fault-injection.md#delaying-packets-below-ceph): the netem
  extension.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): cancellation and
  handles returned with errors.
