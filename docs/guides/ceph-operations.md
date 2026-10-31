# Ceph cluster operations

Concepts that span several `ceph` APIs and have no single godoc home. Each API's
own contract is in its godoc (`go doc github.com/JSYoo5B/ceph-testcontainers-go/ceph`,
or [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph));
the [package README](../../ceph/README.md) gives the usual order of use, and
[Contexts and cleanup](contexts-and-cleanup.md) the shared rules for contexts,
partial results and restoration handles.

## Removing, detaching and reopening an OSD

Three handle methods take an OSD out of service, with different outcomes:

| Call | Container | Store and registration | Next step |
| --- | --- | --- | --- |
| `Stop` / `Start` on the `OSDContainer` | stopped, kept | kept; Ceph marks the OSD down, then out unless `noout` is held | `Start` the same container |
| `OSDContainer.Detach` (only with `OSDStorage.WithVolume`) | removed | store kept in the owned volume, registration kept, OSD down but not out | `DetachedOSD.Reopen` starts a new container on the store and returns a new handle |
| `OSDContainer.Remove` | removed | drained, purged from Ceph, directory discarded | none; `AddOSD` creates a new OSD |

All three fence or keep the OSD's identity: every step checks the registration
UUID, `Remove` and `Detach` fence the Docker instance against new native
commands and drain its tracked ones, and `Remove` refuses an OSD that is being
detached or is detached. After `Detach` and `Reopen` the old handle is stale;
use the handle `Reopen` returns. `Reopen` starts the OSD from the image its
launch recipe recorded, or from the image of `OSDOptions.WithImage`; it refuses
`WithPlacement`. See [OSD storage](../internals/osd-storage.md) for the store
checks.

## Replacing a MON on its store

With `WithMonitorStorage(MonitorStorage{}.WithVolume())` every MON, the first
one included, keeps its store in a Docker volume of its own that the cluster
owns. `MonitorContainer.Replace` then moves one MON to a new container:

| Step | What happens |
| --- | --- |
| Refusals, before the MON is touched | another majority of the monmap is not in quorum; the store is not attached to this handle's container; the store has no recorded address |
| Old container | removed; nothing may still mount the volume |
| New container | from the handle's image, or from `DaemonOptions.WithImage`; same volume, same address (on the host network, the same ports), entrypoint in a mode that refuses to format a missing store |
| Membership | unchanged: same name, rank and monmap entry; `Replace` waits for the MON to rejoin the quorum |

The old handle is stale afterwards; use the one `Replace` returns. Replacing
MON a removes the cluster's embedded container, as `Remove` on MON a does, and
fixture commands then use the control container. The volume needs IPv4
addresses: `Run` refuses it with another address family. On the host network
the MON's address includes its ports. `Replace` holds them from the removal of
the old container until right before the new one starts; a process that takes
one of them in that window makes the new MON fail to start, and the store stays
retained for another `Replace` on the returned handle.

A `Replace` that fails after the old container is gone returns the new handle
with the error. The MON is then down, its store is neither formatted nor
released, and `Replace` on that handle, for example with another image,
continues the step. `MonitorContainer.Remove` releases the removed MON's
volume, so a MON added again under that name starts on a new store.
`Terminate` removes the volumes after the MON containers. See [MON data volumes](../internals/mon-volume-storage.md) for the
store records and their release order.

## Cephx key changes

`ClientConfig.ReplaceKey`, `ClientConfig.UpdateCaps`, `ClientConfig.Delete`,
`ClientConfig.Revoke`, `OSDContainer.RotateKey` and `ManagerContainer.RotateKey`
share one failure model: a call does not report an outcome while one of its MON
commands may still apply, because it settles the MONs first. The sections below
describe that model and how clients behave on the official images.

### ClientConfig.ReplaceKey

Gives an identity created by this cluster a fresh key and keeps its name and
its exact capabilities. The change is one `ceph auth import` of the identity's
own keyring with the new key, so the identity never disappears from the MONs.
The new key has the type of the old one; `ceph.ClientOptions.WithKeyType` selects
another (see [CreateClient](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#CreateClient)). The identity needs at least one
capability, because Ceph refuses to import an entry without any; an identity
without capabilities is refused before any change.

```golang
renewed, err := client.ReplaceKey(ctx)
```

The call returns a new descriptor for the new key and leaves the old one
stale:

- The old descriptor's `ConnectionConfig` still returns the old credentials,
  so a test can check that they are rejected.
- Every other operation with the old descriptor fails:
  [ClientConfig.WithIdentity](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.WithIdentity),
  [ClientConfig.Capabilities](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.Capabilities),
  [ClientConfig.UpdateCaps](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.UpdateCaps) and [ClientConfig.Delete](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.Delete).
  Use the returned descriptor for them.
- `ClientConfig.ReplaceKey` with the old descriptor again returns the same new
  descriptor without another change, as long as that descriptor is still the
  identity's current one.
- Containers attached with the old descriptor keep the old keyring.
- Copies of the old descriptor go stale as well. The cluster records the
  current key of each identity it created, and every operation refuses a
  descriptor with another key or of a deleted identity.

The old key behaves as follows on the official images:

- A fresh connection with the old key fails at once with `EACCES`.
- A client that authenticated before keeps its session until it renews its
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#WithAuth)
  `MonTicketTTL`). The MONs then reject the renewal (`_finish_auth -13` in its
  `debug_monc` log).
- What follows the rejection differs. On Tentacle 20.2.4 the client loses its
  MON session, and librados does not authenticate again on its own. On Squid
  19.2.5 the MON session that is already open keeps answering, and only the
  renewals keep failing.
- MGR and OSD sessions that the client already holds keep working in both.
- Fresh MON, MGR and OSD clients with the new key work at once, also when it
  has another key type than the old one.

The identity must still have the descriptor's key. An identity whose key was
changed outside the module is refused before any change, and external auth
changes must not race the call.

The import is fenced like the MON commands of
[RotateKey](#rotatekey): the command runs under the supervisor,
and after it the call settles the MONs and reads the outcome. So a failed or
canceled call reports what the MONs hold once nothing can change it any more:
either the new key, in which case the call returns the new descriptor without
an error, or the old one. When settling itself fails, the outcome stays open:

- The call returns a descriptor of the attempted key that stays unusable,
  with an error saying so. A returned descriptor never changes afterwards.
- The next operation on the identity settles first and then resolves the
  outcome. `ClientConfig.ReplaceKey` with the old descriptor returns a usable
  descriptor of the attempted key once the import turns out to have applied,
  and replaces the key anew once it turns out not to have.
- Until then, [ClientConfig.WithIdentity](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.WithIdentity) refuses both
  descriptors.

[ClientConfig.UpdateCaps](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.UpdateCaps) and [ClientConfig.Delete](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.Delete) are
fenced the same way, so none of these operations races a command of another
one that may still apply. The CephFS subvolume grants of the
[cephfs](../../cephfs/README.md) package settle these commands before they change a
client. Their own commands go through the MGR, though, which this settling
cannot fence, and the module cannot tell when a failed one has ended: the
volumes module may still create or change the identity later. Do not replace
or revoke a client after a failed `Subvolume.Authorize` or
`SubvolumeAuthorization.Deauthorize` for it. A `SubvolumeAuthorizedClients` listing shows
only the state at that moment.

### ClientConfig.Revoke

Removes an identity created by this cluster from the MONs until `Restore`
puts back exactly its key and its capabilities, so a recovery test can watch
the rejection of a client and then its return with the same credentials. The
identity keeps its name and key type, and the descriptor is not recreated.

```golang
revocation, err := client.Revoke(ctx)
// ... check that the client is rejected ...
err = revocation.Restore(ctx)
```

While the identity is revoked:

- The descriptor's `ConnectionConfig` keeps returning its credentials, which
  work again after `Restore`.
- [ClientConfig.WithIdentity](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.WithIdentity),
  [ClientConfig.Capabilities](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.Capabilities),
  [ClientConfig.UpdateCaps](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.UpdateCaps), [ClientConfig.ReplaceKey](#clientconfigreplacekey)
  and another `ClientConfig.Revoke` refuse the descriptor.
- [CreateClient](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#CreateClient) refuses the name.
- [ClientConfig.Delete](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ClientConfig.Delete) makes the removal permanent, after which
  `Restore` fails.

The client behaves as follows on the official images:

- A fresh connection fails at once with `EACCES`.
- A client that authenticated before keeps its sessions until it renews its
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#WithAuth)
  `MonTicketTTL`). The MONs then reject the renewal, and the client keeps the
  MGR and OSD sessions it already holds.
- On Tentacle 20.2.4 the client also loses its MON session, and librados does
  not authenticate again on its own. On Squid 19.2.5 the open MON session
  keeps answering.
- After `Restore`, an open session renews its tickets only while the MONs
  still keep the rotating secret of its last auth ticket. They replace that
  secret every `MonTicketTTL` and keep a few generations. After a longer
  revocation, the MONs refuse every renewal of that session and log
  `attempt to reclaim global_id ... using bad ticket`, because the cluster
  sets `auth_allow_insecure_global_id_reclaim = false`. A new connection with
  the same credentials authenticates once `Restore` returns, so a recovery
  test reconnects its client instead of waiting for a renewal.

`Restore` checks what the MONs hold first:

- An identity that already has the original key and capabilities counts as
  restored, so a repeated `Restore` succeeds.
- An identity that was created again with another key or other capabilities
  is refused and never overwritten.
- A `Restore` of an earlier revocation does not undo a later one.

The revocation belongs to the identity, not to one descriptor: copies of the
descriptor are refused the same way, and `ClientConfig.Delete` through any
descriptor of the revoked key makes the removal permanent.

The removal and the restoration are fenced like the import of
[ClientConfig.ReplaceKey](#clientconfigreplacekey). A failed or canceled call settles the
MONs and reports what they hold:

- A removal that did not apply returns no handle, and the descriptor stays
  usable.
- When the outcome stays uncertain, `ClientConfig.Revoke` returns the
  handle with the error, and `Restore` settles first and then restores.

The cluster needs no `Restore` before `Terminate`. The handle's `String` and
`GoString` never include credentials.

### RotateKey

Replaces the Cephx key of a running MGR or OSD that the cluster owns, without a
restart, the way cephadm rotates daemon keys. It is a method of
`*ceph.OSDContainer` and `*ceph.ManagerContainer`; `*ceph.MonitorContainer` has
none, so rotating a MON key does not compile.

1. `ceph auth get-or-create-pending` creates the new key on the MONs.
2. The daemon's keyring files in its container get the new key, so a later
   restart authenticates with it.
3. `ceph tell <daemon> rotate-key` hands the key to the running daemon.
4. `ceph auth commit-pending` makes it the entity's only key.

```golang
err := osd.RotateKey(ctx) // osd is the *ceph.OSDContainer of osd.0
key, err := cluster.Ceph(ctx, "auth", "get-key", "osd.0") // the new key
```

- The daemon keeps running and answering throughout.
- Without step 2, a restarted daemon would read its old key and fail to start;
  `ceph tell <daemon> rotate-key` changes only the running process.
- MONs share the `mon.` key, so their handle has no `RotateKey`. A stopped or
  paused daemon and the handle of a removed daemon are refused; nothing changes
  in these cases.
- Until step 4, the MONs accept both keys, so the running daemon and a
  restarted one can always authenticate. The MONs also commit the pending key
  on their own as soon as the daemon authenticates with it, after a restart or
  a handoff. Failures keep that property:

  | Failure | State afterwards | Recovery |
  | --- | --- | --- |
  | Before step 3, including cancellation | The MONs keep the pending key, and each keyring file holds the old or the new key, so the daemon authenticates with either. Nothing is undone: undoing would race with a restart from a file that already holds the new key. If such a restart has already made the MONs commit the new key, the call returns nil instead. | Call `RotateKey` again: it finishes with the same key, unless a restart has made the MONs commit it meanwhile, which finishes the rotation. |
  | Step 3 | Unknown whether the daemon received the new key, and a `rotate-key` that was already sent may still reach it later. The keyring files hold the new key; both keys stay valid. | Call `RotateKey` again: it finishes with the same key. Until the daemon's container restarts, for example through `Stop` and `Start` on its `testcontainers.Container`, the cluster refuses to give that daemon any other key, because a late `rotate-key` would switch it back to this one. |
  | Step 4 | The daemon and its files hold the new key, which the MONs accept whether or not the commit applied. `RotateKey` settles the MONs (see below) and reads the keys: a commit that applied returns nil. | If the error says that the commit did not apply, call `RotateKey` again or run `ceph auth commit-pending <daemon>`. |

  A call that finds a pending key from an earlier one resumes with it and
  never clears it, because the daemon may already hold it.
- A call does not return while one of its commands may still change the keys
  or the keyring files, unless it records that command for the daemon; the
  next call then deals with it before it changes anything, and fails while it
  cannot:
  - Every `ceph` command runs under the supervisor that
    [AdminSocket](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#OSDContainer.AdminSocket) uses. A command that the supervisor could not
    confirm as stopped is stopped by the next call.
  - Keyring files are written to a new file in the daemon container and moved
    into place with `mv` through a Docker exec, whose end is confirmed. Docker
    may complete a file copy after its request failed, but such a copy only
    creates the new file. An exec whose end is unknown is waited for by the
    next call. The daemon image needs `mv`, next to the `sh`, `cp` and `mkdir`
    that the daemon start scripts use.
  - The MON commands that change the keys (`auth get-or-create-pending` and
    `auth commit-pending`) are sent by a short Python script through
    librados, which first records the nonce by which the MONs list its
    client's sessions. Before a call returns, and before it reports the
    outcome of a failed command, it settles the MONs for these commands:
    1. It waits until no MON lists a session of those clients. TCP delivers
       a connection's data before its end, and a MON drops a command whose
       connection has closed before it handles it, so each command has then
       been handled or dropped.
    2. It adds and removes `client.tc-key-rotation-<uuid>`, an identity
       without capabilities, through every MON in turn. Each CLI contacts
       only its MON (`mon_host_override`), and its MON client log must show
       that it reached no other MON. MONs forward commands to the leader in
       the order they handle them, and the leader commits auth changes in
       order, so these changes come after every handled command.
    3. The quorum's election epoch and monmap must stay the same meanwhile.

    Settling needs every MON in the monmap to answer. A call that cannot
    settle records its commands, and the next call settles them first.
- How to observe keys and rotating service secrets from the daemon side is in
  [Running native commands](native-commands.md#observing-cephx-keys-from-the-daemon-side).

## Failure archive and post-test verification

`TerminateOnCleanup` with `FinalizationConfig.Archive` writes a failure archive
from `CollectDiagnostics`, `CollectCrashMetadata`, verification reports and
capture reports, and `Finalization.Verify` checks log lines and new crashes
against expected events. The phases and their order are in the godoc of
`Finalization`; this section describes the archive layout and the verification
model.

### The failure archive

A passing test collects and writes nothing. In a failed test, the archive
runs `CollectDiagnostics` and `CollectCrashMetadata` at the same time, within
`ArchiveTimeout`, and writes each report it gets even when the collector
returned an error, so a failed query or container leaves the rest. It writes
into a new directory `ceph-<cluster>-<execution ID>`, where the execution ID
is random, so parallel tests, repeated executions and several clusters of one
test never share one:

| File | Contents |
| --- | --- |
| `diagnostics.json` | The `ceph.DiagnosticsReport`, absent when the collector returned none. |
| `crash-metadata.json` | The `ceph.CrashMetadataReport`, absent when the collector returned none. |
| `verification-<n>-<name>.json` | The `ceph.VerificationReport` of each [verification](#verify) whose check ran, in activation order. |
| `capture-<n>-<name>.json` | The report of each [settled capture](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Finalization.Capture) that returned one, numbered by registration order. |
| `manifest.json` | Schema version, test, scenario, cluster ID, execution identity, role images, trigger, phase, each check, collector, file write, cleanup, the termination outcome and each tracked debug level window (see below). |

The manifest's execution identity has the Go version and platform, the VCS
revision when the test binary carries one, and the repository, workflow
event revision (`workflow_sha`, from `GITHUB_SHA`), workflow, job, run ID and
run attempt of a GitHub Actions run; no other environment variable is read.
The workflow event revision is not the checked-out source: a comment-triggered
run checks out a pull request's head, so only the binary's VCS revision counts
as the source revision. Provenance that the library does not know is listed in
`missing_provenance`, such as the source revision of a binary without VCS
information and the bug compatibility selection
(image IDs are in the diagnostics' `container-inspect` artifacts). A check
or cleanup records its error (a returned error, a panic, or a `t.Fatal` or
`t.FailNow` that ended its goroutine, which never skips the later phases)
apart from `failed_test`, set when the step turned a passing test into a
failed one through the test; the trigger has both kinds for checks and for
cleanups. Each
collector entry has `reported`, `complete` and its error; the crash entry
counts the containers per `ceph.CrashOutcome`, so an empty or absent crash
directory stays distinct from rejected metadata and found records. The
archive is first written while the cluster runs; after the cleanups and
`Terminate`, the manifest is replaced with their outcomes and `final` set.
Nothing is collected again after termination. The manifest records the
archive's directory name, not its host path.

By default the archive goes into the test's `ArtifactDir`, which `go test`
keeps under its output directory only with `-artifacts` (and `-outputdir`
for another place than the package directory). Without the flag the
directory is temporary and is removed after the test; the manifest's
`retained` field and the log line say so. Errors in creating the directory
or writing a file are logged and recorded, never retried in a loop, and do
not stop the later phases. If `ArtifactDir` itself cannot be created, Go
fails the test, and the cleanups and `Terminate` still run.

`Finalization.TrackDebugLevel` registers an [override.SetDebugLevel](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/override#SetDebugLevel)
handle, including the non-nil handle that comes with an apply error. Each call
is one window in registration order (`debug_windows[].sequence`); windows on the
same daemon, option and generation are never merged or deduplicated. The
manifest keeps a copy of each window's receipt when the failure archive is
taken, labeled with the archive's phase (`archive`), and another after the
cleanups (`final`), so a restoration run by `Finalization.Cleanup` is a later,
separate record and the earlier copy still shows the raised level. The copy
reads only the handle's local state; it never sets, restores or rereads a
daemon, and a generic cleanup success does not stand in for `restored-verified`.
`TrackDebugLevel` takes a `ceph.DebugLevelReceipter` (an interface with
`Receipt() DebugLevelReceipt`), which `*override.DebugLevelHandle` implements.
A nil interface, a typed nil (an interface whose dynamic value is a nil
pointer, map, slice, func, chan or interface; `Receipt` is never called on it,
so a method that is not nil-safe cannot panic), a zero-value handle and any
receipter whose receipt has an empty `State` are recorded with `present: false` and no
receipt, never as a restored window: absence is decided by the receipt, not by
a nil comparison of the interface. A panic raised by a foreign `Receipt` on a non-nil value is not recovered. A receipt in state `not-applied` (`DebugLevelNotApplied`) is
recorded with `present: true` and its `reason`, redacted and bounded to 2048
bytes like a transition error; the field is omitted for every other state. The records are
bounded: 32 windows (`debug_windows_omitted` counts the rest), the first 16
and last 16 transitions of a window (`transitions_omitted` counts the middle),
256-byte fields and 2048-byte transition errors, all redacted like the other
manifest errors. Registering after finalization started fails the test.
Passing runs archive nothing. The copies are not native evidence of a restored
daemon level and do not cover other daemons, options or a window that spans an
OSD replacement.

Limits:

- The reports are a snapshot during finalization: observations are not
  atomic, and log tails are not continuous history.
- No crash records is not proof that a daemon never crashed. A container
  removed earlier took its records along.
- Nothing runs after a forced process exit, a Go test timeout panic or a lost
  runner.
- Redaction is that of the collectors, also for the errors in the manifest;
  review arbitrary daemon logs before sharing an archive.

### Verify

Opt-in post-test verification of a dedicated cluster: the log lines and the
new crashes of its daemons between the activation and the end of the test,
each against its own expected events. `Verify` activates it on a
`Finalization` from [TerminateOnCleanup](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#TerminateOnCleanup) and adds a check
named `verification <name>`, so it runs before the failure archive and the
cleanups. A verification that is not clean fails the test, and with
`Archive` its report is archived with the diagnostics before anything is
removed. Runs that do not call `Verify` keep their behavior.

```golang
finalization, err := cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{Archive: true})
if err != nil {
	t.Fatal(err)
}
// The test crashes osd.0 on purpose: require that crash and its signal line.
_, err = finalization.Verify(ctx, ceph.VerificationConfig{
	Logs: []ceph.LogExpectation{{Name: "osd0-signal", Required: true, Daemon: "osd.0",
		Event: ceph.LogFatalSignal, Pattern: `Caught signal`}},
	Crashes: []ceph.CrashExpectation{{Name: "osd0-crash", Required: true, MaxCount: 1,
		Daemon: "osd.0", Process: "ceph-osd"}},
})
if err != nil {
	t.Fatal(err)
}
```

The activation records the baseline: the selected container generations
(the cluster's daemon containers and `AdditionalContainers`, not the control
container or the toolkit client), with their container ID, start time and
restart count; the crash IDs already in their crash directories; and the
window's start on the Docker engine's clock. A crash is new when its crash ID
was not in that container's directory at activation. `Verify` returns an
error for an invalid configuration, when the engine's clock cannot be read,
after the finalization has started, or for a name already active on the
finalization. `Evaluate` returns the report of the window until now without failing
the test, and can be repeated; it returns an error only when it cannot
evaluate at all.

Log events are lines of the containers' Docker log streams in the window,
read with Docker's timestamps. A line is an event of one `ceph.LogEvent`
kind. Assertion and fatal signatures are found anywhere in a line; the
cluster log kinds come from the severity of the line's log envelope, never
from a marker inside its message:

| Kind | Line |
| --- | --- |
| `LogAssertion` | `FAILED ceph_assert`, `FAILED assert` or `ceph_abort`, anywhere in the line |
| `LogFatalSignal` | `Caught signal`, anywhere in the line |
| `LogClusterError` | a cluster log entry of severity `ERR` |
| `LogClusterSecurity` | a cluster log entry of severity `SEC` |
| `LogClusterWarning` | a cluster log entry of severity `WRN` |

The first match in this order decides. A cluster log entry has one of two
forms, as the pinned Ceph source (Tentacle 20.2.4) prints them: a daemon's own
log line `<stamp> <thread> <level> log_channel(<channel>) log [<PRIO>] : <message>`,
and the cluster log line `[<channel> ]<stamp> <name> (<rank>) <seq> : <channel> [<PRIO>] <message>`
that the MON writes (a leading channel must equal the entry's own). `PRIO` is
`DBG`, `INF`, `SEC`, `WRN` or `ERR`. The stamp must be a real calendar date and time
with three or six fractional digits and a canonical numeric timezone `[+-]HHMM` (minutes below 60, offsets from -1200 to +1400), as the producer writes it;
an envelope with any other stamp is not an envelope. An entry of any channel (`cluster`,
`audit`, ...) is an event by its severity, so an `INF` entry that quotes
`[WRN]` is no event, and a `WRN` entry that quotes `[ERR]` is a warning. A line
that carries an `[ERR]`, `[SEC]` or `[WRN]` marker without one of these forms is
not classified and not guessed: the check has a gap that counts such lines, so
it is incomplete. A quoted assertion or fatal signature still makes an
assertion or fatal event, whatever envelope it is in. An expectation's
`Pattern` still matches the whole line, envelope included. Formats are
qualified per Ceph release natively separately; other releases and outputs
(for example journald or syslog) are not recognized.

Daemons write their cluster log entries to their own output and the MON
writes them again as the cluster log, so one entry can be two events. Other
lines are not evaluated.

Each event goes to the first expectation of its kind (log or crash) that
matches it and still permits one more; one that none permits is unexpected.
A `ceph.LogExpectation` matches all of its non-empty criteria: `Daemon` (the
source as the report names it, such as `osd.0` or `mgr.a`), `Event`, and the
required `Pattern`, a regular expression on the redacted line. A
`ceph.CrashExpectation` matches `Daemon`, `Process` (such as `ceph-osd`) and
`Signature`, a regular expression on one line of the crash's `stack_sig`,
`assert_*` or `backtrace` fields; at least one of the three is required.
Both have `Name`, which identifies them in the report and is unique, and:

- `Required`: the expectation must match at least `MinCount` events (at least
  one) or the check is unexpected. Without it the expectation only permits,
  and `MinCount` must be zero.
- `MaxCount`: the number of events it permits, zero for any. Further events go
  on to the next expectations.

A log expectation never matches a crash, and a crash expectation never a log
line. Expectations do not repair, restart or archive anything, and they do
not suppress the test's own checks.

`ceph.VerificationConfig` fields (zero selects the default):

| Field | Default | Maximum | Meaning |
| --- | --- | --- | --- |
| `Name` | `post-test` | 128 bytes | Names the check and the report. |
| `Logs`, `Crashes` | none | 256 each | The expected events. |
| `Timeout` | 2 minutes | 5 minutes | The activation, and each evaluation; the check is also bounded by `CheckTimeout`. |
| `OperationTimeout` | 20 seconds | 1 minute | Each Docker operation, including one container's log stream. |
| `MaxLogBytes` | 32 MiB | 256 MiB | Log stream bytes of the window per container. |
| `MaxFindings` | 256 | 4096 | Findings kept per check. |
| `AdditionalContainers` | none | 64 | Caller-owned daemon containers, such as mirror daemons; never adopted. |
| `RedactValues` | none | 128 | Additional secrets to remove from the report. |

`ceph.VerificationReport` has `Name`, `Cluster`, `Images`, `Outcome`, the
`Window` (`ceph.VerificationWindow`: `Phase` `test`, `StartedAt` and
`EndedAt` on the engine's clock, `EvaluatedAt`), the `Sources`
(`ceph.VerificationSource`: `Source`, `Role`, `Name`, `ContainerID`,
`ImageID`, `StartedAt`, `RestartCount`, `State`, `LogDriver`, `LogBytes` and
the `BaselineCrashes`), the effective `LogExpectations` and
`CrashExpectations`, `Notes` on its scope, and one `ceph.VerificationCheck`
each for `Logs` and `Crashes`. A check has its `Outcome`, the `Permitted`
and `Unexpected` counts, `Findings` (`ceph.VerificationFinding`: `Source`,
`ContainerID`, `Event`, `Time`, the redacted `Text` of at most 1 KiB with
`Truncated`, `CrashID`, and the permitting `Expectation`, empty when
unexpected), with the unexpected ones first and `OmittedFindings` beyond
`MaxFindings`, the `Expectations` (`ceph.ExpectationResult`: `Name`,
`Required`, `MinCount`, `MaxCount`, `Matched`, `Satisfied`) and the `Gaps`
(`ceph.VerificationGap`: `Source`, `Reason`).

Each check's `ceph.VerificationOutcome`, and the report's, which is the worse
of the two:

| Outcome | Meaning |
| --- | --- |
| `VerificationUnexpected` | An event was not permitted, or a required expectation was not satisfied. Gaps found besides stay in the report. |
| `VerificationIncomplete` | Nothing unexpected was found, but an observation could not be completed; see `Gaps`. |
| `VerificationClean` | Every selected source was observed for the whole window, and nothing unexpected was found. |

An observation that cannot be completed is a gap, never clean: a container
that was not created, removed, restarted or inaccessible at activation or
since; a log driver other than `json-file`, or `json-file` with `max-size`
rotation; a log stream that fails, is cut inside a frame, has a line without
a valid timestamp or exceeds `MaxLogBytes`; a crash directory that is
missing, unreadable, cut off or holds rejected metadata; a new crash without
its done marker; a container added after the activation; more than 128
containers; an unknown end of the window, or an engine clock that did not
advance since the activation; and an expired context.

Limits:

- The window starts at the activation and ends at the check; bootstrap before
  it and the cleanups and termination after it are outside it. Only the
  daemons' output is observed, not lines below their log level.
- Only the container generations selected at activation are covered. A
  removed or replaced container takes its logs and crash records along and is
  reported as a gap, not covered.
- Clean is evidence for the reported sources and window, not proof that no
  daemon failed. Native evidence currently includes one Ceph 20.2.4 / Linux
  amd64 cohort with unchanged generations and a non-rotating `json-file`
  driver: early and late warnings stay in the verification window after the
  early event has left the 24-line diagnostic tail, the selected generation
  and its configuration are unchanged, no log gap, omitted finding or
  truncated finding is reported, and cleanup passes. The same run checks that
  an INF line quoting a warning cannot satisfy a required warning (see the
  #206 receipt on #47). This does not qualify other releases, daemon roles or
  log formats, rotating, restarted or removed generations, or continuous
  collection, and current gaps stay incomplete coverage. Native crash
  completion timing and measured collection-resource defaults remain
  unestablished; the configured limits and their loss accounting are separate
  evidence.
- Scrub results, fresh scrubs and fault recovery waits are not verified, and
  the bug compatibility selection is not known to the report.
