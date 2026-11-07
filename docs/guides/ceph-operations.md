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
| `OSDContainer.Detach` (only with `OSDStorage.WithVolume`) | removed | store kept in the owned volume, registration kept; Ceph marks the OSD down, then out unless `noout` is held | `DetachedOSD.Reopen` starts a new container on the store and returns a new handle |
| `OSDContainer.Remove` | removed | drained, purged from Ceph, directory discarded | none; `AddOSD` creates a new OSD |

All three fence or keep the OSD's identity: every step checks the registration
UUID, `Remove` and `Detach` fence the Docker instance against new native
commands and drain its tracked ones, and `Remove` refuses an OSD that is being
detached or is detached. After `Detach` and `Reopen` the old handle is stale;
use the handle `Reopen` returns. `Reopen` starts the OSD from the image its
launch recipe recorded, or from the image of `OSDOptions.WithImage`; it refuses
`WithPlacement`. See [OSD storage](../internals/osd-storage.md) for the store
checks.

## Removing a MON

`MonitorContainer.Remove` kills the MON's process with SIGKILL before it sends
`mon remove` to the surviving MONs, in the order of Ceph's manual removal
procedure. When the removal commits, the survivors renumber their ranks and
drop every client session, and warm daemons and clients hunt again from a
monmap that still lists the leaving MON. If that MON still ran, it could
authenticate such a client and exit before the session is ready. The client
would keep the gone MON as its session: its messenger retries that address
without resetting the session, and its keepalive timeout needs the session's
features, so it never fires. A MGR caught this way stops beaconing and the
cluster reports `MGR_DOWN`; an MDS goes laggy. A graceful stop does not close
the window, because a MON that handles SIGTERM drops its sessions while it
still answers authentication.

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
with the error when the new container exists; a creation whose reply was lost
returns no handle, and only `Terminate` settles it. The MON is then down, its store is neither formatted nor
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
another (see [CreateClient](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.CreateClient)). The identity needs at least one
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
[cephfs/subvolume](../../cephfs/subvolume/README.md) package settle these
commands before they change a client. Their own commands go through the MGR, though, which this settling
cannot fence, and the module cannot tell when a failed one has ended: the
volumes module may still create or change the identity later. Do not replace
or revoke a client after a failed `Subvolume.Authorize` or
`Authorization.Deauthorize` for it. A `subvolume.AuthorizedClients` listing
shows only the state at that moment.

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
- [CreateClient](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.CreateClient) refuses the name.
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

`finalize.TerminateOnCleanup` with `finalize.Config.Archive` writes a failure
archive from `diagnostics.Collect`, `diagnostics.CollectCrashMetadata`,
verification reports and capture reports, and `Finalization.Verify` checks log
lines and new crashes against expected events. The phases and their order are
in the godoc of `finalize.Finalization` and in the
[finalize](../../ceph/finalize/README.md) package page; this section describes the
archive layout and the verification model.

### The failure archive

A passing test collects and writes nothing. In a failed test, the archive
runs `diagnostics.Collect` and `diagnostics.CollectCrashMetadata` at the same time, within
`ArchiveTimeout`, and writes each report it gets even when the collector
returned an error, so a failed query or container leaves the rest. It writes
into a new directory `ceph-<cluster>-<execution ID>`, where the execution ID
is random, so parallel tests, repeated executions and several clusters of one
test never share one:

| File | Contents |
| --- | --- |
| `diagnostics.json` | The `diagnostics.Report`, absent when the collector returned none. |
| `crash-metadata.json` | The `diagnostics.CrashMetadataReport`, absent when the collector returned none. |
| `verification-<n>-<name>.json` | The `finalize.VerificationReport` of each [verification](#verify) whose check ran, in activation order. |
| `verification-<n>-<name>-logs.jsonl` | With `ArchiveLogs`, every line of that verification's window that its log check read, one JSON object per line (see [Verify](#verify)). |
| `capture-<n>-<name>.json` | The report of each [settled capture](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize#Finalization.Capture) that returned one, numbered by registration order. |
| `capture-<n>-<name>.pcap` | The raw file of each capture registered with [`CaptureWithFile`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize#Finalization.CaptureWithFile), at most 16 files and 64 MiB per finalization. |
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
counts the containers per `diagnostics.CrashOutcome`, so an empty or absent crash
directory stays distinct from rejected metadata and found records. The
archive is first written while the cluster runs; after the cleanups and
`Terminate`, the manifest is replaced with their outcomes and `final` set.
Nothing is collected again after termination. The manifest records the
archive's directory name, not its host path.

A test that `go test -timeout` ends runs no cleanup: Go panics and exits, and
the containers stay until Ryuk removes them. So with `Archive` and a test
binary deadline, the finalization arms one more archive, at `ArchiveTimeout`
plus ten seconds before that deadline. If the test still runs then, the
archive is written into its own directory with phase `before-deadline`,
`trigger.test_deadline` set to the deadline and `trigger.test_failed` as it
was at that moment. The test and the cluster still run during the snapshot,
and its manifest is never final, since no cleanup or termination outcome
follows the panic. A test that ends after it is finalized as usual, and
writes its own archive when it has failed; the finalization waits for a
before-deadline archive in progress first. A test that passes after a
before-deadline archive adds no archive of its own and keeps that one,
whose manifest stays not final. A test that registers its
finalization when less than that margin is left gets no before-deadline
archive. Nothing runs after any other forced exit: `os.Exit`, a signal such
as a cancelled job's, or a lost runner.

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
`TrackDebugLevel` takes a `finalize.DebugLevelReceipter` (an interface with
`Receipt() override.DebugLevelReceipt`), which `*override.DebugLevelHandle`
implements.
A nil interface, a typed nil (an interface whose dynamic value is a nil
pointer, map, slice, func, chan or interface; `Receipt` is never called on it,
so a method that is not nil-safe cannot panic), a zero-value handle and any
receipter whose receipt has an empty `State` are recorded with `present: false` and no
receipt, never as a restored window: absence is decided by the receipt, not by
a nil comparison of the interface. A panic raised by a foreign `Receipt` on a non-nil value is not recovered. A receipt in state `not-applied` (`override.DebugLevelNotApplied`) is
recorded with `present: true` and its `reason`, redacted and bounded to 2048
bytes like a transition error; the field is omitted for every other state. The records are
bounded: 32 windows (`debug_windows_omitted` counts the rest), the first 16
and last 16 transitions of a window (`transitions_omitted` counts the middle),
256-byte fields and 2048-byte transition errors, all redacted like the other
manifest errors. Registering after finalization started fails the test.
The finalization of a passing run archives nothing, so such a run keeps
a copy only in a before-deadline archive taken while it still ran, labeled
`before-deadline`. The copies are not native evidence of a restored
daemon level and do not cover other daemons, options or a window that spans an
OSD replacement.

Limits:

- The reports are a snapshot during finalization: observations are not
  atomic, and log tails are not continuous history. A verification with
  `ArchiveLogs` keeps the whole window of its covered sources instead.
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
`Finalization` from [finalize.TerminateOnCleanup](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize#TerminateOnCleanup) and adds a check
named `verification <name>`, so it runs before the failure archive and the
cleanups. A verification that is not clean fails the test, and with
`Archive` its report is archived with the diagnostics before anything is
removed. Runs that do not call `Verify` keep their behavior.

```golang
finalization, err := finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true})
if err != nil {
	t.Fatal(err)
}
// The test crashes osd.0 on purpose: require that crash and its signal line.
_, err = finalization.Verify(ctx, finalize.VerificationConfig{
	Logs: []finalize.LogExpectation{{Name: "osd0-signal", Required: true, Daemon: "osd.0",
		Event: finalize.LogFatalSignal, Pattern: `Caught signal`}},
	Crashes: []finalize.CrashExpectation{{Name: "osd0-crash", Required: true, MaxCount: 1,
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
read with Docker's timestamps. A line is an event of one `finalize.LogEvent`
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

The first match in this order decides. The cluster log kinds come only from
the severity of an entry's own log envelope, in the two forms the pinned Ceph
source prints (a daemon's `log_channel(<channel>) log [<PRIO>]` line and the
MON's cluster log line), so an `INF` entry that quotes `[WRN]` is no event,
and a line with a marker but no recognized envelope is a gap of the check,
never a guessed event. The grammar is in the godoc of
[`finalize.LogEvent`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize#LogEvent)
and its parser. Other releases and outputs (for example journald or syslog)
are not recognized.

Daemons write their cluster log entries to their own output and the MON
writes them again as the cluster log, so one entry can be two events. Other
lines are not evaluated.

Each event goes to the first expectation of its kind (log or crash) that
matches it and still permits one more; one that none permits is unexpected.
A `finalize.LogExpectation` matches all of its non-empty criteria: `Daemon` (the
source as the report names it, such as `osd.0` or `mgr.a`), `Event`, and the
required `Pattern`, a regular expression on the redacted line. A
`finalize.CrashExpectation` matches `Daemon`, `Process` (such as `ceph-osd`) and
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

The defaults and limits of
[`finalize.VerificationConfig`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize#VerificationConfig)
and the fields of `finalize.VerificationReport`, its sources, checks,
findings, expectation results and gaps are in their godoc.

A log line's redacted text, which expectations match and findings and the
archived lines keep, is redacted one line at a time with two additions that
per-line redaction alone would miss. A private key block is followed across
the lines of each source's stream: what lies between its `BEGIN` and `END`
delimiters becomes `[REDACTED PRIVATE KEY]` on every line, also when the read
ends inside the block, and an `END` whose `BEGIN` was not read (on a line
before the window, without a timestamp, or past the 64 KiB cut of a long
line) withholds the same way every line of that stream since the previous
`END`. A truncated line, whose rest was not read, withholds a tail that is the
start of a secret, such as a `RedactValues` entry, together with every whole
secret on the line, and ends in `[REDACTED]` instead, so that a cut cannot
leave all of a secret but its last characters, even when the secret ends the
line and its last characters are also its start.

Because log lines are redacted one at a time, a `RedactValues` entry with a
line break (`\n` or `\r`) could never match, and its parts on consecutive
lines would each stay readable in the findings and the archived lines.
`Finalization.Verify` refuses such an entry with `ceph.ErrInvalidRequest`, with
or without `ArchiveLogs`, and starts no verification.

Each check's `finalize.VerificationOutcome`, and the report's, which is the worse
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

With `ArchiveLogs`, an evaluation also keeps every line of the window that the
log check reads, events or not, and the failure archive writes them to
`verification-<n>-<name>-logs.jsonl` next to the report, in the order
received. Each object has the `source` (as the report names it),
`container_id`, `stream` (`stdout` or `stderr`), Docker's `time`, the
redacted `text` without that timestamp, and `truncated` for a line whose
rest was not read: one longer than 64 KiB, or the last line of a stream that
`MaxLogBytes` or an error ended. The source's `LogLines` in the report counts
its lines. The lines have the log check's coverage, and its gaps apply to
them: a restarted, removed or rotating source, or one with another log
driver, keeps no lines, and a source over `MaxLogBytes` keeps the prefix that
was read. Lines outside the window and lines without a valid timestamp are
not kept. The file holds what was received, not a record of every Ceph
emission, and repeated lines stay repeated (the MON writes a cluster log
entry more than once). A passing test writes no archive, so its lines are
not kept anywhere, and the file stays only as long as the archive directory.

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
  an INF line quoting a warning cannot satisfy a required warning
  (`TestVerificationJSONFileNativeReceipt`). This does not qualify other
  releases, daemon roles or log formats, rotating, restarted or removed
  generations, or continuous collection, and current gaps stay incomplete coverage. Native crash
  completion timing and measured collection-resource defaults remain
  unestablished; the configured limits and their loss accounting are separate
  evidence.
- Scrub results, fresh scrubs and fault recovery waits are not verified, and
  the bug compatibility selection is not known to the report.
