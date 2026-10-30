# Ceph cluster operations

The methods of a running `ceph.Container` and its handles, by task: lifecycle and cleanup, native commands, topology, pools, Cephx identities, configuration, MGR modules, health, fencing, full ratios, scrub, fault injection, networking, admin sockets and diagnostics, followed by the shared types and error categories. Signatures and short contracts are in the godoc; the [package README](../../ceph/README.md) gives the usual order of use.

## Container Methods

`*ceph.Container` owns every daemon, network and helper container it creates.
These rules apply across its methods:

- Context. Methods are bounded by the caller's context, and most also by
  [WithStartupTimeout](ceph-configuration.md#withstartuptimeout). Topology changes are serialized;
  waiting for a concurrent operation to finish also honours the context. A call
  canceled while waiting returns without native changes and can be retried with
  a fresh context. A call canceled after it sent a native command can leave that
  command applied, which is why results and handles are kept (below).
- Handles. An operation on one daemon, pool or Cephx identity is a method of
  its handle, not of `Container`, and takes no name or ID: `OSDContainer`
  (`Remove`, `SetIn`, `Detach`, `AdminSocket`, `RotateKey`), `DetachedOSD`
  (`Reopen`), `MonitorContainer` (`Remove`, `AdminSocket`), `ManagerContainer`
  (`Remove`, `AdminSocket`, `RotateKey`), `Pool` and `ClientConfig`, and in the
  service packages `cephfs.Filesystem`, `cephfs.MDS` and `rgw.Gateway`. A handle
  acts on the instance it names. The handle of a removed, detached, replaced or
  deleted daemon, pool or identity returns an error, and so does a handle of a
  terminated cluster. Functions that create handles, such as `AddOSD`,
  `CreatePool` and `CreateClient`, and cluster-wide operations stay on
  `Container`.
- Typed values. Global OSD flags are `OSDFlag`, PG IDs are `PGID` and the
  bootstrap files are a `ConnectionConfig`. An untyped string constant still
  converts to the flag and PG ID types, so the types do not replace the run-time
  checks.
- Partial results. A method that creates something (an OSD, a pool, a client
  identity, a handle) can return a non-nil result together with an error. The
  result identifies what was attempted and stays owned by the cluster; inspect it,
  retry the documented operation, or terminate the cluster. No method deletes
  data to roll back a partial failure.
- Observations. Read methods return the native state at the moment of the call.
  Several native reads are not one atomic snapshot. Health, PG statistics and
  usage are reported asynchronously by Ceph; poll with a deadline when you assert
  an eventual state. Several observation methods also verify the cluster's
  original FSID and pool IDs and return a zero value with an error when the
  identity differs.
- No compare-and-swap. Ceph offers no compare-and-swap for configuration, flags,
  CRUSH or auth. Do not change the same keys, flags or identities with raw CLI
  commands while a helper applies or restores them.

Methods that change cluster state temporarily return a restoration handle:

| Handle | Returned by | Ends with |
| --- | --- | --- |
| `*override.ConfigHandle` | [override.SetConfig](../../override/README.md#setconfig) | `Restore` |
| `*override.DebugLevelHandle` | [override.SetDebugLevel](../../override/README.md#setdebuglevel) | `Restore` |
| `*override.OSDFlagHandle` | [override.SetOSDFlag](../../override/README.md#setosdflag) | `Restore` |
| `*override.MGRModuleHandle` | [override.SetMGRModule](../../override/README.md#setmgrmodule) | `Restore` |
| `*override.FullRatiosHandle` | [override.SetFullRatios](../../override/README.md#setfullratios) | `Restore` |
| `*override.BlocklistHandle` | [override.AddBlocklist](../../override/README.md#addblocklist) | `Restore` |
| `*fault.PauseLease` | [fault.Pause](../../fault/README.md#pause) | `Resume` |
| `*fault.NetworkInterruption` | [fault.InterruptNetwork](../../fault/README.md#interruptnetwork) | `Restore` |
| `*fault.NetworkProfileLease` | [fault.ApplyNetworkProfile](../../fault/README.md#applynetworkprofile) | `Restore` |
| `*ceph.ClientRevocationHandle` | [ClientConfig.Revoke](#clientconfigrevoke) | `Restore` |

All handles follow the same contract:

- Keep a non-nil handle even when the call returned an error. A lost reply or a
  canceled context can leave the change applied, so restore it or terminate the
  cluster.
- `Restore`/`Resume` is idempotent and retryable with a fresh context. Copies of a
  handle share state.
- `Restore` returns only the changed item to its captured prior state, including
  "absent". It refuses, with an error, to overwrite a value that someone else
  changed in the meantime. Writes of the same value cannot be detected.
- A second handle for the same item (configuration key, daemon debug option,
  flag, module, ratios, container) is refused while the first is active.
- `Terminate` disposes of configuration, debug levels, flags, modules, ratios
  and the blocklist with the cluster, so restoring is optional when the
  condition lasts until the end of the test. Pauses are resumed and network
  interruptions restored by `Terminate` before it removes anything.

Restore in `t.Cleanup` with a context that is still live. `t.Context()` is
canceled before cleanup functions run:

```golang
change, err := override.SetConfig(ctx, cluster, setting)
t.Cleanup(func() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := change.Restore(ctx); err != nil {
		t.Error(err)
	}
})
if err != nil {
	t.Fatal(err)
}
```

See [Contexts and cleanup](contexts-and-cleanup.md) for cancellation,
retry and cleanup ordering in detail.

## Container Methods: Lifecycle and control

### Terminate

```golang
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes the whole cluster: it restores network interruptions and resumes paused
containers first, cancels the commands that [RunNativeCommand](#runnativecommand)
still runs, releases host port reservations, stops services (RGW, MDS)
before OSDs, removes all owned daemons, helper containers, the OSD memory
volume and the shared run volume of the admin sockets, and finally removes the
owned networks. Every container that the cluster creates carries the label
`org.testcontainers.ceph.cluster` with the cluster's ID, and `Terminate` also
removes a container with that label that it has no handle for. Docker can
create such a container when the reply to a create request was lost, for
example when the startup timeout ended during the request. It works on a partially
initialized cluster returned by `Run` with an error, and a failed cleanup can be
retried. Each container gets a 2-second stop timeout instead of Docker's
10 seconds, because the data is discarded; a `testcontainers.StopTimeout` option
passed by the caller takes precedence.

Containers you attached with [WithClient](#withclient) or
[ClientConfig.WithIdentity](#clientconfigwithidentity) are yours: terminate them before the
cluster, because Docker cannot remove a network that still has attached
containers. If an interrupted endpoint cannot be restored, its network is kept,
the restore failure is returned, and a later `Terminate` retries. With
`testcontainers.CleanupContainer`, register the cluster first and clients
afterwards.

### TerminateOnCleanup

```golang
func (c *Container) TerminateOnCleanup(t testing.TB, config ceph.FinalizationConfig) (*ceph.Finalization, error)
func (f *Finalization) Check(name string, check func(context.Context) error)
func (f *Finalization) Cleanup(name string, cleanup func(context.Context) error)
func (f *Finalization) TrackDebugLevel(handle ceph.DebugLevelReceipter)
func (f *Finalization) Capture(name string, session ceph.CaptureSettler)
```

Registers one `t.Cleanup` that finalizes the cluster and then calls
[Terminate](#terminate). Use it instead of another cleanup that terminates the
cluster, and register it right after `Run`. The cleanup runs these phases in
this order, whatever order the test added its steps in:

1. The checks added with `Check`, in the order they were added. A check is a
   post-test verification that needs the running cluster. It fails the test
   by returning an error, by panicking or through `t` itself.
2. The packet captures added with `Capture`, in the order they were added
   (see [Settling a capture](#settling-a-capture)).
3. With `Archive`, the failure archive, if the test has failed by now. A
   failure that a check or a capture settlement reported therefore triggers it
   too.
4. The cleanups added with `Cleanup`, in reverse order, such as the removal
   of a client container that the archive should still see. Test cleanups
   registered after `TerminateOnCleanup` run before all of this, as Go runs
   cleanups last-in first-out, so route such removals through `Cleanup`.
   With `Archive`, a cleanup that fails when nothing had failed before
   triggers an archive of what still runs, labelled `after-cleanup`.
5. `Terminate`. Its error fails the test.

```golang
cluster, err := ceph.Run(ctx, image)
if cluster != nil {
	finalization, regErr := cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{Archive: true})
	if regErr != nil {
		t.Fatal(regErr)
	}
	// client is a container that the test attached to the cluster.
	finalization.Cleanup("client", func(ctx context.Context) error { return client.Terminate(ctx) })
}
if err != nil {
	t.Fatal(err)
}
```

Each phase gets a fresh context with its own bound, because the test's
context has already ended when cleanups run. Errors stay separate: a check's
error and a cleanup's error fail the test under their names, the
archive's collection and storage errors are only logged and recorded in its
manifest, and none of them skips a later phase. Adding a step after the
finalization has started fails the test instead.

`ceph.FinalizationConfig` fields (zero selects the default):

| Field | Default | Meaning |
| --- | --- | --- |
| `Archive` | off | Write the failure archive when the test fails. |
| `ArchiveDirectory` | the test's `ArtifactDir` | Root of the archives; each gets a new directory under `<root>/<test name>/`. |
| `Scenario` | none | One-line label for the manifest. |
| `CheckTimeout` | 2 minutes | All checks together; at most 10 minutes. |
| `ArchiveTimeout` | 3 minutes | The whole archive, both collectors and every write; at most 10 minutes. |
| `CleanupTimeout` | 3 minutes | The cleanups together, and `Terminate` separately; at most 10 minutes. |
| `Diagnostics` | defaults | The [CollectDiagnostics](#collectdiagnostics) configuration, including `AdditionalContainers` and `RedactValues`. |
| `CrashMetadata` | defaults | The [CollectCrashMetadata](#collectcrashmetadata) configuration. |

Invalid fields are rejected before anything is registered.

#### Settling a capture

`Finalization.Capture(name, session)` hands a running [packet
capture](../capture/README.md) (a `*capture.Session`, through the `ceph.CaptureSettler`
interface that the `capture` package implements) to the finalization. The
capture is settled after the checks and before the failure archive and the
cleanups, so a failed test body that never reached its own `Stop` still ends
the capture, keeps its report and leaves the helper removed before a
`Cleanup` removes the caller-owned target. The caller still owns the
session: an explicit `Stop` earlier is not repeated and its report is the
one kept.

The settlement has its own allowance. It is not part of `CheckTimeout` and
does not use the expired test context: each capture gets a fresh context
bounded by `CaptureSettler.SettlementAllowance()` (for `capture.Session`, the
stop grace plus two minutes, 130 seconds by default), recorded as
`allowance` in the manifest. The finalization waits for the settlement to
return instead of abandoning it, so a target is never removed underneath a
settlement that still runs. A settlement error fails the test under the
capture's name next to the original failure, does not skip the other captures
or any later phase, and triggers the archive. `Capture` with a nil session, or
after the finalization has started, fails the test.

With `Archive`, the archive keeps each settled capture's report as
`capture-<n>-<name>.json` (`n` is the registration order) even when the
settlement returned an error, and the manifest lists each settlement under
`captures` with its error, `failed_test`, `allowance` and `reported`; the
trigger has `capture_failed`. A passing test settles its captures without
writing an archive. The archived report names the output file by its base
name only, and every string in it (such as `stderr`) is masked with the same
redaction as the manifest's errors: the configured `Diagnostics` and
`CrashMetadata` `RedactValues` and the recognized secret patterns. Only the
metadata is masked, never the pcap bytes. A `nil` session, including a typed
nil pointer, is refused at registration. This does not copy or retain the pcap file itself: raw capture
retention is a separate, explicit decision that this method does not make.

#### The failure archive

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
| `capture-<n>-<name>.json` | The report of each [settled capture](#settling-a-capture) that returned one, numbered by registration order. |
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

`Finalization.TrackDebugLevel` registers an [override.SetDebugLevel](../../override/README.md#setdebuglevel)
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

```golang
func (f *Finalization) Verify(ctx context.Context, config ceph.VerificationConfig) (*ceph.Verification, error)
func (v *Verification) Evaluate(ctx context.Context) (*ceph.VerificationReport, error)
```

Opt-in post-test verification of a dedicated cluster: the log lines and the
new crashes of its daemons between the activation and the end of the test,
each against its own expected events. `Verify` activates it on a
`Finalization` from [TerminateOnCleanup](#terminateoncleanup) and adds a check
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

### Ceph

```golang
func (c *Container) Ceph(ctx context.Context, args ...string) ([]byte, error)
```

Runs the `ceph` CLI in the control container with `--connect-timeout 5` and
returns its standard output. Arguments are passed directly, without a shell. Add
`--format json` for machine-readable output. A non-zero exit returns the
standard output with a [`*NativeFailure`](#nativefailure): the exit code, the
errno that Ceph reported, such as `ENOENT`, and Ceph's reason without its log
lines, with keys redacted. `errors.Is(err, fs.ErrNotExist)` holds for
`ENOENT`. When no MON answers the connection within 5 seconds, the CLI exits
before it sends the command; `Ceph` then connects again, until a MON
answers, `ctx` ends or a minute has passed, so a stalled or electing MON
delays the command instead of failing it, and the command runs once. See
[Classified errors](errors.md).

```golang
out, err := cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "noscrub", "true")
```

See [Running native commands](#running-native-commands).

### RunNativeCommand

```golang
func (c *Container) RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
```

Runs one command in `target` under a supervisor that bounds its run time and
each output stream inside the container, and that stops the command when
`ctx` ends. `target` is the control container, a daemon or service container
of this cluster, or a container of yours attached to the cluster's network.
It must be running, not paused, and have `python3`. The essential and client
roles and the rgw setup runtime provide it; the cephfs role does not promise it.

```golang
result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:      []string{"ceph-dencoder", "type", "OSDMap", "import", "/tmp/osdmap", "decode", "dump_json"},
	Timeout:   time.Minute,
	MaxStdout: 4 << 20,
	MaxStderr: 64 << 10,
	Label:     "decode OSDMap",
})
```

| `NativeCommand` field | Meaning |
| --- | --- |
| `Argv` | Program and arguments, without a shell |
| `Timeout` | Up to `MaxNativeCommandTimeout` (one hour); `ctx`'s deadline shortens it |
| `MaxStdout`, `MaxStderr` | Up to `MaxNativeCommandOutput` (64 MiB) each |
| `Label` | Names the command in errors; at most 128 bytes |
| `Input`, `MaxInput` | Optional standard input from `NewNativeInput`, and the bytes it may carry in all, up to `MaxNativeCommandInput` (64 MiB) |

- The result keeps stdout and stderr as separate raw bytes, and the exit code;
  a nonzero exit is not an error.
- A timed-out or truncated command returns its partial result with
  `TimedOut`, `StdoutTruncated` or `StderrTruncated` set, and an error that
  wraps `ErrNativeCommandIncomplete`.
- When `ctx` ends first, the command is stopped inside the container before
  the call returns, within `NativeCommandStopTimeout` (15 seconds) of its own.
  When that cannot be confirmed, the error is a
  `*NativeCommandUnstoppedError`; keep it and call its `Stop` with a fresh
  context. Its `Unwrap` returns the cause, so `errors.Is(err, context.Canceled)`
  still holds. `Terminate` also cancels and stops these commands. It does so
  first, before it waits for any running cluster operation, and from then on
  refuses new ones, also when it then fails.
- Stopping a command does not undo what it did, and a command is never run
  again.
- With `Input`, the command reads what `Input.Write` sends, in order, and the
  end of input after `Input.Close`. `InputWritten`, `InputDelivered` and
  `InputEOF` in the result say how much of it the command could read. Writes
  fail with `ErrNativeInputClosed` once the command ended.
- `Complete()` checks time and output limits, not input delivery. Assert the
  byte counts, EOF and the worker's receipt separately; bytes delivered into
  its pipe do not prove application execution or durability.
- Errors contain the label but never the arguments or the output. A Docker
  or container failure appears only by its kind; `errors.Unwrap` returns the
  original error.
- Failures before the command runs are classified: a malformed command, a
  container that the cluster neither owns nor has attached, or one that is
  stopped or paused is `ErrInvalidRequest`; a target without `python3`, or a
  program that does not exist or is not executable, is `ErrImageCapability`;
  a program that could not be started for want of a process or memory, or
  because a security policy refused it, and another Docker failure are
  `ErrEnvironmentUnavailable`. A container that disappears or stops after
  the command was sent matches no category, since the command may have run.
  Context errors, `ErrNativeCommandIncomplete`, `ErrNativeInputClosed` and
  `*NativeCommandUnstoppedError` belong to no category. See
  [Classified errors](errors.md).

See [Bounded commands with confirmed cleanup](native-commands.md#bounded-commands-with-confirmed-cleanup)
for cancellation details, [standard input](native-commands.md#standard-input),
and an oracle recipe with `rados_stat2` and `ceph-dencoder`.

### NativeInput

```golang
func NewNativeInput() *ceph.NativeInput
func (in *NativeInput) Write(ctx context.Context, data []byte) error
func (in *NativeInput) Close() error
```

The standard input of one `NativeCommand`. `Write` sends binary data after
the data of earlier writes and returns once the input stream to the
container took it; before the command starts it waits, within `ctx`. A write
that would pass `MaxInput` is refused whole with `ErrInvalidRequest`, and the
input stays open. `Close` ends the input. The transport does not put input in
arguments, the environment, files, errors or diagnostics; the command's own
output remains raw, so a worker handling secrets must not echo them. An
input serves one command and must be created with `NewNativeInput`.

### RedactSecrets

```golang
func (c *Container) RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error)
```

Returns a copy of `data` in which the keys of the cluster's keyrings, service
credentials, `values`, and the private keys, secret assignments and bearer
tokens it recognizes are replaced by `[REDACTED]`, as in
[CollectDiagnostics](#collectdiagnostics). Use it before logging native
output; assertions compare the raw bytes. Redaction is best effort.

### ControlContainer

```golang
func (c *Container) ControlContainer(ctx context.Context) (testcontainers.Container, error)
```

Returns the container in which the fixture runs CLI commands. In a single-MON
cluster this is MON `a`. With more than one monitor it is an independent CLI
container created by the first [AddMonitor](#addmonitor) (also during `Run`), so
commands keep working while MON `a` is stopped or removed. The cluster owns it.
`ctx` bounds the wait for a concurrent control-plane creation or termination.
The returned handle is a snapshot; it does not promise that the container stays
alive. The container is created
from the essential image (`ClusterFacts.Images.Essential`), so it carries the `ceph` CLI and
the bootstrap tools such as `ceph-authtool`, not the client role's `rados` and
`rbd` (use [ClientContainer](#clientcontainer)) and not `radosgw-admin` (use
`Gateway.Admin` or the multisite setup clients):

```golang
control, err := cluster.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
code, reader, err := control.Exec(ctx,
	[]string{"ceph", "--connect-timeout", "5", "fsid"}, tcexec.Multiplexed())
```

Do not pause or stop it; [fault.Pause](../../fault/README.md#pause) refuses it.

### ClientContainer

```golang
func (c *Container) ClientContainer(ctx context.Context) (testcontainers.Container, error)
```

Returns the owned userspace toolkit executor. A separate client image starts it
lazily; aggregate images reuse the [control container](#controlcontainer). Run
`rados`, `rbd` and the Python client bindings here.
`ctx` bounds the wait for a concurrent topology change. The cluster owns the container.

### Per-daemon images

```golang
type DaemonOptions struct { /* unexported */ }
func (DaemonOptions) WithImage(image string) DaemonOptions

func (m *MonitorContainer) Image() string
func (m *ManagerContainer) Image() string
func (o *OSDContainer) Image() string
func (d *DetachedOSD) Image() string

func (m *MonitorContainer) Version(ctx context.Context) (string, error)
func (m *ManagerContainer) Version(ctx context.Context) (string, error)
func (o *OSDContainer) Version(ctx context.Context) (string, error)
```

`DaemonOptions` is the configuration value of [AddMonitor](#addmonitor) and
[AddManager](#addmanager), as `OSDOptions` is for [AddOSD](#addosd). Its
`WithImage` starts that one new daemon from the given image instead of the
essential image (`OSDOptions.WithImage` does the same for an OSD). Nothing else
changes: the value does not touch the cluster's settings, `ClusterFacts` or any
other daemon.

```golang
mon, err := cluster.AddMonitor(ctx, "b", ceph.DaemonOptions{}.WithImage(newImage))
```

- Validation. The reference must be non-empty with no whitespace or control
  characters. Within one value a later `WithImage` replaces the earlier one; two
  values that both set the image are refused. A refusal matches `ErrInvalidRequest` and
  happens before the topology lock, any key or identity, any OSD registration
  and any container. Nothing checks
  that the image exists, that it has the daemon binary and the tools the
  fixture scripts use, or that its release works with the running cluster;
  an unusable image fails at start like any other.
- Re-launch. An OSD's launch recipe records the image. `OSDContainer.Detach` and
  `DetachedOSD.Reopen` start the OSD from the same image again, and `Stop` and
  `Start` keep the container. MON and MGR containers are never recreated by the
  package, so their image does not change for their lifetime. `Reopen` has no
  image option.
- Identity. `Image` returns the image a daemon's container was started from: its
  override, or the essential image. It reads the recorded start request, not
  Docker, and says nothing about the image's release.
- Facts that stay cluster-wide. `ClusterFacts.Images` reports the cluster's images
  and is not changed by an override, so it does not describe an overridden daemon. The same holds for the
  `images` field of the finalization manifest and of a verification report. The
  diagnostics archive records the image ID and reference of every container
  from Docker, so it shows the override. Read `Image` of each handle for the
  per-daemon values.
- Versions. `ClusterFacts.CephVersion` is recorded once from the first MON and the CLI
  image, and does not describe an overridden daemon. `Version` reads the
  release a running daemon reports through its own admin socket (`version`),
  such as `"20.2.4"`; it works under the conditions of
  [AdminSocket](#adminsocket) and returns an error for a reply without a
  recognizable version. The admin socket client is the cluster's CLI, so the
  reply is the daemon's own but the transport is the cluster's CLI executor, which runs from the essential image unless a separate CLI executor image was selected.
- Client releases. A separately started client container is still refused when its
  release differs from `ClusterFacts.CephVersion`. That check is unchanged and does not
  consider overridden daemons.

This option is a building block. It does not claim that any mixed-version or
upgrade combination works; no native evidence exists for it.

### ServiceContainers

```golang
func (c *Container) ServiceContainers() []testcontainers.Container
```

Returns the owned optional service containers (RGW gateways and MDS daemons),
sorted by service name. The cluster owns their cleanup. Use the service packages
(`rgw.Gateways`, `cephfs.Filesystems`) for typed descriptors.

## Container Methods: Cluster topology

### Monitors

```golang
func (c *Container) Monitors(ctx context.Context) ([]*ceph.MonitorContainer, error)
```

Returns the owned monitors sorted by daemon name. `ctx` bounds the wait for a
concurrent topology change.
The returned slice is independent; descriptors and containers remain owned by
the cluster. Each `*ceph.MonitorContainer`
embeds its `testcontainers.Container` and has a `DaemonName`. `Stop` and `Start`
on the handle inject failures and preserve the monitor's store; use
[MonitorContainer.Remove](#monitorcontainerremove) to change membership.

```golang
monitors, err := cluster.Monitors(ctx)
if err != nil {
	t.Fatal(err)
}
mon := monitors[1]
timeout := 10 * time.Second
if err := mon.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// With 3 MONs, 2 keep quorum and existing client sessions keep working.
if err := mon.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForQuorum(ctx); err != nil {
	t.Fatal(err)
}
```

When a majority is stopped, quorum is lost and new clients cannot authenticate.
After the monitors start again, both existing and new sessions work.

A MON binds the address that the monmap records for it. On a bridge network,
each MON returns at that address whatever the order of `Start` calls: before
a stopped MON starts, the fixture requests its original address from Docker
again. A MON whose endpoint is interrupted with
[fault.InterruptNetwork](../../fault/README.md#interruptnetwork) does not start until the interruption is
restored. MON a's container waits for a quorum when it starts, so after all
MONs were stopped, start the others first.

### AddMonitor

```golang
func (c *Container) AddMonitor(ctx context.Context, name string, opts ...ceph.DaemonOptions) (*ceph.MonitorContainer, error)
```

Starts a new monitor with the given name and joins it to the live quorum's
monmap with the shared key. Names already in the monmap are rejected, and `a` can
never be reused. The first call also starts the independent control container.
After joining, it copies the new quorum's addresses into the `mon_host` of every
owned daemon's `ceph.conf`, including stopped daemons. A partially started member
stays owned on error; if only the configuration copy failed, retry
[RefreshMonitorConfig](#refreshmonitorconfig). Client containers you own are not
updated (see [RefreshClientMonitorConfig](#refreshclientmonitorconfig)).
[DaemonOptions.WithImage](#per-daemon-images) starts this one MON from another image.

### MonitorContainer.Remove

```golang
func (m *MonitorContainer) Remove(ctx context.Context) error
```

Removes an owned monitor from the monmap, deletes its container and refreshes the
owned daemons' `mon_host`. It refuses a removal that would leave fewer than a
majority of the current map running; add a replacement first. MON `a` can be
removed too; afterwards the embedded container of `*ceph.Container` is gone and
fixture commands use the control container. If the `mon remove` reply is lost,
the call returns an error without resending; calling `MonitorContainer.Remove` again with a
fresh context reads current membership, skips the completed native step and
finishes the container and configuration cleanup. The call acts on the monitor
that this handle names; the handle of a removed monitor returns an error. The
primary MON `a` is described by a fresh descriptor on every listing, so a handle
is matched by its name and container, not by pointer identity.

### QuorumStatus

```golang
func (o *Observer) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
```

Returns the native `quorum_status`: `QuorumNames` (members currently in quorum)
and `MonMap` with the FSID and each monitor's name and advertised address vector
(`PublicAddrs.Addrvec`, entries of `Type` such as `v2`/`v1` and `Addr`). The query
runs under a 5-second process limit inside the control container, so it returns
an error rather than hanging while quorum is lost.

### WaitForQuorum

```golang
func (c *Container) WaitForQuorum(ctx context.Context) error
```

Waits until a majority of the current monmap is in quorum, including when a
monitor is deliberately stopped. It does not require `HEALTH_OK` or every monitor
to run.

### MonitorBootstrapAddresses

```golang
func (o *Observer) MonitorBootstrapAddresses(ctx context.Context) (string, error)
```

Returns the current majority monmap's versioned address vectors in the format of
Ceph's `mon_host` setting, after checking that the live FSID matches the
cluster's original bootstrap configuration. Use it to configure a client you
manage yourself after replacing monitors. It changes nothing.

### RefreshMonitorConfig

```golang
func (c *Container) RefreshMonitorConfig(ctx context.Context) error
```

Copies the live quorum's bootstrap addresses into the global `mon_host` entry of
every owned daemon's `/etc/ceph/ceph.conf`, including stopped daemons, without
starting or restarting them. Other entries and comments are kept, and files that
are already current are not rewritten. `AddMonitor` and `MonitorContainer.Remove` already
do this; call it to retry after an `AddMonitor` that joined the monmap but could
not copy every file. Caller-owned clients and cross-cluster links are not
updated. Running daemons learn new monitors from the monmap; the file matters for
their next start.

### Managers

```golang
func (c *Container) Managers(ctx context.Context) ([]*ceph.ManagerContainer, error)
```

Returns the owned manager candidates sorted by daemon name. `ctx` bounds the
wait for a concurrent topology change. The returned slice is independent; descriptors and containers
remain owned by the cluster. Each
`*ceph.ManagerContainer` embeds its container and has a `DaemonName`. Stop the
active manager's container to trigger a standby takeover, or stop the only
manager to test behaviour without a manager:

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
status, err := observer.ManagerStatus(ctx)
if err != nil {
	t.Fatal(err)
}
managers, err := cluster.Managers(ctx)
if err != nil {
	t.Fatal(err)
}
for _, mgr := range managers {
	if mgr.DaemonName == status.ActiveName {
		timeout := 10 * time.Second
		if err := mgr.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
	}
}
```

### ManagerContainer

```golang
func (c *Container) ManagerContainer() testcontainers.Container
```

Returns the container of the initial manager `a`. It is
nil when `a` has been removed or was never created (for example when the first
manager added after `ClusterConfig.WithNoInitialManagers` has another name). Use
[Managers](#managers) and [ManagerStatus](#managerstatus) for the current
candidates and the active identity.

### AddManager

```golang
func (c *Container) AddManager(ctx context.Context, name string, opts ...ceph.DaemonOptions) (*ceph.ManagerContainer, error)
```

Creates a named manager and waits for it to register in the MGR map, as active
if no manager is active, otherwise as standby. Existing daemon names are rejected,
and it never forces the active role away from a running manager. Success does
not mean that every MGR module is ready; use
[WaitMGRModuleReady](#waitmgrmoduleready) or [WaitForClean](#waitforclean)
afterwards. A non-nil descriptor returned with an error stays owned. On an
`AddressDualStack` cluster it then checks the advertised addresses of the
active MGR; see [Dual-stack clusters](ceph-configuration.md#dual-stack-clusters).
[DaemonOptions.WithImage](#per-daemon-images) starts this one MGR from another image.

### ManagerContainer.Remove

```golang
func (m *ManagerContainer) Remove(ctx context.Context) error
```

Deletes an owned manager after confirming that another running, registered
candidate is available. Removing the last candidate is refused; add a replacement
first. Removing the active manager makes a standby take over; on an
`AddressDualStack` cluster the new active manager's addresses are checked as in
[Dual-stack clusters](ceph-configuration.md#dual-stack-clusters). On failure the handle stays owned
for a retry or for cluster cleanup. The handle of a removed manager returns an
error.

### ManagerStatus

```golang
func (o *Observer) ManagerStatus(ctx context.Context) (ceph.ManagerStatus, error)
```

Returns the native MGR map summary: `Available`, `ActiveName`, `ActiveGID` and
`Standbys` (each with `Name` and `GID`). The GID changes when a manager process
restarts.

### Observe

```golang
func Observe(cluster *Container) (*Observer, error)
```

Returns an `Observer`, which groups read-only live queries of one cluster. A nil
cluster returns an error and a nil `Observer`. There is no `Container` accessor
for it.

### Observer

```golang
func (o *Observer) Status(ctx context.Context) (ceph.Status, error)
func (o *Observer) ManagerStatus(ctx context.Context) (ceph.ManagerStatus, error)
func (o *Observer) DaemonAddresses(ctx context.Context) (ceph.DaemonAddresses, error)
func (o *Observer) Configuration(ctx context.Context) ([]ceph.ConfigEntry, error)
func (o *Observer) BlocklistEntries(ctx context.Context) ([]ceph.BlocklistEntry, error)
func (o *Observer) MGRModules(ctx context.Context) ([]ceph.MGRModuleState, error)
func (o *Observer) OSDFlags(ctx context.Context) ([]ceph.OSDFlag, error)
func (o *Observer) OSDStates(ctx context.Context) ([]ceph.OSDState, error)
func (o *Observer) ManagerServices(ctx context.Context) (map[string]string, error)
func (o *Observer) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
func (o *Observer) MonitorBootstrapAddresses(ctx context.Context) (string, error)
func (o *Observer) HealthDetails(ctx context.Context) (ceph.HealthSnapshot, error)
func (o *Observer) FullRatios(ctx context.Context) (ceph.FullRatioSnapshot, error)
func (o *Observer) PGInconsistencies(ctx context.Context, pgid ceph.PGID) ([]ceph.InconsistentObject, error)
func (o *Observer) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ceph.ObjectWatcher, error)
func (o *Observer) ManagerMetrics(ctx context.Context) (ceph.ManagerMetrics, error)
func (o *Observer) Pools(ctx context.Context) ([]*ceph.Pool, error)
```

Each query reads the cluster when it is called; constructing an `Observer`
captures nothing and a retained one never returns an earlier result. A query is
not an atomic cluster-wide snapshot. These queries are methods of the
`Observer` only: the `Container` has no method of the same names. Its
queries after `Terminate` fail with the error of the command path. An
`Observer` exposes only its queries, owns no resource and needs no cleanup.
`Configuration`, `BlocklistEntries`, `MGRModules`, `OSDFlags`, `OSDStates` and
`ManagerServices` take the topology lock first and refuse a terminated cluster
before running a command.
`ManagerServices` waits for the lock under `ctx`; the others wait for it without
a context. `QuorumStatus` takes no topology lock and selects the control
container for each read. `MonitorBootstrapAddresses`, `HealthDetails` and
`FullRatios` wait for the topology lock under `ctx`, refuse a terminated cluster
before running a command, and keep the control handle and original-FSID checks.
`QuorumStatus` has no liveness check, so on a
terminated cluster it fails with the error of the command path.
`PGInconsistencies` and `ObjectWatchers` validate their arguments, wait for the
topology lock (`PGInconsistencies` under `ctx`, `ObjectWatchers` without a
context), refuse a terminated cluster and then run `rados` in the client
container that the client image role selects.
`ManagerMetrics` reads `ManagerServices` first and then runs the bounded
supervised scrape in the control container. A refused or failed collection is an
error and is never returned as an empty result. `Pools` lists the pools as
`Pool` objects (see [Pools](#pools)). The `Observer` stays in package `ceph`,
because its queries need the captured control handle, the topology and
configuration locks, which are internal to the
package.

### OSDs

```golang
func (c *Container) OSDs(ctx context.Context) ([]*ceph.OSDContainer, error)
```

Returns a snapshot of the owned OSDs sorted by ID. `ctx` bounds the wait for a
concurrent topology change.
The returned slice is independent; descriptors and containers remain owned by
the cluster. Each `*ceph.OSDContainer`
embeds its container and has the numeric `ID`; `Placement()` returns its resolved
CRUSH location. `Stop`/`Start` on the handle inject daemon failures; the OSD keeps
its identity and data (also with in-memory storage):

```golang
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
osd := osds[0]
timeout := 10 * time.Second
if err := osd.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Exercise degraded I/O. Ceph marks the OSD down after the heartbeat grace and
// out after mon_osd_down_out_interval unless the noout flag is set.
if err := osd.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

OSD bootstrap initializes only an empty directory. It refuses non-empty partial
stores without a regular ready marker before copying the launch keyring or
running mkfs. A ready store must have matching numeric ID, OSD UUID and configured
cluster FSID, with non-empty regular identity/keyring files, before daemon
startup. Failed initialization is not automatically replayed. See
[bootstrap admission](../internals/osd-storage.md#bootstrap-admission) for the exact checks
and limits. [Launch recipes](../internals/osd-storage.md#launch-recipes) capture file/config
inputs and rebuild fresh Readers without replaying container creation.

### AddOSD

```golang
func (c *Container) AddOSD(ctx context.Context, configs ...ceph.OSDOptions) (*ceph.OSDContainer, error)
```

Registers, formats and starts a new OSD and waits until it is up and in. With no
argument the OSD gets the default CRUSH location and the essential image. It uses
the block size and storage options selected at `Run`. The optional
[OSDOptions](#osdoptions) values select an explicit location or another image. An OSD returned with an error is tracked for cleanup and can be
inspected; nothing is purged automatically. `AddOSD` is refused while an
earlier [OSDContainer.Remove](#osdcontainerremove) has not finished its cleanup, so a reused numeric
ID cannot be confused with the old handle. Adding OSDs does not change existing
pool sizes. The context is checked and the assembled configuration is
validated once before the topology lock, any OSD
registration and any container.

An omitted host resolves to `osd-<ID>` after native registration. If that name
then conflicts with the selected root or rack, the registered descriptor is
returned with an error before its container starts; terminate the cluster in
that case.

```golang
osd, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(ceph.OSDConfig{
	Host: "host-d", Rack: "rack-b", DeviceClass: "fast",
}))
```

### OSDOptions

```golang
type OSDOptions struct { /* unexported */ }

func (OSDOptions) WithPlacement(config OSDConfig) OSDOptions
func (OSDOptions) WithImage(image string) OSDOptions
```

`OSDOptions` is a value; the zero value selects every default and each `With`
method returns a modified copy, so one base can derive independent
configurations. `WithPlacement` takes an [OSDConfig](ceph-configuration.md#clusterconfigwithinitialosds) whole.
`WithImage` starts this one OSD from another image with the contract of
[DaemonOptions.WithImage](#per-daemon-images): the reference must be non-empty with no
whitespace or control characters, and an empty image is an explicit invalid
selection. `AddOSD` composes several values in argument order: each field may be
set by at most one value, and setting it in two is refused with
`ErrInvalidRequest` even when both agree. Within one value a later call replaces
the earlier one. The rules are in
[configuration composition](../contributing/configuration-composition.md).

### OSDContainer.Remove

After identity and last-OSD admission, removal permanently fences that Docker
instance from new public `RunNativeCommand` calls and drains its tracked native
commands before reweight/out/stop/purge. A command admitted before the fence
must pass a second admission check before dispatch. An unconfirmed stop or
cancelled drain retains the invocation, OSD ownership and fence for a fresh
removal retry. Commands targeting other instances remain admitted. This does
not close cluster-wide native admission.

Admission does not cover attachment leases or fault handles. Internal service
shell admission and raw Docker handles keep their own contracts; raw references are not revoked by a Go admission
fence. `OSDContainer.Remove` still confirms container cleanup after permanent purge.


```golang
func (o *OSDContainer) Remove(ctx context.Context) error
```

Removes an owned OSD in the safe order: mark it out and drain it, wait for
`safe-to-destroy`, stop it, wait until it is down, purge it from Ceph and remove
its container. Every step checks the OSD's registration UUID, so an OSD that
someone else re-registered under the same ID is refused. Removing the last
registered OSD is refused.

If the remaining OSDs cannot hold the data (replica count or free space), the
drain does not finish and the call fails at the timeout; the OSD is left out and
reweighted and is not marked in again automatically. If the purge reply is lost,
the handle is kept: a retry with a fresh context treats the OSD's absence as
completion of its own purge, and after a completed purge only the Docker cleanup
is retried. Do not register or replace OSDs externally while a removal runs.
`OSDContainer.Remove` refuses an OSD that [OSDContainer.Detach](#osdcontainerdetach) is detaching or has
detached.

Like every method of an `*ceph.OSDContainer`, `Remove` acts on the daemon
instance that this handle names, not on whatever owns the ID now. The handle of
a removed or detached OSD, or one that [DetachedOSD.Reopen](#detachedosdreopen)
replaced, returns an error. After `Detach` and `Reopen`, operate on the handle
that `Reopen` returns. A handle of another cluster, or one of a terminated
cluster, returns an error too.

### OSDContainer.Detach

```golang
func (o *OSDContainer) Detach(ctx context.Context) (*ceph.DetachedOSD, error)
```

Stops and removes an owned OSD's container but keeps its store in the owned
volume and its MON registration, so that [DetachedOSD.Reopen](#detachedosdreopen) can start a
new container on them. It needs [OSDStorage.WithVolume](ceph-configuration.md#osdstoragewithvolume);
other storage is an invalid request. The OSD is down while it is detached, not
out or purged: hold `noout` with [override.SetOSDFlag](../../override/README.md#setosdflag) if Ceph
must not mark it out.

Detach checks the registration UUID, refuses an OSD whose container a held
network delay, network interruption or pause still targets, then fences the
instance against new native commands as [OSDContainer.Remove](#osdcontainerremove) does, stops it,
waits for it to be down and removes the container. Only after the removal does
it read the store's identity files, keyring and block inode and size, and
returns a `*ceph.DetachedOSD` with `ID()`, `UUID()`, `ClusterFSID()` and
`Placement()`. The OSD leaves [OSDs](#osds); this `*ceph.OSDContainer` keeps
naming the removed container and is stale: its methods return an error.

On error the OSD stays owned and fenced. Retry `OSDContainer.Detach` with a fresh context;
it resumes after a completed removal. `OSDContainer.Remove` refuses it meanwhile.

Detach only an OSD that is up. A daemon that is still booting can be killed at
the end of the stop grace period before it marks itself down; in a one-OSD
cluster no peer reports it, and `OSDContainer.Detach` waits until its context
ends.

### DetachedOSD.Reopen

```golang
func (d *DetachedOSD) Reopen(ctx context.Context) (*ceph.OSDContainer, error)
```

Starts a new container on a store that `OSDContainer.Detach` of the cluster
that detached it retained and waits for the OSD to be up/in. It returns a new
`*ceph.OSDContainer` with the same ID, UUID and placement and a different
container, started from the same image as the detached OSD (see
[per-daemon images](#per-daemon-images)); `Reopen` takes no image option.

Before anything is written it requires the store's identity files, keyring and
block inode and size to be unchanged since the detach, the MON registration to
hold the same UUID and be down, and the MONs' key for the OSD to equal the
stored one. A refusal leaves the OSD detached for another attempt. Once the
container is requested the `DetachedOSD` is consumed: on a later error the
returned handle is owned by the cluster, so detach or remove it, or terminate
the cluster. The OSD bootstrap checks the stored identity again and never
formats a store that has its ready marker. When the creation reply is lost,
the returned descriptor has a nil embedded container: do not call container
methods on it. `Terminate` finds and removes that instance by its owner label.

```golang
noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
if err != nil {
	t.Fatal(err)
}
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
detached, err := osds[0].Detach(ctx) // osds[0] is osd.0
if err != nil {
	t.Fatal(err)
}
// The store and registration of osd.0 have no container here.
osd, err := detached.Reopen(ctx)
if err != nil {
	t.Fatal(err)
}
if err := noout.Restore(ctx); err != nil {
	t.Fatal(err)
}
_ = osd // same ID and UUID, new container
```

Only graceful detach of one cluster's classic BlueStore store on the same
image is covered. Abrupt exit, host reboot, other images and reopening a store
in another cluster are not.

### OSDStates

```golang
func (o *Observer) OSDStates(ctx context.Context) ([]ceph.OSDState, error)
```

Returns the OSDMap state of the owned OSDs sorted by ID: `ID`, `UUID`, `Up`, `In`
and `Weight`. `Weight` is the OSDMap reweight in `[0,1]`, not the CRUSH capacity
weight. An owned OSD that is missing, purged or registered with another UUID is
reported as an error.

### OSDContainer.SetIn

```golang
func (o *OSDContainer) SetIn(ctx context.Context, in bool) error
```

Marks an owned OSD in or out of the OSDMap, then reads the state back and checks
the UUID. Out does not stop, purge or reweight the OSD in CRUSH: the container,
identity and data stay. Ceph restores the previous reweight when the OSD is
marked in again. Use [WaitForPGClean](#waitforpgclean) to wait for recovery while
an OSD remains out. The `noout` flag suppresses automatic out-marking by the MON
but does not block an explicit `OSDContainer.SetIn(ctx, false)`.

### OSDFlags

```golang
func (o *Observer) OSDFlags(ctx context.Context) ([]ceph.OSDFlag, error)
```

Lists the exact global OSDMap flag tokens as [OSDFlag](#osdflag) values,
including Ceph's internal compatibility flags (for example `sortbitwise`) that
are not among the `OSDFlag` constants. Per-OSD, CRUSH-node and
device-class flags are separate native state.

## Container Methods: Pools

### CreatePool

```golang
func (c *Container) CreatePool(ctx context.Context, config ceph.PoolConfig) (*ceph.Pool, error)
```

Creates a new pool with explicit placement and replication (see the
[PoolConfig fields](ceph-configuration.md#clusterconfigwithpools)). The whole configuration is validated and the
cluster must own enough eligible failure domains (`Replicas`, or `K+M` for EC,
distinct buckets of `FailureDomain` in `CRUSHRoot`, restricted to `DeviceClass`)
before any command is sent. Existing pools, CRUSH rules and EC profiles are
rejected rather than changed.

The pool uses a dedicated CRUSH rule `tc-<name>-replicated` (or `tc-<name>-ec` with
an EC profile of the same name) and is created with the autoscaler off. The
returned `*ceph.Pool` embeds the resolved `PoolConfig` and adds `CRUSHRule` and
`ErasureCodeProfile`. It describes the requested settings at creation; use
[Pool.Status](#poolstatus) for current state.

A non-nil `Pool` with an error identifies resources whose creation was attempted
(this includes a pool that was created but whose native ID could not be read; the
handle then binds to the pool of its name at its first successful operation, so
use it only to clean up; a `Pool` returned without an error is always bound);
no data is deleted after a partial failure. Call [WaitForClean](#waitforclean)
before application I/O.

```golang
appData, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "app-data", Replicas: 2, MinSize: 1, Application: "rados",
})
```

Every later operation on the pool is a method of the returned `*ceph.Pool`:
[Status](#poolstatus), [Usage](#poolusage), [PGs](#poolpgs),
[SetReplication](#poolsetreplication), [SetQuota](#poolsetquota),
[SetPlacement](#poolsetplacement), [SetPGCount](#poolsetpgcount),
[WaitForPGCount](#poolwaitforpgcount) and [Remove](#poolremove). The handle
identifies the native pool by its name and native ID. After `Remove`, or when
the name belongs to another pool, every method returns an error instead of
acting on the pool that now has the name. Copies of the `Pool` share that
identity.

For RBD on an EC pool, create the images in a replicated metadata pool and pass
the EC pool as the data pool (`--data-pool` for the `rbd` CLI, or the data pool
option of librbd).

### Pool

```golang
func (c *Container) Pool(ctx context.Context, name string) (*ceph.Pool, error)
```

Returns the handle of an existing pool by its exact native name, for a pool that
`CreatePool` did not create: an initial pool from `WithPools`, a default pool, or
a pool of a service. It fails when the pool does not exist. The handle is bound
to the pool's native ID. The returned `Pool` reports `Name`, `PGNum`, `Replicas`
(of a replicated pool), `MinSize` and `ErasureCodeProfile`; its other fields,
`CRUSHRule` included, are zero, and [Pool.Status](#poolstatus) reports the full
native state.

```golang
appData, err := cluster.Pool(ctx, "app-data")
```

### Pools

```golang
func (o *Observer) Pools(ctx context.Context) ([]*ceph.Pool, error)
```

Lists every pool, including pools created by Ceph, by services or through the
CLI, as one `*ceph.Pool` for each. Each handle identifies its native pool by
name and native ID, like the handle of [Container.Pool](#pool), and describes
what the listing reports: `Name`, `PGNum`, `Replicas` of a replicated pool,
`MinSize` and `ErasureCodeProfile`. Read the full native state with
[Pool.Status](#poolstatus), the usage with [Pool.Usage](#poolusage) and the PGs
with `Pool.PGs`. The listing is one read of the native pool map, so a failed or
malformed listing is an error and never an empty list.

### Pool.Status

```golang
func (p *Pool) Status(ctx context.Context) (ceph.PoolState, error)
```

Returns the current policy of one pool by its exact name. `ceph.PoolState` fields:

| Field | Meaning |
| --- | --- |
| `ID`, `Name`, `Type` | Native pool ID, name and type (`replicated` or `erasure`). The ID changes if a pool is deleted and recreated under the same name. |
| `Size`, `MinSize` | Replica count or EC shard count (`K+M`), and `min_size`. |
| `PGNum` | Current PG count. |
| `PGNumTarget`, `PGNumPending`, `PGPlacementNum`, `PGPlacementNumTarget` | Split or merge still in progress; zero when the native map omits them. |
| `CRUSHRule` | Native CRUSH rule ID. |
| `AutoscaleMode`, `ErasureCodeProfile`, `Flags` | Native values. |
| `Quota` | `ceph.PoolQuota` with `MaxBytes` and `MaxObjects` (zero means unlimited). |

### Pool.SetReplication

```golang
func (p *Pool) SetReplication(ctx context.Context, replicas, minSize int) error
```

Changes a replicated pool's `size` and `min_size` without changing data, PG count
or placement. The pool's CRUSH rule must contain enough owned placement domains
for the new size. EC shard counts cannot be changed. Commands are sequential; on
a partial failure inspect [Pool.Status](#poolstatus). Use
[WaitForClean](#waitforclean) for the resulting recovery.

### Pool.SetQuota

```golang
func (p *Pool) SetQuota(ctx context.Context, quota ceph.PoolQuota) error
```

Replaces both pool quota limits; a zero field removes that limit. Existing data
is never removed. The native pool ID is checked before each command, and the two
commands can partially succeed (inspect `Pool.Status` after an error). Quotas are
enforced from asynchronously reported PG statistics, so writes can overshoot
briefly; they are not a per-write accounting boundary. When a quota is reached,
Ceph marks the pool full and clients see writes blocked or `EDQUOT`, depending on
the client.

```golang
err := appData.SetQuota(ctx, ceph.PoolQuota{MaxBytes: 64 << 20, MaxObjects: 1000})
```

### Pool.SetPlacement

```golang
func (p *Pool) SetPlacement(ctx context.Context, placement ceph.PoolPlacement) error
```

Moves a replicated pool to another simple CRUSH rule. `ceph.PoolPlacement` has
`FailureDomain` (`osd` by default), `CRUSHRoot` (`default` by default) and
`DeviceClass` (none by default), with the same meaning as in `PoolConfig`. Size
and `min_size` stay unchanged; the new placement needs enough owned domains.

The rule is chosen as follows:

1. If the current rule already matches, nothing is sent.
2. The pool's `CreatePool` rule `tc-<pool>-replicated` is reused if it matches.
3. An existing `tc-<pool>-placement-<hash>` with matching steps is reused; one with
   different steps is refused.
4. Otherwise `tc-<pool>-placement-<hash>` is created. Rules are never deleted, so a
   pool can move back.

Success means the pool references the rule; data moves asynchronously. Until PG
reports reflect the new map, [WaitForClean](#waitforclean) can succeed on the old
placement, so poll [Pool.PGs](#poolpgs) for the expected up/acting sets. To hold
the move incomplete, set both `nobackfill` and `norecover` with
[override.SetOSDFlag](../../override/README.md#setosdflag): small PGs recover through the PG log
instead of backfill, so `nobackfill` alone does not stop them. EC pools are
refused because their rule is bound to the EC profile.

### Pool.SetPGCount

```golang
func (p *Pool) SetPGCount(ctx context.Context, pgNum int) error
```

Requests a new PG count (1 to 65536) for a pool whose autoscale mode is `off` or
`warn`; pools with autoscaling `on` are refused. It records the `pg_num` and
`pgp_num` targets. The active manager then splits or merges PGs in steps
(merges only proceed with clean PGs), so success means the targets were accepted,
not that the PGs changed. Without an active manager the targets stay pending.
Ceph's per-OSD PG limit (`mon_max_pg_per_osd`) can still reject the request. The
pool ID is checked before each command; after a partial failure inspect
[Pool.Status](#poolstatus).

### Pool.WaitForPGCount

```golang
func (p *Pool) WaitForPGCount(ctx context.Context, pgNum int) error
```

Waits until the pool's current, pending and target PG and PGP counts all equal
`pgNum` and the manager reports exactly `pgNum` PGs for it. Until then
`WaitForClean` can pass on the previous PG set. The pool is identified by the
native ID first observed; the wait fails if the name is bound to another pool.
Reaching the count does not imply clean PGs; call `WaitForClean` afterwards.

```golang
if err := appData.SetPGCount(ctx, 16); err != nil {
	t.Fatal(err)
}
if err := appData.WaitForPGCount(ctx, 16); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

### Pool.Remove

```golang
func (p *Pool) Remove(ctx context.Context) error
```

Deletes a pool created by `CreatePool` or `WithPools`, together with its
`tc-<name>-replicated` or `tc-<name>-ec` CRUSH rule and EC profile, so that a
long-lived cluster can give each test its own pool without reaching PG limits.

- Every object in the pool is destroyed, including RBD images and namespaces.
  Stop clients and mirroring that use it first.
- Pools that Ceph or a service created (`.mgr`, RGW zone pools, CephFS pools)
  and pools in use by a CephFS filesystem are refused. Remove filesystems with
  `cephfs.Remove`, which also removes the pools their configuration created.
- Ownership is recognised by the pool's rule name. A pool moved to a
  `tc-<pool>-placement-<hash>` rule with `Pool.SetPlacement` is refused; move it
  back to its original placement first. Placement rules are not removed.
- `mon_allow_pool_delete` is enabled in the central configuration only during the
  removal and then restored. It is refused while an `override.SetConfig` handle holds
  that key, and a config file that sets it to `false` blocks removal.
- A retry after an error removes whatever owned resources remain. An unknown name
  is an error.

### Pool.Usage

```golang
func (p *Pool) Usage(ctx context.Context) (ceph.PoolUsageSnapshot, error)
```

Reads one pool's row of `ceph df detail` and checks the cluster FSID and the
pool's native ID and name before and after. `ceph.PoolUsageSnapshot` fields (all
sizes in bytes):

| Field | Meaning |
| --- | --- |
| `FSID`, `Name`, `ID` | Cluster and pool identity. |
| `StoredBytes` (`StoredDataBytes`, `StoredOMAPBytes`) | Estimated logical user data, DATA plus OMAP. |
| `AllocatedBytes` (`AllocatedDataBytes`, `AllocatedOMAPBytes`) | OSD allocation including replication or EC and allocation overhead, excluding the BlueStore database. |
| `Objects` | Logical object count. |
| `MaxAvailableBytes` | Estimated writable logical capacity. |
| `UsedRatio` | Fraction in `[0, 1]`. |

The values come from asynchronous PG statistics: they are neither a
read-after-write barrier nor a quota check. Poll with a deadline after writing.
A pool without a statistics row is reported as an error, not as empty. Do not
assume `AllocatedBytes` equals `StoredBytes` times the replica count.

```golang
usage, err := appData.Usage(ctx)
if err != nil {
	t.Fatal(err)
}
t.Logf("objects=%d logical=%d allocated=%d", usage.Objects, usage.StoredBytes, usage.AllocatedBytes)
```

### Pool.PGs

```golang
func (p *Pool) PGs(ctx context.Context) (ceph.PoolPGSnapshot, error)
```

Reads the reported state of every PG of one pool (`pg ls-by-pool`), bracketed by
two OSDMap reads that verify the FSID and the pool's name and ID. It works before
any OSD exists. `ceph.PoolPGSnapshot` contains `FSID`, `PoolBefore`/`PoolAfter`
(`PoolState` observed around the query; policies and epochs may differ),
`OSDMapEpochBefore`/`OSDMapEpochAfter`, the manager's `PGReady` flag and `PGs`.

Each `ceph.PGState` has `PGID` (a [PGID](#pgid), which `DeepScrubPG`, `RepairPG` and `PGInconsistencies` accept as is), the native `State` string, `Up` and `Acting` (in
native order), `UpPrimary` and `ActingPrimary` (`-1` means none),
`ReportedEpoch`, `ReportedSequence`, `MappingEpoch`, `LastEpochClean`,
`StatsInvalid` and `Stats`. `ceph.PGStats` holds signed counters: `Objects`,
`Bytes` (logical payload), `ObjectCopies`, `ObjectsDegraded`, `ObjectsMisplaced`
and `ObjectsUnfound`.

These are reports, not current mappings:

- A removed OSD can still appear, EC vectors can contain `2147483647`
  (`CRUSH_ITEM_NONE`) for missing shards, and `unknown` states, empty vectors and
  zero epochs are valid observations.
- `PGReady == true` does not mean that every PG is active, clean or reported.
- Counters arrive asynchronously, even with `StatsInvalid == false`.

```golang
snapshot, err := appData.PGs(ctx)
if err != nil {
	t.Fatal(err)
}
for _, pg := range snapshot.PGs {
	t.Log(pg.PGID, pg.State, pg.Up, pg.Acting, pg.Stats.Objects)
}
```

## Container Methods: Clients and Cephx identities

### WithClient

```golang
func (c *Container) WithClient(opts ...ceph.ClientContainerOptions) testcontainers.CustomizeRequestOption
```

Returns a customizer for your own container that connects it to the cluster and
copies the bootstrap `/etc/ceph/ceph.conf` and the admin keyring
`/etc/ceph/ceph.client.admin.keyring` into it. In bridge mode the container joins
the cluster's public network; in host mode it joins the host network.

- The container must already contain the Ceph clients it uses (CLI, librados,
  go-ceph binary, ...). `WithClient` installs nothing; `ceph.DefaultImage` or
  the cluster's `Images.Client` work for CLI and Python clients.
- The admin key has full privileges on this test cluster. Use it only in trusted
  test containers; use [ClientConfig.WithIdentity](#clientconfigwithidentity) for restricted
  clients.
- Combine it with [WithIdleEntrypoint](#withidleentrypoint) for a container in
  which you run commands with `Exec`.
- Options change the copied `ceph.conf`: `ceph.ClientContainerOptions.WithMessengerPolicy` sets
  the client's Messenger connection modes (see
  [NetworkConfig.WithMessengerPolicy](ceph-configuration.md#networkconfigwithmessengerpolicy)).
- You own the container. Terminate it before the cluster (register its cleanup
  after the cluster's).

Ceph clients connect directly to the OSD addresses that the MONs advertise. In
bridge mode those are container addresses, so a process on the Docker host
(especially on macOS or Windows) cannot reach RADOS, RBD or CephFS through the
MON's mapped port alone. Run the client in a `WithClient` container, or use
[WithHostNetwork](ceph-configuration.md#withhostnetwork) on Linux.

### ClientConfig.WithIdentity

```golang
func (client *ClientConfig) WithIdentity(opts ...ceph.ClientContainerOptions) testcontainers.CustomizeRequestOption
```

Like [WithClient](#withclient), but copies only the keyring of a restricted
identity created by [CreateClient](#createclient). The container gets
`/etc/ceph/ceph.conf` (with the current MON addresses and a `[client.<id>]`
section pointing to the keyring) and the keyring at
`/etc/ceph/ceph.client.<id>.keyring` (`client.KeyringPath()`). The client must
select its identity, for example `rados --id reader ...`,
`CEPH_ARGS="--id reader"`, librados `rados_create(&cluster, "reader")` or go-ceph
`rados.NewConnWithUser("reader")`. Incomplete creations and revoked identities
are rejected, and so are descriptors whose key
[ClientConfig.ReplaceKey](#clientconfigreplacekey) replaced, whose identity
[ClientConfig.Revoke](#clientconfigrevoke) removed until its
`Restore`, or whose identity has a change that is not resolved yet. You own the container. The options are those of
[WithClient](#withclient).

### CreateClient

```golang
func (c *Container) CreateClient(ctx context.Context, name string, caps ceph.ClientCaps, opts ...ceph.ClientOptions) (*ceph.ClientConfig, error)
```

Creates a fresh Cephx identity with exactly the given capabilities. `name` is an
ID or `client.ID` using letters, digits, dots, underscores or hyphens. An existing
identity is rejected before any change; its key and caps are never updated.

`ceph.ClientCaps` has one field per service, each a native capability expression
passed as a separate CLI argument; empty grants nothing for that service:

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=app-data namespace=tenant-a",
})
```

| Field | Service |
| --- | --- |
| `Mon` | monitors |
| `OSD` | OSDs (RADOS data) |
| `MGR` | managers |
| `MDS` | CephFS metadata servers |

Ceph validates the syntax. A non-nil result returned with an error identifies an
attempted or partial creation; inspect auth state or terminate the cluster rather
than retrying with broader caps. The returned `*ceph.ClientConfig` keeps the
credentials private; see [ClientConfig](#clientconfig).

The key has the cluster's [WithAuth](ceph-configuration.md#withauth) `KeyType`, or the release
default without one. `ceph.ClientOptions.WithKeyType` selects
another type for this identity, for example to check that the MONs reject a key type they do not
allow:

```golang
legacy, err := cluster.CreateClient(ctx, "legacy", ceph.ClientCaps{Mon: "allow r"},
	ceph.ClientOptions{}.WithKeyType(ceph.CipherAES))
```

The identity is created whether or not the MONs allow its type; whether it can
authenticate depends on the MonMap's `auth_allowed_ciphers`. `aes256k` needs a
essential image whose `ceph-authtool` has `--key-type` (see
[WithAuth](ceph-configuration.md#withauth)); without it the call fails before any change.

### ClientConfig.Capabilities

```golang
func (client *ClientConfig) Capabilities(ctx context.Context) (ceph.ClientCaps, error)
```

Returns the current capabilities of an identity created by this cluster, after
verifying that its native key is still the one that was created. It does not
return secrets.

### ClientConfig.UpdateCaps

```golang
func (client *ClientConfig) UpdateCaps(ctx context.Context, caps ceph.ClientCaps) error
```

Replaces all capabilities of an owned identity while keeping its key. Services
omitted from `caps` lose their previous capabilities. Sessions that already hold
tickets may keep the previous permissions for a while; verify the change with a
fresh client connection. The change is fenced like the import of
[ClientConfig.ReplaceKey](#clientconfigreplacekey): a failed or canceled call reports what
the MONs hold once nothing can change it any more, and an outcome that stays
uncertain is resolved by the next operation on the identity.

### ClientConfig.Delete

```golang
func (client *ClientConfig) Delete(ctx context.Context) error
```

Revokes an identity created by this cluster, without deleting data. It refuses if
the native key was replaced externally. A repeated call after success is a no-op.
Established sessions may keep their tickets; test revocation with a new
connection. Credentials already copied elsewhere are not erased. The removal is
fenced like the import of [ClientConfig.ReplaceKey](#clientconfigreplacekey).

### ClientConfig.ReplaceKey

```golang
func (client *ClientConfig) ReplaceKey(ctx context.Context, opts ...ceph.ClientOptions) (*ceph.ClientConfig, error)
```

Gives an identity created by this cluster a fresh key and keeps its name and
its exact capabilities. The change is one `ceph auth import` of the identity's
own keyring with the new key, so the identity never disappears from the MONs.
The new key has the type of the old one; `ceph.ClientOptions.WithKeyType` selects
another (see [CreateClient](#createclient)). The identity needs at least one
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
  [ClientConfig.WithIdentity](#clientconfigwithidentity),
  [ClientConfig.Capabilities](#clientconfigcapabilities),
  [ClientConfig.UpdateCaps](#clientconfigupdatecaps) and [ClientConfig.Delete](#clientconfigdelete).
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
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](ceph-configuration.md#withauth)
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
- Until then, [ClientConfig.WithIdentity](#clientconfigwithidentity) refuses both
  descriptors.

[ClientConfig.UpdateCaps](#clientconfigupdatecaps) and [ClientConfig.Delete](#clientconfigdelete) are
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

```golang
func (client *ClientConfig) Revoke(ctx context.Context) (*ceph.ClientRevocationHandle, error)
```

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
- [ClientConfig.WithIdentity](#clientconfigwithidentity),
  [ClientConfig.Capabilities](#clientconfigcapabilities),
  [ClientConfig.UpdateCaps](#clientconfigupdatecaps), [ClientConfig.ReplaceKey](#clientconfigreplacekey)
  and another `ClientConfig.Revoke` refuse the descriptor.
- [CreateClient](#createclient) refuses the name.
- [ClientConfig.Delete](#clientconfigdelete) makes the removal permanent, after which
  `Restore` fails.

The client behaves as follows on the official images:

- A fresh connection fails at once with `EACCES`.
- A client that authenticated before keeps its sessions until it renews its
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](ceph-configuration.md#withauth)
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

```golang
func (o *OSDContainer) RotateKey(ctx context.Context) error
func (m *ManagerContainer) RotateKey(ctx context.Context) error
```

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
    [AdminSocket](#adminsocket) uses. A command that the supervisor could not
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

### ConnectionConfig

```golang
func (c *Container) ConnectionConfig(ctx context.Context) (ceph.ConnectionConfig, error)
```

Returns a [ConnectionConfig](#connectionconfig-type) with independent copies of the bootstrap `ceph.conf` (`Conf`) and the admin keyring (`Keyring`) for
a native client that you start yourself, for example a test process using
go-ceph in host mode or a custom client implementation. The configuration
contains the addresses that the cluster advertises: container addresses in bridge
mode, host addresses (with final MON ports) in host mode. The keyring grants full
privileges. `ctx` bounds the wait for a concurrent topology change or
configuration update. It fails after termination or before bootstrap completed.
On error it returns the zero
`ceph.ConnectionConfig`.

```golang
cfg, err := cluster.ConnectionConfig(ctx)
if err != nil {
	t.Fatal(err)
}
dir := t.TempDir()
confPath := filepath.Join(dir, "ceph.conf")
keyringPath := filepath.Join(dir, "ceph.client.admin.keyring")
if err := os.WriteFile(confPath, cfg.Conf, 0o644); err != nil {
	t.Fatal(err)
}
if err := os.WriteFile(keyringPath, cfg.Keyring, 0o600); err != nil {
	t.Fatal(err)
}
// Point the client at confPath and keyringPath (for example librados
// conf_read_file, plus "keyring" = keyringPath).
```

For a restricted identity use `ClientConfig.ConnectionConfig()` instead.

### RefreshClientMonitorConfig

```golang
func (c *Container) RefreshClientMonitorConfig(ctx context.Context, client testcontainers.Container) error
```

Updates only the global `mon_host` entry in `/etc/ceph/ceph.conf` of a container
you own (attached with `WithClient` or `ClientConfig.WithIdentity`) to the live quorum's
addresses. Use it after replacing monitors: running sessions follow the monmap,
but a restarted client reads the file. The client's FSID must match this cluster
and its live quorum. It works on running and stopped containers through Docker
file copies and never starts, stops or adopts the container. Keyrings, other
settings and `mon_host` overrides in other sections are kept. Successful writes
stay on partial failure, and a retry with a fresh context skips files that are
already current. Do not edit the file concurrently.

## Container Methods: Configuration

The override that sets one stored entry for a while is
[override.SetConfig](../../override/README.md#setconfig). `ceph.ConfigSetting`,
`ceph.ConfigEntry` and `Observer.Configuration` describe and read the database.

### Configuration

```golang
func (o *Observer) Configuration(ctx context.Context) ([]ceph.ConfigEntry, error)
```

Lists the exact entries stored in the central configuration database, including
masked entries. Each `ceph.ConfigEntry` has `Section`, `Mask`, `Name`, `Value`,
`Level` and `CanUpdateAtRuntime`. These are stored entries, not inherited or
effective values; use `ceph config show <daemon> <option>` for a daemon's
effective value. Entries can contain sensitive values.

### Debug level types

The runtime debug level window is the package function
[override.SetDebugLevel](../../override/README.md#setdebuglevel) and its
`*override.DebugLevelHandle`; the call and its restoration rules are documented
there. These types are in `ceph` because the failure manifest copies the
receipt:

- `ceph.DebugLevelSetting` has `Daemon` (such as `osd.0`), `Name` (`debug_ms` or
  `debug_osd`) and `Level` (`N` or `N/M`, from 0 to 99).
- `ceph.DebugLevelReceipt` is the JSON-serializable record of one window: daemon,
  option, generation, requested text, prior, applied and restored readbacks, the
  current `ceph.DebugLevelState`, the `Reason` of a not-applied window and every
  `ceph.DebugLevelTransition` with its time, readback and error.
- `ceph.DebugLevelState` is `DebugLevelPriorRead`, `DebugLevelApplyAttempted`,
  then `DebugLevelApplied` or `DebugLevelApplyUncertain`; a restoration records
  `DebugLevelRestoreAttempted`, then `DebugLevelRestored`, `DebugLevelConflict`,
  `DebugLevelTargetChanged` or `DebugLevelUnconfirmed`. `DebugLevelNotApplied`
  (`not-applied`) is a window that was refused before any native step.
- `ceph.DebugLevelReceipter` is the interface that `Finalization.TrackDebugLevel`
  takes; see [TerminateOnCleanup](#terminateoncleanup).

## Container Methods: MGR modules and services

### MGRModules

```golang
func (o *Observer) MGRModules(ctx context.Context) ([]ceph.MGRModuleState, error)
```

Lists configured and available manager modules sorted by name. Each
`ceph.MGRModuleState` combines configured membership with the active manager's
module metadata: `Name`, `Enabled`, `AlwaysOn`, `ForceDisabled`, `Available`
(metadata present), `CanRun` (meaningful only when `Available`) and `ErrorString`
(Ceph's dependency or load diagnostic). `Enabled` and `CanRun` do not prove that
a module's commands are ready.

The module membership override that enables or disables one manager module is
[override.SetMGRModule](../../override/README.md#setmgrmodule). `Observer.MGRModules`,
`ceph.MGRModuleState` and `WaitMGRModuleReady` are documented here.

### WaitMGRModuleReady

```golang
func (c *Container) WaitMGRModuleReady(ctx context.Context, name string) error
```

Waits until a module is enabled and can run, and its read-only commands succeed.
Only `rbd_support` (task and snapshot schedule lists) and `volumes`
(`fs volume ls`) are supported; for other modules probe your intended command.
Success does not mean that queued worker tasks have completed.

### ManagerServices

```golang
func (o *Observer) ManagerServices(ctx context.Context) (map[string]string, error)
```

Returns the service URLs that the active manager advertises in the MGR map,
keyed by module name (for example `prometheus` or `dashboard`). A module appears
only after it has started its listener; after a failover the URLs are the new
active manager's. URLs contain the address the module bound: a container address
reachable from `WithClient` containers in bridge mode, or the configured address
in host mode. They are not mapped Docker host ports. An empty map means no module
advertises a service. See
[Scrape the MGR Prometheus exporter](ceph-recipes.md#scrape-the-mgr-prometheus-exporter).

### ManagerMetrics

```golang
func (o *Observer) ManagerMetrics(ctx context.Context) (ceph.ManagerMetrics, error)
```

Scrapes the active manager's `prometheus` module once and returns its
samples parsed from the Prometheus text exposition format. It never enables
the module: enable it with [override.SetMGRModule](../../override/README.md#setmgrmodule) and wait
until [ManagerServices](#managerservices) lists `prometheus`; until then the
call fails with `ErrInvalidRequest`. The scrape runs with `python3` in the
control container, at the address that `ManagerServices` advertises, and is
bounded to 30 seconds and 32 MiB. `TestManagerMetrics` checks it in bridge
mode; in host mode the module first needs its own address and port per
manager, as the recipe below shows. A failed fetch is a `*NativeFailure`, and a
standby's empty page or text that does not parse is an error.

| `ceph.ManagerMetrics` field or method | Meaning |
| --- | --- |
| `URL` | The scraped endpoint, the active manager's `…/metrics`. |
| `Types`, `Help` | Each metric family's `# TYPE` and `# HELP` text. |
| `Samples` | Every `ceph.MetricSample` (`Name`, `Labels`, `Value`, `Timestamp` in milliseconds or 0), in the module's order. `Value` may be `NaN` or ±Inf. |
| `Select(name, labels)` | The samples of `name` whose labels include all of `labels`; nil `labels` selects every sample of `name`. |
| `Value(name, labels)` | The value of the one sample that `Select` finds; none or several is an error. |

The module reports cluster-level metrics: pools (`ceph_pool_wr`,
`ceph_pool_objects`, `ceph_pool_stored`), OSD state (`ceph_osd_up`,
`ceph_osd_in`, `ceph_osd_metadata`), health and the MGRs. Since Reef it leaves
out each daemon's perf counters, such as `ceph_osd_op_w`, by default; the
module's `mgr/prometheus/exclude_perf_counters` option set to `false` adds
them back (see the
[Ceph documentation](https://docs.ceph.com/en/tentacle/mgr/prometheus/#ceph-daemon-performance-counters-metrics);
the module's tests do not cover that setting). Each daemon's own counters are
also in `osd.AdminSocket(ctx, "perf", "dump")`.

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
metrics, err := observer.ManagerMetrics(ctx)
if err != nil {
	t.Fatal(err)
}
writes, err := metrics.Value("ceph_pool_wr", map[string]string{"pool_id": "2"})
```

The pool counters come from the MGR's PG statistics, which OSDs report every
few seconds: poll until the expected value appears.

## Container Methods: Health and status

### Status

```golang
func (o *Observer) Status(ctx context.Context) (ceph.Status, error)
```

Runs `ceph status --format json` and decodes the subset used for readiness:
`FSID`, `Health.Status`, `MgrMap.Available`, `OSDMap.NumOSDs`/`NumUpOSDs`/
`NumInOSDs`, and `PGMap.NumPGs` with `PGsByState`. For other fields, run
`cluster.Ceph(ctx, "status", "--format", "json")` and decode what you need.

### HealthDetails

```golang
func (o *Observer) HealthDetails(ctx context.Context) (ceph.HealthSnapshot, error)
```

Reads `ceph health detail` from one control container, checking the cluster's
original FSID before and after. It works before any OSD or manager exists, makes
no changes and does not retry. On any failure, including cancellation after the
query, it returns a zero snapshot and an error without native output.

| Value | Meaning |
| --- | --- |
| `HealthSnapshot.FSID` | The cluster's original FSID. |
| `HealthSnapshot.Status` | `HEALTH_OK`, `HEALTH_WARN` or `HEALTH_ERR` for the worst unmuted check. |
| `HealthSnapshot.Checks` | Map keyed by the native, case-sensitive code (`OSD_DOWN`, `MGR_DOWN`, ...), including codes unknown to this module. |
| `HealthCheck.Severity` | The check's own severity, kept when it is muted. |
| `HealthCheck.Summary`, `Details` | Native summary and detail messages; Ceph may truncate details. |
| `HealthCheck.Count` | Native signed count; need not equal the number of details. |
| `HealthCheck.Muted` | Whether a stored mute covers this check. |
| `HealthSnapshot.Mutes` | Stored mutes (`HealthMute`), whether or not their check is present. |
| `HealthMute.Code`, `Summary`, `Count`, `Sticky` | Native values kept by the mute. |
| `HealthMute.ExpiresAt` | Ceph's TTL timestamp string, empty without TTL. Not compared with your clock. |

`HEALTH_OK` can include muted warnings, so assert on the code and its `Muted`
flag. A sticky mute stays after its check clears, and an expired or cleared
non-sticky mute can remain visible until the next MON tick. Messages can contain
pool names, paths or other application data; choose what you log.

Ceph raises many checks only after a delay, so poll with a deadline:

```golang
func waitForHealthCheck(ctx context.Context, cluster *ceph.Container, code string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	observer, err := ceph.Observe(cluster)
	if err != nil {
		return err
	}
	for {
		health, err := observer.HealthDetails(ctx)
		if err == nil {
			if check, ok := health.Checks[code]; ok && !check.Muted {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("health check %s not reported: %w", code, errors.Join(ctx.Err(), err))
		case <-time.After(time.Second):
		}
	}
}
```

### WaitForClean

```golang
func (c *Container) WaitForClean(ctx context.Context) error
```

Waits until a manager is available, the cluster's OSD count equals the owned OSD
count with all of them up and in, at least one PG exists, and every PG is exactly
`active+clean`. `HEALTH_WARN` alone does not block it. Call it after creating
pools and after topology changes, before client I/O.

- It never succeeds with an intentionally stopped or out OSD; use
  [WaitForPGClean](#waitforpgclean) then.
- PG states with extra flags (`active+clean+scrubbing`,
  `active+clean+inconsistent`) do not count as clean.
- OSDs registered outside the fixture make the count differ.
- After `Pool.SetPGCount` or `Pool.SetPlacement` it can pass before the new PG set
  is reported; see [Pool.WaitForPGCount](#poolwaitforpgcount) and
  [Pool.PGs](#poolpgs).
- A pool whose replica count exceeds the OSD count never becomes clean (for
  example size 2 with one OSD).

### WaitForPGClean

```golang
func (c *Container) WaitForPGClean(ctx context.Context) error
```

Waits until every existing PG is exactly `active+clean`, without checking OSD
topology, so it works while an OSD is deliberately down or out. It requires at
least one PG and an available manager. It is a recovery barrier, not a statement
about health or daemon availability.

## Container Methods: Messenger mode

The override that changes the policy of a running cluster is
[override.SetMessengerPolicy](../../override/README.md#setmessengerpolicy).
`ceph.MessengerPolicy` and `ceph.MessengerConnectionMode` are in `ceph` because `NetworkConfig.WithMessengerPolicy` takes them
at `Run`.

## Container Methods: Object watchers and client fencing

### ObjectWatchers

```golang
func (o *Observer) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ceph.ObjectWatcher, error)
```

Lists the clients watching one RADOS object (`rados listwatchers`), including
clients outside the fixture. `namespace` is empty for the default namespace. Each
`ceph.ObjectWatcher` has `Address` (`IP:port/nonce`, the same form as
[BlocklistEntries](#blocklistentries)), `ClientID` and `Cookie`. A client that
stops answering keeps its watch until the OSD's `osd_client_watch_timeout`
expires (30 seconds by default); until then a notify on the object waits for it
and times out. Use this to assert that a stuck watcher is still registered or has
been removed. It reads native state only.

### BlocklistEntries

```golang
func (o *Observer) BlocklistEntries(ctx context.Context) ([]ceph.BlocklistEntry, error)
```

Lists the OSDMap blocklist: exact client addresses and CIDR ranges, including
entries created by other tools or by Ceph itself (for example when CephFS or RBD
evicts a client). Each `ceph.BlocklistEntry` has `Address`, `Range` (true for a
CIDR range) and `Until` (the MON's absolute expiration time). Listing grants no
ownership to remove an entry.

The blocklist entry that fences a client session is added by
[override.AddBlocklist](../../override/README.md#addblocklist). `Observer.BlocklistEntries`
and `ceph.BlocklistEntry` only read the OSDMap.

## Container Methods: Full ratios

### FullRatios

```golang
func (o *Observer) FullRatios(ctx context.Context) (ceph.FullRatioSnapshot, error)
```

Reads the OSDMap's global `nearfull`, `backfillfull` and `full` ratios, checking
the original FSID before and after. `ceph.FullRatioSnapshot` has `FSID`, `Epoch`
(the whole OSDMap epoch, which unrelated changes also advance) and `Ratios`
(`ceph.FullRatios` with `NearFull`, `BackfillFull` and `Full` as fractions of
capacity). Values are Ceph's float32 values, so do not compare them with
float64 literals for equality. Zero, equal or misordered native values are
reported as they are. These ratios are OSDMap policy, separate from central
configuration and pool quotas, and do not say whether any OSD has reported
fullness.

The override that lowers or raises these thresholds is
[override.SetFullRatios](../../override/README.md#setfullratios). `Observer.FullRatios`,
`ceph.FullRatios` and `ceph.FullRatioSnapshot` describe and read the OSDMap.
See [Fill the cluster with low full ratios](ceph-recipes.md#fill-the-cluster-with-low-full-ratios).

## Container Methods: Scrub inconsistencies

These methods scrub, inspect and repair PGs. A damaged replica to observe with
them comes from [fault.InjectObjectDataError](../../fault/README.md#injectobjectdataerror),
which makes it possible to test monitoring, client behaviour on an inconsistent
PG, or repair automation without corrupting a disk or stopping an OSD. Get an
object's PG and acting set with
`cluster.Ceph(ctx, "osd", "map", pool, object, "--format", "json")` (fields
`pgid`, `acting` and `acting_primary`).

### DeepScrubPG

```golang
func (c *Container) DeepScrubPG(ctx context.Context, pgid ceph.PGID) error
```

Requests a deep scrub of one PG and waits until it reports a deep-scrub stamp
newer than before the request and is no longer scrubbing. Inconsistencies are
reported, not repaired. The `noscrub` or `nodeep-scrub` flags, or an unavailable
primary, keep the scrub from starting, and the wait then fails at the deadline.

### PGInconsistencies

```golang
func (o *Observer) PGInconsistencies(ctx context.Context, pgid ceph.PGID) ([]ceph.InconsistentObject, error)
```

Lists the objects that the last deep scrub of a PG found inconsistent
(`rados list-inconsistent-obj`). It does not start a scrub; an empty result means
that scrub found nothing. A PG whose results are no longer available returns an
error. Each `ceph.InconsistentObject` has `Name`, `Namespace`, object-level
`Errors`, `UnionShardErrors` (for example `read_error` or `data_digest_mismatch`)
and `Shards`. Each `ceph.InconsistentShard` has `OSD`, `Shard` (`-1` for
replicated pools), `Primary` and `Errors`.

### RepairPG

```golang
func (c *Container) RepairPG(ctx context.Context, pgid ceph.PGID) error
```

Requests a repair of one PG and waits until it reports a newer deep-scrub stamp
and is no longer inconsistent. Repair rewrites bad replicas from an authoritative
copy; an object without one cannot be repaired, and the call then fails with the
PG still inconsistent.

## Container Methods: Fault injection

Daemon handles (from `Monitors`, `Managers` and `OSDs`, and the MDS and RGW handles
of the service packages) support `Stop` and `Start`: the process ends and peers
see the connection close. The dedicated operations in [`fault`](../../fault/README.md) add other failure shapes. For
Messenger-level delays and dropped messages (`ms_blackhole_*`,
`ms_inject_delay_*` through `override.SetConfig`), OSD flags, client blocklisting
and full ratios, see [Fault injection](fault-injection.md).

### Container pause faults

[`fault.Pause`](../../fault/README.md#pause) freezes a daemon or client
with the existing cluster's ownership and cleanup lifetime. Its
`*fault.PauseLease` handle resumes the process with `Resume`.

### Object read errors

[`fault.InjectObjectDataError`](../../fault/README.md#injectobjectdataerror) marks one
replica of an object so that scrub and recovery see a read error. It is a
one-shot injection with no handle; observe it with
[DeepScrubPG](#deepscrubpg) and [PGInconsistencies](#pginconsistencies) and
rewrite the replica with [RepairPG](#repairpg).

### Network faults

Network fault functions and handles are in [`fault`](../../fault/README.md). Use
`fault.InterruptNetwork(ctx, cluster, target, plane)` and
`fault.ApplyNetworkProfile(ctx, cluster, target, delay)` with the existing
cluster; its `Terminate` restores cluster-bound faults.

## Container Methods: Networking

See [Host networking](host-network.md) for the networking modes in detail.

### DaemonAddresses

```golang
func (o *Observer) DaemonAddresses(ctx context.Context) (ceph.DaemonAddresses, error)
```

Reads the names and advertised addresses of the cluster's MONs, MGRs and OSDs
from the native maps. These are the addresses a client learns from the cluster,
so a test of a client implementation can compare what it decoded or connected
to with what Ceph advertises.

| Field | Source and meaning |
| --- | --- |
| `MonMapEpoch`, `Monitors` | monmap epoch; every member in rank order with its name and public address vector |
| `MgrMapEpoch`, `ActiveManager`, `ManagerAvailable`, `StandbyManagers` | MGRMap epoch; the active MGR's name and address vector (nil without an active MGR), whether it is available, and the standby names |
| `OSDMapEpoch`, `OSDs` | OSDMap epoch; every OSD in ID order with `Up`, `In` and its `Public`, `Cluster`, `HeartbeatFront` and `HeartbeatBack` address vectors |

Each address is a `ceph.EntityAddress` with `Type` (`v2`, `v1` or `any`),
`Addr` as Ceph prints it (`172.20.0.3:6800`, `[fd00::3]:6800`) and `Nonce`, the
instance nonce that MONs leave at 0. `ceph.DaemonAddress` pairs a MON or MGR
name with its vector, and `ceph.OSDAddress` holds one OSD's entry.

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.DaemonAddresses(ctx)
for _, mon := range addresses.Monitors {
	fmt.Println(mon.Name, mon.Addrs) // a [{v2 172.20.0.2:3300 0} {v1 172.20.0.2:6789 0}]
}
```

- In bridge mode the addresses are the daemon containers' addresses on the
  fixture network; in host mode they are `ClusterFacts.PublicAddress` with
  the allocated ports.
- Standby MGRs advertise no address in the MGRMap. A MGR that has just become
  active advertises its address vector only once it is available, so wait for
  `ManagerAvailable` after a failover.
- An OSD that is down keeps the addresses of its last boot; one that never
  booted has empty vectors.
- The three maps are read one after another, not as one snapshot; compare the
  epochs when the order matters.

## Container Methods: Admin sockets

### AdminSocket

```golang
func (o *OSDContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error)
func (m *MonitorContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error)
func (m *ManagerContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error)
```

Runs one command on the admin socket of the MON, MGR or OSD that the handle
names, which the cluster must still own, and returns its output. `args` are the
command words. `help` lists a daemon's commands. The three handle types cover
exactly the daemons that have an admin socket here; there is no method for an
MDS, and a handle of a removed daemon returns an error.

The admin socket bypasses the Messenger and Cephx: the command reaches the
daemon through a local Unix socket. Use it to check what a daemon has applied
independently of the client under test, for example:

| Command | Shows |
| --- | --- |
| `version` | the daemon's build version, release name and release type |
| `status` (OSD) | `newest_map` and `oldest_map`: the OSDMap epochs the OSD holds, as JSON strings |
| `mon_status` (MON) | the MON's own name, rank, state and monmap, including `auth_epoch` on Tentacle |
| `config get <option>` | the value the running daemon uses, after local files and the central database |
| `perf dump` | the daemon's performance counters |

```golang
// Wait until osd.0 has applied the OSDMap epoch the cluster reports.
out, err := osd.AdminSocket(ctx, "status") // osd is the *ceph.OSDContainer of osd.0
var status struct {
	NewestMap string `json:"newest_map"`
}
err = json.Unmarshal(out, &status)
```

Every MON, MGR, OSD and control container shares one run directory, a Docker
volume mounted at `/var/run/ceph`, and the command runs in the control
container. It therefore works whatever CLI the daemon's own image contains. The
volume belongs to the cluster and `Terminate` removes it.

- The daemon must be running. A stopped or removed daemon returns an error. A
  daemon that has just started can answer before it registers all of its
  commands (`no valid command found`); poll with a deadline.
- A paused daemon (see [fault.Pause](../../fault/README.md#pause)) accepts the
  connection but does not answer, so the call waits until the context ends.
- A nonzero exit returns the command's output together with an error.
- The call is bounded by the context and by
  [WithStartupTimeout](ceph-configuration.md#withstartuptimeout), and runs no command for a daemon
  the cluster does not own. When the context ends first, the call stops its
  command in the control container before it returns, including a command that
  Docker has accepted but not yet started, so canceled calls leave no process
  behind and other calls keep running. An effect that the daemon has already
  applied stays.
- MDS and RGW daemons are not included; reach them with `ceph tell` (see
  [Running native commands](native-commands.md#daemon-admin-sockets)).

## Container Methods: Diagnostics

### CollectDiagnostics

```golang
func (c *Container) CollectDiagnostics(ctx context.Context, config ceph.DiagnosticsConfig) (*ceph.DiagnosticsReport, error)
```

Collects a bounded, read-only snapshot for failure analysis: the state and
selected metadata of every tracked MON, MGR, OSD, MDS, RGW and control container,
network metadata, log tails, and fixed Ceph queries (`version`, `status`,
`health`, `pgs`, `quorum`, `monmap`, `mgrmap`, `osdmap`, `fsmap`). It works on
partially bootstrapped clusters and on stopped or removed containers. It never
starts, stops or reconfigures anything, and it never collects files, environment
variables, keyrings, the auth database, mount sources or arbitrary commands.

[TerminateOnCleanup](#terminateoncleanup) with `Archive` calls it, and
[CollectCrashMetadata](#collectcrashmetadata), when a test fails, and writes
both reports with a manifest. Otherwise it is not called automatically. Call
it before cleanup, with its own short context (the test context may already be
canceled):

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})
	if report != nil {
		// Save the report even when err is set; it keeps the successful parts.
		data, _ := json.MarshalIndent(report, "", "  ")
		name := strings.ReplaceAll(t.Name(), "/", "_") + "-ceph-diagnostics.json"
		path := filepath.Join(os.TempDir(), name) // or your CI artifact directory
		if writeErr := os.WriteFile(path, data, 0o644); writeErr == nil {
			t.Log("Ceph diagnostics saved to", path)
		}
	}
	if err != nil {
		t.Log("diagnostics incomplete:", err)
	}
})
```

Register this cleanup after the cluster's, so it runs before termination.

`ceph.DiagnosticsConfig` fields (zero selects the default):

| Field | Default | Limit | Meaning |
| --- | --- | --- | --- |
| `Timeout` | 1 minute | 5 minutes | Whole collection; the caller's deadline also applies. |
| `OperationTimeout` | 10 seconds | 1 minute | Each query. |
| `MaxOutputBytes` | 64 KiB | 1 MiB | Each artifact's `Data` and `Error`. |
| `LogTail` | 1000 lines | 10000 | Log lines per container. |
| `Concurrency` | 4 | 8 | Collection workers. |
| `MaxContainers` | 128 | 128 | Distinct containers; extra ones are recorded as omitted. |
| `AdditionalContainers` | none | 64 | `ceph.DiagnosticsContainer` values (`Role`, `Name`, `Container`) for clients or mirror daemons you own; they are only inspected. |
| `RedactValues` | none | 128 values of up to 4096 bytes | Extra secrets to mask. |

Invalid limits are rejected before collection, without a report.

`ceph.DiagnosticsReport` is JSON-serializable (snake_case field names) with
`SchemaVersion` (1), `StartedAt`, `FinishedAt`, `HostNetwork`, `Closed` (the
cluster was already terminated), `Complete` and `Artifacts`. Each
`ceph.DiagnosticArtifact` has `Kind` (`inventory`, `container-inspect`,
`container-logs`, `network-inspect` or `ceph`), `Name`, `Role`, `ContainerID`,
`Data` (text; JSON for untruncated inspect results), `Truncated` and `Error`.

- If some queries fail, the report and an aggregate error are both returned, so
  save the report before checking the error. Truncation alone is not an error.
- `Complete` means that every observation succeeded without truncation, not that
  Ceph is healthy.
- Observations are not taken at one atomic moment.
- A `ceph` artifact holds the query's stdout followed by its stderr. A query
  that fails or times out keeps the output it produced.
- When `ctx` ends, every running query is stopped inside the control container
  before `CollectDiagnostics` returns, so a canceled collection leaves no Ceph
  CLI behind.
- Known admin and RGW secrets, every client key the cluster generated (also
  keys that [ClientConfig.ReplaceKey](#clientconfigreplacekey) replaced or only attempted),
  `RedactValues` and named secret patterns are masked, but redaction is best
  effort; review arbitrary application logs before sharing a report.

### CollectCrashMetadata

```golang
func (c *Container) CollectCrashMetadata(ctx context.Context, config ceph.CrashMetadataConfig) (*ceph.CrashMetadataReport, error)
```

Collects the crash reports that Ceph daemons wrote, for failure analysis. When a
daemon dies on a fatal signal or a failed assertion, Ceph writes a report
directory into its crash directory, `ceph.CrashDir` (`/var/lib/ceph/crash`).
The directory holds a `meta` JSON file with the daemon, version, time,
assertion and backtrace, a `log` file of recent events, and a `done` marker.
`CollectCrashMetadata` reads only the `meta` files, of every MON, MGR, OSD, MDS
and RGW container and of `config.AdditionalContainers`. It is separate from
`CollectDiagnostics` and is not called automatically.

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := cluster.CollectCrashMetadata(ctx, ceph.CrashMetadataConfig{})
	if report != nil {
		data, _ := json.MarshalIndent(report, "", "  ")
		t.Logf("Ceph crash metadata:\n%s", data)
	}
	if err != nil {
		t.Log("crash metadata incomplete:", err)
	}
})
```

- The files come through Docker's archive API, so stopped containers work,
  and so do a partially bootstrapped or terminated cluster. Nothing is started,
  stopped, reconfigured or executed in the containers, and nothing is extracted
  to disk. Caller-owned containers are only read.
- Only regular files named `meta` in a report directory, or in `posted/`, are
  read. Crash logs, process memory dumps and every other file are skipped and
  counted in `IgnoredEntries`. Links, paths outside the directory, files over
  the size limit and files that are not one JSON object are listed in
  `Omitted` with the reason.
- Each record keeps the known Ceph crash fields in `Fields`: `crash_id`,
  `timestamp`, `process_name`, `entity_name`, `ceph_version`, the `utsname_*` and
  `os_*` fields, `backtrace`, the `assert_*` and `io_error*` fields,
  `archived` and `stack_sig`. Other fields are named in `OmittedFields` without
  their values. Strings are redacted like `CollectDiagnostics`, but host names
  and paths stay; review a report before sharing it.
- Each `ceph.CrashRecord` has `CrashID`, `Path`, the convenience fields
  `Entity`, `Process`, `CephVersion` and `Timestamp`, `Done`, which reports
  Ceph's `done` marker that completes a report, `Posted` for a report in
  `posted/`, `Fields` and `OmittedFields`. Each `ceph.CrashOmission` has the
  file's `Path` and the `Reason`.

`ceph.CrashMetadataConfig` fields (zero selects the default):

| Field | Default | Limit | Meaning |
| --- | --- | --- | --- |
| `Timeout` | 1 minute | 5 minutes | Whole collection; the caller's deadline also applies. |
| `OperationTimeout` | 20 seconds | 1 minute | Each container. |
| `MaxArchiveBytes` | 16 MiB | 256 MiB | Archive bytes read per container, crash logs included. |
| `MaxEntries` | 4096 | 65536 | Archive entries per container. |
| `MaxRecordBytes` | 64 KiB | 1 MiB | Each `meta` file. |
| `MaxRecords` | 64 | 1024 | Records per container; later ones are listed in `Omitted`. |
| `AdditionalContainers` | none | 64 | Caller-owned containers, such as mirror daemons, to read as well. Each one is read whatever its `Role` and `Name`; the cluster's separate control container, which runs no daemon, is read only when it is listed here. |
| `RedactValues` | none | 128 values of up to 4096 bytes | Extra secrets to mask. |

Each `ceph.CrashMetadataContainer` in `ceph.CrashMetadataReport.Containers` has
the container's role, name, ID, image ID and Docker state (`State`,
`ExitCode`, `OOMKilled`, `StartedAt`, `FinishedAt`), for correlation with the
records. Its `Outcome` (a `ceph.CrashOutcome`) is one of:

| Outcome | Meaning |
| --- | --- |
| `CrashRecordsFound` | The directory was read and held at least one accepted record. |
| `CrashEntriesRejected` | The directory was read, but every `meta` file was rejected; see `Omitted`. |
| `CrashDirectoryEmpty` | The directory was read and held no report. This does not prove that the daemon never crashed. |
| `CrashDirectoryAbsent` | The container has no crash directory. |
| `CrashContainerUnavailable` | The container was never created, or was removed with its reports. |
| `CrashReadFailed` | Docker or the container implementation failed, for example after the context ended; see `Error`. |
| `CrashArchiveIncomplete` | The archive was malformed, cut off or over a limit; `Records` holds what was read before. |

- A failed or incomplete container makes the call return an error, with the
  report and every other container's records. Save the report before checking
  the error.
- `Complete` means that every crash directory was read in full and every
  `meta` file in it was accepted, or that the directory is absent. It does not
  mean that every crash in the cluster's history was captured.
- A container's exit code alone does not tell a crash. A daemon that is PID 1
  of its container, as in this cluster, exits 1 after Ceph wrote its report,
  not with the signal's code.

## Other module functions and types

### FactsOf

```golang
func FactsOf(c *Container) (ClusterFacts, error)
```

Returns an independent snapshot of the static facts of a cluster: address
family, role images, messenger mode, network names, host-network
use, advertised public address, whether a separate cluster network exists, and
the Ceph version. A nil cluster returns an error that matches
`ErrInvalidRequest` and the zero `ClusterFacts`. A terminated cluster still
yields a snapshot, because the values come from the recorded configuration and
identity and need no lock, command or Docker call. `Images` and `CephVersion`
describe the cluster's own settings and first MON only; a daemon started with
[DaemonOptions.WithImage](#per-daemon-images) is not reflected, so read its handle's
`Image` and `Version`.

### ClusterFacts

```golang
type ClusterFacts struct {
	AddressFamily             AddressFamily
	Images                    ImageSet
	MessengerMode             MessengerMode
	NetworkName               string
	UsesHostNetwork           bool
	PublicAddress             string
	ClusterNetworkName        string
	HasSeparateClusterNetwork bool
	CephVersion               string
}
```

A copy of the recorded information, not of the cluster. It holds no cluster
handle, resource or cleanup state, so it cannot control or reactivate the
cluster. Changing a snapshot changes neither the cluster nor another snapshot,
and a later cluster change does not update a snapshot already obtained. The
values describe the recorded configuration. They do not show that the Docker
resources still exist, are reachable or run, and they are not a fresh native
observation. `FactsOf` is the only way to read these facts: `Container` has no
accessor for them. The essential image is `Images.Essential`.

What `FactsOf` reads is the cluster's recorded state: the run settings, the
bridge network records and the Ceph version recorded when the monitor started.
`Run` writes them before it returns the cluster, and `Terminate` does not clear
or rewrite them, so a snapshot can be taken from a terminated cluster, or while
`Terminate` runs, without a lock, a command or a Docker call. Termination
alone never fails the capture, and the internal closed flag is not consulted
because it is set before teardown finishes and is no evidence of cleanup. A
fact that was never recorded stays empty: a cluster returned by `Run` together
with an error can have an empty `CephVersion` or network name, and the snapshot
does not replace them with a default. An empty value therefore does not
distinguish "not recorded" from "recorded as empty", and it does not report
whether the resources still exist.

### OSDFlag

```golang
type OSDFlag string
```

A global OSDMap flag token. The constants name the eleven flags that
[override.SetOSDFlag](../../override/README.md#setosdflag) can change: `OSDFlagNoUp` (`noup`),
`OSDFlagNoDown` (`nodown`), `OSDFlagNoOut` (`noout`), `OSDFlagNoIn` (`noin`),
`OSDFlagNoBackfill` (`nobackfill`), `OSDFlagNoRebalance` (`norebalance`),
`OSDFlagNoRecover` (`norecover`), `OSDFlagNoScrub` (`noscrub`),
`OSDFlagNoDeepScrub` (`nodeep-scrub`), `OSDFlagNoSnapTrim` (`nosnaptrim`) and
`OSDFlagNoAutoscale` (`noautoscale`). [OSDFlags](#osdflags) returns every flag
the cluster reports, including native compatibility flags that have no
constant, so the type stays open: a value outside the constants is
representable and `override.SetOSDFlag` refuses it at run time. `String()` returns
the token. An untyped string constant such as `"noout"` still converts to
`OSDFlag`; a value held in a `string` variable needs an explicit conversion.

### PGID

```golang
type PGID string
func ParsePGID(s string) (PGID, error)
func (p PGID) String() string
```

A placement group ID in the form `<pool>.<hex>`: the decimal pool ID, a dot and
the lowercase hexadecimal PG number, for example `1.0` or `3.1f`.
`PGState.PGID` has this type, so a PG from [Pool.PGs](#poolpgs) passes straight
to [DeepScrubPG](#deepscrubpg), [RepairPG](#repairpg) and
[PGInconsistencies](#pginconsistencies). `ParsePGID` accepts and rejects
exactly what those three methods accept; an invalid string returns an error
that matches `ErrInvalidRequest` and the empty `PGID`. An untyped string constant
converts without validation, so the three methods check the form again.

### ConnectionConfig type

```golang
type ConnectionConfig struct {
	Conf    []byte // ceph.conf
	Keyring []byte
}
```

The result of [Container.ConnectionConfig](#connectionconfig) and
[ClientConfig.ConnectionConfig](#clientconfig). The named fields replace two
positional byte slices, so a swapped assignment of configuration and keyring
no longer compiles. Both methods return independent copies of the bytes and
the zero value with an error. The keyring is a credential; the type does not
redact it.

### WithIdleEntrypoint

```golang
func WithIdleEntrypoint() testcontainers.CustomizeRequestOption
```

Keeps a client or helper container running with `sleep infinity` under Docker's
init process, so you can run commands in it with `Exec`. As PID 1, a bare `sleep`
ignores `SIGTERM` and Docker would wait for the full stop timeout (10 seconds) on
every termination; init forwards the signal, so termination is immediate. Init
also reaps background processes started through `Exec`, such as a test server.
Pass it to `testcontainers.Run`, not to `ceph.Run`.

```golang
facts, err := ceph.FactsOf(cluster)
client, err := testcontainers.Run(ctx, facts.Images.Client,
	cluster.WithClient(),
	ceph.WithIdleEntrypoint(),
)
```

### Standalone network faults

[`fault.InterruptLink`](../../fault/README.md#standalone-bridge-functions) and
[`fault.ApplyLinkProfile`](../../fault/README.md#standalone-bridge-functions) operate on
caller-owned bridges. The caller restores their handles before removing the
container or bridge.

### ErrHostNetworkUnavailable

```golang
var ErrHostNetworkUnavailable error
```

Wrapped by the error that `Run` returns in host mode when the engine does not run
Linux containers, the [NetworkConfig.WithHostAddress](ceph-configuration.md#networkconfigwithhostaddress) address is not assigned
on the Docker host, or the test process cannot reach the first MON's advertised
endpoint. `Run` returns it before creating managers and OSDs; later MON and RGW
port reservations can also return it. The error message explains the likely
cause for the detected Docker environment. It belongs to
[ErrEnvironmentUnavailable](#error-categories).

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithHostNetwork())
testcontainers.CleanupContainer(t, cluster)
if errors.Is(err, ceph.ErrHostNetworkUnavailable) {
	t.Skip("host networking is not usable here:", err)
}
if err != nil {
	t.Fatal(err)
}
```

### Error categories

```golang
var (
	ErrInvalidRequest         error
	ErrUnsupported            error
	ErrImageCapability        error
	ErrEnvironmentUnavailable error
	ErrNativeFailure          error
)
```

Every package of the module classifies its failures with these categories, so
that a test can tell a refused request from a missing feature, image tool or
engine capability, and from a native command that failed. Match them with
`errors.Is`; an error joined from several failures can match several.

| Category | Meaning |
| --- | --- |
| `ErrInvalidRequest` | Refused before any change: malformed arguments, or an operation that the state of the cluster or of the selected resource forbids, such as a terminated cluster, an unowned container or an override that is already held. |
| `ErrUnsupported` | The Ceph of the relevant target lacks the feature, judged from that target's tools or from a documented limit of its release, or, for `AddressDualStack`, from the addresses its daemons advertise after startup. `*OperationError` names the target, its version and the feature. |
| `ErrImageCapability` | The image lacks an executable or runtime that the operation needs, or an optional extension was not selected or does not meet its contract. |
| `ErrEnvironmentUnavailable` | The Docker engine, host, kernel or container permissions cannot provide what the operation needs, although the image may be valid. |
| `ErrNativeFailure` | A native command ran and failed. `*NativeFailure` carries its status. |

Context errors, `ErrNativeCommandIncomplete`, `ErrNativeInputClosed` and
`*NativeCommandUnstoppedError` keep their own contracts and belong to no
category; a call that `ctx` ended is never classified by what it could not
finish. An error that matches no category is a failed check of the fixture's
own, such as native output that it could not parse, or a failure whose
outcome is unknown, such as a container that disappeared after a native
command was sent to it. Only `ErrInvalidRequest` promises that nothing changed. See [Classified errors](errors.md).

```golang
_, err := cluster.CreateClient(ctx, "modern", ceph.ClientCaps{Mon: "allow r"},
	ceph.ClientOptions{}.WithKeyType(ceph.CipherAES256K))
var unsupported *ceph.OperationError
if errors.As(err, &unsupported) && unsupported.Kind == ceph.ErrUnsupported {
	t.Logf("Ceph %s lacks %s", unsupported.Version, unsupported.Capability)
}
```

### OperationError

```golang
type OperationError struct {
	Kind       error
	Op         string
	Target     string
	Version    string
	Image      string
	Capability string
	Err        error
}
func (e *OperationError) Error() string
func (e *OperationError) Is(target error) bool
func (e *OperationError) Unwrap() error
```

A classified failure with the details that identify it. `Kind` is one of the
[error categories](#error-categories), and `Is` matches it. `Op` names the
operation when the cause's text does not, `Target` the container or cluster,
such as `control` (the control container), `destination cluster` or `zone source`, `Version` the Ceph
release observed on it, `Image` its immutable image ID, and `Capability` the
missing feature, such as `ceph-authtool --key-type` or `python3`. Unset fields
were not known or do not apply. The text is `Op`, the cause, and `Target`,
`Version` and `Image` in parentheses; it never includes command arguments,
raw output or credentials. Service packages return it too.

### NativeFailure

```golang
type NativeFailure struct {
	Program  string
	ExitCode int
	Errno    string
	Reason   string
}
const MaxNativeFailureReason = 512
func (f *NativeFailure) Error() string
func (f *NativeFailure) Is(target error) bool
```

The status of a module command that ran and exited nonzero, such as one of
[Ceph](../../ceph/README.md#ceph) or of the rbd and radosgw-admin helpers of the service packages.
`Program` is the program alone, never its arguments. `Errno` is the error
code that the tool printed, from the `Error ENOENT:` that starts a line of
Ceph or the last `(2) No such file or directory` of a line of the other tools,
which print it after the names of the request; it is empty when the tool
printed none and is never guessed from the exit code. `Reason` is the
tool's own explanation, its error output without Ceph's log lines joined with
`; `, at most `MaxNativeFailureReason` bytes, with
keys and the secrets that [RedactSecrets](#redactsecrets) recognizes replaced;
it can name resources of the request, such as a pool. Errors whose text hides
native output keep a copy without `Reason`.

`errors.Is` matches it with `ErrNativeFailure`, with `fs.ErrNotExist` for
`ENOENT`, `fs.ErrPermission` for `EACCES` and `EPERM`, and `fs.ErrExist` for
`EEXIST`. [RunNativeCommand](#runnativecommand) returns a nonzero exit as its
result instead.

```golang
_, err := cluster.Ceph(ctx, "osd", "pool", "get", "missing", "size")
var failure *ceph.NativeFailure
if errors.As(err, &failure) {
	t.Logf("%s exited %d with %s: %s", failure.Program, failure.ExitCode, failure.Errno, failure.Reason)
}
if !errors.Is(err, fs.ErrNotExist) {
	t.Fatalf("a missing pool returned %v", err)
}
```

### Option

```golang
type Option func(*options) error
func (Option) Customize(*testcontainers.GenericContainerRequest) error
```

The type of every module option (the value-group carriers such as `WithClusterConfig`,
the execution functions and the service package options). It implements `testcontainers.ContainerCustomizer`, so it can be mixed
with generic options in `Run`. `Run` reads it as cluster configuration; its
`Customize` method does nothing, so passing it to another container has no
effect.

### MessengerMode

```golang
type MessengerMode int

const (
	MessengerDefault MessengerMode = iota
	MessengerV2Secure
	MessengerV2Only
)
```

The bootstrap protocols and connection policy for [NetworkConfig.WithMessengerMode](ceph-configuration.md#networkconfigwithmessengermode).
Unknown values are rejected by `Run`.

### NetworkPlane

```golang
type NetworkPlane string

const (
	PublicNetworkPlane  NetworkPlane = "public"
	ClusterNetworkPlane NetworkPlane = "cluster"
)
```

Selects the client/public network or the OSD replication network for
[fault.InterruptNetwork](../../fault/README.md#interruptnetwork).

### Daemon handles

| Type | Fields and methods | Notes |
| --- | --- | --- |
| `MonitorContainer` | embeds `testcontainers.Container`; `DaemonName`; `Remove(ctx)`; `AdminSocket(ctx, args...)` | `Stop`/`Start` keep the store. See [Monitors](#monitors). |
| `ManagerContainer` | embeds `testcontainers.Container`; `DaemonName`; `Remove(ctx)`; `AdminSocket(ctx, args...)`; `RotateKey(ctx)` | Active or standby. See [Managers](#managers). |
| `OSDContainer` | embeds `testcontainers.Container`; `ID`; `Placement() OSDConfig`; `Remove(ctx)`; `SetIn(ctx, in)`; `Detach(ctx)`; `AdminSocket(ctx, args...)`; `RotateKey(ctx)` | Backed by a sparse BlueStore file. `Placement` returns a copy of the resolved CRUSH location. See [OSDs](#osds). |
| `DetachedOSD` | `ID() int`; `UUID() string`; `ClusterFSID() string`; `Placement() OSDConfig`; `Reopen(ctx)` | A retained store and registration without a container. See [OSDContainer.Detach](#osdcontainerdetach) and [DetachedOSD.Reopen](#detachedosdreopen). |

The cluster owns these containers. Do not terminate them individually; use
`MonitorContainer.Remove`, `ManagerContainer.Remove` or `OSDContainer.Remove`.

### ClientConfig

`*ceph.ClientConfig` describes an identity created by [CreateClient](#createclient).
The cluster owns the auth database; containers you attach with it are yours.

| Method | Returns |
| --- | --- |
| `Name() string` | The full entity, for example `client.reader`. |
| `User() string` | The ID without `client.`, for APIs such as librados `rados_create` and go-ceph `rados.NewConnWithUser`. |
| `KeyringPath() string` | The keyring path used inside attached containers, `/etc/ceph/ceph.client.<id>.keyring`. |
| `ConnectionConfig() (ceph.ConnectionConfig, error)` | A [ConnectionConfig](#connectionconfig-type) with independent copies of a `ceph.conf` (`Conf`) and the identity's keyring (`Keyring`) for a client you run yourself. The configuration is a snapshot taken at creation; after replacing monitors, update `mon_host` (see [MonitorBootstrapAddresses](#monitorbootstrapaddresses)). Revocation does not erase copies, and a descriptor that [ClientConfig.ReplaceKey](#clientconfigreplacekey) replaced keeps returning its old key. |
| `String() string`, `GoString() string` | A description that never includes credentials, safe for logs and `%v`/`%#v`. |
| `Capabilities(ctx)`, `UpdateCaps(ctx, caps)`, `ReplaceKey(ctx, opts...)`, `Revoke(ctx)`, `Delete(ctx)` | The operations on the identity. See [ClientConfig.Capabilities](#clientconfigcapabilities), [ClientConfig.UpdateCaps](#clientconfigupdatecaps), [ClientConfig.ReplaceKey](#clientconfigreplacekey), [ClientConfig.Revoke](#clientconfigrevoke) and [ClientConfig.Delete](#clientconfigdelete). |

### Restoration handles

| Type | Methods | Created by |
| --- | --- | --- |
| `override.ConfigHandle` | `Restore(ctx) error`; `String()`, `GoString()` for logging | [override.SetConfig](../../override/README.md#setconfig) |
| `override.OSDFlagHandle` | `Restore(ctx) error` | [override.SetOSDFlag](../../override/README.md#setosdflag) |
| `override.MGRModuleHandle` | `Restore(ctx) error` | [override.SetMGRModule](../../override/README.md#setmgrmodule) |
| `override.FullRatiosHandle` | `Restore(ctx) error` | [override.SetFullRatios](../../override/README.md#setfullratios) |
| `override.BlocklistHandle` | `Address() string`, `Restore(ctx) error` | [override.AddBlocklist](../../override/README.md#addblocklist) |
| `fault.PauseLease` | `ContainerID`; `Resume(ctx) error` | [fault.Pause](../../fault/README.md#pause) |
| `fault.NetworkInterruption` | `ContainerID`, `NetworkName`, `OriginalAddress`; `Restore(ctx) error` | [fault.InterruptNetwork](../../fault/README.md#interruptnetwork) (cluster-bound or standalone) |
| `ClientRevocationHandle` | `Name() string`, `Restore(ctx) error`; `String()`, `GoString()` for logging | [ClientConfig.Revoke](#clientconfigrevoke) |

See the [handle contract](#container-methods).

### Configuration and result types

| Type | Used by |
| --- | --- |
| `OSDConfig` | [ClusterConfig.WithInitialOSDs](ceph-configuration.md#clusterconfigwithinitialosds), [OSDOptions](#osdoptions), `OSDContainer.Placement` |
| `OSDState` | [OSDStates](#osdstates) |
| `PoolConfig`, `ErasureCodeConfig` | [WithPools](ceph-configuration.md#clusterconfigwithpools), [CreatePool](#createpool) |
| `Pool` | [CreatePool](#createpool), [Pool](#pool); its methods are [Status](#poolstatus), [Usage](#poolusage), [PGs](#poolpgs), [SetReplication](#poolsetreplication), [SetQuota](#poolsetquota), [SetPlacement](#poolsetplacement), [SetPGCount](#poolsetpgcount), [WaitForPGCount](#poolwaitforpgcount) and [Remove](#poolremove) |
| `PoolState`, `PoolQuota` | [Pool.Status](#poolstatus), [Pool.SetQuota](#poolsetquota) |
| `PoolPlacement` | [Pool.SetPlacement](#poolsetplacement) |
| `PoolUsageSnapshot` | [Pool.Usage](#poolusage) |
| `PoolPGSnapshot`, `PGState`, `PGStats` | [Pool.PGs](#poolpgs) |
| `ClientCaps` | [CreateClient](#createclient), [ClientConfig.Capabilities](#clientconfigcapabilities), [ClientConfig.UpdateCaps](#clientconfigupdatecaps) |
| `ClientOptions` | [CreateClient](#createclient), [ClientConfig.ReplaceKey](#clientconfigreplacekey) |
| `ConfigSetting` | [override.SetConfig](../../override/README.md#setconfig) |
| `ConfigEntry` | [Configuration](#configuration) |
| `MGRModuleState` | [MGRModules](#mgrmodules) |
| `Status` | [Status](#status) |
| `QuorumStatus` | [QuorumStatus](#quorumstatus) |
| `ManagerStatus` | [ManagerStatus](#managerstatus) |
| `HealthSnapshot`, `HealthCheck`, `HealthMute` | [HealthDetails](#healthdetails) |
| `ObjectWatcher` | [ObjectWatchers](#objectwatchers) |
| `BlocklistEntry` | [BlocklistEntries](#blocklistentries) |
| `FullRatios`, `FullRatioSnapshot` | [FullRatios](#fullratios), [override.SetFullRatios](../../override/README.md#setfullratios) |
| `InconsistentObject`, `InconsistentShard` | [PGInconsistencies](#pginconsistencies) |
| `DiagnosticsConfig`, `DiagnosticsContainer`, `DiagnosticsReport`, `DiagnosticArtifact` | [CollectDiagnostics](#collectdiagnostics) |
| `FinalizationConfig`, `Finalization` | [TerminateOnCleanup](#terminateoncleanup) |
| `VerificationConfig`, `LogExpectation`, `CrashExpectation`, `Verification`, `VerificationReport` and its parts | [Verify](#verify) |


## Running native commands

Everything without a typed helper can be done with Ceph's own tools in the
cluster's containers:

| Task | How |
| --- | --- |
| Any `ceph` command | `cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "nodelete", "true")` |
| JSON output | `cluster.Ceph(ctx, "osd", "dump", "--format", "json")` and `json.Unmarshal` |
| `rados`, `rbd`, Python clients | `client.Exec(ctx, []string{"rados", "-p", "app-data", "ls"}, tcexec.Multiplexed())` with `client` from `cluster.ClientContainer(ctx)` |
| `ceph-authtool` and the `ceph` CLI beside a daemon | `control.Exec(ctx, argv, tcexec.Multiplexed())` with `control` from `cluster.ControlContainer(ctx)` (essential image) |
| A command with bounded run time and output that must not outlive the call | [RunNativeCommand](#runnativecommand) |
| Daemon admin socket commands | `cluster.Ceph(ctx, "tell", "osd.0", "dump_ops_in_flight")` |
| A daemon's effective configuration | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_queue")` |
| Any central option | [override.SetConfig](../../override/README.md#setconfig), so it is restored |
| Commands inside one daemon's container | `Exec` on an element of `cluster.OSDs(ctx)` (the daemon image need not contain the `ceph` CLI the control container uses) |
| `radosgw-admin` | `gateway.Admin(ctx, args...)` from the [rgw](../../rgw/README.md) package |

Arguments are passed without a shell. `cluster.Ceph` adds `--connect-timeout 5`
and returns stdout; on failure the error contains the output. Raw commands can
change anything, so do not race them against a helper that changes the same
state, and prefer typed helpers when they exist because they restore state and
check identities. See [Native commands](native-commands.md) for more
examples.
