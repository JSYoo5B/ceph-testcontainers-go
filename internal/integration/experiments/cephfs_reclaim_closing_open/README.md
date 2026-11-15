# MDS session open on the client's own closing session

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSReclaimOpenOnClosingSession`, optional batch
`cephfs_reclaim_closing_open`. The judgement of the reports has Docker-free
tests in the same package (`code/pure`).

## Use case and native contract

An NFS-style libcephfs service restarts and takes over its previous
instance's state with a UUID-targeted RESET: a fresh handle calls
`ceph_start_reclaim(uuid, CEPH_RECLAIM_RESET)` (which opens a session
without a UUID and makes the MDS evict the session that carries the UUID),
`ceph_finish_reclaim` and `ceph_set_uuid(uuid)`, and then mounts. The
contract is that the mount opens a session that carries the UUID.

`Client::set_uuid` (`src/client/Client.cc`) closes the handle's sessions
(`_close_sessions`): it sends `REQUEST_CLOSE` and waits for the MDS's
`CLOSE` for at most `client_shutdown_timeout`. When the MDS does not answer
in time, the client drops its session entry (`_closed_mds_session`) and
`set_uuid` returns. The mount then opens a new session on the same rank
(`_open_mds_session`), with the same client entity and the UUID in its
metadata. On the MDS (`MDSDaemon::ms_handle_accept`) the new connection
gets the client's existing session, which is still `closing` until the
`ESession` close event is safe in the journal.

In `Server::handle_client_session` (`src/mds/Server.cc`) a `REQUEST_OPEN`
on a `closing` session is not dropped like one on an `opening`, `open`,
`stale` or `killing` session. Before the correction it is handled as a new
open at once: the session takes the request's metadata, and
`find_session_by_uuid` finds that same session, which is still in the
session map, as the holder of the UUID. The MDS refuses the open
(`send_reject_message("duplicated session uuid")` and a cluster warning),
the client gets `CEPH_SESSION_REJECT`, and the mount returns `-EPERM`.
The correction makes a `REQUEST_OPEN` on a `closing` session wait until the
journal is safe (`mdlog->wait_for_safe` with `C_MDS_RetryMessage`); the
close then removes the session, and the retried open is accepted.

Upstream: [tracker 66003](https://tracker.ceph.com/issues/66003), fixed by
[ceph/ceph#66180](https://github.com/ceph/ceph/pull/66180) (main), with the
Tentacle backport [ceph/ceph#69504](https://github.com/ceph/ceph/pull/69504)
in 20.2.3 and the Squid backport
[ceph/ceph#69505](https://github.com/ceph/ceph/pull/69505), which no Squid
release carries yet. In the tracker the MDS was in `clientreplay`, which
delays the close; here the OSD pause flags delay it.

The defect is reproduced on Tentacle 20.2.2 and Squid 19.2.7, and not on
Tentacle 20.2.3 or Umbrella 21.2.0.

## Requirements

- One cluster with one filesystem and one active MDS (`cephfs.Run`) and
  its client toolkit, whose `python3` has the `cephfs` module of the same
  release; the actor calls libcephfs through `ctypes`. All from one image
  (`harness.NewSingleImageCluster`). The batch declares the image roles
  `essential`, `client` and `cephfs`; the case runs its own `image` for
  every container and ignores the batch's role images.
- The case sets `debug_mds = 10` for the MDS and the OSD pause flags
  (`ceph osd pause`) for about 25 seconds of the overlap flow. No other
  client may use the cluster.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Known-defect reproducer, optional batch `cephfs_reclaim_closing_open` with a
20-minute budget; no automatic run includes it. On its default image,
Tentacle 20.2.2, it prints `REPRODUCED` (48 s); with Tentacle 20.2.3 it
prints `NOT_REPRODUCED` (64 s), which is the fixed image of its
`Experiments` dispatch:

```sh
gh workflow run Experiments -f case=cephfs_reclaim_closing_open \
  -f image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.3-all
```

The judgement tests run in `code/pure` with every change (under 1 s). The
shared triggers, result checks and image injection are in
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSReclaimOpenOnClosingSession$' \
  ./internal/integration/experiments/cephfs_reclaim_closing_open
```

On the corrected Tentacle release:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_reclaim_closing_open_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.3-all" \
  -run '^TestCephFSReclaimOpenOnClosingSession$' \
  ./internal/integration/experiments/cephfs_reclaim_closing_open
```

The test logs the client's `ceph --version`. The
`CEPHFS_RECLAIM_CLOSING_OPEN_RECEIPT actor` line has the actor's report:
the session listings of the active MDS at each step and the reclaimers'
client log lines. The `CEPHFS_RECLAIM_CLOSING_OPEN_RECEIPT mds` lines are
the MDS log's session lines of the two reclaimers.

## Observations and controls

Each flow has its own UUID, a target handle and a reclaimer handle, which
are separate client entities. The target sets the UUID and mounts; the
reclaimer, with `client_shutdown_timeout = 10`, RESETs the UUID, finishes
the reclaim, calls `ceph_set_uuid` with the UUID and mounts.

1. Given, both flows: the target's session is open with the UUID; the RESET
   returns 0; after it the target's session is gone and the reclaimer's
   first session is open without a UUID.
2. Control flow, without a pause: `ceph_set_uuid` returns before the
   shutdown timeout and the client log has no close timeout, the mount
   returns 0, and the reclaimer's session is open with the UUID. The
   reopening path itself works.
3. Condition, overlap flow: the OSD map has `pauserd` and `pausewr` before
   `ceph_set_uuid`; `ceph_set_uuid` takes at least the shutdown timeout and
   the client log has `did not respond to session close -- timing out`
   (the client dropped its entry); the MDS then lists the reclaimer's
   session as `closing` (the close is not journaled).
4. Trigger: the reclaimer mounts in a thread; after a 15-second hold the
   actor lists the sessions again and clears the pause flags.
5. The MDS log, filtered to the session lines of the two reclaimers,
   must contain the overlap reclaimer's `request_close`, or the test fails
   (`debug_mds` did not apply).

## Reading the result

Standalone runs on a Linux amd64 host, Docker 29.9.1, bridge networking,
one run per aggregate image, with the client of the same build:

| Release | Image (`ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:…`) | Result | Test time |
| --- | --- | --- | --- |
| Tentacle 20.2.2 (0fcffee2) | `dd727dd6af41…` | `REPRODUCED` | 48 s |
| Squid 19.2.7 (9ec952b9) | `17bb5a8e830d…` | `REPRODUCED` | 53 s |
| Tentacle 20.2.3 (06c2f9c3) | `33ba0a4a2b1e…` | `NOT_REPRODUCED` | 64 s |
| Umbrella 21.2.0 (b060042d) | `6e70e465e4a1…` | `NOT_REPRODUCED` | 66 s |

- **REPRODUCED:** the mount returned `-EPERM` during the hold, the client
  logged `rejected us (duplicated session uuid)`, and the MDS log has, after
  the reclaimer's `request_close`, the warning `client session with
  duplicated session uuid '<uuid>' denied (client.<reclaimer> …)`. On
  20.2.2 and 19.2.7 the `request_open` and the refusal were at the same
  millisecond, while the session was closing, and the close's
  `_session_logged` followed after the unpause.
- **NOT_REPRODUCED:** the mount had not returned when the hold ended, the
  session was still `closing`, and after the unpause the mount returned 0
  with the reclaimer's session open with the UUID. The MDS log has the
  reclaimer's `request_close`, a `request_open` during the pause with no
  reply, the close's `_session_logged … close` and `remove_session`, and
  then the same `request_open` handled again on the journal's thread, which
  is the retry of the correction; there is no refusal.
- **NOT_REPRODUCED on a build claimed to be affected:** check in the actor
  receipt that `closing_sessions` lists the reclaimer as `closing` and that
  `close_seconds` is at least 10.
- **Unavailable prerequisite:** the failure starts with `unavailable
  prerequisite`: the cluster, the filesystem or the client does not start,
  `debug_mds` cannot be set, the actor fails (a native call returned an
  error, a mount did not return), or the MDS log cannot be read or has no
  session request of the reclaimer.
- **Control failed:** the failure starts with `control failed` and names
  the step of the control flow that did not hold. The overlap then says
  nothing.
- **Condition not established:** the failure starts with `condition not
  established`: the OSDs were not paused, the close was answered, or the
  session was not closing on the MDS. It is not a reproduction and not a
  pass.
- **Neither outcome:** the failure starts with `neither the contract nor
  the defect`: another errno (a mount timeout, for example), an open
  accepted during the pause, a refusal without the MDS warning for this
  client and UUID, or a successful mount without the retry in the MDS log.
  It is not the defect of this case.

## Cleanup and limits

The actor clears the pause flags when it ends, also after a failure, and
the test clears them again in its cleanup before the cluster is
terminated. The target handles are evicted by the RESETs; the actor leaves
its handles to the process exit, and the containers are terminated with
the cluster.

Not covered: the kernel client and `ceph-fuse`, an MDS in `clientreplay`
(the tracker's trigger), several MDS ranks, a client whose open is sent by
its MDS map handling, the stale capabilities or locks a refused reclaimer
would leave, the blocklist effects of the RESET, and arm64.
