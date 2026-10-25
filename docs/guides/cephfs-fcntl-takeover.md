# POSIX record locks across an MDS standby takeover

`TestCephFSFcntlStandbyTakeover` retains two independent ceph-fuse mounts,
client sessions, actor processes and open descriptors across one owned MDS
standby takeover. It exercises Linux POSIX `fcntl` record locks, separately
from the existing libcephfs `flock` scenarios.

Actor A obtains a write lock on `[0,4096)`. Actor B first proves contention
with a nonblocking lock attempt and `F_GETLK`, then successfully locks and
unlocks the disjoint range `[8192,12288)`. The disjoint control distinguishes
a range conflict from an unusable descriptor or generally refused client.
Each actor records its PID, process start time, descriptor, inode, mount ID
and device. Each subsequent operation must retain these identities.

The test stops the owned active MDS and explicitly fails its old GID once.
The selected standby must become the owned active rank with its own GID.
Both original FUSE processes and MDS client sessions must remain identifiable.
Before actor A makes any additional lock or unlock request, B independently
observes the same conflicting range and native owner field with another nonblocking
attempt and `F_GETLK`. A stays behind its next gate during both observations.
The test therefore does not reconstruct the proof with a new request by A.
The remote FUSE `F_GETLK` may return `l_pid=0`; that value is recorded unchanged
and is not treated as actor A's PID. The owner evidence is causal: only A
requested the held range, B remains refused while A is gated, and A's explicit
unlock permits B to acquire it. Remote POSIX PID attribution is not qualified.

Only then does A unlock. B acquires the overlapping range, writes and fsyncs
predetermined bytes through its original descriptor, reads them back and
unlocks. An independent libcephfs client checks the inode, length and complete
bytes. Native operations are gated once; mutations are never automatically
replayed after an unknown result. Observation deadlines and command failures
are test failures, distinct from an expected `EAGAIN` or `EACCES` lock conflict.

Both actors close their descriptors. The old MDS rejoins as a new standby
generation. Each owned mount is explicitly unmounted under a fresh cleanup
context, and its FUSE process and MDS session must disappear. Actor cancellation,
foreground-process supervision and container cleanup remain registered on
failure. No host configuration is changed.

This is an explicit opt-in FUSE scenario. It reuses the existing admission
probe and client-only `/dev/fuse`, `CAP_SYS_ADMIN` and AppArmor settings.
An ineligible engine fails admission; a refused or unexecuted run is not native
acceptance. No public mount API or default engine configuration is added.
The selected same-release FUSE image is resolved to one immutable image ID
before starting the second client, and both actual IDs appear in the receipt.

On an eligible Linux Docker engine, run just this case with the selected
essential, client and cephfs image environment:

```sh
CGO_ENABLED=0 go test -mod=readonly -tags=all,cephfs_fuse \
  -run '^TestCephFSFcntlStandbyTakeover$' -count=1 -timeout=20m -v \
  ./internal/integration
```

It also belongs to the existing optional `cephfs_fuse` batch. The
`CEPHFS_FCNTL_TAKEOVER_RECEIPT` records MDS generations, retained sessions,
process and descriptor identities, pre/post conflict and lock queries,
unlock/acquire outcomes, byte verification and successful mount cleanup.
Pure fixtures reject changed identities, unrelated errors, missing locks,
wrong ranges and changed lock owners. Those fixtures and tagged compilation
do not establish native Ceph behavior.

This slice qualifies an already-held write range through one standby takeover.
Queued `F_SETLKW` replay, fairness, multiple active ranks, other client/release
combinations and kernel mounts remain outside its evidence. A native receipt
does not establish message-wire decoding or downstream consumer support.
