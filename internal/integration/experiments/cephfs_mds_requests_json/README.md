# Concurrent requests in the libcephfs admin socket

A service diagnosing a blocked filesystem operation may read the client's
`mds_requests` admin-socket command. Ordinary JSON decoding must preserve
each request's identity when two requests overlap. Older clients emit
sibling object members both named `request`. Many JSON decoders keep only
the last member with a given name. The correction places all request objects
in an array. See [Ceph Tracker 73639](https://tracker.ceph.com/issues/73639)
and the [client change](https://github.com/ceph/ceph/commit/69b8e4b238f).

## Running it

The optional native-regression batch is `cephfs_mds_requests_json`. It
needs Linux, Docker, the essential, client and cephfs roles, and a client
toolkit with the Ceph CLI and matching Python libcephfs. The selected
aggregate image provides every role. Override the default Squid 19.2.5
aggregate using the package's image variable, for example to compare with
the correction-bearing Tentacle 20.2.4.

```sh
casepkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_mds_requests_json
go test -v -count=1 -timeout=20m -tags=all,native_regression \
  -run '^TestCephFSConcurrentMDSRequestsJSON$' "$casepkg"
go test -v -count=1 -timeout=20m -tags=all,native_regression \
  -ldflags="-X ${casepkg}_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" \
  -run '^TestCephFSConcurrentMDSRequestsJSON$' "$casepkg"
```

## Controls and result

Two userspace clients mount one private filesystem. The holder opens and
locks two distinct files. The actor's nonblocking attempts on both must
report contention. Two threads on that actor then block on their respective
files. The case collects the raw admin-socket response, checks that the
reported inodes match the files, and compares the request identifiers after
ordinary and duplicate-preserving decoding. Releasing just the first lock
must complete only its own waiter. Releasing the second must complete the
other. Before and after the run the socket must show no outstanding lock
requests.

A successful run reports `REPRODUCED` when ordinary decoding loses an
observed inode and transaction ID. It reports `NOT_REPRODUCED` when both
survive. The receipt includes the raw reply and decoded identities. An
unavailable mount, socket or CLI, an absent overlap, or a failed release
control fails the test before a verdict. The observation window is 30 s,
the client probe is bounded to two minutes and the batch to 20 minutes.

On Linux amd64 with Docker 29.9.1, Squid 19.2.5 reproduced the loss in
38.94 s. Its reply repeated the member name and ordinary decoding discarded
TID 10, retaining TID 11. Tentacle 20.2.4 preserved both TIDs in a
`requests` array in 33.11 s. Both runs used the aggregate for every role
and passed the independent contention and release controls. These two
observations do not locate the first corrected patch in either series.

The probe closes its file descriptors, unlinks both files, shuts down its
clients and removes its temporary sockets. The fixture then removes its
cluster. The case exercises one libcephfs client and the JSON interpretation
of its diagnostics. It does not determine request completion for another
client or change the fixture API.
