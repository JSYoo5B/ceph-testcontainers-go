# Default quiesce policy for RBD group snapshots

An application that uses the default-flags snapshot calls should observe the configured quiesce policy on both an image and a group containing that image.

One OSD and a client with Python 3, librados and librbd. A separate client registers a quiesce watcher and acknowledges every notification. For each mode the caller opens a fresh connection, reads back its configuration, then calls rbd_snap_create and rbd_group_snap_create without flags. Each snapshot must succeed and appear in the corresponding native listing.

In required mode both snapshot types deliver one quiesce and one unquiesce notification. In skip mode the image snapshot delivers neither. The group snapshot delivers both on 19.2.2 and neither on 19.2.3. Missing snapshots, failed registration and invalid callback sequences fail the test. This checks the C binding with an acknowledging watcher.

The Docker-free `TestQuiesceVerdictRequiresCallbacks` supplies synthetic callback counts to the same verdict function in normal and optimized Python. Missing required callbacks must fail in both modes.

## Tier and triggers

Known-defect reproducer, optional batch `rbd_group_snap_default_quiesce` with a 20-minute budget; no
automatic run includes it. It is an `Experiments` case, and its fixed image
there is `quay.io/ceph/ceph:v19.2.3`. `TestQuiesceVerdictRequiresCallbacks` is the package's `code/pure` batch and runs in Code checks for every change. The triggers, the result checks and the image
injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Run and interpret the result

The cluster and client run one aggregate image (essential and client roles); the default is `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.2-all`. The case prints one [result line](../../../../docs/contributing/testing.md#experiment-result-lines): `REPRODUCED` when the group snapshot in skip mode delivers quiesce notifications, `NOT_REPRODUCED` when it delivers none. 19.2.2 printed `REPRODUCED`, 19.2.3 `NOT_REPRODUCED`. A missing prerequisite or an unclassified result fails the test.

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_group_snap_default_quiesce
go test -tags=all,native_regression -count=1 -v -timeout=20m "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=quay.io/ceph/ceph:v19.2.3" "$pkg"
```

The fixture removes the owned cluster, client and their disposable storage at teardown. This result characterizes upstream behavior and changes no fixture API.

[Ceph Tracker 70632](https://tracker.ceph.com/issues/70632) · [Correction](https://github.com/ceph/ceph/commit/b91ab48ccc621431b5327d0806137152e8fbb092)
