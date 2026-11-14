# Default quiesce policy for RBD group snapshots

An application that uses the default-flags snapshot calls should observe the configured quiesce policy on both an image and a group containing that image.

One OSD and a client with Python 3, librados and librbd. A separate client registers a quiesce watcher and acknowledges every notification. For each mode the caller opens a fresh connection, reads back its configuration, then calls rbd_snap_create and rbd_group_snap_create without flags. Each snapshot must succeed and appear in the corresponding native listing.

In required mode both snapshot types deliver one quiesce and one unquiesce notification. In skip mode the image snapshot delivers neither. The group snapshot delivers both on 19.2.2 and neither on 19.2.3. Missing snapshots, failed registration and invalid callback sequences fail the test. This checks the C binding with an acknowledging watcher.

The Docker-free `TestQuiesceVerdictRequiresCallbacks` supplies synthetic callback counts to the same verdict function in normal and optimized Python. Missing required callbacks must fail in both modes.

## Run and interpret the result

This optional `native_regression` experiment uses one aggregate Ceph image for the cluster and client (essential and client roles). The default is `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.2-all`. Both observed outcomes pass. The case emits `EXPERIMENT_RESULT` with `REPRODUCED` when the documented symptom occurs and `NOT_REPRODUCED` when it observes the corrected behavior. A missing prerequisite or an unclassified result fails the test.

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_group_snap_default_quiesce
go test -tags=all,native_regression -count=1 -v -timeout=20m "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=quay.io/ceph/ceph:v19.2.3" "$pkg"
```

The fixture removes the owned cluster, client and their disposable storage at teardown. This result characterizes upstream behavior and changes no fixture API.

[Ceph Tracker 70632](https://tracker.ceph.com/issues/70632) · [Correction](https://github.com/ceph/ceph/commit/b91ab48ccc621431b5327d0806137152e8fbb092)
