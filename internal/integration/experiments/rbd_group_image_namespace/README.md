# RBD group image namespace options

A caller can specify both the group and the image as positional specs or pass their pools, namespaces and names as explicit CLI options. Both forms should modify only the selected membership.

One OSD and an administrative client. The pool contains a named namespace and same-name controls in its default namespace. The positional add and remove establish the selected membership and restore its empty state. Explicit add starts with both groups empty. Explicit remove starts with the same image name present in **both** groups, independently of the add outcome. Both operations must return success.

On 19.2.0 explicit add changes only the default group and explicit remove clears only that group. On 19.2.1 and 19.2.5 both operations change only the named group. Membership comes from formatted listings of both groups after each operation. An unexpected membership, CLI refusal, or failed control fails the test.

## Tier and triggers

Known-defect reproducer, optional batch `rbd_group_image_namespace` with a 20-minute budget; no
automatic run includes it. It is an `Experiments` case, and its fixed image
there is `quay.io/ceph/ceph:v19.2.1`. The triggers, the result checks and the image
injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Run and interpret the result

The cluster and client run one aggregate image (essential and client roles); the default is `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.0-all`. The case prints a separate [result line](../../../../docs/contributing/testing.md#experiment-result-lines) for add and for remove: `REPRODUCED` when the operation changes the default namespace's group, `NOT_REPRODUCED` when it changes only the named group. 19.2.0 printed `REPRODUCED`, 19.2.1 `NOT_REPRODUCED`. A missing prerequisite or an unclassified result fails the test.

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_group_image_namespace
go test -tags=all,native_regression -count=1 -v -timeout=20m "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=quay.io/ceph/ceph:v19.2.1" "$pkg"
```

The fixture removes the owned cluster, client and their disposable storage at teardown. This result characterizes upstream behavior and changes no fixture API.

[Ceph Tracker 69324](https://tracker.ceph.com/issues/69324) · [Correction](https://github.com/ceph/ceph/commit/5a5f0f03aa59682c1d69828f8ac90045da42b809)
