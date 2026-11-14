# Image iostat with an erasure-coded RBD data pool

An operator filters image statistics by the pool receiving I/O. An image whose metadata lives in a replicated pool and data lives in an erasure-coded pool must remain identifiable by metadata pool and image name when the caller selects its data pool.

## Observe the pool filter

The case creates three OSDs with one replicated metadata pool, a k=2/m=1 EC data pool using OSD failure domains and a second replicated pool for a single-pool control. It verifies the EC-backed image's data pool with `rbd info`. Two Python clients write and flush 4 KiB blocks to the two 8 MiB images throughout collection.

Unfiltered `rbd perf image iostat` must report positive writes to both identified images. Selecting the single-pool control's pool must report its image with positive writes. At the same time the case reads the native `rbd perf image stats` MGR reply used by iostat, selecting the EC pool. This direct observation allows the case to read a successful empty response. The CLI waits indefinitely for its first nonempty result. An error or a timeout cannot establish that the image was absent.

Three consecutive qualifying samples at least six seconds apart must agree. If the filter returns the EC-backed image, the caller also checks that the CLI reports positive writes for that exact pool and image. A missing control, malformed reply or failed writer fails the test. The observation loop is bounded to 130 seconds and individual commands to 60 seconds. Test teardown stops both writers and removes the cluster, clients and disposable storage.

## Run and interpret the result

This optional `native_regression` experiment runs MON, MGR, three OSDs and the admin client on one aggregate image (essential and client roles). On the default 19.2.5 aggregate the EC filter returns no active image while both controls report writes. With upstream 19.2.7, the selected EC pool returns the image under its original metadata-pool name. The case passes on either observation and prints the corresponding `EXPERIMENT_RESULT`. This characterizes the native stats query and requires no fixture API change.

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_perf_data_pool
go test -tags=all,native_regression -count=1 -v -timeout=20m "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=quay.io/ceph/ceph:v19.2.7" "$pkg"
```

[Ceph Tracker 75480](https://tracker.ceph.com/issues/75480) · [Correction](https://github.com/ceph/ceph/commit/47472df795ced490e757d974e6d5e9117f5b7a9b)
