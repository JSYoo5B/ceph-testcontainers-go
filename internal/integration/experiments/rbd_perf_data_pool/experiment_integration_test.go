//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_perf_data_pool)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package rbd_perf_data_pool_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all"

func TestRBDPerfDataPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log(harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	out := harness.ExecOutput(t, ctx, client, "python3", "-c", probe)
	t.Log(out)
	for _, line := range strings.Split(out, "\n") {
		verdict, symptoms, ok := strings.Cut(line, "|")
		if ok && (verdict == "REPRODUCED" || verdict == "NOT_REPRODUCED") {
			t.Logf("EXPERIMENT_RESULT case=rbd_perf_data_pool result=%s image=%s symptoms=%s", verdict, image, symptoms)
			return
		}
	}
	t.Fatal("missing experiment observation")
}

const probe = `import concurrent.futures
import json
import subprocess
import threading
import time
import rados
import rbd


def run(*args):
    return subprocess.check_output(args, text=True, timeout=60).strip()


meta, data, control = "rbd-meta", "rbd-data", "rbd-control"
for pool in (meta, control):
    run("ceph", "osd", "pool", "create", pool, "4")
    run("rbd", "pool", "init", pool)
run("ceph", "osd", "erasure-code-profile", "set", "probe", "k=2", "m=1", "crush-failure-domain=osd")
run("ceph", "osd", "pool", "create", data, "4", "4", "erasure", "probe")
run("ceph", "osd", "pool", "set", data, "allow_ec_overwrites", "true")
run("ceph", "osd", "pool", "application", "enable", data, "rbd")
run("rbd", "create", meta + "/striped", "--data-pool", data, "--size", "8M")
run("rbd", "create", control + "/replicated", "--size", "8M")
info = json.loads(run("rbd", "info", meta + "/striped", "--format", "json"))
if info["data_pool"] != data:
    raise RuntimeError(("image data pool mismatch", info))


def sample(pool):
    if pool == data:
        # iostat retries an empty initial result forever. Read the manager
        # reply used by that CLI to distinguish an empty report from a timeout.
        reply = json.loads(
            run("ceph", "rbd", "perf", "image", "stats", data, "write_ops", "--format", "json")
        )
        index = reply["stat_descriptors"].index("write_ops")
        rows = []
        for row in reply["stats"]:
            for key, values in row.items():
                pool_id, name = key.split("/", 1)
                rows.append(
                    dict(
                        pool=reply["pool_descriptors"][pool_id],
                        image=name,
                        pool_namespace="",
                        write_ops=values[index],
                    )
                )
        return rows
    cmd = ["rbd", "perf", "image", "iostat", "--format", "json", "--iterations", "1"]
    if pool:
        cmd.extend(["--pool", pool])
    return json.loads(run(*cmd))


stop = threading.Event()


def write(pool, name):
    with (
        rados.Rados(conffile="") as cluster,
        cluster.open_ioctx(pool) as io,
        rbd.Image(io, name) as image,
    ):
        offset = 0
        while not stop.is_set():
            image.write(b"x" * 4096, offset)
            image.flush()
            offset = (offset + 4096) % (8 * 1024 * 1024)
            stop.wait(0.05)


def active(row):
    return row["write_ops"] > 0 and row["pool_namespace"] == ""


def identity(row):
    return row["pool"], row["image"]


with concurrent.futures.ThreadPoolExecutor(max_workers=5) as workers:
    writers = [workers.submit(write, meta, "striped"), workers.submit(write, control, "replicated")]
    try:
        seen = []
        until = time.monotonic() + 130
        while time.monotonic() < until:
            global_rows, replicated_rows, ec_rows = list(workers.map(sample, ("", control, data)))
            for writer in writers:
                if writer.done():
                    writer.result()
            global_ids = {identity(row) for row in global_rows if active(row)}
            replicated_ids = {identity(row) for row in replicated_rows if active(row)}
            ec_ids = {identity(row) for row in ec_rows if active(row)}
            if (
                (meta, "striped") in global_ids
                and (control, "replicated") in global_ids
                and (control, "replicated") in replicated_ids
            ):
                if {identity(row) for row in ec_rows} - {(meta, "striped")}:
                    raise ValueError(ec_rows)
                if ec_rows and not ec_ids:
                    seen.clear()
                    time.sleep(6)
                    continue
                seen.append(bool(ec_ids))
                print("observed", global_ids, replicated_ids, ec_ids, flush=True)
                if len(seen) >= 3 and (all(seen[-3:]) or not any(seen[-3:])):
                    break
            else:
                seen.clear()
            time.sleep(6)
        else:
            raise RuntimeError(
                ("controls or consecutive samples missing", global_rows, replicated_rows, ec_rows)
            )
        if seen[-1]:
            cli = json.loads(
                run(
                    "rbd",
                    "perf",
                    "image",
                    "iostat",
                    "--pool",
                    data,
                    "--format",
                    "json",
                    "--iterations",
                    "1",
                )
            )
            if (meta, "striped") not in {identity(r) for r in cli if active(r)}:
                raise RuntimeError(("filtered CLI did not report the active EC image", cli))
        print(
            "NOT_REPRODUCED|[]"
            if seen[-1]
            else "REPRODUCED|[active image absent under its data-pool filter]"
        )
    finally:
        stop.set()
        for writer in writers:
            writer.result()
`
