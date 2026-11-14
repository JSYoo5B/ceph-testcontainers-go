//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_group_snap_default_quiesce)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package rbd_group_snap_default_quiesce_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.2-all"

func TestGroupSnapDefaultQuiesce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log(harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	out := harness.ExecOutput(t, ctx, client, "python3", "-c", probe)
	t.Log(out)
	for _, line := range strings.Split(out, "\n") {
		verdict, symptoms, ok := strings.Cut(line, "|")
		if ok && (verdict == "REPRODUCED" || verdict == "NOT_REPRODUCED") {
			t.Logf("EXPERIMENT_RESULT case=rbd_group_snap_default_quiesce result=%s image=%s symptoms=%s", verdict, image, symptoms)
			return
		}
	}
	t.Fatal("missing experiment observation")
}

const probe = quiesceVerdict + `
import ctypes as c
import subprocess
from contextlib import ExitStack


def run(*args):
    return subprocess.check_output(args, text=True, timeout=30)


pool = "snap-mode"
run("ceph", "osd", "pool", "create", pool, "1")
run("rbd", "pool", "init", pool)
run("rbd", "group", "create", pool + "/group")
run("rbd", "create", pool + "/member", "--size", "1M")
run("rbd", "group", "image", "add", pool + "/group", pool + "/member")

rados = c.CDLL("librados.so.2")
rbd = c.CDLL("librbd.so.1")
ptr, string, integer, handle = c.c_void_p, c.c_char_p, c.c_int, c.c_uint64
callback = c.CFUNCTYPE(None, ptr)


def bind(lib, name, result, *arguments):
    function = getattr(lib, name)
    function.restype, function.argtypes = result, arguments
    return function


create = bind(rados, "rados_create", integer, c.POINTER(ptr), string)
conf_read = bind(rados, "rados_conf_read_file", integer, ptr, string)
conf_set = bind(rados, "rados_conf_set", integer, ptr, string, string)
conf_get = bind(rados, "rados_conf_get", integer, ptr, string, string, c.c_size_t)
connect = bind(rados, "rados_connect", integer, ptr)
shutdown = bind(rados, "rados_shutdown", None, ptr)
io_create = bind(rados, "rados_ioctx_create", integer, ptr, string, c.POINTER(ptr))
io_destroy = bind(rados, "rados_ioctx_destroy", None, ptr)
open_image = bind(rbd, "rbd_open", integer, ptr, string, c.POINTER(ptr), string)
close_image = bind(rbd, "rbd_close", integer, ptr)
watch = bind(rbd, "rbd_quiesce_watch", integer, ptr, callback, callback, ptr, c.POINTER(handle))
ack = bind(rbd, "rbd_quiesce_complete", None, ptr, handle, integer)
unwatch = bind(rbd, "rbd_quiesce_unwatch", integer, ptr, handle)
snap = bind(rbd, "rbd_snap_create", integer, ptr, string)
group_snap = bind(rbd, "rbd_group_snap_create", integer, ptr, string, string)


def check(function, *args):
    rc = function(*args)
    if rc:
        raise RuntimeError((function.__name__, rc))


def client(stack, mode):
    cluster, io, image = ptr(), ptr(), ptr()
    check(create, c.byref(cluster), None)
    stack.callback(shutdown, cluster)
    check(conf_read, cluster, None)
    if mode:
        key = b"rbd_default_snapshot_quiesce_mode"
        check(conf_set, cluster, key, mode)
        value = c.create_string_buffer(64)
        check(conf_get, cluster, key, value, len(value))
        if value.value != mode:
            raise ValueError(("configured mode mismatch", mode, value.value))
    check(connect, cluster)
    check(io_create, cluster, pool.encode(), c.byref(io))
    stack.callback(io_destroy, io)
    check(open_image, io, b"member", c.byref(image), None)
    stack.callback(check, close_image, image)
    return io, image


with ExitStack() as resources:
    _, watched = client(resources, None)
    token = handle()
    events = []

    @callback
    def quiesce(_):
        events.append("quiesce")
        ack(watched, token, 0)

    @callback
    def unquiesce(_):
        events.append("unquiesce")

    check(watch, watched, quiesce, unquiesce, None, c.byref(token))
    resources.callback(check, unwatch, watched, token)
    counts = {}
    for mode in (b"required", b"skip"):
        with ExitStack() as caller:
            io, member = client(caller, mode)
            for kind in ("image", "group"):
                name = kind + "-" + mode.decode()
                events.clear()
                if kind == "group":
                    check(group_snap, io, b"group", name.encode())
                    listing = run("rbd", "group", "snap", "list", pool + "/group")
                else:
                    check(snap, member, name.encode())
                    listing = run("rbd", "snap", "list", pool + "/member")
                if name not in listing:
                    raise ValueError(("snapshot absent", name, listing))
                if events not in ([], ["quiesce", "unquiesce"]):
                    raise ValueError(("invalid callback sequence", name, events))
                counts[name] = events.count("quiesce")
    print(counts)
    print(quiesce_verdict(counts))
`
