//go:build integration && ci_optional && ci_batch_desktop_capabilities

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// desktopNBDScript runs in the privileged helper. It maps an RBD image through
// rbd-nbd, writes a known pattern through the block device with direct I/O,
// reads it back through the device and, independently, through librbd
// (rbd export), and unmaps. It prints one JSON object. Arguments: pool/image.
const desktopNBDScript = `import hashlib, json, os, subprocess, sys
spec = sys.argv[1]
out = {"spec": spec}
def run(*argv, data=None):
    r = subprocess.run(argv, capture_output=True, timeout=120, input=data)
    return r.returncode, r.stdout, r.stderr.decode(errors="replace")[-400:]
out["nbd_module"] = os.path.isdir("/sys/module/nbd")
out["nbd_nodes"] = sorted(n for n in os.listdir("/dev") if n.startswith("nbd") and n[3:].isdigit())[:4]
if not out["nbd_module"] or not out["nbd_nodes"]:
    out["eligible"] = False
    print(json.dumps(out)); sys.exit(0)
out["eligible"] = True
rc, mapped, err = run("rbd-nbd", "map", spec)
if rc != 0:
    out["map_error"] = err
    print(json.dumps(out)); sys.exit(0)
dev = mapped.decode().strip()
out["device"] = dev
try:
    pattern = hashlib.sha256(b"desktop-nbd").digest() * (1 << 17)  # 4 MiB
    offset = 8 << 20
    fd = os.open(dev, os.O_RDWR | os.O_DIRECT)
    try:
        import mmap
        buf = mmap.mmap(-1, len(pattern))
        buf.write(pattern)
        os.pwrite(fd, buf, offset)
        os.fsync(fd)
        back = mmap.mmap(-1, len(pattern))
        os.preadv(fd, [back], offset)
        out["device_readback"] = hashlib.sha256(back).hexdigest() == hashlib.sha256(pattern).hexdigest()
    finally:
        os.close(fd)
    with open("/sys/block/%s/size" % os.path.basename(dev)) as f:
        out["device_bytes"] = int(f.read()) * 512
finally:
    rc, _, err = run("rbd-nbd", "unmap", dev)
    out["unmapped"] = rc == 0
    if rc != 0:
        out["unmap_error"] = err
rc, listed, _ = run("rbd-nbd", "list-mapped")
out["still_mapped"] = listed.decode().strip()
rc, exported, err = run("rbd", "export", spec, "-")
if rc == 0:
    out["librbd_readback"] = hashlib.sha256(exported[offset:offset + len(pattern)]).hexdigest() == hashlib.sha256(pattern).hexdigest()
else:
    out["export_error"] = err
print(json.dumps(out))`

// TestDesktopRBDOverNBD is the Docker Desktop PoC of issue 368. The Desktop VM
// kernel has no kernel RBD client but has the NBD driver built in, so rbd-nbd,
// a userspace RBD client, can still give a test a block device backed by an RBD
// image. A privileged helper from the official Ceph image of the cluster's
// release maps the image, writes and reads a pattern through /dev/nbdN with
// direct I/O, unmaps it, and reads the same bytes back through librbd. An
// engine kernel without NBD is reported as not eligible and skipped: the
// module loads no kernel module.
func TestDesktopRBDOverNBD(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t)
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "desktop-nbd", Application: "rbd", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	// The role images ship no rbd-nbd; the official image does.
	helper, err := testcontainers.Run(ctx, "quay.io/ceph/ceph:v"+facts.CephVersion, cluster.WithClient(),
		testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) { h.Privileged = true }))
	if helper != nil {
		t.Cleanup(func() { _ = helper.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	harness.ExecOutput(t, ctx, helper, "rbd", "create", "desktop-nbd/image", "--size", "64")
	raw := harness.ExecOutput(t, ctx, helper, "python3", "-I", "-c", desktopNBDScript, "desktop-nbd/image")
	t.Logf("DESKTOP_NBD_RECEIPT %s", strings.TrimSpace(raw))
	var r struct {
		Eligible       bool   `json:"eligible"`
		Device         string `json:"device"`
		MapError       string `json:"map_error"`
		DeviceReadback bool   `json:"device_readback"`
		DeviceBytes    int64  `json:"device_bytes"`
		Unmapped       bool   `json:"unmapped"`
		StillMapped    string `json:"still_mapped"`
		LibrbdReadback bool   `json:"librbd_readback"`
		ExportError    string `json:"export_error"`
	}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(raw)), &r); err != nil {
		t.Fatalf("receipt: %v: %s", err, raw)
	}
	if !r.Eligible {
		t.Skip("the engine kernel has no NBD driver or no /dev/nbd nodes; nothing is loaded")
	}
	switch {
	case r.MapError != "":
		t.Fatalf("rbd-nbd map: %s", r.MapError)
	case !r.DeviceReadback:
		t.Fatalf("the pattern did not read back through %s", r.Device)
	case r.DeviceBytes != 64<<20:
		t.Fatalf("%s has %d bytes, want the image's 64 MiB", r.Device, r.DeviceBytes)
	case !r.Unmapped || r.StillMapped != "":
		t.Fatalf("unmap: unmapped=%t still mapped %q", r.Unmapped, r.StillMapped)
	case !r.LibrbdReadback:
		t.Fatalf("librbd did not read the pattern written through the device: %s", r.ExportError)
	}
}
