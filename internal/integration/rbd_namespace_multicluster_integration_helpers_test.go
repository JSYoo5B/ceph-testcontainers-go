//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func rbdScopeImage(pool, namespace, name string) string {
	if namespace == "" {
		return pool + "/" + name
	}
	return pool + "/" + namespace + "/" + name
}

func rbdScopeCreate(t *testing.T, ctx context.Context, client testcontainers.Container, pool, namespace, name string, journal bool) {
	t.Helper()
	features := "layering,exclusive-lock"
	if journal {
		features += ",journaling"
	}
	execCommand(t, ctx, client, "rbd", "create", rbdScopeImage(pool, namespace, name), "--size", "2M", "--object-size", "1M", "--image-feature", features)
}

func rbdScopeAssertUnmirrored(t *testing.T, ctx context.Context, client testcontainers.Container, image string) {
	t.Helper()
	var info struct {
		Mirroring *struct {
			State string `json:"state"`
		} `json:"mirroring"`
	}
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil || (info.Mirroring != nil && info.Mirroring.State != "" && info.Mirroring.State != "disabled") {
		t.Fatalf("unselected image was enrolled: %s info=%+v error=%v", image, info, err)
	}
}

func rbdScopeAssertReplicaIdentity(t *testing.T, ctx context.Context, source, destination testcontainers.Container, pool, sourceNamespace, destinationNamespace, name, mode string) {
	t.Helper()
	globalID := ""
	for _, site := range []struct {
		client    testcontainers.Container
		namespace string
		primary   bool
	}{
		{source, sourceNamespace, true}, {destination, destinationNamespace, false},
	} {
		var info struct {
			Features  []string `json:"features"`
			Mirroring struct {
				Mode, State string
				Primary     bool
				GlobalID    string `json:"global_id"`
			} `json:"mirroring"`
		}
		if err := json.Unmarshal(rbdOutput(t, ctx, site.client, "info", rbdScopeImage(pool, site.namespace, name), "--format", "json"), &info); err != nil {
			t.Fatal(err)
		}
		if info.Mirroring.Mode != mode || info.Mirroring.State != "enabled" || info.Mirroring.Primary != site.primary || info.Mirroring.GlobalID == "" || !slices.Contains(info.Features, "exclusive-lock") || (mode == "journal" && !slices.Contains(info.Features, "journaling")) {
			t.Fatalf("native replica identity/mode differs: %+v expected primary=%t mode=%s", info, site.primary, mode)
		}
		if globalID != "" && info.Mirroring.GlobalID != globalID {
			t.Fatalf("destination image is not the source's native replica: source=%s destination=%s", globalID, info.Mirroring.GlobalID)
		}
		globalID = info.Mirroring.GlobalID
	}
}

func rbdScopeIO(t *testing.T, ctx context.Context, client testcontainers.Container, mode, fsid, pool, namespace, name string, offset int, data []byte) {
	t.Helper()
	const path = "/tmp/rbd-scope-fixture"
	if err := client.CopyToContainer(ctx, data, path, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, err := rbdMultiClusterExec(ctx, client, "python3", "-c", rbdScopeIOScript, mode, fsid, pool, namespace, name, fmt.Sprint(offset), path)
	if err != nil || code != 0 {
		t.Fatalf("native namespace I/O failed mode=%s namespace=%q image=%s: exit=%d error=%v output=%s", mode, namespace, name, code, err, output)
	}
}

func rbdScopeWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, fsid, pool, namespace, name string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	const path = "/tmp/rbd-scope-expected"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 35*time.Second)
		code, output, err := rbdMultiClusterExec(attempt, client, "python3", "-c", rbdScopeIOScript, "read", fsid, pool, namespace, name, "0", path)
		stop()
		if err == nil && code == 0 {
			t.Logf("fresh native namespace replica %q/%s: %d bytes sha256=%x evidence=%s", namespace, name, len(expected), sha256.Sum256(expected), output)
			return
		}
		last = fmt.Sprintf("exit=%d error=%v output=%s", code, err, output)
		select {
		case <-ctx.Done():
			statusCtx, stop := context.WithTimeout(parent, 10*time.Second)
			_, status, _ := rbdMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", rbdScopeImage(pool, namespace, name), "--format", "json")
			stop()
			t.Fatalf("native namespace replica never reached expected bytes: %v; %s; status=%s", ctx.Err(), last, status)
		case <-time.After(2 * time.Second):
		}
	}
}

const rbdScopeIOScript = `import hashlib, json, os, rados, rbd, sys, threading
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
mode, fsid, pool, namespace, name, offset, path = sys.argv[1:]
with open(path, "rb") as fixture:
    expected = fixture.read()
try:
    with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
        "keyring": "/etc/ceph/ceph.client.admin.keyring",
        "rados_mon_op_timeout": "15", "rados_osd_op_timeout": "15", "rbd_cache": "false",
    }) as cluster:
        assert cluster.get_fsid() == fsid, "native client reached another cluster"
        with cluster.open_ioctx(pool) as ioctx:
            ioctx.set_namespace(namespace)
            with rbd.Image(ioctx, name, read_only=(mode == "read")) as image:
                if mode == "write":
                    assert image.write(expected, int(offset)) == len(expected), "short native write"
                    image.flush()
                    actual = image.read(int(offset), len(expected))
                elif mode == "read":
                    assert image.size() == len(expected), "replica size mismatch"
                    actual = image.read(0, len(expected))
                else:
                    raise ValueError("unexpected fixture mode")
                assert actual == expected, "native image bytes differ: sha256=" + hashlib.sha256(actual).hexdigest()
                print(json.dumps({"fsid": cluster.get_fsid(), "namespace": namespace, "bytes": len(actual), "sha256": hashlib.sha256(actual).hexdigest(), "mode": mode}))
finally:
    deadline.cancel()
`
