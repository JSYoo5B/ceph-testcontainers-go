//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

func RBDJournalRunLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *rbdmirror.Mirror {
	t.Helper()
	image := StaticFacts(source).Images.RBDAux
	link, err := rbdmirror.Run(ctx, NativeMirrorImage(t, "rbd-aux", image), rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite, Mode: rbdmirror.ModeJournal,
	}))
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && link.Container != nil {
				MultiClusterLogContainer(t, cleanupCtx, link.Container)
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate journal RBD link: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func RBDJournalWrite(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, offset int, data []byte) {
	t.Helper()
	const path = "/tmp/rbd-journal-write"
	if err := client.CopyToContainer(ctx, data, path, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, err := RBDMultiClusterExec(ctx, client, "python3", "-c", RBDJournalIOScript, "write", fsid, pool, image, fmt.Sprint(offset), path)
	if err != nil || code != 0 {
		t.Fatalf("native journal image write failed: exit=%d error=%v output=%s", code, err, output)
	}
}

func RBDJournalRead(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	const path = "/tmp/rbd-journal-expected"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, err := RBDMultiClusterExec(ctx, client, "python3", "-c", RBDJournalIOScript, "read", fsid, pool, image, "0", path)
	if err != nil || code != 0 {
		t.Fatalf("fresh native journal image read differs: exit=%d error=%v output=%s", code, err, output)
	}
}

func RBDJournalWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	const path = "/tmp/rbd-journal-expected"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 35*time.Second)
		code, output, err := RBDMultiClusterExec(attempt, client, "python3", "-c", RBDJournalIOScript, "read", fsid, pool, image, "0", path)
		stop()
		if err == nil && code == 0 {
			t.Logf("fresh native journal replica verified full %d bytes: sha256=%x; evidence=%s", len(expected), sha256.Sum256(expected), output)
			return
		}
		last = fmt.Sprintf("exit=%d error=%v output=%s", code, err, output)
		select {
		case <-ctx.Done():
			statusCtx, statusCancel := context.WithTimeout(parent, 10*time.Second)
			_, status, _ := RBDMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", pool+"/"+image, "--format", "json")
			statusCancel()
			t.Fatalf("journal replica never reached expected bytes: %v; %s; status=%s", ctx.Err(), last, status)
		case <-time.After(2 * time.Second):
		}
	}
}

// Each invocation creates a new librados/librbd session and disables its image
// cache. The expected file is a fixture comparator; only mode=write modifies an
// RBD image. Replication is exclusively performed by the rbd-mirror daemon.
const RBDJournalIOScript = `import hashlib, json, os, rados, rbd, sys, threading
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
mode, fsid, pool, name, offset, path = sys.argv[1:]
with open(path, "rb") as fixture:
    expected = fixture.read()
try:
    with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
        "keyring": "/etc/ceph/ceph.client.admin.keyring",
        "rados_mon_op_timeout": "15", "rados_osd_op_timeout": "15", "rbd_cache": "false",
    }) as cluster:
        assert cluster.get_fsid() == fsid, "native client reached another cluster"
        with cluster.open_ioctx(pool) as ioctx:
            with rbd.Image(ioctx, name, read_only=(mode == "read")) as image:
                if mode == "write":
                    written = image.write(expected, int(offset))
                    assert written == len(expected), "short native write"
                    image.flush()
                    actual = image.read(int(offset), len(expected))
                elif mode == "read":
                    assert image.size() == len(expected), "replica size mismatch"
                    actual = image.read(0, len(expected))
                else:
                    raise ValueError("unexpected fixture mode")
                assert actual == expected, "native image bytes differ: sha256=" + hashlib.sha256(actual).hexdigest()
                print(json.dumps({"fsid": cluster.get_fsid(), "bytes": len(actual), "sha256": hashlib.sha256(actual).hexdigest(), "mode": mode}))
finally:
    deadline.cancel()
`
