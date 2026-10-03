//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

// Native Linux clients verify both automatic journal enrollment and explicit
// snapshot enrollment with distinct namespace mappings. Each comparison opens
// a new librados/librbd session against the destination FSID, with no image cache.
func TestMultiClusterRBDMirrorScopeAndNamespaces(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
			for index, tc := range []struct {
				name, sourceNamespace, destinationNamespace string
				scope                                       multicluster.RBDMirrorScope
			}{
				{"pool-default", "", "", multicluster.RBDMirrorScopePool},
				{"pool-named", "ns-a", "ns-b", multicluster.RBDMirrorScopePool},
				{"pool-named-to-default", "ns-a", "", multicluster.RBDMirrorScopePool},
				{"pool-default-to-named", "", "ns-b", multicluster.RBDMirrorScopePool},
				{"image-snapshot-named", "ns-a", "ns-b", multicluster.RBDMirrorScopeImage},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
					defer cancel()
					pool := fmt.Sprintf("tc-rbd-scope-%d", index)
					for _, cluster := range []*ceph.Container{source, destination} {
						if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 2, MinSize: 1}); err != nil {
							t.Fatal(err)
						}
						if err := cluster.InitRBDPool(ctx, pool); err != nil {
							t.Fatal(err)
						}
						for _, namespace := range []string{"isolated", "ns-a", "ns-b"} {
							if _, err := cluster.CreateRBDNamespace(ctx, pool, namespace); err != nil {
								t.Fatal(err)
							}
						}
						if err := cluster.WaitForClean(ctx); err != nil {
							t.Fatal(err)
						}
					}
					sourceStatus, err := source.Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					destinationStatus, err := destination.Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					const imageSize = 2 << 20
					before := rbdMultiClusterPayload(imageSize, 31+index)
					isolation := rbdMultiClusterPayload(imageSize, 119+index)
					// Same image names in sibling/default namespaces are positive
					// controls against accidentally writing or reading another scope.
					for _, site := range []struct {
						client         testcontainers.Container
						fsid, selected string
					}{
						{sourceClient, sourceStatus.FSID, tc.sourceNamespace},
						{destinationClient, destinationStatus.FSID, tc.destinationNamespace},
					} {
						for _, namespace := range []string{"", "isolated", "ns-a", "ns-b"} {
							if namespace == site.selected {
								continue
							}
							rbdScopeCreate(t, ctx, site.client, pool, namespace, "volume", true)
							rbdScopeIO(t, ctx, site.client, "write", site.fsid, pool, namespace, "volume", 0, isolation)
						}
					}
					existingName := "existing"
					if tc.scope == multicluster.RBDMirrorScopeImage {
						existingName = "volume"
					}
					rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, existingName, tc.scope == multicluster.RBDMirrorScopePool)
					rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, existingName, 0, before)
					rbdScopeAssertUnmirrored(t, ctx, sourceClient, rbdScopeImage(pool, tc.sourceNamespace, existingName))
					// A plain image in pool scope is intentionally not eligible for
					// journal enrollment, even while journaling images are mirrored.
					rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "plain", false)
					mirrorImage := os.Getenv("CEPH_TEST_MIRROR_IMAGE")
					if mirrorImage == "" {
						mirrorImage, _ = integrationImages(t)
					}
					link, err := multicluster.RunRBDMirror(ctx, mirrorImage, multicluster.RBDMirrorConfig{
						Source: source, Destination: destination, Pool: pool, Scope: tc.scope,
						SourceNamespace: tc.sourceNamespace, DestinationNamespace: tc.destinationNamespace,
					})
					if link != nil {
						t.Cleanup(func() {
							cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
							defer stop()
							if t.Failed() && link.Container != nil {
								multiClusterLogContainer(t, cleanupCtx, link.Container)
							}
							if err := link.Terminate(cleanupCtx); err != nil {
								t.Errorf("terminate namespace RBD link: %v", err)
							}
						})
					}
					if err != nil {
						t.Fatal(err)
					}
					policies, err := link.PolicyStatus(ctx)
					if err != nil || policies.Source.PoolID <= 0 || policies.Destination.PoolID <= 0 || policies.Source.Namespace != tc.sourceNamespace || policies.Source.RemoteNamespace != tc.destinationNamespace || policies.Destination.Namespace != tc.destinationNamespace || policies.Destination.RemoteNamespace != tc.sourceNamespace || policies.Source.Mode != string(tc.scope) || policies.Destination.Mode != string(tc.scope) || policies.Source.MirrorUUID == "" || policies.Destination.MirrorUUID == "" {
						t.Fatalf("native policy mapping/identity differs: %+v error=%v", policies, err)
					}
					for _, site := range []struct {
						client    testcontainers.Container
						namespace string
					}{
						{sourceClient, tc.sourceNamespace}, {destinationClient, tc.destinationNamespace},
					} {
						if site.namespace != "" {
							var base struct {
								Mode string `json:"mode"`
							}
							if err := json.Unmarshal(rbdOutput(t, ctx, site.client, "mirror", "pool", "info", pool, "--format", "json"), &base); err != nil || base.Mode != "init-only" {
								t.Fatalf("named mapping broadened default scope: mode=%s error=%v", base.Mode, err)
							}
						}
					}
					if tc.scope == multicluster.RBDMirrorScopePool {
						// Existing journaling images are enrolled when pool scope is
						// enabled. Future images enroll without EnableImage or snapshots.
						rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, existingName, before)
						rbdScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, tc.sourceNamespace, tc.destinationNamespace, existingName, "journal")
						rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "volume", true)
						rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 0, before)
					} else {
						if err := link.EnableImage(ctx, "volume"); err != nil {
							t.Fatal(err)
						}
					}
					rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, "volume", before)
					after := bytes.Clone(before)
					patch := rbdMultiClusterPayload(256<<10, 179+index)
					copy(after[512<<10:], patch)
					rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 512<<10, patch)
					mode := "journal"
					if tc.scope == multicluster.RBDMirrorScopeImage {
						mode = "snapshot"
						execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", rbdScopeImage(pool, tc.sourceNamespace, "volume"))
					}
					rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, "volume", after)
					rbdScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, tc.sourceNamespace, tc.destinationNamespace, "volume", mode)
					rbdScopeAssertUnmirrored(t, ctx, sourceClient, rbdScopeImage(pool, tc.sourceNamespace, "plain"))
					code, _, err := rbdMultiClusterExec(ctx, destinationClient, "rbd", "info", rbdScopeImage(pool, tc.destinationNamespace, "plain"), "--format", "json")
					if err != nil || code == 0 {
						t.Fatalf("plain image unexpectedly appeared in receiving namespace: exit=%d error=%v", code, err)
					}
					for _, site := range []struct {
						client         testcontainers.Container
						fsid, selected string
					}{
						{sourceClient, sourceStatus.FSID, tc.sourceNamespace}, {destinationClient, destinationStatus.FSID, tc.destinationNamespace},
					} {
						for _, namespace := range []string{"", "isolated", "ns-a", "ns-b"} {
							if namespace == site.selected {
								continue
							}
							rbdScopeIO(t, ctx, site.client, "read", site.fsid, pool, namespace, "volume", 0, isolation)
							rbdScopeAssertUnmirrored(t, ctx, site.client, rbdScopeImage(pool, namespace, "volume"))
						}
					}
					t.Logf("native %s scope %q -> %q: selected replica updated, sibling/default same-name images unchanged, plain image excluded, original pool IDs preserved, final sha256=%x", tc.scope, tc.sourceNamespace, tc.destinationNamespace, sha256.Sum256(after))
				})
			}
		})
	}
}

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
