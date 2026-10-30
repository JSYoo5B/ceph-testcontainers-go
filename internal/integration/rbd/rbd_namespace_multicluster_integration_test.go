//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_mirror_scope))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// Native Linux clients verify both automatic journal enrollment and explicit
// snapshot enrollment with distinct namespace mappings. Each comparison opens
// a new librados/librbd session against the destination FSID, with no image cache.
func TestMultiClusterRBDMirrorScopeAndNamespaces(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		t.Run(network, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
			for index, tc := range []struct {
				name, sourceNamespace, destinationNamespace string
				scope                                       rbdmirror.Scope
			}{
				{"pool-default", "", "", rbdmirror.ScopePool},
				{"pool-same-named", "ns-a", "ns-a", rbdmirror.ScopePool},
				{"pool-named", "ns-a", "ns-b", rbdmirror.ScopePool},
				{"pool-named-to-default", "ns-a", "", rbdmirror.ScopePool},
				{"pool-default-to-named", "", "ns-b", rbdmirror.ScopePool},
				{"image-snapshot-named", "ns-a", "ns-b", rbdmirror.ScopeImage},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
					defer cancel()
					pool := fmt.Sprintf("tc-rbd-scope-%d", index)
					for _, cluster := range []*ceph.Container{source, destination} {
						if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 2, MinSize: 1}); err != nil {
							t.Fatal(err)
						}
						if err := rbd.InitPool(ctx, cluster, pool); err != nil {
							t.Fatal(err)
						}
						for _, namespace := range []string{"isolated", "ns-a", "ns-b"} {
							if _, err := rbd.CreateNamespace(ctx, cluster, pool, namespace); err != nil {
								t.Fatal(err)
							}
						}
						if err := cluster.WaitForClean(ctx); err != nil {
							t.Fatal(err)
						}
					}
					sourceStatus, err := harness.ObserveCluster(source).Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					destinationStatus, err := harness.ObserveCluster(destination).Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					const imageSize = 2 << 20
					before := harness.RBDMultiClusterPayload(imageSize, 31+index)
					isolation := harness.RBDMultiClusterPayload(imageSize, 119+index)
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
							harness.RBDScopeCreate(t, ctx, site.client, pool, namespace, "volume", true)
							harness.RBDScopeIO(t, ctx, site.client, "write", site.fsid, pool, namespace, "volume", 0, isolation)
						}
					}
					existingName := "existing"
					if tc.scope == rbdmirror.ScopeImage {
						existingName = "volume"
					}
					harness.RBDScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, existingName, tc.scope == rbdmirror.ScopePool)
					harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, existingName, 0, before)
					harness.RBDScopeAssertUnmirrored(t, ctx, sourceClient, harness.RBDScopeImage(pool, tc.sourceNamespace, existingName))
					// A plain image in pool scope is intentionally not eligible for
					// journal enrollment, even while journaling images are mirrored.
					harness.RBDScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "plain", false)
					mirrorImage := harness.StaticFacts(source).Images.RBDAux
					link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", mirrorImage), rbdmirror.WithConfig(rbdmirror.Config{
						Source: source, Destination: destination, Pool: pool, Scope: tc.scope,
						SourceNamespace: tc.sourceNamespace, DestinationNamespace: tc.destinationNamespace,
					}))
					if link != nil {
						t.Cleanup(func() {
							cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
							defer stop()
							if t.Failed() && link.Container != nil {
								harness.MultiClusterLogContainer(t, cleanupCtx, link.Container)
							}
							if err := link.Terminate(cleanupCtx); err != nil {
								t.Errorf("terminate namespace RBD link: %v", err)
							}
						})
					}
					if tc.sourceNamespace != tc.destinationNamespace && harness.RBDNamespaceMappingUnsupported(t, ctx, source) {
						harness.RequireRBDNamespaceMappingRefused(t, err, source, [2]string{tc.sourceNamespace, tc.destinationNamespace})
						return
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
							// Ceph 19 has no init-only; the fixture prepares the
							// default namespace in image mode, which mirrors nothing
							// until an image is enabled explicitly.
							want := "init-only"
							if harness.RBDNamespaceMappingUnsupported(t, ctx, source) {
								want = "image"
							}
							if err := json.Unmarshal(harness.RBDOutput(t, ctx, site.client, "mirror", "pool", "info", pool, "--format", "json"), &base); err != nil || base.Mode != want {
								t.Fatalf("named mapping broadened default scope: mode=%s error=%v", base.Mode, err)
							}
						}
					}
					if tc.scope == rbdmirror.ScopePool {
						// Existing journaling images are enrolled when pool scope is
						// enabled. Future images enroll without EnableImage or snapshots.
						harness.RBDMirrorReplayReady(t, ctx, link, existingName, rbdmirror.ModeJournal, tc.sourceNamespace, tc.destinationNamespace)
						harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, existingName, before)
						harness.RBDScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, tc.sourceNamespace, tc.destinationNamespace, existingName, "journal")
						harness.RBDScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "volume", true)
						harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 0, before)
					} else {
						if err := link.EnableImage(ctx, "volume"); err != nil {
							t.Fatal(err)
						}
					}
					mirrorMode := rbdmirror.ModeJournal
					if tc.scope == rbdmirror.ScopeImage {
						mirrorMode = rbdmirror.ModeSnapshot
					}
					harness.RBDMirrorReplayReady(t, ctx, link, "volume", mirrorMode, tc.sourceNamespace, tc.destinationNamespace)
					harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, "volume", before)
					after := bytes.Clone(before)
					patch := harness.RBDMultiClusterPayload(256<<10, 179+index)
					copy(after[512<<10:], patch)
					harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 512<<10, patch)
					mode := "journal"
					if tc.scope == rbdmirror.ScopeImage {
						mode = "snapshot"
						harness.ExecCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", harness.RBDScopeImage(pool, tc.sourceNamespace, "volume"))
					}
					harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, "volume", after)
					harness.RBDScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, tc.sourceNamespace, tc.destinationNamespace, "volume", mode)
					harness.RBDScopeAssertUnmirrored(t, ctx, sourceClient, harness.RBDScopeImage(pool, tc.sourceNamespace, "plain"))
					code, _, err := harness.RBDMultiClusterExec(ctx, destinationClient, "rbd", "info", harness.RBDScopeImage(pool, tc.destinationNamespace, "plain"), "--format", "json")
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
							harness.RBDScopeIO(t, ctx, site.client, "read", site.fsid, pool, namespace, "volume", 0, isolation)
							harness.RBDScopeAssertUnmirrored(t, ctx, site.client, harness.RBDScopeImage(pool, namespace, "volume"))
						}
					}
					t.Logf("native %s scope %q -> %q: selected replica updated, sibling/default same-name images unchanged, plain image excluded, original pool IDs preserved, final sha256=%x", tc.scope, tc.sourceNamespace, tc.destinationNamespace, sha256.Sum256(after))
				})
			}
		})
	}
}
