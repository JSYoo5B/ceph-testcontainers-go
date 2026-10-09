//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_mirror_scope))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
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
				scope                                       rbd.MirrorScope
			}{
				{"pool-default", "", "", rbd.MirrorScopePool},
				{"pool-named", "ns-a", "ns-b", rbd.MirrorScopePool},
				{"pool-named-to-default", "ns-a", "", rbd.MirrorScopePool},
				{"pool-default-to-named", "", "ns-b", rbd.MirrorScopePool},
				{"image-snapshot-named", "ns-a", "ns-b", rbd.MirrorScopeImage},
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
					if tc.scope == rbd.MirrorScopeImage {
						existingName = "volume"
					}
					rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, existingName, tc.scope == rbd.MirrorScopePool)
					rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, existingName, 0, before)
					rbdScopeAssertUnmirrored(t, ctx, sourceClient, rbdScopeImage(pool, tc.sourceNamespace, existingName))
					// A plain image in pool scope is intentionally not eligible for
					// journal enrollment, even while journaling images are mirrored.
					rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "plain", false)
					mirrorImage := source.ControlImage()
					link, err := rbd.RunMirror(ctx, mirrorImage, rbd.MirrorConfig{
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
					if tc.scope == rbd.MirrorScopePool {
						// Existing journaling images are enrolled when pool scope is
						// enabled. Future images enroll without EnableImage or snapshots.
						rbdMirrorReplayReady(t, ctx, link, existingName, rbd.MirrorModeJournal, tc.sourceNamespace, tc.destinationNamespace)
						rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, existingName, before)
						rbdScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, tc.sourceNamespace, tc.destinationNamespace, existingName, "journal")
						rbdScopeCreate(t, ctx, sourceClient, pool, tc.sourceNamespace, "volume", true)
						rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 0, before)
					} else {
						if err := link.EnableImage(ctx, "volume"); err != nil {
							t.Fatal(err)
						}
					}
					mirrorMode := rbd.MirrorModeJournal
					if tc.scope == rbd.MirrorScopeImage {
						mirrorMode = rbd.MirrorModeSnapshot
					}
					rbdMirrorReplayReady(t, ctx, link, "volume", mirrorMode, tc.sourceNamespace, tc.destinationNamespace)
					rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, tc.destinationNamespace, "volume", before)
					after := bytes.Clone(before)
					patch := rbdMultiClusterPayload(256<<10, 179+index)
					copy(after[512<<10:], patch)
					rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, tc.sourceNamespace, "volume", 512<<10, patch)
					mode := "journal"
					if tc.scope == rbd.MirrorScopeImage {
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
