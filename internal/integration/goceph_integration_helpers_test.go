//go:build all || (integration && goceph)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

func goCephLinuxClusterPair(t *testing.T, mode, clientImage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	type outcome struct {
		index   int
		cluster *ceph.Container
		err     error
	}
	results := make(chan outcome, 2)
	for i := range 2 {
		go func() {
			opts := append([]testcontainers.ContainerCustomizer(nil), imageOpts...)
			opts = append(opts, ceph.WithOSDCount(2), ceph.WithStartupTimeout(5*time.Minute))
			if mode == "host" {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, err := ceph.Run(ctx, image, opts...)
			results <- outcome{i, cluster, err}
		}()
	}
	clusters := make([]*ceph.Container, 2)
	var failures []string
	for range 2 {
		result := <-results
		clusters[result.index] = result.cluster
		if result.cluster != nil {
			cluster := result.cluster
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				if err := cluster.Terminate(cleanupCtx); err != nil {
					t.Errorf("terminate cluster: %v", err)
				}
			})
		}
		if result.err != nil {
			failures = append(failures, fmt.Sprintf("cluster %d: %v", result.index, result.err))
		}
	}
	if len(failures) != 0 {
		t.Fatal(strings.Join(failures, "; "))
	}
	if mode == "bridge" && clusters[0].NetworkName() == clusters[1].NetworkName() {
		t.Fatal("bridge clusters share a network")
	}
	const pool = "tc-goceph"
	clients := make([]testcontainers.Container, 2)
	fsids := make([]string, 2)
	keyrings := make([][]byte, 2)
	filesystems := make([]string, 2)
	nativeArgs := make([][]string, 2)
	for i, cluster := range clusters {
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fsids[i] = status.FSID
		config, keyring, err := cluster.ConnectionConfig()
		if err != nil {
			t.Fatal(err)
		}
		keyrings[i] = keyring
		cephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "8")
		cephCommand(t, ctx, cluster, "osd", "pool", "set", pool, "pg_autoscale_mode", "off")
		cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", pool, "rbd")
		filesystem, err := cephfs.Start(ctx, cluster, cephfs.Config{})
		if err != nil {
			t.Fatal(err)
		}
		filesystems[i] = filesystem.FilesystemName
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		client, err := testcontainers.Run(ctx, clientImage, cluster.WithClient(),
			ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-x", "/usr/local/bin/go-ceph-probe"})),
		)
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
		inspect, err := client.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "host" && inspect.HostConfig.NetworkMode != "host" {
			t.Fatal("WithClient did not select host networking")
		}
		if mode == "host" {
			dir := t.TempDir()
			configPath, keyringPath := filepath.Join(dir, "ceph.conf"), filepath.Join(dir, "admin.keyring")
			for path, content := range map[string][]byte{configPath: config, keyringPath: keyring} {
				if err := os.WriteFile(path, content, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			nativeArgs[i] = []string{"--config", configPath, "--keyring", keyringPath}
		}
	}
	if fsids[0] == "" || fsids[0] == fsids[1] || bytes.Equal(keyrings[0], keyrings[1]) {
		t.Fatal("clusters lack distinct FSIDs or CephX credentials")
	}
	probe := func(index int, phase string) {
		args := []string{"--fsid", fsids[index], "--pool", pool, "--filesystem", filesystems[index],
			"--token", fmt.Sprintf("%s-cluster-%d", mode, index), "--phase", phase}
		goCephContainerProbe(t, ctx, clients[index], args, fsids[index], phase)
		if mode == "host" && phase == "verify" {
			// This Linux process shares the Docker host namespace. It exercises
			// ConnectionConfig without WithClient or a separate app container.
			goCephNativeProbe(t, ctx, append(append([]string(nil), nativeArgs[index]...), args...), fsids[index], phase)
		}
	}
	for i := range clients {
		probe(i, "seed")
	}
	for i := range clients {
		probe(i, "verify")
	}
	for i, cluster := range clusters {
		advanceServiceTopology(t, ctx, cluster)
		for j := range clients {
			probe(j, "verify")
		}
		t.Logf("go-ceph %s: cluster %d OSD 2 -> 3 -> 2; both clusters retained independent RADOS/RBD/CephFS bytes", mode, i)
	}
	for i := range clients {
		probe(i, "cleanup")
	}
	t.Logf("Linux go-ceph %s: distinct FSIDs=%v, CephX, RADOS, RBD snapshot, userspace CephFS, fresh sessions and cleanup passed", mode, fsids)
}

func goCephContainerProbe(t *testing.T, parent context.Context, client testcontainers.Container, args []string, fsid, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	command := append([]string{"/usr/local/bin/go-ceph-probe"}, args...)
	code, reader, err := client.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("go-ceph container probe %s exited %d: %s", phase, code, output)
	}
	goCephProbeResult(t, output, fsid, phase, "WithClient container")
}

func goCephNativeProbe(t *testing.T, parent context.Context, args []string, fsid, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/local/bin/go-ceph-probe", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Linux native go-ceph probe: %v: %s", err, output)
	}
	goCephProbeResult(t, output, fsid, phase, "Linux native process")
}

func goCephProbeResult(t *testing.T, output []byte, fsid, phase, location string) {
	t.Helper()
	type proof struct {
		Verified         bool   `json:"verified"`
		Deleted          bool   `json:"deleted"`
		Bytes            int    `json:"bytes"`
		RetainedSHA256   string `json:"retained_sha256"`
		FreshSHA256      string `json:"fresh_sha256"`
		SnapshotSHA256   string `json:"snapshot_sha256"`
		SnapshotIsolated bool   `json:"snapshot_isolated"`
		HeadRestored     bool   `json:"head_restored"`
	}
	var result struct {
		GoCeph string `json:"go_ceph"`
		CephX  bool   `json:"cephx"`
		FSID   string `json:"fsid"`
		Phase  string `json:"phase"`
		RADOS  proof  `json:"rados"`
		RBD    proof  `json:"rbd"`
		CephFS proof  `json:"cephfs"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode go-ceph probe: %v: %s", err, output)
	}
	if result.FSID != fsid || result.Phase != phase || result.GoCeph != "v0.41.0" || !result.CephX {
		t.Fatalf("unexpected go-ceph probe identity: %s", output)
	}
	for _, storage := range []proof{result.RADOS, result.RBD, result.CephFS} {
		if !storage.Verified || storage.Bytes == 0 || (phase == "cleanup" && !storage.Deleted) ||
			(phase != "cleanup" && len(storage.RetainedSHA256) != 64) ||
			(phase == "verify" && len(storage.FreshSHA256) != 64) {
			t.Fatalf("incomplete go-ceph storage verification: %s", output)
		}
	}
	if phase == "verify" && (!result.RBD.SnapshotIsolated || !result.RBD.HeadRestored ||
		result.RBD.SnapshotSHA256 != result.RBD.RetainedSHA256) {
		t.Fatalf("incomplete go-ceph RBD snapshot verification: %s", output)
	}
	t.Logf("%s: %s", location, bytes.TrimSpace(output))
}
