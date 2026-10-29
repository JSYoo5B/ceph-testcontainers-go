//go:build integration && role_images && ci_optional && ci_batch_role_mirror_refresh

//ci: timeout=15m job-timeout=20 image-roles=essential,client,rbd-aux

package mirror

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// Synthetic stale bootstrap files isolate local repair from historical MON
// session failures. The policy reads must use the current constrained toolkit,
// even while both link setup clients and its daemon are stopped.
func TestRoleImagesMirrorRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	images := ceph.ImageSet{Essential: image("CEPH_TEST_ESSENTIAL_IMAGE"), Client: image("CEPH_TEST_CLIENT_IMAGE"), RBDAux: image("CEPH_TEST_RBD_AUX_IMAGE")}
	newCluster := func() *ceph.Container {
		t.Helper()
		c, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
		if c != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
				defer stop()
				if err := c.Terminate(cleanup); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		core, err := c.ControlContainer(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if code, _, err := core.Exec(ctx, []string{"sh", "-c", "! command -v rbd"}); err != nil || code != 0 {
			t.Fatalf("essential unexpectedly supplies rbd: exit %d %v", code, err)
		}
		if _, err := c.CreatePool(ctx, ceph.PoolConfig{Name: "role-refresh", PGNum: 1, Replicas: 1, MinSize: 1}); err != nil {
			t.Fatal(err)
		}
		if err := rbd.InitPool(ctx, c, "role-refresh"); err != nil {
			t.Fatal(err)
		}
		if _, err := rbd.CreateNamespace(ctx, c, "role-refresh", "selected"); err != nil {
			t.Fatal(err)
		}
		return c
	}
	source, destination := newCluster(), newCluster()
	facts, err := ceph.FactsOf(destination)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Run(ctx, facts.Images.RBDAux, WithConfig(Config{Source: source, Destination: destination, Pool: "role-refresh", SourceNamespace: "selected", DestinationNamespace: "selected"}))
	if m != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			// Setup clients must run for the existing native policy cleanup.
			for _, c := range []testcontainers.Container{m.sourceClient, m.destinationClient} {
				if c != nil {
					if state, err := c.State(cleanup); err == nil && state != nil && !state.Running {
						_ = c.Start(cleanup)
					}
				}
			}
			if err := m.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	read := func(c testcontainers.Container, path string) []byte {
		t.Helper()
		reader, err := c.CopyFileFromContainer(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatal(err)
		}
		return data
	}
	type target struct {
		container testcontainers.Container
		cluster   *ceph.Container
		keyPath   string
		config    []byte
		key       []byte
		id        string
	}
	targets := []target{
		{container: m.sourceClient, cluster: source, keyPath: "/etc/ceph/ceph.client.admin.keyring"},
		{container: m.destinationClient, cluster: destination, keyPath: "/etc/ceph/ceph.client.admin.keyring"},
		{container: m.Container, cluster: destination, keyPath: "/etc/ceph/rbd-mirror.keyring"},
	}
	bootstrap := regexp.MustCompile(`(?m)^mon host = .*$`)
	stopTimeout := time.Second
	for i := range targets {
		x := &targets[i]
		x.id, x.key = x.container.GetContainerID(), read(x.container, x.keyPath)
		original := read(x.container, "/etc/ceph/ceph.conf")
		if len(bootstrap.FindAll(original, -1)) != 1 {
			t.Fatal("fixture must have one explicit bootstrap setting")
		}
		x.config = append(bootstrap.ReplaceAll(original, []byte("mon host = v2:127.0.0.1:1/0")), []byte("\n# retained-private-setting\n[client]\nclient mount timeout = 37\n")...)
		if err := x.container.Stop(ctx, &stopTimeout); err != nil {
			t.Fatal(err)
		}
		if err := x.container.CopyToContainer(ctx, x.config, "/etc/ceph/ceph.conf", 0o644); err != nil {
			t.Fatal(err)
		}
	}
	identities := m.policyIdentities
	wrong := *identities
	wrong.source.selected.MirrorUUID = uuid.NewString()
	m.policyIdentities = &wrong
	err = m.RefreshMonitorConfig(ctx)
	m.policyIdentities = identities
	if err == nil || !strings.Contains(err.Error(), "identity changed; refusing monitor refresh") {
		t.Fatalf("native selected-policy guard was bypassed: %v", err)
	}
	for _, x := range targets {
		if !bytes.Equal(read(x.container, "/etc/ceph/ceph.conf"), x.config) {
			t.Fatal("identity refusal changed a bootstrap file")
		}
	}
	if err := m.RefreshMonitorConfig(ctx); err != nil {
		t.Fatal(err)
	}
	for _, x := range targets {
		observer, err := ceph.Observe(x.cluster)
		if err != nil {
			t.Fatal(err)
		}
		addresses, err := observer.MonitorBootstrapAddresses(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := bootstrap.ReplaceAll(x.config, []byte("mon host = "+addresses))
		state, err := x.container.State(ctx)
		if err != nil || state == nil || state.Running || x.container.GetContainerID() != x.id || !bytes.Equal(read(x.container, "/etc/ceph/ceph.conf"), want) || !bytes.Equal(read(x.container, x.keyPath), x.key) {
			t.Fatal("refresh changed private state/credentials or failed to repair a stopped target", err)
		}
	}
	for _, c := range []testcontainers.Container{m.sourceClient, m.destinationClient} {
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, x := range targets {
		out, err := exec.CommandContext(ctx, "docker", "container", "inspect", x.id).CombinedOutput()
		if err == nil || !strings.Contains(strings.ToLower(string(out)), "no such container") {
			t.Fatal("owned link container survived termination")
		}
	}
}
