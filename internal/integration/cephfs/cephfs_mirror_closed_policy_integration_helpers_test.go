//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// Only the first termination is refused; every other operation reaches the
// real native container. This models cleanup transport loss without deleting
// caller-owned clusters or their policy.
type cephFSMirrorCleanupFailure struct {
	testcontainers.Container
	fail bool
}

func (c *cephFSMirrorCleanupFailure) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if c.fail {
		return errors.New("injected mirror cleanup transport failure")
	}
	return c.Container.Terminate(ctx, opts...)
}

func cephFSCheckClosedPolicyAdmission(t *testing.T, ctx context.Context, mirror *cephfsmirror.Container, source, destination *ceph.Container, peerID string) {
	t.Helper()
	readPolicy := func() [2]any {
		t.Helper()
		var result [2]any
		for i, command := range []string{"ls", "peer_list"} {
			out, err := source.Ceph(ctx, "fs", "snapshot", "mirror", command, mirror.SourceFilesystem)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(out, &result[i]); err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	expected := readPolicy()
	owned := slices.Clone(mirror.Directories)
	daemon := mirror.Daemons()[0]
	wrapped := &cephFSMirrorCleanupFailure{Container: daemon.Container, fail: true}
	daemon.Container = wrapped
	// The registered fixture cleanup will retry even if an assertion fails.
	t.Cleanup(func() { wrapped.fail = false })
	if err := mirror.Terminate(ctx); err == nil {
		t.Fatal("cleanup failure control was not exercised")
	}
	for _, phase := range []string{"cleanup-error", "cleaned"} {
		if phase == "cleaned" {
			wrapped.fail = false
			if err := mirror.Terminate(ctx); err != nil {
				t.Fatal(err)
			}
		}
		mutations := map[string]func() error{
			"add":            func() error { return mirror.AddDirectory(ctx, "/closed-must-not-register") },
			"remove":         func() error { return mirror.RemoveDirectory(ctx, "/federation") },
			"peer":           func() error { return mirror.RemovePeer(ctx, peerID) },
			"tracked-add":    func() error { _, err := mirror.BeginDirectoryAddition(ctx, "/closed-must-not-register"); return err },
			"tracked-remove": func() error { _, err := mirror.BeginDirectoryRemoval(ctx, "/federation"); return err },
			"tracked-peer":   func() error { _, err := mirror.BeginPeerRemoval(ctx, peerID); return err },
		}
		for name, mutation := range mutations {
			if err := mutation(); err == nil || (!strings.HasPrefix(name, "tracked-") && !errors.Is(err, ceph.ErrInvalidRequest)) {
				t.Fatalf("%s %s refusal: %v", phase, name, err)
			}
		}
		if actual := readPolicy(); !reflect.DeepEqual(actual, expected) || !slices.Equal(owned, mirror.Directories) {
			t.Fatalf("%s changed native policy or fixture ownership", phase)
		}
		for _, cluster := range []*ceph.Container{source, destination} {
			if _, err := cluster.Ceph(ctx, "status", "--format", "json"); err != nil {
				t.Fatalf("%s caller-owned cluster unavailable: %v", phase, err)
			}
		}
		t.Logf("CEPHFS_MIRROR_CLOSED_POLICY phase=%s all six mutations refused; native directory and peer policy and local ownership unchanged; caller-owned clusters answer", phase)
	}
}
