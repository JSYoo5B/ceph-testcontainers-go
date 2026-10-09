//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func verifyObjects(t *testing.T, ctx context.Context, client testcontainers.Container, expected []byte) {
	t.Helper()
	for i := range 16 {
		name := fmt.Sprintf("object-%02d", i)
		execCommand(t, ctx, client, "rados", "-p", "tc-poc", "get", name, "/tmp/result")
		r, err := client.CopyFileFromContainer(ctx, "/tmp/result")
		if err != nil {
			t.Fatal(err)
		}
		actual, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("%s payload changed", name)
		}
	}
}

func logStatus(t *testing.T, ctx context.Context, cluster *ceph.Container, stage string) {
	t.Helper()
	s, err := cluster.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: health=%s osds=%d up=%d in=%d PGs=%d states=%+v", stage,
		s.Health.Status, s.OSDMap.NumOSDs, s.OSDMap.NumUpOSDs, s.OSDMap.NumInOSDs, s.PGMap.NumPGs, s.PGMap.PGsByState)
}

func cephCommand(t *testing.T, ctx context.Context, cluster *ceph.Container, args ...string) {
	t.Helper()
	if _, err := cluster.Ceph(ctx, args...); err != nil {
		t.Fatal(err)
	}
}

func execCommand(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) {
	t.Helper()
	code, r, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", strings.Join(args, " "), code, out)
	}
}

func osdContainers(cluster *ceph.Container) []testcontainers.Container {
	var result []testcontainers.Container
	for _, osd := range cluster.OSDs() {
		result = append(result, osd.Container)
	}
	return result
}
