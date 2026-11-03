//go:build all || integration

package harness

import (
	"context"
	"io"
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func LogStatus(t *testing.T, ctx context.Context, cluster *ceph.Container, stage string) {
	t.Helper()
	s, err := ObserveCluster(cluster).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: health=%s osds=%d up=%d in=%d PGs=%d states=%+v", stage,
		s.Health.Status, s.OSDMap.NumOSDs, s.OSDMap.NumUpOSDs, s.OSDMap.NumInOSDs, s.PGMap.NumPGs, s.PGMap.PGsByState)
}

func CephCommand(t *testing.T, ctx context.Context, cluster *ceph.Container, args ...string) {
	t.Helper()
	if _, err := cluster.Ceph(ctx, args...); err != nil {
		t.Fatal(err)
	}
}

func ExecCommand(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) {
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

func OSDContainers(t testing.TB, cluster *ceph.Container) []testcontainers.Container {
	var result []testcontainers.Container
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		result = append(result, osd.Container)
	}
	return result
}
