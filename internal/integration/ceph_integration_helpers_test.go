//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// poolApplicationHealthLags reports that a POOL_APP_NOT_ENABLED check names
// only pools whose OSDMap already records an application. The MGR computes
// the check from its PG digest, which can trail the OSDMap right after a pool
// is created; a named pool that really lacks an application fails the test.
func poolApplicationHealthLags(t *testing.T, ctx context.Context, cluster *ceph.Container, check json.RawMessage) bool {
	t.Helper()
	var detail struct {
		Detail []struct {
			Message string `json:"message"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(check, &detail); err != nil {
		t.Fatal("decode POOL_APP_NOT_ENABLED detail", err)
	}
	named := 0
	for _, item := range detail.Detail {
		pool, found := strings.CutPrefix(item.Message, "application not enabled on pool '")
		if !found {
			continue
		}
		pool = strings.TrimSuffix(pool, "'")
		data, err := cluster.Ceph(ctx, "osd", "pool", "application", "get", pool, "--format", "json")
		var applications map[string]json.RawMessage
		if err != nil || json.Unmarshal(data, &applications) != nil {
			t.Fatalf("read pool %q applications: %v", pool, err)
		}
		if len(applications) == 0 {
			t.Fatalf("pool %q has no application in the OSDMap", pool)
		}
		named++
	}
	if named == 0 {
		t.Fatalf("POOL_APP_NOT_ENABLED names no pool: %s", check)
	}
	return true
}

// messengerDumpArg tells native messenger probes whether the cluster's
// daemons answer messenger dump ("1"), which Ceph 20 added. On Ceph 19 the
// probes prove encryption from secure-only service modes and from client
// debug_ms READY log lines instead.
func messengerDumpArg(cluster *ceph.Container) string {
	if strings.HasPrefix(cluster.CephVersion(), "19.") {
		return "0"
	}
	return "1"
}
