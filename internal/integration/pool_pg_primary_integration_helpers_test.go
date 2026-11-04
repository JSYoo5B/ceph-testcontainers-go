//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

const pgPrimaryPool = "pg-primary"

// pgPrimaryObservation is the client's own view of one object's PG: the
// OSDMap mapping that "ceph osd map" computes and the acting primary and
// state that the serving PG reports to "ceph pg query".
type pgPrimaryObservation struct {
	PGID           ceph.PGID
	Acting         []int
	ActingPrimary  int
	ServingPrimary int
	ActiveClean    bool
}

func pgPrimaryOracle(t *testing.T, ctx context.Context, client testcontainers.Container, object string) pgPrimaryObservation {
	t.Helper()
	var mapping struct {
		PGID          string `json:"pgid"`
		Acting        []int  `json:"acting"`
		ActingPrimary int    `json:"acting_primary"`
	}
	if err := json.Unmarshal([]byte(harness.ExecOutput(t, ctx, client, "timeout", "30", "ceph", "osd", "map", pgPrimaryPool, object, "--format", "json")), &mapping); err != nil || mapping.PGID == "" {
		t.Fatalf("decode osd map: %v", err)
	}
	var query struct {
		State string `json:"state"`
		Info  struct {
			Stats struct {
				ActingPrimary int `json:"acting_primary"`
			} `json:"stats"`
		} `json:"info"`
	}
	if err := json.Unmarshal([]byte(harness.ExecOutput(t, ctx, client, "timeout", "30", "ceph", "pg", mapping.PGID, "query", "--format", "json")), &query); err != nil || query.State == "" {
		t.Fatalf("decode pg query: %v", err)
	}
	tokens := strings.Split(query.State, "+")
	return pgPrimaryObservation{PGID: ceph.PGID(mapping.PGID), Acting: mapping.Acting, ActingPrimary: mapping.ActingPrimary,
		ServingPrimary: query.Info.Stats.ActingPrimary, ActiveClean: slices.Contains(tokens, "active") && slices.Contains(tokens, "clean")}
}

func pgPrimaryAwait(t *testing.T, ctx context.Context, client testcontainers.Container, object, phase string, ok func(pgPrimaryObservation) bool) pgPrimaryObservation {
	t.Helper()
	started := time.Now()
	for {
		observed := pgPrimaryOracle(t, ctx, client, object)
		if ok(observed) {
			t.Logf("%s after %v: %+v", phase, time.Since(started), observed)
			return observed
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: last %+v: %v", phase, observed, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// pgPrimaryMap is the stored OSDMap state that the override owns.
type pgPrimaryMap struct {
	Require string `json:"require_min_compat_client"`
	Entries []struct {
		PGID string `json:"pgid"`
		OSD  int    `json:"primary_osd"`
	} `json:"pg_upmap_primaries"`
}

func (m pgPrimaryMap) entry(pg ceph.PGID) (int, bool) {
	for _, entry := range m.Entries {
		if entry.PGID == string(pg) {
			return entry.OSD, true
		}
	}
	return 0, false
}

func pgPrimaryStoredMap(t *testing.T, ctx context.Context, cluster *ceph.Container) pgPrimaryMap {
	t.Helper()
	data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump pgPrimaryMap
	if err := json.Unmarshal(data, &dump); err != nil || dump.Require == "" {
		t.Fatalf("decode osd dump: %v", err)
	}
	return dump
}

func pgPrimaryPut(t *testing.T, ctx context.Context, client testcontainers.Container, object string, payload []byte) {
	t.Helper()
	if err := client.CopyToContainer(ctx, payload, "/tmp/"+object, 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", pgPrimaryPool, "put", object, "/tmp/"+object)
}

func pgPrimaryGet(t *testing.T, ctx context.Context, client testcontainers.Container, object string, want []byte) {
	t.Helper()
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", pgPrimaryPool, "get", object, "/tmp/"+object+".read")
	r, err := client.CopyFileFromContainer(ctx, "/tmp/"+object+".read")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("object %s read back %d bytes, want exactly %d: %v", object, len(got), len(want), err)
	}
}
