//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// upgradePool is the pool of the MON replacement and upgrade scenarios.
const upgradePool = "rolling-upgrade"

// The rolling-upgrade PoC of issue 64 reads "ceph versions" with
// parseUpgradeInventory and compares it with an upgradeCheckpoint
// (upgrade_phase_observations_helpers_test.go). The readback below is its
// data oracle.

// upgradeReadback reads keys back and prints [{"key", "sha256"}] with an empty
// digest for a key that could not be read. Arguments: pool, timeout, keys.
const upgradeReadback = `import hashlib, json, rados, sys
pool, timeout, keys = sys.argv[1], sys.argv[2], sys.argv[3:]
out = []
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": timeout}) as c:
    with c.open_ioctx(pool) as io:
        for key in keys:
            try:
                data = io.read(key, 1 << 24)
                out.append({"key": key, "sha256": hashlib.sha256(data).hexdigest()})
            except rados.Error:
                out.append({"key": key, "sha256": ""})
print(json.dumps(out))`

// judgeUpgradeReadback returns the acknowledged keys that are missing or read
// back with another digest.
func judgeUpgradeReadback(acked []attachMutation, raw []byte) ([]string, error) {
	var read []struct {
		Key    string `json:"key"`
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(raw, &read); err != nil {
		return nil, fmt.Errorf("readback: %w", err)
	}
	got := map[string]string{}
	for _, r := range read {
		got[r.Key] = r.SHA256
	}
	var bad []string
	for _, m := range acked {
		if got[m.Key] != m.SHA256 {
			bad = append(bad, m.Key)
		}
	}
	return bad, nil
}

// upgradeRun is the cluster under upgrade or replacement, its client and the writes that were
// acknowledged so far.
type upgradeRun struct {
	cluster *ceph.Container
	client  testcontainers.Container
	acked   []attachMutation
	steps   []map[string]any
}

func (u *upgradeRun) versions(t *testing.T, ctx context.Context) upgradeInventory {
	t.Helper()
	raw, err := u.cluster.Ceph(ctx, "versions", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	v, err := parseUpgradeInventory(raw)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// step writes a batch, reads every acknowledged write back and checks the
// per-role layout. Every write must be acknowledged: the pool keeps two of
// three replicas through each replacement.
func (u *upgradeRun) step(t *testing.T, ctx context.Context, name string, want upgradeCheckpoint) {
	t.Helper()
	// A daemon's metadata reaches "ceph versions" after it reported to the
	// MONs, which can lag its own admin socket; read until the inventory
	// matches or the bound ends.
	deadline := time.Now().Add(2 * time.Minute)
	got := u.versions(t, ctx)
	for want.Check(got) != nil && time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)
		got = u.versions(t, ctx)
	}
	record := map[string]any{"step": name, "versions": got}
	u.steps = append(u.steps, record)
	if err := want.Check(got); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	u.write(t, ctx, name, record)
}

// write acknowledges a batch of writes, reads every acknowledged write back
// and records health, into record, which it logs. A step whose daemon layout
// is not a plain release count (a MON down after an interrupted replacement)
// calls it alone.
func (u *upgradeRun) write(t *testing.T, ctx context.Context, name string, record map[string]any) {
	t.Helper()
	if record == nil {
		record = map[string]any{"step": name}
		u.steps = append(u.steps, record)
	}
	out := harness.ExecOutput(t, ctx, u.client, "python3", "-c", blockTimedWrites, upgradePool, name, "8", strconv.Itoa(16<<10), "60")
	l, err := parseBlockTimedLedger(out)
	if err != nil {
		t.Fatalf("%s writes: %v: %s", name, err, out)
	}
	for _, m := range l.Ledger {
		if m.Outcome != mutAcked {
			t.Fatalf("%s: write %s not acknowledged (%v)", name, m.Key, l.Errnos)
		}
		u.acked = append(u.acked, m)
	}
	record["writes"], record["max_s"] = len(l.Ledger), slicesMax(l.Seconds)
	record["readback"] = u.readback(t, ctx, name)
	health, err := harness.ObserveCluster(u.cluster).HealthDetails(ctx)
	if err == nil {
		checks := []string{}
		for code := range health.Checks {
			checks = append(checks, code)
		}
		record["health"], record["checks"] = health.Status, checks
	}
	t.Logf("ROLLING_UPGRADE_STEP %s", mustMarshal(record))
}

func (u *upgradeRun) readback(t *testing.T, ctx context.Context, name string) int {
	t.Helper()
	keys := make([]string, 0, len(u.acked))
	for _, m := range u.acked {
		keys = append(keys, m.Key)
	}
	out := harness.ExecOutput(t, ctx, u.client, append([]string{"python3", "-c", upgradeReadback, upgradePool, "60"}, keys...)...)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	bad, err := judgeUpgradeReadback(u.acked, []byte(lines[len(lines)-1]))
	if err != nil || len(bad) != 0 {
		t.Fatalf("%s readback: %v missing or different: %v", name, err, bad)
	}
	return len(keys)
}

// monmapIdentity is the part of the monmap a replacement must not change: the
// epoch, the FSID and every member's name and address vector, in rank order.
func monmapIdentity(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	epoch, members := monmapParts(t, ctx, cluster)
	return strconv.Itoa(epoch) + " " + members
}

func monmapParts(t *testing.T, ctx context.Context, cluster *ceph.Container) (int, string) {
	t.Helper()
	raw, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		Epoch int    `json:"epoch"`
		FSID  string `json:"fsid"`
		Mons  []struct {
			Name        string          `json:"name"`
			PublicAddrs json.RawMessage `json:"public_addrs"`
		} `json:"mons"`
	}
	if err := json.Unmarshal(raw, &dump); err != nil || dump.Epoch < 1 || len(dump.Mons) == 0 {
		t.Fatalf("mon dump %s: %v", raw, err)
	}
	parts := []string{dump.FSID}
	for _, m := range dump.Mons {
		parts = append(parts, m.Name+"="+string(m.PublicAddrs))
	}
	return dump.Epoch, strings.Join(parts, " ")
}

func slicesMax(values []float64) float64 {
	m := 0.0
	for _, v := range values {
		m = max(m, v)
	}
	return m
}

func mustMarshal(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
