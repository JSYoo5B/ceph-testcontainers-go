//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// testManagerTelegrafDelivery plays a metrics SUT that receives the telegraf
// module's line protocol at a receiver the test owns, over UDP and then
// TCP, with only existing primitives: override.SetConfig for
// mgr/telegraf/address and interval, override.SetMGRModule, and the active
// manager's container log. It parses every line on its own and identifies
// the cluster by its fsid tag and a pool by its native pool_id, never by the
// sender. It qualifies the module's semantics, not a telemetry pipeline:
//   - the module reads the address and interval once when it starts and
//     waits 10 seconds before its first pass, so the options are set
//     before it is enabled and a changed address needs a restart;
//   - a pass is one timestamp; over UDP each line is its own datagram, over
//     TCP each pass is one connection;
//   - a send failure is only logged: health stays unchanged, ceph telegraf
//     send still reports success, and passes during an outage are lost;
//   - after a failover the passes come from the new active manager's
//     address, after its 10-second start delay;
//   - disabling the module stops the passes.
func testManagerTelegrafDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newMGRModulesCluster(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2).WithPoolDefaults(1, 1)))
	observer := harness.ObserveCluster(cluster)
	managers := map[string]*ceph.ManagerContainer{}
	addresses := map[string]string{}
	for _, manager := range clusteraccess.Managers(t, cluster) {
		address, err := manager.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		managers[manager.DaemonName], addresses[address] = manager, manager.DaemonName
	}
	fsid := strings.TrimSpace(string(harness.MustCeph(t, ctx, cluster, "fsid")))
	healthCodes := func() []string {
		t.Helper()
		snapshot, err := observer.HealthDetails(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Sorted(maps.Keys(snapshot.Checks))
	}
	baselineCodes := healthCodes()

	// Registered first, so it is terminated after the module is restored.
	const udpPort, tcpPort = "8094", "8095"
	receiver := runModuleReceiver(t, ctx, cluster, "telegraf-sink", telegrafReceiverScript, udpPort, tcpPort)
	batches := func() []telegrafBatch {
		t.Helper()
		all, err := telegrafBatches(moduleReceiverRecords[telegrafRecord](t, ctx, receiver))
		if err != nil {
			t.Fatal(err)
		}
		return all
	}
	// awaitBatch waits for a complete batch first received after since that
	// accept takes. A batch is complete once a later one has begun or no line
	// has arrived for two seconds.
	awaitBatch := func(what string, since time.Time, limit time.Duration, accept func(telegrafBatch) error) telegrafBatch {
		t.Helper()
		deadline := time.Now().Add(limit)
		var last error
		for {
			all := batches()
			for i, batch := range all {
				if batch.Received < float64(since.UnixNano())/1e9 {
					continue
				}
				if i == len(all)-1 && time.Since(time.Unix(0, int64(batch.Received*1e9))) < 2*time.Second+time.Duration(len(batch.Points))*time.Millisecond {
					continue
				}
				if last = accept(batch); last == nil {
					return batch
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: no matching batch within %v: %v", what, limit, last)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	from := func(name, proto string) func(telegrafBatch) error {
		return func(b telegrafBatch) error {
			if err := checkTelegrafBatch(b, fsid); err != nil {
				return err
			}
			peer, err := b.Peer()
			if err != nil {
				return err
			}
			if addresses[peer] != name || b.Proto != proto {
				return fmt.Errorf("batch %d over %s from %s (mgr.%s), want %s from mgr.%s", b.Timestamp, b.Proto, peer, addresses[peer], proto, name)
			}
			return nil
		}
	}
	failures := func(name string) int {
		t.Helper()
		n := 0
		for _, line := range harness.ContainerLogLines(t, ctx, managers[name]) {
			if strings.Contains(line, "Failed to send statistics to Telegraf") {
				n++
			}
		}
		return n
	}

	var restores []func(context.Context) error
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for i := len(restores) - 1; i >= 0; i-- {
			if err := restores[i](cleanup); err != nil {
				t.Errorf("restore telegraf setting: %v", err)
			}
		}
	})
	set := func(name, value string) *override.ConfigHandle {
		t.Helper()
		change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "mgr/telegraf/" + name, Value: value})
		restores = append(restores, change.Restore)
		if err != nil {
			t.Fatal(name, err)
		}
		return change
	}
	enable := func() (*override.MGRModuleHandle, time.Time) {
		t.Helper()
		started := time.Now()
		module, err := override.SetMGRModule(ctx, cluster, "telegraf", true)
		restores = append(restores, module.Restore)
		if err != nil {
			t.Fatal("enable telegraf", err)
		}
		return module, started
	}

	set("interval", "5")
	address := set("address", "udp://"+receiver.Alias+":"+udpPort)
	module, enabled := enable()
	active := activeManager(t, ctx, cluster)
	first := awaitBatch("first UDP batch", enabled, time.Minute, from(active, "udp"))
	if len(first.Units) != len(first.Points) {
		t.Fatalf("UDP batch %d: %d lines in %d datagrams, want one line per datagram", first.Timestamp, len(first.Points), len(first.Units))
	}
	t.Logf("MGR_TELEGRAF first UDP batch %v after enabling: %d lines from mgr.%s, %d cluster, %d pool, %d daemon",
		time.Unix(0, int64(first.Received*1e9)).Sub(enabled).Round(time.Millisecond), len(first.Points), active,
		len(first.Select("ceph_cluster_stats", nil)), len(first.Select("ceph_pool_stats", nil)), len(first.Select("ceph_daemon_stats", nil)))

	// A controlled change: a new pool and writes to it, identified by the
	// native pool ID.
	const pool, objects = "tg-probe", 7
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	poolID := nativePoolID(t, ctx, cluster, pool)
	harness.ExecCommand(t, ctx, client, "sh", "-c", fmt.Sprintf(`head -c 4096 /dev/urandom > /tmp/tg && for i in $(seq 1 %d); do rados -p %s put tg-$i /tmp/tg || exit 1; done`, objects, pool))
	written := time.Now()
	tags := func(instance string) map[string]string {
		return map[string]string{"pool_id": poolID, "pool_name": pool, "type_instance": instance}
	}
	change := awaitBatch("batch with the writes", written, 2*time.Minute, func(b telegrafBatch) error {
		if err := from(active, "udp")(b); err != nil {
			return err
		}
		stored, err := b.Value("ceph_pool_stats", tags("objects"))
		if err != nil {
			return err
		}
		writes, err := b.Value("ceph_pool_stats", tags("wr"))
		if err != nil {
			return err
		}
		if stored != objects || writes < objects {
			return fmt.Errorf("pool %s: %d objects and %d writes, want %d and at least %d", poolID, stored, writes, objects, objects)
		}
		return nil
	})
	t.Logf("MGR_TELEGRAF pool %s id %s: %d objects and writes in batch %d, %v after the writes",
		pool, poolID, objects, change.Timestamp, time.Unix(0, int64(change.Received*1e9)).Sub(written).Round(time.Millisecond))

	// Receiver unavailable: the module only logs the failure.
	before := failures(active)
	receiver.SetAvailable(t, ctx, false)
	down := time.Now()
	if out := strings.TrimSpace(string(harness.MustCeph(t, ctx, cluster, "telegraf", "send"))); out != "Sending data to Telegraf" {
		t.Fatalf("telegraf send while the receiver is down answered %q", out)
	}
	time.Sleep(16 * time.Second)
	logged := failures(active) - before
	if logged == 0 {
		t.Fatalf("mgr.%s logged no send failure while the receiver was down", active)
	}
	if codes := healthCodes(); !slices.Equal(codes, baselineCodes) {
		t.Fatalf("health during the outage %v, want the baseline %v", codes, baselineCodes)
	}
	// The receiver reopens as soon as it sees the request, before
	// SetAvailable reads the new state back, so the outage ends at the
	// request: a pass can arrive in between and is not lost.
	up := time.Now()
	receiver.SetAvailable(t, ctx, true)
	resumed := awaitBatch("batch after the outage", up, 30*time.Second, from(active, "udp"))
	for _, batch := range batches() {
		if received := time.Unix(0, int64(batch.Received*1e9)); received.After(down) && received.Before(up) {
			t.Fatalf("batch %d received during the outage", batch.Timestamp)
		}
	}
	t.Logf("MGR_TELEGRAF outage of %v: %d logged send failures, health unchanged, next batch %v after the receiver returned with %d lines",
		up.Sub(down).Round(time.Millisecond), logged, time.Unix(0, int64(resumed.Received*1e9)).Sub(up).Round(time.Millisecond), len(resumed.Points))

	// Failover: the passes come from the promoted manager.
	standby := ""
	for name := range managers {
		if name != active {
			standby = name
		}
	}
	failed := time.Now()
	harness.MustCeph(t, ctx, cluster, "mgr", "fail", active)
	promoted := awaitBatch("batch from the promoted manager", failed, 2*time.Minute, from(standby, "udp"))
	lastOld := 0.0
	for _, batch := range batches() {
		if peer, _ := batch.Peer(); addresses[peer] == active {
			lastOld = max(lastOld, batch.Received)
		}
	}
	t.Logf("MGR_TELEGRAF failover: first batch from mgr.%s %v after mgr fail, %v after the last batch of mgr.%s, %d lines",
		standby, time.Unix(0, int64(promoted.Received*1e9)).Sub(failed).Round(time.Millisecond),
		time.Duration((promoted.Received-lastOld)*1e9).Round(time.Millisecond), active, len(promoted.Points))

	// Disabling stops the passes.
	if err := module.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	disabled := time.Now()
	time.Sleep(12 * time.Second)
	for _, batch := range batches() {
		if time.Unix(0, int64(batch.Received*1e9)).After(disabled.Add(time.Second)) {
			t.Fatalf("batch %d arrived %v after the module was disabled", batch.Timestamp, time.Unix(0, int64(batch.Received*1e9)).Sub(disabled))
		}
	}

	// TCP: the address is read when the module starts, so it changes while
	// the module is disabled.
	if err := address.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	set("address", "tcp://"+receiver.Alias+":"+tcpPort)
	_, enabled = enable()
	active = activeManager(t, ctx, cluster)
	tcp := awaitBatch("first TCP batch", enabled, time.Minute, from(active, "tcp"))
	if len(tcp.Units) != 1 {
		t.Fatalf("TCP batch %d arrived over %d connections, want one", tcp.Timestamp, len(tcp.Units))
	}
	t.Logf("MGR_TELEGRAF first TCP batch %v after enabling: %d lines from mgr.%s in one connection",
		time.Unix(0, int64(tcp.Received*1e9)).Sub(enabled).Round(time.Millisecond), len(tcp.Points), active)

	// The sender stops before the receiver goes: the restorations run here,
	// and the receiver is terminated in cleanup.
	for i := len(restores) - 1; i >= 0; i-- {
		if err := restores[i](ctx); err != nil {
			t.Fatal("restore", err)
		}
	}
	restores = nil
	entries, err := observer.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, "mgr/telegraf/") {
			t.Fatalf("restoration left %+v", entry)
		}
	}
	modules, err := observer.MGRModules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range modules {
		if module.Name == "telegraf" && module.Enabled {
			t.Fatal("telegraf stays enabled after the restoration")
		}
	}
	stopped := time.Now()
	time.Sleep(12 * time.Second)
	for _, batch := range batches() {
		if time.Unix(0, int64(batch.Received*1e9)).After(stopped.Add(time.Second)) {
			t.Fatalf("batch %d arrived after the restoration", batch.Timestamp)
		}
	}
}
