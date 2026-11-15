//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// testManagerPrometheusExporter checks that the active manager serves
// Prometheus metrics to WithClient containers, in host networking with one
// port per manager on a bindable address.
func testManagerPrometheusExporter(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, client := newMGRModulesCluster(t, ctx, opts...)
	managers := clusteraccess.Managers(t, cluster)
	if len(managers) != 2 {
		t.Fatalf("two managers expected, got %d", len(managers))
	}
	// Each manager's expected listener, as host:port.
	expected := map[string]string{}
	var settings []ceph.ConfigSetting
	if host {
		ports := strings.Fields(harness.ExecOutput(t, ctx, client, "python3", "-c", freeHostPorts, strconv.Itoa(len(managers))))
		settings = append(settings, ceph.ConfigSetting{Section: "mgr", Name: "mgr/prometheus/server_addr", Value: harness.StaticFacts(cluster).PublicAddress})
		for i, manager := range managers {
			settings = append(settings, ceph.ConfigSetting{Section: "mgr", Name: "mgr/prometheus/" + manager.DaemonName + "/server_port", Value: ports[i]})
			expected[manager.DaemonName] = harness.StaticFacts(cluster).PublicAddress + ":" + ports[i]
		}
	} else {
		for _, manager := range managers {
			address, err := manager.ContainerIP(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected[manager.DaemonName] = address + ":9283"
		}
	}
	var restores []func(context.Context) error
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		for i := len(restores) - 1; i >= 0; i-- {
			if err := restores[i](cleanup); err != nil {
				t.Errorf("restore prometheus setting: %v", err)
			}
		}
	})
	for _, setting := range settings {
		change, err := override.SetConfig(ctx, cluster, setting)
		restores = append(restores, change.Restore)
		if err != nil {
			t.Fatal(setting.Name, err)
		}
	}
	module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
	restores = append(restores, module.Restore)
	if err != nil {
		t.Fatal("enable prometheus", err)
	}

	first := waitPrometheusService(t, ctx, cluster, "")
	scrapePrometheus(t, ctx, client, first)
	active := activeManager(t, ctx, cluster)
	if first.Host != expected[active] {
		t.Fatalf("active manager %s advertises %s, want %s", active, first.Host, expected[active])
	}
	harness.MustCeph(t, ctx, cluster, "mgr", "fail")
	second := waitPrometheusService(t, ctx, cluster, first.Host)
	scrapePrometheus(t, ctx, client, second)
	promoted := activeManager(t, ctx, cluster)
	if promoted == active || second.Host != expected[promoted] {
		t.Fatalf("failover to %s advertises %s, want %s", promoted, second.Host, expected[promoted])
	}

	for i := len(restores) - 1; i >= 0; i-- {
		if err := restores[i](ctx); err != nil {
			t.Fatal("restore", err)
		}
	}
	restores = nil
	deadline, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	for {
		services, err := harness.ObserveCluster(cluster).ManagerServices(deadline)
		if _, found := services["prometheus"]; err == nil && !found {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatalf("prometheus service remained after restore: %v %v", services, err)
		case <-time.After(time.Second):
		}
	}
	t.Logf("MGR_PROMETHEUS active=%s url=%s failover=%s url=%s restored=true", active, first, promoted, second)
}

// Bind every port first so the same port is never returned twice.
const freeHostPorts = `import socket,sys
sockets=[socket.socket() for _ in range(int(sys.argv[1]))]
for s in sockets: s.bind(('',0))
print(' '.join(str(s.getsockname()[1]) for s in sockets))
`

func activeManager(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || !status.Available || status.ActiveName == "" {
		t.Fatal("active manager unavailable", err)
	}
	return status.ActiveName
}

func waitPrometheusService(t *testing.T, ctx context.Context, cluster *ceph.Container, previous string) *url.URL {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		services, err := harness.ObserveCluster(cluster).ManagerServices(deadline)
		if address, found := services["prometheus"]; err == nil && found {
			parsed, err := url.Parse(address)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Host != previous {
				return parsed
			}
		}
		select {
		case <-deadline.Done():
			t.Fatalf("prometheus service other than %q not advertised: %v %v", previous, services, err)
		case <-time.After(time.Second):
		}
	}
}

func scrapePrometheus(t *testing.T, ctx context.Context, client testcontainers.Container, service *url.URL) {
	t.Helper()
	metrics := service.JoinPath("metrics").String()
	output := harness.ExecOutput(t, ctx, client, "python3", "-c", `import sys,urllib.request
body=urllib.request.urlopen(sys.argv[1],timeout=10).read().decode()
assert 'ceph_health_status' in body and 'ceph_osd_up' in body,body[:200]
print(len(body))`, metrics)
	t.Logf("MGR_PROMETHEUS scraped=%s bytes=%s", metrics, output)
	if _, err := strconv.Atoi(output); err != nil {
		t.Fatalf("unexpected scrape output %q", output)
	}
}

// testManagerMetrics checks the samples Observer.ManagerMetrics returns
// through the control container.
func testManagerMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newMGRModulesCluster(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2).WithPoolDefaults(1, 1)))
	if _, err := harness.ObserveCluster(cluster).ManagerMetrics(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("metrics without the module: %v", err)
	}
	modules, err := harness.ObserveCluster(cluster).MGRModules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range modules {
		if module.Name == "prometheus" && module.Enabled {
			t.Fatal("ManagerMetrics enabled the prometheus module")
		}
	}
	module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := module.Restore(cleanup); err != nil {
			t.Errorf("restore prometheus: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	first := waitPrometheusService(t, ctx, cluster, "")
	before, err := harness.ObserveCluster(cluster).ManagerMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Types["ceph_pool_wr"] != "counter" || before.Types["ceph_pool_objects"] != "gauge" || len(before.Samples) < 100 {
		t.Fatalf("first scrape of %s: types %q and %q, %d samples", before.URL, before.Types["ceph_pool_wr"], before.Types["ceph_pool_objects"], len(before.Samples))
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if up, err := before.Value("ceph_osd_up", map[string]string{"ceph_daemon": fmt.Sprintf("osd.%d", osd.ID)}); err != nil || up != 1 {
			t.Fatalf("osd.%d up = %v, %v", osd.ID, up, err)
		}
	}
	lines := []string{}
	for _, name := range []string{"ceph_osd_up", "ceph_pool_objects", "ceph_health_status", "ceph_mgr_status"} {
		lines = append(lines, fmt.Sprintf("%s %s %v", name, before.Types[name], before.Select(name, nil)))
	}
	t.Logf("MGR_METRICS first scrape %s: %d samples; %s", before.URL, len(before.Samples), strings.Join(lines, "; "))

	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-metrics", PGNum: 1, Replicas: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	out := harness.MustCeph(t, ctx, cluster, "osd", "pool", "stats", "tc-metrics", "--format", "json")
	var stats []struct {
		PoolID int `json:"pool_id"`
	}
	if err := json.Unmarshal(out, &stats); err != nil || len(stats) != 1 {
		t.Fatalf("pool stats: %v: %s", err, out)
	}
	pool := map[string]string{"pool_id": strconv.Itoa(stats[0].PoolID)}
	// The new pool appears in a later scrape, with no writes yet.
	var start float64
	deadline := time.Now().Add(2 * time.Minute)
	for {
		current, err := harness.ObserveCluster(cluster).ManagerMetrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if start, err = current.Value("ceph_pool_wr", pool); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool %v never appeared: %v", pool, err)
		}
		time.Sleep(2 * time.Second)
	}
	const objects = 20
	harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf(`head -c 4096 /dev/urandom > /tmp/m && for i in $(seq 1 %d); do rados -p tc-metrics put object-$i /tmp/m || exit 1; done`, objects))
	var writes, stored float64
	deadline = time.Now().Add(2 * time.Minute)
	for {
		after, err := harness.ObserveCluster(cluster).ManagerMetrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		writes, _ = after.Value("ceph_pool_wr", pool)
		stored, _ = after.Value("ceph_pool_objects", pool)
		if writes-start >= objects && stored == objects {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool %v writes %v -> %v, objects %v", pool, start, writes, stored)
		}
		time.Sleep(2 * time.Second)
	}

	active := activeManager(t, ctx, cluster)
	harness.MustCeph(t, ctx, cluster, "mgr", "fail")
	second := waitPrometheusService(t, ctx, cluster, first.Host)
	var failover ceph.ManagerMetrics
	deadline = time.Now().Add(2 * time.Minute)
	for {
		failover, err = harness.ObserveCluster(cluster).ManagerMetrics(ctx)
		if err == nil && strings.Contains(failover.URL, second.Host) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no scrape of the new active manager at %s: %+v %v", second.Host, failover.URL, err)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("MGR_METRICS pool %v writes %v -> %v after %d writes, objects %v; failover from %s to %s, %d samples",
		pool, start, writes, objects, stored, active, failover.URL, len(failover.Samples))
}

// nativePoolID reads the pool's ID from ceph osd pool stats.
func nativePoolID(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string) string {
	t.Helper()
	out := harness.MustCeph(t, ctx, cluster, "osd", "pool", "stats", pool, "--format", "json")
	var stats []struct {
		PoolID int `json:"pool_id"`
	}
	if err := json.Unmarshal(out, &stats); err != nil || len(stats) != 1 {
		t.Fatalf("pool stats of %s: %v: %s", pool, err, out)
	}
	return strconv.Itoa(stats[0].PoolID)
}

// testManagerPrometheusScraper plays a monitoring SUT that scrapes the
// prometheus module from its own container and follows a failover through
// ManagerWatch events alone. HTTP 200 is not the evidence: the scraper
// identifies a pool by its native pool_id and requires that a write made
// after it reached the advertised endpoint shows up as a fresh increase of
// that pool's ceph_pool_wr there within a deadline. A standby answers
// /metrics with 200 and an empty body under either standby_behaviour, and a
// replay of the reference body is 200 with the pool; the same check refuses
// both. Observer.ManagerMetrics, which scrapes through the control container,
// is never used here.
func testManagerPrometheusScraper(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newMGRModulesCluster(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2).WithPoolDefaults(1, 1)))
	observer := harness.ObserveCluster(cluster)
	const port = "9385"
	addresses := map[string]string{}
	for _, manager := range clusteraccess.Managers(t, cluster) {
		address, err := manager.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		addresses[manager.DaemonName] = address + ":" + port
	}
	if len(addresses) != 2 {
		t.Fatalf("two managers expected, got %v", addresses)
	}
	modules, err := observer.MGRModules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	originalEnabled := enabledMGRModules(modules)
	// Registered first, so it is terminated after every restoration below.
	scraper := runModuleReceiver(t, ctx, cluster, "tc-scraper", "")
	scrape := func(service string) prometheusScrape {
		t.Helper()
		var result prometheusScrape
		out := harness.ExecOutput(t, ctx, scraper, "python3", "-I", "-c", prometheusScrapeScript, service)
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatalf("scrape of %s: %q: %v", service, out, err)
		}
		return result
	}
	// waitScrape scrapes service until accept takes the result.
	waitScrape := func(what, service string, limit time.Duration, accept func(prometheusScrape) error) (prometheusScrape, time.Duration) {
		t.Helper()
		started := time.Now()
		for {
			result := scrape(service)
			err := accept(result)
			if err == nil {
				return result, time.Since(started)
			}
			if time.Since(started) > limit {
				t.Fatalf("%s: %s within %v: %v", what, service, limit, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	var restores []func(context.Context) error
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for i := len(restores) - 1; i >= 0; i-- {
			if err := restores[i](cleanup); err != nil {
				t.Errorf("restore prometheus setting: %v", err)
			}
		}
	})
	set := func(name, value string) {
		t.Helper()
		change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "mgr/prometheus/" + name, Value: value})
		restores = append(restores, change.Restore)
		if err != nil {
			t.Fatal(name, err)
		}
	}
	// An owned port and a short collection interval, set before the module
	// starts so the listener starts once with them.
	set("server_port", port)
	set("scrape_interval", "5")
	module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
	restores = append(restores, module.Restore)
	if err != nil {
		t.Fatal("enable prometheus", err)
	}
	const pool = "tc-scrape"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	poolID := nativePoolID(t, ctx, cluster, pool)
	writeObjects := func(prefix string, n int) {
		t.Helper()
		harness.ExecCommand(t, ctx, client, "sh", "-c", fmt.Sprintf(`head -c 4096 /dev/urandom > /tmp/%[1]s && for i in $(seq 1 %[2]d); do rados -p %[3]s put %[1]s-$i /tmp/%[1]s || exit 1; done`, prefix, n, pool))
	}

	// The SUT learns the endpoint, and later its replacement, only from
	// ManagerWatch events.
	w, err := observer.WatchManagers(ctx, ceph.WatchConfig{Interval: time.Second, Buffer: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitManagerWatchEvent(t, w, &last, &gaps, time.Second, "baseline", func(e ceph.ManagerWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	advertised := baseline
	if !managerWatchSettled(baseline) {
		advertised = awaitManagerWatchEvent(t, w, &last, &gaps, 3*time.Minute, "service of an active manager with a standby", managerWatchSettled)
	}
	active, standby := advertised.Status.ActiveName, advertised.Status.Standbys[0].Name
	if host := mustURLHost(t, advertised.Services["prometheus"]); host != addresses[active] {
		t.Fatalf("mgr.%s advertises %s, want its own listener %s", active, host, addresses[active])
	}
	activeMetrics := mustURL(t, advertised.Services["prometheus"]).JoinPath("metrics").String()
	standbyMetrics := "http://" + addresses[standby] + "/metrics"

	// Writes made after the SUT reached the endpoint.
	const writes = 20
	oldTrace := &poolCounterTrace{start: time.Now()}
	reference, appeared := waitScrape("pool series at the active endpoint", activeMetrics, 2*time.Minute, func(s prometheusScrape) error {
		oldTrace.add(s, poolID, pool)
		return s.poolSeries(poolID, pool)
	})
	writeObjects("before", writes)
	fresh, delay := waitScrape("fresh writes at the active endpoint", activeMetrics, 2*time.Minute, func(s prometheusScrape) error {
		oldTrace.add(s, poolID, pool)
		return freshPoolWrites(reference, s, poolID, pool, writes)
	})
	if err := freshPoolWrites(reference, reference, poolID, pool, writes); err == nil {
		t.Fatal("a replay of the reference body passed the freshness check")
	}
	idle, _ := waitScrape("standby answer", standbyMetrics, time.Minute, func(s prometheusScrape) error {
		if s.Status != 200 {
			return fmt.Errorf("not answering: %s", s)
		}
		return nil
	})
	if idle.Bytes != 0 || freshPoolWrites(reference, idle, poolID, pool, writes) == nil {
		t.Fatalf("standby mgr.%s answered %s, want an empty 200 that the check refuses", standby, idle)
	}
	t.Logf("MGR_PROMETHEUS_SCRAPER before failover: mgr.%s at %s; pool %s id %s visible after %v; %d writes fresh %v after writing (%v -> %v); standby mgr.%s %s; ceph_pool_wr at mgr.%s %s",
		active, activeMetrics, pool, poolID, appeared.Round(time.Millisecond), writes, delay.Round(time.Millisecond), reference.Writes[poolID], fresh.Writes[poolID], standby, idle, active, oldTrace)

	// Failover: the scraper follows the advertisement in the watch events,
	// then writes again and requires the increase at the new endpoint.
	failed := time.Now()
	harness.MustCeph(t, ctx, cluster, "mgr", "fail", active)
	moved := awaitManagerWatchEvent(t, w, &last, &gaps, 3*time.Minute, "service of the promoted manager", func(e ceph.ManagerWatchEvent) bool {
		return e.Status.Available && e.Status.ActiveName == standby && e.Services["prometheus"] != "" && mustURLHost(t, e.Services["prometheus"]) != addresses[active]
	})
	if host := mustURLHost(t, moved.Services["prometheus"]); host != addresses[standby] {
		t.Fatalf("promoted mgr.%s advertises %s, want its own listener %s", standby, host, addresses[standby])
	}
	advertisedAfter := time.Since(failed)
	promotedMetrics := mustURL(t, moved.Services["prometheus"]).JoinPath("metrics").String()
	// The promoted manager rebuilds its PG statistics from the OSD reports it
	// receives, so it can serve the pool series before the first report with
	// a ceph_pool_wr below the old manager's (0 on Squid, observed a few
	// seconds after mgr fail); the counter is cumulative and returns to the
	// old value without a write. The scraper keeps scraping until the
	// counter is back at exactly that value and takes that answer as the
	// reference for the next writes, so a reset that never catches up, a
	// value no write explains and a missing post-promotion write all fail.
	promotedTrace := &poolCounterTrace{start: failed}
	served := fresh.Writes[poolID]
	reference2, answered := waitScrape("pool counter at the promoted endpoint back at the old manager's value", promotedMetrics, 2*time.Minute, func(s prometheusScrape) error {
		promotedTrace.add(s, poolID, pool)
		if err := s.poolSeries(poolID, pool); err != nil {
			return err
		}
		if got := s.Writes[poolID]; got != served {
			return fmt.Errorf("ceph_pool_wr{pool_id=%q} %v, want the %v mgr.%s served (sequence %s)", poolID, got, served, active, promotedTrace)
		}
		return nil
	})
	writeObjects("after", writes)
	fresh2, delay2 := waitScrape("fresh writes at the promoted endpoint", promotedMetrics, 2*time.Minute, func(s prometheusScrape) error {
		return freshPoolWrites(reference2, s, poolID, pool, writes)
	})
	if err := freshPoolWrites(reference2, reference2, poolID, pool, writes); err == nil {
		t.Fatal("a replay of the promoted endpoint's reference body passed the freshness check")
	}
	demotedMetrics := "http://" + addresses[active] + "/metrics"
	demoted, back := waitScrape("demoted manager answering as a standby", demotedMetrics, 2*time.Minute, func(s prometheusScrape) error {
		if s.Status != 200 {
			return fmt.Errorf("not answering: %s", s)
		}
		return nil
	})
	if demoted.Bytes != 0 || freshPoolWrites(reference2, demoted, poolID, pool, writes) == nil {
		t.Fatalf("demoted mgr.%s answered %s, want an empty 200 that the check refuses", active, demoted)
	}
	t.Logf("MGR_PROMETHEUS_SCRAPER failover: advertised by mgr.%s at %s %v after mgr fail (watch event %d, %d gaps); counter back at %v %v later; %d writes fresh %v after writing (%v -> %v); demoted mgr.%s %s %v after the pool series; ceph_pool_wr at mgr.%s from mgr fail %s",
		standby, promotedMetrics, advertisedAfter.Round(time.Millisecond), moved.Sequence, gaps, served, answered.Round(time.Millisecond), writes, delay2.Round(time.Millisecond), reference2.Writes[poolID], fresh2.Writes[poolID], active, demoted, back.Round(time.Millisecond), standby, promotedTrace)

	// standby_behaviour=error changes the standby's index page only:
	// /metrics stays a 200 with an empty body (StandbyModule in
	// src/pybind/mgr/prometheus/module.py), so a scraper still has to check
	// the content.
	set("standby_behaviour", "error")
	errored, _ := waitScrape("standby error status", "http://"+addresses[active]+"/", time.Minute, func(s prometheusScrape) error {
		if s.Status != 500 {
			return fmt.Errorf("index answered %s, want HTTP 500", s)
		}
		return nil
	})
	still := scrape(demotedMetrics)
	if still.Status != 200 || still.Bytes != 0 {
		t.Fatalf("standby /metrics under standby_behaviour=error answered %s, want an empty 200", still)
	}
	t.Logf("MGR_PROMETHEUS_SCRAPER standby_behaviour=error: index %s, /metrics %s", errored, still)

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
		if strings.HasPrefix(entry.Name, "mgr/prometheus/") {
			t.Fatalf("restoration left %+v", entry)
		}
	}
	modules, err = observer.MGRModules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if enabled := enabledMGRModules(modules); !slices.Equal(enabled, originalEnabled) {
		t.Fatalf("enabled modules after restoration %v, want %v", enabled, originalEnabled)
	}
	if services, err := observer.ManagerServices(ctx); err != nil || services["prometheus"] != "" {
		t.Fatalf("prometheus service after restoration: %v %v", services, err)
	}
}

// enabledMGRModules lists the enabled module names in order.
func enabledMGRModules(modules []ceph.MGRModuleState) []string {
	var names []string
	for _, module := range modules {
		if module.Enabled {
			names = append(names, module.Name)
		}
	}
	slices.Sort(names)
	return names
}

func mustURL(t *testing.T, service string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(service)
	if err != nil || parsed.Host == "" {
		t.Fatalf("invalid service URL %q: %v", service, err)
	}
	return parsed
}
