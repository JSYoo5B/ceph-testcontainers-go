//go:build all || (integration && features)

package integration_test

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// awaitManagerWatchEvent reads events until match accepts one, and fails on
// a closed watch or after limit. Gaps are counted, not fatal.
func awaitManagerWatchEvent(t *testing.T, w *ceph.ManagerWatch, last *uint64, gaps *int, limit time.Duration, what string, match func(ceph.ManagerWatchEvent) bool) ceph.ManagerWatchEvent {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("%s: watch ended: %v", what, w.Stop())
			}
			if event.Sequence != *last+1 {
				t.Fatalf("%s: event %d follows %d", what, event.Sequence, *last)
			}
			*last = event.Sequence
			if event.Kind == ceph.WatchGap {
				*gaps++
				t.Logf("%s: gap at event %d after %v: %v", what, event.Sequence, event.Finished.Sub(event.Started), event.Err)
				continue
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s epoch %d: %+v services %v", what, event.Sequence, event.Kind, event.Epoch, event.Status, event.Services)
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

// managerWatchSettled accepts a map whose available active manager
// advertises the Prometheus service while one standby is registered.
func managerWatchSettled(e ceph.ManagerWatchEvent) bool {
	return e.Status.Available && e.Status.ActiveGID != 0 && len(e.Status.Standbys) == 1 && e.Services["prometheus"] != ""
}

func managerWatchStandbyGID(event ceph.ManagerWatchEvent, name string) (uint64, bool) {
	for _, standby := range event.Status.Standbys {
		if standby.Name == name {
			return standby.GID, true
		}
	}
	return 0, false
}

// managerWatchScrape plays the SUT reconnecting to the metrics endpoint: it
// fetches the advertised URL from the client container and requires a
// Prometheus exposition of the cluster.
func managerWatchScrape(t *testing.T, ctx context.Context, client testcontainers.Container, service string) {
	t.Helper()
	parsed, err := url.Parse(service)
	if err != nil {
		t.Fatal(err)
	}
	metrics := parsed.JoinPath("metrics").String()
	output := harness.ExecOutput(t, ctx, client, "python3", "-c", `import sys,urllib.request
body=urllib.request.urlopen(sys.argv[1],timeout=10).read().decode()
assert 'ceph_health_status' in body,body[:200]
print(len(body))`, metrics)
	if _, err := strconv.Atoi(output); err != nil {
		t.Fatalf("unexpected scrape output of %s: %q", metrics, output)
	}
}

// newPrometheusCluster starts a service cluster whose managers run the
// mgr-modules role image, with the integration images for the other daemons.
// The prometheus module is not a baseline module of the essential role, so
// the essential image need not ship it; the mgr-modules role does, and a
// batch of these tests declares it.
func newPrometheusCluster(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	images := harness.IntegrationImageSet(t)
	images.MGR = harness.MGRModulesImage(t)
	t.Logf("MGR slot image: %s", images.MGR)
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), harness.FunctionalOSDConfig(t)}, opts...)
	return harness.NewServiceClusterWithOptions(t, "", opts...)
}

// testManagerWatch stops the active one of two managers and observes the
// standby take over with its own GID, advertise the Prometheus service from
// its own address, and the stopped manager register again as a standby with
// a new GID. Each event proves only what one mgr dump of the original
// cluster reported; the scrape from the client proves that the advertised
// listener answers, and the container address proves whose listener it is.
func testManagerWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newPrometheusCluster(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2)))
	observer := harness.ObserveCluster(cluster)
	managers := clusteraccess.Managers(t, cluster)
	if len(managers) != 2 {
		t.Fatalf("two managers expected, got %d", len(managers))
	}
	addresses := map[string]string{}
	for _, manager := range managers {
		address, err := manager.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		addresses[manager.DaemonName] = address + ":9283"
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
		t.Fatal("enable prometheus", err)
	}
	// The cost of one read: the watch runs ceph fsid, mgr dump and ceph fsid.
	const samples = 10
	began := time.Now()
	for range samples {
		if _, err := cluster.Ceph(ctx, "fsid"); err != nil {
			t.Fatal(err)
		}
		if _, err := observer.ManagerStatus(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.Ceph(ctx, "fsid"); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d reads of fsid, mgr dump and fsid took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	watchStarted := time.Now()
	w, err := observer.WatchManagers(ctx, ceph.WatchConfig{}) // the 3-second default
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitManagerWatchEvent(t, w, &last, &gaps, time.Second, "baseline", func(e ceph.ManagerWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	t.Logf("baseline: epoch %d: %+v services %v, read %v", baseline.Epoch, baseline.Status, baseline.Services, baseline.Finished.Sub(baseline.Started))
	// Enabling a module can make the active manager respawn and register
	// again, so the active manager to stop is the one that advertises the
	// service while the other one stands by.
	advertised := baseline
	if !managerWatchSettled(baseline) {
		advertised = awaitManagerWatchEvent(t, w, &last, &gaps, 3*time.Minute, "service of an active manager with a standby", managerWatchSettled)
	}
	first := advertised.Status
	standby := first.Standbys[0].Name
	if first.ActiveName == standby || addresses[first.ActiveName] == "" || addresses[standby] == "" {
		t.Fatalf("the MGRMap names managers the fixture does not own: %+v", first)
	}
	if host := mustURLHost(t, advertised.Services["prometheus"]); host != addresses[first.ActiveName] {
		t.Fatalf("mgr.%s advertises %s, want %s", first.ActiveName, host, addresses[first.ActiveName])
	}
	managerWatchScrape(t, ctx, client, advertised.Services["prometheus"])
	t.Logf("settled: active mgr.%s GID %d, standby mgr.%s GID %d, prometheus at %s from epoch %d", first.ActiveName, first.ActiveGID, standby, first.Standbys[0].GID, advertised.Services["prometheus"], advertised.Epoch)

	stopped := harness.ManagerHandle(t, ctx, cluster, first.ActiveName)
	grace := 10 * time.Second
	if err := stopped.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	restored := false
	t.Cleanup(func() {
		if !restored {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_ = stopped.Start(cleanup)
		}
	})
	stoppedAt := time.Now()
	promoted := awaitManagerWatchEvent(t, w, &last, &gaps, 3*time.Minute, "promotion of the standby", func(e ceph.ManagerWatchEvent) bool {
		return e.Status.ActiveName == standby && e.Status.ActiveGID != 0
	})
	if promoted.Status.ActiveGID == first.ActiveGID || promoted.Epoch <= advertised.Epoch {
		t.Fatalf("promotion event = %+v", promoted)
	}
	t.Logf("promotion: event %d %s epoch %d, mgr.%s GID %d available %t, observed %v after the stop returned", promoted.Sequence, promoted.Kind, promoted.Epoch, standby, promoted.Status.ActiveGID, promoted.Status.Available, promoted.Finished.Sub(stoppedAt))
	moved := promoted
	if !moved.Status.Available || moved.Services["prometheus"] == "" {
		moved = awaitManagerWatchEvent(t, w, &last, &gaps, 2*time.Minute, "service of the promoted manager", func(e ceph.ManagerWatchEvent) bool {
			return e.Status.ActiveGID == promoted.Status.ActiveGID && e.Status.Available && e.Services["prometheus"] != ""
		})
	}
	// The event advances the test: the SUT reconnects to the URL that the
	// new active manager advertises, which must be its own listener.
	if host := mustURLHost(t, moved.Services["prometheus"]); host != addresses[standby] {
		t.Fatalf("promoted mgr.%s advertises %s, want %s", standby, host, addresses[standby])
	}
	managerWatchScrape(t, ctx, client, moved.Services["prometheus"])
	t.Logf("service moved: event %d epoch %d, prometheus at %s", moved.Sequence, moved.Epoch, moved.Services["prometheus"])

	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restored = true
	startedAt := time.Now()
	returned := awaitManagerWatchEvent(t, w, &last, &gaps, 3*time.Minute, "the restarted manager as a standby", func(e ceph.ManagerWatchEvent) bool {
		_, ok := managerWatchStandbyGID(e, first.ActiveName)
		return ok
	})
	if gid, _ := managerWatchStandbyGID(returned, first.ActiveName); gid == first.ActiveGID || returned.Status.ActiveGID != promoted.Status.ActiveGID {
		t.Fatalf("restarted standby event = %+v (the same name registers with a new GID)", returned)
	}
	t.Logf("standby again: event %d epoch %d, mgr.%s with a new GID, observed %v after the start returned", returned.Sequence, returned.Epoch, first.ActiveName, returned.Finished.Sub(startedAt))

	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	for event := range w.Events() {
		if event.Sequence != last+1 {
			t.Fatalf("event %d follows %d", event.Sequence, last)
		}
		last = event.Sequence
	}
	elapsed := time.Since(watchStarted)
	t.Logf("native MGR watch: baseline -> mgr.%s stopped -> mgr.%s promoted -> service moved and scraped -> mgr.%s standby with a new GID; %d events, %d gaps, watch ran %v at the 3s default interval",
		first.ActiveName, standby, first.ActiveName, last, gaps, elapsed.Round(time.Second))
}

func mustURLHost(t *testing.T, service string) string {
	t.Helper()
	parsed, err := url.Parse(service)
	if err != nil || parsed.Host == "" {
		t.Fatalf("invalid service URL %q: %v", service, err)
	}
	return parsed.Host
}
