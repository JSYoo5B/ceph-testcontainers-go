//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_mgr_prometheus))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

// The active manager serves Prometheus metrics to WithClient containers. Host
// networking needs a bindable address and one port per manager, because the
// active and standby managers share the host network.
func TestManagerPrometheusExporter(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2))}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceCluster(t, opts...)
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
		})
	}
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

// ManagerMetrics returns the active manager's samples: a pool's write counter
// grows with the writes to it and its object count matches, every OSD is up,
// and after a failover the new active manager answers. It never enables the
// module.
func TestManagerMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2).WithPoolDefaults(1, 1)))
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
