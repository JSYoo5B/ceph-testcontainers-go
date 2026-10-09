//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_mgr_prometheus))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// The active manager serves Prometheus metrics to WithClient containers. Host
// networking needs a bindable address and one port per manager, because the
// active and standby managers share the host network.
func TestManagerPrometheusExporter(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithManagerCount(2)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			managers := cluster.Managers()
			if len(managers) != 2 {
				t.Fatalf("two managers expected, got %d", len(managers))
			}
			// Each manager's expected listener, as host:port.
			expected := map[string]string{}
			var settings []ceph.ConfigSetting
			if host {
				ports := strings.Fields(execOutput(t, ctx, client, "python3", "-c", freeHostPorts, strconv.Itoa(len(managers))))
				settings = append(settings, ceph.ConfigSetting{Section: "mgr", Name: "mgr/prometheus/server_addr", Value: cluster.PublicAddress()})
				for i, manager := range managers {
					settings = append(settings, ceph.ConfigSetting{Section: "mgr", Name: "mgr/prometheus/" + manager.DaemonName + "/server_port", Value: ports[i]})
					expected[manager.DaemonName] = cluster.PublicAddress() + ":" + ports[i]
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
				change, err := cluster.TemporaryConfig(ctx, setting)
				if change != nil {
					restores = append(restores, change.Restore)
				}
				if err != nil {
					t.Fatal(setting.Name, err)
				}
			}
			module, err := cluster.TemporaryMGRModule(ctx, "prometheus", true)
			if module != nil {
				restores = append(restores, module.Restore)
			}
			if err != nil {
				t.Fatal("enable prometheus", err)
			}

			first := waitPrometheusService(t, ctx, cluster, "")
			scrapePrometheus(t, ctx, client, first)
			active := activeManager(t, ctx, cluster)
			if first.Host != expected[active] {
				t.Fatalf("active manager %s advertises %s, want %s", active, first.Host, expected[active])
			}
			mustCeph(t, ctx, cluster, "mgr", "fail")
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
				services, err := cluster.ManagerServices(deadline)
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
	status, err := cluster.ManagerStatus(ctx)
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
		services, err := cluster.ManagerServices(deadline)
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
	output := execOutput(t, ctx, client, "python3", "-c", `import sys,urllib.request
body=urllib.request.urlopen(sys.argv[1],timeout=10).read().decode()
assert 'ceph_health_status' in body and 'ceph_osd_up' in body,body[:200]
print(len(body))`, metrics)
	t.Logf("MGR_PROMETHEUS scraped=%s bytes=%s", metrics, output)
	if _, err := strconv.Atoi(output); err != nil {
		t.Fatalf("unexpected scrape output %q", output)
	}
}
