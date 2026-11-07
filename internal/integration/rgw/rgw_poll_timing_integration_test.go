//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_poll_timing)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/testcontainers/testcontainers-go"
)

// pollTimingAggregateImage fixes this comparison cohort to one native release
// and supplies the Ceph CLI used only by its live admin-socket probes. Every
// role in this qualifier uses this image, regardless of the job's role inputs.
const pollTimingAggregateImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// TestRGWIncrementalPollTimingQualification compares caught-up incremental
// work with native notifications enabled. It does not change sync lease
// ownership, suppress notifications, restart gateways or call initial full
// synchronization an incremental-poll saving. New-bucket first-object work
// is reported separately from later overwrite/delete/reverse phases.
// Already scheduled waits can
// retain their previous deadline after an effective configuration update.
func TestRGWIncrementalPollTimingQualification(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
			defer cancel()
			bootstrap := time.Now()
			options := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
			if mode.Host {
				options = append(options, ceph.WithHostNetwork())
			}
			source, destination := pollTimingAggregatePair(t, ctx, options...)
			image := pollTimingAggregateImage
			link, err := multisite.Run(ctx, image, multisite.WithConfig(multisite.Config{Source: source, Destination: destination, SourceZone: "source", DestinationZone: "destination"}))
			if link != nil {
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
					defer stop()
					if err := link.Terminate(cleanup); err != nil {
						t.Errorf("terminate timing fixture: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			gateways := []*rgw.Gateway{link.Source, link.Destination}
			clients := make([]harness.S3HTTPClient, 2)
			for i, gateway := range gateways {
				endpoint, err := gateway.S3Endpoint(ctx)
				if err != nil {
					t.Fatal(err)
				}
				clients[i] = harness.S3HTTPClient{Endpoint: endpoint, AccessKey: link.Source.AccessKey, SecretKey: link.Source.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
				pollTimingEffective(t, ctx, gateway, 20)
			}
			createMultisiteBucket(t, ctx, link, clients[0], "/tc-poll-warmup", nil)
			warmup := []byte("native incremental polling warmup\n")
			clients[0].Request(t, ctx, http.MethodPut, "/tc-poll-warmup/seed", warmup, http.StatusOK)
			waitMultisiteObject(t, ctx, clients[1], "/tc-poll-warmup/seed", http.StatusOK, warmup)
			pollTimingCaughtUp(t, ctx, link)
			facts, err := ceph.FactsOf(source)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("RGW_POLL_BOOTSTRAP ceph=%s elapsed=%s", facts.CephVersion, time.Since(bootstrap))
			for trial, seconds := range []int{20, 1, 20, 1, 20, 1} {
				t.Run(fmt.Sprintf("trial-%d-poll-%d", trial, seconds), func(t *testing.T) {
					started := time.Now()
					var changes []*override.ConfigHandle
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
						defer stop()
						var restoreErr error
						for i := len(changes) - 1; i >= 0; i-- {
							restoreErr = errors.Join(restoreErr, changes[i].Restore(cleanup))
						}
						if restoreErr != nil {
							t.Error("restore polling configuration", restoreErr)
						}
						for _, gateway := range gateways {
							pollTimingEffective(t, cleanup, gateway, 20)
						}
						t.Logf("RGW_POLL_TOTAL ceph=%s trial=%d poll=%d elapsed=%s failed=%t", facts.CephVersion, trial, seconds, time.Since(started), t.Failed())
					}()
					for _, cluster := range []*ceph.Container{source, destination} {
						for _, name := range []string{"rgw_meta_sync_poll_interval", "rgw_data_sync_poll_interval"} {
							change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "client.admin", Name: name, Value: strconv.Itoa(seconds)})
							changes = append(changes, change)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					for _, gateway := range gateways {
						pollTimingEffective(t, ctx, gateway, seconds)
					}
					workload := time.Now()
					bucket := fmt.Sprintf("/tc-poll-trial-%d", trial)
					phase := time.Now()
					createMultisiteBucket(t, ctx, link, clients[0], bucket, nil)
					t.Logf("RGW_POLL_PHASE ceph=%s trial=%d poll=%d phase=metadata elapsed=%s", facts.CephVersion, trial, seconds, time.Since(phase))
					payload := bytes.Repeat([]byte("incremental replication payload\n"), 1024)
					phase = time.Now()
					clients[0].Request(t, ctx, http.MethodPut, bucket+"/forward", payload, http.StatusOK)
					waitMultisiteObject(t, ctx, clients[1], bucket+"/forward", http.StatusOK, payload)
					t.Logf("RGW_POLL_PHASE ceph=%s trial=%d poll=%d phase=create elapsed=%s", facts.CephVersion, trial, seconds, time.Since(phase))
					replacement := bytes.Repeat([]byte("overwritten incremental payload\n"), 2048)
					phase = time.Now()
					clients[0].Request(t, ctx, http.MethodPut, bucket+"/forward", replacement, http.StatusOK)
					waitMultisiteObject(t, ctx, clients[1], bucket+"/forward", http.StatusOK, replacement)
					t.Logf("RGW_POLL_PHASE ceph=%s trial=%d poll=%d phase=overwrite elapsed=%s", facts.CephVersion, trial, seconds, time.Since(phase))
					phase = time.Now()
					clients[0].Request(t, ctx, http.MethodDelete, bucket+"/forward", nil, http.StatusNoContent)
					waitMultisiteObject(t, ctx, clients[1], bucket+"/forward", http.StatusNotFound, nil)
					t.Logf("RGW_POLL_PHASE ceph=%s trial=%d poll=%d phase=delete elapsed=%s", facts.CephVersion, trial, seconds, time.Since(phase))
					phase = time.Now()
					clients[1].Request(t, ctx, http.MethodPut, bucket+"/reverse", payload, http.StatusOK)
					waitMultisiteObject(t, ctx, clients[0], bucket+"/reverse", http.StatusOK, payload)
					clients[1].Request(t, ctx, http.MethodDelete, bucket+"/reverse", nil, http.StatusNoContent)
					waitMultisiteObject(t, ctx, clients[0], bucket+"/reverse", http.StatusNotFound, nil)
					t.Logf("RGW_POLL_PHASE ceph=%s trial=%d poll=%d phase=reverse elapsed=%s", facts.CephVersion, trial, seconds, time.Since(phase))
					pollTimingCaughtUp(t, ctx, link)
					t.Logf("RGW_POLL_WORKLOAD ceph=%s trial=%d poll=%d elapsed=%s", facts.CephVersion, trial, seconds, time.Since(workload))
				})
			}
		})
	}
}

// pollTimingAggregatePair keeps the native qualification image explicit;
// harness.NewMultiClusterPair deliberately uses the job-selected role layout.
func pollTimingAggregatePair(t *testing.T, ctx context.Context, options ...testcontainers.ContainerCustomizer) (*ceph.Container, *ceph.Container) {
	t.Helper()
	clusters := pollTimingClusters(t, ctx, 2, options...)
	return clusters[0], clusters[1]
}

func pollTimingClusters(t *testing.T, ctx context.Context, count int, options ...testcontainers.ContainerCustomizer) []*ceph.Container {
	t.Helper()
	clusters := make([]*ceph.Container, count)
	for index := range clusters {
		opts := append([]testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{All: pollTimingAggregateImage})), ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip))}, options...)
		cluster, err := ceph.Run(ctx, pollTimingAggregateImage, opts...)
		if cluster != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
				defer stop()
				if err := cluster.Terminate(cleanup); err != nil {
					t.Errorf("terminate aggregate polling cluster: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[index] = cluster
	}
	states := make(map[string]bool, count)
	for _, cluster := range clusters {
		observer, err := ceph.Observe(cluster)
		if err != nil {
			t.Fatal(err)
		}
		state, err := observer.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if state.FSID == "" || states[state.FSID] {
			t.Fatal("polling qualification clusters are not independent")
		}
		states[state.FSID] = true
	}
	return clusters
}

func pollTimingCaughtUp(t *testing.T, ctx context.Context, link *multisite.Fixture) {
	t.Helper()
	for _, pair := range [][2]string{{"source", "destination"}, {"destination", "source"}} {
		if _, err := link.WaitSyncReady(ctx, pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func pollTimingEffective(t *testing.T, parent context.Context, gateway *rgw.Gateway, want int) {
	t.Helper()
	pollTimingSettings(t, parent, gateway, map[string]int{
		"rgw_meta_sync_poll_interval": want, "rgw_data_sync_poll_interval": want, "rgw_sync_lease_period": 30,
	})
}

func pollTimingStartupEffective(t *testing.T, ctx context.Context, gateway *rgw.Gateway) {
	t.Helper()
	pollTimingSettings(t, ctx, gateway, map[string]int{"rgw_meta_sync_poll_interval": 1, "rgw_data_sync_poll_interval": 1})
}

func pollTimingSettings(t *testing.T, parent context.Context, gateway *rgw.Gateway, settings map[string]int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	asok := gatewayAsok(t, ctx, gateway)
	for name, want := range settings {
		var last string
		for {
			raw := harness.ExecOutput(t, ctx, gateway, "ceph", "--admin-daemon", asok, "config", "get", name)
			var values map[string]any
			if err := json.Unmarshal([]byte(raw), &values); err != nil {
				t.Fatalf("gateway effective configuration: %v", err)
			}
			last = fmt.Sprint(values[name])
			value, err := strconv.ParseFloat(strings.TrimSpace(last), 64)
			if err == nil && value == float64(want) {
				t.Logf("RGW_POLL_EFFECTIVE option=%s value=%g", name, value)
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("effective %s=%s, want%d", name, last, want)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// TestRGWStartupPollTimingProfile exercises the public startup condition;
// the qualifier separately reads the running daemons through their admin sockets.
func TestRGWStartupPollTimingProfile(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
			if mode.Host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, _, _ := harness.NewMultiClusterPairWithContext(t, ctx, opts...)
			image, _ := harness.IntegrationImages(t)
			if role := os.Getenv("CEPH_TEST_RGW_IMAGE"); role != "" {
				image = role
			}
			link, err := multisite.Run(ctx, image, multisite.WithConfig(multisite.Config{Source: source, Destination: destination, SourceZone: "source", DestinationZone: "destination", Timing: multisite.TimingProfile{IncrementalSync: true}}))
			if link != nil {
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
					defer stop()
					if err := link.Terminate(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			// The qualification judges the gateway's radosgw binary, not the
			// cluster release.
			if link == nil || link.Source == nil || link.Source.Container == nil {
				t.Fatalf("startup timing profile retained no owned source gateway: fixture=%t error=%v", link != nil, err)
			}
			if version := harness.NativeBinaryRelease(t, ctx, link.Source.Container, "radosgw"); version != "20.2.4" {
				if err == nil || !strings.Contains(err.Error(), "qualified only") || !strings.Contains(err.Error(), "(observed "+version+")") {
					t.Fatalf("unqualified profile did not retain its partial fixture: radosgw=%s error=%v", version, err)
				}
				cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
				defer stop()
				if err := link.Terminate(cleanup); err != nil {
					t.Fatal("terminate unqualified profile fixture", err)
				}
				t.Logf("RGW startup timing: unqualified release %s refused and partial fixture cleaned up", version)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			clients := make([]harness.S3HTTPClient, 2)
			for i, gateway := range []*rgw.Gateway{link.Source, link.Destination} {
				endpoint, err := gateway.S3Endpoint(ctx)
				if err != nil {
					t.Fatal(err)
				}
				clients[i] = harness.S3HTTPClient{Endpoint: endpoint, AccessKey: link.Source.AccessKey, SecretKey: link.Source.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
				pollTimingStartupEffective(t, ctx, gateway)
				// Public Run has verified the actual binary version and PID1 arguments.
				args := harness.ExecOutput(t, ctx, gateway, "cat", "/proc/1/cmdline")
				if !strings.Contains(args, "--rgw-meta-sync-poll-interval\x001\x00") || !strings.Contains(args, "--rgw-data-sync-poll-interval\x001\x00") {
					t.Fatal("timing arguments missing from owned daemon")
				}
			}
			createMultisiteBucket(t, ctx, link, clients[0], "/tc-startup-poll", nil)
			payload := bytes.Repeat([]byte("startup profile bytes\n"), 1024)
			clients[0].Request(t, ctx, http.MethodPut, "/tc-startup-poll/forward", payload, http.StatusOK)
			waitMultisiteObject(t, ctx, clients[1], "/tc-startup-poll/forward", http.StatusOK, payload)
			replacement := []byte("incremental replacement\n")
			clients[0].Request(t, ctx, http.MethodPut, "/tc-startup-poll/forward", replacement, http.StatusOK)
			waitMultisiteObject(t, ctx, clients[1], "/tc-startup-poll/forward", http.StatusOK, replacement)
			clients[0].Request(t, ctx, http.MethodDelete, "/tc-startup-poll/forward", nil, http.StatusNoContent)
			waitMultisiteObject(t, ctx, clients[1], "/tc-startup-poll/forward", http.StatusNotFound, nil)
			clients[1].Request(t, ctx, http.MethodPut, "/tc-startup-poll/reverse", payload, http.StatusOK)
			waitMultisiteObject(t, ctx, clients[0], "/tc-startup-poll/reverse", http.StatusOK, payload)
			pollTimingCaughtUp(t, ctx, link)
			if err := link.Terminate(ctx); err != nil {
				t.Fatal(err)
			}
			// The startup flags own their lifetime; no timing entries should persist.
			for _, cluster := range []*ceph.Container{source, destination} {
				raw, err := cluster.Ceph(ctx, "config", "dump", "--format", "json")
				if err != nil {
					t.Fatal(err)
				}
				var entries []ceph.ConfigEntry
				if err := json.Unmarshal(raw, &entries); err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if entry.Name == "rgw_meta_sync_poll_interval" || entry.Name == "rgw_data_sync_poll_interval" {
						t.Fatal("startup profile persisted configuration")
					}
				}
			}
			t.Log("RGW startup timing: owned commandline, bidirectional bytes, overwrite, delete, cleanup and no persistent timing entries verified")
		})
	}
}

// TestRGWTopologyStartupTimingEffectiveValues checks the same startup profile
// on both initial gateways and on a new zone that inherits it from the topology.
func TestRGWTopologyStartupTimingEffectiveValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	clusters := pollTimingClusters(t, ctx, 3, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	link, err := multisite.Run(ctx, pollTimingAggregateImage, multisite.WithTopology(multisite.TopologyConfig{
		Zones:          []multisite.ZoneConfig{{Name: "source", Cluster: clusters[0]}, {Name: "destination", Cluster: clusters[1]}},
		MetadataMaster: "source", Timing: multisite.TimingProfile{IncrementalSync: true},
	}))
	if link != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, zone := range link.Zones() {
		pollTimingStartupEffective(t, ctx, zone.Gateway)
	}
	if _, err := link.AddZone(ctx, pollTimingAggregateImage, multisite.ZoneConfig{Name: "added", Cluster: clusters[2]}); err != nil {
		t.Fatal(err)
	}
	zones := link.Zones()
	if len(zones) != 3 {
		t.Fatalf("after AddZone: got %d owned gateways", len(zones))
	}
	for _, zone := range zones {
		pollTimingStartupEffective(t, ctx, zone.Gateway)
	}
}
