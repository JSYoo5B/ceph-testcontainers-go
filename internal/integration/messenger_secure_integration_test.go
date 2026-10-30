//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_secure))))

//ci: timeout=20m job-timeout=30 representative=topology image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func TestMessengerV2SecureOnly(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()), ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(512<<20)))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			} else {
				opts = append(opts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()))
			}
			cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
			if harness.StaticFacts(cluster).MessengerMode != ceph.MessengerV2Secure {
				t.Fatal("bootstrap mode accessor differs from selected policy")
			}
			if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := cluster.AddOSD(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-messenger", Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			initial, err := harness.PoolHandle(t, ctx, cluster, "tc-messenger").Status(ctx)
			if err != nil || initial.ID <= 0 {
				t.Fatal("native pool identity unavailable", err)
			}
			checkSecureMessengerTopology(t, ctx, cluster)
			secureMessengerClient(t, ctx, cluster, client, "seed")
			for _, monName := range []string{"b", "c"} {
				if _, err := cluster.AddMonitor(ctx, monName); err != nil {
					t.Fatal("secure MON growth", err)
				}
			}
			if err := cluster.RefreshClientMonitorConfig(ctx, client); err != nil {
				t.Fatal("refresh secure client bootstrap", err)
			}
			if _, err := cluster.AddManager(ctx, "b"); err != nil {
				t.Fatal("secure MGR growth", err)
			}
			if _, err := cluster.AddOSD(ctx); err != nil {
				t.Fatal("secure OSD growth", err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			checkSecureMessengerTopology(t, ctx, cluster)
			secureMessengerClient(t, ctx, cluster, client, "grown")
			// Use identical bootstrap and credentials with a single incompatible
			// mode. A watchdog timeout cannot satisfy the rejection assertion.
			negative := harness.TopologyExecOutput(t, ctx, client, "python3", "-c", secureMessengerCRCProbe)
			t.Logf("MSGR2_CRC_REFUSAL %s", negative)
			secureMessengerClient(t, ctx, cluster, client, "after-crc-refusal")
			final, err := harness.PoolHandle(t, ctx, cluster, "tc-messenger").Status(ctx)
			if err != nil || final.ID != initial.ID || final.Size != initial.Size || final.PGNum != initial.PGNum {
				t.Fatalf("secure topology changed pool identity/policy: %+v %v", final, err)
			}
			t.Logf("MSGR2_SECURE pool_id=%d monitor_count=%d manager_count=%d osd_count=%d native_io=true crc_refused=true", final.ID, len(clusteraccess.Monitors(t, cluster)), len(clusteraccess.Managers(t, cluster)), len(clusteraccess.OSDs(t, cluster)))
		})
	}
}

func TestMessengerV2SecureServices(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()), ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(512<<20)))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
			if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := cluster.AddOSD(ctx); err != nil {
					t.Fatal(err)
				}
			}
			fs, err := cephfs.Start(ctx, cluster, cephfs.Config{})
			if err != nil {
				t.Fatal("secure MDS bootstrap", err)
			}
			gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
			if err != nil {
				t.Fatal("secure RGW bootstrap", err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			data := harness.TopologyExecOutput(t, ctx, client, "python3", "-c", secureMessengerCephFSProbe, fs.FilesystemName, harness.MessengerDumpArg(t, ctx, cluster))
			t.Logf("MSGR2_CEPHFS %s", data)
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s3 := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}
			payload := bytes.Repeat([]byte("secure-rgw-native-data\x00"), 4096)
			s3.Request(t, ctx, http.MethodPut, "/tc-secure", nil, http.StatusOK)
			s3.Request(t, ctx, http.MethodPut, "/tc-secure/payload", payload, http.StatusOK)
			if actual := s3.Request(t, ctx, http.MethodGet, "/tc-secure/payload", nil, http.StatusOK); !bytes.Equal(actual, payload) {
				t.Fatal("secure RGW retained bytes differ")
			}
			quorum, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, mds := range fs.MDSs() {
				data := harness.TopologyExecOutput(t, ctx, harness.ToolkitContainer(t, cluster), "python3", "-c", secureMessengerDaemonProbe, "mds."+mds.ID, quorum.MonMap.FSID, "", harness.MessengerDumpArg(t, ctx, cluster))
				t.Logf("MSGR2_NATIVE %s", data)
			}
			inspection, err := gateway.Inspect(ctx)
			if err != nil || inspection.Config == nil || inspection.Config.Hostname == "" {
				t.Fatal("native RGW container identity unavailable", err)
			}
			frontend, err := gateway.DaemonEndpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// ServiceMap GID plus hostname/frontend binds server-side OSD
			// connections to this owned gateway, not an unrelated CLI client.
			data = harness.TopologyExecOutput(t, ctx, harness.ToolkitContainer(t, cluster), "python3", "-c", secureMessengerRGWProbe, inspection.Config.Hostname, frontend, harness.MessengerDumpArg(t, ctx, cluster))
			t.Logf("MSGR2_RGW %s", data)
			s3.Request(t, ctx, http.MethodDelete, "/tc-secure/payload", nil, http.StatusNoContent)
			s3.Request(t, ctx, http.MethodDelete, "/tc-secure", nil, http.StatusNoContent)
		})
	}
}
