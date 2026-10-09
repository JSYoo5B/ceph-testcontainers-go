//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_secure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
)

func TestMessengerV2SecureOnly(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithMessengerMode(ceph.MessengerV2Secure), ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(512<<20))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			} else {
				opts = append(opts, ceph.WithSeparateClusterNetwork())
			}
			cluster, client := newServiceClusterWithOptions(t, image, opts...)
			if cluster.MessengerMode() != ceph.MessengerV2Secure {
				t.Fatal("bootstrap mode accessor differs from selected policy")
			}
			if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
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
			initial, err := cluster.PoolStatus(ctx, "tc-messenger")
			if err != nil || initial.ID <= 0 {
				t.Fatal("native pool identity unavailable", err)
			}
			checkSecureMessengerTopology(t, ctx, cluster)
			secureMessengerClient(t, ctx, client, "seed")
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
			secureMessengerClient(t, ctx, client, "grown")
			// Use identical bootstrap and credentials with a single incompatible
			// mode. A watchdog timeout cannot satisfy the rejection assertion.
			negative := topologyExecOutput(t, ctx, client, "python3", "-c", secureMessengerCRCProbe)
			t.Logf("MSGR2_CRC_REFUSAL %s", negative)
			secureMessengerClient(t, ctx, client, "after-crc-refusal")
			final, err := cluster.PoolStatus(ctx, "tc-messenger")
			if err != nil || final.ID != initial.ID || final.Size != initial.Size || final.PGNum != initial.PGNum {
				t.Fatalf("secure topology changed pool identity/policy: %+v %v", final, err)
			}
			t.Logf("MSGR2_SECURE pool_id=%d monitor_count=%d manager_count=%d osd_count=%d native_io=true crc_refused=true", final.ID, len(cluster.Monitors()), len(cluster.Managers()), len(cluster.OSDs()))
		})
	}
}

func TestMessengerV2SecureServices(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithMessengerMode(ceph.MessengerV2Secure), ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(512<<20))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterWithOptions(t, image, opts...)
			if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
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
			data := topologyExecOutput(t, ctx, client, "python3", "-c", secureMessengerCephFSProbe, fs.FilesystemName)
			t.Logf("MSGR2_CEPHFS %s", data)
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s3 := s3HTTPClient{endpoint: endpoint, accessKey: gateway.AccessKey, secretKey: gateway.SecretKey, region: gateway.Region, http: &http.Client{Timeout: 30 * time.Second}}
			payload := bytes.Repeat([]byte("secure-rgw-native-data\x00"), 4096)
			s3.request(t, ctx, http.MethodPut, "/tc-secure", nil, http.StatusOK)
			s3.request(t, ctx, http.MethodPut, "/tc-secure/payload", payload, http.StatusOK)
			if actual := s3.request(t, ctx, http.MethodGet, "/tc-secure/payload", nil, http.StatusOK); !bytes.Equal(actual, payload) {
				t.Fatal("secure RGW retained bytes differ")
			}
			quorum, err := cluster.QuorumStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, mds := range fs.MDSs() {
				data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "python3", "-c", secureMessengerDaemonProbe, "mds."+mds.ID, quorum.MonMap.FSID, "")
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
			data = topologyExecOutput(t, ctx, cluster.ControlContainer(), "python3", "-c", secureMessengerRGWProbe, inspection.Config.Hostname, frontend)
			t.Logf("MSGR2_RGW %s", data)
			s3.request(t, ctx, http.MethodDelete, "/tc-secure/payload", nil, http.StatusNoContent)
			s3.request(t, ctx, http.MethodDelete, "/tc-secure", nil, http.StatusNoContent)
		})
	}
}
