//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

func TestOSDPolicies(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(4))}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceCluster(t, opts...)
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-osd-policy", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 16}); err != nil {
				t.Fatal(err)
			}
			if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			probe := func(phase string) {
				t.Helper()
				harness.ExecCommand(t, ctx, client, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,"-c",sys.argv[1],sys.argv[2]],timeout=30,check=True)`, osdPolicyProbe, phase)
			}
			probe("seed")
			osds := clusteraccess.OSDs(t, cluster)
			original := osds[0]
			containerID := original.GetContainerID()
			if len(osds) != 4 {
				t.Fatalf("owned OSDs: %d", len(osds))
			}
			states := requireOSDs(t, ctx, cluster, "initial OSDs", expect.OSDs{Select: osdSelection(osds), Up: true, In: true, Exact: true})
			initial := states[slices.IndexFunc(states, func(s ceph.OSDState) bool { return s.ID == original.ID })]
			// Pinning the initial UUID refuses a re-registration of the same ID.
			expect := func(up, in bool) expect.OSDs {
				return expect.OSDs{Select: []ceph.OSDState{initial}, Up: up, In: in}
			}
			if err := original.SetIn(ctx, false); err != nil {
				t.Fatal(err)
			}
			if err := harness.ObserveCluster(cluster).WaitForPGClean(ctx); err != nil {
				t.Fatal("recover PGs while owned OSD remains out", err)
			}
			state := waitOSDPolicyState(t, ctx, cluster, expect(true, false))
			if state.Weight != 0 || original.GetContainerID() != containerID {
				t.Fatalf("out changed identity or container: %+v", state)
			}
			probe("out")
			if err := original.SetIn(ctx, true); err != nil {
				t.Fatal(err)
			}
			if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			state = waitOSDPolicyState(t, ctx, cluster, expect(true, true))
			if state.Weight != initial.Weight || original.GetContainerID() != containerID {
				t.Fatalf("in changed identity or native map reweight: %+v", state)
			}
			probe("in")
			initialFlags, err := harness.ObserveCluster(cluster).OSDFlags(ctx)
			if err != nil || slices.Contains(initialFlags, "noout") {
				t.Fatal("fresh noout=false state required for the auto-out positive control", err)
			}
			interval, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mon", Name: "mon_osd_down_out_interval", Value: "3"})
			t.Cleanup(func() {
				if err := interval.Restore(context.Background()); err != nil {
					t.Errorf("restore down/out interval: %v", err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			// Keep the other auto-out guards fixed across noout=true/false, so the
			// same down OSD is the positive control for the flag's native effect.
			var controlSettings []*override.ConfigHandle
			for _, setting := range []ceph.ConfigSetting{
				{Section: "mon", Name: "mon_osd_down_out_subtree_limit", Value: ""},
				{Section: "mon", Name: "mon_osd_adjust_down_out_interval", Value: "false"},
			} {
				change, err := override.SetConfig(ctx, cluster, setting)
				controlSettings = append(controlSettings, change)
				t.Cleanup(func() {
					if err := change.Restore(context.Background()); err != nil {
						t.Errorf("restore auto-out control setting: %v", err)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
			t.Cleanup(func() {
				if err := noout.Restore(context.Background()); err != nil {
					t.Errorf("restore noout: %v", err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			grace := 5 * time.Second
			if err := original.Stop(ctx, &grace); err != nil {
				t.Fatal(err)
			}
			waitOSDPolicyState(t, ctx, cluster, expect(false, true))
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(6 * time.Second):
			}
			requireOSDs(t, ctx, cluster, "noout retains the stopped OSD in", expect(false, true))
			probe("down-noout")
			if err := noout.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			// With only noout changed, the monitor must now automatically mark
			// this same stopped OSD out; do not manually force the observation.
			state = waitOSDPolicyState(t, ctx, cluster, expect(false, false))
			if state.Weight != 0 || original.GetContainerID() != containerID {
				t.Fatalf("native auto-out changed identity/container: %+v", state)
			}
			if err := harness.ObserveCluster(cluster).WaitForPGClean(ctx); err != nil {
				t.Fatal("recover after native auto-out", err)
			}
			probe("down-autoout")
			if err := original.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := original.SetIn(ctx, true); err != nil {
				t.Fatal(err)
			}
			if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			state = waitOSDPolicyState(t, ctx, cluster, expect(true, true))
			if original.GetContainerID() != containerID {
				t.Fatal("OSD restart replaced owned identity/container")
			}
			if err := interval.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			for _, setting := range controlSettings {
				if err := setting.Restore(ctx); err != nil {
					t.Fatal(err)
				}
			}
			probe("restarted")
			// Preserve an original native flag, including the disable-and-restore path.
			harness.CephCommand(t, ctx, cluster, "osd", "set", "noout")
			disabled, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, false)
			if err != nil {
				t.Fatal(err)
			}
			flags, err := harness.ObserveCluster(cluster).OSDFlags(ctx)
			if err != nil || slices.Contains(flags, "noout") {
				t.Fatal("temporary disable not native", err)
			}
			copy := *disabled
			if err := copy.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if err := disabled.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			flags, err = harness.ObserveCluster(cluster).OSDFlags(ctx)
			if err != nil || !slices.Contains(flags, "noout") {
				t.Fatal("original flag wasn't preserved", err)
			}
			harness.CephCommand(t, ctx, cluster, "osd", "unset", "noout")
			t.Log("native RADOS: owned OSD out→recovered PGs→in, noout=true retains a stopped OSD in, noout restore allows native auto-out of the same OSD, restart preserves UUID/container/payload; original global flag and map reweight restored")
		})
	}
}
