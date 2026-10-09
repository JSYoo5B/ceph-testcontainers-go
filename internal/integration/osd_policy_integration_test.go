//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestOSDPolicies(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(4)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-osd-policy", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 16}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			probe := func(phase string) {
				t.Helper()
				execCommand(t, ctx, client, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,"-c",sys.argv[1],sys.argv[2]],timeout=30,check=True)`, osdPolicyProbe, phase)
			}
			probe("seed")
			original := cluster.OSDs()[0]
			containerID := original.GetContainerID()
			states, err := cluster.OSDStates(ctx)
			if err != nil || len(states) != 4 {
				t.Fatalf("initial native OSD states unavailable: %+v %v", states, err)
			}
			initial := states[0]
			if initial.ID != original.ID || !initial.Up || !initial.In || initial.UUID == "" {
				t.Fatalf("invalid initial OSD identity: %+v", initial)
			}
			if err := cluster.SetOSDIn(ctx, original.ID, false); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForPGClean(ctx); err != nil {
				t.Fatal("recover PGs while owned OSD remains out", err)
			}
			state := waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return state.Up && !state.In })
			if state.UUID != initial.UUID || state.Weight != 0 || original.GetContainerID() != containerID {
				t.Fatalf("out changed identity or container: %+v", state)
			}
			probe("out")
			if err := cluster.SetOSDIn(ctx, original.ID, true); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			state = waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return state.Up && state.In })
			if state.UUID != initial.UUID || state.Weight != initial.Weight || original.GetContainerID() != containerID {
				t.Fatalf("in changed identity or native map reweight: %+v", state)
			}
			probe("in")
			initialFlags, err := cluster.OSDFlags(ctx)
			if err != nil || slices.Contains(initialFlags, "noout") {
				t.Fatal("fresh noout=false state required for the auto-out positive control", err)
			}
			interval, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "mon", Name: "mon_osd_down_out_interval", Value: "3"})
			if interval != nil {
				t.Cleanup(func() {
					if err := interval.Restore(context.Background()); err != nil {
						t.Errorf("restore down/out interval: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			// Keep the other auto-out guards fixed across noout=true/false, so the
			// same down OSD is the positive control for the flag's native effect.
			var controlSettings []*ceph.ConfigOverride
			for _, setting := range []ceph.ConfigSetting{
				{Section: "mon", Name: "mon_osd_down_out_subtree_limit", Value: ""},
				{Section: "mon", Name: "mon_osd_adjust_down_out_interval", Value: "false"},
			} {
				change, err := cluster.TemporaryConfig(ctx, setting)
				if change != nil {
					controlSettings = append(controlSettings, change)
					t.Cleanup(func() {
						if err := change.Restore(context.Background()); err != nil {
							t.Errorf("restore auto-out control setting: %v", err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			noout, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
			if noout != nil {
				t.Cleanup(func() {
					if err := noout.Restore(context.Background()); err != nil {
						t.Errorf("restore noout: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			grace := 5 * time.Second
			if err := original.Stop(ctx, &grace); err != nil {
				t.Fatal(err)
			}
			state = waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return !state.Up })
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(6 * time.Second):
			}
			state = waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return !state.Up && state.In })
			if state.UUID != initial.UUID || !state.In {
				t.Fatalf("noout did not retain down OSD membership: %+v", state)
			}
			probe("down-noout")
			if err := noout.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			// With only noout changed, the monitor must now automatically mark
			// this same stopped OSD out; do not manually force the observation.
			state = waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return !state.Up && !state.In })
			if state.UUID != initial.UUID || state.Weight != 0 || original.GetContainerID() != containerID {
				t.Fatalf("native auto-out changed identity/container: %+v", state)
			}
			if err := cluster.WaitForPGClean(ctx); err != nil {
				t.Fatal("recover after native auto-out", err)
			}
			probe("down-autoout")
			if err := original.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cluster.SetOSDIn(ctx, original.ID, true); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			state = waitOSDPolicyState(t, ctx, cluster, original.ID, func(state ceph.OSDState) bool { return state.Up && state.In })
			if state.UUID != initial.UUID || original.GetContainerID() != containerID {
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
			cephCommand(t, ctx, cluster, "osd", "set", "noout")
			disabled, err := cluster.TemporaryOSDFlag(ctx, "noout", false)
			if err != nil {
				t.Fatal(err)
			}
			flags, err := cluster.OSDFlags(ctx)
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
			flags, err = cluster.OSDFlags(ctx)
			if err != nil || !slices.Contains(flags, "noout") {
				t.Fatal("original flag wasn't preserved", err)
			}
			cephCommand(t, ctx, cluster, "osd", "unset", "noout")
			t.Log("native RADOS: owned OSD out→recovered PGs→in, noout=true retains a stopped OSD in, noout restore allows native auto-out of the same OSD, restart preserves UUID/container/payload; original global flag and map reweight restored")
		})
	}
}
