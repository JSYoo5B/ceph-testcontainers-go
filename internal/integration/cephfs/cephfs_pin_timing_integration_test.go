//go:build native_regression && (all || (integration && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_pin_timing)))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// TestCephFSPinTimingQualification alternates the same native pin handoffs
// without changing polling, data, topology or concurrency. It is opt-in because
// balanced ABBA comparisons deliberately spend time at the ordinary daemon tick
// rate. Equal average order positions reduce a linear warm-up bias.
func TestCephFSPinTimingQualification(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
			defer cancel()
			options := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)), cephfs.WithFilesystems(cephfs.Config{Name: "pin-timing", ActiveMDS: 2})}
			if mode.Host {
				options = append(options, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceCluster(t, options...)
			fs := cephfs.Filesystems(cluster)[0]
			if err := fs.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			facts, err := ceph.FactsOf(cluster)
			if err != nil {
				t.Fatal(err)
			}
			group, err := subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: "handoff"})
			if err != nil {
				t.Fatal(err)
			}
			volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "payload", GroupName: group.Name})
			if err != nil {
				t.Fatal(err)
			}
			prior, err := volume.PinPolicy(ctx)
			if err != nil {
				t.Fatal(err)
			}
			bases := map[string]bool{path.Dir(volume.Path): true}
			cephFSPinIO(t, ctx, client, fs.FilesystemName, "write", []string{volume.Path})
			t.Log("PIN_TIMING_DESIGN order=ABBA ticks=5,1,1,5 handoffs=16 comparison=within-cluster")
			handoffs := map[int]time.Duration{}
			for trial, seconds := range []int{5, 1, 1, 5} {
				t.Run(fmt.Sprintf("trial-%d-tick-%d", trial, seconds), func(t *testing.T) {
					start := time.Now()
					status, err := fs.MDSStatus(ctx)
					if err != nil || len(status.Active) != 2 {
						t.Fatalf("MDS status: %+v %v", status, err)
					}
					var changes []*override.ConfigHandle
					var total time.Duration
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
						defer stop()
						for i := len(changes) - 1; i >= 0; i-- {
							if err := changes[i].Restore(cleanup); err != nil {
								t.Error(err)
							}
						}
						for _, mds := range status.Active {
							pinTimingEffective(t, cleanup, cluster, "mds."+mds.Name, 5)
						}
						t.Logf("PIN_TIMING_TRIAL ceph=%s tick=%d handoffs=%s scenario=%s failed=%t", facts.CephVersion, seconds, total, time.Since(start), t.Failed())
					}()
					if seconds == 1 {
						changes, err = fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})
						if err != nil {
							t.Fatal(err)
						}
						if len(changes) != 2 {
							t.Fatalf("profile handles=%d, want2", len(changes))
						}
					}
					for _, mds := range status.Active {
						if !mds.Owned {
							t.Fatal("unowned MDS")
						}
						pinTimingEffective(t, ctx, cluster, "mds."+mds.Name, seconds)
					}
					for _, rank := range []int{1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0} {
						started := time.Now()
						pin, err := volume.OverridePin(ctx, subvolume.PinSetting{Type: subvolume.PinExport, Value: float64(rank)})
						if err != nil {
							t.Fatal(err)
						}
						cephFSPinTrackRestore(t, pin)
						cephFSPinWaitAuthority(t, ctx, cluster, fs, bases, rank, subvolume.PinExport)
						elapsed := time.Since(started)
						total += elapsed
						cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", []string{volume.Path})
						if err := pin.Restore(ctx); err != nil {
							t.Fatal(err)
						}
						after, err := volume.PinPolicy(ctx)
						if err != nil || after.Inode != prior.Inode || after.ExportRank != prior.ExportRank {
							t.Fatalf("pin restore: %+v %v", after, err)
						}
						t.Logf("PIN_TIMING_HANDOFF ceph=%s tick=%d rank=%d elapsed=%s", facts.CephVersion, seconds, rank, elapsed)
					}
					handoffs[seconds] += total
				})
			}
			if !t.Failed() && (handoffs[1] <= 0 || handoffs[1] >= handoffs[5]) {
				t.Fatalf("balanced handoff totals: tick 1 %s, tick 5 %s", handoffs[1], handoffs[5])
			}
		})
	}
}

func pinTimingEffective(t *testing.T, parent context.Context, c *ceph.Container, daemon string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	var last string
	for {
		out, err := c.Ceph(ctx, "config", "show", daemon, "mds_tick_interval")
		last = fmt.Sprintf("%s: %s %v", daemon, strings.TrimSpace(string(out)), err)
		value, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if err == nil && parseErr == nil && value == float64(want) {
			t.Logf("PIN_TIMING_EFFECTIVE daemon=%s tick=%g", daemon, value)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("effective timing never became %d: %s", want, last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
