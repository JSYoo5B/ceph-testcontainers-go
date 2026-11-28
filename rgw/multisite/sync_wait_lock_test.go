package multisite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

func assertRGWReadinessReleasesTopology(t *testing.T, f *Fixture, observed <-chan struct{}, wait func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- wait(ctx) }()
	select {
	case <-observed:
	case err := <-done:
		t.Fatalf("readiness wait ended before its first sample: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("readiness wait made no sample")
	}
	lockCtx, stop := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer stop()
	if err := clusterutil.LockMutex(lockCtx, &f.topologyMu); err != nil {
		cancel()
		waitErr := <-done
		t.Fatalf("readiness wait retained topology lock between samples: %v (wait: %v)", err, waitErr)
	}
	// Termination must invalidate a captured wait on its next sample.
	f.closed = true
	f.topologyMu.Unlock()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "terminated") {
			t.Fatalf("closed fixture remained in a readiness wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		waitErr := <-done
		t.Fatalf("readiness wait did not revalidate the fixture (wait: %v)", waitErr)
	}
}

func TestRGWReadinessWaitsReleaseTopologyBetweenSamples(t *testing.T) {
	t.Run("zone sync", func(t *testing.T) {
		f, cli, _, _ := newRGWSyncTestFixture()
		cli.metadataOutput = rgwReadyMetadata
		cli.humanOutput = strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 1 shards", 1)
		observed := make(chan struct{}, 1)
		cli.onExec = func(_ context.Context, args []string) {
			if syncTestCommand(args, "sync", "status") {
				select {
				case observed <- struct{}{}:
				default:
				}
			}
		}
		assertRGWReadinessReleasesTopology(t, f, observed, func(ctx context.Context) error {
			_, err := f.WaitSyncReady(ctx, "destination", "source")
			return err
		})
	})
	t.Run("bucket sync", func(t *testing.T) {
		f, cli, group := newRGWSyncBucketStatusFixture(t)
		cli.bucketStatusOutput = strings.Replace(rgwReadyBucketStatus, `"behind_shards":[]`, `"behind_shards":[{"shard_id":0,"shard_marker":"lag"}]`, 1)
		observed := make(chan struct{}, 1)
		cli.onExec = func(_ context.Context, args []string) {
			if syncTestCommand(args, "bucket", "sync", "status") {
				select {
				case observed <- struct{}{}:
				default:
				}
			}
		}
		assertRGWReadinessReleasesTopology(t, f, observed, func(ctx context.Context) error {
			_, err := group.WaitBucketSyncReady(ctx, "selected", "source", "destination")
			return err
		})
	})
	t.Run("bucket policy", func(t *testing.T) {
		f, cli, group := newRGWPolicyImportFixture(t)
		cli.policyLagReads = 100
		observed := make(chan struct{}, 1)
		cli.onLocalPolicyRead = func() {
			select {
			case observed <- struct{}{}:
			default:
			}
		}
		assertRGWReadinessReleasesTopology(t, f, observed, func(ctx context.Context) error {
			_, err := group.WaitBucketPolicyReady(ctx, "destination")
			return err
		})
	})
}
