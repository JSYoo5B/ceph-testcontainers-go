//go:build all || (integration && multicluster && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"errors"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

func TestBucketRecreationEventKeepsTheBaselineIdentity(t *testing.T) {
	original := multisite.SyncBucketIdentity{Name: syncWatchBucket, ID: "original"}
	replacement := multisite.SyncBucketIdentity{Name: syncWatchBucket, ID: "replacement"}
	baseline := multisite.BucketSyncStatus{SourceBucket: original, DestinationBucket: original, CaughtUp: true}
	event := func(kind ceph.WatchEventKind, source, destination multisite.SyncBucketIdentity, err error) multisite.BucketSyncWatchEvent {
		e := multisite.BucketSyncWatchEvent{Status: multisite.BucketSyncStatus{SourceBucket: source, DestinationBucket: destination}, Err: err}
		e.Sequence, e.Kind = 7, kind
		return e
	}
	for name, e := range map[string]multisite.BucketSyncWatchEvent{
		"buffered original": event(ceph.WatchChanged, original, original, nil),
		"recreation error":  event(ceph.WatchGap, multisite.SyncBucketIdentity{}, multisite.SyncBucketIdentity{}, errors.New("recreated")),
	} {
		if err := checkBucketRecreationEvent(baseline, e); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, e := range map[string]multisite.BucketSyncWatchEvent{
		"replacement source":      event(ceph.WatchChanged, replacement, original, nil),
		"replacement destination": event(ceph.WatchChanged, original, replacement, nil),
		"replacement both":        event(ceph.WatchChanged, replacement, replacement, nil),
		"resumed":                 event(ceph.WatchResumed, original, original, nil),
	} {
		if err := checkBucketRecreationEvent(baseline, e); err == nil {
			t.Errorf("%s: %+v passed", name, e)
		}
	}
}
