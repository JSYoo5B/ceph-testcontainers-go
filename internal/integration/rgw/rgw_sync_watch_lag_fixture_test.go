//go:build all || (integration && multicluster && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

func TestSyncWatchDataLagRequiresTheOnlySourceBehind(t *testing.T) {
	stream := func(name string, behind int) multisite.DataSyncStatus {
		return multisite.DataSyncStatus{SourceZone: name, SourceZoneID: name + "-id", Syncing: true, Shards: 4, IncrementalShards: 4, BehindShards: behind}
	}
	event := func(ready bool, err error, sources ...multisite.DataSyncStatus) multisite.SyncWatchEvent {
		var e multisite.SyncWatchEvent
		e.Ready, e.Err = ready, err
		e.Status = multisite.SyncStatus{Zone: "destination", ZoneID: "destination-id", DataSources: sources}
		return e
	}
	if matched, evaluation, err := syncWatchDataLag(event(false, nil, stream("source", 2))); err != nil || !matched || evaluation.Stream.BehindShards != 2 {
		t.Fatalf("one behind source: matched=%t evaluation=%+v err=%v", matched, evaluation, err)
	}
	for name, e := range map[string]multisite.SyncWatchEvent{
		"extra source stream": event(false, nil, stream("source", 2), stream("other", 0)),
		"source caught up":    event(false, nil, stream("source", 0)),
		"no source stream":    event(false, nil, stream("other", 2)),
		"ready":               event(true, nil, stream("source", 2)),
		"observation error":   event(false, errors.New("read failed"), stream("source", 2)),
	} {
		if matched, _, err := syncWatchDataLag(e); err != nil || matched {
			t.Errorf("%s: matched=%t err=%v", name, matched, err)
		}
	}
	if _, _, err := syncWatchDataLag(event(false, nil, stream("destination", 2))); err == nil {
		t.Error("a stream from the receiving zone itself was accepted")
	}
}
