package servicehook

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// The watch helpers refuse through Invalid, the shared construction of
// ceph.ErrInvalidRequest, and name the operation.
func TestWatchRefusalsAreInvalidRequests(t *testing.T) {
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -time.Second},
		"short interval":    {Interval: time.Millisecond},
		"negative timeout":  {QueryTimeout: -time.Second},
		"large buffer":      {Buffer: 4096},
	} {
		if _, err := WatchSettings("watch probe", cfg, time.Second); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.HasPrefix(err.Error(), "watch probe: ") {
			t.Errorf("%s: WatchSettings returned %v", name, err)
		}
	}
	err := WatchNotStarted("watch probe")
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch probe: the watch is not started by its Watch call") {
		t.Fatalf("WatchNotStarted returned %v", err)
	}
}

func TestWatchSettingsAndMeta(t *testing.T) {
	s, err := WatchSettings("watch probe", ceph.WatchConfig{}, 3*time.Second)
	if want, _ := watch.Validate(0, 0, 0, 3*time.Second); err != nil || s != want {
		t.Fatalf("default settings %+v, %v; want %+v", s, err, want)
	}
	started := time.Unix(10, 0)
	seq, kind, from, to := WatchMeta(watch.Meta{Sequence: 4, Kind: watch.Resumed, Started: started, Finished: started.Add(time.Second)})
	if seq != 4 || kind != ceph.WatchResumed || !from.Equal(started) || !to.Equal(started.Add(time.Second)) {
		t.Fatalf("meta %d %q %v %v", seq, kind, from, to)
	}
}
