//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_watch_subscriptions))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// osdMapMessage is an OSD map message a client logged as received.
type osdMapMessage struct {
	From        string
	First, Last uint64
}

// Watch/notify and MON subscriptions of native clients, with receipts: the
// acks, cookies and notify ids both sides report, a watcher that misses its
// ack, the error callbacks of a blocklisted watcher and of watchers whose
// object is removed, and the map, config and log subscriptions of a client
// together with the OSD map epochs, config changes, log entries and the
// MonMap and MgrMap epoch changes the MON delivers to it. The rados CLI, the
// ceph CLI, the client's admin socket and messenger log and the MON's own
// session dump read each result back.
func TestNativeWatchNotifyAndSubscriptions(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "tc-watch-subs"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 1, MinSize: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	type watcher struct {
		Client uint64 `json:"client"`
		Cookie uint64 `json:"cookie"`
	}
	type watchError struct {
		Cookie uint64 `json:"cookie"`
		Error  int    `json:"error"`
	}
	var watch struct {
		Notifier uint64             `json:"notifier"`
		Watchers map[string]watcher `json:"watchers"`
		AllAcked []struct {
			NotifyID uint64     `json:"notify_id"`
			Acks     [][]any    `json:"acks"`
			Timeouts [][]uint64 `json:"timeouts"`
		} `json:"all_acked"`
		MissedAck struct {
			Python struct {
				R int `json:"r"`
			} `json:"python"`
			CLI struct {
				Replies  [][]uint64 `json:"replies"`
				Timeouts [][]uint64 `json:"timeouts"`
			} `json:"cli"`
		} `json:"missed_ack"`
		Blocklisted struct {
			Cookie      uint64     `json:"cookie"`
			Error       int        `json:"error"`
			ListedAfter [][]uint64 `json:"listed_after"`
		} `json:"blocklisted"`
		ObjectRemoved map[string]watchError `json:"object_removed"`
	}
	receipt := watchSubscriptionPhase(t, ctx, cluster, client, &watch, "watch", pool)
	w1, w2, w3 := watch.Watchers["w1"], watch.Watchers["w2"], watch.Watchers["w3"]
	// Two notifies acknowledged by both watchers under distinct notify ids; a
	// silent third watcher is the one timeout of the next notify; the
	// blocklisted watcher's error callback names its own cookie, and the
	// object's removal is ENOTCONN on the remaining watches.
	if len(watch.AllAcked) != 2 || watch.AllAcked[0].NotifyID == watch.AllAcked[1].NotifyID ||
		len(watch.AllAcked[0].Acks) != 2 || len(watch.AllAcked[0].Timeouts) != 0 ||
		watch.MissedAck.Python.R != -110 || len(watch.MissedAck.CLI.Replies) != 2 ||
		len(watch.MissedAck.CLI.Timeouts) != 1 || !slices.Equal(watch.MissedAck.CLI.Timeouts[0], []uint64{w3.Client, w3.Cookie}) ||
		watch.Blocklisted.Cookie != w2.Cookie || (watch.Blocklisted.Error != -107 && watch.Blocklisted.Error != -108) ||
		len(watch.Blocklisted.ListedAfter) != 2 ||
		watch.ObjectRemoved["w1"] != (watchError{w1.Cookie, -107}) || watch.ObjectRemoved["w3"] != (watchError{w3.Cookie, -107}) {
		t.Fatalf("watch receipt: %s", receipt)
	}
	t.Logf("RADOS_WATCH_NOTIFY_RECEIPT %s", receipt)

	var subscriptions struct {
		GlobalID      uint64              `json:"global_id"`
		Subscriptions map[string][]string `json:"subscriptions"`
		Onetime       string              `json:"onetime_next_osdmap"`
		Epochs        struct {
			Before     uint64   `json:"before"`
			After      uint64   `json:"after"`
			Client     uint64   `json:"client_after"`
			MonSession []uint64 `json:"mon_session_after"`
		} `json:"epochs"`
		Received struct {
			OSDMap []osdMapMessage `json:"osd_map"`
			Config []int           `json:"config"`
			Log    []int           `json:"log"`
		} `json:"received"`
		Maps json.RawMessage `json:"maps"`
	}
	receipt = watchSubscriptionPhase(t, ctx, cluster, client, &subscriptions, "subscriptions", pool)
	e := subscriptions.Epochs
	// The client asked for the next OSD map once, and both it and the MON
	// report the bumped epoch as delivered; config and log subscriptions are
	// continuous and delivered their changes.
	if e.After <= e.Before || e.Client < e.After || len(e.MonSession) == 0 || slices.Max(e.MonSession) < e.After ||
		!slices.Contains(subscriptions.Subscriptions["osdmap"], subscriptions.Onetime) ||
		len(subscriptions.Received.Config) < 2 || len(subscriptions.Received.Log) == 0 ||
		!slices.ContainsFunc(subscriptions.Received.OSDMap, func(m osdMapMessage) bool {
			return strings.HasPrefix(m.From, "mon.") && m.First <= e.After && e.After <= m.Last
		}) {
		t.Fatalf("subscription receipt: %s", receipt)
	}
	t.Logf("MON_SUBSCRIPTION_RECEIPT %s", receipt)

	// The MonMap and MgrMap epochs that MON commands committed reach the
	// client over its MON session, as the map messages it logged, the MonMap
	// its MonClient decoded and the MgrMap versions its MgrClient took.
	type mgrRegistration struct {
		Epoch uint64   `json:"epoch"`
		GID   uint64   `json:"gid"`
		Addrs []string `json:"addrs"`
	}
	var maps struct {
		Via    string `json:"via"`
		MonMap struct {
			Before        uint64   `json:"before"`
			After         uint64   `json:"after"`
			Location      string   `json:"location"`
			Messages      []string `json:"mon_map_messages"`
			ClientDecoded []uint64 `json:"client_decoded"`
			ClientDump    []string `json:"client_dump"`
		} `json:"monmap"`
		MgrMap struct {
			Before    uint64          `json:"before"`
			Failed    mgrRegistration `json:"failed"`
			AfterFail struct {
				Epoch     uint64 `json:"epoch"`
				ActiveGID uint64 `json:"active_gid"`
			} `json:"after_fail"`
			After          mgrRegistration `json:"after"`
			Messages       [][]any         `json:"mgrmap_messages"`
			ClientVersions [][]any         `json:"client_versions"`
			ClientSession  string          `json:"client_session"`
		} `json:"mgrmap"`
	}
	if err := json.Unmarshal(subscriptions.Maps, &maps); err != nil {
		t.Fatalf("map delivery receipt: %v: %s", err, subscriptions.Maps)
	}
	mon, mgr := maps.MonMap, maps.MgrMap
	// delivered reports whether the client logged an mgrmap message of epoch
	// from its MON session; taken returns the active MGR that its MgrClient
	// logged for epoch.
	delivered := func(epoch uint64) bool {
		return slices.ContainsFunc(mgr.Messages, func(m []any) bool {
			return len(m) == 2 && m[0] == maps.Via && m[1] == float64(epoch)
		})
	}
	taken := func(epoch uint64) (string, bool) {
		for _, v := range mgr.ClientVersions {
			if len(v) == 2 && v[0] == float64(epoch) {
				active, _ := v[1].(string)
				return active, true
			}
		}
		return "", false
	}
	_, tookFail := taken(mgr.AfterFail.Epoch)
	active, tookAfter := taken(mgr.After.Epoch)
	if !strings.HasPrefix(maps.Via, "mon.") || mon.After != mon.Before+1 || len(mon.Messages) == 0 ||
		!slices.Contains(mon.ClientDecoded, mon.After) || !slices.Contains(mon.ClientDump, fmt.Sprintf("epoch %d", mon.After)) ||
		!slices.ContainsFunc(mon.ClientDump, func(l string) bool {
			return strings.HasSuffix(l, "; crush_location {datacenter="+mon.Location+"}")
		}) ||
		mgr.AfterFail.Epoch <= mgr.Before || mgr.AfterFail.ActiveGID == mgr.Failed.GID || mgr.After.Epoch < mgr.AfterFail.Epoch ||
		mgr.After.GID == 0 || mgr.After.GID == mgr.Failed.GID || len(mgr.After.Addrs) == 0 ||
		!delivered(mgr.AfterFail.Epoch) || !delivered(mgr.After.Epoch) || !tookFail || !tookAfter ||
		!slices.ContainsFunc(mgr.After.Addrs, func(a string) bool {
			return strings.Contains(active, a) && strings.Contains(mgr.ClientSession, a)
		}) ||
		slices.ContainsFunc(mgr.Failed.Addrs, func(a string) bool { return strings.Contains(active, a) }) {
		t.Fatalf("map delivery receipt: %s", subscriptions.Maps)
	}
	t.Logf("MAP_EPOCH_DELIVERY_RECEIPT %s", subscriptions.Maps)
}
