//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

func ecFourTwoFacts(t *testing.T, ctx context.Context, cluster *ceph.Container, pool *ceph.Pool) int {
	t.Helper()
	profileRaw, err := cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", pool.ErasureCodeProfile, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]string
	if json.Unmarshal(profileRaw, &profile) != nil || profile["k"] != "4" || profile["m"] != "2" || profile["plugin"] != "jerasure" || profile["technique"] != "reed_sol_van" || profile["crush-failure-domain"] != "host" || profile["crush-root"] != "tc-ec-four-two" || profile["mapping"] != "" {
		t.Fatalf("unsupported 4+2 host profile: %s", profileRaw)
	}
	factsRaw, err := cluster.Ceph(ctx, "osd", "pool", "get", pool.Name, "all", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var facts map[string]any
	if json.Unmarshal(factsRaw, &facts) != nil || facts["size"] != float64(6) || facts["min_size"] != float64(pool.MinSize) || facts["allow_ec_overwrites"] != true {
		t.Fatalf("native pool policy differs: %s", factsRaw)
	}
	ruleRaw, err := cluster.Ceph(ctx, "osd", "crush", "rule", "dump", pool.CRUSHRule, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var rule struct{ Steps []struct{ Op, Type string } }
	if err := json.Unmarshal(ruleRaw, &rule); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(rule.Steps, func(s struct{ Op, Type string }) bool { return s.Op == "chooseleaf_indep" && s.Type == "host" }) {
		t.Fatalf("rule is not host-domain erasure placement: %s", ruleRaw)
	}
	locations := map[int]map[string]string{}
	hosts := map[string]bool{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		raw, err := cluster.Ceph(ctx, "osd", "find", strconv.Itoa(osd.ID), "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var found struct {
			OSD      int               `json:"osd"`
			Location map[string]string `json:"crush_location"`
		}
		if json.Unmarshal(raw, &found) != nil || found.OSD != osd.ID || found.Location["host"] != osd.Placement().Host || found.Location["root"] != "tc-ec-four-two" || hosts[found.Location["host"]] {
			t.Fatalf("six native CRUSH host domains unavailable: %s", raw)
		}
		hosts[found.Location["host"]] = true
		locations[osd.ID] = found.Location
	}
	if len(locations) != 6 {
		t.Fatalf("need six owned native host domains, have %d", len(locations))
	}
	detailRaw, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var details []struct {
		Name   string  `json:"pool_name"`
		Stripe *uint64 `json:"stripe_width"`
	}
	if err := json.Unmarshal(detailRaw, &details); err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(details, func(d struct {
		Name   string  `json:"pool_name"`
		Stripe *uint64 `json:"stripe_width"`
	}) bool {
		return d.Name == pool.Name
	})
	if index < 0 || details[index].Stripe == nil || *details[index].Stripe == 0 || *details[index].Stripe%4 != 0 || *details[index].Stripe > 256<<10 {
		t.Fatalf("unsupported bounded stripe geometry: %s", detailRaw)
	}
	stripe := *details[index].Stripe
	t.Logf("EC_FOUR_TWO_POOL %s", mustAdmissionJSON(t, map[string]any{"profile": profile, "pool": facts, "rule": json.RawMessage(ruleRaw), "logical_host_locations": locations, "stripe_width": stripe, "scope": "six logical CRUSH hosts on one Docker engine"}))
	return int(2*stripe + 31)
}

func ecFourTwoWait(t *testing.T, parent context.Context, cluster *ceph.Container, pool string, p ecFourTwoPlan, baseline ceph.PoolPGSnapshot, lost []int, floor uint32, primary int) ceph.PoolPGSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last error
	for {
		s, err := poolHandle(t, ctx, cluster, pool).PGs(ctx)
		if err == nil {
			last = checkECFourTwoPlacement(p, baseline, s, lost, floor, primary)
			if last == nil {
				return s
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("4+2 planned phase was not reached: %v", last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func testECFourTwoAdmission(t *testing.T, minSize int) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	layout := make([]ceph.OSDConfig, 6)
	for i := range layout {
		layout[i] = ceph.OSDConfig{Host: fmt.Sprintf("tc-ec-host-%d", i), Root: "tc-ec-four-two"}
	}
	cluster, client := newServiceCluster(t, ceph.WithInitialOSDs(layout...), ceph.WithDefaultCRUSHRoot("tc-ec-four-two"))
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-four-two", Application: "rados", PGNum: 1, MinSize: minSize, FailureDomain: "host", CRUSHRoot: "tc-ec-four-two", ErasureCode: &ceph.ErasureCodeConfig{K: 4, M: 2, AllowOverwrites: true}})
	if err != nil {
		t.Fatalf("4+2 topology/pool admission failed: %v", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	size := ecFourTwoFacts(t, ctx, cluster, pool)
	initialStates, err := cluster.OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[int]*ceph.OSDContainer{}
	captured := map[int]ecAdmissionInstance{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		byID[osd.ID] = osd
		captured[osd.ID] = ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionHealthy(captured[osd.ID]); err != nil {
			t.Fatal(err)
		}
	}
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	var actor *ecFourTwoActor
	requested := map[int]bool{}
	var order []int
	restoredPolicy := false
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		if actor != nil {
			if err := actor.finish(cleanup); err != nil {
				t.Errorf("actor shutdown unconfirmed; preserve the unknown outcome and leave store removal to Terminate: %v", err)
				return
			}
		}
		for i := len(order) - 1; i >= 0; i-- {
			osd := byID[order[i]]
			state, err := osd.State(cleanup)
			if err != nil || state == nil {
				t.Errorf("restoration state osd.%d: %v", osd.ID, err)
				continue
			}
			if !state.Running {
				if err := osd.Start(cleanup); err != nil {
					t.Errorf("restore osd.%d: %v", osd.ID, err)
				}
			}
		}
		if !restoredPolicy && noout != nil {
			if err := noout.Restore(cleanup); err != nil {
				t.Error("restore noout:", err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	path := "/tmp/tc-ec-four-two-actor"
	actor = startECFourTwoActor(ctx, cluster, client, pool.Name, path, size)
	seed := ecFourTwoReadStep(t, ctx, cluster, client, path, 0)
	if seed.Phase != "seed" || seed.PID <= 0 || seed.Writer == 0 || seed.Reader == 0 || seed.Writer == seed.Reader || seed.Size != size || seed.Snap == 0 || seed.HeadVersion == 0 || seed.HeadSHA256 != ecFourTwoPayloadHash(size, 0) || seed.SnapSHA256 != seed.HeadSHA256 || seed.SnapVersion != seed.HeadVersion {
		t.Fatalf("invalid seed/client/snapshot: %+v", seed)
	}
	baseline, err := pool.PGs(ctx)
	if err != nil || len(baseline.PGs) != 1 {
		t.Fatalf("initial 4+2 placement: %+v %v", baseline, err)
	}
	p, err := planECFourTwo(baseline.PGs[0].Acting, baseline.PGs[0].ActingPrimary, pool.MinSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range p.Original {
		if byID[id] == nil {
			t.Fatal("acting holder not owned")
		}
	}
	stopHolder := func(index int) {
		t.Helper()
		osd := byID[p.Original[index]]
		now := ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionStop(captured[osd.ID], now); err != nil {
			t.Fatal(err)
		}
		t.Logf("EC_FOUR_TWO_STOP_REQUEST %s", mustAdmissionJSON(t, map[string]any{"fragment": index, "captured": captured[osd.ID], "admitted": now}))
		requested[osd.ID] = true
		order = append(order, osd.ID)
		timeout := 20 * time.Second
		if err := osd.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		after := ecAdmissionInstanceOf(t, ctx, osd)
		t.Logf("EC_FOUR_TWO_STOP_RESULT %s", mustAdmissionJSON(t, after))
		if err := checkECAdmissionStopped(captured[osd.ID], after); err != nil {
			t.Fatal(err)
		}
	}
	instances := func() map[int]ecAdmissionInstance {
		t.Helper()
		out := map[int]ecAdmissionInstance{}
		for id, osd := range byID {
			out[id] = ecAdmissionInstanceOf(t, ctx, osd)
			var err error
			if requested[id] {
				err = checkECAdmissionStopped(captured[id], out[id])
			} else {
				err = checkECAdmissionStop(captured[id], out[id])
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	firstIndex := slices.Index(p.Original, p.Primary)
	stopHolder(firstIndex)
	first := ecFourTwoWait(t, ctx, cluster, pool.Name, p, baseline, []int{firstIndex}, baseline.PGs[0].ReportedEpoch, -1)
	t.Logf("EC_FOUR_TWO_PHASE %s", mustAdmissionJSON(t, map[string]any{"phase": "five-holders", "placement": first, "instances": instances()}))
	if err := actor.send(ctx, 1, 1, "write"); err != nil {
		t.Fatal("first retained write delivery unknown:", err)
	}
	one := ecFourTwoReadStep(t, ctx, cluster, client, path, 1)
	if err := checkECFourTwoAck(seed, seed, one, size, 1, 1); err != nil {
		t.Fatal(err)
	}
	firstAfter, err := pool.PGs(ctx)
	if err != nil || checkECFourTwoPlacement(p, baseline, firstAfter, []int{firstIndex}, baseline.PGs[0].ReportedEpoch, -1) != nil {
		t.Fatalf("first I/O placement interval changed: %+v %v", firstAfter, err)
	}
	lost, err := p.losses(firstAfter.PGs[0].ActingPrimary)
	if err != nil {
		t.Fatal(err)
	}
	stopHolder(lost[1])
	second := ecFourTwoWait(t, ctx, cluster, pool.Name, p, baseline, lost, firstAfter.PGs[0].ReportedEpoch, firstAfter.PGs[0].ActingPrimary)
	t.Logf("EC_FOUR_TWO_PHASE %s", mustAdmissionJSON(t, map[string]any{"phase": "four-holders", "placement": second, "instances": instances()}))
	phase := "write"
	if pool.MinSize == 5 {
		phase = "blocked"
	}
	if err := actor.send(ctx, 2, 2, phase); err != nil {
		t.Fatal("second retained write delivery unknown:", err)
	}
	two := ecFourTwoReadStep(t, ctx, cluster, client, path, 2)
	last := one
	if pool.MinSize == 4 {
		if err := checkECFourTwoAck(seed, one, two, size, 2, 2); err != nil {
			t.Fatal(err)
		}
		last = two
	} else if two.Phase != "blocked" || two.PID != seed.PID || two.Writer != seed.Writer || two.Reader != seed.Reader || two.Read == nil || two.Write == nil || two.Read.Outcome != "unknown" || two.Write.Outcome != "unknown" || two.Read.Errno != 110 || two.Write.Errno != 110 {
		t.Fatalf("default threshold did not bound the original operations: %+v", two)
	}
	secondAfter, err := pool.PGs(ctx)
	if err != nil || checkECFourTwoPlacement(p, baseline, secondAfter, lost, firstAfter.PGs[0].ReportedEpoch, firstAfter.PGs[0].ActingPrimary) != nil {
		t.Fatalf("second I/O placement interval changed: %+v %v", secondAfter, err)
	}
	if err := actor.finish(ctx); err != nil {
		t.Fatal("retained clients not confirmed shut down before store restoration:", err)
	}
	var shutdown ecFourTwoShutdown
	if json.Unmarshal(actor.finished.result.Stdout, &shutdown) != nil || checkECFourTwoShutdown(seed, shutdown) != nil {
		t.Fatalf("actor did not attest its original clients' shutdown: %.2048s", actor.finished.result.Stdout)
	}
	t.Logf("EC_FOUR_TWO_ACTOR_SHUTDOWN %s", actor.finished.result.Stdout)
	for i := len(order) - 1; i >= 0; i-- {
		if err := byID[order[i]].Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := pool.PGs(ctx)
	if err != nil || len(recovered.PGs) != 1 || recovered.FSID != baseline.FSID || recovered.PoolAfter.MinSize != p.MinSize || !slices.Equal(recovered.PGs[0].Acting, p.Original) || !strings.Contains(recovered.PGs[0].State, "clean") {
		t.Fatalf("original 4+2 placement not restored: %+v %v", recovered, err)
	}
	for id, osd := range byID {
		now := ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionHealthy(now); err != nil {
			t.Fatal(err)
		}
		if now.ContainerID != captured[id].ContainerID || requested[id] && now.StartedAt == captured[id].StartedAt {
			t.Fatalf("restored generation does not retain the owned container: %+v", now)
		}
		if !requested[id] {
			if err := checkECAdmissionStop(captured[id], now); err != nil {
				t.Fatal(err)
			}
		}
	}
	finalStates, err := cluster.OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalStates) != len(initialStates) {
		t.Fatal("owned OSD registrations changed")
	}
	for i, s := range finalStates {
		if s.ID != initialStates[i].ID || s.UUID != initialStates[i].UUID || !s.Up || !s.In {
			t.Fatalf("owned registration was replaced or not restored: %+v", s)
		}
	}
	r, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", ecFourTwoVerifyScript, pool.Name, strconv.Itoa(size), strconv.FormatUint(seed.Snap, 10), strconv.Itoa(last.Value)}, Timeout: time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "fresh 4+2 readback"})
	var readback ecFourTwoReadback
	if err != nil || !r.Complete() || r.ExitCode != 0 || json.Unmarshal(r.Stdout, &readback) != nil || checkECFourTwoReadback(seed, last, readback, size, pool.MinSize == 5) != nil {
		t.Fatalf("fresh readback does not match acknowledged bytes/versions: %s %v %.2048s", r.Stdout, err, r.Stderr)
	}
	t.Logf("EC_FOUR_TWO_RECEIPT %s", mustAdmissionJSON(t, map[string]any{"case": t.Name(), "plan": p, "lost_indices": lost, "seed": seed, "five_holders": one, "four_holders": two, "client_shutdown": shutdown, "recovered": recovered, "readback": readback, "initial_osds": initialStates, "final_osds": finalStates, "scope": "4+2 admission, retained stopped stores, not interrupted reconstruction or permanent loss"}))
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restoredPolicy = true
	if err := pool.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}
