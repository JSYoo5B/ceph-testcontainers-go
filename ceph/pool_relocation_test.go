package ceph

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const relocationRules = `[{"rule_id":3,"rule_name":"tc-fixture-replicated","steps":[{"op":"take","item_name":"default"},{"op":"chooseleaf_firstn","type":"osd","num":0},{"op":"emit"}]}]`

func relocationFixture() *poolFixtureContainer {
	ctr := policyFixture("2", "1", "1")
	ctr.output["osd crush rule dump --format json"] = relocationRules
	return ctr
}

func relocationMutations(ctr *poolFixtureContainer) []string {
	var result []string
	for _, args := range ctr.calls {
		if slices.Contains(args, "set") || slices.Contains(args, "create-replicated") {
			result = append(result, strings.Join(args, " "))
		}
	}
	return result
}

func TestPoolPGCountValidatesBeforeNativeCommands(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
	}{{"--option", 16}, {"fixture", 0}, {"fixture", -8}, {"fixture", 65537}} {
		ctr := relocationFixture()
		cluster := poolFixtureCluster(ctr, 3)
		if err := cluster.SetPoolPGCount(t.Context(), tc.name, tc.count); err == nil || len(ctr.calls) != 0 {
			t.Fatalf("invalid PG count request reached native CLI: %+v", tc)
		}
		if err := cluster.WaitForPoolPGCount(t.Context(), tc.name, tc.count); err == nil || len(ctr.calls) != 0 {
			t.Fatalf("invalid PG count wait reached native CLI: %+v", tc)
		}
	}
	ctr := relocationFixture()
	ctr.output["osd pool ls detail --format json"] = strings.Replace(ctr.output["osd pool ls detail --format json"], `"pg_num":8`, `"pg_num":8,"pg_autoscale_mode":"on"`, 1)
	if err := poolFixtureCluster(ctr, 3).SetPoolPGCount(t.Context(), "fixture", 16); err == nil || len(relocationMutations(ctr)) != 0 {
		t.Fatal("autoscaled pool accepted a fixed PG target")
	}
}

func TestPoolPGCountSetsBothTargetsInOrderWithoutRollback(t *testing.T) {
	ctr := relocationFixture()
	if err := poolFixtureCluster(ctr, 3).SetPoolPGCount(t.Context(), "fixture", 16); err != nil {
		t.Fatal(err)
	}
	if want := []string{"osd pool set fixture pg_num 16", "osd pool set fixture pgp_num 16"}; !slices.Equal(relocationMutations(ctr), want) {
		t.Fatalf("unexpected PG target commands: %v", relocationMutations(ctr))
	}
	ctr = relocationFixture()
	ctr.fail = "osd pool set fixture pg_num 4"
	if err := poolFixtureCluster(ctr, 3).SetPoolPGCount(t.Context(), "fixture", 4); err == nil || len(relocationMutations(ctr)) != 1 {
		t.Fatalf("failed pg_num target continued: %v", relocationMutations(ctr))
	}
	replaced := &replacedPoolFixture{poolFixtureContainer: relocationFixture()}
	if err := poolFixtureCluster(replaced, 3).SetPoolPGCount(t.Context(), "fixture", 16); err == nil || len(relocationMutations(replaced.poolFixtureContainer)) != 0 {
		t.Fatal("replacement pool PG count changed")
	}
}

// progressingPoolFixture returns one native listing and PG report set per
// read and repeats the last one.
type progressingPoolFixture struct {
	*poolFixtureContainer
	listings []string
	reported []int
	reads    int
}

func (ctr *progressingPoolFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Contains(args, "detail") {
		index := min(ctr.reads, len(ctr.listings)-1)
		ctr.output["osd pool ls detail --format json"] = ctr.listings[index]
		reports := make([]string, 0, 16)
		if index < len(ctr.reported) {
			for pg := range ctr.reported[index] {
				reports = append(reports, strings.Replace(poolPGFixtureReport, `"7.a"`, fmt.Sprintf(`"7.%x"`, pg), 1))
			}
		}
		ctr.output["pg ls-by-pool fixture --format json"] = poolPGFixtureReports(strings.Join(reports, ","), true)
		ctr.reads++
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func pgProgressListing(id, pgNum, target, pending, pgp, pgpTarget string) string {
	return `[{"pool_id":` + id + `,"pool_name":"fixture","type":1,"size":2,"min_size":1,"pg_num":` + pgNum +
		`,"pg_num_target":` + target + `,"pg_num_pending":` + pending + `,"pg_placement_num":` + pgp + `,"pg_placement_num_target":` + pgpTarget + `,"crush_rule":3}]`
}

func TestWaitForPoolPGCountRequiresEveryNativeCounter(t *testing.T) {
	ctr := &progressingPoolFixture{poolFixtureContainer: relocationFixture(), listings: []string{
		pgProgressListing("7", "8", "16", "8", "8", "16"),
		pgProgressListing("7", "16", "16", "16", "12", "16"),
		pgProgressListing("7", "16", "16", "16", "16", "16"),
		pgProgressListing("7", "16", "16", "16", "16", "16"),
	}, reported: []int{8, 8, 8, 16}}
	cluster := poolFixtureCluster(ctr, 3)
	cluster.settings.startupTimeout = 10 * time.Second
	if err := cluster.WaitForPoolPGCount(t.Context(), "fixture", 16); err != nil || ctr.reads != 4 {
		t.Fatalf("PG count wait returned before the map and reports agreed: reads=%d err=%v", ctr.reads, err)
	}
	// A merge is incomplete while pg_num_pending is still above the target.
	ctr = &progressingPoolFixture{poolFixtureContainer: relocationFixture(), listings: []string{
		pgProgressListing("7", "8", "8", "16", "8", "8"),
	}, reported: []int{8}}
	if err := poolFixtureCluster(ctr, 3).WaitForPoolPGCount(t.Context(), "fixture", 8); err == nil || !strings.Contains(err.Error(), "pending 16") {
		t.Fatalf("pending merge satisfied the wait: %v", err)
	}
	ctr = &progressingPoolFixture{poolFixtureContainer: relocationFixture(), listings: []string{
		pgProgressListing("7", "8", "8", "8", "8", "8"),
	}, reported: []int{16}}
	if err := poolFixtureCluster(ctr, 3).WaitForPoolPGCount(t.Context(), "fixture", 8); err == nil || !strings.Contains(err.Error(), "reports 16 PGs") {
		t.Fatalf("stale PG reports satisfied the wait: %v", err)
	}
	ctr = &progressingPoolFixture{poolFixtureContainer: relocationFixture(), listings: []string{
		`[{"pool_id":7,"pool_name":"fixture","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":3}]`,
	}}
	if err := poolFixtureCluster(ctr, 3).WaitForPoolPGCount(t.Context(), "fixture", 8); err == nil || !strings.Contains(err.Error(), "native PG targets") {
		t.Fatalf("missing native targets satisfied the wait: %v", err)
	}
	ctr = &progressingPoolFixture{poolFixtureContainer: relocationFixture(), listings: []string{
		pgProgressListing("7", "8", "16", "8", "8", "16"),
		pgProgressListing("9", "16", "16", "16", "16", "16"),
	}, reported: []int{8, 16}}
	if err := poolFixtureCluster(ctr, 3).WaitForPoolPGCount(t.Context(), "fixture", 16); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("replacement pool satisfied the wait: %v", err)
	}
}

func TestPoolStatusReportsPGProgress(t *testing.T) {
	ctr := relocationFixture()
	ctr.output["osd pool ls detail --format json"] = pgProgressListing("7", "12", "16", "12", "10", "16")
	state, err := poolFixtureCluster(ctr, 3).PoolStatus(t.Context(), "fixture")
	if err != nil || state.PGNum != 12 || state.PGNumTarget != 16 || state.PGNumPending != 12 || state.PGPlacementNum != 10 || state.PGPlacementNumTarget != 16 {
		t.Fatalf("native PG progress was not retained: %+v %v", state, err)
	}
}

func TestPoolPlacementCreatesHashedRuleAndSelectsIt(t *testing.T) {
	ctr := relocationFixture()
	rule := poolPlacementRuleName("fixture", PoolConfig{CRUSHRoot: "default", FailureDomain: "osd", DeviceClass: "ssd"})
	if !strings.HasPrefix(rule, "tc-fixture-placement-") || len(rule) != len("tc-fixture-placement-")+8 {
		t.Fatalf("unexpected placement rule name %q", rule)
	}
	if err := poolFixtureCluster(ctr, 3).SetPoolPlacement(t.Context(), "fixture", PoolPlacement{DeviceClass: "ssd"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"osd crush rule create-replicated " + rule + " default osd ssd", "osd pool set fixture crush_rule " + rule}
	if !slices.Equal(relocationMutations(ctr), want) {
		t.Fatalf("unexpected placement commands: %v", relocationMutations(ctr))
	}
}

func TestPoolPlacementReusesMatchingFixtureRules(t *testing.T) {
	ctr := relocationFixture()
	rule := poolPlacementRuleName("fixture", PoolConfig{CRUSHRoot: "default", FailureDomain: "osd", DeviceClass: "ssd"})
	ctr.output["osd crush rule dump --format json"] = strings.TrimSuffix(relocationRules, "]") +
		`,{"rule_id":4,"rule_name":"` + rule + `","steps":[{"op":"take","item_name":"default~ssd"},{"op":"choose_firstn","type":"osd","num":0},{"op":"emit"}]}]`
	ctr.output["osd pool ls detail --format json"] = strings.Replace(ctr.output["osd pool ls detail --format json"], `"crush_rule":3`, `"crush_rule":4`, 1)
	if err := poolFixtureCluster(ctr, 3).SetPoolPlacement(t.Context(), "fixture", PoolPlacement{}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"osd pool set fixture crush_rule tc-fixture-replicated"}; !slices.Equal(relocationMutations(ctr), want) {
		t.Fatalf("original rule was not reused: %v", relocationMutations(ctr))
	}
	// The current rule already matches, so moving to it again is a no-op.
	if err := poolFixtureCluster(ctr, 3).SetPoolPlacement(t.Context(), "fixture", PoolPlacement{DeviceClass: "ssd"}); err != nil || len(relocationMutations(ctr)) != 1 {
		t.Fatalf("matching placement mutated the pool: %v %v", relocationMutations(ctr), err)
	}
}

func TestPoolPlacementRefusesUnsafeRequestsBeforeMutation(t *testing.T) {
	for name, prepare := range map[string]func(*poolFixtureContainer) PoolPlacement{
		"invalid domain": func(*poolFixtureContainer) PoolPlacement { return PoolPlacement{FailureDomain: "datacenter"} },
		"erasure pool": func(ctr *poolFixtureContainer) PoolPlacement {
			ctr.output["osd pool ls detail --format json"] = strings.Replace(ctr.output["osd pool ls detail --format json"], `"type":1`, `"type":3`, 1)
			return PoolPlacement{DeviceClass: "ssd"}
		},
		"insufficient class domains": func(*poolFixtureContainer) PoolPlacement { return PoolPlacement{DeviceClass: "hdd"} },
		"conflicting hashed rule": func(ctr *poolFixtureContainer) PoolPlacement {
			rule := poolPlacementRuleName("fixture", PoolConfig{CRUSHRoot: "default", FailureDomain: "osd", DeviceClass: "ssd"})
			ctr.output["osd crush rule dump --format json"] = strings.TrimSuffix(relocationRules, "]") +
				`,{"rule_id":4,"rule_name":"` + rule + `","steps":[{"op":"take","item_name":"default"},{"op":"chooseleaf_firstn","type":"host","num":0},{"op":"emit"}]}]`
			return PoolPlacement{DeviceClass: "ssd"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctr := relocationFixture()
			placement := prepare(ctr)
			if err := poolFixtureCluster(ctr, 3).SetPoolPlacement(t.Context(), "fixture", placement); err == nil || len(relocationMutations(ctr)) != 0 {
				t.Fatalf("unsafe placement mutated Ceph: %v %v", relocationMutations(ctr), err)
			}
		})
	}
	replaced := &replacedPoolFixture{poolFixtureContainer: relocationFixture()}
	replaced.output["osd crush rule dump --format json"] = relocationRules
	if err := poolFixtureCluster(replaced, 3).SetPoolPlacement(t.Context(), "fixture", PoolPlacement{DeviceClass: "ssd"}); err == nil || slices.ContainsFunc(relocationMutations(replaced.poolFixtureContainer), func(c string) bool { return strings.Contains(c, "crush_rule") }) {
		t.Fatal("replacement pool received the placement rule")
	}
}
