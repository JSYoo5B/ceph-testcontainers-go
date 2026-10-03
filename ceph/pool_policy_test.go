package ceph

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func policyFixture(size, minSize, kind string) *poolFixtureContainer {
	return &poolFixtureContainer{output: map[string]string{
		"osd pool ls detail --format json":  `[{"pool":7,"pool_name":"fixture","type":` + kind + `,"size":` + size + `,"min_size":` + minSize + `,"pg_num":8,"crush_rule":3,"quota_max_bytes":100,"quota_max_objects":4}]`,
		"osd crush rule dump --format json": `[{"rule_id":3,"steps":[{"op":"take","item_name":"default"},{"op":"chooseleaf_firstn","type":"osd","num":0},{"op":"emit"}]}]`,
		"osd crush dump --format json":      `{"devices":[{"id":0,"class":"ssd"},{"id":1,"class":"ssd"},{"id":2,"class":"ssd"}],"buckets":[{"id":-1,"name":"default","type_name":"root","items":[{"id":0,"weight":1},{"id":1,"weight":1},{"id":2,"weight":1}]}]}`,
	}}
}

func policyMutations(ctr *poolFixtureContainer) []string {
	var result []string
	for _, args := range ctr.calls {
		if slices.Contains(args, "set") || slices.Contains(args, "set-quota") {
			result = append(result, strings.Join(args, " "))
		}
	}
	return result
}

func TestPoolPolicyValidatesBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		size, min int
	}{{"--option", 2, 1}, {"fixture", 0, 1}, {"fixture", 2, 3}} {
		ctr := policyFixture("2", "1", "1")
		if err := poolFixtureCluster(ctr, 3).SetPoolReplication(t.Context(), tc.name, tc.size, tc.min); err == nil || len(ctr.calls) != 0 {
			t.Fatal("invalid policy reached native CLI")
		}
	}
	for _, kind := range []string{"1", "3"} {
		ctr := policyFixture("2", "1", kind)
		if err := poolFixtureCluster(ctr, 1).SetPoolReplication(t.Context(), "fixture", 3, 2); err == nil || len(policyMutations(ctr)) != 0 {
			t.Fatal("EC/insufficient domains accepted")
		}
	}
}

func TestPoolReplicationPreservesValidIntermediateSizes(t *testing.T) {
	for _, tc := range []struct {
		oldSize, oldMin string
		size, min       int
		want            []string
	}{
		{"2", "1", 3, 2, []string{"osd pool set fixture size 3", "osd pool set fixture min_size 2"}},
		{"3", "3", 2, 2, []string{"osd pool set fixture min_size 2", "osd pool set fixture size 2"}},
		{"2", "2", 1, 1, []string{"osd pool set fixture min_size 1", "osd pool set fixture size 1 --yes-i-really-mean-it"}},
	} {
		ctr := policyFixture(tc.oldSize, tc.oldMin, "1")
		if err := poolFixtureCluster(ctr, 3).SetPoolReplication(t.Context(), "fixture", tc.size, tc.min); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(policyMutations(ctr), tc.want) {
			t.Fatalf("unsafe policy order: %v", policyMutations(ctr))
		}
	}
}

func TestPoolPolicyPartialFailureDoesNotRollback(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	ctr.fail = "osd pool set fixture min_size 2"
	if err := poolFixtureCluster(ctr, 3).SetPoolReplication(t.Context(), "fixture", 3, 2); err == nil {
		t.Fatal("failure ignored")
	}
	if !slices.Equal(policyMutations(ctr), []string{"osd pool set fixture size 3", ctr.fail}) {
		t.Fatal("partial change rolled back/continued")
	}
	ctr = policyFixture("2", "1", "1")
	ctr.fail = "osd pool set-quota fixture max_objects 0"
	if err := poolFixtureCluster(ctr, 3).SetPoolQuota(t.Context(), "fixture", PoolQuota{}); err == nil || len(policyMutations(ctr)) != 1 {
		t.Fatal("quota failure continued")
	}
}

func TestPoolQuotaZeroClearsBothLimitsAndStatusIncludesNativeIDs(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	cluster := poolFixtureCluster(ctr, 3)
	state, err := cluster.PoolStatus(t.Context(), "fixture")
	if err != nil || state.ID != 7 || state.Quota.MaxBytes != 100 || state.Quota.MaxObjects != 4 {
		t.Fatalf("invalid native snapshot: %+v %v", state, err)
	}
	if err := cluster.SetPoolQuota(t.Context(), "fixture", PoolQuota{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(policyMutations(ctr), []string{"osd pool set-quota fixture max_objects 0", "osd pool set-quota fixture max_bytes 0"}) {
		t.Fatal("zero quota did not clear both settings")
	}
	ctr.output["osd pool ls detail --format json"] = "null"
	if _, err := cluster.Pools(t.Context()); err == nil {
		t.Fatal("malformed listing accepted")
	}
}

type replacedPoolFixture struct {
	*poolFixtureContainer
	reads int
}

func (ctr *replacedPoolFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Contains(args, "detail") {
		ctr.reads++
		if ctr.reads > 1 {
			ctr.output["osd pool ls detail --format json"] = strings.ReplaceAll(ctr.output["osd pool ls detail --format json"], `"pool":7`, `"pool":9`)
		}
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}
func TestPoolPolicyRefusesRecreatedPoolBetweenCommands(t *testing.T) {
	ctr := &replacedPoolFixture{poolFixtureContainer: policyFixture("2", "1", "1")}
	if err := poolFixtureCluster(ctr, 3).SetPoolQuota(t.Context(), "fixture", PoolQuota{}); err == nil || len(policyMutations(ctr.poolFixtureContainer)) != 0 {
		t.Fatal("replacement pool policy changed")
	}
}

func TestPoolPolicyRefusesFixedChoiceAndChangedNativePlacement(t *testing.T) {
	for _, num := range []string{`,"num":2`, `,"num":-1`, ``} {
		ctr := policyFixture("2", "1", "1")
		ctr.output["osd crush rule dump --format json"] = strings.ReplaceAll(ctr.output["osd crush rule dump --format json"], `,"num":0`, num)
		if err := poolFixtureCluster(ctr, 3).SetPoolReplication(t.Context(), "fixture", 3, 2); err == nil || len(policyMutations(ctr)) != 0 {
			t.Fatal("unsupported fixed/missing CRUSH choice mutated policy")
		}
	}
	for _, nativeEdit := range []string{`{"id":2,"weight":0}`, `{"id":8,"weight":1}`} {
		ctr := policyFixture("2", "1", "1")
		ctr.output["osd crush dump --format json"] = strings.ReplaceAll(ctr.output["osd crush dump --format json"], `{"id":2,"weight":1}`, nativeEdit)
		if err := poolFixtureCluster(ctr, 3).SetPoolReplication(t.Context(), "fixture", 3, 2); err == nil || len(policyMutations(ctr)) != 0 {
			t.Fatal("cached placement hid native weight/move change")
		}
	}
}
