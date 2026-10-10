package cluster

import (
	"slices"
	"strings"
	"testing"
)

const (
	removalRules = `[{"rule_id":0,"rule_name":"replicated_rule"},{"rule_id":1,"rule_name":"tc-data-replicated"},{"rule_id":2,"rule_name":"tc-wide-ec"}]`
	removalPools = `[{"pool_id":1,"pool_name":".mgr","type":1,"size":2,"min_size":1,"pg_num":1,"crush_rule":0},` +
		`{"pool_id":3,"pool_name":"data","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":1},` +
		`{"pool_id":4,"pool_name":"wide","type":3,"size":3,"min_size":3,"pg_num":8,"crush_rule":2,"erasure_code_profile":"tc-wide-ec"},` +
		`{"pool_id":5,"pool_name":"fs-meta","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":3}]`
)

func removalFixture(output map[string]string) *poolFixtureContainer {
	defaults := map[string]string{
		"osd crush rule dump --format json":         removalRules,
		"osd pool ls detail --format json":          removalPools,
		"osd erasure-code-profile ls --format json": `["default","tc-wide-ec"]`,
		"fs ls --format json":                       `[]`,
		"config dump --format json":                 `[]`,
	}
	for command, value := range output {
		defaults[command] = value
	}
	return &poolFixtureContainer{output: defaults}
}

func removalMutations(ctr *poolFixtureContainer) []string {
	var mutations []string
	for _, call := range ctr.calls {
		command := strings.Join(call, " ")
		if slices.Contains(call, "rm") || strings.HasPrefix(command, "config set") {
			mutations = append(mutations, command)
		}
	}
	return mutations
}

func TestRemovePoolDeletesFixtureResourcesAndRestoresPermission(t *testing.T) {
	for name, test := range map[string]struct {
		pool string
		want []string
	}{
		"replicated": {"data", []string{
			"config set mon mon_allow_pool_delete true",
			"osd pool rm data data --yes-i-really-really-mean-it",
			"config rm mon mon_allow_pool_delete",
			"osd crush rule rm tc-data-replicated",
		}},
		"erasure": {"wide", []string{
			"config set mon mon_allow_pool_delete true",
			"osd pool rm wide wide --yes-i-really-really-mean-it",
			"config rm mon mon_allow_pool_delete",
			"osd crush rule rm tc-wide-ec",
			"osd erasure-code-profile rm tc-wide-ec",
		}},
	} {
		t.Run(name, func(t *testing.T) {
			ctr := removalFixture(nil)
			if err := poolFixtureCluster(ctr, 3).RemovePool(t.Context(), test.pool); err != nil {
				t.Fatal(err)
			}
			if got := removalMutations(ctr); !slices.Equal(got, test.want) {
				t.Fatalf("mutations = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRemovePoolKeepsExistingPermission(t *testing.T) {
	ctr := removalFixture(map[string]string{
		"config dump --format json": `[{"section":"mon","mask":"","name":"mon_allow_pool_delete","value":"true","level":"advanced","can_update_at_runtime":true}]`,
	})
	if err := poolFixtureCluster(ctr, 3).RemovePool(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
	want := []string{"osd pool rm data data --yes-i-really-really-mean-it", "osd crush rule rm tc-data-replicated"}
	if got := removalMutations(ctr); !slices.Equal(got, want) {
		t.Fatalf("mutations = %q, want %q", got, want)
	}
}

func TestRemovePoolRestoresPreviousPermissionValue(t *testing.T) {
	ctr := removalFixture(map[string]string{
		"config dump --format json": `[{"section":"mon","mask":"","name":"mon_allow_pool_delete","value":"false","level":"advanced","can_update_at_runtime":true}]`,
	})
	ctr.fail = "osd pool rm data data --yes-i-really-really-mean-it"
	err := poolFixtureCluster(ctr, 3).RemovePool(t.Context(), "data")
	if err == nil || !strings.Contains(err.Error(), `remove pool "data"`) {
		t.Fatalf("RemovePool error = %v", err)
	}
	want := []string{
		"config set mon mon_allow_pool_delete true",
		"osd pool rm data data --yes-i-really-really-mean-it",
		"config set mon mon_allow_pool_delete false",
	}
	if got := removalMutations(ctr); !slices.Equal(got, want) {
		t.Fatalf("mutations = %q, want %q", got, want)
	}
}

func TestRemovePoolFinishesAfterPartialRemoval(t *testing.T) {
	ctr := removalFixture(map[string]string{
		"osd pool ls detail --format json": `[{"pool_id":1,"pool_name":".mgr","type":1,"size":2,"min_size":1,"pg_num":1,"crush_rule":0}]`,
	})
	if err := poolFixtureCluster(ctr, 3).RemovePool(t.Context(), "wide"); err != nil {
		t.Fatal(err)
	}
	want := []string{"osd crush rule rm tc-wide-ec", "osd erasure-code-profile rm tc-wide-ec"}
	if got := removalMutations(ctr); !slices.Equal(got, want) {
		t.Fatalf("mutations = %q, want %q", got, want)
	}
}

func TestRemovePoolRefusesUnownedOrUsedPools(t *testing.T) {
	for name, test := range map[string]struct {
		pool, fs, want string
	}{
		"ceph pool":    {pool: ".mgr", want: "not created by CreatePool"},
		"foreign rule": {pool: "fs-meta", want: "not created by CreatePool"},
		"missing":      {pool: "absent", want: `pool "absent" does not exist`},
		"invalid name": {pool: "bad name", want: "invalid native pool name"},
		"cephfs": {
			pool: "data", fs: `[{"name":"tc-cephfs","metadata_pool":"meta","data_pools":["data"]}]`,
			want: `used by CephFS filesystem "tc-cephfs"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			output := map[string]string{}
			if test.fs != "" {
				output["fs ls --format json"] = test.fs
			}
			ctr := removalFixture(output)
			err := poolFixtureCluster(ctr, 3).RemovePool(t.Context(), test.pool)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("RemovePool error = %v, want %q", err, test.want)
			}
			if got := removalMutations(ctr); len(got) != 0 {
				t.Fatalf("refused removal mutated Ceph: %q", got)
			}
		})
	}
}

func TestRemovePoolRefusesActivePermissionOverride(t *testing.T) {
	ctr := removalFixture(nil)
	cluster := poolFixtureCluster(ctr, 3)
	// The fake has no stored readback, so the handle is returned with an
	// error but stays tracked, which is the state RemovePool must respect.
	override, _ := cluster.TemporaryConfig(t.Context(), ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"})
	if override == nil {
		t.Fatal("TemporaryConfig did not return a tracked handle")
	}
	ctr.calls = nil
	if err := cluster.RemovePool(t.Context(), "data"); err == nil || !strings.Contains(err.Error(), "TemporaryConfig override") {
		t.Fatalf("RemovePool error = %v", err)
	}
	if got := removalMutations(ctr); len(got) != 0 {
		t.Fatalf("refused removal mutated Ceph: %q", got)
	}
}
