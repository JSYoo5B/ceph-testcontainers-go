package cluster

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const scrubInconsistentFixture = `{"epoch":11,"inconsistents":[{"object":{"name":"victim","nspace":"","locator":"","snap":"head","version":1},"errors":[],"union_shard_errors":["read_error"],"selected_object_info":{"version":"14'1"},"shards":[{"osd":0,"primary":false,"errors":["read_error"],"size":12},{"osd":1,"primary":true,"errors":[],"size":12,"data_digest":"0x9f649b72"}]}]}`

func scrubFixture() *poolFixtureContainer {
	ctr := policyFixture("2", "1", "1")
	ctr.output["osd map fixture victim --format json"] = `{"epoch":14,"pool":"fixture","pool_id":7,"objname":"victim","pgid":"7.0","up":[1,0],"up_primary":1,"acting":[1,0],"acting_primary":1}`
	return ctr
}

func scrubCommands(ctr *poolFixtureContainer) []string {
	var result []string
	for _, args := range ctr.calls {
		if slices.Contains(args, "tell") || slices.Contains(args, "deep-scrub") || slices.Contains(args, "repair") {
			result = append(result, strings.Join(args, " "))
		}
	}
	return result
}

func TestInjectObjectDataErrorValidatesTargetBeforeNativeCommands(t *testing.T) {
	for name, call := range map[string]func(*Container) error{
		"pool":        func(c *Container) error { return c.InjectObjectDataError(t.Context(), "--pool", "victim", 0) },
		"object dash": func(c *Container) error { return c.InjectObjectDataError(t.Context(), "fixture", "-victim", 0) },
		"object NUL":  func(c *Container) error { return c.InjectObjectDataError(t.Context(), "fixture", "vic\x00tim", 0) },
		"unowned OSD": func(c *Container) error { return c.InjectObjectDataError(t.Context(), "fixture", "victim", 9) },
	} {
		ctr := scrubFixture()
		if err := call(poolFixtureCluster(ctr, 2)); err == nil || len(ctr.calls) != 0 {
			t.Fatalf("%s: invalid target reached native CLI: %v %v", name, err, ctr.calls)
		}
	}
	ctr := scrubFixture()
	ctr.output["osd pool ls detail --format json"] = strings.Replace(ctr.output["osd pool ls detail --format json"], `"type":1`, `"type":3`, 1)
	if err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", "victim", 0); err == nil || len(scrubCommands(ctr)) != 0 {
		t.Fatal("EC pool accepted a replica data error")
	}
	ctr = scrubFixture()
	ctr.output["osd map fixture victim --format json"] = strings.Replace(ctr.output["osd map fixture victim --format json"], `"acting":[1,0]`, `"acting":[1]`, 1)
	if err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", "victim", 0); err == nil || len(scrubCommands(ctr)) != 0 {
		t.Fatal("OSD outside the acting set received an injection")
	}
	ctr = scrubFixture()
	ctr.output["osd map fixture victim --format json"] = strings.Replace(ctr.output["osd map fixture victim --format json"], `"pool_id":7`, `"pool_id":8`, 1)
	if err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", "victim", 0); err == nil || len(scrubCommands(ctr)) != 0 {
		t.Fatal("mapping of another pool was accepted")
	}
}

func TestInjectObjectDataErrorEnablesOnlyTheTargetDaemon(t *testing.T) {
	ctr := scrubFixture()
	if err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", "victim", 0); err != nil {
		t.Fatal(err)
	}
	want := []string{"tell osd.0 config set bluestore_debug_inject_read_err true", "tell osd.0 injectdataerr fixture victim"}
	if !slices.Equal(scrubCommands(ctr), want) {
		t.Fatalf("unexpected injection commands: %v", scrubCommands(ctr))
	}
}

func TestPGInconsistenciesDecodesNativeShards(t *testing.T) {
	objects, err := decodeInconsistentObjects([]byte(scrubInconsistentFixture))
	want := []InconsistentObject{{Name: "victim", Errors: []string{}, UnionShardErrors: []string{"read_error"}, Shards: []InconsistentShard{
		{OSD: 0, Shard: -1, Errors: []string{"read_error"}}, {OSD: 1, Shard: -1, Primary: true, Errors: []string{}},
	}}}
	if err != nil || !reflect.DeepEqual(objects, want) {
		t.Fatalf("unexpected decode: %+v %v", objects, err)
	}
	if objects, err := decodeInconsistentObjects([]byte(`{"epoch":3,"inconsistents":[]}`)); err != nil || objects == nil || len(objects) != 0 {
		t.Fatalf("empty native result was not retained: %v %v", objects, err)
	}
	for _, data := range []string{`{}`, `{"inconsistents":[{"object":{}}]}`, `{"inconsistents":[{"object":{"name":"x"},"shards":[{"errors":[]}]}]}`, `not json`} {
		if _, err := decodeInconsistentObjects([]byte(data)); err == nil {
			t.Fatalf("malformed native result accepted: %s", data)
		}
	}
	ctr := scrubFixture()
	if _, err := poolFixtureCluster(ctr, 2).PGInconsistencies(t.Context(), "7.0; rm"); err == nil || len(ctr.calls) != 0 {
		t.Fatal("invalid PG ID reached native CLI")
	}
}

// scrubbingFixture reports one PG state per ls-by-pool read and repeats the last.
type scrubbingFixture struct {
	*poolFixtureContainer
	reports []string
	reads   int
}

func (ctr *scrubbingFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Contains(args, "ls-by-pool") {
		report := ctr.reports[min(ctr.reads, len(ctr.reports)-1)]
		ctr.output["pg ls-by-pool fixture --format json"] = `{"pg_ready":true,"pg_stats":[` + report + `]}`
		ctr.reads++
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func scrubReport(state, stamp string) string {
	return `{"pgid":"7.0","state":"` + state + `","last_deep_scrub_stamp":"2026-10-09T10:01:` + stamp + `.255811+0000"}`
}

func TestScrubPGWaitsForANewerNativeStamp(t *testing.T) {
	ctr := &scrubbingFixture{poolFixtureContainer: scrubFixture(), reports: []string{
		scrubReport("active+clean", "03"), scrubReport("active+clean+scrubbing+deep", "03"),
		scrubReport("active+clean+inconsistent", "10"),
	}}
	cluster := poolFixtureCluster(ctr, 2)
	cluster.settings.startupTimeout = 10 * time.Second
	if err := cluster.DeepScrubPG(t.Context(), "7.0"); err != nil || ctr.reads != 3 {
		t.Fatalf("deep scrub did not wait for its report: reads=%d %v", ctr.reads, err)
	}
	if want := []string{"pg deep-scrub 7.0"}; !slices.Equal(scrubCommands(ctr.poolFixtureContainer), want) {
		t.Fatalf("unexpected scrub commands: %v", scrubCommands(ctr.poolFixtureContainer))
	}
	ctr = &scrubbingFixture{poolFixtureContainer: scrubFixture(), reports: []string{
		scrubReport("active+clean+inconsistent", "10"), scrubReport("active+clean+scrubbing+deep+inconsistent+repair", "10"),
		scrubReport("active+clean", "27"),
	}}
	cluster = poolFixtureCluster(ctr, 2)
	cluster.settings.startupTimeout = 10 * time.Second
	if err := cluster.RepairPG(t.Context(), "7.0"); err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	ctr = &scrubbingFixture{poolFixtureContainer: scrubFixture(), reports: []string{
		scrubReport("active+clean+inconsistent", "10"), scrubReport("active+clean+inconsistent", "27"),
	}}
	if err := poolFixtureCluster(ctr, 2).RepairPG(t.Context(), "7.0"); err == nil || !strings.Contains(err.Error(), "still") {
		t.Fatalf("unrepaired PG passed: %v", err)
	}
	ctr = &scrubbingFixture{poolFixtureContainer: scrubFixture(), reports: []string{scrubReport("active+clean", "03")}}
	if err := poolFixtureCluster(ctr, 2).DeepScrubPG(t.Context(), "7.0"); err == nil {
		t.Fatal("unchanged scrub stamp satisfied the wait")
	}
}
