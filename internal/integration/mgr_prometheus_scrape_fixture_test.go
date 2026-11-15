//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// prometheusPoolBody is an exposition of the prometheus module with one pool
// whose write counter is writes, trimmed to the families the check reads
// plus a label value with escapes.
func prometheusPoolBody(writes string) string {
	return `# HELP ceph_health_status Cluster health status
# TYPE ceph_health_status untyped
ceph_health_status 1.0
# HELP ceph_pool_metadata POOL Metadata
# TYPE ceph_pool_metadata untyped
ceph_pool_metadata{pool_id="1",name=".mgr",type="replicated",description="replica:1",compression_mode="none"} 1.0
ceph_pool_metadata{pool_id="2",name="tc-scrape",type="replicated",description="replica:1",compression_mode="none"} 1.0
# HELP ceph_pool_wr DF pool wr
# TYPE ceph_pool_wr counter
ceph_pool_wr{pool_id="1"} 41.0
ceph_pool_wr{pool_id="2"} ` + writes + `
# HELP ceph_mgr_metadata MGR metadata
# TYPE ceph_mgr_metadata gauge
ceph_mgr_metadata{ceph_daemon="mgr.a",hostname="h\"q\\,x",ceph_version="ceph version 20.2.4"} 1.0
`
}

// parsePrometheusBody runs prometheusScrapeScript on body as a 200 answer.
func parsePrometheusBody(t *testing.T, body string) (prometheusScrape, error) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	command := exec.Command(python, "-I", "-c", prometheusScrapeScript, "-")
	command.Stdin = strings.NewReader(body)
	out, err := command.CombinedOutput()
	if err != nil {
		return prometheusScrape{}, fmt.Errorf("%v: %s", err, out)
	}
	var scrape prometheusScrape
	if err := json.Unmarshal(out, &scrape); err != nil {
		t.Fatalf("scrape output %q: %v", out, err)
	}
	return scrape, nil
}

// The independent scraper finds the pool by its native pool_id and name and
// accepts a body only when that pool's write counter grew by the writes made.
func TestPrometheusScrapeFreshness(t *testing.T) {
	before, err := parsePrometheusBody(t, prometheusPoolBody("0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if before.Status != 200 || before.Samples != 6 || before.Families != 4 || before.Pools["2"] != "tc-scrape" || before.Writes["2"] != 0 || before.Writes["1"] != 41 {
		t.Fatalf("parsed %+v", before)
	}
	after, err := parsePrometheusBody(t, prometheusPoolBody("20.0"))
	if err != nil {
		t.Fatal(err)
	}
	if err := freshPoolWrites(before, after, "2", "tc-scrape", 20); err != nil {
		t.Fatalf("fresh scrape refused: %v", err)
	}
	standby, err := parsePrometheusBody(t, "")
	if err != nil {
		t.Fatal(err)
	}
	unavailable := prometheusScrape{Error: "ConnectionRefusedError: [Errno 111] Connection refused"}
	failed := prometheusScrape{Status: 500}
	for _, test := range []struct {
		name          string
		before, after prometheusScrape
		poolID, pool  string
		want          string
	}{
		// A replay of the reference body is a 200 that names the pool.
		{"replay of the reference body", before, before, "2", "tc-scrape", "0 -> 0, want an increase of 20"},
		{"fewer writes than made", before, mustParsePrometheus(t, prometheusPoolBody("19.0")), "2", "tc-scrape", "0 -> 19"},
		{"standby answer: 200 with an empty body", before, standby, "2", "tc-scrape", "absent: HTTP 200, 0 bytes"},
		{"another pool's ID", before, after, "3", "tc-scrape", "reference scrape: pool 3 (tc-scrape) absent"},
		{"the same ID under another name", before, after, "2", "other", "reference scrape: pool 2 (other) absent"},
		{"connection refused", before, unavailable, "2", "tc-scrape", "scrape failed: no answer: ConnectionRefusedError"},
		{"standby error status", before, failed, "2", "tc-scrape", "scrape failed: HTTP 500"},
		{"empty reference", standby, after, "2", "tc-scrape", "reference scrape: pool 2 (tc-scrape) absent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := freshPoolWrites(test.before, test.after, test.poolID, test.pool, 20)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("freshPoolWrites = %v, want an error containing %q", err, test.want)
			}
		})
	}
	if mgr := mustParsePrometheus(t, `ceph_pool_wr{pool_id="2",} 3.0 1700000000000`); mgr.Writes["2"] != 3 {
		t.Fatalf("trailing comma and timestamp: %+v", mgr)
	}
	for _, malformed := range []string{"ceph_pool_wr{pool_id=\"2\" 3.0\n", "ceph_pool_wr{pool_id=2} 3.0\n", "ceph_pool_wr{pool_id=\"2\"} three\n"} {
		if scrape, err := parsePrometheusBody(t, malformed); err == nil {
			t.Fatalf("%q parsed as %+v", malformed, scrape)
		}
	}
}

func mustParsePrometheus(t *testing.T, body string) prometheusScrape {
	t.Helper()
	scrape, err := parsePrometheusBody(t, body)
	if err != nil {
		t.Fatal(err)
	}
	return scrape
}

// TestPoolCounterTraceRecordsChanges keeps one step per change of the
// answer, so the failover log shows a promoted manager's 0 before its first
// PG report and the return to the old value, and not every repeated scrape.
func TestPoolCounterTraceRecordsChanges(t *testing.T) {
	trace := &poolCounterTrace{start: time.Now()}
	pool := func(wr float64) prometheusScrape {
		return prometheusScrape{Status: 200, Bytes: 1, Pools: map[string]string{"2": "tc-scrape"}, Writes: map[string]float64{"2": wr}}
	}
	for _, s := range []prometheusScrape{{Status: 200}, {Status: 200}, pool(0), pool(0), pool(20), pool(20), {Error: "reset"}} {
		trace.add(s, "2", "tc-scrape")
	}
	var values []string
	for _, step := range trace.steps {
		values = append(values, strings.SplitN(step, "@", 2)[0])
	}
	if got := strings.Join(values, " "); got != "absent 0 20 absent" {
		t.Fatalf("trace values %q, want %q (trace %s)", got, "absent 0 20 absent", trace)
	}
}
