//go:build all || integration

package integration_test

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// prometheusScrapeScript plays a monitoring SUT that scrapes the prometheus
// module on its own: it fetches argv[1] with urllib and parses the text
// exposition itself, without the fixture's control container or parser. It
// prints one JSON object: the HTTP status and body size, the number of
// samples and declared families, ceph_pool_metadata names and ceph_pool_wr
// values by native pool_id, or "error" when no HTTP answer arrived (a refused
// or reset connection, a timeout). With "-" as argv[1] it parses standard
// input as a 200 body instead, which the pure test uses. A malformed sample
// line fails the script.
const prometheusScrapeScript = `import json, re, sys, urllib.error, urllib.request
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?[ \t]+(\S+)(?:[ \t]+-?[0-9]+)?$')
LABEL = re.compile(r'[ \t]*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"[ \t]*(?:,|$)')
UNESCAPE = re.compile(r'\\(.)')
def parse(body):
    out = {'samples': 0, 'families': 0, 'pools': {}, 'writes': {}}
    for line in body.splitlines():
        if not line.strip():
            continue
        if line.startswith('#'):
            if line.startswith('# TYPE '):
                out['families'] += 1
            continue
        m = SAMPLE.match(line)
        if not m:
            raise SystemExit('malformed sample: ' + line[:200])
        name, text, value = m.groups()
        labels, pos = {}, 0
        while text and pos < len(text):
            l = LABEL.match(text, pos)
            if not l:
                raise SystemExit('malformed labels: ' + line[:200])
            labels[l.group(1)] = UNESCAPE.sub(lambda e: '\n' if e.group(1) == 'n' else e.group(1), l.group(2))
            pos = l.end()
        float(value)
        out['samples'] += 1
        if name == 'ceph_pool_metadata' and 'pool_id' in labels:
            out['pools'][labels['pool_id']] = labels.get('name', '')
        elif name == 'ceph_pool_wr' and 'pool_id' in labels:
            out['writes'][labels['pool_id']] = float(value)
    return out
if sys.argv[1] == '-':
    status, body = 200, sys.stdin.read()
else:
    try:
        with urllib.request.urlopen(sys.argv[1], timeout=5) as r:
            status, body = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        status, body = e.code, ''
    except OSError as e:
        print(json.dumps({'error': '%s: %s' % (type(e).__name__, e)}))
        raise SystemExit(0)
result = parse(body)
result.update(status=status, bytes=len(body))
print(json.dumps(result))
`

// prometheusScrape is what prometheusScrapeScript reports about one fetch.
type prometheusScrape struct {
	Error    string             `json:"error"`
	Status   int                `json:"status"`
	Bytes    int                `json:"bytes"`
	Samples  int                `json:"samples"`
	Families int                `json:"families"`
	Pools    map[string]string  `json:"pools"`
	Writes   map[string]float64 `json:"writes"`
}

func (s prometheusScrape) String() string {
	if s.Error != "" {
		return "no answer: " + s.Error
	}
	return fmt.Sprintf("HTTP %d, %d bytes, %d samples", s.Status, s.Bytes, s.Samples)
}

// poolSeries reports why s does not carry the pool with the native pool ID
// and name and its ceph_pool_wr series.
func (s prometheusScrape) poolSeries(poolID, name string) error {
	switch {
	case s.Error != "" || s.Status != 200:
		return fmt.Errorf("scrape failed: %s", s)
	case s.Pools[poolID] != name:
		return fmt.Errorf("pool %s (%s) absent: %s", poolID, name, s)
	}
	if _, ok := s.Writes[poolID]; !ok {
		return fmt.Errorf("pool %s (%s) has no ceph_pool_wr sample: %s", poolID, name, s)
	}
	return nil
}

// freshPoolWrites reports why after is not a fresh scrape of the pool with
// the native pool ID and name: an HTTP 200 answer that names the pool and
// whose ceph_pool_wr counter grew by at least writes over before. A standby's
// 200 with an empty body, a body without the pool and a replay of before all
// fail it.
func freshPoolWrites(before, after prometheusScrape, poolID, name string, writes float64) error {
	if err := before.poolSeries(poolID, name); err != nil {
		return fmt.Errorf("reference scrape: %w", err)
	}
	if err := after.poolSeries(poolID, name); err != nil {
		return err
	}
	if from, to := before.Writes[poolID], after.Writes[poolID]; to-from < writes {
		return fmt.Errorf("ceph_pool_wr{pool_id=%q} %v -> %v, want an increase of %v", poolID, from, to, writes)
	}
	return nil
}

// poolCounterTrace records each change of the ceph_pool_wr answer one
// endpoint gives for a pool, with its time from start, so a log shows the
// sequence a scraper saw and not only the sample it accepted.
type poolCounterTrace struct {
	start time.Time
	last  string
	steps []string
}

func (c *poolCounterTrace) add(s prometheusScrape, poolID, name string) {
	value := "absent"
	if s.poolSeries(poolID, name) == nil {
		value = strconv.FormatFloat(s.Writes[poolID], 'f', -1, 64)
	}
	if value == c.last {
		return
	}
	c.last = value
	c.steps = append(c.steps, value+"@"+time.Since(c.start).Round(time.Millisecond).String())
}

func (c *poolCounterTrace) String() string { return strings.Join(c.steps, " ") }
