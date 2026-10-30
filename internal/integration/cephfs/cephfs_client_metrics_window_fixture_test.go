//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// These controls feed synthetic counter dumps and timed samples to the
// client-metrics reader and judge. They test parsing and the window policy
// only. They do not reproduce a native MDS, and the synthetic shapes follow the
// pinned source rather than a captured run.

const (
	metricsFS     = "fs0"
	metricsWriter = 4101
	metricsIdle   = 4102
)

func metricsRow(client uint64, rank string, counters string) string {
	return fmt.Sprintf(`{"labels":{"client":"client.%d","rank":%q},"counters":{%s}}`, client, rank, counters)
}

func metricsDump(rows ...string) []byte {
	return []byte(fmt.Sprintf(`{"mds_client_metrics-%s":[%s],"other":[]}`, metricsFS, strings.Join(rows, ",")))
}

func totals(ops, size uint64) string {
	return fmt.Sprintf(`"total_write_ops":%d,"total_write_size":%d,"cap_hits":9`, ops, size)
}

func TestClientMetricsSectionReader(t *testing.T) {
	good := metricsDump(metricsRow(metricsWriter, "0", totals(1, 131072)), metricsRow(metricsIdle, "0", totals(0, 0)), metricsRow(9, "0", `"total_write_ops":null`))
	got, err := parseClientMetricsSection(good, metricsFS, 0, metricsWriter, metricsIdle)
	if err != nil || !got.Section || got.Rows[metricsWriter] != (clientWriteTotals{1, 131072}) || got.Rows[metricsIdle] != (clientWriteTotals{}) || len(got.Rows) != 2 {
		t.Fatalf("good dump: %+v %v", got, err)
	}

	t.Run("absent section is not an error and not zero", func(t *testing.T) {
		got, err := parseClientMetricsSection([]byte(`{"other":[]}`), metricsFS, 0, metricsWriter)
		if err != nil || got.Section || len(got.Rows) != 0 {
			t.Fatalf("%+v %v", got, err)
		}
	})
	t.Run("absent row stays absent", func(t *testing.T) {
		got, err := parseClientMetricsSection(metricsDump(metricsRow(metricsWriter, "0", totals(0, 0))), metricsFS, 0, metricsWriter, metricsIdle)
		if _, ok := got.Rows[metricsIdle]; err != nil || ok || !got.Section {
			t.Fatalf("%+v %v", got, err)
		}
	})
	t.Run("other rank is not the selected row", func(t *testing.T) {
		got, err := parseClientMetricsSection(metricsDump(metricsRow(metricsWriter, "1", totals(5, 5))), metricsFS, 0, metricsWriter)
		if _, ok := got.Rows[metricsWriter]; err != nil || ok {
			t.Fatalf("%+v %v", got, err)
		}
	})
	rejects := map[string][]byte{
		"not an object":         []byte(`[]`),
		"null document":         []byte(`null`),
		"section not an array":  []byte(`{"mds_client_metrics-fs0":{}}`),
		"section null":          []byte(`{"mds_client_metrics-fs0":null}`),
		"missing write ops":     metricsDump(metricsRow(metricsWriter, "0", `"total_write_size":1`)),
		"missing write size":    metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":1`)),
		"null counter":          metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":null,"total_write_size":1`)),
		"string counter":        metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":"1","total_write_size":1`)),
		"negative counter":      metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":-1,"total_write_size":1`)),
		"fractional counter":    metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":1.0,"total_write_size":1`)),
		"exponent counter":      metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":1e2,"total_write_size":1`)),
		"overflow counter":      metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":18446744073709551616,"total_write_size":1`)),
		"object counter":        metricsDump(metricsRow(metricsWriter, "0", `"total_write_ops":{"avgcount":1},"total_write_size":1`)),
		"duplicate row":         metricsDump(metricsRow(metricsWriter, "0", totals(0, 0)), metricsRow(metricsWriter, "0", totals(1, 1))),
		"row not an object":     metricsDump(`7`),
		"row without counters":  metricsDump(`{"labels":{"client":"client.9","rank":"0"}}`),
		"row without labels":    metricsDump(`{"counters":{}}`),
		"unrelated row no id":   metricsDump(`{"labels":{"rank":"0"},"counters":{}}`),
		"label not a string":    metricsDump(`{"labels":{"client":4101,"rank":"0"},"counters":{}}`),
		"rank label not string": metricsDump(`{"labels":{"client":"client.4101","rank":0},"counters":{}}`),
	}
	for name, dump := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			if got, err := parseClientMetricsSection(dump, metricsFS, 0, metricsWriter, metricsIdle); err == nil {
				t.Fatalf("accepted: %+v", got)
			}
		})
	}
}

var metricsT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(sec float64) time.Time { return metricsT0.Add(time.Duration(sec * float64(time.Second))) }

func sample(start, end float64, w, i *clientWriteTotals) clientMetricsSample {
	s := clientMetricsSample{Start: at(start), End: at(end), Section: clientMetricsSection{Section: true, Rows: map[uint64]clientWriteTotals{}}}
	if w != nil {
		s.Section.Rows[metricsWriter] = *w
	}
	if i != nil {
		s.Section.Rows[metricsIdle] = *i
	}
	return s
}

func tot(ops, size uint64) *clientWriteTotals { return &clientWriteTotals{ops, size} }

// window gates at t=10, acknowledges at t=20 and ends the phase at t=60.
func window(samples ...clientMetricsSample) clientMetricsWindow {
	return clientMetricsWindow{
		Writer: metricsWriter, Idle: metricsIdle, AckedBytes: 131072,
		GateOpened: at(10), Acked: at(20), Deadline: at(60),
		MDSGIDStart: 7, MDSGIDEnd: 7, Samples: samples,
	}
}

func TestClientMetricsWindowJudge(t *testing.T) {
	base := sample(1, 2, tot(0, 0), tot(0, 0))
	attributed := func(extra ...clientMetricsSample) clientMetricsWindow {
		return window(append([]clientMetricsSample{base}, extra...)...)
	}
	cases := []struct {
		name string
		w    clientMetricsWindow
		want clientMetricsVerdict
		part string
	}{
		{"attributed", attributed(sample(25, 26, tot(1, 131072), tot(0, 0))), clientMetricsAttributed, ""},
		{"attributed after a partial report", attributed(sample(25, 26, tot(0, 0), tot(0, 0)), sample(30, 31, tot(1, 131072), tot(0, 0))), clientMetricsAttributed, ""},
		{"cumulative bytes above the acknowledged size still attribute", attributed(sample(25, 26, tot(2, 262144), tot(0, 0))), clientMetricsAttributed, ""},
		{"idle client attributed", attributed(sample(25, 26, tot(1, 131072), tot(1, 4096))), clientMetricsContradicted, ""},
		{"idle writes win over a later covering writer", attributed(sample(25, 26, tot(0, 0), tot(1, 1)), sample(30, 31, tot(1, 131072), tot(1, 1))), clientMetricsContradicted, ""},
		{"covering writer then idle writes", attributed(sample(25, 26, tot(1, 131072), tot(0, 0)), sample(30, 31, tot(1, 131072), tot(1, 4096))), clientMetricsContradicted, ""},
		{"writer bytes short", attributed(sample(25, 26, tot(1, 4096), tot(0, 0))), clientMetricsInconclusive, "no observation sample"},
		{"writer ops zero", attributed(sample(25, 26, tot(0, 131072), tot(0, 0))), clientMetricsInconclusive, "no observation sample"},
		{"writer row missing", attributed(sample(25, 26, nil, tot(0, 0))), clientMetricsInconclusive, "row identity lost after workload"},
		{"idle row missing is not zero", attributed(sample(25, 26, tot(1, 131072), nil)), clientMetricsInconclusive, "row identity lost after workload"},
		{"section missing after the write", attributed(clientMetricsSample{Start: at(25), End: at(26)}), clientMetricsInconclusive, "row identity lost after workload"},
		{"no samples at all", window(), clientMetricsInconclusive, "no baseline"},
		{"no observation", attributed(), clientMetricsInconclusive, "no observation sample"},
		{"baseline missing a row", window(sample(1, 2, tot(0, 0), nil), sample(25, 26, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "no baseline"},
		{"baseline straddles the gate", window(sample(5, 11, tot(0, 0), tot(0, 0)), sample(25, 26, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "no baseline"},
		{"baseline writes", window(sample(1, 2, tot(1, 8), tot(0, 0)), sample(25, 26, tot(2, 131080), tot(0, 0))), clientMetricsInconclusive, "baseline sample 0 already shows"},
		{"baseline idle writes", window(sample(1, 2, tot(0, 0), tot(0, 1)), sample(25, 26, tot(1, 131072), tot(0, 1))), clientMetricsInconclusive, "baseline sample 0 already shows"},
		{"observation straddles the acknowledgement", attributed(sample(19, 26, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "no observation sample"},
		{"observation after the deadline", attributed(sample(59, 61, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "no observation sample"},
		{"covering sample only before the acknowledgement", attributed(sample(12, 13, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "no observation sample"},
		{"counter decrease", attributed(sample(25, 26, tot(1, 131072), tot(0, 0)), sample(30, 31, tot(0, 0), tot(0, 0))), clientMetricsInconclusive, "decreased"},
		{"samples out of order", attributed(sample(30, 31, tot(0, 0), tot(0, 0)), sample(25, 26, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "out-of-order"},
		{"sample ends before it starts", attributed(sample(26, 25, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive, "out-of-order"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := judgeClientMetricsWindow(c.w)
			if got.Verdict != c.want || !strings.Contains(got.Reason, c.part) {
				t.Fatalf("verdict %d reason %q, want %d containing %q", got.Verdict, got.Reason, c.want, c.part)
			}
		})
	}

	good := attributed(sample(25, 26, tot(1, 131072), tot(0, 0)))
	admission := map[string]func(*clientMetricsWindow){
		"zero writer":       func(w *clientMetricsWindow) { w.Writer = 0 },
		"zero idle":         func(w *clientMetricsWindow) { w.Idle = 0 },
		"same client":       func(w *clientMetricsWindow) { w.Idle = w.Writer },
		"no acked bytes":    func(w *clientMetricsWindow) { w.AckedBytes = 0 },
		"no gate time":      func(w *clientMetricsWindow) { w.GateOpened = time.Time{} },
		"no ack time":       func(w *clientMetricsWindow) { w.Acked = time.Time{} },
		"no deadline":       func(w *clientMetricsWindow) { w.Deadline = time.Time{} },
		"ack before gate":   func(w *clientMetricsWindow) { w.Acked = at(5) },
		"deadline before":   func(w *clientMetricsWindow) { w.Deadline = at(15) },
		"unknown MDS":       func(w *clientMetricsWindow) { w.MDSGIDStart, w.MDSGIDEnd = 0, 0 },
		"MDS generation":    func(w *clientMetricsWindow) { w.MDSGIDEnd = 8 },
		"sample time unset": func(w *clientMetricsWindow) { w.Samples[1].Start = time.Time{} },
	}
	for name, mutate := range admission {
		t.Run("admission "+name, func(t *testing.T) {
			w := good
			w.Samples = append([]clientMetricsSample(nil), good.Samples...)
			mutate(&w)
			if got := judgeClientMetricsWindow(w); got.Verdict != clientMetricsInconclusive {
				t.Fatalf("verdict %d: %s", got.Verdict, got.Reason)
			}
		})
	}
}
