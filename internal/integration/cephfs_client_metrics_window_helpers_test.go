//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// This file holds the Docker-free part of the CephFS client-metrics recipe:
// a strict reader for the active MDS's per-client counter section and a judge
// that decides, from timed samples, whether one writer's acknowledged write
// reached its own client/rank row while an idle client stayed at zero.
//
// Source basis (Tentacle 20.2.4, 7f793731): MetricAggregator registers one
// counter set per client and rank under "mds_client_metrics-<fs name>" with
// the labels "client" ("client.<id>") and "rank", and assigns the cumulative
// total_write_ops and total_write_size with set(), never by adding. A repeated
// or unchanged row is therefore not proof that a new client report arrived,
// and a missing row or field is not a zero.

// clientWriteTotals are the cumulative write counters of one client/rank row.
type clientWriteTotals struct{ Ops, Bytes uint64 }

// clientMetricsSection is one parsed counter-dump read for a chosen set of
// clients on one rank. Section reports whether the filesystem's section
// existed at all. Rows holds only the clients whose row was actually present;
// an absent client is never given an empty value.
type clientMetricsSection struct {
	Section bool
	Rows    map[uint64]clientWriteTotals
}

// parseClientMetricsSection reads the output of "tell mds.<name> counter dump".
// A missing section is reported as Section=false without an error, because the
// section appears only once the first client report arrives. Everything that
// is present must be well formed: a section that is not an array, a row
// without object "labels" and "counters", labels that are not strings, a
// selected row without both write counters, a counter that is not an unsigned
// decimal integer, or two rows for one selected client/rank is an error. Rows
// of other clients are checked for their labels only, so a row that cannot be
// attributed never passes as unrelated.
func parseClientMetricsSection(dump []byte, fsName string, rank uint64, clients ...uint64) (clientMetricsSection, error) {
	var out clientMetricsSection
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(dump, &sections); err != nil || sections == nil {
		return out, fmt.Errorf("counter dump is not a JSON object (%v)", err)
	}
	raw, ok := sections["mds_client_metrics-"+fsName]
	if !ok {
		return out, nil
	}
	out.Section = true
	var rows []json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil || rows == nil {
		return out, fmt.Errorf("client metrics section is not an array (%v)", err)
	}
	selected := map[string]uint64{}
	for _, id := range clients {
		selected[fmt.Sprintf("client.%d", id)] = id
	}
	out.Rows = map[uint64]clientWriteTotals{}
	for i, rowRaw := range rows {
		var row struct {
			Labels   map[string]json.RawMessage `json:"labels"`
			Counters map[string]json.RawMessage `json:"counters"`
		}
		if err := json.Unmarshal(rowRaw, &row); err != nil || row.Labels == nil || row.Counters == nil {
			return out, fmt.Errorf("row %d lacks object labels and counters (%v)", i, err)
		}
		client, err := labelString(row.Labels, "client")
		if err != nil {
			return out, fmt.Errorf("row %d: %w", i, err)
		}
		rowRank, err := labelString(row.Labels, "rank")
		if err != nil {
			return out, fmt.Errorf("row %d: %w", i, err)
		}
		id, want := selected[client]
		if !want || rowRank != strconv.FormatUint(rank, 10) {
			continue
		}
		if _, dup := out.Rows[id]; dup {
			return out, fmt.Errorf("duplicate row for %s rank %d", client, rank)
		}
		var t clientWriteTotals
		if t.Ops, err = counterUint(row.Counters, "total_write_ops"); err != nil {
			return out, fmt.Errorf("%s: %w", client, err)
		}
		if t.Bytes, err = counterUint(row.Counters, "total_write_size"); err != nil {
			return out, fmt.Errorf("%s: %w", client, err)
		}
		out.Rows[id] = t
	}
	return out, nil
}

func labelString(labels map[string]json.RawMessage, name string) (string, error) {
	raw, ok := labels[name]
	if !ok {
		return "", fmt.Errorf("label %q is missing", name)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("label %q is not a string: %.64s", name, raw)
	}
	return s, nil
}

// counterUint accepts only an unsigned decimal integer literal. A missing
// field, null, a string, a fraction, an exponent, a sign or an overflow is an
// error and never decodes to zero.
func counterUint(counters map[string]json.RawMessage, name string) (uint64, error) {
	raw, ok := counters[name]
	if !ok {
		return 0, fmt.Errorf("counter %q is missing", name)
	}
	text := string(bytes.TrimSpace(raw))
	v, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("counter %q is not an unsigned integer: %.64s", name, raw)
	}
	return v, nil
}

// clientMetricsSample is one counter-dump query. Start and End bracket the
// query, because the daemon samples its value somewhere inside that interval.
type clientMetricsSample struct {
	Start, End time.Time
	Section    clientMetricsSection
}

// clientMetricsWindow is everything the judge needs. GateOpened is when the
// writer was released to write, Acked when its write and fsync receipt
// arrived, and Deadline ends the phase. MDSGIDStart and MDSGIDEnd are the
// active MDS generation checked before and after the observation.
type clientMetricsWindow struct {
	Writer, Idle           uint64
	AckedBytes             uint64
	GateOpened, Acked      time.Time
	Deadline               time.Time
	MDSGIDStart, MDSGIDEnd uint64
	Samples                []clientMetricsSample
}

type clientMetricsVerdict int

const (
	// clientMetricsInconclusive means the evidence cannot support either
	// answer. It is the default, so a forgotten branch can never pass.
	clientMetricsInconclusive clientMetricsVerdict = iota
	// clientMetricsAttributed means a post-acknowledgement sample showed the
	// writer's own row covering the acknowledged bytes with the idle row at zero.
	clientMetricsAttributed
	// clientMetricsContradicted means the idle client's row carried writes.
	clientMetricsContradicted
)

type clientMetricsOutcome struct {
	Verdict clientMetricsVerdict
	Reason  string
	Writer  clientWriteTotals
	Idle    clientWriteTotals
}

func metricsInconclusive(format string, args ...any) clientMetricsOutcome {
	return clientMetricsOutcome{Reason: fmt.Sprintf(format, args...)}
}

// judgeClientMetricsWindow applies the counter-window policy:
//
//   - Admission: distinct nonzero client IDs, a positive acknowledged size,
//     ordered gate, acknowledgement and deadline times, and one unchanged
//     nonzero MDS generation.
//   - Baseline: only samples that ended before the gate opened count, so a
//     query that straddled the gate cannot vouch for a zero. At least one must
//     show both rows with zero totals, and none may show anything else.
//   - Observation: only samples that started at or after the acknowledgement
//     and ended by the deadline count. A straddling sample or a late one is not
//     evidence. Cumulative totals must never decrease for a client across
//     the series.
//   - Continuity: both rows were admitted at zero in the baseline, so an
//     eligible observation sample that lacks the metrics section, the writer
//     row or the idle row has lost the row identity. That sample is not a
//     zero and not an observation, and no later or earlier sample restores the
//     window: the result is inconclusive with a "row identity lost after
//     workload" reason, even when another sample covered the writer. Samples
//     outside the acknowledgement-to-deadline window are not part of this
//     check. This matches the experimental handoff collector, which aborts
//     when a selected row is not ready after the workload.
//   - Result: every eligible observation sample is scanned. An idle
//     row with writes in any of them is a contradiction, even after a sample
//     that already covered the writer or after a lost row, because that row
//     is positive evidence. The writer needs at least one write
//     op and at least the acknowledged bytes. Exact equality is not required,
//     because the totals are cumulative counters, not message counts.
func judgeClientMetricsWindow(w clientMetricsWindow) clientMetricsOutcome {
	switch {
	case w.Writer == 0 || w.Idle == 0 || w.Writer == w.Idle:
		return metricsInconclusive("client identities are not two distinct nonzero IDs (%d, %d)", w.Writer, w.Idle)
	case w.AckedBytes == 0:
		return metricsInconclusive("no acknowledged write size")
	case w.GateOpened.IsZero() || w.Acked.IsZero() || w.Deadline.IsZero() || w.Acked.Before(w.GateOpened) || w.Deadline.Before(w.Acked):
		return metricsInconclusive("gate, acknowledgement and deadline times are missing or out of order")
	case w.MDSGIDStart == 0 || w.MDSGIDStart != w.MDSGIDEnd:
		return metricsInconclusive("MDS generation is unknown or changed (%d to %d)", w.MDSGIDStart, w.MDSGIDEnd)
	}
	last := map[uint64]clientWriteTotals{}
	var prevStart time.Time
	for i, s := range w.Samples {
		if s.Start.IsZero() || s.End.Before(s.Start) || s.Start.Before(prevStart) {
			return metricsInconclusive("sample %d has missing or out-of-order times", i)
		}
		prevStart = s.Start
		for id, t := range s.Section.Rows {
			if p, seen := last[id]; seen && (t.Ops < p.Ops || t.Bytes < p.Bytes) {
				return metricsInconclusive("sample %d: totals of client.%d decreased from %+v to %+v", i, id, p, t)
			}
			last[id] = t
		}
	}
	baseline := false
	for i, s := range w.Samples {
		if s.End.After(w.GateOpened) {
			continue
		}
		wt, wok := s.Section.Rows[w.Writer]
		it, iok := s.Section.Rows[w.Idle]
		if (wok && wt != (clientWriteTotals{})) || (iok && it != (clientWriteTotals{})) {
			return metricsInconclusive("baseline sample %d already shows writes (writer %+v, idle %+v)", i, wt, it)
		}
		if wok && iok {
			baseline = true
		}
	}
	if !baseline {
		return metricsInconclusive("no baseline sample before the gate showed both rows at zero")
	}
	var attributed *clientMetricsOutcome
	lost := ""
	for i, s := range w.Samples {
		if s.Start.Before(w.Acked) || s.End.After(w.Deadline) {
			continue
		}
		wt, wok := s.Section.Rows[w.Writer]
		it, iok := s.Section.Rows[w.Idle]
		if lost == "" {
			switch {
			case !s.Section.Section:
				lost = fmt.Sprintf("row identity lost after workload: sample %d has no metrics section", i)
			case !wok:
				lost = fmt.Sprintf("row identity lost after workload: sample %d lacks the writer client.%d row", i, w.Writer)
			case !iok:
				lost = fmt.Sprintf("row identity lost after workload: sample %d lacks the idle client.%d row", i, w.Idle)
			}
		}
		if iok && it != (clientWriteTotals{}) {
			return clientMetricsOutcome{Verdict: clientMetricsContradicted, Reason: "idle client row carries writes", Writer: wt, Idle: it}
		}
		if attributed == nil && wok && iok && wt.Ops >= 1 && wt.Bytes >= w.AckedBytes {
			attributed = &clientMetricsOutcome{Verdict: clientMetricsAttributed, Reason: "writer row covers the acknowledged write", Writer: wt, Idle: it}
		}
	}
	if lost != "" {
		return metricsInconclusive("%s", lost)
	}
	if attributed != nil {
		return *attributed
	}
	return metricsInconclusive("no observation sample inside the acknowledgement-to-deadline window showed both rows with the writer covering %d bytes", w.AckedBytes)
}
