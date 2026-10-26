//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// This file adds read-only map evidence to the retained-client receipt. When
// a map wait of TestCephFSOSDFullRetainedClient fails, the receipt also keeps
// what the retained client's own admin socket and each OSD's admin socket
// report at that moment, beside the unchanged wait record. It changes no
// readiness criterion, timeout or poke.
//
// What the commands show, at Ceph Squid 19.2.5 (abc7aa7f) and Tentacle 20.2.4
// (7f793731); the sources of both register the same commands and print the
// same fields:
//
//   - client "perf dump objecter": Objecter's map_epoch gauge (set after a map
//     was applied) and the map_full and map_inc counters of full and
//     incremental maps received since the client started. The deltas over one
//     wait show whether any map message reached the Objecter during it.
//   - client "objecter_requests": the Objecter's outstanding OSD operations,
//     linger operations and commands. It has no field for a pending map
//     subscription or map request, and an operation's entry has no map epoch.
//   - OSD "status": the OSD's own oldest_map and newest_map, printed as
//     strings by the source.
//   - OSD "dump_historic_ops" and "dump_ops_in_flight": tracked client
//     operations. An operation's description ends in "e<N>", the map epoch the
//     client stamped on that operation when it sent it. The history keeps a
//     bounded number of recent operations for a bounded time, so an operation
//     that is not listed is not evidence that it never arrived.
//
// No admin socket command shows an OSD's record of which map epoch it already
// shared with a client (the session's projected epoch), or whether a client
// has a map request pending at a monitor.

const (
	mapEvidenceCallBound = 20 * time.Second
	mapEvidenceTotal     = 3 * time.Minute
	mapEvidenceOpLimit   = 8
	mapEvidenceDescLimit = 200
)

// mapEvidenceSources is how the evidence reads the cluster. Every read is
// bounded by its context and has no side effect.
type mapEvidenceSources struct {
	// ClientAdmin runs a command on the retained client's admin socket.
	ClientAdmin func(ctx context.Context, args ...string) ([]byte, error)
	// OSDAdmin runs a command on the admin socket of one OSD.
	OSDAdmin func(ctx context.Context, osd int, args ...string) ([]byte, error)
	// MonCommand runs a read-only "ceph" command through the monitors.
	MonCommand func(ctx context.Context, args ...string) ([]byte, error)
	OSDs       []int
}

// mapCounters is the Objecter's map progress. A reading with Unavailable set
// carries no counters.
type mapCounters struct {
	Unavailable string  `json:"unavailable,omitempty"`
	MapEpoch    *uint64 `json:"map_epoch,omitempty"`
	MapFull     *uint64 `json:"map_full,omitempty"`
	MapInc      *uint64 `json:"map_inc,omitempty"`
}

func parseMapCounters(out []byte) mapCounters {
	var dump struct {
		Objecter *struct {
			MapEpoch *uint64 `json:"map_epoch"`
			MapFull  *uint64 `json:"map_full"`
			MapInc   *uint64 `json:"map_inc"`
		} `json:"objecter"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return mapCounters{Unavailable: fmt.Sprintf("perf dump objecter: %v", err)}
	}
	o := dump.Objecter
	switch {
	case o == nil:
		return mapCounters{Unavailable: "perf dump objecter has no objecter section"}
	case o.MapEpoch == nil || o.MapFull == nil || o.MapInc == nil:
		return mapCounters{Unavailable: "perf dump objecter lacks map_epoch, map_full or map_inc"}
	}
	return mapCounters{MapEpoch: o.MapEpoch, MapFull: o.MapFull, MapInc: o.MapInc}
}

// pendingOSDOp is one outstanding operation of the client's Objecter, with
// the fields that the dump printed.
type pendingOSDOp struct {
	TID      *uint64  `json:"tid,omitempty"`
	OSD      *int     `json:"osd,omitempty"`
	PG       *string  `json:"pg,omitempty"`
	Paused   *int     `json:"paused,omitempty"`
	Attempts *int     `json:"attempts,omitempty"`
	Age      *float64 `json:"age,omitempty"`
}

// objecterRequestsReading is the client's outstanding requests. Counts are
// pointers: an array the dump lacked is absent, not zero.
type objecterRequestsReading struct {
	Unavailable string         `json:"unavailable,omitempty"`
	Ops         *int           `json:"ops,omitempty"`
	LingerOps   *int           `json:"linger_ops,omitempty"`
	CommandOps  *int           `json:"command_ops,omitempty"`
	Pending     []pendingOSDOp `json:"pending,omitempty"`
	Truncated   bool           `json:"truncated,omitempty"`
}

func parseObjecterRequestsReading(out []byte) objecterRequestsReading {
	var dump struct {
		Ops        *[]pendingOSDOp    `json:"ops"`
		LingerOps  *[]json.RawMessage `json:"linger_ops"`
		CommandOps *[]json.RawMessage `json:"command_ops"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return objecterRequestsReading{Unavailable: fmt.Sprintf("objecter_requests: %v", err)}
	}
	if dump.Ops == nil {
		return objecterRequestsReading{Unavailable: "objecter_requests has no ops array"}
	}
	reading := objecterRequestsReading{Pending: *dump.Ops}
	count := len(*dump.Ops)
	reading.Ops = &count
	if dump.LingerOps != nil {
		n := len(*dump.LingerOps)
		reading.LingerOps = &n
	}
	if dump.CommandOps != nil {
		n := len(*dump.CommandOps)
		reading.CommandOps = &n
	}
	if len(reading.Pending) > mapEvidenceOpLimit {
		reading.Pending, reading.Truncated = reading.Pending[:mapEvidenceOpLimit], true
	}
	return reading
}

// clientStatusReading is the client's status read again after the wait ended.
// It can differ from the last sample of the wait, and it does not change the
// wait's outcome.
type clientStatusReading struct {
	Unavailable string  `json:"unavailable,omitempty"`
	ID          *uint64 `json:"id,omitempty"`
	OSDEpoch    *uint32 `json:"osd_epoch,omitempty"`
	Barrier     *uint32 `json:"osd_epoch_barrier,omitempty"`
	Blocklisted *bool   `json:"blocklisted,omitempty"`
}

func parseClientStatusReading(out []byte) clientStatusReading {
	var s clientStatusReading
	if err := json.Unmarshal(out, &s); err != nil {
		return clientStatusReading{Unavailable: fmt.Sprintf("status: %v", err)}
	}
	if s.ID == nil || s.OSDEpoch == nil || s.Barrier == nil || s.Blocklisted == nil {
		return clientStatusReading{Unavailable: "status lacks id, osd_epoch, osd_epoch_barrier or blocklisted"}
	}
	return s
}

// osdMapRange is an OSD's own map range. The source prints both as strings;
// a JSON number is read as the same value.
type osdMapRange struct {
	Unavailable string  `json:"unavailable,omitempty"`
	State       *string `json:"state,omitempty"`
	OldestMap   *uint64 `json:"oldest_map,omitempty"`
	NewestMap   *uint64 `json:"newest_map,omitempty"`
}

func epochField(raw json.RawMessage) (uint64, bool) {
	text := strings.TrimSpace(string(raw))
	if unquoted, err := strconv.Unquote(text); err == nil {
		text = unquoted
	}
	n, err := strconv.ParseUint(text, 10, 64)
	return n, err == nil && text != ""
}

func parseOSDMapRange(out []byte) osdMapRange {
	var status struct {
		State  *string         `json:"state"`
		Oldest json.RawMessage `json:"oldest_map"`
		Newest json.RawMessage `json:"newest_map"`
	}
	if err := json.Unmarshal(out, &status); err != nil {
		return osdMapRange{Unavailable: fmt.Sprintf("status: %v", err)}
	}
	oldest, okOldest := epochField(status.Oldest)
	newest, okNewest := epochField(status.Newest)
	if status.State == nil || !okOldest || !okNewest {
		return osdMapRange{Unavailable: "status lacks state, or oldest_map or newest_map is not an epoch"}
	}
	return osdMapRange{State: status.State, OldestMap: &oldest, NewestMap: &newest}
}

// clientOp is one tracked OSD operation of the retained client. Epoch is the
// map epoch printed at the end of the description; it stays absent when the
// description has none (an operation that was not decoded yet prints none).
type clientOp struct {
	TID         *uint64 `json:"tid,omitempty"`
	Epoch       *uint32 `json:"client_epoch,omitempty"`
	InitiatedAt string  `json:"initiated_at,omitempty"`
	Description string  `json:"description,omitempty"`
}

// osdOpsReading is the retained client's operations among those that one
// OSD lists. Listed counts all operations of the dump, Matching the client's.
type osdOpsReading struct {
	Unavailable string     `json:"unavailable,omitempty"`
	Listed      *int       `json:"listed,omitempty"`
	Matching    *int       `json:"matching,omitempty"`
	Ops         []clientOp `json:"ops,omitempty"`
	Truncated   bool       `json:"truncated,omitempty"`
}

var opEpochSuffix = regexp.MustCompile(` e([0-9]+)\)$`)

func parseOSDOpsReading(out []byte, client uint64) osdOpsReading {
	var dump struct {
		Ops *[]struct {
			Description *string `json:"description"`
			InitiatedAt string  `json:"initiated_at"`
			TypeData    struct {
				ClientInfo *struct {
					Client *string `json:"client"`
					TID    *uint64 `json:"tid"`
				} `json:"client_info"`
			} `json:"type_data"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return osdOpsReading{Unavailable: fmt.Sprintf("ops dump: %v", err)}
	}
	if dump.Ops == nil {
		return osdOpsReading{Unavailable: "ops dump has no ops array"}
	}
	listed, matching := len(*dump.Ops), 0
	reading := osdOpsReading{Listed: &listed}
	name := "client." + strconv.FormatUint(client, 10)
	for _, op := range *dump.Ops {
		info := op.TypeData.ClientInfo
		if info == nil || info.Client == nil || *info.Client != name {
			continue
		}
		matching++
		if len(reading.Ops) == mapEvidenceOpLimit {
			reading.Truncated = true
			continue
		}
		entry := clientOp{TID: info.TID, InitiatedAt: op.InitiatedAt}
		if op.Description != nil {
			entry.Description = *op.Description
			if len(entry.Description) > mapEvidenceDescLimit {
				entry.Description = entry.Description[:mapEvidenceDescLimit]
			}
			if m := opEpochSuffix.FindStringSubmatch(*op.Description); m != nil {
				if e, err := strconv.ParseUint(m[1], 10, 32); err == nil {
					epoch := uint32(e)
					entry.Epoch = &epoch
				}
			}
		}
		reading.Ops = append(reading.Ops, entry)
	}
	reading.Matching = &matching
	return reading
}

// retainedOSDView is what one OSD reports about itself and about the client.
type retainedOSDView struct {
	OSD      int           `json:"osd"`
	Maps     osdMapRange   `json:"maps"`
	Historic osdOpsReading `json:"historic_ops"`
	InFlight osdOpsReading `json:"ops_in_flight"`
}

// clusterEpochReading is the monitors' current OSDMap epoch at capture.
type clusterEpochReading struct {
	Unavailable string  `json:"unavailable,omitempty"`
	Epoch       *uint32 `json:"epoch,omitempty"`
}

func parseClusterEpoch(out []byte) clusterEpochReading {
	var dump struct {
		Epoch *uint32 `json:"epoch"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return clusterEpochReading{Unavailable: fmt.Sprintf("osd dump: %v", err)}
	}
	if dump.Epoch == nil || *dump.Epoch == 0 {
		return clusterEpochReading{Unavailable: "osd dump has no epoch"}
	}
	return clusterEpochReading{Epoch: dump.Epoch}
}

// retainedMapEvidence is the read-only evidence of one failed map wait. The
// reads happen after the wait ended, so none of them is the state at an
// earlier probe: the counter deltas cover the span from the start of the wait
// to the capture, which is longer than the wait itself.
type retainedMapEvidence struct {
	Phase         string `json:"phase"`
	TargetEpoch   uint32 `json:"target_epoch"`
	WaitOutcome   string `json:"wait_outcome"`
	CaptureFromMs int64  `json:"capture_from_ms"`
	CaptureToMs   int64  `json:"capture_to_ms"`
	// CaptureFromMs and CaptureToMs are measured from the baseline reading.
	Baseline     mapCounters             `json:"counters_at_wait_start"`
	Final        mapCounters             `json:"counters_at_capture"`
	MapFullDelta *uint64                 `json:"map_full_received,omitempty"`
	MapIncDelta  *uint64                 `json:"map_inc_received,omitempty"`
	DeltaNote    string                  `json:"delta_unavailable,omitempty"`
	ClientStatus clientStatusReading     `json:"client_status_at_capture"`
	Requests     objecterRequestsReading `json:"objecter_requests"`
	ClusterEpoch clusterEpochReading     `json:"cluster_epoch_at_capture"`
	OSDs         []retainedOSDView       `json:"osds"`
}

// mapEvidenceBaseline is the counters read when a wait starts.
type mapEvidenceBaseline struct {
	at       time.Time
	counters mapCounters
}

func (s mapEvidenceSources) run(ctx context.Context, call func(context.Context) ([]byte, error)) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, mapEvidenceCallBound)
	defer cancel()
	out, err := call(callCtx)
	if err == nil && callCtx.Err() != nil {
		err = callCtx.Err()
	}
	return out, err
}

func (s mapEvidenceSources) counters(ctx context.Context) mapCounters {
	out, err := s.run(ctx, func(c context.Context) ([]byte, error) { return s.ClientAdmin(c, "perf", "dump", "objecter") })
	if err != nil {
		return mapCounters{Unavailable: fmt.Sprintf("perf dump objecter: %v", err)}
	}
	return parseMapCounters(out)
}

// beginMapEvidence reads the Objecter's map counters before a wait starts. A
// failed read is kept as unavailable; it never stops the wait.
func (r *retainedClientReceipt) beginMapEvidence(ctx context.Context, phase string, src mapEvidenceSources) {
	if r.mapBaselines == nil {
		r.mapBaselines = map[string]mapEvidenceBaseline{}
	}
	r.mapBaselines[phase] = mapEvidenceBaseline{at: time.Now(), counters: src.counters(ctx)}
}

// captureMapEvidence records, for the wait of phase that just failed, what
// the client's and the OSDs' admin sockets report. It only reads, and every
// read that fails or prints an unexpected shape is recorded as unavailable.
// It never changes the wait record, so the original failure is unchanged.
func (r *retainedClientReceipt) captureMapEvidence(ctx context.Context, phase string, src mapEvidenceSources) {
	var wait *retainedMapWait
	for i := len(r.Waits) - 1; i >= 0; i-- {
		if r.Waits[i].Phase == phase {
			wait = &r.Waits[i]
			break
		}
	}
	if wait == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, mapEvidenceTotal)
	defer cancel()
	base, haveBase := r.mapBaselines[phase]
	if !haveBase {
		base = mapEvidenceBaseline{at: time.Now(), counters: mapCounters{Unavailable: "no baseline was read for this phase"}}
	}
	ev := retainedMapEvidence{Phase: phase, TargetEpoch: wait.TargetEpoch, WaitOutcome: wait.Outcome, Baseline: base.counters}
	ev.CaptureFromMs = time.Since(base.at).Milliseconds()
	ev.Final = src.counters(ctx)
	ev.MapFullDelta, ev.MapIncDelta, ev.DeltaNote = counterDeltas(ev.Baseline, ev.Final)
	if out, err := src.run(ctx, func(c context.Context) ([]byte, error) { return src.ClientAdmin(c, "status") }); err != nil {
		ev.ClientStatus = clientStatusReading{Unavailable: fmt.Sprintf("status: %v", err)}
	} else {
		ev.ClientStatus = parseClientStatusReading(out)
	}
	if out, err := src.run(ctx, func(c context.Context) ([]byte, error) { return src.ClientAdmin(c, "objecter_requests") }); err != nil {
		ev.Requests = objecterRequestsReading{Unavailable: fmt.Sprintf("objecter_requests: %v", err)}
	} else {
		ev.Requests = parseObjecterRequestsReading(out)
	}
	if out, err := src.run(ctx, func(c context.Context) ([]byte, error) { return src.MonCommand(c, "osd", "dump", "--format", "json") }); err != nil {
		ev.ClusterEpoch = clusterEpochReading{Unavailable: fmt.Sprintf("osd dump: %v", err)}
	} else {
		ev.ClusterEpoch = parseClusterEpoch(out)
	}
	for _, id := range src.OSDs {
		view := retainedOSDView{OSD: id}
		osdRead := func(args ...string) ([]byte, error) {
			return src.run(ctx, func(c context.Context) ([]byte, error) { return src.OSDAdmin(c, id, args...) })
		}
		if out, err := osdRead("status"); err != nil {
			view.Maps = osdMapRange{Unavailable: fmt.Sprintf("status: %v", err)}
		} else {
			view.Maps = parseOSDMapRange(out)
		}
		if out, err := osdRead("dump_historic_ops"); err != nil {
			view.Historic = osdOpsReading{Unavailable: fmt.Sprintf("dump_historic_ops: %v", err)}
		} else {
			view.Historic = parseOSDOpsReading(out, r.Client)
		}
		if out, err := osdRead("dump_ops_in_flight"); err != nil {
			view.InFlight = osdOpsReading{Unavailable: fmt.Sprintf("dump_ops_in_flight: %v", err)}
		} else {
			view.InFlight = parseOSDOpsReading(out, r.Client)
		}
		ev.OSDs = append(ev.OSDs, view)
	}
	ev.CaptureToMs = time.Since(base.at).Milliseconds()
	r.MapEvidence = append(r.MapEvidence, ev)
}

// counterDeltas is the number of full and incremental maps the Objecter
// received between two readings. A reading that is unavailable, or a counter
// that went down, gives no delta.
func counterDeltas(before, after mapCounters) (full, inc *uint64, note string) {
	switch {
	case before.Unavailable != "":
		return nil, nil, "baseline: " + before.Unavailable
	case after.Unavailable != "":
		return nil, nil, "capture: " + after.Unavailable
	case *after.MapFull < *before.MapFull || *after.MapInc < *before.MapInc:
		return nil, nil, "a map counter decreased between the readings"
	}
	f, i := *after.MapFull-*before.MapFull, *after.MapInc-*before.MapInc
	return &f, &i, ""
}

// retainedMapSources reads the retained client through its admin socket
// path in the client container and each owned OSD through the cluster's
// admin socket API. The OSD list is taken when the sources are built.
func retainedMapSources(t testing.TB, cluster *ceph.Container, client testcontainers.Container, asok string) mapEvidenceSources {
	src := mapEvidenceSources{
		ClientAdmin: func(ctx context.Context, args ...string) ([]byte, error) {
			result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
				Argv: append([]string{"ceph", "--admin-daemon", asok}, args...), Timeout: mapEvidenceCallBound,
				MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client map evidence",
			})
			if err != nil || result.ExitCode != 0 || !result.Complete() {
				return nil, fmt.Errorf("exit=%d complete=%t err=%v stderr=%.256s", result.ExitCode, result.Complete(), err, result.Stderr)
			}
			return result.Stdout, nil
		},
		OSDAdmin: func(ctx context.Context, osd int, args ...string) ([]byte, error) {
			return cluster.AdminSocket(ctx, fmt.Sprintf("osd.%d", osd), args...)
		},
		MonCommand: func(ctx context.Context, args ...string) ([]byte, error) { return cluster.Ceph(ctx, args...) },
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		src.OSDs = append(src.OSDs, osd.ID)
	}
	return src
}
