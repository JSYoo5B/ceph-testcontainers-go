//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The late phase of TestRADOSMutationContracts holds an OSD dispatch delay
// (probability 1) while an independent reader polls with a ten second
// operation timeout. When one of the reader's calls times out, the client
// checkpoint records the call but nothing about the cluster at that moment.
// The helpers below add a bounded, read-only record of the cluster before the
// delay is held and again, before it is released, when the late phase fails.
// They read the MON's OSDMap and status and osd.0's admin socket. They never
// mutate anything, never retry a client operation and never turn a missing,
// null or mistyped field into a value that counts as evidence.
const (
	radosLateStateLimit      = 24 << 10
	radosLateOpLimit         = 24
	radosLateEventLimit      = 16
	radosLateProbeBound      = 5 * time.Second
	radosLateStateBound      = 20 * time.Second
	radosLateAdminOutputSize = 1 << 20
)

// radosLateReader is the read-only boundary to the cluster. The real
// implementation runs the Ceph CLI and the admin socket in the control
// container; the fixtures replace it.
type radosLateReader interface {
	Ceph(ctx context.Context, args ...string) ([]byte, error)
	OSDAdminSocket(ctx context.Context, id int, args ...string) ([]byte, error)
}

type radosLateCluster struct{ cluster *ceph.Container }

func (r radosLateCluster) Ceph(ctx context.Context, args ...string) ([]byte, error) {
	return r.cluster.Ceph(ctx, args...)
}

func (r radosLateCluster) OSDAdminSocket(ctx context.Context, id int, args ...string) ([]byte, error) {
	osd, err := findOSD(ctx, r.cluster, id)
	if err != nil {
		return nil, err
	}
	return osd.AdminSocket(ctx, args...)
}

// radosLateProbe is one read: when it ran (Unix seconds, to join the client
// checkpoint's wall clock) and why it produced no evidence, if it did not.
type radosLateProbe struct {
	Name      string  `json:"name"`
	StartUnix float64 `json:"start_unix"`
	EndUnix   float64 `json:"end_unix"`
	Bytes     int     `json:"bytes"`
	Error     string  `json:"error,omitempty"`
}

type radosLateEvent struct {
	Event    string   `json:"event"`
	Time     string   `json:"time"`
	Duration *float64 `json:"duration,omitempty"`
}

// radosLateOp is an operation as osd.0's tracker reported it. Client and
// Epoch are read from the description ("osd_op(client.4218.0:51 2.0 ... e15)")
// and stay absent when the description does not have that form.
type radosLateOp struct {
	Description string `json:"description"`
	Client      string `json:"client,omitempty"`
	Epoch       *int   `json:"epoch,omitempty"`
	// Initiated is the tracker's initiated_at text and InitiatedUnix its Unix
	// seconds; the latter is absent when the text is not in the tracker's
	// timestamp form.
	Initiated     string   `json:"initiated_at,omitempty"`
	InitiatedUnix *float64 `json:"initiated_unix,omitempty"`
	// Waits lists every event of the operation (before the event cap) that
	// the pinned source emits when an op is parked, so that they survive the
	// shedding of the full timeline. WaitsForMap is true for the two events
	// that mean the PG's map is older than the epoch the client stamped.
	Waits       []radosLateEvent `json:"waits,omitempty"`
	WaitsForMap bool             `json:"waits_for_map,omitempty"`
	// EpochLead is the op's stamped epoch minus the newest map osd.0 had
	// stored when it was dumped; EpochAtInitiated is the newest epoch among
	// the timeline observations (the baseline and the epochs read) that was
	// already committed when the op was initiated. When epochs were skipped or
	// later omitted it is the newest observation, not proof of the globally
	// exact epoch at that moment. Both are absent unless every input was read.
	EpochLead        *int             `json:"epoch_lead_over_osd,omitempty"`
	EpochAtInitiated *int             `json:"epoch_committed_at_initiated,omitempty"`
	Age              *float64         `json:"age,omitempty"`
	Duration         *float64         `json:"duration,omitempty"`
	FlagPoint        string           `json:"flag_point,omitempty"`
	Events           []radosLateEvent `json:"events,omitempty"`
	EventsDropped    int              `json:"events_dropped,omitempty"`
}

// radosLateWaitEvent reports whether an operation event is one that the
// pinned source emits for a parked op: every PrimaryLogPG mark_delayed
// reason begins with "waiting" except the two map waits.
func radosLateWaitEvent(event string) (wait, forMap bool) {
	switch event {
	case "op must wait for map", "waiting_for_map not empty":
		return true, true
	}
	return strings.HasPrefix(event, "waiting "), false
}

// radosLateTime parses the timestamp form of the OSD op tracker and the
// OSDMap "modified" field. A zero or pre-epoch time is not a timestamp.
func radosLateTime(text string) (float64, bool) {
	at, err := time.Parse("2006-01-02T15:04:05.999999-0700", text)
	if err != nil || at.Unix() <= 0 {
		return 0, false
	}
	return float64(at.UnixNano()) / 1e9, true
}

// radosLateOSDInfo is one "osd dump" entry. UpThru and UpFrom are absent when
// the dump did not carry them.
type radosLateOSDInfo struct {
	ID     int  `json:"osd"`
	Up     bool `json:"up"`
	In     bool `json:"in"`
	UpFrom *int `json:"up_from,omitempty"`
	UpThru *int `json:"up_thru,omitempty"`
}

type radosLatePGState struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

// radosLateClusterState carries only what a probe established. A pointer or
// slice that is nil means that the field is unavailable, never zero; the
// matching probe's Error says why.
type radosLateClusterState struct {
	Label  string           `json:"label"`
	Pool   string           `json:"pool"`
	Probes []radosLateProbe `json:"probes"`
	Notes  []string         `json:"notes,omitempty"`

	MonEpoch        *int     `json:"mon_epoch,omitempty"`
	MonModified     string   `json:"mon_modified,omitempty"`
	MonModifiedUnix *float64 `json:"mon_modified_unix,omitempty"`
	PoolID          *int     `json:"pool_id,omitempty"`
	// What a MON epoch bump can be traced to: the flags and change stamps of
	// the map, the pool's last_change epoch and each OSD's up_from/up_thru.
	// Comparing the failure state with the pre-late state shows which changed.
	MonFlags       string             `json:"mon_flags,omitempty"`
	LastUpChange   string             `json:"last_up_change,omitempty"`
	LastInChange   string             `json:"last_in_change,omitempty"`
	PoolLastChange string             `json:"pool_last_change,omitempty"`
	OSDs           []radosLateOSDInfo `json:"osds,omitempty"`
	OSDsUpAndIn    *bool              `json:"osds_up_and_in,omitempty"`
	PGsActiveClean *bool              `json:"pgs_active_clean,omitempty"`
	PGStates       []radosLatePGState `json:"pg_states,omitempty"`
	// Timeline holds one entry per epoch committed after the baseline, read
	// from "osd dump <epoch>" (see radosLateEpoch).
	Timeline        []radosLateEpoch `json:"epoch_timeline,omitempty"`
	TimelineSkipped int              `json:"epoch_timeline_skipped,omitempty"`
	// Epochs and changes that were read but dropped to fit the record budget.
	// TimelineSkipped counts epochs never read; these count epochs and
	// changes that were read and then shed.
	TimelineOmitted        int `json:"epoch_timeline_omitted,omitempty"`
	TimelineChangesOmitted int `json:"epoch_timeline_changes_omitted,omitempty"`
	fallbackStages         int

	OSDState     string `json:"osd_state,omitempty"`
	OSDNewestMap *int   `json:"osd_newest_map,omitempty"`
	OSDOldestMap *int   `json:"osd_oldest_map,omitempty"`
	// OSDMapLag is the MON epoch minus the newest map osd.0 has stored. It is
	// present only when both were read.
	OSDMapLag *int `json:"osd_map_lag,omitempty"`
	// EpochAdvance is the MON epoch minus the baseline's, present only when
	// both were read.
	EpochAdvance *int `json:"epoch_advance_since_baseline,omitempty"`

	// The totals are the lengths of the dumps that were read, before the cap;
	// zero is a conclusive empty queue and absent means the dump was not read.
	InFlightTotal   *int          `json:"in_flight_total,omitempty"`
	InFlight        []radosLateOp `json:"in_flight,omitempty"`
	InFlightDropped int           `json:"in_flight_dropped,omitempty"`
	HistoricTotal   *int          `json:"historic_total,omitempty"`
	Historic        []radosLateOp `json:"historic_by_duration,omitempty"`
	HistoricDropped int           `json:"historic_dropped,omitempty"`

	// Ready is true when every input is conclusive and healthy, false when
	// any conclusive input is not, and absent when an input is unavailable
	// and nothing conclusive contradicts readiness.
	Ready     *bool `json:"ready,omitempty"`
	Truncated bool  `json:"truncated,omitempty"`
}

var radosLateOpEpoch = regexp.MustCompile(` e([0-9]+)\)$`)

func radosLateInt(value json.Number) (int, error) {
	if i, err := value.Int64(); err == nil && i >= math.MinInt32 && i <= math.MaxInt32 {
		return int(i), nil
	}
	return 0, fmt.Errorf("%q is not an integer", value.String())
}

func radosLateDecode(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var top map[string]json.RawMessage
	if err := decoder.Decode(&top); err != nil {
		return nil, fmt.Errorf("not a JSON object: %w", err)
	}
	if top == nil {
		return nil, errors.New("JSON null, not an object")
	}
	// More() only reports whether another element of an enclosing array or
	// object follows, so a stray "}" or "]" would pass it. Require that the
	// next read finds the end of the input; whitespace alone is fine.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON object")
	}
	return top, nil
}

func radosLateField(object map[string]json.RawMessage, name string, into any) error {
	raw, ok := object[name]
	if !ok {
		return fmt.Errorf("field %q is missing", name)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("field %q is null", name)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("field %q has the wrong type: %w", name, err)
	}
	return nil
}

func radosLateOptional(object map[string]json.RawMessage, name string, into any) (bool, error) {
	if _, ok := object[name]; !ok {
		return false, nil
	}
	return true, radosLateField(object, name, into)
}

// radosLateNumber decodes a JSON number. encoding/json would also accept a
// quoted number such as "15" into a json.Number; a string is the wrong type.
func radosLateNumber(object map[string]json.RawMessage, name string, into *json.Number) error {
	if err := radosLateField(object, name, into); err != nil {
		return err
	}
	if raw := bytes.TrimSpace(object[name]); len(raw) == 0 || raw[0] == '"' {
		return fmt.Errorf("field %q has the wrong type: a string, not a number", name)
	}
	return nil
}

func radosLateIntField(object map[string]json.RawMessage, name string) (int, error) {
	var number json.Number
	if err := radosLateNumber(object, name, &number); err != nil {
		return 0, err
	}
	value, err := radosLateInt(number)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	return value, nil
}

func radosLateFloatPointer(object map[string]json.RawMessage, name string) (*float64, error) {
	var number json.Number
	if _, ok := object[name]; !ok {
		return nil, nil
	}
	if err := radosLateNumber(object, name, &number); err != nil {
		return nil, err
	}
	value, parseErr := number.Float64()
	if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("field %q: %q is not a finite number", name, number.String())
	}
	return &value, nil
}

// parseRadosLateOSDDump reads "osd dump": the epoch is required and positive,
// "osds" is required, "pools" is optional and only locates the pool.
func parseRadosLateOSDDump(raw []byte, pool string, state *radosLateClusterState) error {
	top, err := radosLateDecode(raw)
	if err != nil {
		return err
	}
	epoch, err := radosLateIntField(top, "epoch")
	if err != nil {
		return err
	}
	if epoch <= 0 {
		return fmt.Errorf("epoch %d is not positive", epoch)
	}
	var osds []map[string]json.RawMessage
	if err := radosLateField(top, "osds", &osds); err != nil {
		return err
	}
	allUpAndIn := len(osds) > 0
	infos := make([]radosLateOSDInfo, 0, len(osds))
	for index, osd := range osds {
		if osd == nil {
			return fmt.Errorf("osds[%d] is null", index)
		}
		id, idErr := radosLateIntField(osd, "osd")
		up, upErr := radosLateIntField(osd, "up")
		in, inErr := radosLateIntField(osd, "in")
		if err := errors.Join(idErr, upErr, inErr); err != nil {
			return fmt.Errorf("osds[%d]: %w", index, err)
		}
		info := radosLateOSDInfo{ID: id, Up: up == 1, In: in == 1}
		for name, target := range map[string]**int{"up_from": &info.UpFrom, "up_thru": &info.UpThru} {
			if _, ok := osd[name]; !ok {
				continue
			}
			value, err := radosLateIntField(osd, name)
			if err != nil {
				return fmt.Errorf("osds[%d]: %w", index, err)
			}
			*target = &value
		}
		infos = append(infos, info)
		if !info.Up || !info.In {
			allUpAndIn = false
		}
	}
	var modified, flags, lastUp, lastIn string
	for name, target := range map[string]*string{"modified": &modified, "flags": &flags, "last_up_change": &lastUp, "last_in_change": &lastIn} {
		if _, err := radosLateOptional(top, name, target); err != nil {
			return err
		}
	}
	var poolID *int
	var poolChange string
	if _, ok := top["pools"]; ok {
		var pools []map[string]json.RawMessage
		if err := radosLateField(top, "pools", &pools); err != nil {
			return err
		}
		for index, entry := range pools {
			if entry == nil {
				return fmt.Errorf("pools[%d] is null", index)
			}
			var name string
			if err := radosLateField(entry, "pool_name", &name); err != nil {
				return fmt.Errorf("pools[%d]: %w", index, err)
			}
			if name != pool {
				continue
			}
			id, err := radosLateIntField(entry, "pool")
			if err != nil {
				return fmt.Errorf("pools[%d]: %w", index, err)
			}
			poolID = &id
			if _, err := radosLateOptional(entry, "last_change", &poolChange); err != nil {
				return fmt.Errorf("pools[%d]: %w", index, err)
			}
		}
	}
	state.MonEpoch, state.MonModified, state.PoolID, state.OSDsUpAndIn = &epoch, modified, poolID, &allUpAndIn
	if unix, ok := radosLateTime(modified); ok {
		state.MonModifiedUnix = &unix
	}
	state.MonFlags, state.LastUpChange, state.LastInChange, state.PoolLastChange, state.OSDs = flags, lastUp, lastIn, poolChange, infos
	if poolID == nil {
		state.Notes = append(state.Notes, "pool "+strconv.Quote(pool)+" is not in the OSDMap that was read")
	}
	return nil
}

// parseRadosLateStatus reads "status": the OSD counts and every PG state are
// required. PGsActiveClean follows Container.WaitForClean: every PG exactly
// active+clean and the counts add up to the PG total.
func parseRadosLateStatus(raw []byte, state *radosLateClusterState) error {
	top, err := radosLateDecode(raw)
	if err != nil {
		return err
	}
	var osdmap, pgmap map[string]json.RawMessage
	if err := errors.Join(radosLateField(top, "osdmap", &osdmap), radosLateField(top, "pgmap", &pgmap)); err != nil {
		return err
	}
	numPGs, err := radosLateIntField(pgmap, "num_pgs")
	if err != nil {
		return fmt.Errorf("pgmap: %w", err)
	}
	var byState []map[string]json.RawMessage
	if err := radosLateField(pgmap, "pgs_by_state", &byState); err != nil {
		return fmt.Errorf("pgmap: %w", err)
	}
	var states []radosLatePGState
	clean, total := true, 0
	for index, entry := range byState {
		if entry == nil {
			return fmt.Errorf("pgmap: pgs_by_state[%d] is null", index)
		}
		var name string
		if err := radosLateField(entry, "state_name", &name); err != nil {
			return fmt.Errorf("pgmap: pgs_by_state[%d]: %w", index, err)
		}
		count, err := radosLateIntField(entry, "count")
		if err != nil {
			return fmt.Errorf("pgmap: pgs_by_state[%d]: %w", index, err)
		}
		states = append(states, radosLatePGState{State: name, Count: count})
		clean = clean && name == "active+clean"
		total += count
	}
	if numPGs < 0 || total < 0 {
		return errors.New("pgmap: negative PG count")
	}
	clean = clean && numPGs > 0 && total == numPGs
	state.PGStates, state.PGsActiveClean = states, &clean
	return nil
}

// parseRadosLateOSDStatus reads the admin socket "status": OSD::status
// prints the map epochs as JSON strings of digits.
func parseRadosLateOSDStatus(raw []byte, state *radosLateClusterState) error {
	top, err := radosLateDecode(raw)
	if err != nil {
		return err
	}
	var name, newest, oldest string
	if err := errors.Join(radosLateField(top, "state", &name), radosLateField(top, "newest_map", &newest), radosLateField(top, "oldest_map", &oldest)); err != nil {
		return err
	}
	if name == "" {
		return errors.New(`field "state" is empty`)
	}
	newestMap, newestErr := strconv.Atoi(newest)
	oldestMap, oldestErr := strconv.Atoi(oldest)
	if err := errors.Join(newestErr, oldestErr); err != nil || newestMap <= 0 || oldestMap <= 0 || oldestMap > newestMap {
		return fmt.Errorf("map range %q..%q is not a positive ascending epoch pair", oldest, newest)
	}
	state.OSDState, state.OSDNewestMap, state.OSDOldestMap = name, &newestMap, &oldestMap
	return nil
}

// parseRadosLateOps reads dump_ops_in_flight or dump_historic_ops_by_duration.
// "ops" must be an array; one malformed operation makes the whole dump
// unavailable rather than a shorter list that would look complete.
func parseRadosLateOps(raw []byte) (ops []radosLateOp, count *int, err error) {
	top, err := radosLateDecode(raw)
	if err != nil {
		return nil, nil, err
	}
	var entries []map[string]json.RawMessage
	if err := radosLateField(top, "ops", &entries); err != nil {
		return nil, nil, err
	}
	if _, ok := top["num_ops"]; ok {
		n, err := radosLateIntField(top, "num_ops")
		if err != nil {
			return nil, nil, err
		}
		count = &n
	}
	ops = make([]radosLateOp, 0, len(entries))
	for index, entry := range entries {
		op, err := parseRadosLateOp(entry)
		if err != nil {
			return nil, nil, fmt.Errorf("ops[%d]: %w", index, err)
		}
		ops = append(ops, op)
	}
	return ops, count, nil
}

func parseRadosLateOp(entry map[string]json.RawMessage) (radosLateOp, error) {
	var op radosLateOp
	if entry == nil {
		return op, errors.New("null operation")
	}
	if err := radosLateField(entry, "description", &op.Description); err != nil {
		return op, err
	}
	if op.Description == "" {
		return op, errors.New(`field "description" is empty`)
	}
	if strings.HasPrefix(op.Description, "osd_op(client.") {
		op.Client = trackedClient(op.Description)
	}
	if match := radosLateOpEpoch.FindStringSubmatch(op.Description); match != nil {
		if epoch, err := strconv.Atoi(match[1]); err == nil {
			op.Epoch = &epoch
		}
	}
	var err error
	if _, err := radosLateOptional(entry, "initiated_at", &op.Initiated); err != nil {
		return op, err
	}
	if op.Initiated != "" {
		if unix, ok := radosLateTime(op.Initiated); ok {
			op.InitiatedUnix = &unix
		}
	}
	if op.Age, err = radosLateFloatPointer(entry, "age"); err != nil {
		return op, err
	}
	if op.Duration, err = radosLateFloatPointer(entry, "duration"); err != nil {
		return op, err
	}
	var typeData map[string]json.RawMessage
	present, err := radosLateOptional(entry, "type_data", &typeData)
	if err != nil || !present {
		return op, err
	}
	if _, err := radosLateOptional(typeData, "flag_point", &op.FlagPoint); err != nil {
		return op, err
	}
	var events []map[string]json.RawMessage
	if present, err := radosLateOptional(typeData, "events", &events); err != nil || !present {
		return op, err
	}
	for index, entry := range events {
		if entry == nil {
			return op, fmt.Errorf("events[%d] is null", index)
		}
		var event radosLateEvent
		if err := errors.Join(radosLateField(entry, "event", &event.Event), radosLateField(entry, "time", &event.Time)); err != nil {
			return op, fmt.Errorf("events[%d]: %w", index, err)
		}
		if event.Duration, err = radosLateFloatPointer(entry, "duration"); err != nil {
			return op, fmt.Errorf("events[%d]: %w", index, err)
		}
		if wait, forMap := radosLateWaitEvent(event.Event); wait {
			if len(op.Waits) < radosLateEventLimit {
				op.Waits = append(op.Waits, event)
			}
			op.WaitsForMap = op.WaitsForMap || forMap
		}
		if len(op.Events) == radosLateEventLimit {
			op.EventsDropped++
			continue
		}
		op.Events = append(op.Events, event)
	}
	return op, nil
}

// radosLateRun runs one probe inside the remaining overall budget. Output
// that arrives after the probe's own deadline is discarded: it is not
// evidence of the moment the probe was meant to describe.
func radosLateRun(overall context.Context, state *radosLateClusterState, name string, call func(context.Context) ([]byte, error), parse func([]byte) error) {
	probe := radosLateProbe{Name: name, StartUnix: float64(time.Now().UnixNano()) / 1e9}
	finish := func(err error) {
		probe.EndUnix = float64(time.Now().UnixNano()) / 1e9
		if err != nil {
			probe.Error = err.Error()
		}
		state.Probes = append(state.Probes, probe)
	}
	if err := overall.Err(); err != nil {
		finish(fmt.Errorf("not run, diagnostic budget spent: %w", err))
		return
	}
	ctx, cancel := context.WithTimeout(overall, radosLateProbeBound)
	defer cancel()
	raw, err := call(ctx)
	probe.Bytes = len(raw)
	if ctxErr := ctx.Err(); ctxErr != nil {
		finish(fmt.Errorf("answer after the probe deadline is not evidence: %w", ctxErr))
		return
	}
	if err != nil {
		finish(fmt.Errorf("command failed: %w", err))
		return
	}
	if len(raw) > radosLateAdminOutputSize {
		finish(fmt.Errorf("output of %d bytes exceeds %d", len(raw), radosLateAdminOutputSize))
		return
	}
	if err := parse(raw); err != nil {
		finish(fmt.Errorf("unusable output: %w", err))
		return
	}
	finish(nil)
}

// collectRadosLateClusterState reads the cluster once. The parent should not
// be cancelled by the failure being diagnosed (derive it with WithoutCancel).
// baseline, when not nil, is an earlier state of the same cluster.
func collectRadosLateClusterState(parent context.Context, reader radosLateReader, label, pool string, osd int, baseline *radosLateClusterState) *radosLateClusterState {
	overall, cancel := context.WithTimeout(parent, radosLateStateBound)
	defer cancel()
	state := &radosLateClusterState{Label: label, Pool: pool}
	var inFlight, historic []radosLateOp
	var inFlightCount *int
	// The OSD first: its queue changes fastest.
	radosLateRun(overall, state, "ops_in_flight", func(ctx context.Context) ([]byte, error) {
		return reader.OSDAdminSocket(ctx, osd, "dump_ops_in_flight")
	}, func(raw []byte) (err error) {
		inFlight, inFlightCount, err = parseRadosLateOps(raw)
		return err
	})
	radosLateRun(overall, state, "historic_ops_by_duration", func(ctx context.Context) ([]byte, error) {
		return reader.OSDAdminSocket(ctx, osd, "dump_historic_ops_by_duration")
	}, func(raw []byte) (err error) {
		historic, _, err = parseRadosLateOps(raw)
		return err
	})
	radosLateRun(overall, state, "osd_status", func(ctx context.Context) ([]byte, error) {
		return reader.OSDAdminSocket(ctx, osd, "status")
	}, func(raw []byte) error { return parseRadosLateOSDStatus(raw, state) })
	radosLateRun(overall, state, "osd_dump", func(ctx context.Context) ([]byte, error) {
		return reader.Ceph(ctx, "osd", "dump", "--format", "json")
	}, func(raw []byte) error { return parseRadosLateOSDDump(raw, pool, state) })
	radosLateRun(overall, state, "status", func(ctx context.Context) ([]byte, error) {
		return reader.Ceph(ctx, "status", "--format", "json")
	}, func(raw []byte) error { return parseRadosLateStatus(raw, state) })

	if inFlight != nil {
		total := len(inFlight)
		state.InFlightTotal = &total
		if inFlightCount != nil && *inFlightCount != total {
			state.Notes = append(state.Notes, fmt.Sprintf("dump_ops_in_flight num_ops %d differs from its %d listed operations", *inFlightCount, total))
		}
		state.InFlight, state.InFlightDropped = radosLateCap(inFlight)
	}
	if historic != nil {
		total := len(historic)
		state.HistoricTotal = &total
		state.Historic, state.HistoricDropped = radosLateCap(historic)
	}
	if state.MonEpoch != nil && state.OSDNewestMap != nil {
		lag := *state.MonEpoch - *state.OSDNewestMap
		state.OSDMapLag = &lag
	}
	if baseline != nil && baseline.MonEpoch != nil && state.MonEpoch != nil {
		advance := *state.MonEpoch - *baseline.MonEpoch
		state.EpochAdvance = &advance
	}
	collectRadosLateEpochTimeline(overall, reader, pool, state, baseline)
	joinRadosLateOps(state, baseline)
	noteRadosLateTimelineGap(state)
	state.Ready = radosLateReady(state)
	boundRadosLateState(state)
	return state
}

// radosLateRelevant marks the operations that can explain a stall: one that
// was parked, or one that took or has so far taken a second or more.
func radosLateRelevant(op radosLateOp) bool {
	if len(op.Waits) > 0 {
		return true
	}
	return (op.Duration != nil && *op.Duration >= 1) || (op.Age != nil && *op.Age >= 1)
}

// radosLateCap keeps the relevant operations first (each group in the order
// the OSD reported it) and caps the list, so that the cap drops the
// uninformative short operations before it drops a parked or slow one.
func radosLateCap(ops []radosLateOp) ([]radosLateOp, int) {
	ranked := slices.Clone(ops)
	slices.SortStableFunc(ranked, func(a, b radosLateOp) int {
		switch ra, rb := radosLateRelevant(a), radosLateRelevant(b); {
		case ra && !rb:
			return -1
		case !ra && rb:
			return 1
		}
		return 0
	})
	if len(ranked) <= radosLateOpLimit {
		return ranked, 0
	}
	return ranked[:radosLateOpLimit], len(ranked) - radosLateOpLimit
}

// radosLateReady is a three-valued conjunction: a conclusive "no" wins over an
// unavailable input, and unavailable inputs never count as "yes".
func radosLateReady(state *radosLateClusterState) *bool {
	inputs := []*bool{state.OSDsUpAndIn, state.PGsActiveClean}
	if state.OSDState != "" {
		active := state.OSDState == "active"
		inputs = append(inputs, &active)
	} else {
		inputs = append(inputs, nil)
	}
	if state.OSDMapLag != nil {
		caught := *state.OSDMapLag == 0
		inputs = append(inputs, &caught)
	} else {
		inputs = append(inputs, nil)
	}
	unknown := false
	for _, input := range inputs {
		switch {
		case input == nil:
			unknown = true
		case !*input:
			no := false
			return &no
		}
	}
	if unknown {
		return nil
	}
	yes := true
	return &yes
}

// boundRadosLateState keeps the encoded state within radosLateStateLimit by
// dropping detail in a fixed order and recording that it did.
// radosLateFallbackStages is the number of whole-record shedding stages that
// follow the per-field caps and the operation shedding.
const radosLateFallbackStages = 9

// radosLateProbeNameLimit bounds a probe name; the real names are shorter.
const radosLateProbeNameLimit = 48

const (
	radosLateErrorLimit = 512
	radosLateTextLimit  = 128
	radosLateNoteLimit  = 8
	radosLateListLimit  = 32
)

// radosLateText cuts text to at most limit bytes of valid UTF-8 and says how
// much was cut, so a cut is never mistaken for the whole value.
func radosLateText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return fmt.Sprintf("%s...[cut %d bytes]", text[:cut], len(text)-cut), true
}

// capRadosLateFields bounds every retained variable-length field that is not
// an operation list: probe errors, notes, parsed strings and the PG and OSD
// lists. It reports whether anything was cut.
func capRadosLateFields(state *radosLateClusterState) bool {
	cut := false
	text := func(field *string, limit int) {
		var did bool
		*field, did = radosLateText(*field, limit)
		cut = cut || did
	}
	text(&state.Label, radosLateTextLimit)
	text(&state.Pool, radosLateTextLimit)
	text(&state.MonModified, radosLateTextLimit)
	text(&state.MonFlags, radosLateTextLimit)
	text(&state.LastUpChange, radosLateTextLimit)
	text(&state.LastInChange, radosLateTextLimit)
	text(&state.PoolLastChange, radosLateTextLimit)
	text(&state.OSDState, radosLateTextLimit)
	for index := range state.Probes {
		text(&state.Probes[index].Name, radosLateProbeNameLimit)
		text(&state.Probes[index].Error, radosLateErrorLimit)
	}
	if len(state.Probes) > radosLateListLimit {
		state.Probes, cut = state.Probes[:radosLateListLimit], true
	}
	for index := range state.Notes {
		text(&state.Notes[index], radosLateErrorLimit)
	}
	if len(state.Notes) > radosLateNoteLimit {
		state.Notes, cut = state.Notes[:radosLateNoteLimit], true
	}
	for index := range state.PGStates {
		text(&state.PGStates[index].State, radosLateTextLimit)
	}
	for index := range state.Timeline {
		text(&state.Timeline[index].Modified, radosLateTextLimit)
		for change := range state.Timeline[index].Changes {
			text(&state.Timeline[index].Changes[change], radosLateTextLimit)
		}
		if len(state.Timeline[index].Changes) > radosLateListLimit {
			state.Timeline[index].Changes, cut = state.Timeline[index].Changes[:radosLateListLimit], true
		}
	}
	if len(state.Timeline) > radosLateEpochLimit {
		state.Timeline, cut = state.Timeline[:radosLateEpochLimit], true
	}
	if len(state.PGStates) > radosLateListLimit {
		state.PGStates, cut = state.PGStates[:radosLateListLimit], true
	}
	if len(state.OSDs) > radosLateListLimit {
		state.OSDs, cut = state.OSDs[:radosLateListLimit], true
	}
	for _, list := range [][]radosLateOp{state.Historic, state.InFlight} {
		for index := range list {
			text(&list[index].Description, radosLateErrorLimit)
			text(&list[index].Client, radosLateTextLimit)
			text(&list[index].FlagPoint, radosLateTextLimit)
			text(&list[index].Initiated, radosLateTextLimit)
			for wait := range list[index].Waits {
				text(&list[index].Waits[wait].Event, radosLateTextLimit)
				text(&list[index].Waits[wait].Time, radosLateTextLimit)
			}
			for event := range list[index].Events {
				text(&list[index].Events[event].Event, radosLateTextLimit)
				text(&list[index].Events[event].Time, radosLateTextLimit)
			}
		}
	}
	return cut
}

// boundRadosLateState keeps the encoded state within radosLateStateLimit by
// capping every variable-length field and then dropping detail in a fixed
// order, recording that it did. The final encoded size is what is bounded.
func boundRadosLateState(state *radosLateClusterState) {
	size := func() int {
		encoded, err := json.Marshal(state)
		if err != nil {
			return math.MaxInt
		}
		return len(encoded)
	}
	if capRadosLateFields(state) {
		state.Truncated = true
	}
	boundRadosLateTimelineShare(state)
	if size() <= radosLateStateLimit {
		return
	}
	state.Truncated = true
	// Shed the timeline of the uninformative operations first, then those
	// operations, and only then the events of the relevant ones (their
	// parked-op events stay in Waits regardless).
	lists := []*[]radosLateOp{&state.Historic, &state.InFlight}
	dropEvents := func(only func(radosLateOp) bool) {
		for _, list := range lists {
			for index := range *list {
				if only((*list)[index]) {
					(*list)[index].EventsDropped += len((*list)[index].Events)
					(*list)[index].Events = nil
				}
			}
		}
	}
	dropEvents(func(op radosLateOp) bool { return !radosLateRelevant(op) })
	if size() <= radosLateStateLimit {
		return
	}
	for size() > radosLateStateLimit {
		dropped := false
		for _, list := range lists {
			for index := len(*list) - 1; index >= 0; index-- {
				if !radosLateRelevant((*list)[index]) {
					*list = slices.Delete(*list, index, index+1)
					if list == &state.Historic {
						state.HistoricDropped++
					} else {
						state.InFlightDropped++
					}
					dropped = true
					break
				}
			}
			if dropped {
				break
			}
		}
		if !dropped {
			break
		}
	}
	if size() <= radosLateStateLimit {
		return
	}
	dropEvents(func(radosLateOp) bool { return true })
	if size() <= radosLateStateLimit {
		return
	}
	for size() > radosLateStateLimit && (len(state.Historic) > 0 || len(state.InFlight) > 0) {
		if len(state.Historic) > 0 {
			state.Historic, state.HistoricDropped = state.Historic[:len(state.Historic)-1], state.HistoricDropped+1
		} else {
			state.InFlight, state.InFlightDropped = state.InFlight[:len(state.InFlight)-1], state.InFlightDropped+1
		}
	}
	for _, step := range radosLateFallbackSteps(state, size) {
		if size() <= radosLateStateLimit {
			return
		}
		step()
		state.fallbackStages++
	}
}

// radosLateFallbackSteps are the whole-record shedding stages that follow the
// per-field caps and the operation shedding, in order of least value first.
// Escaping can still make capped text larger than its byte count, so these
// run until the encoded record fits; the probe table and the epoch, readiness
// and lag fields are kept last.
func radosLateFallbackSteps(state *radosLateClusterState, size func() int) []func() {
	return []func(){
		func() { state.Notes = nil },
		func() { state.PGStates = nil },
		func() { state.OSDs = nil },
		func() {
			state.MonFlags, state.LastUpChange, state.LastInChange, state.PoolLastChange, state.MonModified = "", "", "", "", ""
		},
		// The epoch timeline is shed after the other detail and before the
		// probe table, in three steps, each counted.
		func() { shedRadosLateTimelineChanges(state, 1) },
		func() { shedRadosLateTimelineChanges(state, 0) },
		func() {
			shedRadosLateTimelineEpochs(state, func() bool { return size() <= radosLateStateLimit })
		},
		func() {
			for index := range state.Probes {
				state.Probes[index].Error, _ = radosLateText(state.Probes[index].Error, 64)
			}
		},
		func() {
			for index := range state.Probes {
				if state.Probes[index].Error != "" {
					state.Probes[index].Error = "error cut to fit the record budget"
				}
			}
		},
	}
}

func logRadosLateClusterState(t *testing.T, state *radosLateClusterState) {
	t.Helper()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Logf("RADOS_LATE_CLUSTER_STATE label=%s unavailable=%q", state.Label, err)
		return
	}
	t.Logf("RADOS_LATE_CLUSTER_STATE label=%s bytes=%d limit=%d %s", state.Label, len(encoded), radosLateStateLimit, encoded)
}
