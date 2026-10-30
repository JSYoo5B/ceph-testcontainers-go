//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	mobycl "github.com/moby/moby/client"
)

// This file is a test-only, per-target accounting adapter over the decoded
// Docker ContainerStats JSON. It retains the sample time, the frozen
// container/start generation, the source and the units of each counter.
//
// Field semantics were checked against Moby commit
// 14ebc602124e280ef4ae3e1a8e688dac1255baa6 (daemon/stats_unix.go, cgroup v2
// conversion, and api/types/container/stats.go):
//
//   - throttling_data.periods, throttled_periods come from cpu.stat nr_periods
//     and nr_throttled unchanged.
//   - throttling_data.throttled_time is cpu.stat throttled_usec multiplied by
//     1000 and documented by the API type as nanoseconds.
//   - memory_stats.failcnt is set from memory.events "oom" only. It does not
//     carry oom_kill, max or a ".local" discriminator, and it has omitempty, so
//     an observed zero and a missing input both vanish from the JSON.
//
// This describes that source implementation only. It does not qualify the
// installed engine, and no native cgroup was read.

// containerGeneration is the frozen identity a sample must belong to.
type containerGeneration struct {
	ID        string
	StartedAt string
}

func (g containerGeneration) Valid() bool { return g.ID != "" && g.StartedAt != "" }

// accountingUnit names the unit a retained counter is expressed in.
type accountingUnit string

const (
	unitCount       accountingUnit = "count"
	unitNanoseconds accountingUnit = "nanoseconds"

	accountingSourceAPI = "docker-api-stats:moby-14ebc602"
)

type fieldState int

const (
	fieldAbsent fieldState = iota
	fieldNull
	fieldInvalid
	fieldValue
)

func (s fieldState) String() string {
	return [...]string{"absent", "null", "invalid", "value"}[s]
}

// counterField distinguishes a key that was not present from an observed zero.
type counterField struct {
	State fieldState
	Value uint64
	Unit  accountingUnit
}

func (f counterField) ok() bool { return f.State == fieldValue }

// accountingSample is one decoded stats projection of one container generation.
type accountingSample struct {
	Generation containerGeneration
	At         time.Time
	Source     string

	Periods          counterField
	ThrottledPeriods counterField
	ThrottledNanos   counterField

	// MemoryFailcnt is supplementary context only. See classifyOOM.
	MemoryFailcnt counterField
}

var (
	errAccountingGeneration   = errors.New("sample generation is not comparable")
	errAccountingInconclusive = errors.New("sample counters are missing, null or mistyped")
	errAccountingDecrease     = errors.New("counter decreased or time went backwards")
)

// accountingChecks disables individual rules. The zero value enforces all of
// them; negative controls set one field to prove the matching rule matters.
type accountingChecks struct {
	skipGeneration bool
	skipPresence   bool
	skipDecrease   bool
	// nanosScale replaces the 1 unit-per-field assumption when nonzero, to
	// emulate a reader that treats throttled_time as another unit.
	nanosScale uint64
}

// lookupCounter walks path through decoded JSON objects.
func lookupCounter(root map[string]json.RawMessage, unit accountingUnit, path ...string) counterField {
	cur := root
	for i, key := range path {
		raw, ok := cur[key]
		if !ok {
			return counterField{State: fieldAbsent, Unit: unit}
		}
		raw = bytes.TrimSpace(raw)
		if bytes.Equal(raw, []byte("null")) {
			return counterField{State: fieldNull, Unit: unit}
		}
		if i == len(path)-1 {
			// ParseUint rejects strings, floats, exponents and negatives.
			value, err := strconv.ParseUint(string(raw), 10, 64)
			if err != nil {
				return counterField{State: fieldInvalid, Unit: unit}
			}
			return counterField{State: fieldValue, Value: value, Unit: unit}
		}
		var next map[string]json.RawMessage
		if err := json.Unmarshal(raw, &next); err != nil || next == nil {
			return counterField{State: fieldInvalid, Unit: unit}
		}
		cur = next
	}
	return counterField{State: fieldInvalid, Unit: unit}
}

// parseAccountingSample is a pure function over one ContainerStats JSON body.
// A body that is not an object, has a different "id" than the frozen
// generation, or has no parseable "read" time is refused outright. Counter
// problems are retained per field and surface as inconclusive at delta time.
func parseAccountingSample(gen containerGeneration, body []byte) (accountingSample, error) {
	if !gen.Valid() {
		return accountingSample{}, fmt.Errorf("%w: frozen generation %+v is incomplete", errAccountingGeneration, gen)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return accountingSample{}, fmt.Errorf("%w: body is not an object", errAccountingInconclusive)
	}
	var id string
	if err := json.Unmarshal(root["id"], &id); err != nil || id != gen.ID {
		return accountingSample{}, fmt.Errorf("%w: stats id does not match the frozen container", errAccountingGeneration)
	}
	var readText string
	if err := json.Unmarshal(root["read"], &readText); err != nil {
		return accountingSample{}, fmt.Errorf("%w: read time missing or mistyped", errAccountingInconclusive)
	}
	at, err := time.Parse(time.RFC3339Nano, readText)
	if err != nil || at.Year() < 2000 {
		return accountingSample{}, fmt.Errorf("%w: read time %q is unusable", errAccountingInconclusive, readText)
	}
	return accountingSample{
		Generation:       gen,
		At:               at,
		Source:           accountingSourceAPI,
		Periods:          lookupCounter(root, unitCount, "cpu_stats", "throttling_data", "periods"),
		ThrottledPeriods: lookupCounter(root, unitCount, "cpu_stats", "throttling_data", "throttled_periods"),
		ThrottledNanos:   lookupCounter(root, unitNanoseconds, "cpu_stats", "throttling_data", "throttled_time"),
		MemoryFailcnt:    lookupCounter(root, unitCount, "memory_stats", "failcnt"),
	}, nil
}

// cpuThrottleDelta is the qualified difference between two samples of the same
// container generation. ThrottledNanos is in nanoseconds.
type cpuThrottleDelta struct {
	Periods, ThrottledPeriods, ThrottledNanos uint64
	Elapsed                                   time.Duration
	Source                                    string
}

// Throttled reports positive throttling evidence: both the count of throttled
// periods and the throttled time advanced.
func (d cpuThrottleDelta) Throttled() bool { return d.ThrottledPeriods > 0 && d.ThrottledNanos > 0 }

func computeCPUThrottleDelta(prev, cur accountingSample, checks accountingChecks) (cpuThrottleDelta, error) {
	if !checks.skipGeneration && (!prev.Generation.Valid() || prev.Generation != cur.Generation) {
		return cpuThrottleDelta{}, fmt.Errorf("%w: %+v vs %+v", errAccountingGeneration, prev.Generation, cur.Generation)
	}
	if prev.Source != cur.Source {
		return cpuThrottleDelta{}, fmt.Errorf("%w: sources differ", errAccountingGeneration)
	}
	pairs := [][2]counterField{
		{prev.Periods, cur.Periods},
		{prev.ThrottledPeriods, cur.ThrottledPeriods},
		{prev.ThrottledNanos, cur.ThrottledNanos},
	}
	if !checks.skipPresence {
		for _, pair := range pairs {
			for _, f := range pair {
				if !f.ok() {
					return cpuThrottleDelta{}, fmt.Errorf("%w: field is %s", errAccountingInconclusive, f.State)
				}
			}
		}
	}
	if !checks.skipDecrease {
		if cur.At.Before(prev.At) {
			return cpuThrottleDelta{}, fmt.Errorf("%w: sample time", errAccountingDecrease)
		}
		for _, pair := range pairs {
			if pair[1].Value < pair[0].Value {
				return cpuThrottleDelta{}, fmt.Errorf("%w: %d -> %d", errAccountingDecrease, pair[0].Value, pair[1].Value)
			}
		}
	}
	scale := uint64(1)
	if checks.nanosScale != 0 {
		scale = checks.nanosScale
	}
	return cpuThrottleDelta{
		Periods:          cur.Periods.Value - prev.Periods.Value,
		ThrottledPeriods: cur.ThrottledPeriods.Value - prev.ThrottledPeriods.Value,
		ThrottledNanos:   (cur.ThrottledNanos.Value - prev.ThrottledNanos.Value) * scale,
		Elapsed:          cur.At.Sub(prev.At),
		Source:           cur.Source,
	}, nil
}

// localMemoryEvidence is independently identified, target-local cgroup
// evidence (for example memory.events.local plus victim attribution). The
// stats API cannot produce it.
type localMemoryEvidence struct {
	Generation     containerGeneration
	Source         string
	Local          bool // read from memory.events.local, not the hierarchical file
	OomKill        *uint64
	VictimIsTarget bool // victim attributed to the frozen daemon process
}

type oomObservation string

const (
	oomUnsupported oomObservation = "unsupported"
	oomIncomplete  oomObservation = "incomplete"
	oomEvidenced   oomObservation = "evidenced"
)

// classifyOOM never derives an OOM verdict from the API failcnt. Without
// independent local evidence the observation is unsupported, whatever failcnt
// holds, including zero or absent.
func classifyOOM(sample accountingSample, evidence *localMemoryEvidence) oomObservation {
	_ = sample.MemoryFailcnt // supplementary context only, deliberately unused
	if evidence == nil {
		return oomUnsupported
	}
	if !sample.Generation.Valid() || evidence.Generation != sample.Generation ||
		!evidence.Local || evidence.OomKill == nil || !evidence.VictimIsTarget || evidence.Source == "" {
		return oomIncomplete
	}
	return oomEvidenced
}

// sampleContainerAccounting is the live reader. It mirrors the ContainerStats
// access path of the memory sampler without modifying it, and freezes the
// generation from inspection taken before and after the stats call; a restart
// in between makes the sample unusable. Docker is required, so only the pure
// functions above are exercised in the Docker-free batch.
func sampleContainerAccounting(parent context.Context, docker *mobycl.Client, cid string) (accountingSample, error) {
	ctx, done := context.WithTimeout(parent, 5*time.Second)
	defer done()
	inspect := func() (containerGeneration, error) {
		result, err := docker.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
		if err != nil {
			return containerGeneration{}, err
		}
		if result.Container.State == nil {
			return containerGeneration{}, fmt.Errorf("%w: no state", errAccountingGeneration)
		}
		return containerGeneration{ID: result.Container.ID, StartedAt: result.Container.State.StartedAt}, nil
	}
	before, err := inspect()
	if err != nil {
		return accountingSample{}, err
	}
	stats, err := docker.ContainerStats(ctx, cid, mobycl.ContainerStatsOptions{})
	if err != nil {
		return accountingSample{}, err
	}
	var body json.RawMessage
	err = json.NewDecoder(stats.Body).Decode(&body)
	_ = stats.Body.Close()
	if err != nil {
		return accountingSample{}, err
	}
	after, err := inspect()
	if err != nil {
		return accountingSample{}, err
	}
	if before != after {
		return accountingSample{}, fmt.Errorf("%w: %+v became %+v", errAccountingGeneration, before, after)
	}
	return parseAccountingSample(before, body)
}
