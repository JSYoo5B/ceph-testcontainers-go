//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The majority-loss observer decides whether a cluster of MONs has a quorum
// from what every MON says about itself on its local admin socket, which is
// served without Messenger, Cephx or a quorum. A client call that times out,
// a management query that fails or a data request that stalls does not decide
// it. A sample counts only when the response is attributed to the MON that
// was asked, its container is the one captured before the first interruption,
// and its monmap is the baseline's. Every other sample stays inconclusive
// evidence and never counts toward an outage or a quorum.

// Verdicts of one attributed local mon_status sample.
const (
	// majorityOutage: the MON is probing or electing and lists no quorum.
	majorityOutage = "outage"
	// majorityService: the MON reports itself as leader or peon, or lists a quorum.
	majorityService = "service"
	// majorityInconclusive: the sample cannot support either conclusion.
	majorityInconclusive = "inconclusive"
)

// majorityUptimeSlackMS is the tolerance for rounding and clock granularity
// when a MON's reported uptime is compared with the wall-clock time between
// two samples of it. It does not cover transport delay: the instant at which
// the MON sampled its uptime is only known to lie within the query's start and
// end, and the comparison uses that interval instead of a wider slack.
const majorityUptimeSlackMS = 1500

// majorityUptimeUncertaintyMS is the largest total query duration (baseline
// plus current) for which an uptime comparison can tell a continuing process
// from a restarted one. A slower pair of queries leaves the comparison
// undecided and the observation is taken again.
const majorityUptimeUncertaintyMS = 6000

// majorityMember is one MON as captured before any interruption.
type majorityMember struct {
	Name        string `json:"name"`
	ContainerID string `json:"container_id"`
	StartedAt   string `json:"started_at"`
	Address     string `json:"address"`
	UptimeMS    int64  `json:"uptime_ms"`
	// UptimeStart and UptimeEnd bracket the query whose response reported
	// UptimeMS; the MON sampled it at an unknown instant in between.
	UptimeStart time.Time `json:"uptime_query_started"`
	UptimeEnd   time.Time `json:"uptime_query_ended"`
}

// majorityBaseline is the healthy cluster's identity: its FSID, the monmap and
// the election epoch of the formed quorum, and each owned MON's container
// generation, public address and reported uptime.
type majorityBaseline struct {
	At            time.Time                 `json:"at"`
	FSID          string                    `json:"fsid"`
	Members       []string                  `json:"members"`
	MonMapEpoch   int                       `json:"monmap_epoch"`
	ElectionEpoch int                       `json:"election_epoch"`
	Mons          map[string]majorityMember `json:"mons"`
}

// majoritySample is one MON's classified local mon_status.
type majoritySample struct {
	Name          string    `json:"name"`
	Verdict       string    `json:"verdict"`
	Reason        string    `json:"reason,omitempty"`
	State         string    `json:"state,omitempty"`
	ElectionEpoch int       `json:"election_epoch,omitempty"`
	MonMapEpoch   int       `json:"monmap_epoch,omitempty"`
	Quorum        []string  `json:"quorum,omitempty"`
	UptimeMS      int64     `json:"uptime_ms"`
	QueryStarted  time.Time `json:"query_started"`
	QueryEnded    time.Time `json:"query_ended"`
	Digest        string    `json:"digest,omitempty"`
}

// majorityRound is one concurrent pass over the baseline's MONs.
type majorityRound struct {
	At         time.Time        `json:"at"`
	Samples    []majoritySample `json:"samples"`
	Incomplete []string         `json:"incomplete,omitempty"`
}

// summary is the round as one log line: state, election epoch, listed quorum
// and verdict per MON.
func (r majorityRound) summary() string {
	parts := make([]string, 0, len(r.Samples))
	for _, s := range r.Samples {
		parts = append(parts, fmt.Sprintf("%s=%s/e%d/q%v/%s", s.Name, s.State, s.ElectionEpoch, s.Quorum, s.Verdict))
	}
	return strings.Join(parts, " ")
}

// majorityWait bounds a wait for a state: it must hold in MinRounds
// consecutive rounds that span at least Window, within Bound overall.
type majorityWait struct {
	Window, Interval, Bound time.Duration
	MinRounds               int
}

// parseMonStatusUptime reads the uptime in milliseconds that a mon_status
// response reports, from a payload that was retained whole.
func parseMonStatusUptime(local monLocalStatus) (int64, bool) {
	if local.Payload == nil || local.Payload.Truncated || local.Payload.Data == "" {
		return 0, false
	}
	var reported struct {
		Uptime *float64 `json:"uptime"`
	}
	if err := json.Unmarshal([]byte(local.Payload.Data), &reported); err != nil || reported.Uptime == nil || *reported.Uptime < 0 {
		return 0, false
	}
	return int64(*reported.Uptime), true
}

// monStatusQuorumShape says why a retained mon_status response does not report
// its quorum as an explicit JSON array, or returns "" when it does. The shared
// parser decodes a missing field and null into the same empty slice as [], so
// an outage may only be concluded from a response that this check accepts.
func monStatusQuorumShape(local monLocalStatus) string {
	if local.Payload == nil || local.Payload.Truncated || local.Payload.Data == "" {
		return "the response was not retained whole"
	}
	var reported struct {
		Quorum json.RawMessage `json:"quorum"`
	}
	if err := json.Unmarshal([]byte(local.Payload.Data), &reported); err != nil {
		return "the response is not decodable: " + err.Error()
	}
	raw := bytes.TrimSpace(reported.Quorum)
	switch {
	case len(raw) == 0:
		return "the response omits quorum"
	case string(raw) == "null":
		return "the response reports a null quorum"
	case raw[0] != '[':
		return "the response reports a quorum that is not an array"
	}
	return ""
}

func sortedClone(names []string) []string {
	result := slices.Clone(names)
	sort.Strings(result)
	return result
}

// captureMajorityBaseline records the healthy cluster before any interruption:
// the control quorum query supplies the FSID, the monmap and each MON's public
// address, and every owned MON must then report itself, attributed and in a
// stable container generation, as part of one full quorum.
func captureMajorityBaseline(ctx context.Context, rec *monReplacementRecorder, cluster monReplacementCluster, count int) (*majorityBaseline, error) {
	queryCtx, cancel := context.WithTimeout(ctx, rec.bounds.Query)
	status, err := cluster.QuorumStatus(queryCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("baseline quorum: %w", err)
	}
	b := &majorityBaseline{At: time.Now(), FSID: status.MonMap.FSID, Mons: make(map[string]majorityMember)}
	if b.FSID == "" {
		return nil, errors.New("baseline quorum reports no FSID")
	}
	addresses := make(map[string]string)
	for _, mon := range status.MonMap.Mons {
		b.Members = append(b.Members, mon.Name)
		var addrs []string
		for _, addr := range mon.PublicAddrs.Addrvec {
			addrs = append(addrs, addr.Type+":"+addr.Addr)
		}
		sort.Strings(addrs)
		addresses[mon.Name] = strings.Join(addrs, ",")
	}
	sort.Strings(b.Members)
	if len(b.Members) != count || !slices.Equal(b.Members, sortedClone(status.QuorumNames)) {
		return nil, fmt.Errorf("baseline is not a full quorum of %d: monmap %v, quorum %v", count, b.Members, status.QuorumNames)
	}
	listCtx, cancelList := context.WithTimeout(ctx, rec.bounds.Query)
	monitors, err := cluster.Monitors(listCtx)
	cancelList()
	if err != nil {
		return nil, fmt.Errorf("list monitors: %w", err)
	}
	if len(monitors) != count {
		return nil, fmt.Errorf("the cluster owns %d monitors, want %d", len(monitors), count)
	}
	for _, mon := range monitors {
		if !slices.Contains(b.Members, mon.DaemonName) {
			return nil, fmt.Errorf("owned monitor %s is not in the monmap %v", mon.DaemonName, b.Members)
		}
		local, problems := rec.monStatus(ctx, mon, b.FSID, "quorum")
		switch {
		case len(problems) > 0:
			return nil, fmt.Errorf("baseline mon.%s: %s", mon.DaemonName, strings.Join(problems, "; "))
		case local.Identity == nil || local.Identity.Attribution != monStatusMatched || local.Identity.Generation != "unchanged" || local.Identity.ContainerID == "":
			return nil, fmt.Errorf("baseline mon.%s is not attributed to a stable container: %+v", mon.DaemonName, local.Identity)
		case local.State != "leader" && local.State != "peon":
			return nil, fmt.Errorf("baseline mon.%s is %q, not part of a formed quorum", mon.DaemonName, local.State)
		case !slices.Equal(sortedClone(local.Quorum), b.Members) || !slices.Equal(sortedClone(local.Members), b.Members):
			return nil, fmt.Errorf("baseline mon.%s lists quorum %v of monmap %v, want %v", mon.DaemonName, local.Quorum, local.Members, b.Members)
		}
		uptime, ok := parseMonStatusUptime(local)
		if !ok {
			return nil, fmt.Errorf("baseline mon.%s reports no complete uptime", mon.DaemonName)
		}
		if b.Mons[mon.DaemonName].Name != "" {
			return nil, fmt.Errorf("monitor %s is listed twice", mon.DaemonName)
		}
		if len(b.Mons) > 0 && (local.MonMapEpoch != b.MonMapEpoch || local.ElectionEpoch != b.ElectionEpoch) {
			return nil, fmt.Errorf("baseline monitors disagree: mon.%s has monmap epoch %d and election epoch %d, others %d and %d",
				mon.DaemonName, local.MonMapEpoch, local.ElectionEpoch, b.MonMapEpoch, b.ElectionEpoch)
		}
		b.MonMapEpoch, b.ElectionEpoch = local.MonMapEpoch, local.ElectionEpoch
		b.Mons[mon.DaemonName] = majorityMember{Name: mon.DaemonName, ContainerID: local.Identity.ContainerID, StartedAt: local.Identity.StartedAfter,
			Address: addresses[mon.DaemonName], UptimeMS: uptime, UptimeStart: local.Payload.QueryStarted, UptimeEnd: local.Payload.QueryEnded}
	}
	return b, nil
}

// classifyMajoritySample attributes one MON's local mon_status to a verdict.
// problems are the recorder's incomplete entries for this MON. Only a sample
// that is attributed, from the baseline's container generation and monmap, is
// either an outage or a service observation.
func classifyMajoritySample(b *majorityBaseline, local monLocalStatus, problems []string) majoritySample {
	sample := majoritySample{Name: local.Name, Verdict: majorityInconclusive, UptimeMS: -1,
		State: local.State, ElectionEpoch: local.ElectionEpoch, MonMapEpoch: local.MonMapEpoch, Quorum: local.Quorum}
	if local.Payload != nil {
		sample.QueryStarted, sample.QueryEnded, sample.Digest = local.Payload.QueryStarted, local.Payload.QueryEnded, local.Payload.Digest
		if uptime, ok := parseMonStatusUptime(local); ok {
			sample.UptimeMS = uptime
		}
	}
	inconclusive := func(format string, args ...any) majoritySample {
		sample.Reason = fmt.Sprintf(format, args...)
		if len(problems) > 0 {
			sample.Reason += " (" + strings.Join(problems, "; ") + ")"
		}
		return sample
	}
	base, known := b.Mons[local.Name]
	identity := local.Identity
	switch {
	case !known:
		return inconclusive("mon.%s is not a baseline member", local.Name)
	case local.Error != "":
		return inconclusive("no usable response: %s", local.Error)
	case identity == nil || identity.Attribution != monStatusMatched:
		return inconclusive("response not attributed to mon.%s", local.Name)
	case identity.Generation != "unchanged":
		return inconclusive("container generation %s", identity.Generation)
	case identity.ContainerID != base.ContainerID || identity.StartedBefore != base.StartedAt || identity.StartedAfter != base.StartedAt:
		return inconclusive("container %s started %s..%s, baseline %s started %s", identity.ContainerID, identity.StartedBefore, identity.StartedAfter, base.ContainerID, base.StartedAt)
	case local.MonMapEpoch != b.MonMapEpoch || !slices.Equal(sortedClone(local.Members), b.Members):
		return inconclusive("monmap epoch %d members %v differs from the baseline epoch %d members %v", local.MonMapEpoch, local.Members, b.MonMapEpoch, b.Members)
	case monStatusQuorumShape(local) != "":
		return inconclusive("%s", monStatusQuorumShape(local))
	case local.State == "leader" || local.State == "peon" || len(local.Quorum) > 0:
		sample.Verdict = majorityService
	case local.State == "probing" || local.State == "electing":
		sample.Verdict = majorityOutage
	default:
		return inconclusive("state %q is neither service nor a quorum search", local.State)
	}
	return sample
}

// observeMajorityRound queries every baseline MON once, concurrently, with the
// recorder's per-query and snapshot bounds, and classifies the responses.
func observeMajorityRound(parent context.Context, rec *monReplacementRecorder, cluster monReplacementCluster, b *majorityBaseline) majorityRound {
	ctx, cancel := context.WithTimeout(parent, rec.bounds.Snapshot)
	defer cancel()
	round := majorityRound{At: time.Now(), Samples: make([]majoritySample, len(b.Members))}
	listCtx, cancelList := context.WithTimeout(ctx, rec.bounds.Query)
	monitors, err := cluster.Monitors(listCtx)
	cancelList()
	if err != nil {
		round.Incomplete = append(round.Incomplete, fmt.Sprintf("list monitors: %v", err))
	}
	var wg sync.WaitGroup
	for i, name := range b.Members {
		index := slices.IndexFunc(monitors, func(mon *ceph.MonitorContainer) bool { return mon != nil && mon.DaemonName == name })
		if index < 0 {
			round.Samples[i] = majoritySample{Name: name, Verdict: majorityInconclusive, Reason: "monitor not listed", UptimeMS: -1}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			local, problems := rec.monStatus(ctx, monitors[index], b.FSID, "baseline quorum")
			round.Samples[i] = classifyMajoritySample(b, local, problems)
		}()
	}
	wg.Wait()
	return round
}

// evaluateMajorityRound returns why a round does not show the wanted state,
// or nothing when it does. An empty want is a lost majority: every baseline
// MON is attributed and in an outage. Otherwise exactly want form the quorum:
// each of them leads or follows and lists want as its quorum under one
// election epoch above the baseline's, with a single leader, while every other
// MON is in an outage.
func evaluateMajorityRound(b *majorityBaseline, round majorityRound, want []string) []string {
	var reasons []string
	want = sortedClone(want)
	epochs := make(map[int]bool)
	leaders := 0
	for _, s := range round.Samples {
		inQuorum := slices.Contains(want, s.Name)
		switch {
		case s.Verdict == majorityInconclusive:
			reasons = append(reasons, fmt.Sprintf("mon.%s inconclusive: %s", s.Name, s.Reason))
		case !inQuorum && s.Verdict != majorityOutage:
			reasons = append(reasons, fmt.Sprintf("mon.%s is %q with quorum %v, want an outage", s.Name, s.State, s.Quorum))
		case inQuorum && (s.State != "leader" && s.State != "peon" || !slices.Equal(sortedClone(s.Quorum), want)):
			reasons = append(reasons, fmt.Sprintf("mon.%s is %q with quorum %v, want a member of %v", s.Name, s.State, s.Quorum, want))
		case inQuorum:
			epochs[s.ElectionEpoch] = true
			if s.State == "leader" {
				leaders++
			}
			if s.ElectionEpoch <= b.ElectionEpoch {
				reasons = append(reasons, fmt.Sprintf("mon.%s election epoch %d did not advance past the baseline %d", s.Name, s.ElectionEpoch, b.ElectionEpoch))
			}
		}
	}
	if len(want) > 0 && len(reasons) == 0 && (leaders != 1 || len(epochs) != 1) {
		reasons = append(reasons, fmt.Sprintf("quorum %v has %d leaders over election epochs %v", want, leaders, epochs))
	}
	if len(round.Samples) != len(b.Members) {
		reasons = append(reasons, fmt.Sprintf("round has %d samples for %d members", len(round.Samples), len(b.Members)))
	}
	return reasons
}

// waitMajorityState observes rounds until the wanted state holds in MinRounds
// consecutive rounds that span Window, or Bound ends. A round that does not
// show it, including one with an inconclusive sample, restarts the count. It
// returns every round it took; the error says why the last one did not match.
func waitMajorityState(parent context.Context, rec *monReplacementRecorder, cluster monReplacementCluster, b *majorityBaseline, want []string, w majorityWait, logf func(string, ...any)) ([]majorityRound, error) {
	ctx, cancel := context.WithTimeout(parent, w.Bound)
	defer cancel()
	var rounds []majorityRound
	var streak int
	var first time.Time
	for {
		round := observeMajorityRound(ctx, rec, cluster, b)
		rounds = append(rounds, round)
		reasons := evaluateMajorityRound(b, round, want)
		if logf != nil {
			logf("round %d at %s: %s", len(rounds), round.At.UTC().Format("15:04:05.000"), round.summary())
		}
		if len(reasons) == 0 {
			if streak == 0 {
				first = round.At
			}
			streak++
			if streak >= w.MinRounds && round.At.Sub(first) >= w.Window {
				return rounds, nil
			}
		} else {
			streak = 0
		}
		select {
		case <-ctx.Done():
			return rounds, fmt.Errorf("no stable window of %d rounds over %v for quorum %v within %v (%d rounds); last round: %s",
				w.MinRounds, w.Window, want, w.Bound, len(rounds), strings.Join(reasons, "; "))
		case <-time.After(w.Interval):
		}
	}
}

// checkMajorityContinuity compares each MON's reported uptime with the time
// between its baseline and this round. A restarted ceph-mon reports an uptime
// that restarts, which the container's unchanged start time alone would not
// show. The MON sampled its uptime at an unknown instant inside each query, so
// the elapsed time between the two samples is only known to lie between the
// current query's start minus the baseline query's end and the current query's
// end minus the baseline query's start; a process continued if the uptime
// advanced by an amount in that interval (plus the rounding slack). problems
// are samples that contradict a continuing process or lack an uptime;
// undecided are samples whose queries were too slow to tell, which the caller
// takes again.
func checkMajorityContinuity(b *majorityBaseline, round majorityRound) (problems, undecided []string) {
	for _, s := range round.Samples {
		base := b.Mons[s.Name]
		if s.UptimeMS < 0 || s.QueryStarted.IsZero() || s.QueryEnded.IsZero() || base.UptimeStart.IsZero() || base.UptimeEnd.IsZero() {
			problems = append(problems, fmt.Sprintf("mon.%s reports no complete uptime", s.Name))
			continue
		}
		lowest := s.QueryStarted.Sub(base.UptimeEnd).Milliseconds()
		highest := s.QueryEnded.Sub(base.UptimeStart).Milliseconds()
		advanced := s.UptimeMS - base.UptimeMS
		uncertainty := s.QueryEnded.Sub(s.QueryStarted).Milliseconds() + base.UptimeEnd.Sub(base.UptimeStart).Milliseconds()
		switch {
		case advanced < lowest-majorityUptimeSlackMS || advanced > highest+majorityUptimeSlackMS:
			problems = append(problems, fmt.Sprintf("mon.%s uptime advanced %d ms but %d to %d ms of wall clock passed between its samples", s.Name, advanced, lowest, highest))
		case uncertainty > majorityUptimeUncertaintyMS:
			undecided = append(undecided, fmt.Sprintf("mon.%s queries took %d ms in total, too long to tell a restart from a continuing process", s.Name, uncertainty))
		}
	}
	return problems, undecided
}

// checkMajorityAddresses compares the control quorum query's public MON
// addresses with the baseline's.
func checkMajorityAddresses(b *majorityBaseline, status ceph.QuorumStatus) []string {
	var problems []string
	seen := make(map[string]bool)
	for _, mon := range status.MonMap.Mons {
		var addrs []string
		for _, addr := range mon.PublicAddrs.Addrvec {
			addrs = append(addrs, addr.Type+":"+addr.Addr)
		}
		sort.Strings(addrs)
		seen[mon.Name] = true
		if base, ok := b.Mons[mon.Name]; !ok {
			problems = append(problems, fmt.Sprintf("mon.%s joined the monmap", mon.Name))
		} else if got := strings.Join(addrs, ","); got != base.Address {
			problems = append(problems, fmt.Sprintf("mon.%s address %s, baseline %s", mon.Name, got, base.Address))
		}
	}
	for name := range b.Mons {
		if !seen[name] {
			problems = append(problems, fmt.Sprintf("mon.%s left the monmap", name))
		}
	}
	return problems
}

// majorityManagement is what one quorum-dependent control query did while the
// cluster was in an outage. It is evidence only: neither a failure nor a
// success of the query decides whether a majority exists.
type majorityManagement struct {
	Started  time.Time     `json:"started"`
	Duration time.Duration `json:"duration"`
	Quorum   []string      `json:"quorum,omitempty"`
	Error    string        `json:"error,omitempty"`
}

func probeMajorityManagement(parent context.Context, cluster monReplacementCluster, bound time.Duration) majorityManagement {
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	probe := majorityManagement{Started: time.Now()}
	status, err := cluster.QuorumStatus(ctx)
	probe.Duration = time.Since(probe.Started)
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	probe.Quorum = slices.Clone(status.QuorumNames)
	return probe
}
