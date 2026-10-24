//go:build all || (integration && topology)

package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// The two-domain outage crosses the quorum boundary of the correlated-outage
// scenario: two of three logical hosts lose their MON, OSD and MGR, so one MON
// remains and cannot form a majority, and one OSD of three remains, below the
// pool's min_size of two. The helpers below only decide from what the existing
// ledger, the frozen manifest and the attributed local MON status already
// provide.

// twoDomainPlan is the stop order for a set of lost logical hosts, and the
// MONs that stay outside it. Restore runs the stop order in reverse.
type twoDomainPlan struct {
	// Members are the members of the lost hosts in stop order: every OSD and
	// MGR (hosts in the given order, OSD before MGR) and then the MONs, the
	// primary MON last.
	Members []outageMember
	// LostMONs and SurvivingMONs are daemon names.
	LostMONs, SurvivingMONs []string
}

// newTwoDomainPlan selects the members of the lost hosts from a frozen
// manifest. The primary MON is pinned to its bridge address and Docker hands a
// released address to whichever container starts first, so it must be stopped
// last, which makes it start first when the plan is restored. The selection is
// refused unless it loses a strict majority of the MONs and leaves at least one
// MON outside it, because a plan that keeps a quorum is the one-host scenario
// and a plan that removes every MON leaves nothing to observe.
func newTwoDomainPlan(manifest outageManifest, lost []string, primaryMON string) (twoDomainPlan, error) {
	seen := map[string]bool{}
	for _, host := range lost {
		switch {
		case !slices.Contains(manifest.Hosts, host):
			return twoDomainPlan{}, fmt.Errorf("lost host %q is not in the manifest", host)
		case seen[host]:
			return twoDomainPlan{}, fmt.Errorf("lost host %q is listed twice", host)
		}
		seen[host] = true
	}
	var plan twoDomainPlan
	var mons []outageMember
	totalMONs := 0
	for _, m := range manifest.Members {
		if m.Role == "mon" {
			totalMONs++
			if !seen[m.Host] {
				plan.SurvivingMONs = append(plan.SurvivingMONs, m.Name)
			}
		}
	}
	for _, host := range lost {
		members := manifest.onHost(host)
		for _, role := range []string{"osd", "mgr"} {
			for _, m := range members {
				if m.Role == role {
					plan.Members = append(plan.Members, m)
				}
			}
		}
		for _, m := range members {
			if m.Role == "mon" {
				mons = append(mons, m)
				plan.LostMONs = append(plan.LostMONs, m.Name)
			}
		}
	}
	switch {
	case len(lost) < 2:
		return twoDomainPlan{}, fmt.Errorf("%d lost hosts do not cross the quorum boundary", len(lost))
	case (totalMONs-len(plan.LostMONs))*2 > totalMONs:
		return twoDomainPlan{}, fmt.Errorf("losing %d of %d MONs keeps a quorum", len(plan.LostMONs), totalMONs)
	case len(plan.SurvivingMONs) == 0:
		return twoDomainPlan{}, fmt.Errorf("no MON stays outside the lost hosts to observe")
	case !slices.Contains(plan.LostMONs, primaryMON):
		return twoDomainPlan{}, fmt.Errorf("the primary MON %q is not in the lost hosts", primaryMON)
	}
	// Partition rather than compare: every other MON, then the primary.
	for _, m := range mons {
		if m.Name != primaryMON {
			plan.Members = append(plan.Members, m)
		}
	}
	for _, m := range mons {
		if m.Name == primaryMON {
			plan.Members = append(plan.Members, m)
		}
	}
	return plan, nil
}

// evaluateSurvivorRound says why a round does not show the lost majority, or
// returns nothing when it does. Every surviving MON must be attributed to its
// own unchanged container and report that it searches for a quorum. A stopped
// MON cannot answer, so its sample is inconclusive, which is expected and
// carries no evidence; it must only never claim to be in service. A surviving
// MON that is inconclusive, in service, or missing from the round never shows
// the outage, and an unexpected name is refused.
func evaluateSurvivorRound(round majorityRound, survivors, stopped []string) []string {
	var reasons []string
	seen := map[string]bool{}
	for _, s := range round.Samples {
		seen[s.Name] = true
		switch {
		case slices.Contains(survivors, s.Name):
			if s.Verdict != majorityOutage {
				reasons = append(reasons, fmt.Sprintf("surviving mon.%s is %q (%s), want an attributed quorum search", s.Name, s.Verdict, s.Reason))
			}
		case slices.Contains(stopped, s.Name):
			if s.Verdict == majorityService {
				reasons = append(reasons, fmt.Sprintf("stopped mon.%s reports service", s.Name))
			}
		default:
			reasons = append(reasons, fmt.Sprintf("unexpected sample for mon.%s", s.Name))
		}
	}
	for _, name := range slices.Concat(survivors, stopped) {
		if !seen[name] {
			reasons = append(reasons, fmt.Sprintf("round has no sample for mon.%s", name))
		}
	}
	return reasons
}

// waitStableRounds takes rounds until evaluate reports no reason in MinRounds
// consecutive rounds that span Window, or Bound ends. A round with a reason
// restarts the count.
func waitStableRounds(w majorityWait, observe func() majorityRound, evaluate func(majorityRound) []string) ([]majorityRound, error) {
	deadline := time.Now().Add(w.Bound)
	var rounds []majorityRound
	var streak int
	var first time.Time
	for {
		round := observe()
		rounds = append(rounds, round)
		reasons := evaluate(round)
		if len(reasons) == 0 {
			if streak == 0 {
				first = round.At
			}
			if streak++; streak >= w.MinRounds && round.At.Sub(first) >= w.Window {
				return rounds, nil
			}
		} else {
			streak = 0
		}
		if !time.Now().Add(w.Interval).Before(deadline) {
			return rounds, fmt.Errorf("no stable window of %d rounds over %v within %v (%d rounds); last round: %s",
				w.MinRounds, w.Window, w.Bound, len(rounds), strings.Join(reasons, "; "))
		}
		time.Sleep(w.Interval)
	}
}

// twoDomainOutcomes counts what the retained client's operations did, by the
// token under which they began. Only operations that began under the outage
// token, which the controller issues after every stop was observed, are
// attributed to the fully applied outage.
type twoDomainOutcomes struct {
	// Begun counts operations that began under the outage token. Acked,
	// Failed and Inflight split them into operations that returned without
	// an error before the restoring token was issued, raised, or had no
	// return record.
	Begun, Acked, Failed, Inflight int
	// LateAcked are operations that began under the outage token and returned
	// without an error only after the restoring token was issued. They were
	// pending when the restoration started, so the acknowledgement may be a
	// consequence of it; they are reported and never credited to the outage.
	LateAcked int
	// OverlapAcked are operations that began under the applying token and
	// returned an acknowledgement after the outage token was issued. They
	// overlapped the injection and are reported, never credited to the outage.
	OverlapAcked int
}

// twoDomainLedgerOutcomes tallies the ledger. An acknowledgement of an
// operation that began and returned under the outage token is an error: with
// two of the three replicas stopped no write can be acknowledged by the pool's
// min_size, so such a record would mean a stopped replica or a wrong
// classification. An operation that began under the outage token but returned
// only after the restoring token is not an error, because the cluster was
// being restored by then; it is counted as LateAcked. The counts are returned
// either way.
func twoDomainLedgerOutcomes(ops []outageOp) (twoDomainOutcomes, error) {
	var out twoDomainOutcomes
	var acked []string
	for _, op := range ops {
		switch {
		case op.Phase == outagePhaseOutage:
			out.Begun++
			switch {
			case op.Outcome() == "acked" && op.PhaseAfter == outagePhaseOutage:
				out.Acked++
				acked = append(acked, op.Op)
			case op.Outcome() == "acked":
				out.LateAcked++
			case op.Outcome() == "failed":
				out.Failed++
			default:
				out.Inflight++
			}
		case op.Phase == outagePhaseApplying && op.Outcome() == "acked" && op.PhaseAfter == outagePhaseOutage:
			out.OverlapAcked++
		}
	}
	if len(acked) > 0 {
		return out, fmt.Errorf("%d operation(s) that began and returned under the fully applied outage were acknowledged: %v", len(acked), acked)
	}
	return out, nil
}
