package netem

import (
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"strings"
)

// Preflight is what the preflight checks beyond netem itself: netem's rate,
// the requested packet faults (netem keywords), and with peers the prio
// qdisc and u32 classifier, with their filter read as Match says.
type Preflight struct {
	Rate   bool
	Faults []string
	Peers  bool
	Match  Match
}

// Args are Script's arguments for the preflight.
func (p Preflight) Args() []string {
	args := []string{"preflight"}
	if p.Rate {
		args = append(args, "rate")
	}
	args = append(args, p.Faults...)
	if p.Peers {
		args = append(args, "peers")
		if p.Match.Source {
			args = append(args, "sport")
		}
		if p.Match.Setup {
			args = append(args, "setup")
		}
	}
	return args
}

// Check judges the preflight's report: the contract's executables, NET_ADMIN,
// a netem qdisc that tc adds, shows and removes on the helper's loopback,
// and what Rate, Faults and Peers ask for. It returns ErrImageUnsupported,
// ErrPermissionDenied or ErrKernelUnsupported, or a native failure of tc.
func (p Preflight) Check(report Report) error {
	// The helper's script wrote this report; anything that it leaves out or
	// gets wrong is a helper that does not meet the contract.
	switch {
	case report.Missing == nil:
		return fmt.Errorf("%w: the preflight report lists no missing executables", ErrImageUnsupported)
	case len(report.Missing) > 0:
		return fmt.Errorf("%w: missing %s", ErrImageUnsupported, strings.Join(report.Missing, ", "))
	case report.Broken == nil:
		return fmt.Errorf("%w: the preflight report lists no broken commands", ErrImageUnsupported)
	case len(report.Broken) > 0:
		return fmt.Errorf("%w: failed %s", ErrImageUnsupported, strings.Join(report.Broken, ", "))
	case !report.NetAdmin:
		return ErrPermissionDenied
	}
	code, text, err := result(report.Add, "add")
	if err != nil {
		return err
	}
	if code != 0 {
		return Classify("add netem on the helper's loopback", code, text)
	}
	var shown []Qdisc
	if err := json.Unmarshal([]byte(report.Shown), &shown); err != nil || len(shown) == 0 || shown[0].Kind != "netem" {
		return fmt.Errorf("%w: tc did not show the netem qdisc it added", ErrImageUnsupported)
	}
	if code, text, err = result(report.Delete, "delete"); err != nil {
		return err
	}
	if code != 0 {
		return Classify("remove netem from the helper's loopback", code, text)
	}
	if p.Rate {
		if err := checkRate(report); err != nil {
			return err
		}
	}
	if len(p.Faults) > 0 {
		if err := checkFaults(report, p.Faults); err != nil {
			return err
		}
	}
	if p.Peers {
		if err := checkPeers(report, p.Match); err != nil {
			return err
		}
	}
	return nil
}

// checkRate checks the preflight's netem rate on the helper's loopback: tc
// must parse netem's rate and limit, and the kernel must keep both.
func checkRate(report Report) error {
	code, text, err := result(report.Rate, "rate")
	if err != nil {
		return err
	}
	if code != 0 {
		return Classify("add a netem rate on the helper's loopback", code, text)
	}
	var shown []Qdisc
	if err := json.Unmarshal([]byte(report.RateShown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the netem rate it added", ErrImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(qdisc Qdisc) bool { return qdisc.Kind == "netem" && qdisc.Handle == "c7e3:" })
	switch {
	case i < 0:
		return fmt.Errorf("%w: tc did not show the netem rate it added", ErrImageUnsupported)
	case shown[i].Options.Rate.Rate != 125000 || shown[i].Options.Limit != 100:
		return fmt.Errorf("%w: netem kept a rate of %d bytes/s and a limit of %d instead of 125000 and 100", ErrKernelUnsupported,
			shown[i].Options.Rate.Rate, shown[i].Options.Limit)
	}
	if code, text, err = result(report.RateDelete, "rate_delete"); err != nil {
		return err
	}
	if code != 0 {
		return Classify("remove the netem rate from the helper's loopback", code, text)
	}
	return nil
}

// PreflightFault is the packet fault that the preflight adds for each netem
// keyword, with a correlation, as Script's arguments say.
var PreflightFault = map[string]Fault{
	"reorder":   {Percent: 25, Correlation: 50},
	"duplicate": {Percent: 3, Correlation: 10},
	"corrupt":   {Percent: 2, Correlation: 5},
}

// checkFaults checks the preflight's netem packet faults on the helper's
// loopback: tc must parse each requested fault with its correlation, and the
// kernel must keep each, and for a reorder the gap.
func checkFaults(report Report, faults []string) error {
	code, text, err := result(report.Faults, "faults")
	if err != nil {
		return err
	}
	if code != 0 {
		return Classify("add netem's "+strings.Join(faults, ", ")+" on the helper's loopback", code, text)
	}
	var shown []Qdisc
	if err := json.Unmarshal([]byte(report.FaultsShown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the netem packet faults it added", ErrImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(qdisc Qdisc) bool { return qdisc.Kind == "netem" && qdisc.Handle == "c7e4:" })
	if i < 0 {
		return fmt.Errorf("%w: tc did not show the netem packet faults it added", ErrImageUnsupported)
	}
	options := shown[i].Options
	kernel := map[string]FaultMember{"reorder": options.Reorder, "duplicate": options.Duplicate, "corrupt": options.Corrupt}
	for _, name := range faults {
		if got := kernel[name].Fault(name); !PreflightFault[name].Matches(got) {
			return fmt.Errorf("%w: netem kept %s %v instead of %v", ErrKernelUnsupported, name, got, PreflightFault[name])
		}
	}
	if slices.Contains(faults, "reorder") && options.Gap != 1 {
		return fmt.Errorf("%w: netem kept a reorder gap of %d instead of 1", ErrKernelUnsupported, options.Gap)
	}
	if code, text, err = result(report.FaultsDelete, "faults_delete"); err != nil {
		return err
	}
	if code != 0 {
		return Classify("remove the netem packet faults from the helper's loopback", code, text)
	}
	return nil
}

// checkPeers checks the preflight's prio root and u32 filter on the helper's
// loopback: what a profile with peers adds above netem.
func checkPeers(report Report, match Match) error {
	code, text, err := result(report.Prio, "prio")
	if err != nil {
		return err
	}
	if code != 0 {
		return Classify("add a prio qdisc on the helper's loopback", code, text)
	}
	if code, text, err = result(report.U32, "u32"); err != nil {
		return err
	}
	if code != 0 {
		return Classify("add a u32 filter on the helper's loopback", code, text)
	}
	var shown []Filter
	if err := json.Unmarshal([]byte(report.U32Shown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the u32 filter it added", ErrImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(filter Filter) bool { return filter.Kind == "u32" && filter.Options.FlowID == "c7e2:3" })
	if i < 0 {
		return fmt.Errorf("%w: tc did not show the u32 filter it added", ErrImageUnsupported)
	}
	if match.Source || match.Setup {
		// The source port and SYN keys must reach the kernel as the lease
		// will read them back.
		got, err := shown[i].Keys()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrImageUnsupported, err)
		}
		if want := FilterKeys(netip.MustParseAddr("127.0.0.2"), 7000, match); !slices.Equal(got, want) {
			return fmt.Errorf("%w: the u32 filter kept the keys %v instead of %v", ErrKernelUnsupported, got, want)
		}
	}
	if code, text, err = result(report.PrioDelete, "prio_delete"); err != nil {
		return err
	}
	if code != 0 {
		return Classify("remove the prio qdisc from the helper's loopback", code, text)
	}
	return nil
}

// result reads one [exit code, stderr] pair of the preflight report.
func result(value []any, name string) (int, string, error) {
	malformed := fmt.Errorf("%w: the preflight report has no valid %s result", ErrImageUnsupported, name)
	if len(value) != 2 {
		return 0, "", malformed
	}
	code, isNumber := value[0].(float64)
	text, isText := value[1].(string)
	if !isNumber || !isText || code != math.Trunc(code) || code < 0 || code > 255 {
		return 0, "", malformed
	}
	return int(code), text, nil
}
