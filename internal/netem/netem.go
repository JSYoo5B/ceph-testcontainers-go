// Package netem is the pure tc and netem layer of the network profile fault
// of package fault: the helper's script, the decoding of what tc reports,
// the arguments and peer filters that a profile installs, the checks of the
// preflight and of an installed hierarchy, and the error kinds of the netem
// contract. It runs nothing: package fault starts the helpers, runs the
// commands and owns the lease, its admission and its registry.
package netem

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

var (
	// ErrImageUnsupported is fault.ErrNetemImageUnsupported.
	ErrImageUnsupported error = nshelper.Category("the netem helper image does not meet the netem contract", ceph.ErrImageCapability)
	// ErrPermissionDenied is fault.ErrNetemPermissionDenied.
	ErrPermissionDenied error = nshelper.Category("the netem helper has no NET_ADMIN", ceph.ErrEnvironmentUnavailable)
	// ErrKernelUnsupported is fault.ErrNetemKernelUnsupported.
	ErrKernelUnsupported error = nshelper.Category("the engine's kernel lacks a qdisc or classifier that the profile needs", ceph.ErrEnvironmentUnavailable)
)

// Fault is one of netem's packet faults in percentages, with the fields of
// fault.NetworkPacketFault, which converts to it.
type Fault struct {
	Percent, Correlation float64
}

// IsZero reports whether the fault is off.
func (f Fault) IsZero() bool { return f == Fault{} }

// String formats the fault as tc's arguments do: the percentage, and the
// correlation when it is not zero.
func (f Fault) String() string {
	text := strconv.FormatFloat(f.Percent, 'f', -1, 64) + "%"
	if f.Correlation != 0 {
		text += " " + strconv.FormatFloat(f.Correlation, 'f', -1, 64) + "%"
	}
	return text
}

// Matches reports whether the kernel's fault is f: the kernel keeps each
// percentage as a 32-bit probability.
func (f Fault) Matches(kernel Fault) bool {
	return math.Abs(f.Percent-kernel.Percent) <= 1e-4 && math.Abs(f.Correlation-kernel.Correlation) <= 1e-4
}

// Named is one packet fault with its netem keyword.
type Named struct {
	Name  string
	Fault Fault
}

// Params is the netem qdisc of a profile: Rate (bit/s) and Limit as the
// lease resolved them, Limit zero without a Rate.
type Params struct {
	Delay, Jitter               time.Duration
	Loss                        float64
	Rate                        uint64
	Limit                       int
	Reorder, Duplicate, Corrupt Fault
}

// Faults are the packet faults, set or not, in tc's order.
func (p Params) Faults() []Named {
	return []Named{{"reorder", p.Reorder}, {"duplicate", p.Duplicate}, {"corrupt", p.Corrupt}}
}

// Requested are the netem keywords of the packet faults that are set, in
// tc's order.
func (p Params) Requested() []string {
	var names []string
	for _, fault := range p.Faults() {
		if !fault.Fault.IsZero() {
			names = append(names, fault.Name)
		}
	}
	return names
}

// Arguments is the netem qdisc's parameter list, starting with "netem".
func (p Params) Arguments() []string {
	netem := []string{"netem"}
	// tc (iproute2 6.1, as in the published helper) sends a duplicate's
	// correlation only together with a delay's correlation, and drops it
	// silently otherwise. A zero delay correlation, which the kernel also
	// has without one, makes it send both.
	dupCorrelation := p.Duplicate.Correlation != 0
	if p.Delay > 0 || dupCorrelation {
		netem = append(netem, "delay", fmt.Sprintf("%dus", p.Delay.Microseconds()))
		if p.Jitter > 0 || dupCorrelation {
			netem = append(netem, fmt.Sprintf("%dus", p.Jitter.Microseconds()))
		}
		if dupCorrelation {
			netem = append(netem, "0%")
		}
	}
	if p.Loss > 0 {
		netem = append(netem, "loss", strconv.FormatFloat(p.Loss, 'f', -1, 64)+"%")
	}
	for _, fault := range p.Faults() {
		if !fault.Fault.IsZero() {
			netem = append(append(netem, fault.Name), strings.Fields(fault.Fault.String())...)
		}
	}
	if p.Rate > 0 {
		netem = append(netem, "rate", strconv.FormatUint(p.Rate, 10)+"bit", "limit", strconv.Itoa(p.Limit))
	}
	return netem
}

// Verify requires an applied state with exactly the delay, jitter, loss,
// rate, queue limit, packet faults and reorder gap of p.
func (p Params) Verify(s State) error {
	// The kernel keeps the loss as a 32-bit probability.
	if !s.Applied || s.Delay != p.Delay || s.Jitter != p.Jitter || math.Abs(s.Loss-p.Loss) > 1e-4 ||
		s.Rate != p.Rate || p.Rate > 0 && s.Limit != p.Limit {
		return errors.Join(fmt.Errorf("the kernel reports root %s %s with delay %v, jitter %v, loss %v%%, rate %s and queue limit %d instead of the requested profile",
			s.RootKind, s.RootHandle, s.Delay, s.Jitter, s.Loss, FormatRate(s.Rate), s.Limit), s.Mismatch)
	}
	kernel := map[string]Fault{"reorder": s.Reorder, "duplicate": s.Duplicate, "corrupt": s.Corrupt}
	for _, fault := range p.Faults() {
		if !fault.Fault.Matches(kernel[fault.Name]) {
			return fmt.Errorf("the kernel reports %s %v instead of the requested %v", fault.Name, kernel[fault.Name], fault.Fault)
		}
	}
	wantGap := 0
	if !p.Reorder.IsZero() {
		wantGap = 1
	}
	if s.Gap != wantGap {
		return fmt.Errorf("the kernel reports a reorder gap of %d instead of %d", s.Gap, wantGap)
	}
	return nil
}

// FormatRate formats a rate in bit/s with the largest decimal prefix that
// divides it.
func FormatRate(rate uint64) string {
	for _, unit := range []struct {
		size   uint64
		suffix string
	}{{1e9, "Gbit/s"}, {1e6, "Mbit/s"}, {1e3, "Kbit/s"}} {
		if rate >= unit.size && rate%unit.size == 0 {
			return strconv.FormatUint(rate/unit.size, 10) + unit.suffix
		}
	}
	return strconv.FormatUint(rate, 10) + "bit/s"
}

// Percent converts one of tc's fractions to a percentage, rounded to the
// micro-percent that the kernel's 32-bit probability can tell apart.
func Percent(fraction float64) float64 { return math.Round(fraction*100*1e6) / 1e6 }

// Seconds converts tc's seconds to whole microseconds.
func Seconds(value float64) time.Duration {
	return time.Duration(math.Round(value*1e6)) * time.Microsecond
}

// CommandError describes a tc command that timed out or exited nonzero.
func CommandError(label string, result supervised.Report) error {
	if result.TimedOut {
		return fmt.Errorf("%s timed out", label)
	}
	return Classify(label, result.ExitCode, strings.TrimSpace(string(result.Stderr)))
}

// Classify turns tc's exit code and error text into the error kinds of the
// contract, or a *ceph.NativeFailure of tc.
func Classify(label string, code int, text string) error {
	switch {
	case strings.Contains(text, "Operation not permitted"):
		return fmt.Errorf("%s: %w", label, ErrPermissionDenied)
	case strings.Contains(text, "Specified qdisc kind is unknown"):
		return fmt.Errorf("%s: %w: no such qdisc", label, ErrKernelUnsupported)
	case strings.Contains(text, "TC classifier not found"):
		return fmt.Errorf("%s: %w: no such classifier", label, ErrKernelUnsupported)
	case strings.Contains(text, "Unknown qdisc") || strings.Contains(text, "Unknown filter") || strings.Contains(text, "unparsable") ||
		strings.Contains(text, `What is "`):
		return fmt.Errorf("%s: %w: tc cannot parse it", label, ErrImageUnsupported)
	}
	return fmt.Errorf("%s: %w", label, faulthook.NativeFailure("tc", code, nil, []byte(text)))
}
