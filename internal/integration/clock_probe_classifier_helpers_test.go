//go:build all || integration

package integration_test

import (
	"fmt"
	"sort"
	"strings"
)

// This file models, without a clock, a process or Docker, how the results of a
// same-process, four-route Linux clock probe are judged. It classifies
// recorded observations only. It never samples a clock, does not show that the
// interposer is loaded into any Ceph daemon and is not evidence that a clock
// fault backend works.
//
// Source facts the expectations follow (src/libfaketime.c and src/Makefile at
// libfaketime v0.9.12, commit 3ccdd344aadf1e317156fa5fb7c881f2c4404778):
//
//   - The public clock_gettime wrapper calls the real function and then
//     overwrites the result with the faked value. It skips the overwrite for
//     CLOCK_MONOTONIC, CLOCK_MONOTONIC_RAW, CLOCK_MONOTONIC_COARSE and
//     CLOCK_BOOTTIME unless fake_monotonic_clock is set. That flag defaults to
//     1 except on ARM builds, and FAKETIME_DONT_FAKE_MONOTONIC (checked
//     first) or DONT_FAKE_MONOTONIC with the exact value "1" clears it; any
//     other value sets it.
//   - The libc syscall() symbol is only interposed when the library is built
//     with INTERCEPT_SYSCALL, which the default Makefile CFLAGS do not
//     define. In such a build the clock_gettime branch calls the interposed
//     clock_gettime only when the target process has FAKETIME or
//     FAKETIME_TIMESTAMP_FILE in its environment; otherwise the call goes to
//     the real syscall unchanged. Without the macro there is no wrapper at
//     all.
//   - Nothing in the library names a vDSO symbol or issues a raw architecture
//     syscall instruction, so those two routes are expected to bypass it. This
//     is a hypothesis from reading the source. The probe has to measure it.
//
// An expectation derived here is never an observation. A path whose
// observation contradicts its expectation invalidates the build or
// environment facts the expectation was derived from.

// clockProbePath names one independently labelled route to the kernel clock.
type clockProbePath string

const (
	clockPathPublic     clockProbePath = "public_clock_gettime"
	clockPathLibcSys    clockProbePath = "libc_syscall"
	clockPathVDSO       clockProbePath = "vdso_direct"
	clockPathRawSyscall clockProbePath = "raw_syscall"
	// clockPathMonotonic is the monotonic control: the public call with
	// CLOCK_MONOTONIC.
	clockPathMonotonic clockProbePath = "monotonic_control"
)

var clockAllPaths = []clockProbePath{clockPathPublic, clockPathLibcSys, clockPathVDSO, clockPathRawSyscall, clockPathMonotonic}

// clockProbePhase is when a sample was taken relative to the offset update.
type clockProbePhase string

const (
	clockPhaseBefore clockProbePhase = "before"
	clockPhaseDuring clockProbePhase = "during"
	clockPhaseAfter  clockProbePhase = "after"
)

var clockAllPhases = []clockProbePhase{clockPhaseBefore, clockPhaseDuring, clockPhaseAfter}

// clockRouteStatus is the return status of one route in one phase.
type clockRouteStatus string

const (
	clockRouteOK          clockRouteStatus = "ok"
	clockRouteError       clockRouteStatus = "error"
	clockRouteUnsupported clockRouteStatus = "unsupported"
)

// clockOffsetInterval is a closed interval of possible offsets in nanoseconds.
// Known false keeps an unmeasured interval from reading as [0,0].
type clockOffsetInterval struct {
	Known  bool
	Lo, Hi int64
}

// bracketedClockOffset turns a remote realtime sample T taken between the
// controller samples rLo and rHi into [T-rHi, T-rLo]. An inverted bracket is
// not evidence. The width of the result is the measurement uncertainty and no
// midpoint is reported.
func bracketedClockOffset(t, rLo, rHi int64) clockOffsetInterval {
	if rHi < rLo {
		return clockOffsetInterval{}
	}
	return clockOffsetInterval{Known: true, Lo: t - rHi, Hi: t - rLo}
}

func (a clockOffsetInterval) overlaps(b clockOffsetInterval) bool {
	return a.Known && b.Known && a.Lo <= b.Hi && b.Lo <= a.Hi
}

func (a clockOffsetInterval) shifted(d int64) clockOffsetInterval {
	return clockOffsetInterval{Known: a.Known, Lo: a.Lo + d, Hi: a.Hi + d}
}

// clockRouteSample is one route's record in one phase. A missing map entry and
// an unknown status are both unusable.
type clockRouteSample struct {
	Status clockRouteStatus
	Offset clockOffsetInterval
}

// clockBinding is where the public clock_gettime symbol resolved in the probe.
type clockBinding string

const (
	clockBindInterposer clockBinding = "interposer"
	clockBindLibc       clockBinding = "libc"
	clockBindUnknown    clockBinding = ""
)

// clockProbeFacts are the build and process facts recorded with the probe.
// Tri-state booleans are pointers so that an unrecorded fact stays unknown.
type clockProbeFacts struct {
	InterposerSHA256 string
	// InterceptSyscall is whether the interposer was built with
	// INTERCEPT_SYSCALL, read from the build record and not assumed.
	InterceptSyscall *bool
	Libc             string
	Architecture     string
	// StaticOrSetuid is true when the probe binary is statically linked or
	// setuid, in which case LD_PRELOAD is not honoured.
	StaticOrSetuid *bool
	Binding        clockBinding
	// Environment of the probe process itself, read from the process.
	EnvKnown      bool
	Faketime      *string
	TimestampFile *string
	DontFakeMono  *string // FAKETIME_DONT_FAKE_MONOTONIC
	DontFakeMono2 *string // DONT_FAKE_MONOTONIC
}

// clockProbeRecord is everything one probe run hands to the judge.
type clockProbeRecord struct {
	// IntendedOffset is the nanosecond offset the controller applied during
	// the "during" phase. The before and after phases intend zero.
	IntendedOffset int64
	// IntendedBefore and IntendedAfter are recorded so a run that did not
	// intend zero is rejected instead of assumed.
	IntendedBefore, IntendedAfter int64
	Facts                         clockProbeFacts
	Samples                       map[clockProbePath]map[clockProbePhase]clockRouteSample
}

// clockExpectation is the source-derived expectation for one route.
type clockExpectation string

const (
	clockExpectShift   clockExpectation = "shift"
	clockExpectBypass  clockExpectation = "bypass"
	clockExpectUnknown clockExpectation = "unknown"
)

// monotonicFaked mirrors get_fake_monotonic_setting: the first variable that
// is present decides, and only the exact value "1" disables monotonic faking.
// The result is the default (faked) on x86 builds; ARM builds default the
// other way, which is why ARM is reported as unknown here.
func monotonicFaked(f clockProbeFacts) (faked, known bool) {
	if !f.EnvKnown {
		return false, false
	}
	v := f.DontFakeMono
	if v == nil {
		v = f.DontFakeMono2
	}
	if v != nil {
		return *v != "1", true
	}
	switch f.Architecture {
	case "amd64", "x86_64", "386":
		return true, true
	}
	return false, false
}

// expectedClockRoute derives the expectation for one path from the facts.
func expectedClockRoute(p clockProbePath, f clockProbeFacts) clockExpectation {
	switch p {
	case clockPathPublic:
		if f.Binding == clockBindInterposer {
			return clockExpectShift
		}
		if f.Binding == clockBindLibc {
			return clockExpectBypass
		}
	case clockPathLibcSys:
		if f.InterceptSyscall == nil || !f.EnvKnown {
			return clockExpectUnknown
		}
		if !*f.InterceptSyscall {
			return clockExpectBypass
		}
		if f.Faketime != nil || f.TimestampFile != nil {
			return clockExpectShift
		}
		return clockExpectBypass
	case clockPathVDSO, clockPathRawSyscall:
		return clockExpectBypass
	case clockPathMonotonic:
		if faked, ok := monotonicFaked(f); ok {
			if faked {
				return clockExpectShift
			}
			return clockExpectBypass
		}
	}
	return clockExpectUnknown
}

// clockPhaseState says where a phase interval sits relative to the baseline
// and the baseline moved by the intended offset.
type clockPhaseState string

const (
	clockAtBaseline clockPhaseState = "baseline"
	clockAtShifted  clockPhaseState = "shifted"
	// clockAmbiguous covers an interval that overlaps both, neither, or that
	// was not measured. It is never promoted to either state.
	clockAmbiguous clockPhaseState = "ambiguous"
	// clockNoResult is a route that is unsupported or returned an error.
	clockNoResult clockPhaseState = "no_result"
)

func classifyClockPhase(base, got clockOffsetInterval, d int64) clockPhaseState {
	if !base.Known || !got.Known {
		return clockAmbiguous
	}
	e := base.shifted(d)
	atBase, atShift := got.overlaps(base), got.overlaps(e)
	switch {
	case atBase && !atShift:
		return clockAtBaseline
	case atShift && !atBase:
		return clockAtShifted
	}
	return clockAmbiguous
}

type clockProbeVerdict string

const (
	clockProbeAdmitted     clockProbeVerdict = "admitted"
	clockProbeRejected     clockProbeVerdict = "rejected"
	clockProbeInconclusive clockProbeVerdict = "inconclusive"
)

// clockPathReport is the per-path result.
type clockPathReport struct {
	Expectation clockExpectation
	Before      clockPhaseState
	During      clockPhaseState
	After       clockPhaseState
	// ControllerAgrees is true when the baseline interval contains zero, i.e.
	// the route agrees with the uninjected controller before the update.
	ControllerAgrees bool
	// Unsupported is true when the route reported unsupported in every phase.
	Unsupported bool
	// UsableReference is true only for a route that was measured at baseline
	// in every phase, agrees with the controller and was not expected to
	// shift. A route that shifted is never a reference.
	UsableReference bool
}

type clockProbeJudgement struct {
	Verdict clockProbeVerdict
	Reasons []string
	Paths   map[clockProbePath]clockPathReport
}

func (j *clockProbeJudgement) add(v clockProbeVerdict, format string, args ...any) {
	rank := map[clockProbeVerdict]int{clockProbeAdmitted: 0, clockProbeInconclusive: 1, clockProbeRejected: 2}
	if rank[v] > rank[j.Verdict] {
		j.Verdict = v
	}
	j.Reasons = append(j.Reasons, fmt.Sprintf(format, args...))
}

// judgeClockProbe decides whether a probe record admits the public realtime
// route as a shiftable clock and says which other routes may serve as an
// unaffected reference. An unsupported vdso or raw route is reported as such;
// it neither fails admission nor counts as a bypass observation.
func judgeClockProbe(r clockProbeRecord) clockProbeJudgement {
	j := clockProbeJudgement{Verdict: clockProbeAdmitted, Paths: map[clockProbePath]clockPathReport{}}
	f := r.Facts
	if r.IntendedBefore != 0 || r.IntendedAfter != 0 {
		j.add(clockProbeRejected, "before/after phases do not intend a zero offset")
	}
	if r.IntendedOffset == 0 {
		j.add(clockProbeRejected, "the during phase intends no offset")
	}
	for name, v := range map[string]string{"interposer hash": f.InterposerSHA256, "libc": f.Libc, "architecture": f.Architecture} {
		if v == "" {
			j.add(clockProbeInconclusive, "%s was not recorded", name)
		}
	}
	if f.InterceptSyscall == nil {
		j.add(clockProbeInconclusive, "INTERCEPT_SYSCALL build flag was not recorded")
	}
	if !f.EnvKnown {
		j.add(clockProbeInconclusive, "probe process environment was not read")
	}
	if f.StaticOrSetuid == nil {
		j.add(clockProbeInconclusive, "static/setuid status was not recorded")
	} else if *f.StaticOrSetuid {
		j.add(clockProbeRejected, "probe binary is static or setuid, so preload is not honoured")
	}
	switch f.Binding {
	case clockBindInterposer:
	case clockBindLibc:
		j.add(clockProbeRejected, "public clock_gettime binds to libc, not the interposer")
	default:
		j.add(clockProbeInconclusive, "public clock_gettime symbol binding was not recorded")
	}

	for _, p := range clockAllPaths {
		rep, ok := judgeClockPath(&j, r, p)
		if ok {
			j.Paths[p] = rep
		}
	}
	if pr, ok := j.Paths[clockPathPublic]; ok && j.Verdict != clockProbeRejected {
		switch {
		case pr.Unsupported:
			j.add(clockProbeRejected, "public clock_gettime is unsupported")
		case pr.Before == clockAtBaseline && pr.During == clockAtShifted && pr.After == clockAtBaseline:
		case pr.During == clockAtBaseline:
			j.add(clockProbeRejected, "public clock_gettime did not shift during the update")
		default:
			j.add(clockProbeInconclusive, "public clock_gettime phases are not a clean baseline/shifted/baseline sequence")
		}
	}
	if mr, ok := j.Paths[clockPathMonotonic]; ok {
		switch {
		case mr.Unsupported:
			j.add(clockProbeInconclusive, "monotonic control is unsupported")
		case mr.Expectation == clockExpectShift:
			j.add(clockProbeRejected, "monotonic control is faked in this configuration, so it is not an eligible control")
		case mr.Expectation == clockExpectUnknown:
			j.add(clockProbeInconclusive, "whether the monotonic control is faked is not known")
		case mr.Before != clockAtBaseline || mr.During != clockAtBaseline || mr.After != clockAtBaseline:
			j.add(clockProbeInconclusive, "monotonic control did not stay at baseline")
		}
	}
	sort.Strings(j.Reasons)
	return j
}

// judgeClockPath classifies one path. It returns false when the path has no
// usable record at all, after adding the reason to j.
func judgeClockPath(j *clockProbeJudgement, r clockProbeRecord, p clockProbePath) (clockPathReport, bool) {
	rep := clockPathReport{Expectation: expectedClockRoute(p, r.Facts)}
	phases := r.Samples[p]
	states := map[clockProbePhase]clockPhaseState{}
	unsupported := 0
	var base clockOffsetInterval
	for _, ph := range clockAllPhases {
		s, ok := phases[ph]
		if !ok {
			j.add(clockProbeInconclusive, "%s/%s has no sample", p, ph)
			states[ph] = clockAmbiguous
			continue
		}
		switch s.Status {
		case clockRouteUnsupported:
			unsupported++
			states[ph] = clockNoResult
			continue
		case clockRouteError:
			states[ph] = clockNoResult
			j.add(clockProbeInconclusive, "%s/%s returned an error, which is not evidence", p, ph)
			continue
		case clockRouteOK:
		default:
			states[ph] = clockAmbiguous
			j.add(clockProbeInconclusive, "%s/%s has unknown status %q", p, ph, s.Status)
			continue
		}
		if !s.Offset.Known {
			states[ph] = clockAmbiguous
			j.add(clockProbeInconclusive, "%s/%s has no measured offset interval", p, ph)
			continue
		}
		if ph == clockPhaseBefore {
			base = s.Offset
			rep.ControllerAgrees = s.Offset.Lo <= 0 && 0 <= s.Offset.Hi
			if !rep.ControllerAgrees {
				j.add(clockProbeInconclusive, "%s baseline does not contain zero, so it disagrees with the uninjected controller", p)
			}
			states[ph] = clockAtBaseline
			continue
		}
		states[ph] = classifyClockPhase(base, s.Offset, r.IntendedOffset)
	}
	rep.Before, rep.During, rep.After = states[clockPhaseBefore], states[clockPhaseDuring], states[clockPhaseAfter]
	if unsupported == len(clockAllPhases) {
		rep.Unsupported = true
		return rep, true
	}
	if unsupported > 0 {
		j.add(clockProbeInconclusive, "%s is unsupported in only some phases", p)
	}
	switch {
	case rep.During == clockAtShifted && rep.Expectation == clockExpectBypass:
		j.add(clockProbeRejected, "%s shifted but the pinned source predicts a bypass; the recorded build or environment facts are wrong", p)
	case rep.During == clockAtBaseline && rep.Expectation == clockExpectShift && p != clockPathPublic:
		j.add(clockProbeRejected, "%s stayed at baseline but the pinned source predicts a shift; the recorded build or environment facts are wrong", p)
	}
	rep.UsableReference = rep.ControllerAgrees && rep.Expectation == clockExpectBypass &&
		rep.Before == clockAtBaseline && rep.During == clockAtBaseline && rep.After == clockAtBaseline
	return rep, true
}

// clockReferencePaths lists, in stable order, the routes that may be used as an
// unaffected reference. It is empty unless the whole record was admitted.
func clockReferencePaths(j clockProbeJudgement) []string {
	var out []string
	if j.Verdict != clockProbeAdmitted {
		return out
	}
	for p, rep := range j.Paths {
		if rep.UsableReference {
			out = append(out, string(p))
		}
	}
	sort.Strings(out)
	return out
}

func (j clockProbeJudgement) String() string {
	return fmt.Sprintf("%s: %s", j.Verdict, strings.Join(j.Reasons, "; "))
}
