//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

const probeD = int64(30e9)

func boolPtr(b bool) *bool { return &b }

// probeIv is an offset interval of 2ms width around centre.
func probeIv(centre int64) clockOffsetInterval {
	return clockOffsetInterval{Known: true, Lo: centre - 1e6, Hi: centre + 1e6}
}

func probeOK(centre int64) clockRouteSample {
	return clockRouteSample{Status: clockRouteOK, Offset: probeIv(centre)}
}

func probeSeq(before, during, after int64) map[clockProbePhase]clockRouteSample {
	return map[clockProbePhase]clockRouteSample{
		clockPhaseBefore: probeOK(before), clockPhaseDuring: probeOK(during), clockPhaseAfter: probeOK(after),
	}
}

func probeUnsupported() map[clockProbePhase]clockRouteSample {
	u := clockRouteSample{Status: clockRouteUnsupported}
	return map[clockProbePhase]clockRouteSample{clockPhaseBefore: u, clockPhaseDuring: u, clockPhaseAfter: u}
}

// probeBase is the expected source-derived outcome on a default build:
// the public route shifts, the syscall wrapper is absent, vdso and raw bypass
// and the monotonic control is exempt through FAKETIME_DONT_FAKE_MONOTONIC=1.
func probeBase() clockProbeRecord {
	return clockProbeRecord{
		IntendedOffset: probeD,
		Facts: clockProbeFacts{
			InterposerSHA256: "0123", InterceptSyscall: boolPtr(false), Libc: "glibc-2.39", Architecture: "amd64",
			StaticOrSetuid: boolPtr(false), Binding: clockBindInterposer, EnvKnown: true,
			TimestampFile: strPtr("owned-faketime.txt"), DontFakeMono: strPtr("1"),
		},
		Samples: map[clockProbePath]map[clockProbePhase]clockRouteSample{
			clockPathPublic:     probeSeq(0, probeD, 0),
			clockPathLibcSys:    probeSeq(0, 0, 0),
			clockPathVDSO:       probeSeq(0, 0, 0),
			clockPathRawSyscall: probeSeq(0, 0, 0),
			clockPathMonotonic:  probeSeq(0, 0, 0),
		},
	}
}

func wantProbe(t *testing.T, name string, r clockProbeRecord, want clockProbeVerdict, reason string) clockProbeJudgement {
	t.Helper()
	got := judgeClockProbe(r)
	if got.Verdict != want || (reason != "" && !strings.Contains(got.String(), reason)) {
		t.Errorf("%s: got %s, want %s containing %q", name, got, want, reason)
	}
	return got
}

func TestClockProbeBracketedOffset(t *testing.T) {
	got := bracketedClockOffset(1_000_000_030, 1_000_000_000, 1_000_000_010)
	if got != (clockOffsetInterval{Known: true, Lo: 20, Hi: 30}) {
		t.Errorf("bracket: %+v", got)
	}
	if bracketedClockOffset(5, 10, 9).Known {
		t.Error("an inverted bracket must not be evidence")
	}
	if (clockOffsetInterval{}).overlaps(clockOffsetInterval{}) {
		t.Error("two unknown intervals must not overlap")
	}
}

func TestClockProbeDefaultBuildAdmitted(t *testing.T) {
	j := wantProbe(t, "default build", probeBase(), clockProbeAdmitted, "")
	if got := strings.Join(clockReferencePaths(j), ","); got != "libc_syscall,monotonic_control,raw_syscall,vdso_direct" {
		t.Errorf("reference paths %q", got)
	}
}

// The libc syscall route is only a reference when the build lacks
// INTERCEPT_SYSCALL. With the macro and a live-file environment it shifts and
// must not be offered as an unaffected reference.
func TestClockProbeInterceptSyscallBuild(t *testing.T) {
	r := probeBase()
	r.Facts.InterceptSyscall = boolPtr(true)
	r.Samples[clockPathLibcSys] = probeSeq(0, probeD, 0)
	j := wantProbe(t, "intercept build", r, clockProbeAdmitted, "")
	for _, p := range clockReferencePaths(j) {
		if p == string(clockPathLibcSys) {
			t.Errorf("a shifted libc syscall route was offered as a reference")
		}
	}

	// Same macro, but neither FAKETIME nor FAKETIME_TIMESTAMP_FILE in the
	// process: the wrapper passes through, so a shift contradicts the source.
	r = probeBase()
	r.Facts.InterceptSyscall = boolPtr(true)
	r.Facts.TimestampFile = nil
	r.Samples[clockPathLibcSys] = probeSeq(0, probeD, 0)
	wantProbe(t, "wrapper without env mode", r, clockProbeRejected, "libc_syscall shifted but the pinned source predicts a bypass")

	// Macro present and env set but the route stayed at baseline.
	r = probeBase()
	r.Facts.InterceptSyscall = boolPtr(true)
	wantProbe(t, "wrapper silently absent", r, clockProbeRejected, "libc_syscall stayed at baseline but the pinned source predicts a shift")
}

// A vdso or raw route that shifts contradicts the bypass hypothesis and is
// rejected rather than being quietly accepted as a second faked route.
func TestClockProbeBypassRoutesContradicted(t *testing.T) {
	for _, p := range []clockProbePath{clockPathVDSO, clockPathRawSyscall} {
		r := probeBase()
		r.Samples[p] = probeSeq(0, probeD, 0)
		wantProbe(t, string(p)+" shifted", r, clockProbeRejected, string(p)+" shifted but the pinned source predicts a bypass")
	}
}

func TestClockProbeUnsupportedRoutesStayExplicit(t *testing.T) {
	r := probeBase()
	r.Samples[clockPathVDSO] = probeUnsupported()
	r.Samples[clockPathRawSyscall] = probeUnsupported()
	j := wantProbe(t, "unsupported direct routes", r, clockProbeAdmitted, "")
	if !j.Paths[clockPathVDSO].Unsupported || j.Paths[clockPathVDSO].UsableReference {
		t.Errorf("unsupported vdso must be explicit and never a reference: %+v", j.Paths[clockPathVDSO])
	}
	// Absence of a record is not the same as an explicit unsupported.
	delete(r.Samples, clockPathVDSO)
	wantProbe(t, "missing vdso record", r, clockProbeInconclusive, "vdso_direct/before has no sample")
	// Unsupported only in some phases is inconclusive.
	r = probeBase()
	r.Samples[clockPathRawSyscall][clockPhaseDuring] = clockRouteSample{Status: clockRouteUnsupported}
	wantProbe(t, "partially unsupported", r, clockProbeInconclusive, "unsupported in only some phases")
}

func TestClockProbePublicRouteRequirements(t *testing.T) {
	r := probeBase()
	r.Samples[clockPathPublic] = probeSeq(0, 0, 0)
	wantProbe(t, "public did not shift", r, clockProbeRejected, "did not shift")

	r = probeBase()
	r.Samples[clockPathPublic] = probeSeq(0, probeD, probeD)
	wantProbe(t, "public stuck shifted after", r, clockProbeInconclusive, "not a clean baseline/shifted/baseline")

	r = probeBase()
	r.Facts.Binding = clockBindLibc
	r.Samples[clockPathPublic] = probeSeq(0, 0, 0)
	wantProbe(t, "binds to libc", r, clockProbeRejected, "binds to libc")

	r = probeBase()
	r.Facts.StaticOrSetuid = boolPtr(true)
	wantProbe(t, "static or setuid", r, clockProbeRejected, "static or setuid")

	// An offset smaller than the interval width cannot tell shift from
	// baseline, and no wider slack is invented to decide it.
	r = probeBase()
	r.IntendedOffset = 1e6
	r.Samples[clockPathPublic] = probeSeq(0, 1e6, 0)
	wantProbe(t, "offset below resolution", r, clockProbeInconclusive, "not a clean baseline/shifted/baseline")
}

func TestClockProbeMonotonicControl(t *testing.T) {
	// x86 default fakes monotonic: the control is ineligible.
	r := probeBase()
	r.Facts.DontFakeMono = nil
	r.Samples[clockPathMonotonic] = probeSeq(0, probeD, 0)
	wantProbe(t, "monotonic faked by default", r, clockProbeRejected, "not an eligible control")

	// Only the exact value "1" opts out; "0" does not.
	r = probeBase()
	r.Facts.DontFakeMono = strPtr("0")
	wantProbe(t, "DONT_FAKE_MONOTONIC=0", r, clockProbeRejected, "not an eligible control")

	// The first variable that is present decides, even if empty.
	r = probeBase()
	r.Facts.DontFakeMono = strPtr("")
	r.Facts.DontFakeMono2 = strPtr("1")
	wantProbe(t, "FAKETIME_ variable wins", r, clockProbeRejected, "not an eligible control")

	// The unprefixed variable alone is honoured.
	r = probeBase()
	r.Facts.DontFakeMono, r.Facts.DontFakeMono2 = nil, strPtr("1")
	wantProbe(t, "unprefixed variable", r, clockProbeAdmitted, "")

	// ARM defaults the other way, and an unread default stays unknown.
	r = probeBase()
	r.Facts.DontFakeMono, r.Facts.Architecture = nil, "arm64"
	wantProbe(t, "arm default not asserted", r, clockProbeInconclusive, "is not known")

	// A control that moved despite the opt-out contradicts the facts.
	r = probeBase()
	r.Samples[clockPathMonotonic] = probeSeq(0, probeD, 0)
	wantProbe(t, "monotonic moved", r, clockProbeRejected, "monotonic_control shifted but the pinned source predicts a bypass")
}

func TestClockProbeMissingFactsAreInconclusive(t *testing.T) {
	cases := map[string]func(*clockProbeRecord){
		"hash":           func(r *clockProbeRecord) { r.Facts.InterposerSHA256 = "" },
		"libc":           func(r *clockProbeRecord) { r.Facts.Libc = "" },
		"architecture":   func(r *clockProbeRecord) { r.Facts.Architecture = "" },
		"intercept flag": func(r *clockProbeRecord) { r.Facts.InterceptSyscall = nil },
		"environment":    func(r *clockProbeRecord) { r.Facts.EnvKnown = false },
		"static status":  func(r *clockProbeRecord) { r.Facts.StaticOrSetuid = nil },
		"binding":        func(r *clockProbeRecord) { r.Facts.Binding = clockBindUnknown },
		"public sample":  func(r *clockProbeRecord) { delete(r.Samples[clockPathPublic], clockPhaseDuring) },
		"public interval": func(r *clockProbeRecord) {
			r.Samples[clockPathPublic][clockPhaseDuring] = clockRouteSample{Status: clockRouteOK}
		},
		"public error": func(r *clockProbeRecord) {
			r.Samples[clockPathPublic][clockPhaseDuring] = clockRouteSample{Status: clockRouteError, Offset: probeIv(probeD)}
		},
		"unknown status": func(r *clockProbeRecord) {
			r.Samples[clockPathPublic][clockPhaseDuring] = clockRouteSample{Status: "", Offset: probeIv(probeD)}
		},
		"baseline mismatch": func(r *clockProbeRecord) { r.Samples[clockPathRawSyscall][clockPhaseBefore] = probeOK(5e9) },
	}
	for name, mut := range cases {
		r := probeBase()
		mut(&r)
		j := wantProbe(t, name, r, clockProbeInconclusive, "")
		if got := clockReferencePaths(j); len(got) != 0 {
			t.Errorf("%s: a non-admitted record offered references %v", name, got)
		}
	}
}

func TestClockProbeIntendedPhases(t *testing.T) {
	r := probeBase()
	r.IntendedOffset = 0
	wantProbe(t, "no intended offset", r, clockProbeRejected, "intends no offset")
	r = probeBase()
	r.IntendedBefore = 1
	wantProbe(t, "non-zero before", r, clockProbeRejected, "do not intend a zero offset")
	r = probeBase()
	r.IntendedAfter = 1
	wantProbe(t, "non-zero after", r, clockProbeRejected, "do not intend a zero offset")
}

func TestClockProbeControllerDisagreement(t *testing.T) {
	// A bypass candidate that disagrees with the controller before the update
	// is not offered as a reference, even if it stays flat.
	r := probeBase()
	r.Samples[clockPathVDSO] = probeSeq(5e9, 5e9, 5e9)
	j := judgeClockProbe(r)
	if j.Paths[clockPathVDSO].UsableReference {
		t.Error("a route that disagrees with the controller must not be a reference")
	}
	if j.Verdict != clockProbeInconclusive {
		t.Errorf("verdict %s", j)
	}
}
