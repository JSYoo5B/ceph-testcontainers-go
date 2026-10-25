//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const envelopeFixtureToken = "envelopequote0123abcd"

func envelopeFixtureReport() *ceph.VerificationReport {
	return &ceph.VerificationReport{
		Sources: []ceph.VerificationSource{
			{Source: "mgr.a", ContainerID: "c2", ImageID: "sha256:image", StartedAt: "2026-10-26T01:00:00Z", LogDriver: "json-file", LogBytes: 10},
			{Source: envelopeSource, ContainerID: "c1", ImageID: "sha256:image", StartedAt: "2026-10-26T01:00:00Z", LogDriver: "json-file", LogBytes: 4096},
		},
		Logs: ceph.VerificationCheck{
			Outcome: ceph.VerificationUnexpected,
			Findings: []ceph.VerificationFinding{
				{Source: envelopeSource, Event: string(ceph.LogClusterWarning), Text: "Health check failed: noout flag(s) set (OSDMAP_FLAGS)", Expectation: envelopeProbeName},
			},
			Expectations: []ceph.ExpectationResult{
				{Name: envelopeProbeName, Required: true, MinCount: 1, Matched: 2, Satisfied: true},
				{Name: envelopeQuotedName, Required: true, MinCount: 1},
			},
		},
	}
}

func envelopeFixtureRaw() (probe, quoted envelopeRaw) {
	return envelopeRaw{Priorities: map[string]int{"WRN": 2}}, envelopeRaw{Priorities: map[string]int{"INF": 2}}
}

func TestEnvelopeEvidenceAcceptsTheDiscriminatingObservation(t *testing.T) {
	probe, quoted := envelopeFixtureRaw()
	if err := judgeEnvelopeEvidence(envelopeFixtureReport(), envelopeFixtureToken, probe, quoted); err != nil {
		t.Fatal(err)
	}
}

// Each control withholds or breaks one piece of the evidence; none may pass.
func TestEnvelopeEvidenceRefusesMissingOrBrokenEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ceph.VerificationReport, *envelopeRaw, *envelopeRaw)
		want   string
	}{
		{"missing MON source", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources = r.Sources[:1] }, "no mon.a source"},
		{"no sources at all", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources = nil }, "no mon.a source"},
		{"unidentified generation", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources[1].ContainerID = "" }, "not identified"},
		{"missing image", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources[1].ImageID = "" }, "not identified"},
		{"rotating driver", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources[1].LogDriver = "local" }, "non-rotating"},
		{"empty driver", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources[1].LogDriver = "" }, "non-rotating"},
		{"empty log stream", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Sources[1].LogBytes = 0 }, "stream is empty"},
		{"log gap", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Gaps = []ceph.VerificationGap{{Source: "osd.0", Reason: "driver not covered"}}
		}, "log gaps"},
		{"omitted findings", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Logs.OmittedFindings = 1 }, "omitted"},
		{"no expectations", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Logs.Expectations = nil }, "no expectation warning-probe"},
		{"probe expectation absent", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Expectations = r.Logs.Expectations[1:]
		}, "no expectation warning-probe"},
		{"quoted expectation absent", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Expectations = r.Logs.Expectations[:1]
		}, "no expectation quoted-info"},
		{"probe unsatisfied", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Expectations[0].Satisfied, r.Logs.Expectations[0].Matched = false, 0
		}, "did not satisfy warning-probe"},
		{"probe satisfied without a match", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Logs.Expectations[0].Matched = 0 }, "did not satisfy warning-probe"},
		{"quoted expectation satisfied", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Expectations[1].Satisfied, r.Logs.Expectations[1].Matched = true, 1
		}, "satisfied quoted-info"},
		{"quoted expectation matched but unsatisfied", func(r *ceph.VerificationReport, _, _ *envelopeRaw) { r.Logs.Expectations[1].Matched = 1 }, "satisfied quoted-info"},
		{"finding from the token", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Findings = append(r.Logs.Findings, ceph.VerificationFinding{Source: envelopeSource, Event: string(ceph.LogClusterWarning), Text: "x " + envelopeFixtureToken})
		}, "derived from the quoted token"},
		{"probe finding of another kind", func(r *ceph.VerificationReport, _, _ *envelopeRaw) {
			r.Logs.Findings[0].Event = string(ceph.LogClusterError)
		}, "probe finding"},
		{"no raw WRN probe", func(_ *ceph.VerificationReport, probe, _ *envelopeRaw) { probe.Priorities = map[string]int{"INF": 1} }, "no WRN probe line"},
		{"nil raw probe", func(_ *ceph.VerificationReport, probe, _ *envelopeRaw) { probe.Priorities = nil }, "no WRN probe line"},
		{"no raw INF quote", func(_ *ceph.VerificationReport, _, quoted *envelopeRaw) { quoted.Priorities = map[string]int{} }, "no INF line"},
		{"only an unrecognized quote", func(_ *ceph.VerificationReport, _, quoted *envelopeRaw) {
			quoted.Priorities, quoted.Unrecognized = map[string]int{}, 1
		}, "no INF line"},
		{"quote also in a WRN line", func(_ *ceph.VerificationReport, _, quoted *envelopeRaw) { quoted.Priorities["WRN"] = 1 }, "void"},
		{"quote also in an ERR line", func(_ *ceph.VerificationReport, _, quoted *envelopeRaw) { quoted.Priorities["ERR"] = 1 }, "void"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := envelopeFixtureReport()
			probe, quoted := envelopeFixtureRaw()
			test.mutate(report, &probe, &quoted)
			err := judgeEnvelopeEvidence(report, envelopeFixtureToken, probe, quoted)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want an error containing %q", err, test.want)
			}
		})
	}
	probe, quoted := envelopeFixtureRaw()
	if err := judgeEnvelopeEvidence(nil, envelopeFixtureToken, probe, quoted); err == nil {
		t.Fatal("a nil report passed")
	}
	if err := judgeEnvelopeEvidence(envelopeFixtureReport(), "", probe, quoted); err == nil {
		t.Fatal("an empty token passed")
	}
}

func TestEnvelopeRawCountsUseTheStructuralPriority(t *testing.T) {
	const token = envelopeFixtureToken
	lines := []string{
		"2026-10-26T01:02:03.123+0000 7f3a1c0e5640  0 log_channel(cluster) log [INF] : " + token + " quotes [WRN]",
		"cluster 2026-10-26T01:02:03.123456+0000 mon.a (mon.0) 17 : cluster [INF] " + token + " quotes [WRN]",
		`audit 2026-10-26T01:02:03.123456+0000 mon.a (mon.0) 18 : audit [INF] from='client.1' cmd=[{"prefix": "log", "logtext": ["` + token + ` quotes [WRN]"]}]: dispatch`,
		"2026-10-26T01:02:03.123+0000 7f3a1c0e5640  0 log_channel(cluster) log [WRN] : Health check failed: noout flag(s) set (OSDMAP_FLAGS) " + token,
		"unrelated " + token + " [WRN] without an envelope",
		"2026-10-26T01:02:03.123+0000 7f3a1c0e5640  0 log_channel(cluster) log [ERR] : other",
		"2026-10-26T01:02:03.123+0000 7f3a1c0e5640  0 log_channel(cluster) log [INF] : unrelated",
	}
	got := rawEnvelopeCounts(lines, token)
	if got.Priorities["INF"] != 3 || got.Priorities["WRN"] != 1 || got.Priorities["ERR"] != 0 || got.Unrecognized != 1 {
		t.Fatalf("counts %+v", got)
	}
	if empty := rawEnvelopeCounts(lines, ""); len(empty.Priorities) != 0 || empty.Unrecognized != 0 {
		t.Fatalf("an empty token counted %+v", empty)
	}
	if none := rawEnvelopeCounts(nil, token); len(none.Priorities) != 0 {
		t.Fatalf("no lines counted %+v", none)
	}
}
