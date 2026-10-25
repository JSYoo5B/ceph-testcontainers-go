//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"strings"
	"testing"
)

// These are Docker-free controls of the observation model, with synthetic
// "ceph versions" documents shaped like the monitor's output. They establish
// no native upgrade behavior.

const (
	upgradeSrc = "ceph version 19.2.5 (aaaa) squid (stable)"
	upgradeDst = "ceph version 20.2.4 (bbbb) tentacle (stable)"
)

func upgradeVersions(mon, mgr, osd string) []byte {
	return []byte(`{"mon":{` + mon + `},"mgr":{` + mgr + `},"osd":{` + osd + `},"overall":{"ceph version 19.2.5 (aaaa) squid (stable)":6,"ceph version 20.2.4 (bbbb) tentacle (stable)":2}}`)
}

func TestUpgradeInventoryParsing(t *testing.T) {
	src3, src2 := `"`+upgradeSrc+`":3`, `"`+upgradeSrc+`":2`
	good := []byte(`{"mon":{` + src3 + `},"mgr":{` + src2 + `},"osd":{` + src3 + `},"overall":{"` + upgradeSrc + `":8}}`)
	got, err := parseUpgradeInventory(good)
	if err != nil || got["mon"]["19.2.5"] != 3 || got["mgr"]["19.2.5"] != 2 || got["osd"]["19.2.5"] != 3 {
		t.Fatalf("good inventory = %v, %v", got, err)
	}
	mixed, err := parseUpgradeInventory([]byte(`{"mon":{"` + upgradeSrc + `":1,"` + upgradeDst + `":2},"overall":{"` + upgradeSrc + `":1,"` + upgradeDst + `":2}}`))
	if err != nil || mixed["mon"]["19.2.5"] != 1 || mixed["mon"]["20.2.4"] != 2 {
		t.Fatalf("mixed inventory = %v, %v", mixed, err)
	}
	for name, doc := range map[string]string{
		"empty":             ``,
		"not an object":     `[]`,
		"null document":     `null`,
		"missing mon":       `{"overall":{"` + upgradeSrc + `":1}}`,
		"null mon":          `{"mon":null,"overall":{"` + upgradeSrc + `":1}}`,
		"missing overall":   `{"mon":{"` + upgradeSrc + `":1}}`,
		"null overall":      `{"mon":{"` + upgradeSrc + `":1},"overall":null}`,
		"empty mon":         `{"mon":{},"overall":{}}`,
		"empty mgr":         `{"mon":{"` + upgradeSrc + `":1},"mgr":{},"overall":{"` + upgradeSrc + `":1}}`,
		"role is a list":    `{"mon":[1],"overall":{}}`,
		"string count":      `{"mon":{"` + upgradeSrc + `":"1"},"overall":{"` + upgradeSrc + `":1}}`,
		"zero count":        `{"mon":{"` + upgradeSrc + `":0},"overall":{"` + upgradeSrc + `":0}}`,
		"fractional count":  `{"mon":{"` + upgradeSrc + `":1.5},"overall":{"` + upgradeSrc + `":1.5}}`,
		"unknown release":   `{"mon":{"unknown":1},"overall":{"unknown":1}}`,
		"overall too small": `{"mon":{"` + upgradeSrc + `":3},"overall":{"` + upgradeSrc + `":2}}`,
		"overall extra":     `{"mon":{"` + upgradeSrc + `":1},"overall":{"` + upgradeSrc + `":1,"` + upgradeDst + `":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if inv, err := parseUpgradeInventory([]byte(doc)); err == nil {
				t.Fatalf("parsed %q as %v", doc, inv)
			}
		})
	}
}

func TestUpgradeCheckpointComparison(t *testing.T) {
	inv, err := parseUpgradeInventory(upgradeVersions(`"`+upgradeSrc+`":1,"`+upgradeDst+`":2`, `"`+upgradeSrc+`":2`, `"`+upgradeSrc+`":3`))
	if err != nil {
		t.Fatal(err)
	}
	mixed := upgradeCheckpoint{
		"mon": {"19.2.5": 1, "20.2.4": 2},
		"mgr": {"19.2.5": 2},
		"osd": {"19.2.5": 3},
	}
	if err := mixed.check(inv); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]upgradeCheckpoint{
		"uniform baseline":  {"mon": {"19.2.5": 3}, "mgr": {"19.2.5": 2}, "osd": {"19.2.5": 3}},
		"wrong split":       {"mon": {"19.2.5": 2, "20.2.4": 1}, "mgr": {"19.2.5": 2}, "osd": {"19.2.5": 3}},
		"missing role":      {"mon": {"19.2.5": 1, "20.2.4": 2}, "mgr": {"19.2.5": 2}, "osd": {"19.2.5": 3}, "mds": {"19.2.5": 1}},
		"unexpected osd":    {"mon": {"19.2.5": 1, "20.2.4": 2}, "mgr": {"19.2.5": 2}},
		"early destination": {"mon": {"19.2.5": 1, "20.2.4": 2}, "mgr": {"20.2.4": 2}, "osd": {"19.2.5": 3}},
	} {
		if err := want.check(inv); err == nil {
			t.Errorf("%s: checkpoint accepted %v", name, inv)
		}
	}
}

func TestUpgradeObservationPhaseLifetime(t *testing.T) {
	targets := newUpgradeTargets("mon.a", "mon.b", "cli")
	baseline := targets.begin("baseline")
	uniform := upgradeCheckpoint{"mon": {"19.2.5": 3}}
	base := []byte(`{"mon":{"` + upgradeSrc + `":3},"overall":{"` + upgradeSrc + `":3}}`)
	if err := baseline.observeInventory(base, uniform); err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.satisfied(); err != nil {
		t.Fatal(err)
	}
	probes := 0
	probe := func() (bool, error) { probes++; return false, nil }
	for i := 0; i < 2; i++ {
		if got, err := baseline.capability("cli", "feature", probe); err != nil || got {
			t.Fatalf("capability = %v, %v", got, err)
		}
	}
	if probes != 1 {
		t.Fatalf("unchanged target and phase probed %d times, want 1 reused answer", probes)
	}

	// A target changes inside the phase: the old success and the old answer
	// do not satisfy it.
	if err := targets.advance("mon.a"); err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.satisfied(); !errors.Is(err, errUpgradeStale) {
		t.Fatalf("stale inventory satisfied = %v, want errUpgradeStale", err)
	}
	if err := baseline.observeInventory(base, uniform); !errors.Is(err, errUpgradeStale) {
		t.Fatalf("observation in a stale phase = %v, want errUpgradeStale", err)
	}
	// A different target's answer is about an unchanged target and stays
	// reusable; the changed executor's answer is not.
	if got, err := baseline.capability("cli", "feature", probe); err != nil || got || probes != 1 {
		t.Fatalf("unrelated target capability = %v, %v after %d probes", got, err, probes)
	}
	if _, err := baseline.capability("mon.a", "feature", probe); !errors.Is(err, errUpgradeStale) {
		t.Fatalf("changed target capability error = %v, want errUpgradeStale", err)
	}

	// The next phase starts with nothing inherited and re-probes the changed
	// executor, which now answers differently.
	next := targets.begin("first-mon")
	if _, err := next.satisfied(); err == nil {
		t.Fatal("new phase inherited an inventory")
	}
	if err := next.observeInventory(base, upgradeCheckpoint{"mon": {"19.2.5": 1, "20.2.4": 2}}); err == nil {
		t.Fatal("uniform document satisfied the mixed checkpoint")
	}
	if _, err := next.satisfied(); err == nil {
		t.Fatal("rejected observation was kept")
	}
	if err := targets.advance("cli"); err != nil {
		t.Fatal(err)
	}
	next = targets.begin("destination-cli")
	if got, err := next.capability("cli", "feature", func() (bool, error) { probes++; return true, nil }); err != nil || !got || probes != 2 {
		t.Fatalf("new phase capability = %v, %v after %d probes", got, err, probes)
	}

	// A superseded phase object is closed.
	if _, err := baseline.satisfied(); !errors.Is(err, errUpgradePhaseClosed) {
		t.Fatalf("closed phase satisfied = %v, want errUpgradePhaseClosed", err)
	}
	if _, err := baseline.capability("cli", "feature", probe); !errors.Is(err, errUpgradePhaseClosed) {
		t.Fatalf("closed phase capability = %v, want errUpgradePhaseClosed", err)
	}
}

func TestUpgradeObservationUnavailableAndUnknown(t *testing.T) {
	targets := newUpgradeTargets("cli")
	phase := targets.begin("baseline")
	boom := errors.New("exec unavailable")
	if got, err := phase.capability("cli", "feature", func() (bool, error) { return false, boom }); !errors.Is(err, boom) || got {
		t.Fatalf("probe failure = %v, %v, want the probe error and no answer", got, err)
	}
	probes := 0
	if got, err := phase.capability("cli", "feature", func() (bool, error) { probes++; return true, nil }); err != nil || !got || probes != 1 {
		t.Fatalf("after a failed probe = %v, %v, %d probes; the failure was cached", got, err, probes)
	}
	if _, err := phase.capability("nope", "feature", nil); err == nil || !strings.Contains(err.Error(), "unknown upgrade target") {
		t.Fatalf("unknown target error = %v", err)
	}
	if err := targets.advance("nope"); err == nil {
		t.Fatal("advanced an unknown target")
	}
}
