package placement

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
)

func TestStoppedApplyRechecksCreationDocumentsAndPoolIDsBeforeStart(t *testing.T) {
	for _, change := range []string{"zonegroup", "zone", "replaced pool"} {
		t.Run(change, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			ctr := &interruptedGateway{PlacementContainer: f, failStart: true, started: "original"}
			g.Replace(ctr)
			p, err := create(t.Context(), g.Core, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := apply(t.Context(), g.Core, p); err == nil || !ctr.stopped {
				t.Fatalf("first restart: %v, stopped=%t", err, ctr.stopped)
			}
			group, zone, poolID := gwtest.Clone(f.Group), gwtest.Clone(f.Zone), f.PoolID
			switch change {
			case "zonegroup":
				f.Group["default_placement"] = "foreign"
			case "zone":
				f.Zone["outside"] = "preserve"
			case "replaced pool":
				f.PoolID++
			}
			// Reload cannot weaken the strict checks of the interrupted Apply.
			copyHandle := *p
			if err := reload(t.Context(), g.Core, &copyHandle); err == nil || !ctr.stopped || ctr.StartCount != 0 {
				t.Fatalf("resumed after %s drift: %v, starts=%d", change, err, ctr.StartCount)
			}
			f.Group, f.Zone, f.PoolID = group, zone, poolID
			if err := reload(t.Context(), g.Core, &copyHandle); err != nil || ctr.stopped || ctr.StartCount != 1 || ctr.StopCount != 1 {
				t.Fatalf("retry after restoring the creation policy: %v, starts=%d stops=%d", err, ctr.StartCount, ctr.StopCount)
			}
		})
	}
}

func TestStoppedRealmReloadRequiresPublishedTargetBeforeStart(t *testing.T) {
	g, f := realmPlacementFixture()
	ctr := &interruptedGateway{PlacementContainer: f, failStart: true, started: "original"}
	g.Replace(ctr)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, p); err != nil {
		t.Fatal(err)
	}
	if err := reload(t.Context(), g.Core, p); err == nil || !ctr.stopped {
		t.Fatalf("first reload: %v, stopped=%t", err, ctr.stopped)
	}
	published := gwtest.Clone(f.CurrentPeriod)
	f.CurrentPeriod["period_map"].(map[string]any)["zonegroups"] = []any{}
	if err := apply(t.Context(), g.Core, p); err == nil || !ctr.stopped || ctr.StartCount != 0 {
		t.Fatalf("started gateway after publication disappeared: %v, starts=%d", err, ctr.StartCount)
	}
	f.CurrentPeriod = published
	f.Group["unrelated"] = "allowed by Reload"
	if err := reload(t.Context(), g.Core, p); err != nil || ctr.stopped || ctr.StartCount != 1 {
		t.Fatalf("valid reload blocked: %v, starts=%d", err, ctr.StartCount)
	}
}

func TestGatewayStopFencesOtherPlacementsUntilGenerationChanges(t *testing.T) {
	g, f := newRGWPlacementFixture()
	ctr := &interruptedGateway{PlacementContainer: f, loseStopReply: true, started: "original"}
	g.Replace(ctr)
	a, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, a); err == nil {
		t.Fatal("lost Stop was accepted")
	}
	config := testConfig()
	config.Name = "second"
	b, err := create(t.Context(), g.Core, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, b); err == nil || ctr.StopCount != 0 || ctr.StartCount != 0 {
		t.Fatalf("second placement bypassed unconfirmed Stop: %v", err)
	}
	ctr.stopped = true
	if err := apply(t.Context(), g.Core, b); err == nil || !ctr.stopped || ctr.StartCount != 0 {
		t.Fatalf("second placement resumed the first placement's stop: %v", err)
	}
	if err := apply(t.Context(), g.Core, a); err == nil || !ctr.stopped || ctr.StartCount != 0 {
		t.Fatalf("first placement ignored the second placement's native edits: %v", err)
	}
	ctr.stopped, ctr.started = false, "externally reconciled"
	if err := apply(t.Context(), g.Core, b); err != nil || ctr.StartCount != 1 || ctr.StopCount != 1 {
		t.Fatalf("second placement failed fresh activation: %v", err)
	}
}

func TestRecoveredGatewayRetriesOnlyPostStartVerification(t *testing.T) {
	for _, followup := range []string{"same handle", "other handle", "external restart", "external stop"} {
		t.Run(followup, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			ctr := &interruptedGateway{PlacementContainer: f, loseCompletedStop: true, started: "original", losePostStartHostname: true}
			g.Replace(ctr)
			a, err := create(t.Context(), g.Core, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := apply(t.Context(), g.Core, a); err == nil || ctr.stopped || ctr.StartCount != 1 {
				t.Fatalf("lost post-start read: %v, stopped=%t starts=%d", err, ctr.stopped, ctr.StartCount)
			}
			switch followup {
			case "same handle":
				if err := apply(t.Context(), g.Core, a); err != nil || ctr.StartCount != 1 || ctr.StopCount != 1 {
					t.Fatalf("retry cycled a healthy gateway: %v, starts=%d stops=%d", err, ctr.StartCount, ctr.StopCount)
				}
			case "other handle":
				config := testConfig()
				config.Name = "second"
				b, err := create(t.Context(), g.Core, config)
				if err != nil {
					t.Fatal(err)
				}
				if err := apply(t.Context(), g.Core, b); err != nil || ctr.StartCount != 2 || ctr.StopCount != 2 {
					t.Fatalf("second placement borrowed earlier verification: %v, starts=%d", err, ctr.StartCount)
				}
				if err := apply(t.Context(), g.Core, a); err == nil || ctr.StartCount != 2 {
					t.Fatalf("first placement claimed the second's activation: %v", err)
				}
			case "external restart":
				ctr.started = "external"
				if err := apply(t.Context(), g.Core, a); err == nil || ctr.StartCount != 1 {
					t.Fatalf("accepted unrelated generation as earlier activation: %v", err)
				}
				if err := apply(t.Context(), g.Core, a); err != nil || ctr.StartCount != 2 {
					t.Fatalf("fresh activation after replacement: %v", err)
				}
			case "external stop":
				ctr.stopped = true
				if err := apply(t.Context(), g.Core, a); err == nil || !ctr.stopped || ctr.StartCount != 1 {
					t.Fatalf("resumed externally stopped generation: %v", err)
				}
			}
		})
	}
}
