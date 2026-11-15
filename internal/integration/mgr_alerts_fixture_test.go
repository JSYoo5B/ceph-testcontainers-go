//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// alertsChangeMessage is the data of an alerts module message for a new
// noout flag on a cluster whose .mgr pool has one replica, in the layout
// _send_alert_smtp writes.
const alertsChangeMessage = `From: Ceph 5f1c <ceph-alerts@fixture.test>
Subject: HEALTH_WARN
To: sut-a@fixture.test,sut-b@fixture.test
Message-Id: <170000000000.1.1@mgr-a>
Date: Tue, 14 Nov 2026 10:00:00 -0000

HEALTH_WARN

--- New ---
[WARN] OSDMAP_FLAGS: noout flag(s) set


=== Full health status ===
[WARN] OSDMAP_FLAGS: noout flag(s) set
[WARN] POOL_NO_REDUNDANCY: 1 pool(s) have no replicas configured
        pool '.mgr' has no replicas configured
`

func TestAlertMessageSections(t *testing.T) {
	message, err := parseAlertMessage(alertsChangeMessage)
	if err != nil {
		t.Fatal(err)
	}
	if message.Status != "HEALTH_WARN" || message.Headers["From"] != "Ceph 5f1c <ceph-alerts@fixture.test>" || !message.Change() {
		t.Fatalf("parsed %+v", message)
	}
	if got := message.Codes(alertsNew); !slices.Equal(got, []string{"OSDMAP_FLAGS"}) {
		t.Fatalf("new codes %v", got)
	}
	if got := message.Codes(alertsCleared); got != nil {
		t.Fatalf("cleared codes %v", got)
	}
	full := message.Sections[alertsFull]
	if len(full) != 2 || full[1].Code != "POOL_NO_REDUNDANCY" || full[1].Severity != "WARN" || !slices.Equal(full[1].Details, []string{"pool '.mgr' has no replicas configured"}) {
		t.Fatalf("full status %+v", full)
	}

	// A forced send (ceph alerts send) reports no change.
	forced, err := parseAlertMessage(strings.Replace(alertsChangeMessage, "--- New ---\n[WARN] OSDMAP_FLAGS: noout flag(s) set\n", "", 1))
	if err != nil || forced.Change() {
		t.Fatalf("forced send %+v %v", forced, err)
	}
	cleared, err := parseAlertMessage(strings.Replace(alertsChangeMessage, "--- New ---", "--- Cleared ---", 1))
	if err != nil || !slices.Equal(cleared.Codes(alertsCleared), []string{"OSDMAP_FLAGS"}) || cleared.Codes(alertsNew) != nil {
		t.Fatalf("cleared %+v %v", cleared, err)
	}

	for name, data := range map[string]string{
		"no header end":        "Subject: HEALTH_OK",
		"subject differs":      strings.Replace(alertsChangeMessage, "Subject: HEALTH_WARN", "Subject: HEALTH_OK", 1),
		"unknown section":      strings.Replace(alertsChangeMessage, "--- New ---", "--- Fresh ---", 1),
		"entry without code":   strings.Replace(alertsChangeMessage, "[WARN] OSDMAP_FLAGS: noout", "[WARN] noout", 1),
		"no full status":       alertsChangeMessage[:strings.Index(alertsChangeMessage, "\n\n=== Full")],
		"repeated section":     alertsChangeMessage + "\n--- New ---\n[WARN] OSD_DOWN: 1 osds down\n",
		"orphan detail line":   strings.Replace(alertsChangeMessage, "--- New ---\n[WARN] OSDMAP_FLAGS: noout flag(s) set", "--- New ---\n        detail", 1),
		"malformed header":     strings.Replace(alertsChangeMessage, "To: ", "To ", 1),
		"text before sections": strings.Replace(alertsChangeMessage, "HEALTH_WARN\n\n--- New", "HEALTH_WARN\nextra\n--- New", 1),
	} {
		if message, err := parseAlertMessage(data); err == nil {
			t.Errorf("%s: parsed as %+v", name, message)
		}
	}
}
