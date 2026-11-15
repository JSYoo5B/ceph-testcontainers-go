//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// testManagerAlertsDelivery plays a notification-processing SUT that
// receives the alerts module's mail at an SMTP receiver the test owns, with
// only existing primitives: override.SetConfig for each mgr/alerts option,
// override.SetMGRModule, override.SetOSDFlag for an independent health
// change and Observer.HealthDetails for the module's own failure check. It
// qualifies what the module delivers, not a mail system:
//   - the first pass after the module starts reports the existing health
//     checks as New, so a SUT sees the baseline once more on every enable
//     and on every failover;
//   - ceph alerts send is a transport control that reports no change;
//   - the module sends on a change of the set of checks (or a higher count),
//     never periodically, and has no retry: a change made while the
//     receiver is unavailable is lost, ALERTS_SMTP_ERROR is the only native
//     signal, and the next change arrives as a Cleared without its New;
//   - an authentication refusal is the same error, and restoring the
//     credentials through their handles stops the module from logging in;
//   - after a failover the new active manager sends from its own address;
//   - a comma-separated smtp_destination is one envelope recipient per
//     address where the release splits it, and one recipient carrying the
//     whole list where it does not (CapabilityAlertsRecipientList).
func testManagerAlertsDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := newMGRModulesCluster(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2)))
	observer := harness.ObserveCluster(cluster)
	managers := map[string]string{}
	for _, manager := range clusteraccess.Managers(t, cluster) {
		address, err := manager.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		managers[address] = manager.DaemonName
	}
	health := func() ceph.HealthSnapshot {
		t.Helper()
		snapshot, err := observer.HealthDetails(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	baseline := health()
	baselineCodes := slices.Sorted(maps.Keys(baseline.Checks))
	entries, err := observer.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, "mgr/alerts/") {
			t.Fatalf("fresh cluster stores %+v", entry)
		}
	}

	// Registered first, so it is terminated after the module is restored.
	const port = "2525"
	sink := runModuleReceiver(t, ctx, cluster, "smtp-sink", smtpSinkScript, port)
	messages := func() ([]smtpRecord, []alertMessage) {
		t.Helper()
		var records []smtpRecord
		var parsed []alertMessage
		for _, record := range moduleReceiverRecords[smtpRecord](t, ctx, sink) {
			if record.Event != "message" {
				continue
			}
			message, err := parseAlertMessage(record.Data)
			if err != nil {
				t.Fatalf("message from %s: %v", record.Peer, err)
			}
			records, parsed = append(records, record), append(parsed, message)
		}
		return records, parsed
	}
	// awaitMessage waits for a message after the first seen ones that
	// accept takes, and returns its index.
	awaitMessage := func(what string, seen int, limit time.Duration, accept func(smtpRecord, alertMessage) bool) int {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			records, parsed := messages()
			for i := seen; i < len(parsed); i++ {
				if accept(records[i], parsed[i]) {
					return i
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: no matching message within %v; messages after %d: %+v", what, limit, seen, parsed[min(seen, len(parsed)):])
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	// quiet requires that no message arrives for d. moduleReceiverRecords
	// already fails on a history shorter than an earlier one; the check here
	// still reports one without slicing past its end.
	quiet := func(what string, d time.Duration) int {
		t.Helper()
		_, before := messages()
		time.Sleep(d)
		switch _, after := messages(); {
		case len(after) < len(before):
			t.Fatalf("%s: history shrank from %d messages to %d in %v", what, len(before), len(after), d)
		case len(after) > len(before):
			t.Fatalf("%s: %d messages arrived in %v: %+v", what, len(after)-len(before), d, after[len(before):])
		}
		return len(before)
	}
	awaitCheck := func(what, code string, present bool, limit time.Duration) ceph.HealthCheck {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			check, found := health().Checks[code]
			if found == present {
				return check
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %s present %t after %v", what, code, found, limit)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	var restores []func(context.Context) error
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for i := len(restores) - 1; i >= 0; i-- {
			if err := restores[i](cleanup); err != nil {
				t.Errorf("restore alerts setting: %v", err)
			}
		}
	})
	set := func(name, value string) *override.ConfigHandle {
		t.Helper()
		change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "mgr/alerts/" + name, Value: value})
		restores = append(restores, change.Restore)
		if err != nil {
			t.Fatal(name, err)
		}
		return change
	}
	flag := func() *override.OSDFlagHandle {
		t.Helper()
		change, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
		restores = append(restores, change.Restore)
		if err != nil {
			t.Fatal(err)
		}
		return change
	}
	// The cluster's fsid is not in the module's message; the SUT gets it
	// only from an identity the test chooses, here the From name.
	fromName := "Ceph " + baseline.FSID
	recipients := []string{"<sut-a@fixture.test>", "<sut-b@fixture.test>"}
	if !harness.CapabilitySupported(t, ctx, cluster, harness.CapabilityAlertsRecipientList) {
		recipients = []string{"<sut-a@fixture.test,sut-b@fixture.test>"}
	}
	for _, setting := range [][2]string{
		{"smtp_host", sink.Alias},
		{"smtp_port", port},
		{"smtp_ssl", "false"},
		{"smtp_sender", "ceph-alerts@fixture.test"},
		{"smtp_destination", "sut-a@fixture.test,sut-b@fixture.test"},
		{"smtp_from_name", fromName},
		{"interval", "5"},
	} {
		set(setting[0], setting[1])
	}
	enabled := time.Now()
	module, err := override.SetMGRModule(ctx, cluster, "alerts", true)
	restores = append(restores, module.Restore)
	if err != nil {
		t.Fatal("enable alerts", err)
	}
	active := activeManager(t, ctx, cluster)
	fromActive := func(name string) func(smtpRecord, alertMessage) bool {
		return func(record smtpRecord, _ alertMessage) bool { return managers[record.Peer] == name }
	}

	// The first pass reports the baseline checks as New. It is not an event
	// the test caused.
	seen := 0
	if len(baselineCodes) > 0 {
		first := awaitMessage("enable-time message", 0, time.Minute, func(smtpRecord, alertMessage) bool { return true })
		records, parsed := messages()
		if !slices.Equal(parsed[first].Codes(alertsNew), baselineCodes) || parsed[first].Codes(alertsCleared) != nil || managers[records[first].Peer] != active {
			t.Fatalf("enable-time message from %s: new %v cleared %v, want new %v from mgr.%s", records[first].Peer, parsed[first].Codes(alertsNew), parsed[first].Codes(alertsCleared), baselineCodes, active)
		}
		if records[first].From != "<ceph-alerts@fixture.test>" || !slices.Equal(records[first].To, recipients) || parsed[first].Headers["From"] != fromName+" <ceph-alerts@fixture.test>" {
			t.Fatalf("envelope %s -> %v, From %q; want <ceph-alerts@fixture.test> -> %v", records[first].From, records[first].To, parsed[first].Headers["From"], recipients)
		}
		seen = first + 1
		t.Logf("MGR_ALERTS enable-time message %v after enabling: new %v", time.Unix(0, int64(records[first].Time*1e9)).Sub(enabled).Round(time.Millisecond), baselineCodes)
	}

	// Transport control: a forced send reports no change.
	harness.MustCeph(t, ctx, cluster, "alerts", "send")
	forced := awaitMessage("forced send", seen, 30*time.Second, func(_ smtpRecord, m alertMessage) bool { return true })
	records, parsed := messages()
	if parsed[forced].Change() || parsed[forced].Status != health().Status {
		t.Fatalf("forced send %+v, want no change and status %s", parsed[forced], health().Status)
	}
	// The envelope is checked here too, since a cluster without baseline
	// checks sends no enable-time message.
	if !slices.Equal(records[forced].To, recipients) {
		t.Fatalf("forced send envelope -> %v, want %v", records[forced].To, recipients)
	}
	t.Logf("MGR_ALERTS smtp_destination %q: envelope recipients %v, To header %q", "sut-a@fixture.test,sut-b@fixture.test", records[forced].To, parsed[forced].Headers["To"])
	seen = forced + 1

	// An independent health change and its clearance.
	noout := flag()
	changed := time.Now()
	added := awaitMessage("new OSDMAP_FLAGS", seen, time.Minute, func(_ smtpRecord, m alertMessage) bool {
		return slices.Contains(m.Codes(alertsNew), "OSDMAP_FLAGS")
	})
	addedAfter := time.Since(changed)
	seen = quiet("unchanged health", 15*time.Second)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	changed = time.Now()
	cleared := awaitMessage("cleared OSDMAP_FLAGS", seen, time.Minute, func(_ smtpRecord, m alertMessage) bool {
		return slices.Contains(m.Codes(alertsCleared), "OSDMAP_FLAGS")
	})
	t.Logf("MGR_ALERTS noout: new %v after the set (message %d), cleared %v after the restore (message %d), no message for 15s between",
		addedAfter.Round(time.Millisecond), added, time.Since(changed).Round(time.Millisecond), cleared)

	// Receiver unavailable: the change is lost and only the module's health
	// check reports it. Two intervals with the receiver down let the module
	// finish its own reaction to that check, so the receiver's return is not
	// raced by a send.
	sink.SetAvailable(t, ctx, false)
	lost := flag()
	failure := awaitCheck("receiver down", "ALERTS_SMTP_ERROR", true, time.Minute)
	if !strings.Contains(strings.Join(failure.Details, " "), "Connection refused") {
		t.Fatalf("ALERTS_SMTP_ERROR details %v, want a refused connection", failure.Details)
	}
	time.Sleep(12 * time.Second)
	sink.SetAvailable(t, ctx, true)
	seen = quiet("receiver back, no retry", 15*time.Second)
	if err := lost.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	orphan := awaitMessage("cleared OSDMAP_FLAGS after the outage", seen, time.Minute, func(_ smtpRecord, m alertMessage) bool {
		return slices.Contains(m.Codes(alertsCleared), "OSDMAP_FLAGS")
	})
	_, parsed = messages()
	for _, message := range parsed[seen:orphan] {
		if slices.Contains(message.Codes(alertsNew), "OSDMAP_FLAGS") {
			t.Fatalf("the New of the change made during the outage arrived: %+v", message)
		}
	}
	recovered := awaitMessage("cleared ALERTS_SMTP_ERROR", orphan, time.Minute, func(_ smtpRecord, m alertMessage) bool {
		return slices.Contains(m.Codes(alertsCleared), "ALERTS_SMTP_ERROR")
	})
	awaitCheck("receiver back", "ALERTS_SMTP_ERROR", false, time.Minute)
	t.Logf("MGR_ALERTS outage: %s %v; lost New OSDMAP_FLAGS, then Cleared OSDMAP_FLAGS (message %d) and Cleared ALERTS_SMTP_ERROR (message %d)", failure.Summary, failure.Details, orphan, recovered)

	// Authentication refused, then the credentials restored through their
	// handles: the module must stop logging in, which a removal of the stored
	// options alone does not achieve in a running MGR.
	harness.ExecCommand(t, ctx, sink, "sh", "-c", "echo deny-auth > "+moduleReceiverDir+"/mode")
	user := set("smtp_user", "sut")
	password := set("smtp_password", "not-a-real-secret")
	harness.MustCeph(t, ctx, cluster, "alerts", "send")
	denied := awaitCheck("authentication refused", "ALERTS_SMTP_ERROR", true, time.Minute)
	if !strings.Contains(strings.Join(denied.Details, " "), "535") {
		t.Fatalf("ALERTS_SMTP_ERROR details %v, want the 535 refusal", denied.Details)
	}
	var mechanisms []string
	for _, record := range moduleReceiverRecords[smtpRecord](t, ctx, sink) {
		if record.Event == "auth" {
			mechanisms = append(mechanisms, record.Mechanism)
		}
	}
	if len(mechanisms) == 0 {
		t.Fatal("the receiver saw no AUTH attempt")
	}
	harness.ExecCommand(t, ctx, sink, "sh", "-c", "echo accept > "+moduleReceiverDir+"/mode")
	if err := password.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := user.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	_, parsed = messages()
	seen = len(parsed)
	harness.MustCeph(t, ctx, cluster, "alerts", "send")
	delivered := awaitMessage("forced send after the credentials were restored", seen, time.Minute, func(_ smtpRecord, m alertMessage) bool { return !m.Change() })
	awaitCheck("credentials restored", "ALERTS_SMTP_ERROR", false, time.Minute)
	t.Logf("MGR_ALERTS authentication: %v refused with %v; after the restore message %d arrived without AUTH", mechanisms, denied.Details, delivered)

	// Failover: the new active manager's first pass reports every current
	// check as New again, from its own address. A change made right after
	// the failover is in that pass or in a later one, not lost.
	records, _ = messages()
	seen = len(records)
	standby := ""
	for _, name := range managers {
		if name != active {
			standby = name
		}
	}
	failed := time.Now()
	harness.MustCeph(t, ctx, cluster, "mgr", "fail", active)
	pending := flag()
	rebaseline := awaitMessage("first message of the promoted manager", seen, 2*time.Minute, fromActive(standby))
	records, parsed = messages()
	for i := seen; i < rebaseline; i++ {
		if managers[records[i].Peer] == active {
			t.Logf("MGR_ALERTS message %d from the failed manager after mgr fail: %+v", i, parsed[i])
		}
	}
	if !parsed[rebaseline].Change() || len(parsed[rebaseline].Codes(alertsCleared)) != 0 {
		t.Fatalf("first message of mgr.%s %+v, want only New checks", standby, parsed[rebaseline])
	}
	for _, code := range baselineCodes {
		if !slices.Contains(parsed[rebaseline].Codes(alertsNew), code) {
			t.Fatalf("first message of mgr.%s reports new %v, want the baseline %v again", standby, parsed[rebaseline].Codes(alertsNew), baselineCodes)
		}
	}
	carried := awaitMessage("the change made after the failover", rebaseline, time.Minute, func(record smtpRecord, m alertMessage) bool {
		return managers[record.Peer] == standby && slices.Contains(m.Codes(alertsNew), "OSDMAP_FLAGS")
	})
	t.Logf("MGR_ALERTS failover: mgr.%s sent message %d with new %v %v after mgr fail was sent; OSDMAP_FLAGS set right after the failover is new in message %d",
		standby, rebaseline, parsed[rebaseline].Codes(alertsNew), time.Unix(0, int64(records[rebaseline].Time*1e9)).Sub(failed).Round(time.Millisecond), carried)
	if err := pending.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	awaitMessage("cleared OSDMAP_FLAGS from the promoted manager", carried, time.Minute, func(record smtpRecord, m alertMessage) bool {
		return managers[record.Peer] == standby && slices.Contains(m.Codes(alertsCleared), "OSDMAP_FLAGS")
	})

	for i := len(restores) - 1; i >= 0; i-- {
		if err := restores[i](ctx); err != nil {
			t.Fatal("restore", err)
		}
	}
	restores = nil
	entries, err = observer.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, "mgr/alerts/") {
			t.Fatalf("restoration left %+v", entry)
		}
	}
	modules, err := observer.MGRModules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range modules {
		if module.Name == "alerts" && module.Enabled {
			t.Fatal("alerts stays enabled after the restoration")
		}
	}
	deadline := time.Now().Add(time.Minute)
	for {
		codes := slices.Sorted(maps.Keys(health().Checks))
		if slices.Equal(codes, baselineCodes) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("health checks after the restoration %v, want the baseline %v", codes, baselineCodes)
		}
		time.Sleep(time.Second)
	}
}
