//go:build all || integration

package integration_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// runnerStorageAlert is the health check that a runner's storage, not the
// fixture, raises. Ceph raises BLUESTORE_SLOW_OP_ALERT after one BlueStore
// operation exceeds bluestore_log_op_age (5 s) and keeps it for
// bluestore_slow_ops_warn_lifetime (86400 s): one storage stall of the runner,
// such as a 12.8 s kv commit, leaves the cluster in HEALTH_WARN for the rest
// of the fixture, whatever the fixture does.
const runnerStorageAlert = "BLUESTORE_SLOW_OP_ALERT"

// bluestoreSlowOpDetail is the only detail that the alert may carry to be
// treated as runner evidence.
var bluestoreSlowOpDetail = regexp.MustCompile(`^\s*osd\.\d+ observed slow operation indications in BlueStore$`)

// isRunnerStorageAlert reports whether a check is the runner's storage alert,
// with only slow-operation details.
func isRunnerStorageAlert(code string, details []string) bool {
	if code != runnerStorageAlert || len(details) == 0 {
		return false
	}
	for _, detail := range details {
		if !bluestoreSlowOpDetail.MatchString(detail) {
			return false
		}
	}
	return true
}

var healthSeverities = map[string]int{"HEALTH_OK": 0, "HEALTH_WARN": 1, "HEALTH_ERR": 2}

// withoutRunnerStorageAlert returns raw `ceph health detail --format json`
// output without the runner's storage alert and with the status of its other
// checks, so that an exact health guard still judges every other check. It
// logs the alert, never drops it silently, and returns data unchanged when
// the alert is absent or carries anything else.
func withoutRunnerStorageAlert(t *testing.T, data []byte) []byte {
	t.Helper()
	var health map[string]json.RawMessage
	var checks map[string]json.RawMessage
	if json.Unmarshal(data, &health) != nil || json.Unmarshal(health["checks"], &checks) != nil {
		return data
	}
	raw, present := checks[runnerStorageAlert]
	if !present {
		return data
	}
	var alert struct {
		Detail []struct {
			Message string `json:"message"`
		} `json:"detail"`
	}
	if json.Unmarshal(raw, &alert) != nil {
		return data
	}
	details := make([]string, len(alert.Detail))
	for i, detail := range alert.Detail {
		details[i] = detail.Message
	}
	if !isRunnerStorageAlert(runnerStorageAlert, details) {
		return data
	}
	delete(checks, runnerStorageAlert)
	status := "HEALTH_OK"
	for _, check := range checks {
		var native struct {
			Severity string `json:"severity"`
			Muted    bool   `json:"muted"`
		}
		if json.Unmarshal(check, &native) != nil {
			return data
		}
		// Ceph leaves muted checks out of the status.
		if !native.Muted && healthSeverities[native.Severity] > healthSeverities[status] {
			status = native.Severity
		}
	}
	t.Logf("RUNNER_STORAGE_ALERT %s excluded from the exact health guard: %s", runnerStorageAlert, strings.Join(details, "; "))
	health["checks"], _ = json.Marshal(checks)
	health["status"], _ = json.Marshal(status)
	filtered, err := json.Marshal(health)
	if err != nil {
		return data
	}
	return filtered
}

// snapshotWithoutRunnerStorageAlert is withoutRunnerStorageAlert for a
// HealthDetails snapshot.
func snapshotWithoutRunnerStorageAlert(t *testing.T, snapshot ceph.HealthSnapshot) ceph.HealthSnapshot {
	t.Helper()
	alert, present := snapshot.Checks[runnerStorageAlert]
	if !present || alert.Muted || !isRunnerStorageAlert(runnerStorageAlert, alert.Details) {
		return snapshot
	}
	checks := make(map[string]ceph.HealthCheck, len(snapshot.Checks))
	status := "HEALTH_OK"
	for code, check := range snapshot.Checks {
		if code == runnerStorageAlert {
			continue
		}
		checks[code] = check
		if !check.Muted && healthSeverities[check.Severity] > healthSeverities[status] {
			status = check.Severity
		}
	}
	t.Logf("RUNNER_STORAGE_ALERT %s excluded from the exact health guard: %s", runnerStorageAlert, strings.Join(alert.Details, "; "))
	snapshot.Checks, snapshot.Status = checks, status
	return snapshot
}
