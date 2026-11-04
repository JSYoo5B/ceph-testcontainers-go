//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Compare against independently decoded native JSON read just before and just
// after the API call. HealthDetails performs three reads, so it is not an
// atomic health snapshot. When both native reads agree, the API result must
// equal them. Some checks are never quiet: the SLOW_OPS summary carries the age
// of the oldest blocked operation, which advances every couple of
// seconds, so a slow comparison may never see two equal reads while it is
// raised. Then the API result must equal one of the two native reads that
// bracket it, which it does when the summary changed at most once in between;
// any other result is retried, not accepted.
func integrationHealthDetails(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, phase string) ceph.HealthSnapshot {
	t.Helper()
	compared, changed := 0, 0
	var lastCodes []string
	for {
		before, err := integrationNativeHealth(ctx, cluster, fsid)
		if err != nil {
			t.Fatalf("native health oracle failed: phase=%s read=before comparisons=%d changed=%d last_codes=%v context=%v: %v", phase, compared, changed, lastCodes, ctx.Err(), err)
		}
		actual, err := harness.ObserveCluster(cluster).HealthDetails(ctx)
		if err != nil {
			t.Fatalf("HealthDetails failed: phase=%s comparisons=%d changed=%d context=%v: %v", phase, compared, changed, ctx.Err(), err)
		}
		after, err := integrationNativeHealth(ctx, cluster, fsid)
		if err != nil {
			t.Fatalf("native health oracle failed: phase=%s read=after comparisons=%d changed=%d last_codes=%v context=%v: %v", phase, compared, changed, lastCodes, ctx.Err(), err)
		}
		compared++
		lastCodes = healthCodes(after)
		match := ""
		switch {
		case reflect.DeepEqual(before, after):
			if !reflect.DeepEqual(actual, after) {
				t.Fatal("HealthDetails differs from stable native JSON", phase)
			}
			match = "stable"
		case reflect.DeepEqual(actual, before):
			match = "before"
		case reflect.DeepEqual(actual, after):
			match = "after"
		default:
			changed++
		}
		if match != "" {
			t.Logf("HEALTH_DETAILS phase=%s fsid=%s status=%s codes=%v mutes=%d native_match=%s comparisons=%d", phase, fsid, actual.Status, healthCodes(actual), len(actual.Mutes), match, compared)
			return actual
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native health did not match HealthDetails: phase=%s comparisons=%d changed=%d last_codes=%v: %v", phase, compared, changed, lastCodes, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func healthCodes(snapshot ceph.HealthSnapshot) []string {
	codes := make([]string, 0, len(snapshot.Checks))
	for code := range snapshot.Checks {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes
}

// integrationNativeHealth decodes one native health read. Its errors carry no
// native output.
func integrationNativeHealth(ctx context.Context, cluster *ceph.Container, fsid string) (ceph.HealthSnapshot, error) {
	data, err := cluster.Ceph(ctx, "health", "detail", "--format", "json")
	if err != nil {
		return ceph.HealthSnapshot{}, err
	}
	var native struct {
		Status string `json:"status"`
		Checks map[string]struct {
			Severity string `json:"severity"`
			Summary  struct {
				Message string `json:"message"`
				Count   int64  `json:"count"`
			} `json:"summary"`
			Detail []struct {
				Message string `json:"message"`
			} `json:"detail"`
			Muted bool `json:"muted"`
		} `json:"checks"`
		Mutes []struct {
			Code    string `json:"code"`
			Summary string `json:"summary"`
			Count   int64  `json:"count"`
			Sticky  bool   `json:"sticky"`
			TTL     string `json:"ttl"`
		} `json:"mutes"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Checks == nil || native.Mutes == nil || native.Status == "" {
		return ceph.HealthSnapshot{}, fmt.Errorf("native health JSON unavailable: %v", err)
	}
	result := ceph.HealthSnapshot{FSID: fsid, Status: native.Status,
		Checks: make(map[string]ceph.HealthCheck, len(native.Checks)), Mutes: make([]ceph.HealthMute, 0, len(native.Mutes))}
	for code, check := range native.Checks {
		details := make([]string, 0, len(check.Detail))
		for _, detail := range check.Detail {
			details = append(details, detail.Message)
		}
		result.Checks[code] = ceph.HealthCheck{Severity: check.Severity, Summary: check.Summary.Message,
			Count: check.Summary.Count, Details: details, Muted: check.Muted}
	}
	for _, mute := range native.Mutes {
		result.Mutes = append(result.Mutes, ceph.HealthMute{Code: mute.Code, Summary: mute.Summary,
			Count: mute.Count, Sticky: mute.Sticky, ExpiresAt: mute.TTL})
	}
	return result, nil
}
