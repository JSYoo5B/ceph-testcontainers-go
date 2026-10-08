//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

// Compare against independently decoded native JSON at a quiet observation.
// HealthDetails performs three reads, so changes between the surrounding raw
// observations are pending rather than evidence of an atomic health snapshot.
func integrationHealthDetails(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, phase string) ceph.HealthSnapshot {
	t.Helper()
	for {
		before := integrationNativeHealth(t, ctx, cluster, fsid)
		actual, err := cluster.HealthDetails(ctx)
		if err != nil {
			t.Fatal("HealthDetails failed", phase, err)
		}
		after := integrationNativeHealth(t, ctx, cluster, fsid)
		if reflect.DeepEqual(before, after) {
			if !reflect.DeepEqual(actual, after) {
				t.Fatal("HealthDetails differs from stable native JSON", phase)
			}
			codes := make([]string, 0, len(actual.Checks))
			for code := range actual.Checks {
				codes = append(codes, code)
			}
			slices.Sort(codes)
			t.Logf("HEALTH_DETAILS phase=%s fsid=%s status=%s codes=%v mutes=%d native_match=true", phase, fsid, actual.Status, codes, len(actual.Mutes))
			return actual
		}
		select {
		case <-ctx.Done():
			t.Fatal("native health did not stabilize for comparison", phase, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func integrationNativeHealth(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid string) ceph.HealthSnapshot {
	t.Helper()
	data, err := cluster.Ceph(ctx, "health", "detail", "--format", "json")
	if err != nil {
		t.Fatal("native health oracle failed", err)
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
		t.Fatal("native health oracle JSON unavailable", err)
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
	return result
}
