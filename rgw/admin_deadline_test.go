package rgw

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// Admin's budget is its caller's: the cluster's startup timeout bounds the
// typed operations, not a native command such as user stats --sync-stats.
func TestRGWAdminBudgetIsTheCallersContext(t *testing.T) {
	gateway, ctr := newRGWAdminTestGateway(Config{}, rgwAdminResponse{data: "{}"}, rgwAdminResponse{data: "{}"})
	testhelper.TestSetStartupTimeout(gateway.base().Cluster(), 20*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	if _, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats"); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Admin(context.Background(), "user", "list"); err != nil {
		t.Fatal(err)
	}
	if len(ctr.limits) != 2 {
		t.Fatalf("supervised calls: %v", ctr.limits)
	}
	// 8 minutes less the 15 s stop margin, not the 20 s startup timeout.
	if limit, err := strconv.Atoi(ctr.limits[0]); err != nil || limit < 460 || limit > 465 {
		t.Fatalf("an 8-minute caller deadline gave the command %s s", ctr.limits[0])
	}
	if ctr.limits[1] != "0" {
		t.Fatalf("a caller without a deadline gave the command %s s", ctr.limits[1])
	}
}

func TestRGWAdminReportsAStoppedCommand(t *testing.T) {
	gateway, _ := newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 143, data: supervised.ShellDeadlineLine + "\n"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "still ran after") {
		t.Fatalf("a stopped radosgw-admin: %v", err)
	}
	// Another failure stays redacted.
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 22, data: "Error: invalid secret PRIVATE"})
	if _, err := gateway.Admin(ctx, "user", "list"); err == nil || strings.Contains(err.Error(), "still ran after") || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("an ordinary failure: %v", err)
	}
	// A gateway image without radosgw-admin lacks a capability.
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 127, data: "sh: radosgw-admin: not found\n"})
	if _, err := gateway.Admin(ctx, "user", "list"); !errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("a missing radosgw-admin: %v", err)
	}
}
