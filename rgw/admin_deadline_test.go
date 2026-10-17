package rgw

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

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

func TestRGWAdminLimit(t *testing.T) {
	if limit := rgwAdminLimit(context.Background()); limit != 0 {
		t.Fatalf("no deadline: %d", limit)
	}
	for bound, want := range map[time.Duration][2]int{3 * time.Minute: {164, 165}, 20 * time.Second: {9, 10}, time.Second: {1, 1}} {
		ctx, cancel := context.WithTimeout(t.Context(), bound)
		limit := rgwAdminLimit(ctx)
		cancel()
		if limit < want[0] || limit > want[1] {
			t.Errorf("bound %s: limit %d", bound, limit)
		}
	}
}
