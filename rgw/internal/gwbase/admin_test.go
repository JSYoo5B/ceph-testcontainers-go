package gwbase

import (
	"context"
	"testing"
	"time"
)

func TestRGWAdminLimit(t *testing.T) {
	if limit := adminLimit(context.Background()); limit != 0 {
		t.Fatalf("no deadline: %d", limit)
	}
	for bound, want := range map[time.Duration][2]int{3 * time.Minute: {164, 165}, 20 * time.Second: {9, 10}, time.Second: {1, 1}} {
		ctx, cancel := context.WithTimeout(t.Context(), bound)
		limit := adminLimit(ctx)
		cancel()
		if limit < want[0] || limit > want[1] {
			t.Errorf("bound %s: limit %d", bound, limit)
		}
	}
}
