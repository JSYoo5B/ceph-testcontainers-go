//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// readNotificationEvents returns the events that the receiver in client has
// logged so far, one per line.
func readNotificationEvents(ctx context.Context, client testcontainers.Container) ([]string, error) {
	code, reader, err := client.Exec(ctx, []string{"sh", "-c", "cat /tmp/tc-events.log 2>/dev/null || true"}, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("read notification events: %w", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read notification events: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("read notification events: exit %d: %s", code, out)
	}
	if output := strings.TrimSpace(string(out)); output != "" {
		return strings.Split(output, "\n"), nil
	}
	return nil, nil
}

// observeNotificationEvents waits, for at most bound, until the receiver in
// client has logged every event of want. Each read of the log is bounded
// within that phase, and a read that returns after it is not accepted.
func observeNotificationEvents(ctx context.Context, client testcontainers.Container, want []string, bound time.Duration) (observation[[]string], error) {
	return observe(ctx, bound, min(bound, 30*time.Second), time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		events, err := readNotificationEvents(probeCtx, client)
		if err != nil {
			return nil, false, err
		}
		return events, len(missingNotificationEvents(want, events)) == 0, nil
	})
}

// missingNotificationEvents returns the events of want that events lacks.
func missingNotificationEvents(want, events []string) []string {
	return slices.DeleteFunc(slices.Clone(want), func(event string) bool { return slices.Contains(events, event) })
}
