//go:build all || integration

package harness

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

// ReadNotificationEvents returns the events that the receiver in client has
// logged so far, one per line. The receiver must have created the log;
// a missing or unreadable log cannot establish an empty event history.
func ReadNotificationEvents(ctx context.Context, client testcontainers.Container) ([]string, error) {
	code, reader, err := client.Exec(ctx, []string{"cat", "/tmp/tc-events.log"}, tcexec.Multiplexed())
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

// ObserveNotificationEvents waits, for at most bound, until the receiver in
// client has logged every event of want. Each read of the log is bounded
// within that phase, and a read that returns after it is not accepted.
func ObserveNotificationEvents(ctx context.Context, client testcontainers.Container, want []string, bound time.Duration) (Observation[[]string], error) {
	return Observe(ctx, bound, min(bound, 30*time.Second), time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		events, err := ReadNotificationEvents(probeCtx, client)
		if err != nil {
			return nil, false, err
		}
		return events, len(MissingNotificationEvents(want, events)) == 0, nil
	})
}

// MissingNotificationEvents returns the events of want that events lacks.
func MissingNotificationEvents(want, events []string) []string {
	return slices.DeleteFunc(slices.Clone(want), func(event string) bool { return slices.Contains(events, event) })
}
