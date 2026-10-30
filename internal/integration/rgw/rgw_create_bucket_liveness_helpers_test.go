//go:build all || integration

package rgw_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// rgwGatewayTimeline records RFC 3339 checkpoints of gateway container state
// around a reload, so a failing log can be compared with daemon log lines
// without matching testcontainers lines by hand. It observes only: an inspect
// failure is recorded as a failed field and never as a healthy state.
//
// The Docker times in a checkpoint are the container's most recent start and
// most recent finish; a stop/start cycle between two checkpoints shows as a
// changed started= value. They are not the daemon's own ready time.
type rgwGatewayTimeline struct {
	now    func() time.Time
	events []string
}

func (l *rgwGatewayTimeline) clock() time.Time {
	if l.now != nil {
		return l.now().UTC()
	}
	return time.Now().UTC()
}

// Mark records a named point in time.
func (l *rgwGatewayTimeline) Mark(label string) {
	l.events = append(l.events, fmt.Sprintf("%s %s", l.clock().Format(time.RFC3339Nano), label))
}

// Gateways records label with the Docker state of each named gateway, each
// inspected within inspect. It returns the recorded line.
func (l *rgwGatewayTimeline) Gateways(ctx context.Context, label string, inspect time.Duration, gateways map[string]rgwContainerStateReader, order []string) string {
	var parts []string
	for _, name := range order {
		reader := gateways[name]
		inspectCtx, cancel := context.WithTimeout(ctx, inspect)
		state, err := reader.State(inspectCtx)
		cancel()
		sample := rgwSourceSampleOf(state)
		if err != nil {
			sample = rgwSourceSample{Err: err.Error()}
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", name, reader.GetContainerID(), sample))
	}
	line := fmt.Sprintf("%s %s: %s", l.clock().Format(time.RFC3339Nano), label, strings.Join(parts, "; "))
	l.events = append(l.events, line)
	return line
}

func (l *rgwGatewayTimeline) String() string {
	if l == nil || len(l.events) == 0 {
		return "no timeline recorded"
	}
	return strings.Join(l.events, " | ")
}

// rgwCreateBucketAttempt is the status, body and error of one bucket-creation
// PUT, kept for the failure message.
type rgwCreateBucketAttempt struct {
	Status int
	Body   []byte
	Err    error
}

func (a rgwCreateBucketAttempt) String() string {
	return fmt.Sprintf("HTTP=%d body=%s err=%v", a.Status, a.Body, a.Err)
}

// rgwCreateBucketProbe inspects the bound source gateway and, only while it
// is not in a terminal state, sends one bucket-creation PUT through client.
// A terminal source state ends the wait at once because more waiting cannot
// make a stopped process answer. An inspect error is recorded and the wait
// continues within its own bound.
func rgwCreateBucketProbe(client harness.S3HTTPClient, source rgwReplicaSource, inspect time.Duration, path string, payload []byte) func(context.Context) (rgwCreateBucketObservation, bool, error) {
	inspectErrors := 0
	var last rgwCreateBucketAttempt
	return func(ctx context.Context) (rgwCreateBucketObservation, bool, error) {
		// A terminal inspection reports the last PUT result that preceded it.
		seen := rgwCreateBucketObservation{Attempt: last}
		inspectCtx, cancel := context.WithTimeout(ctx, inspect)
		var terminal error
		seen.Source, terminal = source.Check(inspectCtx)
		cancel()
		if seen.Source.Err != "" {
			inspectErrors++
		}
		seen.Source.InspectErrors = inspectErrors
		if terminal != nil {
			return seen, false, rgwCreateBucketSourceError{terminal}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, client.Endpoint+path, bytes.NewReader(payload))
		if err != nil {
			return seen, false, err
		}
		client.Sign(req, payload, time.Now().UTC())
		response, err := client.HTTP.Do(req)
		if err != nil {
			seen.Attempt = rgwCreateBucketAttempt{Err: err}
			last = seen.Attempt
			return seen, false, nil
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		seen.Attempt = rgwCreateBucketAttempt{Status: response.StatusCode, Body: body, Err: err}
		last = seen.Attempt
		if err != nil {
			// A response whose body cannot be read is terminal and the PUT
			// is not replayed: the creation may already be accepted.
			return seen, false, rgwCreateBucketReadError{err}
		}
		return seen, response.StatusCode == http.StatusOK && ctx.Err() == nil, nil
	}
}

// rgwCreateBucketSourceError marks a terminal state of the bound source
// gateway; rgwCreateBucketReadError marks a response body that failed to read.
type rgwCreateBucketSourceError struct{ error }

func (e rgwCreateBucketSourceError) Unwrap() error { return e.error }

type rgwCreateBucketReadError struct{ error }

func (e rgwCreateBucketReadError) Unwrap() error { return e.error }

type rgwCreateBucketObservation struct {
	Attempt rgwCreateBucketAttempt
	Source  rgwSourceSample
}

// rgwCreateBucketConverged waits until the PUT of path is accepted. A stopped,
// removed or restarted source gateway ends it at once with an error that names
// the container and its state and says it is not a placement-reload failure.
// Only a wait that ran out of time with the source still alive keeps the
// placement-reload wording, together with the source state and the timeline.
func rgwCreateBucketConverged(ctx context.Context, client harness.S3HTTPClient, source rgwReplicaSource, path string, payload []byte, bounds rgwReplicaBounds, timeline *rgwGatewayTimeline) error {
	if source.reader == nil {
		return fmt.Errorf("create bucket %s: no bound source gateway", path)
	}
	seen, err := harness.Observe(ctx, bounds.Wait, bounds.Read, bounds.Interval, rgwCreateBucketProbe(client, source, bounds.Inspect, path, payload))
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("realm gateway did not reload placement: %s; last %s; source %s %s; timeline: %s", err, seen.Value.Attempt, source.ID, seen.Value.Source, timeline)
	}
	var readErr rgwCreateBucketReadError
	if errors.As(err, &readErr) {
		return fmt.Errorf("bucket creation response body read failed; the request may have been accepted and is not replayed: %w; last %s; source %s %s; timeline: %s", err, seen.Value.Attempt, source.ID, seen.Value.Source, timeline)
	}
	var sourceErr rgwCreateBucketSourceError
	if !errors.As(err, &sourceErr) {
		return fmt.Errorf("bucket creation aborted: %w; last %s; source %s %s; timeline: %s", err, seen.Value.Attempt, source.ID, seen.Value.Source, timeline)
	}
	return fmt.Errorf("source gateway %s is not running, so bucket creation at its endpoint cannot succeed; this is not a placement-reload failure (the cause of the exit is not established here): %w; last %s; timeline: %s", source.ID, err, seen.Value.Attempt, timeline)
}

// rgwPlacementWaitCreateBucketFrom is rgwPlacementWaitCreateBucket for a
// source gateway whose process the caller bound before the wait.
func rgwPlacementWaitCreateBucketFrom(t *testing.T, parent context.Context, client harness.S3HTTPClient, source rgwReplicaSource, path string, payload []byte, timeline *rgwGatewayTimeline) {
	t.Helper()
	bounds := rgwReplicaBounds{Wait: time.Minute, Read: 30 * time.Second, Inspect: 10 * time.Second, Interval: 250 * time.Millisecond}
	if err := rgwCreateBucketConverged(parent, client, source, path, payload, bounds, timeline); err != nil {
		t.Fatal(err)
	}
}

// rgwBindCreateSource binds the source gateway before the first request to
// it. A source that has already stopped fails here, with its state and the
// timeline, instead of after a wait.
func rgwBindCreateSource(t *testing.T, parent context.Context, source rgwContainerStateReader, inspect time.Duration, timeline *rgwGatewayTimeline) rgwReplicaSource {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, inspect)
	defer cancel()
	bound, err := rgwReplicaBindSource(ctx, source)
	if err != nil {
		t.Fatalf("%v; this is not a placement-reload failure; timeline: %s", err, timeline)
	}
	return bound
}
