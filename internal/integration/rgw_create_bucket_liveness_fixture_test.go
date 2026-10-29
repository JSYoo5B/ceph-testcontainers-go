//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// Docker times of the source gateway in job 123155516649 of issue #2: the
// process started once and Docker recorded the exit 7.6 s after the signal
// record in the daemon log. The sub-second digits are rounded in the log.
const (
	liveSourceStarted  = "2026-10-28T23:19:31.630000000Z"
	liveSourceFinished = "2026-10-28T23:21:17.455000000Z"
)

func liveSourceExited() *container.State {
	return &container.State{Status: container.StateExited, ExitCode: 139, StartedAt: liveSourceStarted, FinishedAt: liveSourceFinished}
}

func liveSourceRunning() *container.State {
	return &container.State{Status: container.StateRunning, Running: true, StartedAt: liveSourceStarted}
}

// createBucketClient answers every signed PUT with reply, counting requests.
func createBucketClient(requests *atomic.Int32, reply func(*http.Request) (*http.Response, error)) s3HTTPClient {
	return s3HTTPClient{endpoint: "http://source.invalid", accessKey: "fixture-access", secretKey: "fixture-secret", region: "us-east-1",
		http: &http.Client{Transport: replicaRoundTrip(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPut || request.Header.Get("Authorization") == "" {
				return nil, errors.New("bucket creation sent an unsigned or non-PUT request")
			}
			requests.Add(1)
			return reply(request)
		})}}
}

func createBucketUnreachable(*http.Request) (*http.Response, error) {
	return nil, errors.New("context deadline exceeded")
}

// createBucketSource binds a fake source that is running at the bind and
// answers later inspections with later.
func createBucketSource(t *testing.T, later func(ctx context.Context, call int) (*container.State, error)) rgwReplicaSource {
	t.Helper()
	fake := &replicaSourceFake{state: func(ctx context.Context, call int) (*container.State, error) {
		if call == 1 {
			return liveSourceRunning(), nil
		}
		return later(ctx, call)
	}}
	source, err := rgwReplicaBindSource(t.Context(), fake)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

var createBucketFixtureBounds = rgwReplicaBounds{Wait: 400 * time.Millisecond, Read: 50 * time.Millisecond, Inspect: 20 * time.Millisecond, Interval: 5 * time.Millisecond}

// The failure of job 123155516649: the source exited with 139 and the client
// saw only unreachable PUTs. The wait must end at once, name the container and
// the exit, and not call it a placement-reload failure. The old wait kept
// sending until its 60 s deadline and then blamed the reload.
func TestRGWCreateBucketWaitEndsOnExitedSource(t *testing.T) {
	source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return liveSourceExited(), nil })
	var requests atomic.Int32
	client := createBucketClient(&requests, createBucketUnreachable)
	bounds := createBucketFixtureBounds
	bounds.Wait = time.Minute
	timeline := &rgwGatewayTimeline{now: func() time.Time { return time.Date(2026, 10, 28, 23, 20, 40, 170000000, time.UTC) }}
	timeline.Mark("destination restarted")
	started := time.Now()
	err := rgwCreateBucketConverged(t.Context(), client, source, "/bucket", []byte("body"), bounds, timeline)
	if err == nil || time.Since(started) > 10*time.Second {
		t.Fatalf("exited source did not end the wait at once: %v after %s", err, time.Since(started))
	}
	if requests.Load() != 0 {
		t.Fatalf("%d PUTs were sent to a stopped source", requests.Load())
	}
	for _, want := range []string{"source gateway source-fixture is not running", "status=exited exit=139", "finished=" + liveSourceFinished,
		"not a placement-reload failure", "2026-10-28T23:20:40.17Z destination restarted"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "did not reload placement") {
		t.Fatalf("a dead source was reported as a reload failure: %v", err)
	}
}

// A source that dies while the PUTs are failing ends the wait at the next
// inspection, with the last PUT result in the message.
func TestRGWCreateBucketWaitEndsWhenSourceDiesMidWait(t *testing.T) {
	source := createBucketSource(t, func(_ context.Context, call int) (*container.State, error) {
		if call <= 3 {
			return liveSourceRunning(), nil
		}
		return liveSourceExited(), nil
	})
	var requests atomic.Int32
	client := createBucketClient(&requests, createBucketUnreachable)
	bounds := createBucketFixtureBounds
	bounds.Wait = time.Minute
	err := rgwCreateBucketConverged(t.Context(), client, source, "/bucket", nil, bounds, nil)
	if err == nil || requests.Load() != 2 {
		t.Fatalf("%d PUTs, %v", requests.Load(), err)
	}
	for _, want := range []string{"exit=139", "not a placement-reload failure", "last HTTP=0 body= err=Put", "no timeline recorded"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
}

// An exit with code 0 and a changed start generation are as terminal as a
// crash: the source process that was bound no longer serves the endpoint.
func TestRGWCreateBucketWaitEndsOnOtherTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state *container.State
		want  string
	}{
		{"exit zero", &container.State{Status: container.StateExited, StartedAt: liveSourceStarted}, "status=exited exit=0"},
		{"restarted", &container.State{Status: container.StateRunning, Running: true, StartedAt: "later"}, "start generation changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return tc.state, nil })
			var requests atomic.Int32
			bounds := createBucketFixtureBounds
			bounds.Wait = time.Minute
			err := rgwCreateBucketConverged(t.Context(), createBucketClient(&requests, createBucketUnreachable), source, "/bucket", nil, bounds, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "not a placement-reload failure") || requests.Load() != 0 {
				t.Fatalf("%d PUTs, %v", requests.Load(), err)
			}
		})
	}
}

// A running source whose endpoint does not accept the bucket yet is the
// reload wait the old message described: it keeps that wording and names the
// source as running and the last answer.
func TestRGWCreateBucketWaitKeepsReloadWordingForRunningSource(t *testing.T) {
	source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return liveSourceRunning(), nil })
	var requests atomic.Int32
	client := createBucketClient(&requests, func(*http.Request) (*http.Response, error) {
		return replicaResponse(http.StatusBadRequest, strings.NewReader("InvalidLocationConstraint")), nil
	})
	err := rgwCreateBucketConverged(t.Context(), client, source, "/bucket", nil, createBucketFixtureBounds, nil)
	if err == nil || requests.Load() < 2 {
		t.Fatalf("%d PUTs, %v", requests.Load(), err)
	}
	for _, want := range []string{"realm gateway did not reload placement", "HTTP=400 body=InvalidLocationConstraint", "status=running exit=0"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "not a placement-reload failure") {
		t.Fatalf("a running source was reported as dead: %v", err)
	}
}

// An inspect error says something about the Docker API, not the process: it
// is neither a crash nor a healthy state, and the wait ends on its own bound.
func TestRGWCreateBucketInspectErrorsAreNotACrash(t *testing.T) {
	source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return nil, errors.New("docker unavailable") })
	var requests atomic.Int32
	err := rgwCreateBucketConverged(t.Context(), createBucketClient(&requests, createBucketUnreachable), source, "/bucket", nil, createBucketFixtureBounds, nil)
	if err == nil || requests.Load() < 2 {
		t.Fatalf("%d PUTs, %v", requests.Load(), err)
	}
	for _, want := range []string{"realm gateway did not reload placement", "inspect error: docker unavailable", "failed inspections"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "not a placement-reload failure") {
		t.Fatalf("an inspect error was reported as a dead source: %v", err)
	}
}

// A source that accepts the bucket after some failed attempts ends the wait.
func TestRGWCreateBucketWaitSucceedsAfterRetries(t *testing.T) {
	source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return liveSourceRunning(), nil })
	var requests atomic.Int32
	client := createBucketClient(&requests, func(*http.Request) (*http.Response, error) {
		if requests.Load() < 3 {
			return replicaResponse(http.StatusBadRequest, strings.NewReader("InvalidLocationConstraint")), nil
		}
		return replicaResponse(http.StatusOK, strings.NewReader("")), nil
	})
	if err := rgwCreateBucketConverged(t.Context(), client, source, "/bucket", nil, createBucketFixtureBounds, nil); err != nil || requests.Load() != 3 {
		t.Fatalf("%d PUTs, %v", requests.Load(), err)
	}
	if err := rgwCreateBucketConverged(t.Context(), client, rgwReplicaSource{}, "/bucket", nil, createBucketFixtureBounds, nil); err == nil {
		t.Fatal("wait ran without a bound source")
	}
}

// The timeline records RFC 3339 times with each gateway's container, start
// and finish; a failed inspection is a failed field, not a healthy state.
func TestRGWGatewayTimelineRecordsStatesAndFailedInspections(t *testing.T) {
	ticks := 0
	timeline := &rgwGatewayTimeline{now: func() time.Time {
		ticks++
		return time.Date(2026, 10, 28, 23, 20, 38, 0, time.UTC).Add(time.Duration(ticks) * time.Second)
	}}
	source := &replicaSourceFake{state: func(context.Context, int) (*container.State, error) { return liveSourceRunning(), nil }}
	destination := &replicaSourceFake{state: func(context.Context, int) (*container.State, error) { return nil, errors.New("not found") }}
	timeline.Mark("ReloadPlacement begins")
	line := timeline.Gateways(t.Context(), "after reload", time.Second,
		map[string]rgwContainerStateReader{"source": source, "destination": destination}, []string{"source", "destination"})
	for _, want := range []string{"2026-10-28T23:20:40Z after reload: source source-fixture status=running exit=0 oom=false started=" + liveSourceStarted,
		"; destination source-fixture inspect error: not found"} {
		if !strings.Contains(line, want) {
			t.Fatalf("timeline line lacks %q: %s", want, line)
		}
	}
	if got := timeline.String(); !strings.HasPrefix(got, "2026-10-28T23:20:39Z ReloadPlacement begins | ") {
		t.Fatalf("timeline: %s", got)
	}
}

type createBucketFailingBody struct{}

func (createBucketFailingBody) Read([]byte) (int, error) {
	return 0, errors.New("connection reset while reading the body")
}
func (createBucketFailingBody) Close() error { return nil }

// A response whose body cannot be read is terminal, as before: an accepted
// creation (here HTTP 200) must not be replayed, and the read error is not a
// source crash while the source is still running.
func TestRGWCreateBucketBodyReadFailureIsTerminalNotReplayed(t *testing.T) {
	source := createBucketSource(t, func(context.Context, int) (*container.State, error) { return liveSourceRunning(), nil })
	var requests atomic.Int32
	client := createBucketClient(&requests, func(*http.Request) (*http.Response, error) {
		if requests.Load() == 1 {
			return replicaResponse(http.StatusOK, createBucketFailingBody{}), nil
		}
		return replicaResponse(http.StatusOK, strings.NewReader("")), nil
	})
	bounds := createBucketFixtureBounds
	bounds.Wait = time.Minute
	err := rgwCreateBucketConverged(t.Context(), client, source, "/bucket", nil, bounds, nil)
	if err == nil || requests.Load() != 1 {
		t.Fatalf("%d PUTs, %v", requests.Load(), err)
	}
	for _, want := range []string{"response body read failed", "connection reset while reading the body", "status=running", "HTTP=200"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
	for _, bad := range []string{"is not running", "did not reload placement"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("read failure relabelled %q: %v", bad, err)
		}
	}
}
