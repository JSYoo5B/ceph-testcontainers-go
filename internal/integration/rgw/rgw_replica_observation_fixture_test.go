//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
)

type replicaRoundTrip func(*http.Request) (*http.Response, error)

func (f replicaRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

// replicaLateBody returns its bytes only after the request's context ended.
type replicaLateBody struct {
	ctx  context.Context
	data *bytes.Reader
}

func (b *replicaLateBody) Read(p []byte) (int, error) {
	<-b.ctx.Done()
	return b.data.Read(p)
}

func (*replicaLateBody) Close() error { return nil }

// replicaClient answers every GET with reply, counting the requests.
func replicaClient(requests *atomic.Int32, reply func(*http.Request) (*http.Response, error)) harness.S3HTTPClient {
	return harness.S3HTTPClient{Endpoint: "http://replica.invalid", AccessKey: "fixture-access", SecretKey: "fixture-secret", Region: "us-east-1",
		HTTP: &http.Client{Transport: replicaRoundTrip(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet || request.Header.Get("Authorization") == "" {
				return nil, errors.New("replica probe sent an unsigned or mutating request")
			}
			requests.Add(1)
			return reply(request)
		})}}
}

func replicaResponse(status int, body io.Reader) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(body)}
}

var replicaFixtureBounds = rgwReplicaBounds{Wait: 400 * time.Millisecond, Read: 50 * time.Millisecond, Inspect: 20 * time.Millisecond, Interval: 5 * time.Millisecond}

// replicaSourceFake is a source gateway container whose inspections answer
// state(n) for the n-th call, counting from 1 for the bind before the write.
type replicaSourceFake struct {
	calls atomic.Int32
	state func(ctx context.Context, call int) (*container.State, error)
}

func (*replicaSourceFake) GetContainerID() string { return "source-fixture" }

func (f *replicaSourceFake) State(ctx context.Context) (*container.State, error) {
	return f.state(ctx, int(f.calls.Add(1)))
}

func replicaRunning(started string) *container.State {
	return &container.State{Status: container.StateRunning, Running: true, StartedAt: started}
}

// replicaSource binds a fake source whose bind sees a running start-1 and
// whose later inspections answer later.
func replicaSource(t *testing.T, later func(ctx context.Context, call int) (*container.State, error)) (rgwReplicaSource, *replicaSourceFake) {
	t.Helper()
	fake := &replicaSourceFake{state: func(ctx context.Context, call int) (*container.State, error) {
		if call == 1 {
			return replicaRunning("start-1"), nil
		}
		return later(ctx, call)
	}}
	source, err := rgwReplicaBindSource(t.Context(), fake)
	if err != nil {
		t.Fatal(err)
	}
	return source, fake
}

func replicaHealthySource(t *testing.T) rgwReplicaSource {
	source, _ := replicaSource(t, func(context.Context, int) (*container.State, error) { return replicaRunning("start-1"), nil })
	return source
}

func replicaMissing() (*http.Response, error) {
	return replicaResponse(http.StatusNotFound, strings.NewReader("<Error><Code>NoSuchKey</Code><Message>key</Message></Error>")), nil
}

// Exact bytes that arrive only after a read's deadline are not convergence,
// however the phase ends: the read is retried and the phase fails with the
// late sample rather than accepting it.
func TestRGWReplicaLateExactBodyIsNotConvergence(t *testing.T) {
	payload := bytes.Repeat([]byte("replica\n"), 512)
	var requests atomic.Int32
	client := replicaClient(&requests, func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: &replicaLateBody{ctx: request.Context(), data: bytes.NewReader(payload)}}, nil
	})
	seen, err := rgwReplicaConverged(t.Context(), client, replicaHealthySource(t), "/bucket/replicated", payload, replicaFixtureBounds)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late exact body accepted: %v %+v", err, seen)
	}
	if !seen.Value.Replica.Late || seen.Value.Replica.Bytes != len(payload) || seen.Value.Replica.Status != http.StatusOK || requests.Load() < 2 {
		t.Fatalf("last sample %+v after %d requests", seen.Value, requests.Load())
	}
	if !strings.Contains(err.Error(), "read after the probe deadline") || !strings.Contains(err.Error(), "expected bytes=4096 sha256=") {
		t.Fatalf("error %v", err)
	}
}

// Missing objects and buckets, a short body and a transport error are waited
// past and kept distinct; only the exact bytes end the wait.
func TestRGWReplicaWaitsPastMissingAndPartialReplicas(t *testing.T) {
	payload := []byte("exact replica payload")
	replies := []func() (*http.Response, error){
		func() (*http.Response, error) {
			return replicaResponse(http.StatusNotFound, strings.NewReader("<Error><Code>NoSuchBucket</Code><Message>bucket</Message></Error>")), nil
		},
		func() (*http.Response, error) {
			return replicaResponse(http.StatusNotFound, strings.NewReader("<Error><Code>NoSuchKey</Code><Message>key</Message></Error>")), nil
		},
		func() (*http.Response, error) { return nil, errors.New("connection refused") },
		func() (*http.Response, error) {
			return replicaResponse(http.StatusOK, bytes.NewReader(payload[:5])), nil
		},
		func() (*http.Response, error) { return replicaResponse(http.StatusOK, bytes.NewReader(payload)), nil },
	}
	var requests atomic.Int32
	var samples []rgwReplicaSample
	client := replicaClient(&requests, func(*http.Request) (*http.Response, error) {
		return replies[min(int(requests.Load())-1, len(replies)-1)]()
	})
	probe := rgwReplicaProbe(client, "/bucket/replicated", payload)
	for range replies {
		sample, ready, err := probe(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		samples = append(samples, sample)
		if ready != (len(samples) == len(replies)) {
			t.Fatalf("sample %d ready=%v: %+v", len(samples), ready, sample)
		}
	}
	if samples[0].Code != "NoSuchBucket" || samples[1].Code != "NoSuchKey" || samples[2].Err != "Get \"http://replica.invalid/bucket/replicated\": connection refused" || samples[3].Bytes != 5 {
		t.Fatalf("samples %+v", samples)
	}
	requests.Store(0)
	seen, err := rgwReplicaConverged(t.Context(), client, replicaHealthySource(t), "/bucket/replicated", payload, replicaFixtureBounds)
	if err != nil || seen.Attempts != len(replies) || seen.Value.Replica.Bytes != len(payload) || seen.Value.Replica.Late {
		t.Fatalf("converged %v %+v", err, seen)
	}
}

// A caller that cancels the wait ends it with its cause, without waiting for
// the whole convergence budget.
func TestRGWReplicaWaitEndsWithTheCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var requests atomic.Int32
	client := replicaClient(&requests, func(*http.Request) (*http.Response, error) {
		cancel()
		return replicaResponse(http.StatusNotFound, strings.NewReader("<Error><Code>NoSuchKey</Code></Error>")), nil
	})
	started := time.Now()
	bounds := replicaFixtureBounds
	bounds.Wait = time.Minute
	_, err := rgwReplicaConverged(ctx, client, replicaHealthySource(t), "/bucket/replicated", []byte("payload"), bounds)
	if !errors.Is(err, context.Canceled) || time.Since(started) > 10*time.Second || requests.Load() != 1 {
		t.Fatalf("cancelled wait: %v after %s and %d requests", err, time.Since(started), requests.Load())
	}
}

// A source gateway that exits or restarts during the wait ends it at the next
// probe, long before the budget, with the last replica sample and the source
// state. The wait never restarts the gateway or replays the write.
func TestRGWReplicaWaitEndsOnTerminalSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state *container.State
		want  []string
	}{
		{"exited", &container.State{Status: container.StateExited, ExitCode: 139, StartedAt: "start-1", FinishedAt: "finish-1"},
			[]string{"source gateway source-fixture stopped", "status=exited exit=139", "finished=finish-1"}},
		{"dead", &container.State{Status: container.StateDead, Dead: true, ExitCode: 1, StartedAt: "start-1"},
			[]string{"stopped", "status=dead exit=1"}},
		{"restarted", replicaRunning("start-2"),
			[]string{"restarted: start generation changed from start-1 to start-2", "status=running"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, fake := replicaSource(t, func(_ context.Context, call int) (*container.State, error) {
				if call == 2 {
					return replicaRunning("start-1"), nil
				}
				return tc.state, nil
			})
			var requests atomic.Int32
			client := replicaClient(&requests, func(*http.Request) (*http.Response, error) { return replicaMissing() })
			bounds := replicaFixtureBounds
			bounds.Wait = time.Minute
			started := time.Now()
			seen, err := rgwReplicaConverged(t.Context(), client, source, "/bucket/replicated", []byte("payload"), bounds)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 10*time.Second {
				t.Fatalf("terminal source did not end the wait at once: %v after %s", err, time.Since(started))
			}
			if seen.Attempts != 2 || requests.Load() != 2 || fake.calls.Load() != 3 || seen.Value.Replica.Code != "NoSuchKey" {
				t.Fatalf("%d probes, %d reads, %d inspections, last %+v", seen.Attempts, requests.Load(), fake.calls.Load(), seen.Value)
			}
			for _, want := range append(tc.want, "last HTTP=404 code=NoSuchKey", "expected bytes=7") {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error lacks %q: %v", want, err)
				}
			}
		})
	}
}

// Exact replica bytes are convergence even when the source stopped after the
// write: the data was already replicated.
func TestRGWReplicaExactBytesConvergeAfterSourceExit(t *testing.T) {
	source, _ := replicaSource(t, func(context.Context, int) (*container.State, error) {
		return &container.State{Status: container.StateExited, ExitCode: 0, StartedAt: "start-1"}, nil
	})
	var requests atomic.Int32
	client := replicaClient(&requests, func(*http.Request) (*http.Response, error) {
		return replicaResponse(http.StatusOK, strings.NewReader("payload")), nil
	})
	if _, err := rgwReplicaConverged(t.Context(), client, source, "/bucket/replicated", []byte("payload"), replicaFixtureBounds); err != nil {
		t.Fatal(err)
	}
}

// A removed source container is terminal; any other inspect error, including
// an inspection that outlasts its bound, is recorded and polled past, never
// read as a healthy source. The wait then ends on its own budget naming the
// failed inspections.
func TestRGWReplicaInspectErrorsAreNotHealthy(t *testing.T) {
	source, fake := replicaSource(t, func(ctx context.Context, _ int) (*container.State, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	var requests atomic.Int32
	client := replicaClient(&requests, func(*http.Request) (*http.Response, error) { return replicaMissing() })
	seen, err := rgwReplicaConverged(t.Context(), client, source, "/bucket/replicated", []byte("payload"), replicaFixtureBounds)
	if !errors.Is(err, context.DeadlineExceeded) || requests.Load() < 2 || fake.calls.Load() < 3 || seen.Value.Source.InspectErrors < 2 {
		t.Fatalf("inspect timeouts: %v after %d reads and %d inspections, last %+v", err, requests.Load(), fake.calls.Load(), seen.Value)
	}
	if !strings.Contains(err.Error(), "source source-fixture inspect error: context deadline exceeded (") || !strings.Contains(err.Error(), "failed inspections)") || !strings.Contains(err.Error(), "code=NoSuchKey") {
		t.Fatalf("error %v", err)
	}

	removed, _ := replicaSource(t, func(context.Context, int) (*container.State, error) {
		return nil, errdefs.ErrNotFound.WithMessage("no such container")
	})
	if _, err := rgwReplicaConverged(t.Context(), client, removed, "/bucket/replicated", []byte("payload"), replicaFixtureBounds); err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "was removed") {
		t.Fatalf("removed source: %v", err)
	}
}

// Paused and restarting sources are classified, not inferred as a crash: the
// wait goes on and, if the replica never arrives, ends on its budget naming
// that state.
func TestRGWReplicaPausedOrRestartingSourceIsNotACrash(t *testing.T) {
	for _, state := range []*container.State{
		{Status: container.StatePaused, Running: true, Paused: true, StartedAt: "start-1"},
		{Status: container.StateRestarting, Restarting: true, ExitCode: 139, StartedAt: "start-1"},
	} {
		t.Run(string(state.Status), func(t *testing.T) {
			source, _ := replicaSource(t, func(context.Context, int) (*container.State, error) { return state, nil })
			var requests atomic.Int32
			client := replicaClient(&requests, func(*http.Request) (*http.Response, error) { return replicaMissing() })
			seen, err := rgwReplicaConverged(t.Context(), client, source, "/bucket/replicated", []byte("payload"), replicaFixtureBounds)
			if !errors.Is(err, context.DeadlineExceeded) || seen.Attempts < 2 || seen.Value.Source.Status != state.Status {
				t.Fatalf("%s source: %v %+v", state.Status, err, seen)
			}
			if !strings.Contains(err.Error(), "status="+string(state.Status)) || strings.Contains(err.Error(), "stopped") {
				t.Fatalf("error %v", err)
			}
		})
	}
}

// A healthy source with a late replica keeps the wait going until the exact
// bytes arrive, inspecting the source once per probe.
func TestRGWReplicaHealthySourceLateReplicaConverges(t *testing.T) {
	payload := []byte("late replica payload")
	source, fake := replicaSource(t, func(context.Context, int) (*container.State, error) { return replicaRunning("start-1"), nil })
	var requests atomic.Int32
	client := replicaClient(&requests, func(*http.Request) (*http.Response, error) {
		if requests.Load() < 4 {
			return replicaMissing()
		}
		return replicaResponse(http.StatusOK, bytes.NewReader(payload)), nil
	})
	seen, err := rgwReplicaConverged(t.Context(), client, source, "/bucket/replicated", payload, replicaFixtureBounds)
	if err != nil || seen.Attempts != 4 || fake.calls.Load() != 5 || seen.Value.Source.Status != container.StateRunning || seen.Value.Source.InspectErrors != 0 {
		t.Fatalf("healthy late replica: %v %+v after %d inspections", err, seen, fake.calls.Load())
	}
}

// A source that is not running cannot be bound before the write.
func TestRGWReplicaBindSourceNeedsARunningProcess(t *testing.T) {
	for _, state := range []*container.State{
		{Status: container.StateExited, StartedAt: "start-1"},
		{Status: container.StatePaused, Running: true, Paused: true, StartedAt: "start-1"},
		{Status: container.StateRunning, Running: true},
	} {
		fake := &replicaSourceFake{state: func(context.Context, int) (*container.State, error) { return state, nil }}
		if _, err := rgwReplicaBindSource(t.Context(), fake); err == nil {
			t.Fatalf("bound %+v", state)
		}
	}
	if _, err := rgwReplicaConverged(t.Context(), harness.S3HTTPClient{}, rgwReplicaSource{}, "/bucket/replicated", nil, replicaFixtureBounds); err == nil {
		t.Fatal("wait ran without a bound source")
	}
}
