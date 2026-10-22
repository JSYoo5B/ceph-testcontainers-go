//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
func replicaClient(requests *atomic.Int32, reply func(*http.Request) (*http.Response, error)) s3HTTPClient {
	return s3HTTPClient{endpoint: "http://replica.invalid", accessKey: "fixture-access", secretKey: "fixture-secret", region: "us-east-1",
		http: &http.Client{Transport: replicaRoundTrip(func(request *http.Request) (*http.Response, error) {
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

var replicaFixtureBounds = rgwReplicaBounds{Wait: 400 * time.Millisecond, Read: 50 * time.Millisecond, Interval: 5 * time.Millisecond}

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
	seen, err := rgwReplicaConverged(t.Context(), client, "/bucket/replicated", payload, replicaFixtureBounds)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late exact body accepted: %v %+v", err, seen)
	}
	if !seen.Value.Late || seen.Value.Bytes != len(payload) || seen.Value.Status != http.StatusOK || requests.Load() < 2 {
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
	seen, err := rgwReplicaConverged(t.Context(), client, "/bucket/replicated", payload, replicaFixtureBounds)
	if err != nil || seen.Attempts != len(replies) || seen.Value.Bytes != len(payload) || seen.Value.Late {
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
	_, err := rgwReplicaConverged(ctx, client, "/bucket/replicated", []byte("payload"), bounds)
	if !errors.Is(err, context.Canceled) || time.Since(started) > 10*time.Second || requests.Load() != 1 {
		t.Fatalf("cancelled wait: %v after %s and %d requests", err, time.Since(started), requests.Load())
	}
}
