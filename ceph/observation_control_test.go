package ceph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// fakeControl records the order of services, lookup and run calls.
type fakeControl struct {
	services    map[string]string
	servicesErr error
	lookupErr   error
	report      supervised.Report
	runErr      error

	order     []string
	label     string
	limit     time.Duration
	stdoutMax int
	stderrMax int
	argv      []string
}

func (f *fakeControl) servicesFn(context.Context) (map[string]string, error) {
	f.order = append(f.order, "services")
	return f.services, f.servicesErr
}

func (f *fakeControl) lookup(context.Context) (supervisedRun, error) {
	f.order = append(f.order, "lookup")
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return func(_ context.Context, label string, limit time.Duration, stdoutMax, stderrMax int, argv ...string) (supervised.Report, error) {
		f.order = append(f.order, "run")
		f.label, f.limit, f.stdoutMax, f.stderrMax, f.argv = label, limit, stdoutMax, stderrMax, argv
		return f.report, f.runErr
	}, nil
}

const controlMetricsPage = "# TYPE up gauge\nup 1\n"

func TestReadManagerMetricsScrapeContract(t *testing.T) {
	for endpoint, want := range map[string]string{
		"http://mgr:9283":  "http://mgr:9283/metrics",
		"http://mgr:9283/": "http://mgr:9283/metrics",
	} {
		f := &fakeControl{services: map[string]string{"prometheus": endpoint}, report: supervised.Report{Stdout: []byte(controlMetricsPage)}}
		got, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup)
		if err != nil || got.URL != want || len(got.Samples) != 1 || got.Samples[0].Name != "up" {
			t.Fatalf("%s: metrics = %+v, %v", endpoint, got, err)
		}
		if !reflect.DeepEqual(f.order, []string{"services", "lookup", "run"}) {
			t.Fatalf("order = %v", f.order)
		}
		wantArgv := []string{"python3", "-c", managerMetricsScript, want}
		if f.label != "scrape manager metrics" || f.limit != managerMetricsTimeout || f.stdoutMax != managerMetricsLimit || f.stderrMax != 64<<10 || !reflect.DeepEqual(f.argv, wantArgv) {
			t.Fatalf("run = %q %v %d %d %q", f.label, f.limit, f.stdoutMax, f.stderrMax, f.argv)
		}
	}
}

func TestReadManagerMetricsRefusals(t *testing.T) {
	boom := errors.New("boom")
	// A services failure is returned as is, before any lookup.
	f := &fakeControl{servicesErr: boom}
	if _, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup); err != boom || !reflect.DeepEqual(f.order, []string{"services"}) {
		t.Fatalf("services failure = %v, order %v", err, f.order)
	}
	// Absent, empty and wrong-keyed endpoints are an invalid request without lookup.
	for name, services := range map[string]map[string]string{
		"nil":     nil,
		"empty":   {},
		"blank":   {"prometheus": ""},
		"another": {"dashboard": "http://mgr:8443/"},
	} {
		f = &fakeControl{services: services}
		_, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup)
		if !errors.Is(err, ErrInvalidRequest) || !reflect.DeepEqual(f.order, []string{"services"}) {
			t.Fatalf("%s: err = %v, order %v", name, err, f.order)
		}
	}
	// A lookup failure keeps its cause and runs nothing.
	f = &fakeControl{services: map[string]string{"prometheus": "http://mgr:9283/"}, lookupErr: boom}
	if _, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup); !errors.Is(err, boom) || !strings.Contains(err.Error(), "select the control container") || len(f.order) != 2 {
		t.Fatalf("lookup failure = %v, order %v", err, f.order)
	}
}

func TestReadManagerMetricsReportOutcomes(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]struct {
		report supervised.Report
		runErr error
		want   string
	}{
		"run error":  {runErr: boom, want: "scrape http://mgr:9283/metrics"},
		"timed out":  {report: supervised.Report{TimedOut: true, Stdout: []byte(controlMetricsPage)}, want: "timed out"},
		"exit code":  {report: supervised.Report{ExitCode: 3, Stdout: []byte(controlMetricsPage), Stderr: []byte("denied")}, want: "denied"},
		"truncated":  {report: supervised.Report{StdoutTruncated: true, Stdout: []byte(controlMetricsPage)}, want: "exceed"},
		"malformed":  {report: supervised.Report{Stdout: []byte("up one\n")}, want: "parse the metrics"},
		"no samples": {report: supervised.Report{Stdout: []byte("# TYPE up gauge\n")}, want: "served no samples"},
	}
	for name, tc := range cases {
		f := &fakeControl{services: map[string]string{"prometheus": "http://mgr:9283/"}, report: tc.report, runErr: tc.runErr}
		got, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup)
		if err == nil || !strings.Contains(err.Error(), tc.want) || got.URL != "" || got.Samples != nil {
			t.Fatalf("%s: got %+v, %v", name, got, err)
		}
		if tc.runErr != nil && !errors.Is(err, boom) {
			t.Fatalf("%s: cause lost: %v", name, err)
		}
	}
	var failure *NativeFailure
	f := &fakeControl{services: map[string]string{"prometheus": "http://mgr:9283/"}, report: supervised.Report{ExitCode: 3, Stderr: []byte("denied")}}
	if _, err := readManagerMetrics(t.Context(), f.servicesFn, f.lookup); !errors.As(err, &failure) {
		t.Fatalf("exit code is not a *NativeFailure: %v", err)
	}
}
