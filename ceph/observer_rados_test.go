package ceph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	observerPGCommand      = "rados list-inconsistent-obj 1.0 --format=json"
	observerWatchersLine   = "rados --pool app-data listwatchers notify-object"
	observerInconsistent   = `{"epoch":3,"inconsistents":[{"object":{"name":"obj-a","nspace":""},"errors":["data_digest_mismatch"],"union_shard_errors":[],"shards":[]}]}`
	observerInconsistent2  = `{"epoch":4,"inconsistents":[]}`
	observerWatcherReply   = "watcher=172.19.0.6:0/3461995546 client.4193 cookie=187650986454688\n"
	observerWatcherReply2  = "watcher=172.19.0.7:0/11 client.4200 cookie=9\n"
	observerPrometheusKey  = "mgr services --format json"
	observerNoPrometheus   = `{"dashboard":"http://mgr:8443/"}`
	observerWithPrometheus = `{"prometheus":"http://mgr:9283/"}`
)

// observerRados answers every command of the fake control container (it is
// also the client container, because the zero-value image set has equal client
// and essential images). `ceph` commands are keyed without their connection
// prefix, other commands by their full line. It records every command it
// received.
type observerRados struct {
	testcontainers.Container
	mu      sync.Mutex
	replies map[string]string
	calls   []string
	exit    int
}

func (ctr *observerRados) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	key := strings.Join(args, " ")
	if len(args) > 3 && args[0] == "ceph" {
		key = strings.Join(args[3:], " ")
	}
	ctr.calls = append(ctr.calls, key)
	return ctr.exit, healthDetailsFixtureStream(ctr.replies[key]), nil
}

func (ctr *observerRados) commands() []string {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	return slices.Clone(ctr.calls)
}

func (ctr *observerRados) set(command, reply string) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	ctr.replies[command] = reply
}

func (ctr *observerRados) setExit(code int) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	ctr.exit = code
}

func newObserverRadosCluster() (*Container, *observerRados) {
	ctr := &observerRados{replies: map[string]string{
		observerPGCommand:     observerInconsistent,
		observerWatchersLine:  observerWatcherReply,
		observerPrometheusKey: observerNoPrometheus,
	}}
	c := &Container{Container: ctr}
	c.settings.startupTimeout = 5 * time.Second
	return c, ctr
}

func radosQueries(observer *Observer) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"PGInconsistencies": func(ctx context.Context) error { _, err := observer.PGInconsistencies(ctx, "1.0"); return err },
		"ObjectWatchers": func(ctx context.Context) error {
			_, err := observer.ObjectWatchers(ctx, "app-data", "", "notify-object")
			return err
		},
		"ManagerMetrics": func(ctx context.Context) error { _, err := observer.ManagerMetrics(ctx); return err },
	}
}

func TestObserverRadosQueriesReadActualStateOnEveryQuery(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	if len(ctr.commands()) != 0 {
		t.Fatal("construction ran commands")
	}
	found, err := observer.PGInconsistencies(t.Context(), "1.0")
	if err != nil || len(found) != 1 || found[0].Name != "obj-a" {
		t.Fatalf("inconsistencies = %+v, %v", found, err)
	}
	ctr.set(observerPGCommand, observerInconsistent2)
	found, err = observer.PGInconsistencies(t.Context(), "1.0")
	if err != nil || len(found) != 0 {
		t.Fatalf("a retained observer returned earlier inconsistencies: %+v, %v", found, err)
	}
	watchers, err := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object")
	want := []ObjectWatcher{{Address: "172.19.0.6:0/3461995546", ClientID: 4193, Cookie: 187650986454688}}
	if err != nil || !reflect.DeepEqual(watchers, want) {
		t.Fatalf("watchers = %+v, %v", watchers, err)
	}
	ctr.set(observerWatchersLine, observerWatcherReply2)
	watchers, err = observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object")
	want = []ObjectWatcher{{Address: "172.19.0.7:0/11", ClientID: 4200, Cookie: 9}}
	if err != nil || !reflect.DeepEqual(watchers, want) {
		t.Fatalf("a retained observer returned earlier watchers: %+v, %v", watchers, err)
	}
}

func TestObserverRadosQueriesReturnIndependentCopies(t *testing.T) {
	cluster, _ := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	first, err := observer.PGInconsistencies(t.Context(), "1.0")
	if err != nil || len(first) != 1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	first[0].Name = "changed"
	first[0].Errors[0] = "changed"
	second, err := observer.PGInconsistencies(t.Context(), "1.0")
	if err != nil || second[0].Name != "obj-a" || second[0].Errors[0] != "data_digest_mismatch" {
		t.Fatalf("a caller edit leaked into the next result: %+v, %v", second, err)
	}
	watchers, _ := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object")
	watchers[0].Cookie = 1
	again, _ := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object")
	if again[0].Cookie != 187650986454688 {
		t.Fatalf("a caller edit leaked into the next result: %+v", again)
	}
}

func TestObserverRadosQueriesKeepArgumentValidationBeforeAnyCommand(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	if _, err := observer.PGInconsistencies(t.Context(), "not-a-pg"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid PG id returned %v", err)
	}
	for _, args := range [][3]string{{"", "", "o"}, {"app-data", "-ns", "o"}, {"app-data", "", ""}, {"app-data", "", "-o"}} {
		if _, err := observer.ObjectWatchers(t.Context(), args[0], args[1], args[2]); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("ObjectWatchers%q returned %v", args, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("an invalid request ran commands: %q", got)
	}
}

// A failed collection is an error with the original attribution; it is never
// an empty list that a caller could count as "no inconsistencies" or "no
// watchers".
func TestObserverRadosQueriesFailedCollectionIsNeverAnEmptyResult(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	ctr.setExit(2)
	found, err := observer.PGInconsistencies(t.Context(), "1.0")
	if err == nil || found != nil {
		t.Fatalf("a failed scrape-result read returned %+v, %v", found, err)
	}
	watchers, err := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object")
	var native *NativeFailure
	if !errors.As(err, &native) || watchers != nil {
		t.Fatalf("a failed listwatchers returned %+v, %v", watchers, err)
	}
	ctr.setExit(0)
	// Output that is not the expected form is also an error, not an empty list.
	ctr.set(observerPGCommand, `{"epoch":3}`)
	if found, err := observer.PGInconsistencies(t.Context(), "1.0"); err == nil || found != nil {
		t.Fatalf("a report without the inconsistents field returned %+v, %v", found, err)
	}
	ctr.set(observerWatchersLine, "unexpected output\n")
	if watchers, err := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object"); err == nil || watchers != nil {
		t.Fatalf("unparsable watchers returned %+v, %v", watchers, err)
	}
}

func TestObserverRadosQueriesRefuseATerminatedClusterBeforeAnyCommand(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	cluster.mu.Lock()
	cluster.closed = true
	cluster.mu.Unlock()
	for name, query := range radosQueries(observer) {
		if err := query(t.Context()); !errors.Is(err, errClusterTerminated) {
			t.Errorf("%s on a terminated cluster returned %v", name, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("a terminated cluster ran commands: %q", got)
	}
}

func TestObserverRadosQueriesKeepTheirOwnTopologyLockAcquisition(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	queries := radosQueries(observer)
	cluster.mu.Lock()
	// PGInconsistencies and ManagerMetrics (through ManagerServices) wait for the
	// topology lock under the caller's context.
	for _, name := range []string{"PGInconsistencies", "ManagerMetrics"} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		err := queries[name](ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			cluster.mu.Unlock()
			t.Fatalf("%s did not honor its deadline while the lock was held: %v", name, err)
		}
	}
	// ObjectWatchers takes the mutex without a context: it waits for the holder.
	done := make(chan error, 1)
	go func() { done <- queries["ObjectWatchers"](t.Context()) }()
	select {
	case early := <-done:
		_ = early
		cluster.mu.Unlock()
		t.Fatal("ObjectWatchers ran while the topology lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	if got := ctr.commands(); len(got) != 0 {
		cluster.mu.Unlock()
		t.Fatalf("commands ran under a held lock: %q", got)
	}
	cluster.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ObjectWatchers after the lock was released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ObjectWatchers did not resume after the lock was released")
	}
}

func TestObserverManagerMetricsRefusesAMissingEndpointWithoutScraping(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	observer, _ := Observe(cluster)
	metrics, err := observer.ManagerMetrics(t.Context())
	if !errors.Is(err, ErrInvalidRequest) || !reflect.DeepEqual(metrics, ManagerMetrics{}) {
		t.Fatalf("no endpoint returned %+v, %v", metrics, err)
	}
	for _, call := range ctr.commands() {
		if strings.HasPrefix(call, "python3") || strings.Contains(call, "metrics") {
			t.Fatalf("a scrape ran without an endpoint: %q", call)
		}
	}
}

func TestObserverManagerMetricsReachesTheSupervisedScrapeAtTheAdvertisedEndpoint(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	ctr.set(observerPrometheusKey, observerWithPrometheus)
	observer, _ := Observe(cluster)
	metrics, err := observer.ManagerMetrics(t.Context())
	// The fake serves no metrics, so the scrape cannot succeed; it must be an
	// error, never an empty ManagerMetrics presented as a result.
	if err == nil || !reflect.DeepEqual(metrics, ManagerMetrics{}) {
		t.Fatalf("an unserved scrape returned %+v, %v", metrics, err)
	}
	if errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("the scrape was refused before it ran: %v", err)
	}
	var scraped bool
	for _, call := range ctr.commands() {
		scraped = scraped || strings.Contains(call, "http://mgr:9283/metrics")
	}
	if !scraped {
		t.Fatalf("no command carried the advertised endpoint: %q", ctr.commands())
	}
}

func TestObserverRadosQueriesValidateTheTenantLeaseOnEveryQuery(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	lease := newLeaseDouble(cluster)
	observer := lease.observer()
	if found, err := observer.PGInconsistencies(t.Context(), "1.0"); err != nil || len(found) != 1 {
		t.Fatalf("live tenant inconsistencies = %+v, %v", found, err)
	}
	if watchers, err := observer.ObjectWatchers(t.Context(), "app-data", "", "notify-object"); err != nil || len(watchers) != 1 {
		t.Fatalf("live tenant watchers = %+v, %v", watchers, err)
	}
	before := len(ctr.commands())
	lease.release()
	check := func(when string) {
		for name, query := range radosQueries(observer) {
			if err := query(t.Context()); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("%s %s returned %v, want ErrInvalidRequest", name, when, err)
			}
		}
		if after := len(ctr.commands()); after != before {
			t.Fatalf("%s: %d commands ran", when, after-before)
		}
	}
	check("after release")
	lease.terminate()
	check("after release and termination")
}

func TestObserverRadosQueriesRefuseNilAndUnconstructedObservers(t *testing.T) {
	var nilObserver *Observer
	for name, observer := range map[string]*Observer{"nil": nilObserver, "zero": {}} {
		for query, run := range radosQueries(observer) {
			if err := run(t.Context()); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("%s observer %s returned %v", name, query, err)
			}
		}
	}
}

func TestObserverPGInconsistenciesQueriesTheRequestedPG(t *testing.T) {
	cluster, ctr := newObserverRadosCluster()
	ctr.set("rados list-inconsistent-obj 2.7 --format=json", observerInconsistent2)
	observer, _ := Observe(cluster)
	if found, err := observer.PGInconsistencies(t.Context(), "2.7"); err != nil || len(found) != 0 {
		t.Fatalf("found = %+v, %v", found, err)
	}
	if got := ctr.commands(); !slices.Equal(got, []string{"rados list-inconsistent-obj 2.7 --format=json"}) {
		t.Fatalf("commands = %q", got)
	}
}
