package dashboard

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/types/container"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// observationCLI supplies stable manager discovery and a probe response gate.
// Its server-side write occurs after authorization, before the complete response.
// It models only the fixture-owned record and uses the real public call paths.
type observationCLI struct {
	*restoreCLI
	mu      sync.Mutex
	url     string
	arrived chan struct{}
	release chan struct{}
}

func (*observationCLI) GetContainerID() string { return "dashboard-observation-control" }
func (*observationCLI) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}
func (f *observationCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch command := clustertest.ModuleArgs(args); {
	case slices.Equal(command, []string{"config", "rm", "mgr", multiclusterOption}):
		delete(f.keys, multiclusterOption)
		return clustertest.Reply(args, 0, nil)
	case slices.Equal(command, []string{"mgr", "dump", "--format", "json"}):
		return clustertest.Reply(args, 0, []byte(`{"available":true,"active_name":"a","active_gid":1}`))
	case slices.Equal(command, []string{"mgr", "services", "--format", "json"}):
		data, _ := json.Marshal(map[string]string{"dashboard": f.url})
		return clustertest.Reply(args, 0, data)
	default:
		return f.restoreCLI.Exec(ctx, args, opts...)
	}
}
func (f *observationCLI) AttachExec(ctx context.Context, _ []string, _ []string) (supervised.Attachment, error) {
	reader, writer := io.Pipe()
	input := &observationInput{writer: writer, complete: func(size int) error {
		select {
		case f.arrived <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		f.mu.Lock()
		f.keys[multiclusterOption] = `{"current_user":"tester","config":{}}`
		f.mu.Unlock()
		report := readyReport()
		report.Finished = true
		report.Results = append(report.Results, probeResult{Path: rgwDaemonPath, Status: 403, Body: `{"detail":"AccessDenied"}`})
		body, _ := json.Marshal(report)
		envelope, _ := json.Marshal(map[string]any{"schema_version": 1, "stdout": base64.StdEncoding.EncodeToString(body), "stderr": "", "exit_code": 0, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false, "input_received": size, "input_delivered": size, "input_eof": true})
		_, err := writer.Write(append(binary.BigEndian.AppendUint32([]byte{1, 0, 0, 0}, uint32(len(envelope))), envelope...))
		if err != nil {
			return err
		}
		return writer.Close()
	}}
	return supervised.Attachment{Input: input, Output: reader, ExitCode: func(context.Context) (int, error) { return 0, nil }, Release: func() { _ = reader.Close() }}, nil
}

type observationInput struct {
	bytes.Buffer
	writer   *io.PipeWriter
	complete func(int) error
}

func (in *observationInput) CloseWrite() error { return in.complete(in.Len()) }
func (in *observationInput) Close() error      { return in.writer.Close() }

func TestDashboardObservationsDrainBeforeCleanup(t *testing.T) {
	for _, cleanup := range []string{"restore", "remove"} {
		t.Run(cleanup, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			installed, want := probeCertificate(t)
			listener := newProbeListener(t, installed, installed, 1<<30)
			f := &observationCLI{restoreCLI: &restoreCLI{accountName: "owned-marker", keys: map[string]string{}}, url: listener.server.URL, arrived: make(chan struct{}, 3), release: make(chan struct{})}
			s := &state{cluster: testhelper.TestCluster(f, testhelper.TestConfig{HostNetwork: true, StartupTimeout: 5 * time.Second}), registry: &registry{}, username: "tester", password: probePassword, leaf: want, accountMarker: "owned-marker", multicluster: &multiclusterState{}}
			s.registry.active = s
			d := &Dashboard{s}
			link := &GatewayLink{&linkState{dashboard: s}}
			s.link = link
			results := make(chan error, 3)
			go func() { _, err := d.Endpoint(ctx); results <- err }()
			go func() { _, err := d.HostEndpoint(ctx); results <- err }()
			go func() {
				err := link.Check(ctx)
				if errors.Is(err, ErrGatewayRefused) {
					err = nil
				}
				results <- err
			}()
			for range 3 {
				select {
				case <-f.arrived:
				case <-ctx.Done():
					t.Fatal("observations did not overlap:", ctx.Err())
				}
			}
			// This is a synchronization assertion, not a sleep-based inference that
			// the cleanup goroutine has happened to get CPU time.
			if s.mu.TryLock() {
				s.mu.Unlock()
				close(f.release)
				t.Fatal("admitted observations released their lifetime before the response")
			}
			cleaned := make(chan error, 1)
			go func() {
				if cleanup == "restore" {
					cleaned <- d.Restore(ctx)
				} else {
					cleaned <- link.Remove(ctx)
				}
			}()
			close(f.release)
			for range 3 {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			if err := <-cleaned; err != nil {
				t.Fatal(err)
			}
			if cleanup == "remove" {
				if err := d.Restore(ctx); err != nil {
					t.Fatal(err)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.accountName != "" || len(f.keys) != 0 || s.registry.active != nil {
				t.Fatalf("late server write survived cleanup: account=%q keys=%v active=%t", f.accountName, f.keys, s.registry.active != nil)
			}
			if _, err := d.Endpoint(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("post-restoration endpoint: %v", err)
			}
		})
	}
}

func TestGatewayLinkIdentityReadersJoinRemoval(t *testing.T) {
	link := &GatewayLink{&linkState{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 10000 {
			link.mu.Lock()
			link.user = &rgw.User{}
			link.mu.Unlock()
			link.mu.Lock()
			link.user = nil
			link.mu.Unlock()
		}
	}()
	for range 10000 {
		_ = link.User()
		_ = fmt.Sprint(*link)
		_ = fmt.Sprintf("%#v", *link)
	}
	<-done
}
