//go:build all || integration

package rgw_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io"
	"net/http"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
)

// rgwReplicaSample is one read of a replicated object: what the gateway
// answered, or why no answer arrived. A missing object (NoSuchKey) and a
// missing bucket (NoSuchBucket) are different observations.
type rgwReplicaSample struct {
	Status  int
	Code    string
	Message string
	Bytes   int
	SHA256  string
	ReadErr string
	Err     string
	// Late marks a body that was read only after the probe's own deadline;
	// it is not evidence of the probe even when its bytes match.
	Late bool
}

func (s rgwReplicaSample) String() string {
	if s.Err != "" {
		return "request error: " + s.Err
	}
	text := fmt.Sprintf("HTTP=%d code=%s message=%s bytes=%d sha256=%s", s.Status, s.Code, s.Message, s.Bytes, s.SHA256)
	if s.ReadErr != "" {
		text += " read error: " + s.ReadErr
	}
	if s.Late {
		text += " (read after the probe deadline)"
	}
	return text
}

// rgwReplicaBodyLimit bounds a body read beyond the expected payload, so an
// oversized reply is reported by its size rather than buffered whole.
const rgwReplicaBodyLimit = 64 << 10

// rgwReplicaProbe reads path once through client and reports whether it holds
// exactly payload. It is read-only for observe: a request or body error is a
// sample to wait past, and a body read after the probe's context ended is
// never accepted.
func rgwReplicaProbe(client harness.S3HTTPClient, path string, payload []byte) func(context.Context) (rgwReplicaSample, bool, error) {
	return func(ctx context.Context) (rgwReplicaSample, bool, error) {
		var sample rgwReplicaSample
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.Endpoint+path, nil)
		if err != nil {
			return sample, false, err
		}
		client.Sign(request, nil, time.Now().UTC())
		response, err := client.HTTP.Do(request)
		if err != nil {
			sample.Err = err.Error()
			return sample, false, nil
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(len(payload))+rgwReplicaBodyLimit))
		_ = response.Body.Close()
		sum := sha256.Sum256(body)
		sample.Status, sample.Bytes, sample.SHA256 = response.StatusCode, len(body), hex.EncodeToString(sum[:])
		if readErr != nil {
			sample.ReadErr = readErr.Error()
		}
		if response.StatusCode != http.StatusOK {
			var failure struct{ Code, Message string }
			_ = xml.Unmarshal(body, &failure)
			sample.Code, sample.Message = failure.Code, failure.Message
		}
		if ctx.Err() != nil {
			sample.Late = true
			return sample, false, nil
		}
		return sample, readErr == nil && response.StatusCode == http.StatusOK && bytes.Equal(body, payload), nil
	}
}

// rgwContainerStateReader reads one container's identity and Docker state.
// *rgw.Gateway satisfies it; fixtures fake it without Docker.
type rgwContainerStateReader interface {
	GetContainerID() string
	State(context.Context) (*container.State, error)
}

// rgwReplicaSource is the source gateway of a replicated write, bound to the
// container ID and process start time captured before the write. A wait for
// the replica checks it read-only; it never restarts the gateway.
type rgwReplicaSource struct {
	reader        rgwContainerStateReader
	ID, StartedAt string
}

// rgwReplicaBindSource captures the running source gateway's container and
// process start generation.
func rgwReplicaBindSource(ctx context.Context, reader rgwContainerStateReader) (rgwReplicaSource, error) {
	id := reader.GetContainerID()
	state, err := reader.State(ctx)
	if err != nil {
		return rgwReplicaSource{}, fmt.Errorf("source gateway %s: inspect before the replicated write: %w", id, err)
	}
	sample := rgwSourceSampleOf(state)
	if id == "" || sample.Status != container.StateRunning || state.StartedAt == "" {
		return rgwReplicaSource{}, fmt.Errorf("source gateway %q is not a running process to bind: %s", id, sample)
	}
	return rgwReplicaSource{reader: reader, ID: id, StartedAt: state.StartedAt}, nil
}

// rgwSourceSample is one inspection of the source gateway during a wait.
type rgwSourceSample struct {
	Status     container.ContainerState
	ExitCode   int
	OOMKilled  bool
	StartedAt  string
	FinishedAt string
	Err        string
	// InspectErrors counts the failed inspections of the wait so far.
	InspectErrors int
}

func rgwSourceSampleOf(state *container.State) rgwSourceSample {
	if state == nil {
		return rgwSourceSample{Err: "empty container state"}
	}
	sample := rgwSourceSample{Status: state.Status, ExitCode: state.ExitCode, OOMKilled: state.OOMKilled, StartedAt: state.StartedAt, FinishedAt: state.FinishedAt}
	if sample.Status == "" {
		switch {
		case state.Paused:
			sample.Status = container.StatePaused
		case state.Restarting:
			sample.Status = container.StateRestarting
		case state.Running:
			sample.Status = container.StateRunning
		default:
			sample.Status = container.StateExited
		}
	}
	return sample
}

func (s rgwSourceSample) String() string {
	text := "not inspected"
	if s.Err != "" {
		text = "inspect error: " + s.Err
	} else if s.Status != "" {
		text = fmt.Sprintf("status=%s exit=%d oom=%v started=%s finished=%s", s.Status, s.ExitCode, s.OOMKilled, s.StartedAt, s.FinishedAt)
	}
	if s.InspectErrors > 0 {
		text += fmt.Sprintf(" (%d failed inspections)", s.InspectErrors)
	}
	return text
}

// check inspects the source once. It returns an error only for a terminal
// state: a removed container, a changed process start generation, or a
// container that is no longer running. Paused and restarting are classified
// in the sample, not inferred as a crash: a paused process is frozen, and a
// restarting one shows its new start generation at the next inspection once
// Docker runs it again.
//
// An inspect error is neither healthy nor a crash. It says something about
// the Docker API, not the gateway process, so it is recorded in the sample and
// the wait keeps polling within its own bound; a wait that ends on its budget
// names the failed inspections instead of a healthy state.
func (s rgwReplicaSource) Check(ctx context.Context) (rgwSourceSample, error) {
	if id := s.reader.GetContainerID(); id != s.ID {
		return rgwSourceSample{}, fmt.Errorf("source gateway container changed from %s to %s", s.ID, id)
	}
	state, err := s.reader.State(ctx)
	if errdefs.IsNotFound(err) {
		return rgwSourceSample{Err: err.Error()}, fmt.Errorf("source gateway %s was removed: %w", s.ID, err)
	}
	if err != nil {
		return rgwSourceSample{Err: err.Error()}, nil
	}
	sample := rgwSourceSampleOf(state)
	if state == nil {
		return sample, nil
	}
	if state.StartedAt != s.StartedAt {
		return sample, fmt.Errorf("source gateway %s restarted: start generation changed from %s to %s", s.ID, s.StartedAt, state.StartedAt)
	}
	switch sample.Status {
	case container.StateRunning, container.StatePaused, container.StateRestarting:
		return sample, nil
	}
	return sample, fmt.Errorf("source gateway %s stopped: %s", s.ID, sample)
}

// rgwReplicaObservation is one convergence probe: the replica read and the
// source gateway inspection that went with it.
type rgwReplicaObservation struct {
	Replica rgwReplicaSample
	Source  rgwSourceSample
}

// rgwReplicaSourceProbe checks the source within inspect, then reads the
// replica with what remains of the probe. Exact replica bytes are convergence
// whatever the source state; otherwise a terminal source state ends the wait.
func rgwReplicaSourceProbe(source rgwReplicaSource, inspect time.Duration, replica func(context.Context) (rgwReplicaSample, bool, error)) func(context.Context) (rgwReplicaObservation, bool, error) {
	inspectErrors := 0
	return func(ctx context.Context) (rgwReplicaObservation, bool, error) {
		var seen rgwReplicaObservation
		inspectCtx, cancel := context.WithTimeout(ctx, inspect)
		var terminal error
		seen.Source, terminal = source.Check(inspectCtx)
		cancel()
		if seen.Source.Err != "" {
			inspectErrors++
		}
		seen.Source.InspectErrors = inspectErrors
		var ready bool
		var err error
		seen.Replica, ready, err = replica(ctx)
		if err != nil || ready {
			return seen, ready, err
		}
		return seen, false, terminal
	}
}

// rgwReplicaBounds bound one replica convergence: the whole wait, one probe,
// the source inspection inside a probe and the pause between probes.
type rgwReplicaBounds struct {
	Wait, Read, Inspect, Interval time.Duration
}

// rgwReplicaConverged waits until path holds exactly payload, ending at once
// when source stops or restarts. Its error names the last replica sample, the
// source state and the expected size and digest.
func rgwReplicaConverged(ctx context.Context, client harness.S3HTTPClient, source rgwReplicaSource, path string, payload []byte, bounds rgwReplicaBounds) (harness.Observation[rgwReplicaObservation], error) {
	if source.reader == nil {
		return harness.Observation[rgwReplicaObservation]{}, fmt.Errorf("replica %s: no bound source gateway", path)
	}
	seen, err := harness.Observe(ctx, bounds.Wait, bounds.Read, bounds.Interval, rgwReplicaSourceProbe(source, bounds.Inspect, rgwReplicaProbe(client, path, payload)))
	if err != nil {
		sum := sha256.Sum256(payload)
		return seen, fmt.Errorf("replica %s: %w; last %s; source %s %s; expected bytes=%d sha256=%s", path, err, seen.Value.Replica, source.ID, seen.Value.Source, len(payload), hex.EncodeToString(sum[:]))
	}
	return seen, nil
}
