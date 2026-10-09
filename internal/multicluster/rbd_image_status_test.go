package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rbdImageStatusFixture struct {
	*rbdNamespacePolicyFixture
	imageInfo, imageStatus string
	imageCalls             [][]string
	infoHook               func(*rbdImageStatusFixture)
	statusHook             func(*rbdImageStatusFixture)
	execError              func([]string) error
	execOutput             func([]string) (string, bool)
}

func (c *rbdImageStatusFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if c.execError != nil {
		if err := c.execError(args); err != nil {
			return 0, nil, err
		}
	}
	if c.execOutput != nil {
		if data, ok := c.execOutput(args); ok {
			return 0, rbdImageStatusStream(data), nil
		}
	}
	var data string
	if len(args) > 2 && slices.Equal(args[:2], []string{"rbd", "info"}) {
		c.imageCalls = append(c.imageCalls, slices.Clone(args))
		if c.infoHook != nil {
			c.infoHook(c)
		}
		data = c.imageInfo
	} else if len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "image", "status"}) {
		c.imageCalls = append(c.imageCalls, slices.Clone(args))
		if c.statusHook != nil {
			c.statusHook(c)
		}
		data = c.imageStatus
	} else {
		return c.rbdNamespacePolicyFixture.Exec(ctx, args, opts...)
	}
	return 0, rbdImageStatusStream(data), nil
}

func rbdImageStatusStream(data string) io.Reader {
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.WriteString(data)
	return bytes.NewReader(stream.Bytes())
}

func rbdImageStatusInfo(id, mode, global string, primary bool) string {
	return fmt.Sprintf(`{"name":"volume","id":%q,"mirroring":{"mode":%q,"state":"enabled","global_id":%q,"primary":%t}}`, id, mode, global, primary)
}

type rbdImageStatusDaemon struct {
	testcontainers.Container
	running             bool
	status              string
	state               *container.State
	stateErr, statusErr error
}

func (d *rbdImageStatusDaemon) State(context.Context) (*container.State, error) {
	if d.state != nil || d.stateErr != nil {
		return d.state, d.stateErr
	}
	return &container.State{Running: d.running}, nil
}

func (d *rbdImageStatusDaemon) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	return 0, rbdImageStatusStream(d.status), d.statusErr
}

func newRBDImageStatusFixture(t *testing.T, mode RBDMirrorMode, sourceNS, destinationNS string) (*RBDMirror, *rbdImageStatusFixture, *rbdImageStatusFixture, *rbdImageStatusDaemon) {
	t.Helper()
	source := &rbdImageStatusFixture{rbdNamespacePolicyFixture: newRBDNamespacePolicyFixture(), imageInfo: rbdImageStatusInfo("source-local", string(mode), "global-image", true)}
	destination := &rbdImageStatusFixture{rbdNamespacePolicyFixture: newRBDNamespacePolicyFixture(), imageInfo: rbdImageStatusInfo("destination-local", string(mode), "global-image", false), imageStatus: `{"name":"volume","global_id":"global-image","state":"up+replaying","description":"replaying","last_update":"2026-10-07 00:00:00","daemon_service":{"service_id":"22","instance_id":"123","daemon_id":"tc-owned"}}`}
	link := &RBDMirror{
		sourceClient: source, destinationClient: destination,
		config:         RBDMirrorConfig{Pool: "images", SourceNamespace: sourceNS, DestinationNamespace: destinationNS, Mode: mode, Scope: RBDMirrorScopeImage, SourceSite: "source", DestinationSite: "destination", Source: &ceph.Container{Container: source}, Destination: &ceph.Container{Container: destination}},
		poolIdentities: &rbdMirrorPoolIdentities{source: 42, destination: 42},
	}
	if err := link.provisionRBDMirrorPolicies(t.Context()); err != nil {
		t.Fatal(err)
	}
	daemon := &rbdImageStatusDaemon{running: true, status: `{"pool_replayers":[{"pool":"images","peer":"peer","state":"running","instance_id":"123"}]}`}
	link.daemons = []*RBDMirrorDaemon{{Container: daemon, DaemonName: "a", ClientName: "client.rbd-mirror.tc-owned"}}
	return link, source, destination, daemon
}

func TestRBDMirrorImageStatusScopedIdentityReadiness(t *testing.T) {
	for _, mode := range []RBDMirrorMode{RBDMirrorModeSnapshot, RBDMirrorModeJournal} {
		t.Run(string(mode), func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, mode, "ns-a", "ns-b")
			sourceMutations, destinationMutations := len(source.mutations), len(destination.mutations)
			status, err := link.ImageStatus(t.Context(), "volume")
			if err != nil || !status.ReplayReady || status.GlobalID != "global-image" || status.SourceImageID != "source-local" || status.DestinationImageID != "destination-local" || status.SourceNamespace != "ns-a" || status.DestinationNamespace != "ns-b" || status.DaemonName != "a" || status.InstanceID != "123" {
				t.Fatalf("scoped image observer: %+v %v", status, err)
			}
			if !slices.Equal(source.imageCalls[0], []string{"rbd", "info", "images/ns-a/volume", "--format", "json"}) || !slices.Equal(destination.imageCalls[1], []string{"rbd", "mirror", "image", "status", "images/ns-b/volume", "--format", "json"}) || len(source.mutations) != sourceMutations || len(destination.mutations) != destinationMutations {
				t.Fatal("observer changed namespace selection or native policy")
			}
		})
	}
}

func TestRBDMirrorImageStatusPreservesDefaultAndMappedNamespaceScopes(t *testing.T) {
	for _, namespaces := range [][2]string{{"", ""}, {"", "ns-b"}, {"ns-a", ""}, {"ns-a", "ns-a"}} {
		link, source, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, namespaces[0], namespaces[1])
		status, err := link.ImageStatus(t.Context(), "volume")
		if err != nil || !status.ReplayReady || status.SourceNamespace != namespaces[0] || status.DestinationNamespace != namespaces[1] {
			t.Fatalf("namespace pair %v was not retained: %+v %v", namespaces, status, err)
		}
		if source.imageCalls[0][2] != rbdMirrorImageSpec("images", namespaces[0], "volume") || destination.imageCalls[0][2] != rbdMirrorImageSpec("images", namespaces[1], "volume") {
			t.Fatalf("namespace pair %v reached a different native image", namespaces)
		}
	}
}

func TestRBDMirrorImageStatusRejectsUnsafeNameAndNativeIdentityDrift(t *testing.T) {
	for _, name := range []string{"", "--force", " volume", "images/volume", "volume@snap", "volume\x00", "volume\n"} {
		link, source, _, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
		if _, err := link.ImageStatus(t.Context(), name); err == nil || len(source.imageCalls) != 0 {
			t.Fatalf("ambiguous name %q reached image CLI", name)
		}
	}
	for _, fault := range []string{"pool", "policy", "global", "status-name", "status-global", "second-info"} {
		t.Run(fault, func(t *testing.T) {
			link, _, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			switch fault {
			case "pool":
				destination.poolID++
			case "policy":
				policy := destination.policies["images"]
				policy.MirrorUUID = "replacement"
				destination.policies["images"] = policy
			case "global":
				destination.imageInfo = rbdImageStatusInfo("destination-local", "snapshot", "foreign", false)
			case "status-name":
				destination.imageStatus = strings.Replace(destination.imageStatus, `"volume"`, `"foreign"`, 1)
			case "status-global":
				destination.imageStatus = strings.Replace(destination.imageStatus, `"global-image"`, `"foreign"`, 1)
			case "second-info":
				destination.infoHook = func(c *rbdImageStatusFixture) {
					if len(c.imageCalls) >= 3 {
						c.imageInfo = rbdImageStatusInfo("replacement-local", "snapshot", "global-image", false)
					}
				}
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			if err == nil || status.ReplayReady {
				t.Fatalf("native replacement %s adopted: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDMirrorImageStatusSchemaFailsClosed(t *testing.T) {
	link, source, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	goodInfo, goodStatus := source.imageInfo, destination.imageStatus
	for _, field := range []string{"name", "id", "mirroring.mode", "mirroring.state", "mirroring.global_id", "mirroring.primary"} {
		for _, null := range []bool{false, true} {
			var fields map[string]any
			_ = json.Unmarshal([]byte(goodInfo), &fields)
			parts := strings.Split(field, ".")
			target, key := fields, parts[0]
			if len(parts) == 2 {
				target, key = fields[parts[0]].(map[string]any), parts[1]
			}
			if null {
				target[key] = nil
			} else {
				delete(target, key)
			}
			bad, _ := json.Marshal(fields)
			source.imageInfo = string(bad)
			if status, err := link.ImageStatus(t.Context(), "volume"); err == nil || status.ReplayReady {
				t.Fatalf("missing/null info %s accepted: %+v %v", field, status, err)
			}
		}
	}
	source.imageInfo = goodInfo
	for _, field := range []string{"name", "global_id", "state", "description", "last_update"} {
		for _, null := range []bool{false, true} {
			var fields map[string]any
			_ = json.Unmarshal([]byte(goodStatus), &fields)
			if null {
				fields[field] = nil
			} else {
				delete(fields, field)
			}
			bad, _ := json.Marshal(fields)
			destination.imageStatus = string(bad)
			if status, err := link.ImageStatus(t.Context(), "volume"); err == nil || status.ReplayReady {
				t.Fatalf("missing/null status %s accepted: %+v %v", field, status, err)
			}
		}
	}
}

func TestRBDMirrorImageStatusStaleNativeUpDoesNotImplyLiveReceiver(t *testing.T) {
	for _, fault := range []string{"stopped", "missing-service", "foreign-service", "wrong-instance", "not-running", "destination-primary", "creating"} {
		t.Run(fault, func(t *testing.T) {
			link, _, destination, daemon := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			switch fault {
			case "stopped":
				daemon.running = false
			case "missing-service":
				destination.imageStatus = `{"name":"volume","global_id":"global-image","state":"up+replaying","description":"replaying","last_update":""}`
			case "foreign-service":
				destination.imageStatus = strings.Replace(destination.imageStatus, "tc-owned", "tc-foreign", 1)
			case "wrong-instance":
				daemon.status = strings.Replace(daemon.status, `"123"`, `"456"`, 1)
			case "not-running":
				daemon.status = strings.Replace(daemon.status, `"running"`, `"stopped"`, 1)
			case "destination-primary":
				destination.imageInfo = rbdImageStatusInfo("destination-local", "snapshot", "global-image", true)
			case "creating":
				destination.imageInfo = strings.Replace(destination.imageInfo, `"enabled"`, `"creating"`, 1)
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			if err != nil || status.ReplayReady {
				t.Fatalf("non-ready state %s was rejected or accepted as ready: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDMirrorImageStatusWaitPinsIdentitiesAndPreservesCancellation(t *testing.T) {
	base := RBDMirrorImageStatus{SourceImageID: "source", DestinationImageID: "destination", GlobalID: "global", State: "down+unknown"}
	for _, source := range []bool{true, false} {
		calls := 0
		status, err := waitRBDMirrorReplay(t.Context(), time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
			calls++
			s := base
			if calls > 1 {
				s.ReplayReady = true
				if source {
					s.SourceImageID = "replacement"
				} else {
					s.DestinationImageID = "replacement"
				}
			}
			return s, nil
		})
		if err == nil || status.ReplayReady || calls != 2 {
			t.Fatalf("wait adopted replacement: %+v %v calls=%d", status, err, calls)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	queryErr := errors.New("private-native-output")
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
		cancel()
		return base, rbdImageQueryError("native query", queryErr)
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, queryErr) || strings.Contains(err.Error(), queryErr.Error()) || status.SourceImageID != base.SourceImageID {
		t.Fatalf("wait lost cancellation/observation or leaked native stderr: %+v %v", status, err)
	}
	link, source, _, daemon := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	link.mu.Lock()
	deadline, stop := context.WithTimeout(t.Context(), 15*time.Millisecond)
	_, err = link.WaitReplayReady(deadline, "volume")
	stop()
	link.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || len(source.imageCalls) != 0 {
		t.Fatal("lock acquisition ignored caller deadline")
	}
	// Public waiting must release its mutation lock between observations.
	daemon.running = false
	deadline, stop = context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer stop()
	started := make(chan struct{})
	source.infoHook = func(*rbdImageStatusFixture) {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	finished := make(chan error, 1)
	go func() { _, err := link.WaitReplayReady(deadline, "volume"); finished <- err }()
	<-started
	if err := lockRGWSyncObservation(deadline, &link.mu); err != nil {
		t.Fatal("wait held mutation lock across polling sleep")
	}
	link.mu.Unlock()
	if err := <-finished; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("non-ready wait lost deadline: %v", err)
	}
}

func TestRBDMirrorImageStatusRejectsMalformedJSONAndWrongTypes(t *testing.T) {
	for _, fault := range []string{
		"info malformed", "info array", "info null", "info ID type", "info primary type",
		"info mode type", "info mode mismatch", "info disabled",
		"status malformed", "status array", "status global type", "status description type",
		"status unsupported state", "status service type", "status service ID type", "status service missing ID", "status all local null",
	} {
		t.Run(fault, func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			switch fault {
			case "info malformed":
				source.imageInfo = "{"
			case "info array":
				source.imageInfo = "[]"
			case "info null":
				source.imageInfo = "null"
			case "info ID type":
				source.imageInfo = strings.Replace(source.imageInfo, "\"source-local\"", "42", 1)
			case "info primary type":
				source.imageInfo = strings.Replace(source.imageInfo, "\"primary\":true", "\"primary\":\"true\"", 1)
			case "info mode type":
				source.imageInfo = strings.Replace(source.imageInfo, "\"snapshot\"", "true", 1)
			case "info mode mismatch":
				source.imageInfo = rbdImageStatusInfo("source-local", "journal", "global-image", true)
			case "info disabled":
				source.imageInfo = strings.Replace(source.imageInfo, "\"enabled\"", "\"disabled\"", 1)
			case "status malformed":
				destination.imageStatus = "{"
			case "status array":
				destination.imageStatus = "[]"
			case "status global type":
				destination.imageStatus = strings.Replace(destination.imageStatus, "\"global-image\"", "42", 1)
			case "status description type":
				destination.imageStatus = strings.Replace(destination.imageStatus, "\"description\":\"replaying\"", "\"description\":false", 1)
			case "status unsupported state":
				destination.imageStatus = strings.Replace(destination.imageStatus, "up+replaying", "up+future_state", 1)
			case "status service type":
				destination.imageStatus = "{\"name\":\"volume\",\"global_id\":\"global-image\",\"state\":\"up+replaying\",\"description\":\"\",\"last_update\":\"\",\"daemon_service\":[]}"
			case "status service ID type":
				destination.imageStatus = strings.Replace(destination.imageStatus, "\"service_id\":\"22\"", "\"service_id\":22", 1)
			case "status service missing ID":
				destination.imageStatus = strings.Replace(destination.imageStatus, "\"service_id\":\"22\",", "", 1)
			case "status all local null":
				destination.imageStatus = "{\"name\":\"volume\",\"global_id\":\"global-image\",\"state\":null,\"description\":null,\"last_update\":null}"
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			var observation *rbdImageObservationError
			if err == nil || !errors.As(err, &observation) || !observation.permanent || status.ReplayReady {
				t.Fatalf("malformed native %s did not fail permanently: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDMirrorImageStatusRejectsUnavailableFixtureBeforeImageReads(t *testing.T) {
	for _, fault := range []string{"nil", "closed", "missing source client", "missing destination client", "missing pools", "missing policies", "missing source cluster", "missing destination cluster", "invalid mode"} {
		t.Run(fault, func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			switch fault {
			case "nil":
				link = nil
			case "closed":
				link.closed = true
			case "missing source client":
				link.sourceClient = nil
			case "missing destination client":
				link.destinationClient = nil
			case "missing pools":
				link.poolIdentities = nil
			case "missing policies":
				link.policyIdentities = nil
			case "missing source cluster":
				link.config.Source = nil
			case "missing destination cluster":
				link.config.Destination = nil
			case "invalid mode":
				link.config.Mode = "future-mode"
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			var observation *rbdImageObservationError
			if err == nil || status.ReplayReady || !errors.As(err, &observation) || !observation.permanent || len(source.imageCalls)+len(destination.imageCalls) != 0 {
				t.Fatalf("unavailable %s reached image reads or passed readiness: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDMirrorImageStatusAcceptsMissingLocalReportAndRecognizedTransitions(t *testing.T) {
	link, _, destination, daemon := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	destination.imageStatus = "{\"name\":\"volume\",\"global_id\":\"global-image\"}"
	status, err := link.ImageStatus(t.Context(), "volume")
	if err != nil || status.ReplayReady || status.State != "" || status.DestinationImageID != "destination-local" {
		t.Fatalf("supported absent local-site report rejected or marked ready: %+v %v", status, err)
	}
	for _, up := range []string{"up", "down"} {
		for _, state := range []string{"unknown", "error", "syncing", "starting_replay", "replaying", "stopping_replay", "stopped"} {
			destination.imageStatus = fmt.Sprintf("{\"name\":\"volume\",\"global_id\":\"global-image\",\"state\":%q,\"description\":\"\",\"last_update\":\"\"}", up+"+"+state)
			status, err := link.ImageStatus(t.Context(), "volume")
			if err != nil || status.ReplayReady || status.State != up+"+"+state {
				t.Fatalf("recognized state rejected or unowned report marked ready: %+v %v", status, err)
			}
		}
	}
	for _, state := range []*container.State{{Running: true, Paused: true}, {Running: true, Restarting: true}, {Running: true, Dead: true}} {
		destination.imageStatus = "{\"name\":\"volume\",\"global_id\":\"global-image\",\"state\":\"up+replaying\",\"description\":\"\",\"last_update\":\"\",\"daemon_service\":{\"service_id\":\"22\",\"instance_id\":\"123\",\"daemon_id\":\"tc-owned\"}}"
		daemon.state = state
		if status, err := link.ImageStatus(t.Context(), "volume"); err != nil || status.ReplayReady {
			t.Fatalf("unavailable receiving process claimed readiness: %+v %v", status, err)
		}
	}
}

func TestRBDMirrorImageStatusRechecksNativePoliciesAfterImageStatus(t *testing.T) {
	for _, fault := range []string{"source pool", "destination pool", "source default UUID", "source selected UUID", "destination selected mapping", "destination site", "source primary", "destination primary", "creating policy drift"} {
		t.Run(fault, func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "ns-a", "ns-b")
			mutate := func() {
				switch fault {
				case "source pool":
					source.poolID++
				case "destination pool":
					destination.poolID++
				case "source default UUID", "source selected UUID":
					spec := "images"
					if fault == "source selected UUID" {
						spec = "images/ns-a"
					}
					policy := source.policies[spec]
					policy.MirrorUUID = "outside-replacement"
					source.policies[spec] = policy
				case "destination selected mapping":
					policy := destination.policies["images/ns-b"]
					policy.RemoteNamespace = stringPointer("outside-mapping")
					destination.policies["images/ns-b"] = policy
				case "destination site":
					destination.site = "outside-site"
				case "source primary":
					source.imageInfo = rbdImageStatusInfo("source-local", "snapshot", "global-image", false)
				case "destination primary":
					destination.imageInfo = rbdImageStatusInfo("destination-local", "snapshot", "global-image", true)
				case "creating policy drift":
					policy := destination.policies["images/ns-b"]
					policy.Mode = "pool"
					destination.policies["images/ns-b"] = policy
				}
			}
			destination.statusHook = func(*rbdImageStatusFixture) { mutate() }
			if fault == "creating policy drift" {
				destination.imageInfo = strings.Replace(destination.imageInfo, "\"enabled\"", "\"creating\"", 1)
				destination.infoHook = func(*rbdImageStatusFixture) { mutate() }
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			var observation *rbdImageObservationError
			if err == nil || status.ReplayReady || !errors.As(err, &observation) || !observation.permanent || status.SourceImageID != "source-local" {
				t.Fatalf("post-read drift %s adopted or partial report lost: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDMirrorWaitReplayReadyRetriesTransientNativeGuardQueries(t *testing.T) {
	for _, policy := range []bool{false, true} {
		link, source, _, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "ns-a", "ns-b")
		queryErr := errors.New("private-native-query-detail")
		failures, observations := 0, 0
		source.execError = func(args []string) error {
			poolQuery := slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"})
			policyQuery := len(args) >= 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"})
			if failures == 0 && ((policy && policyQuery) || (!policy && poolQuery)) {
				failures++
				return queryErr
			}
			return nil
		}
		status, err := waitRBDMirrorReplay(t.Context(), time.Millisecond, func(ctx context.Context) (RBDMirrorImageStatus, error) {
			observations++
			current, err := link.ImageStatus(ctx, "volume")
			if observations == 1 {
				var observation *rbdImageObservationError
				if !errors.Is(err, queryErr) || !errors.As(err, &observation) || observation.permanent || strings.Contains(err.Error(), queryErr.Error()) {
					t.Errorf("transient guard query is permanent, lost cause or leaked detail: %v", err)
				}
			}
			return current, err
		})
		if err != nil || !status.ReplayReady || failures != 1 || observations != 2 {
			t.Fatalf("transient guard did not retry into readiness: %+v %v observations=%d", status, err, observations)
		}
	}
}

func TestRBDMirrorWaitReplayReadyStopsOnMalformedOrChangedGuard(t *testing.T) {
	for _, data := range []string{"{", "null", "{}", "[]", "[{\"pool_id\":42,\"pool_name\":\"images\"}]", "[{\"pool_id\":42,\"pool_name\":\"images\",\"type\":3}]", "[{\"pool_id\":99,\"pool_name\":\"images\",\"type\":1}]", "[{\"pool_id\":42,\"pool_name\":\"images\",\"type\":1},{\"pool_id\":42,\"pool_name\":\"other\",\"type\":1}]"} {
		link, source, _, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
		source.execOutput = func(args []string) (string, bool) {
			return data, slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"})
		}
		calls := 0
		status, err := waitRBDMirrorReplay(t.Context(), time.Millisecond, func(ctx context.Context) (RBDMirrorImageStatus, error) {
			calls++
			return link.ImageStatus(ctx, "volume")
		})
		var observation *rbdImageObservationError
		if err == nil || status.ReplayReady || calls != 1 || !errors.As(err, &observation) || !observation.permanent || len(source.imageCalls) != 0 {
			t.Fatalf("malformed/replaced pool guard did not stop before image reads: %+v %v calls=%d", status, err, calls)
		}
	}
}

func TestRBDMirrorImageStatusDistinguishesPolicyAndDaemonQueryFromSchemaErrors(t *testing.T) {
	for _, fault := range []string{"policy malformed", "policy UUID type", "policy null mapping", "daemon malformed", "daemon missing list", "daemon list type", "daemon instance type", "daemon query", "daemon inspect"} {
		t.Run(fault, func(t *testing.T) {
			link, source, _, daemon := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			queryErr := errors.New("private-native-daemon-query-detail")
			switch fault {
			case "policy malformed", "policy UUID type", "policy null mapping":
				data := "{"
				if fault == "policy UUID type" {
					data = "{\"mode\":\"image\",\"mirror_uuid\":42,\"remote_namespace\":\"\",\"site_name\":\"source\"}"
				}
				if fault == "policy null mapping" {
					data = "{\"mode\":\"image\",\"mirror_uuid\":\"native-images\",\"remote_namespace\":null,\"site_name\":\"source\"}"
				}
				source.execOutput = func(args []string) (string, bool) {
					return data, len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"})
				}
			case "daemon malformed":
				daemon.status = "{"
			case "daemon missing list":
				daemon.status = "{}"
			case "daemon list type":
				daemon.status = "{\"pool_replayers\":{}}"
			case "daemon instance type":
				daemon.status = "{\"pool_replayers\":[{\"pool\":\"images\",\"state\":\"running\",\"instance_id\":123}]}"
			case "daemon query":
				daemon.statusErr = queryErr
			case "daemon inspect":
				daemon.stateErr = queryErr
			}
			status, err := link.ImageStatus(t.Context(), "volume")
			var observation *rbdImageObservationError
			query := fault == "daemon query" || fault == "daemon inspect"
			if err == nil || status.ReplayReady || !errors.As(err, &observation) || observation.permanent == query {
				t.Fatalf("fault %s was not classified safely: %+v %v", fault, status, err)
			}
			if query && (!errors.Is(err, queryErr) || strings.Contains(err.Error(), queryErr.Error())) {
				t.Fatalf("fault %s lost its query cause or exposed native detail: %v", fault, err)
			}
		})
	}
}

func TestRBDMirrorImageStatusDoesNotSubstituteAnotherHealthyOwnedReceiver(t *testing.T) {
	link, _, destination, daemon := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	daemon.running = false
	second := &rbdImageStatusDaemon{running: true, status: daemon.status}
	link.daemons = append(link.daemons, &RBDMirrorDaemon{Container: second, DaemonName: "b", ClientName: "client.rbd-mirror.tc-other"})
	status, err := link.ImageStatus(t.Context(), "volume")
	if err != nil || status.ReplayReady || status.DaemonName != "a" {
		t.Fatalf("healthy receiver b was substituted for stopped report owner a: %+v %v", status, err)
	}
	destination.imageStatus = strings.Replace(destination.imageStatus, "tc-owned", "tc-other", 1)
	status, err = link.ImageStatus(t.Context(), "volume")
	if err != nil || !status.ReplayReady || status.DaemonName != "b" {
		t.Fatalf("new exact image assignment to owned b was not observed: %+v %v", status, err)
	}
}

func TestRBDMirrorImageStatusMatchesExactNativeOwnedDaemonIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, clientName, nativeID string
		ready                      bool
	}{
		{"native registered ID", "client.rbd-mirror.tc-owned", "tc-owned", true},
		{"old client-only stripping", "client.rbd-mirror.tc-owned", "rbd-mirror.tc-owned", false},
		{"full Ceph entity", "client.rbd-mirror.tc-owned", "client.rbd-mirror.tc-owned", false},
		{"foreign same suffix", "client.rbd-mirror.tc-owned", "foreign-tc-owned", false},
		{"extended same prefix", "client.rbd-mirror.tc-owned", "tc-owned-extra", false},
		{"different case", "client.rbd-mirror.tc-owned", "TC-owned", false},
		{"different native receiver", "client.rbd-mirror.tc-owned", "tc-other", false},
		{"foreign Ceph identity prefix", "client.other.tc-owned", "tc-owned", false},
		{"foreign nested identity", "client.rbd-mirror.foreign.tc-owned", "tc-owned", false},
		{"missing Ceph entity prefix", "rbd-mirror.tc-owned", "tc-owned", false},
		{"empty registered owned ID", "client.rbd-mirror.", "tc-owned", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link, _, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
			link.daemons[0].ClientName = tc.clientName
			destination.imageStatus = strings.Replace(destination.imageStatus, "\"daemon_id\":\"tc-owned\"", fmt.Sprintf("\"daemon_id\":%q", tc.nativeID), 1)
			status, err := link.ImageStatus(t.Context(), "volume")
			if err != nil || status.ReplayReady != tc.ready {
				t.Fatalf("native receiver identity matched incorrectly: %+v error=%v expected-ready=%t", status, err, tc.ready)
			}
			if !tc.ready && status.DaemonName != "" {
				t.Fatal("a foreign native identity was attributed to an owned daemon")
			}
		})
	}
}

func TestRBDMirrorWaitReplayReadyPreservesLastReportBeforeQueryFailure(t *testing.T) {
	previous := RBDMirrorImageStatus{Pool: "images", Name: "volume", SourceImageID: "source", DestinationImageID: "destination", GlobalID: "global", State: "down+unknown", Description: "known earlier observation"}
	calls := 0
	ctx, cancel := context.WithCancel(t.Context())
	queryErr := errors.New("private-native-failure")
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
		calls++
		if calls == 1 {
			return previous, nil
		}
		cancel()
		return RBDMirrorImageStatus{}, rbdImageQueryError("native image query", queryErr)
	})
	if calls != 2 || status != previous || !errors.Is(err, context.Canceled) || !errors.Is(err, queryErr) || strings.Contains(err.Error(), queryErr.Error()) {
		t.Fatalf("query failure erased a useful report or error causes: %+v %v calls=%d", status, err, calls)
	}
}

func TestRBDMirrorWaitReplayReadyPinsSourceWhileDestinationAppears(t *testing.T) {
	link, _, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "ns-a", "ns-b")
	absent := errors.New("destination not present yet")
	attempt := 0
	destination.execError = func(args []string) error {
		if attempt == 1 && len(args) > 2 && slices.Equal(args[:2], []string{"rbd", "info"}) {
			return absent
		}
		return nil
	}
	status, err := waitRBDMirrorReplay(t.Context(), time.Millisecond, func(ctx context.Context) (RBDMirrorImageStatus, error) {
		attempt++
		destination.imageInfo = rbdImageStatusInfo("destination-local", "snapshot", "global-image", false)
		if attempt == 2 {
			destination.imageInfo = strings.Replace(destination.imageInfo, "\"enabled\"", "\"creating\"", 1)
		}
		current, err := link.ImageStatus(ctx, "volume")
		if attempt == 1 && (current.SourceImageID != "source-local" || current.DestinationImageID != "" || !errors.Is(err, absent)) {
			t.Errorf("missing destination lost source identity: %+v %v", current, err)
		}
		return current, err
	})
	if err != nil || !status.ReplayReady || status.SourceImageID != "source-local" || status.DestinationImageID != "destination-local" || attempt != 3 {
		t.Fatalf("destination appearance did not preserve the pinned source: %+v %v attempts=%d", status, err, attempt)
	}
}

func TestRBDMirrorWaitReplayReadyDoesNotSucceedAfterCallerExpires(t *testing.T) {
	ready := RBDMirrorImageStatus{SourceImageID: "source", DestinationImageID: "destination", GlobalID: "global", ReplayReady: true}
	ctx, cancel := context.WithCancel(t.Context())
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) { cancel(); return ready, nil })
	if !errors.Is(err, context.Canceled) || status.ReplayReady || status.SourceImageID != "source" {
		t.Fatalf("ready observation bypassed caller cancellation: %+v %v", status, err)
	}
	deadline, stop := context.WithTimeout(t.Context(), time.Millisecond)
	defer stop()
	status, err = waitRBDMirrorReplay(deadline, time.Millisecond, func(ctx context.Context) (RBDMirrorImageStatus, error) {
		<-ctx.Done()
		return ready, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || status.ReplayReady || status.SourceImageID != "source" {
		t.Fatalf("ready observation bypassed caller deadline: %+v %v", status, err)
	}
	link, _, destination, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	destination.statusHook = func(*rbdImageStatusFixture) { cancel() }
	status, err = link.ImageStatus(ctx, "volume")
	if !errors.Is(err, context.Canceled) || status.ReplayReady || status.SourceImageID != "source-local" {
		t.Fatalf("post-status cancellation bypassed final native guards: %+v %v", status, err)
	}
}
