package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// RBDMirrorImageStatus is a read-only observation of the image in this link's
// selected source and destination namespaces. State, Description and LastUpdate
// are the destination's native local status, not its remote peer status.
// ReplayReady means an owned receiving process is currently replaying this
// source-primary/destination-secondary pair. It proves neither completion of a
// particular write nor completion of a mirror snapshot. Verify application data
// or the application's explicit checkpoint separately.
type RBDMirrorImageStatus struct {
	Pool, SourceNamespace, DestinationNamespace, Name string
	GlobalID, SourceImageID, DestinationImageID       string
	Mode                                              RBDMirrorMode
	SourcePrimary, DestinationPrimary                 bool
	SourceMirrorState, DestinationMirrorState         string
	State, Description, LastUpdate                    string
	DaemonName, InstanceID                            string
	ReplayReady                                       bool
}

// ImageStatus observes one image name without pool/namespace/snapshot syntax.
// It never enables mirroring, creates checkpoints, resyncs, changes primary
// ownership or restarts a process. Original pool and namespace policy identities
// are checked before and after reading the image. Source/destination global IDs
// must agree. A non-zero partial observation can accompany an error.
func (m *RBDMirror) ImageStatus(ctx context.Context, imageName string) (RBDMirrorImageStatus, error) {
	var result RBDMirrorImageStatus
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return result, err
	}
	if m == nil {
		return result, rbdImageGuardError("RBD mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Hold the mutation lock for one bounded observation, never across a poll.
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return result, err
	}
	defer m.mu.Unlock()
	if m.closed || m.sourceClient == nil || m.destinationClient == nil || m.config.Source == nil || m.config.Destination == nil {
		return result, rbdImageGuardError("RBD mirror fixture is unavailable or terminated")
	}
	// Do not allow a hand-constructed or partially bootstrapped public handle
	// to claim readiness without a confirmed pool/policy identity.
	if m.poolIdentities == nil || m.policyIdentities == nil {
		return result, rbdImageGuardError("RBD mirror pool and policies are not confirmed")
	}
	return m.observeRBDMirrorImageLocked(ctx, imageName, nil, nil)
}

// The original owner's gate is held throughout this bounded read. A non-nil
// scope belongs to a retained view; it never replaces the owner's config.
func (m *RBDMirror) observeRBDMirrorImageLocked(ctx context.Context, imageName string, scope *rbdReceiverScope, witness *rbdReceiverWitness) (RBDMirrorImageStatus, error) {
	var result RBDMirrorImageStatus
	pool, sourceNS, destinationNS := m.config.Pool, m.config.SourceNamespace, m.config.DestinationNamespace
	mode := m.config.Mode
	checkIdentities := m.checkRBDMirrorObservedIdentities
	if scope != nil {
		pool, sourceNS, destinationNS = scope.selection[0], scope.selection[1], scope.selection[2]
		mode = "" // The source image, not the owner's original mode, is authoritative.
		checkIdentities = func(ctx context.Context) error { return m.checkRBDReceiverScopeIdentities(ctx, scope) }
	}
	result.Pool, result.Name = pool, imageName
	result.SourceNamespace, result.DestinationNamespace = sourceNS, destinationNS
	if err := checkIdentities(ctx); err != nil {
		return result, err
	}
	if scope == nil {
		var err error
		mode, err = normalizeRBDMirrorMode(mode)
		if err != nil {
			return result, rbdImageGuardError("RBD mirror image mode is invalid")
		}
		result.Mode = mode
	}
	source, err := readRBDMirrorImageInfoForScope(ctx, m.sourceClient, rbdMirrorImageSpec(pool, sourceNS, imageName), imageName, mode, scope != nil)
	if err != nil {
		return result, err
	}
	result.GlobalID, result.SourceImageID, result.SourcePrimary = source.GlobalID, source.ID, source.Primary
	result.SourceMirrorState = source.State
	if scope != nil {
		mode, result.Mode = source.Mode, source.Mode
		if scope.policies.source.selected.Mode == "pool" && mode != RBDMirrorModeJournal {
			return result, rbdImageGuardError("RBD pool-scoped image must use journal mirroring")
		}
	}
	destination, err := readRBDMirrorImageInfoForScope(ctx, m.destinationClient, rbdMirrorImageSpec(pool, destinationNS, imageName), imageName, mode, scope != nil)
	if err != nil {
		return result, err
	}
	result.DestinationImageID, result.DestinationPrimary = destination.ID, destination.Primary
	result.DestinationMirrorState = destination.State
	if source.GlobalID != destination.GlobalID {
		return result, rbdImageGuardError("RBD source and destination image global IDs differ")
	}
	// Creating/disabling are valid transitions, never replay-ready. Native status
	// refuses these states. Preserve identities and let Wait retry.
	if scope == nil && (source.State != "enabled" || destination.State != "enabled") {
		if err := checkIdentities(ctx); err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		return result, nil
	}
	var status rbdObservedStatus
	if source.State == "enabled" && destination.State == "enabled" {
		var err error
		status, err = readRBDMirrorImageStatusForScope(ctx, m.destinationClient, rbdMirrorImageSpec(pool, destinationNS, imageName), imageName, source.GlobalID, scope != nil)
		if err != nil {
			return result, err
		}
	}
	result.State, result.Description, result.LastUpdate = status.State, status.Description, status.LastUpdate
	// A native "up" entry may outlive the process. Attribute an up/replaying
	// report to a currently running owned process and its actual pool instance.
	if source.Primary && !destination.Primary && status.State == "up+replaying" && status.Service != nil {
		if scope != nil {
			for _, name := range witness.names {
				member := witness.members[name]
				nativeID, ownedClient := strings.CutPrefix(member.client, "client.rbd-mirror.")
				if !ownedClient || nativeID == "" || nativeID != status.Service.DaemonID {
					continue
				}
				result.DaemonName, result.InstanceID = name, status.Service.InstanceID
				observed, err := m.readRBDReceiver(ctx, scope, member)
				if err != nil {
					return result, err
				}
				result.ReplayReady = observed.problem == "" && observed.report.Running && observed.report.NamespaceDiscovered && observed.report.PoolState == "running" && observed.report.InstanceID == status.Service.InstanceID
				break
			}
		} else {
			for _, daemon := range m.daemons {
				if daemon == nil || daemon.Container == nil {
					continue
				}
				// AddDaemon creates client.rbd-mirror.<id>. Ceph registers its
				// service as <id>, stripping both the client entity type and the
				// rbd-mirror daemon prefix. Match this exact owned identity.
				nativeDaemonID, ownedClient := strings.CutPrefix(daemon.ClientName, "client.rbd-mirror.")
				if !ownedClient || nativeDaemonID == "" || nativeDaemonID != status.Service.DaemonID {
					continue
				}
				result.DaemonName, result.InstanceID = daemon.DaemonName, status.Service.InstanceID
				state, err := daemon.State(ctx)
				if err != nil {
					return result, rbdImageQueryError("inspect RBD receiving process", err)
				}
				if state == nil || !state.Running || state.Paused || state.Restarting || state.Dead {
					break
				}
				native, err := readRBDMirrorObservedDaemon(ctx, daemon.Container)
				if err != nil {
					return result, err
				}
				result.ReplayReady = slices.ContainsFunc(native.PoolReplayers, func(pool RBDMirrorPoolReplayerStatus) bool {
					return pool.Pool == m.config.Pool && pool.State == "running" && pool.InstanceID == status.Service.InstanceID
				})
				break
			}
		}
	}
	// Detect replacement between info and status instead of blessing a new image
	// with the same name. Also reject source replacement while the receiver read
	// was in progress. This does not impose atomicity on external native edits.
	for _, site := range []struct {
		client testcontainers.Container
		spec   string
		before rbdObservedInfo
	}{
		{m.sourceClient, rbdMirrorImageSpec(pool, sourceNS, imageName), source},
		{m.destinationClient, rbdMirrorImageSpec(pool, destinationNS, imageName), destination},
	} {
		after, err := readRBDMirrorImageInfoForScope(ctx, site.client, site.spec, imageName, mode, scope != nil)
		if err != nil {
			result.ReplayReady = false
			return result, err
		}
		if after != site.before {
			result.ReplayReady = false
			if scope == nil || after.ID != site.before.ID || after.GlobalID != site.before.GlobalID || after.Mode != site.before.Mode || after.Primary != site.before.Primary {
				return result, rbdImageGuardError("RBD image identity or primary ownership changed during observation")
			}
			// A recognized state transition with stable scoped identity is non-ready.
			// Retain all final authority checks and let a later poll observe progress.
		}
	}
	if err := checkIdentities(ctx); err != nil {
		result.ReplayReady = false
		return result, err
	}
	if scope != nil {
		if _, err := m.receiverWitness(scope, nil, witness); err != nil {
			result.ReplayReady = false
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		result.ReplayReady = false
		return result, err
	}
	return result, nil
}

// WaitReplayReady waits for up to four minutes, bounded by the caller context,
// for this link to have a currently running owned receiver replaying the named
// image. It does not wait for particular data or snapshot completion. The first
// observed source identity is pinned even while the destination is not present;
// once a destination appears its local image identity is pinned as well.
// Source and destination image replacements are rejected, never adopted.
// Deadline/cancellation returns the last observation and the last query error.
func (m *RBDMirror) WaitReplayReady(ctx context.Context, imageName string) (RBDMirrorImageStatus, error) {
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return RBDMirrorImageStatus{}, err
	}
	if m == nil {
		return RBDMirrorImageStatus{}, rbdImageGuardError("RBD mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitRBDMirrorReplay(ctx, time.Second, func(attempt context.Context) (RBDMirrorImageStatus, error) {
		return m.ImageStatus(attempt, imageName)
	})
}

func waitRBDMirrorReplay(ctx context.Context, interval time.Duration, observe func(context.Context) (RBDMirrorImageStatus, error)) (RBDMirrorImageStatus, error) {
	var last RBDMirrorImageStatus
	var lastErr error
	var sourceID, destinationID, globalID string
	for {
		if err := ctx.Err(); err != nil {
			last.ReplayReady = false
			return last, fmt.Errorf("wait RBD mirror replay readiness: %w", errors.Join(err, lastErr))
		}
		current, err := observe(ctx)
		if err != nil {
			current.ReplayReady = false
		}
		// Do not erase an earlier useful observation when a later operation fails
		// before it can retrieve any image information.
		if current.SourceImageID != "" {
			last = current
		}
		lastErr = err
		if current.SourceImageID != "" {
			if sourceID != "" && (sourceID != current.SourceImageID || globalID != current.GlobalID) {
				last.ReplayReady = false
				return last, rbdImageGuardError("RBD source image changed while waiting for replay")
			}
			sourceID, globalID = current.SourceImageID, current.GlobalID
		}
		if current.DestinationImageID != "" {
			if destinationID != "" && destinationID != current.DestinationImageID {
				last.ReplayReady = false
				return last, rbdImageGuardError("RBD destination image changed while waiting for replay")
			}
			destinationID = current.DestinationImageID
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			last.ReplayReady = false
			return last, fmt.Errorf("wait RBD mirror replay readiness: %w", errors.Join(ctxErr, lastErr))
		}
		var observation *rbdImageObservationError
		if errors.As(err, &observation) && observation.permanent {
			last.ReplayReady = false
			return last, err
		}
		if err == nil && current.ReplayReady {
			return current, nil
		}
		select {
		case <-ctx.Done():
			last.ReplayReady = false
			return last, fmt.Errorf("wait RBD mirror replay readiness: %w", errors.Join(ctx.Err(), lastErr))
		case <-time.After(interval):
		}
	}
}

type rbdImageObservationError struct {
	message   string
	cause     error
	permanent bool
}

func (e *rbdImageObservationError) Error() string { return e.message }
func (e *rbdImageObservationError) Unwrap() error { return e.cause }

func rbdImageGuardError(message string) error {
	return &rbdImageObservationError{message: message, permanent: true}
}

// Keep arbitrary native stderr out of error text while preserving errors.Is
// classification for a transport/cancellation error through Unwrap.
func rbdImageQueryError(operation string, cause error) error {
	return &rbdImageObservationError{message: operation + " failed", cause: cause}
}

func validateRBDMirrorObservedImage(name string) error {
	if name == "" || strings.TrimSpace(name) != name || strings.HasPrefix(name, "-") || strings.ContainsAny(name, "/@\x00\t\r\n") {
		return rbdImageGuardError("RBD image must be a name without pool, namespace, snapshot or option syntax")
	}
	return nil
}

type rbdObservedInfo struct {
	ID, GlobalID, State string
	Primary             bool
	Mode                RBDMirrorMode
}

func readRBDMirrorObservedInfo(ctx context.Context, client testcontainers.Container, spec, name string, mode RBDMirrorMode) (rbdObservedInfo, error) {
	var result rbdObservedInfo
	data, err := exec(ctx, client, "rbd", "info", spec, "--format", "json")
	if err != nil {
		return result, rbdImageQueryError("read RBD mirror image info", err)
	}
	return decodeRBDMirrorObservedInfo(data, name, mode)
}

func decodeRBDMirrorObservedInfo(data []byte, name string, mode RBDMirrorMode) (rbdObservedInfo, error) {
	var result rbdObservedInfo
	// Explicit native key tags matter: global_id is not Go's GlobalID.
	var tagged struct {
		Name      *string `json:"name"`
		ID        *string `json:"id"`
		Mirroring *struct {
			Mode     *string `json:"mode"`
			State    *string `json:"state"`
			GlobalID *string `json:"global_id"`
			Primary  *bool   `json:"primary"`
		} `json:"mirroring"`
	}
	if json.Unmarshal(data, &tagged) != nil || tagged.Name == nil || tagged.ID == nil || *tagged.Name != name || *tagged.ID == "" || tagged.Mirroring == nil {
		return result, rbdImageGuardError("decode RBD mirror image info: missing or differing name/identity")
	}
	mirror := tagged.Mirroring
	if mirror.Mode == nil || mirror.State == nil || mirror.GlobalID == nil || mirror.Primary == nil || (mode != "" && *mirror.Mode != string(mode) || mode == "" && !slices.Contains([]string{"snapshot", "journal"}, *mirror.Mode)) || !slices.Contains([]string{"creating", "enabled", "disabling"}, *mirror.State) || *mirror.GlobalID == "" {
		return result, rbdImageGuardError("decode RBD mirror image info: missing or differing mirroring fields")
	}
	result.ID, result.GlobalID, result.State, result.Primary = *tagged.ID, *mirror.GlobalID, *mirror.State, *mirror.Primary
	result.Mode = RBDMirrorMode(*mirror.Mode)
	return result, nil
}

type rbdObservedService struct {
	ServiceID  string `json:"service_id"`
	InstanceID string `json:"instance_id"`
	DaemonID   string `json:"daemon_id"`
}

type rbdObservedStatus struct {
	State, Description, LastUpdate string
	Service                        *rbdObservedService
}

func readRBDMirrorObservedStatus(ctx context.Context, client testcontainers.Container, spec, name, globalID string) (rbdObservedStatus, error) {
	var result rbdObservedStatus
	data, err := exec(ctx, client, "rbd", "mirror", "image", "status", spec, "--format", "json")
	if err != nil {
		return result, rbdImageQueryError("read RBD mirror image status", err)
	}
	return decodeRBDMirrorObservedStatus(data, name, globalID)
}

func decodeRBDMirrorObservedStatus(data []byte, name, globalID string) (rbdObservedStatus, error) {
	var result rbdObservedStatus
	var native struct {
		Name        *string             `json:"name"`
		GlobalID    *string             `json:"global_id"`
		State       *string             `json:"state"`
		Description *string             `json:"description"`
		LastUpdate  *string             `json:"last_update"`
		Service     *rbdObservedService `json:"daemon_service"`
	}
	if json.Unmarshal(data, &native) != nil || native.Name == nil || *native.Name != name || native.GlobalID == nil || *native.GlobalID != globalID {
		return result, rbdImageGuardError("decode RBD mirror image status: missing, malformed or differing identity")
	}
	// Ceph omits all local fields when no local site report exists yet.
	// This supported initial state is non-ready, not a malformed schema.
	if native.State == nil && native.Description == nil && native.LastUpdate == nil && native.Service == nil {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		for _, key := range []string{"state", "description", "last_update", "daemon_service"} {
			if _, exists := fields[key]; exists {
				return result, rbdImageGuardError("decode RBD mirror image status: null local fields")
			}
		}
		return result, nil
	}
	if native.State == nil || native.Description == nil || native.LastUpdate == nil || !validRBDMirrorObservedState(*native.State) {
		return result, rbdImageGuardError("decode RBD mirror image status: missing, malformed or differing identity/state")
	}
	// service is optional in v20.2.4; an incomplete present service cannot be
	// used as proof of a live owned receiving daemon.
	if native.Service != nil && (native.Service.ServiceID == "" || native.Service.InstanceID == "" || native.Service.DaemonID == "") {
		return result, rbdImageGuardError("decode RBD mirror image status: incomplete daemon service identity")
	}
	result.State, result.Description, result.LastUpdate, result.Service = *native.State, *native.Description, *native.LastUpdate, native.Service
	return result, nil
}

func validRBDMirrorObservedState(state string) bool {
	parts := strings.Split(state, "+")
	return len(parts) == 2 && slices.Contains([]string{"up", "down"}, parts[0]) && slices.Contains([]string{"unknown", "error", "syncing", "starting_replay", "replaying", "stopping_replay", "stopped"}, parts[1])
}

// Observer guards distinguish unavailable native queries from a successful
// read proving identity drift. Setup clients avoid taking a cluster mutation
// mutex that could outlive this observation's caller deadline.
func (m *RBDMirror) checkRBDMirrorObservedIdentities(ctx context.Context) error {
	for _, site := range []struct {
		client    testcontainers.Container
		poolID    int64
		namespace string
		identity  rbdMirrorSitePolicyIdentity
	}{
		{m.sourceClient, m.poolIdentities.source, m.config.SourceNamespace, m.policyIdentities.source},
		{m.destinationClient, m.poolIdentities.destination, m.config.DestinationNamespace, m.policyIdentities.destination},
	} {
		if err := readRBDMirrorObservedPool(ctx, site.client, m.config.Pool, site.poolID); err != nil {
			return err
		}
		base, err := readRBDMirrorObservedPolicy(ctx, site.client, m.config.Pool, "")
		if err != nil {
			return err
		}
		selected := base
		if site.namespace != "" {
			selected, err = readRBDMirrorObservedPolicy(ctx, site.client, m.config.Pool, site.namespace)
			if err != nil {
				return err
			}
		}
		if !sameRBDMirrorPolicy(base, site.identity.base) || !sameRBDMirrorPolicy(selected, site.identity.selected) {
			return rbdImageGuardError("RBD mirror UUID, scope, remote mapping or site identity changed during observation")
		}
		if err := readRBDMirrorObservedPool(ctx, site.client, m.config.Pool, site.poolID); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func readRBDMirrorObservedPool(ctx context.Context, client testcontainers.Container, name string, expectedID int64) error {
	data, err := exec(ctx, client, "ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return rbdImageQueryError("read RBD mirror pool identity", err)
	}
	var pools []struct {
		ID   *int64  `json:"pool_id"`
		Name *string `json:"pool_name"`
		Type *int    `json:"type"`
	}
	if json.Unmarshal(data, &pools) != nil || pools == nil {
		return rbdImageGuardError("decode RBD mirror pool identity")
	}
	found := false
	seenIDs, seenNames := make(map[int64]bool), make(map[string]bool)
	for _, pool := range pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == nil || *pool.Name == "" || pool.Type == nil ||
			seenIDs[*pool.ID] || seenNames[*pool.Name] || (*pool.Type != 1 && *pool.Type != 3) {
			return rbdImageGuardError("decode RBD mirror pool identity: missing, malformed or duplicate fields")
		}
		seenIDs[*pool.ID], seenNames[*pool.Name] = true, true
		if *pool.Name == name {
			if *pool.ID != expectedID || *pool.Type != 1 {
				return rbdImageGuardError("RBD mirror pool identity or type changed during observation")
			}
			found = true
		}
	}
	if !found {
		return rbdImageGuardError("RBD mirror pool no longer exists")
	}
	return nil
}

func readRBDMirrorObservedPolicy(ctx context.Context, client testcontainers.Container, pool, namespace string) (nativeRBDMirrorPolicy, error) {
	var policy nativeRBDMirrorPolicy
	data, err := exec(ctx, client, "rbd", "mirror", "pool", "info", rbdMirrorNamespaceSpec(pool, namespace), "--format", "json")
	if err != nil {
		return policy, rbdImageQueryError("read RBD mirror namespace policy", err)
	}
	if json.Unmarshal(data, &policy) != nil || !slices.Contains([]string{"disabled", "init-only", "image", "pool"}, policy.Mode) {
		return policy, rbdImageGuardError("decode RBD mirror namespace policy: invalid native mode")
	}
	if policy.Mode != "disabled" && (policy.MirrorUUID == "" || policy.RemoteNamespace == nil) {
		return policy, rbdImageGuardError("decode RBD mirror namespace policy: missing UUID or remote namespace")
	}
	if namespace != "" && policy.Mode == "init-only" {
		return policy, rbdImageGuardError("native init-only mode is invalid on a named namespace")
	}
	return policy, nil
}

func readRBDMirrorObservedDaemon(ctx context.Context, daemon testcontainers.Container) (*RBDMirrorDaemonStatus, error) {
	data, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil {
		return nil, rbdImageQueryError("read RBD receiving process status", err)
	}
	var status RBDMirrorDaemonStatus
	if json.Unmarshal(data, &status) != nil || status.PoolReplayers == nil {
		return nil, rbdImageGuardError("decode RBD receiving process status: missing or malformed pool replayers")
	}
	return &status, nil
}
