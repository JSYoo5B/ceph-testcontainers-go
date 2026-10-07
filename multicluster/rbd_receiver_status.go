package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// RBDMirrorReceiverStatus observes the selected pool/namespace and an exact
// owned receiver cohort. Ready means scope discovery and native leader/member
// agreement, including an owned leader. It requires no mirrored image and
// proves neither image replay nor delivery of a particular write/checkpoint.
// ForeignInstances makes the exact-cohort predicate false; it does not mean
// that another fixture sharing the receiving pool is unhealthy.
type RBDMirrorReceiverStatus struct {
	Pool, SourceNamespace, DestinationNamespace string
	SourceFSID, DestinationFSID                 string
	SourcePoolID, DestinationPoolID             int64
	PeerID                                      string
	ExpectedDaemons                             []string
	Daemons                                     map[string]RBDMirrorReceiverDaemonStatus
	LeaderDaemonName, LeaderInstanceID          string
	Instances, ForeignInstances                 []string
	DaemonProblems                              map[string]string
	Ready                                       bool
}

// RBDMirrorReceiverDaemonStatus is one current owned receiver's selected native
// pool/peer and namespace discovery, not an aggregate image replay state.
type RBDMirrorReceiverDaemonStatus struct {
	ContainerID, ClientName, StartedAt      string
	InstanceID, LeaderInstanceID, PoolState string
	Running, NamespaceDiscovered, Leader    bool
}

type rbdReceiverMemberWitness struct {
	daemon      *RBDMirrorDaemon
	handle      testcontainers.Container
	cid, client string
}

type rbdReceiverWitness struct {
	peer     *rbdReceiverPeerIdentity
	clusters *rbdReceiverClusterIdentities
	pools    *rbdMirrorPoolIdentities
	policies *rbdMirrorPolicyIdentities
	names    []string
	members  map[string]rbdReceiverMemberWitness
	allOwned bool
	scope    [3]string
}

// ReceiverStatus reads an exact owned cohort in this link's configured scope.
// With no names, all current owned daemons (including stopped/partial ones)
// are expected. Explicit names support waiting for an HA survivor while another
// owned daemon is stopped. It never adopts a process, bootstraps a peer or
// creates an image. The observation takes at most 30 seconds, bounded by ctx,
// including owner/member admission. A partial report may accompany an error.
func (m *RBDMirror) ReceiverStatus(ctx context.Context, expectedDaemons ...string) (RBDMirrorReceiverStatus, error) {
	status, _, err := m.receiverStatus(ctx, expectedDaemons, nil)
	return status, err
}

// WaitReceiverReady waits up to four minutes, bounded by ctx, for the exact
// selected cohort's scope discovery and leader/member agreement. It pins the
// original cluster/pool/policy/peer generation and owned handles/container IDs
// on its first admitted observation; same-name replacement is never adopted.
// Same-container Stop/Start may produce a new native instance. Polls release
// the fixture lock. Cancellation returns the last useful report, with Ready
// cleared, and only canonical context causes in the secret-safe error chain.
func (m *RBDMirror) WaitReceiverReady(ctx context.Context, expectedDaemons ...string) (RBDMirrorReceiverStatus, error) {
	if m == nil {
		return RBDMirrorReceiverStatus{}, rbdReceiverGuard("RBD receiver fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitRBDReceiverReady(ctx, time.Second, func(ctx context.Context, previous *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
		return m.receiverStatus(ctx, expectedDaemons, previous)
	})
}

func waitRBDReceiverReady(ctx context.Context, interval time.Duration, observe func(context.Context, *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error)) (RBDMirrorReceiverStatus, error) {
	var last RBDMirrorReceiverStatus
	var witness *rbdReceiverWitness
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			last.Ready = false
			return last, rbdReceiverQuery(ctx, "wait RBD receiver readiness", errors.Join(err, lastErr))
		}
		current, captured, err := observe(ctx, witness)
		if witness == nil && captured != nil {
			witness = captured
		}
		if current.PeerID != "" {
			last = current
		}
		if err != nil {
			last.Ready = false
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			last.Ready = false
			return last, rbdReceiverQuery(ctx, "wait RBD receiver readiness", errors.Join(err, lastErr))
		}
		if rbdReceiverPermanent(err) {
			return last, err
		}
		if err == nil && current.Ready {
			return current, nil
		}
		select {
		case <-ctx.Done():
			last.Ready = false
			return last, rbdReceiverQuery(ctx, "wait RBD receiver readiness", errors.Join(ctx.Err(), lastErr))
		case <-time.After(interval):
		}
	}
}

func (m *RBDMirror) receiverWitness(names []string, previous *rbdReceiverWitness) (*rbdReceiverWitness, error) {
	if m.closed || !m.receiverSetupConfirmed || m.sourceClient == nil || m.destinationClient == nil || m.config.Source == nil || m.config.Destination == nil || m.receiverClusters == nil || m.receiverPeer == nil || m.poolIdentities == nil || m.policyIdentities == nil {
		return nil, rbdReceiverGuard("RBD receiver bootstrap capability is unavailable or unconfirmed")
	}
	all := len(names) == 0
	owned := make(map[string]*RBDMirrorDaemon)
	for _, d := range m.daemons {
		if d == nil || d.DaemonName == "" || owned[d.DaemonName] != nil {
			return nil, rbdReceiverGuard("RBD owned receiver inventory is malformed")
		}
		owned[d.DaemonName] = d
	}
	if all {
		for name := range owned {
			names = append(names, name)
		}
	} else {
		names = slices.Clone(names)
	}
	slices.Sort(names)
	for i, name := range names {
		if validateRBDMirrorDaemonName(name) != nil || i > 0 && names[i-1] == name || owned[name] == nil {
			return nil, rbdReceiverGuard("RBD expected receiver names must be distinct owned daemons")
		}
	}
	w := &rbdReceiverWitness{peer: m.receiverPeer, clusters: m.receiverClusters, pools: m.poolIdentities, policies: m.policyIdentities, names: names, members: make(map[string]rbdReceiverMemberWitness), allOwned: all, scope: [3]string{m.config.Pool, m.config.SourceNamespace, m.config.DestinationNamespace}}
	for _, name := range names {
		d := owned[name]
		if d.Container == nil || d.receiverHandle == nil || !sameRBDReceiverHandle(d.Container, d.receiverHandle) || !rbdReceiverContainerID(d.receiverContainerID) || d.receiverDaemonName != name || d.Container.GetContainerID() != d.receiverContainerID || d.ClientName != d.receiverClientName {
			return nil, rbdReceiverGuard("RBD original owned receiver handle or identity is unavailable or substituted")
		}
		w.members[name] = rbdReceiverMemberWitness{d, d.receiverHandle, d.receiverContainerID, d.receiverClientName}
	}
	if previous != nil {
		if previous.peer != w.peer || previous.peer.generation != m.receiverPeerGeneration || previous.clusters != w.clusters || previous.pools != w.pools || previous.policies != w.policies || previous.allOwned != w.allOwned || previous.scope != w.scope || !slices.Equal(previous.names, w.names) {
			return nil, rbdReceiverGuard("RBD original receiver scope or cohort changed while waiting")
		}
		for name, current := range w.members {
			if old, ok := previous.members[name]; !ok || old.daemon != current.daemon || !sameRBDReceiverHandle(old.handle, current.handle) || old.cid != current.cid || old.client != current.client {
				return nil, rbdReceiverGuard("RBD original owned receiver changed while waiting")
			}
		}
		return previous, nil
	}
	return w, nil
}

func (m *RBDMirror) receiverStatus(ctx context.Context, names []string, previous *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
	var result RBDMirrorReceiverStatus
	if m == nil {
		return result, previous, rbdReceiverGuard("RBD receiver fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return result, previous, rbdReceiverQuery(ctx, "admit RBD receiver observation", err)
	}
	defer m.mu.Unlock()
	w, err := m.receiverWitness(names, previous)
	if err != nil {
		return result, previous, err
	}
	result = RBDMirrorReceiverStatus{
		Pool: m.config.Pool, SourceNamespace: m.config.SourceNamespace, DestinationNamespace: m.config.DestinationNamespace,
		SourceFSID: w.clusters.source, DestinationFSID: w.clusters.destination, SourcePoolID: w.pools.source, DestinationPoolID: w.pools.destination, PeerID: w.peer.uuid,
		ExpectedDaemons: slices.Clone(w.names), Daemons: make(map[string]RBDMirrorReceiverDaemonStatus), DaemonProblems: make(map[string]string),
	}
	if err := m.checkRBDReceiverIdentities(ctx); err != nil {
		return result, w, err
	}
	var failures []error
	first := make(map[string]rbdReceiverNativeObservation)
	second := make(map[string]rbdReceiverNativeObservation)
	for pass, observations := range []map[string]rbdReceiverNativeObservation{first, second} {
		for _, name := range w.names {
			member := w.members[name]
			observation, err := m.readRBDReceiver(ctx, member)
			observations[name] = observation
			result.Daemons[name] = observation.report
			if observation.problem != "" {
				result.DaemonProblems[name] = observation.problem
			}
			if err != nil {
				failures = append(failures, err)
				var classified *rbdReceiverObservationError
				if errors.As(err, &classified) && classified.permanent {
					return result, w, errors.Join(failures...)
				}
			}
			if pass == 1 && !reflect.DeepEqual(first[name], observation) {
				result.DaemonProblems[name] = "receiver-observation-changing"
			}
		}
	}
	deriveRBDReceiverElection(&result, second)
	if err := m.checkRBDReceiverIdentities(ctx); err != nil {
		return result, w, errors.Join(append(failures, err)...)
	}
	if _, err := m.receiverWitness(names, w); err != nil {
		return result, w, errors.Join(append(failures, err)...)
	}
	if err := ctx.Err(); err != nil {
		return result, w, rbdReceiverQuery(ctx, "confirm RBD receiver observation", errors.Join(append(failures, err)...))
	}
	if len(failures) != 0 {
		return result, w, errors.Join(failures...)
	}
	result.Ready = len(w.names) > 0 && result.LeaderDaemonName != "" && len(result.ForeignInstances) == 0 && len(result.DaemonProblems) == 0
	return result, w, nil
}

type rbdReceiverNativeObservation struct {
	report  RBDMirrorReceiverDaemonStatus
	members []string
	problem string
}

func (m *RBDMirror) readRBDReceiver(ctx context.Context, member rbdReceiverMemberWitness) (rbdReceiverNativeObservation, error) {
	var result rbdReceiverNativeObservation
	result.report.ContainerID, result.report.ClientName = member.cid, member.client
	if err := lockRGWSyncObservation(ctx, &member.daemon.mu); err != nil {
		return result, rbdReceiverQuery(ctx, "admit RBD receiving process observation", err)
	}
	defer member.daemon.mu.Unlock()
	if member.daemon.terminated {
		result.problem = "receiver-terminated"
		return result, nil
	}
	if !member.daemon.receiverStartupConfirmed {
		result.problem = "receiver-startup-unconfirmed"
		return result, nil
	}
	before, err := member.handle.State(ctx)
	if err != nil {
		result.problem = "receiver-state-query-failed"
		return result, rbdReceiverQuery(ctx, "inspect RBD receiving process", err)
	}
	if !normalRBDReceiverProcess(before) {
		result.problem = "receiver-not-running"
		return result, nil
	}
	result.report.Running, result.report.StartedAt = true, before.StartedAt
	data, err := rbdReceiverExec(ctx, member.handle, "read RBD receiving process discovery", "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil {
		result.problem = "receiver-status-query-failed"
		return result, err
	}
	selected, present, err := decodeRBDReceiverDiscovery(data, m.config.Pool, m.receiverPeer)
	if err != nil {
		result.problem = "receiver-status-invalid"
		return result, err
	}
	if !present {
		result.problem = "receiver-scope-undiscovered"
	} else {
		result.report.PoolState, result.report.InstanceID, result.report.LeaderInstanceID = selected.state, selected.instance, selected.leaderInstance
		result.report.Leader, result.members = selected.leader, slices.Clone(selected.members)
		result.report.NamespaceDiscovered = slices.Contains(selected.namespaces, rbdReceiverNamespace{m.config.DestinationNamespace, m.config.SourceNamespace})
		if selected.state != "running" {
			result.problem = "receiver-pool-not-running"
		} else if !result.report.NamespaceDiscovered {
			result.problem = "receiver-namespace-undiscovered"
		}
	}
	after, err := member.handle.State(ctx)
	if err != nil {
		result.problem = "receiver-state-query-failed"
		return result, rbdReceiverQuery(ctx, "recheck RBD receiving process", err)
	}
	if !normalRBDReceiverProcess(after) || before.StartedAt != after.StartedAt || before.Pid != after.Pid {
		result.problem = "receiver-process-changing"
		result.report.Running = false
	}
	return result, nil
}

func normalRBDReceiverProcess(state *container.State) bool {
	if state == nil || !state.Running || state.Paused || state.Restarting || state.Dead || state.Status != container.StateRunning || state.Pid <= 0 || state.Error != "" {
		return false
	}
	start, err := time.Parse(time.RFC3339Nano, state.StartedAt)
	return err == nil && !start.IsZero()
}

type rbdReceiverNamespace struct{ local, remote string }
type rbdReceiverDiscovery struct {
	state, instance, leaderInstance string
	leader                          bool
	members                         []string
	namespaces                      []rbdReceiverNamespace
}

func decodeRBDReceiverDiscovery(data []byte, pool string, peer *rbdReceiverPeerIdentity) (rbdReceiverDiscovery, bool, error) {
	var selected rbdReceiverDiscovery
	if err := rbdReceiverJSON(data); err != nil {
		return selected, false, err
	}
	var native struct {
		Pools json.RawMessage `json:"pool_replayers"`
	}
	if json.Unmarshal(data, &native) != nil {
		return selected, false, rbdReceiverGuard("decode RBD receiver pool discovery")
	}
	var rows []struct {
		Pool           *string         `json:"pool"`
		Peer           *string         `json:"peer"`
		State          *string         `json:"state"`
		Instance       *string         `json:"instance_id"`
		LeaderInstance *string         `json:"leader_instance_id"`
		Leader         *bool           `json:"leader"`
		Instances      json.RawMessage `json:"instances"`
		Namespaces     json.RawMessage `json:"namespace_replayers"`
	}
	if len(native.Pools) == 0 || json.Unmarshal(native.Pools, &rows) != nil || rows == nil {
		return selected, false, rbdReceiverGuard("decode RBD receiver pool discovery array")
	}
	expectedPeer := "uuid: " + peer.uuid + " cluster: " + peer.site + " client: " + peer.client
	found := false
	for _, row := range rows {
		if row.Peer == nil || *row.Peer == "" || row.State == nil || !slices.Contains([]string{"running", "error", "stopped", "stopped (manual)"}, *row.State) {
			return selected, false, rbdReceiverGuard("decode RBD receiver discovery identity/state")
		}
		var namespaces []struct {
			Local  *string         `json:"local_namespace"`
			Remote *string         `json:"remote_namespace"`
			Images json.RawMessage `json:"image_replayers"`
		}
		if len(row.Namespaces) == 0 || json.Unmarshal(row.Namespaces, &namespaces) != nil || namespaces == nil {
			return selected, false, rbdReceiverGuard("decode RBD receiver namespace discovery array")
		}
		entry := rbdReceiverDiscovery{state: *row.State}
		seenNamespaces := make(map[string]bool)
		for _, namespace := range namespaces {
			var images []json.RawMessage
			if namespace.Local == nil || namespace.Remote == nil || seenNamespaces[*namespace.Local] || len(namespace.Images) == 0 || json.Unmarshal(namespace.Images, &images) != nil || images == nil {
				return selected, false, rbdReceiverGuard("decode RBD receiver namespace discovery identity")
			}
			seenNamespaces[*namespace.Local] = true
			entry.namespaces = append(entry.namespaces, rbdReceiverNamespace{*namespace.Local, *namespace.Remote})
		}
		if row.Instance != nil {
			if !rbdReceiverInstance(*row.Instance) {
				return selected, false, rbdReceiverGuard("decode RBD receiver native instance")
			}
			entry.instance = *row.Instance
		}
		if row.LeaderInstance != nil {
			if *row.LeaderInstance != "" && !rbdReceiverInstance(*row.LeaderInstance) {
				return selected, false, rbdReceiverGuard("decode RBD receiver native leader instance")
			}
			entry.leaderInstance = *row.LeaderInstance
		}
		if row.Leader != nil {
			entry.leader = *row.Leader
		}
		if len(row.Instances) != 0 {
			if json.Unmarshal(row.Instances, &entry.members) != nil || entry.members == nil {
				return selected, false, rbdReceiverGuard("decode RBD receiver native membership")
			}
			seen := make(map[string]bool)
			for _, id := range entry.members {
				if !rbdReceiverInstance(id) || seen[id] {
					return selected, false, rbdReceiverGuard("decode RBD receiver duplicate or invalid membership")
				}
				seen[id] = true
			}
			slices.Sort(entry.members)
		}
		if *row.State == "running" && (row.Pool == nil || *row.Pool == "" || row.Instance == nil || row.LeaderInstance == nil || row.Leader == nil || entry.leader && len(row.Instances) == 0) {
			return selected, false, rbdReceiverGuard("decode RBD running receiver election fields")
		}
		if row.Pool == nil || *row.Pool != pool || *row.Peer != expectedPeer {
			continue
		}
		if found {
			return selected, false, rbdReceiverGuard("RBD selected receiver pool/peer discovery is ambiguous")
		}
		selected, found = entry, true
	}
	return selected, found, nil
}

func deriveRBDReceiverElection(result *RBDMirrorReceiverStatus, observations map[string]rbdReceiverNativeObservation) {
	ids := make(map[string]bool)
	ownedIDs := make(map[string]bool)
	for _, name := range result.ExpectedDaemons {
		if id := observations[name].report.InstanceID; rbdReceiverInstance(id) {
			ownedIDs[id] = true
		}
	}
	var leader string
	for _, name := range result.ExpectedDaemons {
		o := observations[name]
		if !o.report.Running || o.report.PoolState != "running" || !o.report.NamespaceDiscovered || !rbdReceiverInstance(o.report.InstanceID) {
			if result.DaemonProblems[name] == "" {
				result.DaemonProblems[name] = "receiver-scope-not-ready"
			}
			continue
		}
		if ids[o.report.InstanceID] {
			result.DaemonProblems[name] = "receiver-instance-ambiguous"
		}
		ids[o.report.InstanceID] = true
		if o.report.Leader {
			if leader != "" {
				result.DaemonProblems[name], result.DaemonProblems[leader] = "receiver-leader-ambiguous", "receiver-leader-ambiguous"
			} else {
				leader = name
			}
		}
	}
	if leader == "" {
		foreign := make(map[string]bool)
		for _, observation := range observations {
			id := observation.report.LeaderInstanceID
			if rbdReceiverInstance(id) && !ownedIDs[id] {
				foreign[id] = true
			}
		}
		for id := range foreign {
			result.ForeignInstances = append(result.ForeignInstances, id)
		}
		slices.Sort(result.ForeignInstances)
		for _, name := range result.ExpectedDaemons {
			if result.DaemonProblems[name] == "" {
				result.DaemonProblems[name] = "owned-leader-unobserved"
			}
		}
		return
	}
	l := observations[leader]
	result.LeaderDaemonName, result.LeaderInstanceID, result.Instances = leader, l.report.InstanceID, slices.Clone(l.members)
	var expected []string
	for id := range ids {
		expected = append(expected, id)
	}
	slices.Sort(expected)
	for _, id := range l.members {
		if !ownedIDs[id] {
			result.ForeignInstances = append(result.ForeignInstances, id)
		}
	}
	if l.report.LeaderInstanceID != l.report.InstanceID || !slices.Equal(expected, l.members) || len(ids) != len(result.ExpectedDaemons) {
		result.DaemonProblems[leader] = "receiver-membership-converging"
	}
	for _, name := range result.ExpectedDaemons {
		if observations[name].report.LeaderInstanceID != l.report.InstanceID {
			result.DaemonProblems[name] = "receiver-leader-converging"
		}
	}
}

func sameRBDReceiverHandle(a, b testcontainers.Container) bool {
	if a == nil || b == nil {
		return false
	}
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	return x.Kind() == reflect.Pointer && y.Kind() == reflect.Pointer && x.Type() == y.Type() && x.Pointer() == y.Pointer()
}
