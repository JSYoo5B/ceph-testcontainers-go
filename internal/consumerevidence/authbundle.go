package consumerevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

// AuthBundleSchema names the consumer's authentication first-failure bundle.
const AuthBundleSchema = "consumer-example/auth-first-failure/v1"

// authExcerptLimit caps the output and error text that a query keeps.
const authExcerptLimit = 1024

// authTextWithheld replaces text that could not be redacted within the
// collection's budget.
const authTextWithheld = "text withheld: redaction did not finish within the collection budget"

// AuthBounds bounds a collection: the budget of the whole collection, the
// bound of each query, and the stdout that a native control keeps.
type AuthBounds struct {
	Budget, Query time.Duration
	ControlOutput int
}

// Provenance names who produced a bundle and for which run.
type Provenance struct {
	Producer string `json:"producer"`
	Subject  string `json:"subject"`
	Run      string `json:"run"`
}

// Rejection is the consumer's record of the first rejection, kept as given.
type Rejection struct {
	Run       string    `json:"run"`
	Attempt   int       `json:"attempt"`
	At        time.Time `json:"at"`
	Target    string    `json:"target"`
	TargetGID uint64    `json:"target_gid"`
	Result    int       `json:"result"`
	Error     string    `json:"error"`
}

// Expected is what the consumer froze right after the rejection, before any
// diagnostic: the cluster, its MONs and the target MGR's registration.
type Expected struct {
	FSID     string   `json:"fsid"`
	Monitors []string `json:"monitors"`
	Manager  string   `json:"manager"`
	GID      uint64   `json:"gid"`
}

// AuthRequest asks for one bundle.
type AuthRequest struct {
	Provenance Provenance `json:"provenance"`
	Expected   Expected   `json:"expected_target"`
	Rejection  Rejection  `json:"first_failure"`
}

// AuthQuery is one query of a collection.
type AuthQuery struct {
	Query
	Kind     string    `json:"kind"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Excerpt  string    `json:"excerpt,omitempty"`
}

// MonitorStatus is one MON's own mon_status through its admin socket.
// AuthEpoch is nil when the MON does not report it.
type MonitorStatus struct {
	Name          string  `json:"name"`
	Rank          int     `json:"rank"`
	State         string  `json:"state"`
	ElectionEpoch uint64  `json:"election_epoch"`
	Quorum        []int   `json:"quorum"`
	MonmapEpoch   uint64  `json:"monmap_epoch"`
	AuthEpoch     *uint64 `json:"auth_epoch"`
	FSID          string  `json:"fsid"`
}

// AuthManager is one owned MGR: its MgrMap GID, its container instance and,
// before the controls, the global IDs of its beacon and module sessions.
type AuthManager struct {
	Name            string   `json:"name"`
	GID             uint64   `json:"gid"`
	Active          bool     `json:"active"`
	ContainerID     string   `json:"container_id"`
	StartedAt       string   `json:"started_at"`
	BeaconGlobalIDs []uint64 `json:"beacon_global_ids"`
	ModuleGlobalIDs []uint64 `json:"module_global_ids"`
}

// AuthBundle is one collection around a first rejection. It holds no
// debug_auth output, which prints keys, and never a repeat of the tested
// authentication.
type AuthBundle struct {
	Schema             string          `json:"schema"`
	Provenance         Provenance      `json:"provenance"`
	Expected           Expected        `json:"expected_target"`
	Rejection          Rejection       `json:"first_failure"`
	Budget             string          `json:"budget"`
	QueryBound         string          `json:"query_bound"`
	Started            time.Time       `json:"started"`
	Ended              time.Time       `json:"ended"`
	Monitors           []MonitorStatus `json:"monitors"`
	Quorum             []string        `json:"quorum_names"`
	Managers           []AuthManager   `json:"managers"`
	ManagersAfter      []AuthManager   `json:"managers_after"`
	Controls           []Control       `json:"native_controls"`
	Queries            []AuthQuery     `json:"queries"`
	CleanupErrors      []string        `json:"cleanup_errors,omitempty"`
	CollectionComplete bool            `json:"collection_complete"`
	GenerationEquality string          `json:"generation_equality"`
}

// AuthVerdict is the consumer's judgment of a bundle.
type AuthVerdict struct {
	Complete           bool     `json:"complete"`
	Problems           []string `json:"problems,omitempty"`
	Target             string   `json:"target"`
	ControlsSucceeded  bool     `json:"controls_succeeded"`
	Result             int      `json:"result"`
	GenerationEquality string   `json:"generation_equality"`
}

// The target MGR as the bundle saw it.
const (
	TargetRegistered  = "registered"
	TargetChanged     = "changed"
	TargetAbsent      = "absent"
	TargetUnavailable = "unavailable"
)

// ClassifyAuthBundle judges a bundle: whether its collection is complete and
// matches the frozen expectation, whether the target MGR kept its frozen
// registration across the collection, and whether every native control
// succeeded. It never reports generation equality.
func ClassifyAuthBundle(bundle AuthBundle) AuthVerdict {
	verdict := AuthVerdict{Result: bundle.Rejection.Result, GenerationEquality: GenerationUnavailable, Target: TargetUnavailable}
	if !bundle.CollectionComplete {
		verdict.Problems = append(verdict.Problems, "the collection is incomplete")
	}
	if bundle.GenerationEquality != GenerationUnavailable {
		verdict.Problems = append(verdict.Problems, "generation equality is claimed")
	}
	names := []string{}
	for _, monitor := range bundle.Monitors {
		names = append(names, monitor.Name)
		if monitor.FSID != bundle.Expected.FSID {
			verdict.Problems = append(verdict.Problems, fmt.Sprintf("mon.%s reports FSID %s", monitor.Name, monitor.FSID))
		}
	}
	slices.Sort(names)
	if expected := slices.Sorted(slices.Values(bundle.Expected.Monitors)); !slices.Equal(names, expected) {
		verdict.Problems = append(verdict.Problems, fmt.Sprintf("MON statuses of %v, expected %v", names, expected))
	}
	if bundle.Rejection.Target != "mgr."+bundle.Expected.Manager || bundle.Rejection.TargetGID != bundle.Expected.GID {
		verdict.Problems = append(verdict.Problems, "the rejection does not name the frozen target")
	}
	find := func(managers []AuthManager) (AuthManager, bool) {
		index := slices.IndexFunc(managers, func(m AuthManager) bool { return m.Name == bundle.Expected.Manager })
		if index < 0 {
			return AuthManager{}, false
		}
		return managers[index], true
	}
	if len(bundle.Managers) > 0 && len(bundle.ManagersAfter) > 0 {
		before, okBefore := find(bundle.Managers)
		after, okAfter := find(bundle.ManagersAfter)
		switch {
		case !okBefore || !okAfter || before.GID == 0 || after.GID == 0:
			verdict.Target = TargetAbsent
		case before.GID != bundle.Expected.GID || after.GID != before.GID || after.ContainerID != before.ContainerID || after.StartedAt != before.StartedAt:
			verdict.Target = TargetChanged
		default:
			verdict.Target = TargetRegistered
		}
	}
	verdict.ControlsSucceeded = len(bundle.Controls) > 0 && !slices.ContainsFunc(bundle.Controls, func(c Control) bool { return c.ExitCode != 0 || !c.Complete })
	verdict.Complete = len(verdict.Problems) == 0
	return verdict
}

// CheckAuthBundleSecrecy refuses data, a marshaled bundle, when it holds a
// field beyond the bundle's schema or anything that diagnostics.Redact would
// mask: a secret that the cluster knows or one of values.
func CheckAuthBundleSecrecy(ctx context.Context, cluster *ceph.Container, data []byte, values ...string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bundle AuthBundle
	if err := decoder.Decode(&bundle); err != nil {
		return fmt.Errorf("the bundle has fields beyond its schema: %w", err)
	}
	redacted, err := diagnostics.Redact(ctx, cluster, data, values...)
	if err != nil {
		return fmt.Errorf("redact the bundle: %w", err)
	}
	if !bytes.Equal(redacted, data) {
		return errors.New("the bundle holds a secret")
	}
	return nil
}

// authCollector runs the bounded queries of one collection.
type authCollector struct {
	cluster *ceph.Container
	ctx     context.Context
	bound   time.Duration
	bundle  *AuthBundle
}

// redact masks the cluster's secrets in text and caps it, within what is left
// of the budget.
func (c *authCollector) redact(text string) string {
	redacted, err := diagnostics.Redact(c.ctx, c.cluster, []byte(text))
	if err != nil {
		return authTextWithheld
	}
	return string(redacted[:min(len(redacted), authExcerptLimit)])
}

// query runs one query within the per-query bound and records it. run returns
// the output it read, which an unsuccessful query keeps as a redacted
// excerpt.
func (c *authCollector) query(name, kind string, run func(context.Context) ([]byte, error)) bool {
	call, cancel := context.WithTimeout(c.ctx, c.bound)
	defer cancel()
	entry := AuthQuery{Query: Query{Name: name, Status: StatusComplete}, Kind: kind, Started: time.Now().UTC()}
	output, err := run(call)
	if err != nil {
		entry.Status, entry.Error = queryStatus(err), c.redact(err.Error())
		if len(output) > 0 {
			entry.Excerpt = c.redact(string(output))
		}
	}
	entry.Finished = time.Now().UTC()
	c.bundle.Queries = append(c.bundle.Queries, entry)
	return err == nil
}

// observeManagers records every owned MGR with its MgrMap GID and container
// instance, and, with sessions, the global IDs of its MON sessions.
func (c *authCollector) observeManagers(phase string, sessions []MonitorSessions) []AuthManager {
	var mgrMap ManagerMap
	c.query("mgr dump "+phase, "observation", func(ctx context.Context) ([]byte, error) {
		data, err := c.cluster.Ceph(ctx, "mgr", "dump", "--format", "json")
		if err == nil {
			mgrMap, err = decodeManagerMap(data)
		}
		return nil, err
	})
	var managers []*ceph.ManagerContainer
	if !c.query("owned MGRs "+phase, "observation", func(ctx context.Context) ([]byte, error) {
		var err error
		managers, err = c.cluster.Managers(ctx)
		return nil, err
	}) {
		return []AuthManager{}
	}
	observed := []AuthManager{}
	for _, manager := range managers {
		gid, _ := mgrMap.registered(manager.DaemonName)
		entry := AuthManager{Name: manager.DaemonName, GID: gid, Active: manager.DaemonName == mgrMap.Active.Name, ContainerID: manager.GetContainerID(),
			BeaconGlobalIDs: []uint64{}, ModuleGlobalIDs: []uint64{}}
		c.query("container of mgr."+manager.DaemonName+" "+phase, "observation", func(ctx context.Context) ([]byte, error) {
			state, err := manager.State(ctx)
			if err == nil && state == nil {
				err = errMissing
			}
			if err == nil {
				entry.StartedAt = state.StartedAt
			}
			return nil, err
		})
		if sessions != nil {
			for _, monitor := range sessions {
				for _, beacon := range monitor.Beacons {
					if beacon.Entity == "mgr."+manager.DaemonName && !slices.Contains(entry.BeaconGlobalIDs, beacon.GlobalID) {
						entry.BeaconGlobalIDs = append(entry.BeaconGlobalIDs, beacon.GlobalID)
					}
				}
			}
			slices.Sort(entry.BeaconGlobalIDs)
			if ids := moduleGlobalIDs(sessions, manager.DaemonName); ids != nil {
				entry.ModuleGlobalIDs = ids
			}
		}
		observed = append(observed, entry)
	}
	return observed
}

// parseMonitorStatus reads a MON's mon_status. Empty output, output that is
// not JSON and a status without its name, FSID or MonMap epoch are errors.
func parseMonitorStatus(data []byte) (MonitorStatus, error) {
	var status struct {
		Name          string `json:"name"`
		Rank          int    `json:"rank"`
		State         string `json:"state"`
		ElectionEpoch uint64 `json:"election_epoch"`
		Quorum        []int  `json:"quorum"`
		Monmap        *struct {
			Epoch     uint64  `json:"epoch"`
			AuthEpoch *uint64 `json:"auth_epoch"`
			FSID      string  `json:"fsid"`
		} `json:"monmap"`
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return MonitorStatus{}, errMissing
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return MonitorStatus{}, err
	}
	if status.Name == "" || status.Monmap == nil || status.Monmap.FSID == "" || status.Monmap.Epoch == 0 {
		return MonitorStatus{}, fmt.Errorf("%w: mon_status without its name, FSID or MonMap epoch", errMissing)
	}
	return MonitorStatus{Name: status.Name, Rank: status.Rank, State: status.State, ElectionEpoch: status.ElectionEpoch,
		Quorum: status.Quorum, MonmapEpoch: status.Monmap.Epoch, AuthEpoch: status.Monmap.AuthEpoch, FSID: status.Monmap.FSID}, nil
}

// CollectAuthBundle collects the bundle of one first rejection, once, within
// bounds.Budget, from a context of its own, since the caller's may have ended
// with the failure. It reads every expected MON's mon_status and sessions
// through its admin socket, the quorum through a MON session, the MgrMap and
// every owned MGR's container instance before and after, and runs one fresh
// "tell <mgr> version" as a new native client for the target and for the
// active MGR. Each query has bounds.Query, records its status and is never
// retried; a failed query does not stop the collection. It never repeats the
// tested authentication, wipes keys, restarts or reconfigures a daemon, or
// raises debug_auth. Only the stop of a control that could not be confirmed as
// stopped runs outside the budget; its failure goes to CleanupErrors.
func CollectAuthBundle(parent context.Context, cluster *ceph.Container, request AuthRequest, bounds AuthBounds) AuthBundle {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), bounds.Budget)
	defer cancel()
	bundle := AuthBundle{Schema: AuthBundleSchema, Provenance: request.Provenance, Expected: request.Expected, Rejection: request.Rejection,
		Budget: bounds.Budget.String(), QueryBound: bounds.Query.String(), Started: time.Now().UTC(), Monitors: []MonitorStatus{},
		Quorum: []string{}, Controls: []Control{}, Queries: []AuthQuery{}, GenerationEquality: GenerationUnavailable}
	collector := &authCollector{cluster: cluster, ctx: ctx, bound: bounds.Query, bundle: &bundle}
	var monitors []*ceph.MonitorContainer
	collector.query("owned MONs", "observation", func(ctx context.Context) ([]byte, error) {
		var err error
		monitors, err = cluster.Monitors(ctx)
		return nil, err
	})
	sessions := []MonitorSessions{}
	for _, name := range request.Expected.Monitors {
		index := slices.IndexFunc(monitors, func(m *ceph.MonitorContainer) bool { return m.DaemonName == name })
		collector.query("mon."+name+" mon_status", "local observation", func(ctx context.Context) ([]byte, error) {
			if index < 0 {
				return nil, fmt.Errorf("%w: mon.%s is not an owned MON", errMissing, name)
			}
			out, err := monitors[index].AdminSocket(ctx, "mon_status")
			if err != nil {
				return out, err
			}
			status, err := parseMonitorStatus(out)
			if err == nil {
				bundle.Monitors = append(bundle.Monitors, status)
			}
			return out, err
		})
		collector.query("mon."+name+" sessions", "local observation", func(ctx context.Context) ([]byte, error) {
			if index < 0 {
				return nil, fmt.Errorf("%w: mon.%s is not an owned MON", errMissing, name)
			}
			out, err := monitors[index].AdminSocket(ctx, "sessions")
			if err != nil {
				return out, err
			}
			observed, err := decodeSessions(name, out)
			if err == nil {
				sessions = append(sessions, observed)
			}
			return out, err
		})
	}
	collector.query("quorum", "messenger observation", func(ctx context.Context) ([]byte, error) {
		observer, err := ceph.Observe(cluster)
		if err != nil {
			return nil, err
		}
		quorum, err := observer.QuorumStatus(ctx)
		if err == nil {
			bundle.Quorum = slices.Sorted(slices.Values(quorum.QuorumNames))
		}
		return nil, err
	})
	bundle.Managers = collector.observeManagers("before", sessions)
	// A standby MGR answers no tell, so the controls address the tested
	// target and the active MGR.
	targets := []string{request.Rejection.Target}
	for _, manager := range bundle.Managers {
		if manager.Active && !slices.Contains(targets, "mgr."+manager.Name) {
			targets = append(targets, "mgr."+manager.Name)
		}
	}
	for _, target := range targets {
		entry := AuthQuery{Kind: "behavioral control", Started: time.Now().UTC()}
		control, query, output, stopErr := tellVersion(ctx, cluster, target, bounds.Query, bounds.ControlOutput)
		entry.Query, entry.Finished = query, time.Now().UTC()
		if !query.Complete() {
			entry.Error = collector.redact(query.Error)
		}
		if !query.Complete() || control.ExitCode != 0 {
			entry.Excerpt = collector.redact(string(output))
		}
		bundle.Controls = append(bundle.Controls, control)
		bundle.Queries = append(bundle.Queries, entry)
		if stopErr != nil {
			bundle.CleanupErrors = append(bundle.CleanupErrors, collector.redact("stop "+target+": "+stopErr.Error()))
		}
	}
	bundle.ManagersAfter = collector.observeManagers("after", nil)
	bundle.Ended = time.Now().UTC()
	bundle.CollectionComplete = len(bundle.CleanupErrors) == 0 &&
		!slices.ContainsFunc(bundle.Queries, func(q AuthQuery) bool { return !q.Complete() })
	return bundle
}
