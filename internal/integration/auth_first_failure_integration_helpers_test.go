//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// authGenerationUnavailable is all that a stock image can say about whether
// the issuing MON and the verifying MGR used the same rotating-key
// generation: no stock command reports a daemon's effective key set without
// its secrets.
const authGenerationUnavailable = "unavailable/unverified"

// authTestedRejection is the caller's redacted record of the first rejected
// authentication that it tested. The collector stores it as given; it
// neither repeats that authentication nor replaces the record with a later
// result.
type authTestedRejection struct {
	Cycle   int       `json:"cycle"`
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	// Target and TargetGID name the MGR that the client dialed and the GID
	// that the MgrMap registered for it then.
	Target    string `json:"target"`
	TargetGID uint64 `json:"target_gid"`
	ClientID  uint64 `json:"client_id,omitempty"`
	// Service, SecretID and TicketLabel describe the ticket that the client
	// selected; the label is an opaque, run-scoped equality label, never the
	// ticket.
	Service     int     `json:"service,omitempty"`
	SecretID    *uint64 `json:"secret_id,omitempty"`
	TicketLabel string  `json:"ticket_label,omitempty"`
	// RefusedGlobalID is the global ID that a MON's refusal named, if any.
	RefusedGlobalID *uint64 `json:"refused_global_id,omitempty"`
	Result          int     `json:"result"`
	Error           string  `json:"error"`
}

// authHeldSession is the caller's control: a session established before the
// rejection, and whether it was still usable.
type authHeldSession struct {
	ClientID uint64 `json:"client_id"`
	Usable   bool   `json:"usable"`
}

// authTicketRecord is the caller's provenance of a ticket it installed in one
// cycle, with an opaque label.
type authTicketRecord struct {
	Cycle    int    `json:"cycle"`
	Service  int    `json:"service"`
	SecretID uint64 `json:"secret_id"`
	Label    string `json:"label"`
}

// authQuery is one bounded query of a collection. Error holds a redacted,
// shortened error text.
type authQuery struct {
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	Started   time.Time `json:"started"`
	Elapsed   string    `json:"elapsed"`
	Error     string    `json:"error,omitempty"`
	Truncated bool      `json:"truncated,omitempty"`
}

// authMonitorObservation is one MON's own mon_status, read through its admin
// socket. AuthEpoch is nil when the release does not report it.
type authMonitorObservation struct {
	Name          string  `json:"name"`
	Rank          int     `json:"rank"`
	State         string  `json:"state"`
	ElectionEpoch uint64  `json:"election_epoch"`
	Quorum        []int   `json:"quorum"`
	MonmapEpoch   uint64  `json:"monmap_epoch"`
	AuthEpoch     *uint64 `json:"auth_epoch"`
	FSID          string  `json:"fsid"`
}

// authManagerObservation is one MGR: its MgrMap GID, its container instance,
// and the global IDs of its MON sessions, split as after #75 into the beacon
// session (con_type "mgr") and the clients of its modules.
type authManagerObservation struct {
	Name            string   `json:"name"`
	GID             uint64   `json:"gid"`
	Active          bool     `json:"active"`
	ContainerID     string   `json:"container_id"`
	StartedAt       string   `json:"started_at"`
	BeaconGlobalIDs []uint64 `json:"beacon_global_ids"`
	ModuleGlobalIDs []uint64 `json:"module_global_ids"`
}

// authControl is one fresh native command run as a behavioral control. Its
// success shows that a new client authenticated with its own ticket; it says
// nothing about the tested client's ticket.
type authControl struct {
	Target   string `json:"target"`
	Command  string `json:"command"`
	ExitCode int    `json:"exit_code"`
	Complete bool   `json:"complete"`
}

// authFirstFailureBundle is what a collection retains after the first
// rejection of a tested authentication.
type authFirstFailureBundle struct {
	Schema             string                   `json:"schema"`
	Release            string                   `json:"release"`
	Images             map[string]string        `json:"images"`
	FSID               string                   `json:"fsid"`
	Budget             string                   `json:"budget"`
	QueryBound         string                   `json:"query_bound"`
	Started            time.Time                `json:"started"`
	Ended              time.Time                `json:"ended"`
	Rejection          authTestedRejection      `json:"tested_rejection"`
	Held               *authHeldSession         `json:"held_session,omitempty"`
	Tickets            []authTicketRecord       `json:"tickets,omitempty"`
	Monitors           []authMonitorObservation `json:"monitors"`
	Managers           []authManagerObservation `json:"managers"`
	ManagersAfter      []authManagerObservation `json:"managers_after"`
	Controls           []authControl            `json:"controls"`
	Queries            []authQuery              `json:"queries"`
	CleanupErrors      []string                 `json:"cleanup_errors,omitempty"`
	GenerationEquality string                   `json:"generation_equality"`
}

const authBundleSchema = "auth-first-failure/v1"

// authBundleVerdict classifies a bundle. Problems lists what makes it
// incomplete or invalid; the rest describes what it shows.
type authBundleVerdict struct {
	Complete bool
	Problems []string
	// Target is "registered" when the MGR that the client dialed still had
	// the GID that it dialed, "changed" when it had another, and "absent".
	Target string
	// RefusedClient is "registered-mgr" when a MON's refusal named the
	// target's GID, "module-client" when it named another global ID of that
	// MGR, and "unknown" otherwise.
	RefusedClient string
	// ReusedSecretIDs are secret IDs that tickets of different cycles share.
	// A secret ID repeats across wipes and identifies no generation.
	ReusedSecretIDs    []uint64
	ControlsSucceeded  bool
	GenerationEquality string
}

// classifyAuthBundle checks a bundle's completeness and identities. It
// never establishes generation equality: a stock bundle cannot.
func classifyAuthBundle(bundle authFirstFailureBundle) authBundleVerdict {
	verdict := authBundleVerdict{Target: "absent", RefusedClient: "unknown", GenerationEquality: authGenerationUnavailable}
	problem := func(format string, args ...any) {
		verdict.Problems = append(verdict.Problems, fmt.Sprintf(format, args...))
	}
	if bundle.Schema != authBundleSchema {
		problem("schema %q", bundle.Schema)
	}
	rejection := bundle.Rejection
	if rejection.Target == "" || rejection.TargetGID == 0 || rejection.Result == 0 || rejection.At.IsZero() {
		problem("no tested rejection")
	}
	if bundle.GenerationEquality != authGenerationUnavailable {
		problem("generation equality %q: a stock bundle cannot establish it", bundle.GenerationEquality)
	}
	for _, query := range bundle.Queries {
		switch {
		case query.Error != "":
			problem("%s failed: %s", query.Name, query.Error)
		case query.Truncated:
			problem("%s was truncated", query.Name)
		}
	}
	if len(bundle.Monitors) == 0 {
		problem("no MON observed")
	}
	for _, monitor := range bundle.Monitors {
		if monitor.FSID != bundle.FSID {
			problem("mon.%s reports FSID %q, not %q", monitor.Name, monitor.FSID, bundle.FSID)
		}
	}
	if len(bundle.Managers) == 0 || len(bundle.ManagersAfter) == 0 {
		problem("no MGR observed")
	}
	for _, before := range bundle.Managers {
		index := slices.IndexFunc(bundle.ManagersAfter, func(after authManagerObservation) bool { return after.Name == before.Name })
		if index < 0 {
			problem("mgr.%s left during the collection", before.Name)
			continue
		}
		if after := bundle.ManagersAfter[index]; after.GID != before.GID || after.ContainerID != before.ContainerID || after.StartedAt != before.StartedAt {
			problem("mgr.%s changed during the collection: GID %d -> %d, container %s/%s -> %s/%s",
				before.Name, before.GID, after.GID, before.ContainerID, before.StartedAt, after.ContainerID, after.StartedAt)
		}
	}
	if index := slices.IndexFunc(bundle.Managers, func(m authManagerObservation) bool { return "mgr."+m.Name == rejection.Target }); index >= 0 {
		target := bundle.Managers[index]
		verdict.Target = "changed"
		if target.GID == rejection.TargetGID {
			verdict.Target = "registered"
		}
		if refused := rejection.RefusedGlobalID; refused != nil {
			switch {
			case *refused == target.GID:
				verdict.RefusedClient = "registered-mgr"
			case slices.Contains(target.ModuleGlobalIDs, *refused):
				verdict.RefusedClient = "module-client"
			}
		}
	}
	labels := map[uint64]map[string]bool{}
	cycles := map[uint64]map[int]bool{}
	record := func(cycle int, secretID uint64, label string) {
		if labels[secretID] == nil {
			labels[secretID], cycles[secretID] = map[string]bool{}, map[int]bool{}
		}
		labels[secretID][label], cycles[secretID][cycle] = true, true
	}
	for _, ticket := range bundle.Tickets {
		record(ticket.Cycle, ticket.SecretID, ticket.Label)
	}
	if rejection.SecretID != nil {
		record(rejection.Cycle, *rejection.SecretID, rejection.TicketLabel)
	}
	for secretID, seen := range cycles {
		if len(seen) > 1 {
			verdict.ReusedSecretIDs = append(verdict.ReusedSecretIDs, secretID)
		}
	}
	slices.Sort(verdict.ReusedSecretIDs)
	verdict.ControlsSucceeded = len(bundle.Controls) > 0 && !slices.ContainsFunc(bundle.Controls, func(c authControl) bool { return c.ExitCode != 0 || !c.Complete })
	verdict.Complete = len(verdict.Problems) == 0
	return verdict
}

// checkAuthBundleSecrecy refuses a bundle whose JSON holds any of secrets,
// or a field that the bundle does not define.
func checkAuthBundleSecrecy(data []byte, secrets ...string) error {
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(data, []byte(secret)) {
			return errors.New("the bundle holds a secret")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bundle authFirstFailureBundle
	if err := decoder.Decode(&bundle); err != nil {
		return fmt.Errorf("the bundle has fields beyond its schema: %w", err)
	}
	return nil
}

// authCollector runs the bounded queries of one collection.
type authCollector struct {
	cluster *ceph.Container
	ctx     context.Context
	bound   time.Duration
	bundle  *authFirstFailureBundle
}

// query runs one query within the per-query bound and records it.
func (c *authCollector) query(name, kind string, run func(context.Context) (truncated bool, err error)) bool {
	ctx, cancel := context.WithTimeout(c.ctx, c.bound)
	defer cancel()
	started := time.Now()
	truncated, err := run(ctx)
	entry := authQuery{Name: name, Kind: kind, Started: started.UTC(), Elapsed: time.Since(started).Round(time.Millisecond).String(), Truncated: truncated}
	if err != nil {
		text := err.Error()
		redactCtx, stop := context.WithTimeout(context.WithoutCancel(c.ctx), 10*time.Second)
		if redacted, redactErr := c.cluster.RedactSecrets(redactCtx, []byte(text)); redactErr == nil {
			text = string(redacted)
		} else {
			text = "error text withheld: " + redactErr.Error()
		}
		stop()
		entry.Error = text[:min(len(text), 300)]
	}
	c.bundle.Queries = append(c.bundle.Queries, entry)
	return err == nil && !truncated
}

// observeManagers records every owned MGR with its MgrMap GID and container
// instance, and, when sessions is set, the global IDs of its MON sessions.
func (c *authCollector) observeManagers(sessions map[string][]monSession) []authManagerObservation {
	var status ceph.ManagerStatus
	c.query("mgr dump", "observation", func(ctx context.Context) (bool, error) {
		var err error
		status, err = c.cluster.ManagerStatus(ctx)
		return false, err
	})
	gids := map[string]uint64{status.ActiveName: status.ActiveGID}
	for _, standby := range status.Standbys {
		gids[standby.Name] = standby.GID
	}
	var observed []authManagerObservation
	for _, manager := range c.cluster.Managers() {
		entry := authManagerObservation{Name: manager.DaemonName, GID: gids[manager.DaemonName], Active: manager.DaemonName == status.ActiveName,
			ContainerID: manager.GetContainerID()}
		c.query("state of mgr."+manager.DaemonName, "observation", func(ctx context.Context) (bool, error) {
			state, err := manager.State(ctx)
			if err == nil {
				entry.StartedAt = state.StartedAt
			}
			return false, err
		})
		if sessions != nil {
			beacon, others := splitManagerSessions(sessions["mgr."+manager.DaemonName])
			for _, session := range beacon {
				entry.BeaconGlobalIDs = append(entry.BeaconGlobalIDs, session.GlobalID)
			}
			slices.Sort(entry.BeaconGlobalIDs)
			entry.BeaconGlobalIDs = slices.Compact(entry.BeaconGlobalIDs)
			entry.ModuleGlobalIDs = slices.Compact(others)
		}
		observed = append(observed, entry)
	}
	return observed
}

// collectAuthFirstFailureBundle collects the bundle of one tested rejection
// within budget, from a context of its own, since the test's own may have
// ended with the failure, and bounds each query by bound. It reads every
// owned MON's mon_status and sessions through its admin socket, the MgrMap
// and each MGR's container instance before and after, and runs a fresh
// "tell mgr.<name> version" from control for the tested target and the
// active MGR as a behavioral control. A query that fails or is truncated is recorded, not retried, and
// the rejection is kept as given.
func collectAuthFirstFailureBundle(parent context.Context, cluster *ceph.Container, control testcontainers.Container, images map[string]string,
	rejection authTestedRejection, held *authHeldSession, tickets []authTicketRecord, budget, bound time.Duration) authFirstFailureBundle {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), budget)
	defer cancel()
	bundle := authFirstFailureBundle{Schema: authBundleSchema, Images: images, Budget: budget.String(), QueryBound: bound.String(),
		Started: time.Now().UTC(), Rejection: rejection, Held: held, Tickets: tickets, GenerationEquality: authGenerationUnavailable}
	collector := &authCollector{cluster: cluster, ctx: ctx, bound: bound, bundle: &bundle}
	sessions := map[string][]monSession{}
	for _, monitor := range cluster.Monitors() {
		name := monitor.DaemonName
		collector.query("mon."+name+" mon_status", "observation", func(ctx context.Context) (bool, error) {
			out, err := cluster.AdminSocket(ctx, "mon."+name, "mon_status")
			if err != nil {
				return false, err
			}
			var status struct {
				Name          string `json:"name"`
				Rank          int    `json:"rank"`
				State         string `json:"state"`
				ElectionEpoch uint64 `json:"election_epoch"`
				Quorum        []int  `json:"quorum"`
				Monmap        struct {
					Epoch     uint64  `json:"epoch"`
					AuthEpoch *uint64 `json:"auth_epoch"`
					FSID      string  `json:"fsid"`
				} `json:"monmap"`
			}
			if err := json.Unmarshal(out, &status); err != nil {
				return false, fmt.Errorf("mon_status: %w", err)
			}
			bundle.Monitors = append(bundle.Monitors, authMonitorObservation{Name: status.Name, Rank: status.Rank, State: status.State,
				ElectionEpoch: status.ElectionEpoch, Quorum: status.Quorum, MonmapEpoch: status.Monmap.Epoch, AuthEpoch: status.Monmap.AuthEpoch, FSID: status.Monmap.FSID})
			if bundle.FSID == "" {
				bundle.FSID = status.Monmap.FSID
			}
			return false, nil
		})
		collector.query("mon."+name+" sessions", "observation", func(ctx context.Context) (bool, error) {
			out, err := cluster.AdminSocket(ctx, "mon."+name, "sessions")
			if err != nil {
				return false, err
			}
			var list []monSession
			if err := json.Unmarshal(out, &list); err != nil {
				return false, fmt.Errorf("sessions: %w", err)
			}
			for _, session := range list {
				if strings.HasPrefix(session.EntityName, "mgr.") {
					sessions[session.EntityName] = append(sessions[session.EntityName], session)
				}
			}
			return false, nil
		})
	}
	bundle.Managers = collector.observeManagers(sessions)
	// A standby MGR answers no tell, so the controls address the tested
	// target and the active MGR.
	targets := []string{rejection.Target}
	for _, manager := range bundle.Managers {
		if manager.Active && !slices.Contains(targets, "mgr."+manager.Name) {
			targets = append(targets, "mgr."+manager.Name)
		}
	}
	for _, target := range targets {
		collector.query("tell "+target+" version", "behavioral-control", func(ctx context.Context) (bool, error) {
			result, err := cluster.RunNativeCommand(ctx, control, ceph.NativeCommand{
				Argv: []string{"ceph", "--connect-timeout", "5", "tell", target, "version"}, Timeout: bound,
				MaxStdout: 4096, MaxStderr: 16 << 10, Label: "tell " + target + " version",
			})
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
				if stopErr := unstopped.Stop(stopCtx); stopErr != nil {
					bundle.CleanupErrors = append(bundle.CleanupErrors, fmt.Sprintf("stop %s: %v", target, stopErr))
				}
				stop()
			}
			bundle.Controls = append(bundle.Controls, authControl{Target: target, Command: "version", ExitCode: result.ExitCode, Complete: err == nil && result.Complete()})
			if err == nil && bundle.Release == "" && result.ExitCode == 0 {
				var version struct {
					Version string `json:"version"`
					Release string `json:"release"`
				}
				if json.Unmarshal(result.Stdout, &version) == nil {
					bundle.Release = version.Version + " " + version.Release
				}
			}
			if errors.Is(err, ceph.ErrNativeCommandIncomplete) {
				return true, nil
			}
			return false, err
		})
	}
	bundle.ManagersAfter = collector.observeManagers(nil)
	bundle.Ended = time.Now().UTC()
	return bundle
}

// testAuthFirstFailureBundle runs the collection recipe on a fixture of three
// MONs and two MGRs after one rotating-key wipe, where the MONs have that
// command. Its tested authentication is
// a native client with a wrong key, which the MONs refuse; it runs once and
// is never retried. The bundle must be complete, keep that rejection as it
// was recorded, show the target MGR still registered with the GID it had,
// record successful behavioral controls, and leave generation equality
// unavailable/unverified. It must hold neither the admin key nor a field
// beyond its schema. A second collection whose budget ends at once fails
// every query and still keeps the rejection.
func testAuthFirstFailureBundle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	cluster, client := newServiceClusterWithOptions(t, image, append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs())...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	before, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var monmap struct {
		Epoch uint64 `json:"epoch"`
	}
	if err := json.Unmarshal(before, &monmap); err != nil {
		t.Fatal(err)
	}
	// One wipe, as in a consumer's cycle; every MON's own MonMap must move
	// past it before the tested authentication. MONs that do not know the
	// command, such as Squid's, answer that no valid command was found; the
	// collection then runs without a wipe.
	wiped := true
	if _, err := cluster.Ceph(ctx, "auth", "wipe-rotating-service-keys"); err != nil {
		if !strings.Contains(err.Error(), "no valid command found") {
			t.Fatal("wipe rotating service keys:", err)
		}
		wiped = false
		t.Log("the MONs do not know auth wipe-rotating-service-keys; collecting without a wipe")
	}
	for _, monitor := range cluster.Monitors() {
		if !wiped {
			break
		}
		deadline := time.Now().Add(time.Minute)
		for {
			out, err := cluster.AdminSocket(ctx, "mon."+monitor.DaemonName, "mon_status")
			var status struct {
				Monmap struct {
					Epoch uint64 `json:"epoch"`
				} `json:"monmap"`
			}
			if err == nil && json.Unmarshal(out, &status) == nil && status.Monmap.Epoch > monmap.Epoch {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("mon.%s did not move past MonMap epoch %d: %v", monitor.DaemonName, monmap.Epoch, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	status, err := cluster.ManagerStatus(ctx)
	if err != nil || !status.Available {
		t.Fatalf("no active MGR: %+v, %v", status, err)
	}
	target := "mgr." + status.ActiveName
	attempted := time.Now().UTC()
	tested, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv:    []string{"sh", "-c", `ceph-authtool --create-keyring /tmp/wrong.keyring --gen-key -n client.admin >/dev/null && exec ceph --keyring /tmp/wrong.keyring --connect-timeout 10 tell "$1" version`, "sh", target},
		Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 16 << 10, Label: "tested authentication",
	})
	// Tentacle's MonClient logs "(13) Permission denied" and Squid's
	// "allowed_methods [2] but i only support [2]"; both end with errno 13.
	if err != nil || tested.ExitCode != 13 || !bytes.Contains(tested.Stderr, []byte("[errno 13] RADOS permission denied")) {
		t.Fatalf("the tested authentication was not refused: exit=%d err=%v stderr=%.512s", tested.ExitCode, err, tested.Stderr)
	}
	lines := strings.Split(strings.TrimSpace(string(tested.Stderr)), "\n")
	rejection := authTestedRejection{Cycle: 1, Attempt: 1, At: attempted, Target: target, TargetGID: status.ActiveGID, Result: -13, Error: lines[len(lines)-1]}
	recorded := rejection

	adminKey, err := cluster.Ceph(ctx, "auth", "get-key", "client.admin")
	if err != nil {
		t.Fatal(err)
	}
	bundle := collectAuthFirstFailureBundle(ctx, cluster, client, map[string]string{"control": image}, rejection, nil, nil, time.Minute, 15*time.Second)
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAuthBundleSecrecy(data, strings.TrimSpace(string(adminKey))); err != nil {
		t.Fatal(err)
	}
	verdict := classifyAuthBundle(bundle)
	if !verdict.Complete || verdict.Target != "registered" || verdict.RefusedClient != "unknown" || !verdict.ControlsSucceeded ||
		verdict.GenerationEquality != authGenerationUnavailable || len(bundle.Monitors) != 3 || len(bundle.Managers) != 2 || len(bundle.CleanupErrors) != 0 {
		t.Fatalf("verdict %+v for bundle %s", verdict, data)
	}
	if bundle.Rejection != recorded {
		t.Fatalf("the rejection changed: %+v, recorded %+v", bundle.Rejection, recorded)
	}
	for _, monitor := range bundle.Monitors {
		if wiped && monitor.MonmapEpoch <= monmap.Epoch {
			t.Fatalf("mon.%s reports MonMap epoch %d, not past %d", monitor.Name, monitor.MonmapEpoch, monmap.Epoch)
		}
	}
	for _, manager := range bundle.Managers {
		if len(manager.BeaconGlobalIDs) != 1 || manager.BeaconGlobalIDs[0] != manager.GID {
			t.Fatalf("mgr.%s beacon sessions %v, GID %d", manager.Name, manager.BeaconGlobalIDs, manager.GID)
		}
	}
	// Diagnostics that fail keep the rejection: with a budget that ends at
	// once, every query fails, the bundle is incomplete, and the rejection is
	// still the one recorded.
	starved := collectAuthFirstFailureBundle(ctx, cluster, client, map[string]string{"control": image}, rejection, nil, nil, time.Nanosecond, time.Second)
	if starvedVerdict := classifyAuthBundle(starved); starvedVerdict.Complete || starved.Rejection != recorded || len(starved.Queries) == 0 ||
		slices.ContainsFunc(starved.Queries, func(q authQuery) bool { return q.Error == "" }) {
		t.Fatalf("a starved collection gave verdict %+v, rejection %+v, queries %+v", starvedVerdict, starved.Rejection, starved.Queries)
	}
	// The controls ran under the supervisor; none may have left a marker.
	if code, out, err := sessionExec(ctx, client, "sh", "-c", "ls -A /tmp/tc-supervised 2>/dev/null"); err != nil || strings.TrimSpace(out) != "" {
		t.Fatalf("supervisor markers remain: %d %q %v", code, out, err)
	}
	t.Logf("AUTH_FIRST_FAILURE_BUNDLE wiped=%t verdict %+v\n%s", wiped, verdict, data)
}
