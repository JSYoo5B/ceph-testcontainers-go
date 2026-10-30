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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// authGenerationUnavailable is all that a stock image can say about whether
// the issuing MON and the verifying MGR used the same rotating-key
// generation: no stock command reports a daemon's effective key set without
// its secrets.
const authGenerationUnavailable = "unavailable/unverified"

const authBundleSchema = "auth-first-failure/v1"

// authStopAllowance bounds the stop of one control that RunNativeCommand
// could not confirm as stopped. It is a cleanup allowance outside the
// collection budget, since a command that may still run must not be left
// behind.
const authStopAllowance = time.Minute

// Statuses of one query of a collection.
const (
	authQueryComplete    = "complete"
	authQueryUnsupported = "unsupported"
	authQueryTimedOut    = "timed_out"
	authQueryTruncated   = "truncated"
	authQueryFailed      = "failed"
	authQueryUnavailable = "unavailable"
)

// authProvenance names, as the caller declares them, the producer and subject
// sources, the run and a report. The collector does not verify them.
type authProvenance struct {
	Producer string `json:"producer"`
	Subject  string `json:"subject"`
	Run      string `json:"run"`
	Report   string `json:"report,omitempty"`
}

// authTestedRejection is the caller's redacted record of the first rejected
// authentication that it tested. The collector stores it as given; it
// neither repeats that authentication nor replaces the record with a later
// result.
type authTestedRejection struct {
	Run     string    `json:"run"`
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

// authExpectedTarget is the identity that the caller froze when the
// authentication failed, before any diagnostic ran: the cluster, its MONs and
// the target MGR's GID and container instance.
type authExpectedTarget struct {
	FSID        string   `json:"fsid"`
	Monitors    []string `json:"monitors"`
	Manager     string   `json:"manager"`
	GID         uint64   `json:"gid"`
	ContainerID string   `json:"container_id"`
	StartedAt   string   `json:"started_at"`
}

// authHeldSession is the caller's control: a session established before the
// rejection, and whether it was still usable.
type authHeldSession struct {
	ClientID uint64 `json:"client_id"`
	Usable   bool   `json:"usable"`
}

// authTicketRecord is the caller's provenance of a ticket it installed, with
// an opaque, run-scoped label and the epochs that its issuer connection
// showed. Those epochs are not carried by the ticket.
type authTicketRecord struct {
	Run             string `json:"run"`
	Cycle           int    `json:"cycle"`
	Service         int    `json:"service"`
	SecretID        uint64 `json:"secret_id"`
	Label           string `json:"label"`
	IssuerMonmap    uint64 `json:"issuer_monmap_epoch,omitempty"`
	IssuerAuthEpoch uint64 `json:"issuer_auth_epoch,omitempty"`
}

// authQuery is one bounded query of a collection. Error and Excerpt are
// redacted and capped; Excerpt keeps output that did not yield a complete
// observation.
type authQuery struct {
	Name     string    `json:"name"`
	Kind     string    `json:"kind"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Status   string    `json:"status"`
	Error    string    `json:"error,omitempty"`
	Excerpt  string    `json:"excerpt,omitempty"`
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
// and the global IDs of its MON sessions, split into the beacon
// session (con_type "mgr") and the clients of its modules. GID 0 means that
// the MgrMap could not be read.
type authManagerObservation struct {
	Name            string   `json:"name"`
	GID             uint64   `json:"gid"`
	Active          bool     `json:"active"`
	ContainerID     string   `json:"container_id"`
	StartedAt       string   `json:"started_at"`
	BeaconGlobalIDs []uint64 `json:"beacon_global_ids"`
	ModuleGlobalIDs []uint64 `json:"module_global_ids"`
}

// authControl is one fresh native command run as a behavioral control after
// the rejection. Its success shows that a new client authenticated with its
// own ticket; it says nothing about the tested client's ticket.
type authControl struct {
	Target string `json:"target"`
	// Operation is the control's own attempt identity, never the rejected
	// attempt's.
	Operation string    `json:"operation,omitempty"`
	Command   string    `json:"command"`
	Phase     string    `json:"phase"`
	Started   time.Time `json:"started"`
	ExitCode  int       `json:"exit_code"`
	Complete  bool      `json:"complete"`
}

// authFirstFailureBundle is what a collection retains after the first
// rejection of a tested authentication.
type authFirstFailureBundle struct {
	Schema     string              `json:"schema"`
	Provenance authProvenance      `json:"provenance"`
	Release    string              `json:"release"`
	Images     map[string]string   `json:"images"`
	Expected   authExpectedTarget  `json:"expected_target"`
	Budget     string              `json:"budget"`
	QueryBound string              `json:"query_bound"`
	Started    time.Time           `json:"started"`
	Ended      time.Time           `json:"ended"`
	Rejection  authTestedRejection `json:"first_failure"`
	// FirstExcerpt is the redacted, bounded excerpt of the original rejected
	// operation's own stderr, nil when the producer attached none.
	FirstExcerpt       *authFirstExcerptRecord  `json:"first_failure_excerpt,omitempty"`
	Held               *authHeldSession         `json:"held_session,omitempty"`
	Tickets            []authTicketRecord       `json:"consumer_tickets,omitempty"`
	Monitors           []authMonitorObservation `json:"monitors"`
	Quorum             []string                 `json:"quorum_names,omitempty"`
	Managers           []authManagerObservation `json:"managers"`
	ManagersAfter      []authManagerObservation `json:"managers_after"`
	Controls           []authControl            `json:"native_controls"`
	Queries            []authQuery              `json:"queries"`
	CleanupErrors      []string                 `json:"cleanup_errors,omitempty"`
	CollectionComplete bool                     `json:"collection_complete"`
	GenerationEquality string                   `json:"generation_equality"`
}

// authBundleVerdict classifies a bundle. Problems lists what makes it
// incomplete or invalid; the rest describes what it shows.
type authBundleVerdict struct {
	Complete bool
	Problems []string
	// Target is "registered" when the MGR that the client dialed still had
	// the GID and container instance that the caller froze, "changed" when
	// either differs, "absent" when the MgrMap was read without it, and
	// "unavailable" when its observation is missing.
	Target string
	// RefusedClient is "registered-mgr" when a MON's refusal named the
	// target's GID, "module-client" when it named another global ID of that
	// MGR, and "unknown" otherwise.
	RefusedClient string
	// ReusedSecretIDs are secret IDs that tickets of different runs or
	// cycles share. A secret ID repeats across wipes and identifies no
	// generation.
	ReusedSecretIDs   []uint64
	ControlsSucceeded bool
	// ExcerptStatus is the first-failure excerpt's own status, or "absent".
	// It is separate from the command's stderr completeness and does not make
	// a bundle incomplete: a refusal may leave no prefix.
	ExcerptStatus string
	// Cause is always unavailable: no excerpt or control establishes it.
	Cause              string
	Result             int
	GenerationEquality string
}

// classifyAuthBundle checks a bundle's completeness and associations. It
// never establishes generation equality: a stock bundle cannot.
func classifyAuthBundle(bundle authFirstFailureBundle) authBundleVerdict {
	rejection := bundle.Rejection
	verdict := authBundleVerdict{Target: "unavailable", RefusedClient: "unknown", Result: rejection.Result, GenerationEquality: authGenerationUnavailable,
		ExcerptStatus: "absent", Cause: authCauseUnavailable}
	problem := func(format string, args ...any) {
		verdict.Problems = append(verdict.Problems, fmt.Sprintf(format, args...))
	}
	if bundle.Schema != authBundleSchema {
		problem("schema %q", bundle.Schema)
	}
	if rejection.Target == "" || rejection.TargetGID == 0 || rejection.Result == 0 || rejection.At.IsZero() {
		problem("no tested rejection")
	}
	if bundle.GenerationEquality != authGenerationUnavailable {
		problem("generation equality %q: a stock bundle cannot establish it", bundle.GenerationEquality)
	}
	for _, query := range bundle.Queries {
		if query.Status != authQueryComplete {
			problem("%s: %s %s", query.Name, query.Status, query.Error)
		}
	}
	for _, cleanup := range bundle.CleanupErrors {
		problem("cleanup: %s", cleanup)
	}
	if bundle.FirstExcerpt != nil {
		verdict.ExcerptStatus = bundle.FirstExcerpt.Status
		if err := checkAuthFirstExcerpt(*bundle.FirstExcerpt, rejection); err != nil {
			problem("first-failure excerpt: %v", err)
		}
	}
	for _, control := range bundle.Controls {
		if control.Operation != "" && control.Operation == authRejectionOperation(rejection) {
			problem("control %s reuses the rejected attempt's identity", control.Target)
		}
	}
	expected := bundle.Expected
	for _, name := range expected.Monitors {
		index := slices.IndexFunc(bundle.Monitors, func(m authMonitorObservation) bool { return m.Name == name })
		switch {
		case index < 0:
			problem("mon.%s unavailable", name)
		case bundle.Monitors[index].FSID != expected.FSID:
			problem("mon.%s reports FSID %q, not %q", name, bundle.Monitors[index].FSID, expected.FSID)
		}
	}
	for _, before := range bundle.Managers {
		index := slices.IndexFunc(bundle.ManagersAfter, func(after authManagerObservation) bool { return after.Name == before.Name })
		if index < 0 {
			problem("mgr.%s unavailable after the collection", before.Name)
			continue
		}
		if after := bundle.ManagersAfter[index]; after.GID != before.GID || after.ContainerID != before.ContainerID || after.StartedAt != before.StartedAt {
			problem("mgr.%s changed during the collection: GID %d -> %d, container %s/%s -> %s/%s",
				before.Name, before.GID, after.GID, before.ContainerID, before.StartedAt, after.ContainerID, after.StartedAt)
		}
	}
	if index := slices.IndexFunc(bundle.Managers, func(m authManagerObservation) bool { return "mgr."+m.Name == rejection.Target }); index >= 0 && bundle.Managers[index].GID != 0 {
		target := bundle.Managers[index]
		verdict.Target = "registered"
		if target.GID != rejection.TargetGID || target.GID != expected.GID ||
			expected.ContainerID != "" && (target.ContainerID != expected.ContainerID || target.StartedAt != expected.StartedAt) {
			verdict.Target = "changed"
		}
		if refused := rejection.RefusedGlobalID; refused != nil {
			switch {
			case *refused == target.GID:
				verdict.RefusedClient = "registered-mgr"
			case slices.Contains(target.ModuleGlobalIDs, *refused):
				verdict.RefusedClient = "module-client"
			}
		}
	} else if index < 0 && slices.ContainsFunc(bundle.Queries, func(q authQuery) bool { return q.Name == "mgr dump" && q.Status == authQueryComplete }) {
		verdict.Target = "absent"
	}
	if verdict.Target != "registered" {
		problem("target %s is %s", rejection.Target, verdict.Target)
	}
	type origin struct {
		run   string
		cycle int
	}
	origins := map[uint64]map[origin]bool{}
	record := func(run string, cycle int, secretID uint64) {
		if origins[secretID] == nil {
			origins[secretID] = map[origin]bool{}
		}
		origins[secretID][origin{run, cycle}] = true
	}
	for _, ticket := range bundle.Tickets {
		record(ticket.Run, ticket.Cycle, ticket.SecretID)
	}
	if rejection.SecretID != nil {
		record(rejection.Run, rejection.Cycle, *rejection.SecretID)
	}
	for secretID, seen := range origins {
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

// authQueryStatus classifies a query that returned err: an ended context or
// a command that ran into its timeout is "timed_out", a command the daemon
// does not know is "unsupported", output past its limit is "truncated", and
// anything else "failed".
func authQueryStatus(err error) string {
	if err == nil {
		return authQueryComplete
	}
	text := err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, ceph.ErrNativeCommandIncomplete) && strings.Contains(text, "timed out true"):
		return authQueryTimedOut
	case errors.Is(err, ceph.ErrNativeCommandIncomplete) && strings.Contains(text, "truncated true"):
		return authQueryTruncated
	case strings.Contains(text, "invalid command") || strings.Contains(text, "no valid command found") || strings.Contains(text, "unknown command"):
		return authQueryUnsupported
	}
	return authQueryFailed
}

// parseMonStatusObservation reads a MON's mon_status. Empty output, output
// that is not JSON and a status without its name, FSID or MonMap epoch are
// errors: none of them is an observation.
func parseMonStatusObservation(data []byte) (authMonitorObservation, error) {
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
		return authMonitorObservation{}, errors.New("mon_status printed nothing")
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return authMonitorObservation{}, fmt.Errorf("mon_status is not JSON: %w", err)
	}
	if status.Name == "" || status.Monmap == nil || status.Monmap.FSID == "" || status.Monmap.Epoch == 0 {
		return authMonitorObservation{}, errors.New("mon_status lacks its name, FSID or MonMap epoch")
	}
	return authMonitorObservation{Name: status.Name, Rank: status.Rank, State: status.State, ElectionEpoch: status.ElectionEpoch,
		Quorum: status.Quorum, MonmapEpoch: status.Monmap.Epoch, AuthEpoch: status.Monmap.AuthEpoch, FSID: status.Monmap.FSID}, nil
}

// parseMonSessions reads a MON's sessions and keeps those of MGRs, by entity
// name. Empty output and output that is not a JSON list are errors.
func parseMonSessions(data []byte) (map[string][]monSession, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("sessions printed nothing")
	}
	var list []monSession
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("sessions is not a JSON list: %w", err)
	}
	managers := map[string][]monSession{}
	for _, session := range list {
		if strings.HasPrefix(session.EntityName, "mgr.") {
			managers[session.EntityName] = append(managers[session.EntityName], session)
		}
	}
	return managers, nil
}

// authSources is what a collection reads; *ceph.Container provides it. Each
// method waits at most until its context ends.
type authSources interface {
	AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error)
	ManagerStatus(ctx context.Context) (ceph.ManagerStatus, error)
	Managers(ctx context.Context) ([]*ceph.ManagerContainer, error)
	QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
	RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
	RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error)
}

// authCollector runs the bounded queries of one collection.
type authCollector struct {
	sources authSources
	ctx     context.Context
	bound   time.Duration
	bundle  *authFirstFailureBundle
}

// authExcerptLimit caps the output and error text that a query retains.
const authExcerptLimit = 1024

// authTextWithheld replaces text that could not be redacted within the
// collection's budget.
const authTextWithheld = "text withheld: redaction did not finish within the collection budget"

// redact masks the cluster's secrets in text and caps it, within ctx.
func (c *authCollector) redact(ctx context.Context, text string) string {
	redacted, err := c.sources.RedactSecrets(ctx, []byte(text))
	if err != nil {
		return authTextWithheld
	}
	return string(redacted[:min(len(redacted), authExcerptLimit)])
}

// query runs one query within the per-query bound and records its status.
// run returns the output it read, which the query keeps as an excerpt when
// it did not yield a complete observation. Redaction runs within what is
// left of the collection's budget, and Finished includes it.
func (c *authCollector) query(name, kind string, run func(context.Context) ([]byte, error)) bool {
	ctx, cancel := context.WithTimeout(c.ctx, c.bound)
	defer cancel()
	entry := authQuery{Name: name, Kind: kind, Started: time.Now().UTC(), Status: authQueryComplete}
	output, err := run(ctx)
	if err != nil {
		entry.Status = authQueryStatus(err)
		entry.Error = c.redact(c.ctx, err.Error())
		if len(output) > 0 {
			entry.Excerpt = c.redact(c.ctx, string(output))
		}
	}
	entry.Finished = time.Now().UTC()
	c.bundle.Queries = append(c.bundle.Queries, entry)
	return err == nil
}

// observeManagers records every owned MGR with its MgrMap GID and container
// instance, and, when sessions is set, the global IDs of its MON sessions.
func (c *authCollector) observeManagers(phase string, sessions map[string][]monSession) []authManagerObservation {
	var status ceph.ManagerStatus
	c.query("mgr dump", "observation", func(ctx context.Context) ([]byte, error) {
		var err error
		status, err = c.sources.ManagerStatus(ctx)
		return nil, err
	})
	gids := map[string]uint64{}
	if status.ActiveName != "" {
		gids[status.ActiveName] = status.ActiveGID
	}
	for _, standby := range status.Standbys {
		gids[standby.Name] = standby.GID
	}
	var managers []*ceph.ManagerContainer
	if !c.query("owned MGRs "+phase, "observation", func(ctx context.Context) ([]byte, error) {
		var err error
		managers, err = c.sources.Managers(ctx)
		return nil, err
	}) {
		return nil
	}
	var observed []authManagerObservation
	for _, manager := range managers {
		entry := authManagerObservation{Name: manager.DaemonName, GID: gids[manager.DaemonName], Active: manager.DaemonName == status.ActiveName,
			ContainerID: manager.GetContainerID()}
		c.query("container of mgr."+manager.DaemonName+" "+phase, "observation", func(ctx context.Context) ([]byte, error) {
			state, err := manager.State(ctx)
			if err == nil {
				entry.StartedAt = state.StartedAt
			}
			return nil, err
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
// expected MON's mon_status and sessions through its admin socket, the
// Messenger quorum, the MgrMap and each owned MGR's container instance before
// and after, and runs a fresh "tell mgr.<name> version" from control for the
// tested target and the active MGR as a behavioral control, keeping at most
// controlOutput bytes of its output. Every query keeps its status; a query
// that fails or is truncated is recorded, not retried, and the rejection is
// kept as given. Every wait, the redaction of errors included, ends with the
// budget. The one exception is the stop of a control that could not be
// confirmed as stopped: it has a separate allowance of authStopAllowance,
// outside the budget, and its failure is recorded under cleanup_errors.
func collectAuthFirstFailureBundle(parent context.Context, cluster authSources, control testcontainers.Container, provenance authProvenance,
	images map[string]string, expected authExpectedTarget, rejection authTestedRejection, held *authHeldSession, tickets []authTicketRecord,
	budget, bound time.Duration, controlOutput int) authFirstFailureBundle {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), budget)
	defer cancel()
	bundle := authFirstFailureBundle{Schema: authBundleSchema, Provenance: provenance, Images: images, Expected: expected,
		Budget: budget.String(), QueryBound: bound.String(), Started: time.Now().UTC(), Rejection: rejection, Held: held,
		Tickets: tickets, GenerationEquality: authGenerationUnavailable}
	collector := &authCollector{sources: cluster, ctx: ctx, bound: bound, bundle: &bundle}
	sessions := map[string][]monSession{}
	for _, name := range expected.Monitors {
		collector.query("mon."+name+" mon_status", "local observation", func(ctx context.Context) ([]byte, error) {
			out, err := cluster.AdminSocket(ctx, "mon."+name, "mon_status")
			if err != nil {
				return out, err
			}
			observed, err := parseMonStatusObservation(out)
			if err != nil {
				return out, err
			}
			bundle.Monitors = append(bundle.Monitors, observed)
			return out, nil
		})
		collector.query("mon."+name+" sessions", "local observation", func(ctx context.Context) ([]byte, error) {
			out, err := cluster.AdminSocket(ctx, "mon."+name, "sessions")
			if err != nil {
				return out, err
			}
			managers, err := parseMonSessions(out)
			if err != nil {
				return out, err
			}
			for entity, list := range managers {
				sessions[entity] = append(sessions[entity], list...)
			}
			return out, nil
		})
	}
	// The quorum as a MON session reports it is a different observation from
	// each MON's own state.
	collector.query("quorum", "messenger observation", func(ctx context.Context) ([]byte, error) {
		quorum, err := cluster.QuorumStatus(ctx)
		if err == nil {
			bundle.Quorum = quorum.QuorumNames
		}
		return nil, err
	})
	bundle.Managers = collector.observeManagers("before", sessions)
	// A standby MGR answers no tell, so the controls address the tested
	// target and the active MGR.
	targets := []string{rejection.Target}
	for _, manager := range bundle.Managers {
		if manager.Active && !slices.Contains(targets, "mgr."+manager.Name) {
			targets = append(targets, "mgr."+manager.Name)
		}
	}
	for _, target := range targets {
		collector.query("tell "+target+" version", "behavioral control", func(ctx context.Context) ([]byte, error) {
			started := time.Now().UTC()
			result, err := cluster.RunNativeCommand(ctx, control, ceph.NativeCommand{
				Argv: []string{"ceph", "--connect-timeout", "5", "tell", target, "version"}, Timeout: bound,
				MaxStdout: controlOutput, MaxStderr: 16 << 10, Label: "tell " + target + " version",
			})
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), authStopAllowance)
				if stopErr := unstopped.Stop(stopCtx); stopErr != nil {
					bundle.CleanupErrors = append(bundle.CleanupErrors, fmt.Sprintf("stop %s: %v", target, stopErr))
				}
				stop()
			}
			bundle.Controls = append(bundle.Controls, authControl{Target: target, Operation: authControlOperation(rejection, target), Command: "version", Phase: "after first failure", Started: started,
				ExitCode: result.ExitCode, Complete: err == nil && result.Complete()})
			if err == nil && bundle.Release == "" && result.ExitCode == 0 {
				var version struct {
					Version string `json:"version"`
					Release string `json:"release"`
				}
				if json.Unmarshal(result.Stdout, &version) == nil {
					bundle.Release = version.Version + " " + version.Release
				}
			}
			return append(result.Stdout, result.Stderr...), err
		})
	}
	bundle.ManagersAfter = collector.observeManagers("after", nil)
	bundle.Ended = time.Now().UTC()
	bundle.CollectionComplete = len(bundle.CleanupErrors) == 0 &&
		!slices.ContainsFunc(bundle.Queries, func(q authQuery) bool { return q.Status != authQueryComplete })
	return bundle
}

// testAuthFirstFailureBundle runs the collection recipe on a fixture of three
// MONs and two MGRs after one rotating-key wipe, where the MONs have that
// command. Its tested authentication is a native client with a wrong key,
// which the MONs refuse; it runs once and is never retried. The caller
// freezes the expected identities right after the rejection. The bundle must
// be complete, keep that rejection as it was recorded, show the target MGR
// still registered with its GID and container instance, record successful
// behavioral controls, and leave generation equality unavailable/unverified.
// It must hold neither the admin key nor a field beyond its schema. Two more
// collections show partial results that keep the rejection: one whose budget
// ends at once, so that every query times out, and one whose control may keep
// only 8 bytes, so that the control is truncated.
func testAuthFirstFailureBundle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(slices.Clone(roles), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2).WithNoInitialOSDs()))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	before, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var monmap struct {
		Epoch uint64 `json:"epoch"`
		FSID  string `json:"fsid"`
	}
	if err := json.Unmarshal(before, &monmap); err != nil {
		t.Fatal(err)
	}
	var monitors []string
	for _, monitor := range clusteraccess.Monitors(t, cluster) {
		monitors = append(monitors, monitor.DaemonName)
	}
	// One wipe, as in a consumer's cycle; every MON's own MonMap must move
	// past it before the tested authentication. MONs that do not know the
	// command, such as Squid's, answer that no valid command was found; the
	// collection then runs without a wipe.
	wiped := true
	if _, err := cluster.Ceph(ctx, "auth", "wipe-rotating-service-keys"); err != nil {
		if authQueryStatus(err) != authQueryUnsupported {
			t.Fatal("wipe rotating service keys:", err)
		}
		wiped = false
		t.Log("the MONs do not know auth wipe-rotating-service-keys; collecting without a wipe")
	}
	for _, name := range monitors {
		if !wiped {
			break
		}
		deadline := time.Now().Add(time.Minute)
		for {
			out, err := harness.MonitorHandle(t, ctx, cluster, name).AdminSocket(ctx, "mon_status")
			if err == nil {
				if observed, err := parseMonStatusObservation(out); err == nil && observed.MonmapEpoch > monmap.Epoch {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("mon.%s did not move past MonMap epoch %d: %v", name, monmap.Epoch, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	// A command that the MON does not know is classified as unsupported.
	if _, err := harness.MonitorHandle(t, ctx, cluster, monitors[0]).AdminSocket(ctx, "quorum_status"); err == nil || authQueryStatus(err) != authQueryUnsupported {
		t.Fatalf("an unknown admin socket command gave %v, status %s", err, authQueryStatus(err))
	}
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || !status.Available {
		t.Fatalf("no active MGR: %+v, %v", status, err)
	}
	target := "mgr." + status.ActiveName
	wrong, err := cluster.CreateClient(ctx, "auth-first-failure-wrong", ceph.ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	cc, err := wrong.ConnectionConfig()
	wrongKeyring := cc.Keyring
	if err != nil {
		t.Fatal(err)
	}
	wrongKeyring = bytes.ReplaceAll(wrongKeyring, []byte("["+wrong.Name()+"]"), []byte("[client.admin]"))
	if err := client.CopyToContainer(ctx, wrongKeyring, "/tmp/wrong.keyring", 0o600); err != nil {
		t.Fatal("copy deliberately wrong keyring failed")
	}
	attempted := time.Now().UTC()
	tested, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv:    []string{"ceph", "--keyring", "/tmp/wrong.keyring", "--connect-timeout", "10", "tell", target, "version"},
		Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 16 << 10, Label: "tested authentication",
	})
	// Tentacle's MonClient logs "(13) Permission denied" and Squid's
	// "allowed_methods [2] but i only support [2]"; both end with errno 13.
	if err != nil || tested.ExitCode != 13 || !bytes.Contains(tested.Stderr, []byte("[errno 13] RADOS permission denied")) {
		t.Fatalf("the tested authentication was not refused: exit=%d err=%v stderr=%.512s", tested.ExitCode, err, tested.Stderr)
	}
	ended := time.Now().UTC()
	lines := strings.Split(strings.TrimSpace(string(tested.Stderr)), "\n")
	rejection := authTestedRejection{Run: t.Name(), Cycle: 1, Attempt: 1, At: attempted, Target: target, TargetGID: status.ActiveGID, Result: -13, Error: lines[len(lines)-1]}
	recorded := rejection
	// The excerpt of the original operation's own stderr: client-side lines,
	// bound to this invocation's capture in the client container instance and
	// to the operation's window, and not to the MGR it addressed. The command
	// result exposes no process ID, so the capture is the invocation itself.
	clientState, err := client.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstExcerpt, firstRecord := produceAuthFirstExcerpt(authFailedOperation{
		Operation: authRejectionOperation(rejection),
		Binding: authExcerptBinding{Daemon: "client.admin", Generation: "container " + client.GetContainerID() + " started " + clientState.StartedAt,
			Capture: authRejectionOperation(rejection) + "/stderr/" + attempted.Format(time.RFC3339Nano)},
		Start: attempted, End: ended, Stderr: tested.Stderr, StderrComplete: tested.Complete(),
	}, newAuthExcerptRedactor(ctx, harness.WithSockets(t, cluster), authExcerptRedactionBudget), authFailureExcerptLimit)
	excerpts := newAuthExcerptLedger()
	if first, err := excerpts.record(firstExcerpt); err != nil || !first {
		t.Fatalf("first excerpt not recorded: %v", err)
	}
	// The expected identities are frozen now, before any diagnostic.
	expected := authExpectedTarget{FSID: monmap.FSID, Monitors: monitors, Manager: status.ActiveName, GID: status.ActiveGID}
	for _, manager := range clusteraccess.Managers(t, cluster) {
		if manager.DaemonName == status.ActiveName {
			state, err := manager.State(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected.ContainerID, expected.StartedAt = manager.GetContainerID(), state.StartedAt
		}
	}
	provenance := authProvenance{Producer: "ceph-testcontainers-go internal/integration", Subject: "native client with a wrong key", Run: t.Name()}

	adminKey, err := cluster.Ceph(ctx, "auth", "get-key", "client.admin")
	if err != nil {
		t.Fatal(err)
	}
	collect := func(budget time.Duration, controlOutput int) authFirstFailureBundle {
		collected := collectAuthFirstFailureBundle(ctx, harness.WithSockets(t, cluster), client, provenance, map[string]string{"control": image}, expected, rejection, nil, nil, budget, 15*time.Second, controlOutput)
		attached := firstRecord
		collected.FirstExcerpt = &attached
		return collected
	}
	bundle := collect(time.Minute, 4096)
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAuthBundleSecrecy(data, strings.TrimSpace(string(adminKey))); err != nil {
		t.Fatal(err)
	}
	verdict := classifyAuthBundle(bundle)
	if !verdict.Complete || !bundle.CollectionComplete || verdict.Target != "registered" || verdict.RefusedClient != "unknown" || !verdict.ControlsSucceeded ||
		verdict.Result != -13 || verdict.GenerationEquality != authGenerationUnavailable || len(bundle.Monitors) != 3 || len(bundle.Managers) != 2 || len(bundle.Quorum) != 3 {
		t.Fatalf("verdict %+v for bundle %s", verdict, data)
	}
	// The excerpt is the original operation's, with the command's stderr
	// completeness kept apart from its own status. Tentacle and Squid may
	// print no prefix, so no particular status is required; a clipped or
	// withheld one stays visible. The controls ran as other attempts and the
	// first excerpt is unchanged.
	if bundle.FirstExcerpt == nil || bundle.FirstExcerpt.Operation != authRejectionOperation(rejection) || verdict.ExcerptStatus != firstRecord.Status ||
		verdict.Cause != authCauseUnavailable || !bundle.FirstExcerpt.CommandStderrComplete {
		t.Fatalf("first-failure excerpt %+v, verdict %+v", bundle.FirstExcerpt, verdict)
	}
	if first, ok := excerpts.First(authRejectionOperation(rejection)); !ok || first.Status != firstExcerpt.Status || excerpts.Later(authRejectionOperation(rejection)) != 0 {
		t.Fatalf("first excerpt ledger %+v %v", first, ok)
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
	// Diagnostics that fail keep the rejection.
	for name, test := range map[string]struct {
		budget        time.Duration
		controlOutput int
		status        string
	}{
		"starved budget":    {time.Nanosecond, 4096, authQueryTimedOut},
		"truncated control": {time.Minute, 8, authQueryTruncated},
	} {
		partial := collect(test.budget, test.controlOutput)
		partialVerdict := classifyAuthBundle(partial)
		if partialVerdict.Complete || partial.CollectionComplete || partial.Rejection != recorded || partialVerdict.Result != -13 ||
			!slices.ContainsFunc(partial.Queries, func(q authQuery) bool { return q.Status == test.status }) {
			t.Fatalf("%s: verdict %+v, rejection %+v, queries %+v", name, partialVerdict, partial.Rejection, partial.Queries)
		}
	}
	// The controls ran under the supervisor; none may have left a marker.
	if code, out, err := harness.SessionExec(ctx, client, "sh", "-c", "ls -A /tmp/tc-supervised 2>/dev/null"); err != nil || strings.TrimSpace(out) != "" {
		t.Fatalf("supervisor markers remain: %d %q %v", code, out, err)
	}
	t.Logf("AUTH_FIRST_FAILURE_BUNDLE wiped=%t verdict %+v\n%s", wiped, verdict, data)
}
