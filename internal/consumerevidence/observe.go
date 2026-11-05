package consumerevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The status of one bounded read. Only a complete read is an observation.
const (
	StatusComplete    = "complete"
	StatusMissing     = "missing"
	StatusTimedOut    = "timed_out"
	StatusTruncated   = "truncated"
	StatusUnsupported = "unsupported"
	StatusParse       = "parse_failure"
	StatusFailed      = "failed"
)

// GenerationUnavailable is what every recipe reports for the equality of
// rotating-key generations between the MON that issued a ticket and the
// daemon that verified it: no stock command shows it without the keys, and
// debug_auth, which would, prints them.
const GenerationUnavailable = "unavailable/unverified"

var (
	errMissing   = errors.New("output without the selected fields")
	errMalformed = errors.New("malformed selected fields")
	errOversized = errors.New("output over its bound")
)

// managerDumpLimit bounds the "mgr dump" output that is decoded. The dump
// holds every module's option descriptions, a few hundred KB.
const managerDumpLimit = 4 << 20

// Query is the outcome of one bounded read.
type Query struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Complete reports whether the read is an observation.
func (q Query) Complete() bool { return q.Status == StatusComplete }

// queryStatus classifies the error of a read.
func queryStatus(err error) string {
	var syntax *json.SyntaxError
	var kind *json.UnmarshalTypeError
	text := ""
	if err != nil {
		text = err.Error()
	}
	switch {
	case err == nil:
		return StatusComplete
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		return StatusTimedOut
	case errors.Is(err, errMissing):
		return StatusMissing
	case errors.Is(err, errOversized), errors.Is(err, ceph.ErrNativeCommandIncomplete) && strings.Contains(text, "truncated true"):
		return StatusTruncated
	case errors.Is(err, ceph.ErrNativeCommandIncomplete) && strings.Contains(text, "timed out true"):
		return StatusTimedOut
	case errors.As(err, &syntax) || errors.As(err, &kind) || errors.Is(err, errMalformed):
		return StatusParse
	case strings.Contains(text, "invalid command") || strings.Contains(text, "no valid command found") || strings.Contains(text, "unknown command"):
		return StatusUnsupported
	}
	return StatusFailed
}

// errorText caps the text of err that a record keeps.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	return text[:min(len(text), 256)]
}

// bounded runs one read within bound and records its outcome under name.
func bounded(ctx context.Context, bound time.Duration, name string, read func(context.Context) error) Query {
	call, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	err := read(call)
	return Query{Name: name, Status: queryStatus(err), Error: errorText(err)}
}

// Registration is one MgrMap registration.
type Registration struct {
	Name string `json:"name"`
	GID  uint64 `json:"gid"`
}

// ManagerMap holds the selected fields of one "mgr dump". Observer's
// ManagerStatus has no epoch, and the epoch is what tells two reads of the
// same map from two maps, so the recipes read the native dump.
type ManagerMap struct {
	Query     Query          `json:"query"`
	Epoch     uint64         `json:"epoch,omitempty"`
	Available bool           `json:"available"`
	Active    Registration   `json:"active"`
	Standbys  []Registration `json:"standbys"`
}

// same reports whether two maps hold the same epoch and registrations.
func (m ManagerMap) same(other ManagerMap) bool {
	return m.Epoch == other.Epoch && m.Available == other.Available && m.Active == other.Active && slices.Equal(m.Standbys, other.Standbys)
}

// registered returns the GID that the map holds for name.
func (m ManagerMap) registered(name string) (uint64, bool) {
	if m.Active.Name == name && m.Active.GID != 0 {
		return m.Active.GID, true
	}
	for _, standby := range m.Standbys {
		if standby.Name == name {
			return standby.GID, true
		}
	}
	return 0, false
}

// names returns every registered name of the map.
func (m ManagerMap) names() []string {
	var names []string
	if m.Active.Name != "" {
		names = append(names, m.Active.Name)
	}
	for _, standby := range m.Standbys {
		names = append(names, standby.Name)
	}
	return names
}

// decodeManagerMap decodes the selected fields of "mgr dump" output. Output
// without them, an empty one included, is missing, never an empty map.
func decodeManagerMap(data []byte) (ManagerMap, error) {
	if len(data) > managerDumpLimit {
		return ManagerMap{}, errOversized
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return ManagerMap{}, errMissing
	}
	var native struct {
		Epoch      *uint64 `json:"epoch"`
		Available  *bool   `json:"available"`
		ActiveName *string `json:"active_name"`
		ActiveGID  *uint64 `json:"active_gid"`
		Standbys   *[]struct {
			Name *string `json:"name"`
			GID  *uint64 `json:"gid"`
		} `json:"standbys"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return ManagerMap{}, err
	}
	if native.Epoch == nil || native.Available == nil || native.ActiveName == nil || native.ActiveGID == nil || native.Standbys == nil {
		return ManagerMap{}, errMissing
	}
	observed := ManagerMap{Query: Query{Name: "mgr dump", Status: StatusComplete}, Epoch: *native.Epoch, Available: *native.Available,
		Active: Registration{Name: *native.ActiveName, GID: *native.ActiveGID}, Standbys: []Registration{}}
	if (observed.Active.Name == "") != (observed.Active.GID == 0) {
		return ManagerMap{}, fmt.Errorf("%w: active %q with GID %d", errMalformed, observed.Active.Name, observed.Active.GID)
	}
	for _, standby := range *native.Standbys {
		if standby.Name == nil || standby.GID == nil || *standby.Name == "" || *standby.GID == 0 {
			return ManagerMap{}, fmt.Errorf("%w: a standby without its name or GID", errMalformed)
		}
		observed.Standbys = append(observed.Standbys, Registration{Name: *standby.Name, GID: *standby.GID})
	}
	slices.SortFunc(observed.Standbys, func(a, b Registration) int { return strings.Compare(a.Name, b.Name) })
	return observed, nil
}

// readManagerMap reads one MgrMap within bound.
func readManagerMap(ctx context.Context, cluster *ceph.Container, bound time.Duration) ManagerMap {
	var observed ManagerMap
	query := bounded(ctx, bound, "mgr dump", func(ctx context.Context) error {
		data, err := cluster.Ceph(ctx, "mgr", "dump", "--format", "json")
		if err == nil {
			observed, err = decodeManagerMap(data)
		}
		return err
	})
	if !query.Complete() {
		return ManagerMap{Query: query, Standbys: []Registration{}}
	}
	return observed
}

// Session is one MGR session that a MON holds.
type Session struct {
	Entity   string `json:"entity"`
	GlobalID uint64 `json:"global_id"`
}

// MonitorSessions holds the MGR sessions of one MON: the beacon sessions,
// which a MGR daemon opens as con_type "mgr" with its MgrMap GID as global
// ID, and the sessions of its other MonClients, whose global IDs are their
// own. Entity names do not tell them apart.
type MonitorSessions struct {
	Monitor string    `json:"monitor"`
	Query   Query     `json:"query"`
	Beacons []Session `json:"beacons"`
	Modules []Session `json:"modules"`
}

// decodeSessions decodes a MON's "sessions" admin socket output and keeps
// its MGR sessions. An empty list is an answer; empty output is not.
func decodeSessions(monitor string, data []byte) (MonitorSessions, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return MonitorSessions{}, errMissing
	}
	var list []struct {
		EntityName string `json:"entity_name"`
		ConType    string `json:"con_type"`
		GlobalID   uint64 `json:"global_id"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return MonitorSessions{}, err
	}
	if list == nil {
		return MonitorSessions{}, errMissing
	}
	observed := MonitorSessions{Monitor: monitor, Query: Query{Name: "sessions of mon." + monitor, Status: StatusComplete}, Beacons: []Session{}, Modules: []Session{}}
	for _, session := range list {
		if !strings.HasPrefix(session.EntityName, "mgr.") {
			continue
		}
		entry := Session{Entity: session.EntityName, GlobalID: session.GlobalID}
		if session.ConType == "mgr" {
			observed.Beacons = append(observed.Beacons, entry)
		} else {
			observed.Modules = append(observed.Modules, entry)
		}
	}
	return observed, nil
}

// readSessions reads every MON's sessions through its own admin socket, each
// within bound; the MON answers without a MON session of the reader.
func readSessions(ctx context.Context, monitors []*ceph.MonitorContainer, bound time.Duration) []MonitorSessions {
	var all []MonitorSessions
	for _, monitor := range monitors {
		observed := MonitorSessions{Monitor: monitor.DaemonName}
		query := bounded(ctx, bound, "sessions of mon."+monitor.DaemonName, func(ctx context.Context) error {
			data, err := monitor.AdminSocket(ctx, "sessions")
			if err == nil {
				observed, err = decodeSessions(monitor.DaemonName, data)
			}
			return err
		})
		if !query.Complete() {
			observed = MonitorSessions{Monitor: monitor.DaemonName, Query: query, Beacons: []Session{}, Modules: []Session{}}
		}
		all = append(all, observed)
	}
	return all
}

// beaconMonitors returns the MONs that hold the beacon session of
// registration. A session of the same entity with another global ID belongs
// to another MonClient of the process and does not count.
func beaconMonitors(monitors []MonitorSessions, registration Registration) []string {
	sources := []string{}
	for _, monitor := range monitors {
		if slices.Contains(monitor.Beacons, Session{Entity: "mgr." + registration.Name, GlobalID: registration.GID}) {
			sources = append(sources, monitor.Monitor)
		}
	}
	return sources
}

// moduleGlobalIDs returns the global IDs of the other MonClients of the MGR
// name.
func moduleGlobalIDs(monitors []MonitorSessions, name string) []uint64 {
	var ids []uint64
	for _, monitor := range monitors {
		for _, session := range monitor.Modules {
			if session.Entity == "mgr."+name {
				ids = append(ids, session.GlobalID)
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// Instance is the container instance of one owned MGR. A restart keeps the
// container ID and changes the start time.
type Instance struct {
	Name        string `json:"name"`
	Query       Query  `json:"query"`
	ContainerID string `json:"container_id"`
	StartedAt   string `json:"started_at,omitempty"`
	Running     bool   `json:"running"`
}

// readInstances reads the container state of every MGR handle, each within
// bound.
func readInstances(ctx context.Context, managers []*ceph.ManagerContainer, bound time.Duration) []Instance {
	var all []Instance
	for _, manager := range managers {
		instance := Instance{Name: manager.DaemonName, ContainerID: manager.GetContainerID()}
		instance.Query = bounded(ctx, bound, "container of mgr."+manager.DaemonName, func(ctx context.Context) error {
			state, err := manager.State(ctx)
			if err == nil && state == nil {
				err = errMissing
			}
			if err == nil {
				instance.StartedAt, instance.Running = state.StartedAt, state.Running
			}
			return err
		})
		all = append(all, instance)
	}
	return all
}

// instanceOf returns the instance of the MGR name.
func instanceOf(instances []Instance, name string) (Instance, bool) {
	index := slices.IndexFunc(instances, func(i Instance) bool { return i.Name == name })
	if index < 0 {
		return Instance{}, false
	}
	return instances[index], true
}

// daemonNames returns the names of handles, sorted.
func daemonNames[T any](handles []T, name func(T) string) []string {
	names := make([]string, 0, len(handles))
	for _, handle := range handles {
		names = append(names, name(handle))
	}
	slices.Sort(names)
	return names
}

// wait pauses for pause or until ctx ends.
func wait(ctx context.Context, pause time.Duration) error {
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
