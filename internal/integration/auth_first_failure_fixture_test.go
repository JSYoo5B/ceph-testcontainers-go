//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// retainedAuthFailureBundle replays, as a bundle, the consumer's retained
// first failure of 2026-10-15: cycle 2's first cold MGR authentication,
// refused with -13 after mgr.a logged that it could not decrypt the ticket
// info. The values and links are the report's. Its opaque run-scoped labels
// are replaced by synthetic placeholders that keep their equality: the
// installed, selected and encoded ticket share "label-A". The report has no
// MON session or container data, so those fields are empty, and its readiness
// probes appear as complete queries and a control before the failure.
const retainedAuthFailureBundle = `{
 "schema": "auth-first-failure/v1",
 "provenance": {"producer": "ceph-testcontainers-go v0.2.0",
  "subject": "https://github.com/JSYoo5B/ceph-msgr-go/commit/b01a3cfb90c483832a61cbe7716e0252d7108214",
  "run": "https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38739526554/job/116437288897",
  "report": "https://github.com/JSYoo5B/ceph-msgr-go/blob/3ebe00e6d01f66786b4d17ff15888041744b59a6/integration/fixture/SERVICE_KEY_EPOCH_FAILURE_20261015.md"},
 "release": "20.2.4 tentacle",
 "images": {"control": "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"},
 "expected_target": {"fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab", "monitors": ["a", "b", "c"], "manager": "a", "gid": 4191, "container_id": "", "started_at": ""},
 "budget": "1m0s",
 "query_bound": "15s",
 "started": "2026-10-15T13:12:01.700Z",
 "ended": "2026-10-15T13:12:03.100Z",
 "first_failure": {"run": "38739526554", "cycle": 2, "attempt": 3, "at": "2026-10-15T13:12:01.630Z", "target": "mgr.a", "target_gid": 4191, "client_id": 4464,
  "service": 16, "secret_id": 2, "ticket_label": "label-A", "result": -13,
  "error": "server refused authentication method 2 with code -13; mgr.a: could not decrypt ticket info"},
 "held_session": {"client_id": 4434, "usable": true},
 "consumer_tickets": [{"run": "38739526554", "cycle": 2, "service": 16, "secret_id": 2, "label": "label-A", "issuer_monmap_epoch": 5, "issuer_auth_epoch": 11}],
 "monitors": [
  {"name": "a", "rank": 0, "state": "leader", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"},
  {"name": "b", "rank": 1, "state": "peon", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"},
  {"name": "c", "rank": 2, "state": "peon", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"}],
 "managers": [{"name": "a", "gid": 4191, "active": true, "container_id": "", "started_at": "", "beacon_global_ids": null, "module_global_ids": null}],
 "managers_after": [{"name": "a", "gid": 4191, "active": true, "container_id": "", "started_at": "", "beacon_global_ids": null, "module_global_ids": null}],
 "native_controls": [{"target": "mgr.a", "command": "version", "phase": "readiness before the first failure", "started": "2026-10-15T13:12:01.545Z", "exit_code": 0, "complete": true}],
 "queries": [
  {"name": "mon.a mon_status", "kind": "local observation", "started": "2026-10-15T13:12:01.500Z", "finished": "2026-10-15T13:12:01.512Z", "status": "complete"},
  {"name": "mon.b mon_status", "kind": "local observation", "started": "2026-10-15T13:12:01.512Z", "finished": "2026-10-15T13:12:01.523Z", "status": "complete"},
  {"name": "mon.c mon_status", "kind": "local observation", "started": "2026-10-15T13:12:01.523Z", "finished": "2026-10-15T13:12:01.533Z", "status": "complete"},
  {"name": "mgr dump", "kind": "observation", "started": "2026-10-15T13:12:01.533Z", "finished": "2026-10-15T13:12:01.540Z", "status": "complete"},
  {"name": "tell mgr.a version", "kind": "behavioral control", "started": "2026-10-15T13:12:01.540Z", "finished": "2026-10-15T13:12:01.546Z", "status": "complete"}],
 "collection_complete": true,
 "generation_equality": "unavailable/unverified"
}`

func retainedBundle(t *testing.T) authFirstFailureBundle {
	t.Helper()
	if err := checkAuthBundleSecrecy([]byte(retainedAuthFailureBundle)); err != nil {
		t.Fatal(err)
	}
	var bundle authFirstFailureBundle
	if err := json.Unmarshal([]byte(retainedAuthFailureBundle), &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

// The retained failure classifies as the preserved -13 with its target still
// registered and successful controls, and leaves generation equality and the
// cause unverified: the classification draws no conclusion from the controls
// or from the epochs.
func TestAuthBundleRetainedFailure(t *testing.T) {
	verdict := classifyAuthBundle(retainedBundle(t))
	if !verdict.Complete || verdict.Result != -13 || verdict.Target != "registered" || verdict.RefusedClient != "unknown" || !verdict.ControlsSucceeded ||
		len(verdict.ReusedSecretIDs) != 0 || verdict.GenerationEquality != authGenerationUnavailable {
		t.Fatalf("verdict %+v", verdict)
	}
}

// One fact of the retained bundle changes at a time. The subject's -13 and
// the unverified generation equality never change; the checks name what
// becomes unavailable or partial.
func TestAuthBundleClassification(t *testing.T) {
	for name, test := range map[string]struct {
		edit    func(*authFirstFailureBundle)
		check   func(authBundleVerdict) bool
		problem string
	}{
		"missing MON query": {func(b *authFirstFailureBundle) {
			b.Monitors = b.Monitors[:2]
			b.Queries[2].Status, b.Queries[2].Error = authQueryTimedOut, "context deadline exceeded"
		}, nil, "mon.c unavailable"},
		"truncated MON query": {func(b *authFirstFailureBundle) {
			b.Monitors = b.Monitors[1:]
			b.Queries[0].Status, b.Queries[0].Excerpt = authQueryTruncated, `{"name": "a", "rank": 0, "sta`
		}, nil, "mon.a mon_status: truncated"},
		"unsupported query": {func(b *authFirstFailureBundle) {
			b.Queries = append(b.Queries, authQuery{Name: "mon.a quorum_status", Kind: "local observation", Status: authQueryUnsupported, Error: "invalid command"})
		}, nil, "mon.a quorum_status: unsupported"},
		"target GID replaced": {func(b *authFirstFailureBundle) {
			b.Managers[0].GID, b.ManagersAfter[0].GID = 4300, 4300
		}, func(v authBundleVerdict) bool { return v.Target == "changed" }, "target mgr.a is changed"},
		"target process generation replaced": {func(b *authFirstFailureBundle) {
			b.Expected.ContainerID, b.Expected.StartedAt = "c1", "start-1"
			for _, list := range [][]authManagerObservation{b.Managers, b.ManagersAfter} {
				list[0].ContainerID, list[0].StartedAt = "c1", "start-2"
			}
		}, func(v authBundleVerdict) bool { return v.Target == "changed" }, "target mgr.a is changed"},
		"MGR restarted during the collection": {func(b *authFirstFailureBundle) {
			b.Managers[0].StartedAt, b.ManagersAfter[0].StartedAt = "start-1", "start-2"
		}, nil, "mgr.a changed during the collection"},
		// A successful query that found no MGR in the MgrMap is not a
		// registered target.
		"empty but successful MGR object": {func(b *authFirstFailureBundle) {
			b.Managers[0].GID, b.ManagersAfter[0].GID = 0, 0
		}, func(v authBundleVerdict) bool { return v.Target == "unavailable" }, "target mgr.a is unavailable"},
		"target absent from a complete MgrMap": {func(b *authFirstFailureBundle) { b.Managers, b.ManagersAfter = nil, nil },
			func(v authBundleVerdict) bool { return v.Target == "absent" }, "target mgr.a is absent"},
		"later successful control": {func(b *authFirstFailureBundle) {
			b.Controls = append(b.Controls, authControl{Target: "mgr.a", Command: "version", Phase: "after first failure", ExitCode: 0, Complete: true})
		}, func(v authBundleVerdict) bool { return v.Complete && v.ControlsSucceeded }, ""},
		"failed control": {func(b *authFirstFailureBundle) { b.Controls[0].ExitCode = 13 },
			func(v authBundleVerdict) bool { return v.Complete && !v.ControlsSucceeded }, ""},
		"failed cleanup": {func(b *authFirstFailureBundle) { b.CleanupErrors = []string{"stop mgr.a: container is paused"} }, nil, "cleanup: stop mgr.a"},
		"another FSID":   {func(b *authFirstFailureBundle) { b.Monitors[1].FSID = "other" }, nil, `mon.b reports FSID "other"`},
		"no tested rejection": {func(b *authFirstFailureBundle) {
			b.Rejection = authTestedRejection{}
		}, func(v authBundleVerdict) bool { return v.Result == 0 }, "no tested rejection"},
		"claimed equality": {func(b *authFirstFailureBundle) { b.GenerationEquality = "equal" }, nil, `generation equality "equal"`},
		"other schema":     {func(b *authFirstFailureBundle) { b.Schema = "v0" }, nil, `schema "v0"`},
		// A wipe recreates the MGR key set, and its secret IDs start again:
		// ID 2 of another cycle or run is a different key.
		"secret ID reused by another cycle": {func(b *authFirstFailureBundle) {
			b.Tickets = append(b.Tickets, authTicketRecord{Run: "38739526554", Cycle: 1, Service: 16, SecretID: 2, Label: "label-B"})
		}, func(v authBundleVerdict) bool { return v.Complete && slices.Equal(v.ReusedSecretIDs, []uint64{2}) }, ""},
		"secret ID reused by another run": {func(b *authFirstFailureBundle) {
			b.Tickets = append(b.Tickets, authTicketRecord{Run: "38595094701", Cycle: 2, Service: 16, SecretID: 2, Label: "label-C"})
		}, func(v authBundleVerdict) bool { return v.Complete && slices.Equal(v.ReusedSecretIDs, []uint64{2}) }, ""},
		"refusal of the registered MGR": {func(b *authFirstFailureBundle) {
			gid := uint64(4191)
			b.Rejection.RefusedGlobalID = &gid
		}, func(v authBundleVerdict) bool { return v.RefusedClient == "registered-mgr" }, ""},
		"refusal of a module client": {func(b *authFirstFailureBundle) {
			id := uint64(4209)
			b.Managers[0].ModuleGlobalIDs = []uint64{4197, 4209}
			b.Rejection.RefusedGlobalID = &id
		}, func(v authBundleVerdict) bool { return v.RefusedClient == "module-client" }, ""},
		"refusal of another client": {func(b *authFirstFailureBundle) {
			id := uint64(9999)
			b.Rejection.RefusedGlobalID = &id
		}, func(v authBundleVerdict) bool { return v.RefusedClient == "unknown" }, ""},
	} {
		bundle := retainedBundle(t)
		test.edit(&bundle)
		verdict := classifyAuthBundle(bundle)
		if verdict.GenerationEquality != authGenerationUnavailable || name != "no tested rejection" && verdict.Result != -13 {
			t.Errorf("%s: generation equality %q, result %d", name, verdict.GenerationEquality, verdict.Result)
		}
		if test.problem != "" && (verdict.Complete || !slices.ContainsFunc(verdict.Problems, func(p string) bool { return strings.Contains(p, test.problem) })) {
			t.Errorf("%s: problems %q, want %q", name, verdict.Problems, test.problem)
		}
		if test.check != nil && !test.check(verdict) {
			t.Errorf("%s: verdict %+v", name, verdict)
		}
	}
}

// Empty, non-JSON and partial outputs are not observations; a Squid status
// without auth_epoch is.
func TestAuthQueryParsers(t *testing.T) {
	const fsid = "3f991852-459f-46a3-bd7b-c55161b0bfab"
	for name, data := range map[string]string{
		"empty":            "",
		"blank":            " \n",
		"not JSON":         "admin_socket: invalid command",
		"truncated":        `{"name": "a", "rank": 0, "monmap": {"epoch": 5, "fsid": "3f99`,
		"without monmap":   `{"name": "a", "rank": 0, "state": "leader"}`,
		"without its FSID": `{"name": "a", "monmap": {"epoch": 5}}`,
	} {
		if observed, err := parseMonStatusObservation([]byte(data)); err == nil {
			t.Errorf("%s mon_status parsed as %+v", name, observed)
		}
	}
	tentacle, err := parseMonStatusObservation([]byte(`{"name": "a", "rank": 0, "state": "leader", "election_epoch": 14, "quorum": [0, 1, 2], "monmap": {"epoch": 4, "auth_epoch": 6, "fsid": "` + fsid + `"}}`))
	if err != nil || tentacle.AuthEpoch == nil || *tentacle.AuthEpoch != 6 || tentacle.MonmapEpoch != 4 || tentacle.FSID != fsid {
		t.Fatalf("Tentacle mon_status %+v, %v", tentacle, err)
	}
	squid, err := parseMonStatusObservation([]byte(`{"name": "b", "rank": 1, "state": "peon", "election_epoch": 8, "quorum": [0, 1, 2], "monmap": {"epoch": 3, "fsid": "` + fsid + `"}}`))
	if err != nil || squid.AuthEpoch != nil || squid.MonmapEpoch != 3 {
		t.Fatalf("Squid mon_status %+v, %v", squid, err)
	}
	for name, data := range map[string]string{"empty": "", "not JSON": "<html>", "an object": `{"sessions": []}`} {
		if sessions, err := parseMonSessions([]byte(data)); err == nil {
			t.Errorf("%s sessions parsed as %+v", name, sessions)
		}
	}
	sessions, err := parseMonSessions([]byte(`[{"entity_name": "", "con_type": "mon", "global_id": 0},
		{"entity_name": "mgr.a", "con_type": "mgr", "global_id": 4161}, {"entity_name": "mgr.a", "con_type": "client", "global_id": 4179},
		{"entity_name": "client.admin", "con_type": "client", "global_id": 4300}]`))
	if err != nil || len(sessions) != 1 || len(sessions["mgr.a"]) != 2 {
		t.Fatalf("sessions %+v, %v", sessions, err)
	}
	if empty, err := parseMonSessions([]byte(`[]`)); err != nil || len(empty) != 0 {
		t.Fatalf("an empty session list gave %+v, %v", empty, err)
	}
}

func TestAuthQueryStatus(t *testing.T) {
	for want, err := range map[string]error{
		authQueryTimedOut:    fmt.Errorf("mon.a mon_status: %w", context.DeadlineExceeded),
		authQueryUnsupported: errors.New("ceph exited 22 (EINVAL): admin_socket: invalid command"),
		authQueryTruncated:   fmt.Errorf("tell mgr.a version: %w (timed out false, stdout truncated true, stderr truncated false)", ceph.ErrNativeCommandIncomplete),
		authQueryFailed:      errors.New("container is paused"),
	} {
		if got := authQueryStatus(err); got != want {
			t.Errorf("%v: status %s, want %s", err, got, want)
		}
	}
	if got := authQueryStatus(fmt.Errorf("x: %w (timed out true, stdout truncated false, stderr truncated false)", ceph.ErrNativeCommandIncomplete)); got != authQueryTimedOut {
		t.Errorf("a command that ran into its timeout is %s", got)
	}
	if got := authQueryStatus(errors.New("Error EINVAL: no valid command found; 10 closest matches")); got != authQueryUnsupported {
		t.Errorf("an unknown MON command is %s", got)
	}
	if got := authQueryStatus(nil); got != authQueryComplete {
		t.Errorf("no error is %s", got)
	}
}

func TestAuthBundleSecrecy(t *testing.T) {
	if err := checkAuthBundleSecrecy([]byte(retainedAuthFailureBundle), "AQBlbW9ja2tleW1hdGVyaWFsZm9yYXRlc3Q9PQ=="); err != nil {
		t.Fatal(err)
	}
	leaked := strings.Replace(retainedAuthFailureBundle, "server refused authentication method 2", "key AQBlbW9ja2tleW1hdGVyaWFsZm9yYXRlc3Q9PQ==", 1)
	if err := checkAuthBundleSecrecy([]byte(leaked), "AQBlbW9ja2tleW1hdGVyaWFsZm9yYXRlc3Q9PQ=="); err == nil {
		t.Fatal("a bundle with the secret passed")
	}
	extra := strings.Replace(retainedAuthFailureBundle, `"budget": "1m0s"`, `"ticket": "AAAA", "budget": "1m0s"`, 1)
	if err := checkAuthBundleSecrecy([]byte(extra)); err == nil {
		t.Fatal("a bundle with a field beyond its schema passed")
	}
}
