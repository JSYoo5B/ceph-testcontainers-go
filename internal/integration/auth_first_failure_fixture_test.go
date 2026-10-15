//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// retainedAuthFailureBundle replays, as a bundle, the consumer's retained
// first failure of 2026-10-15 (ceph-msgr-go b01a3cfb, CI job 116437288897,
// SERVICE_KEY_EPOCH_FAILURE_20261015.md): cycle 2's first cold MGR
// authentication, refused with -13 after mgr.a logged that it could not
// decrypt the ticket info. The values are the report's; the report has no MON
// session or container data, so those fields are empty, and its readiness
// probes appear as the controls and queries. The ticket label is the report's
// run-scoped HMAC label, not ticket bytes.
const retainedAuthFailureBundle = `{
 "schema": "auth-first-failure/v1",
 "release": "20.2.4 tentacle",
 "images": {"control": "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"},
 "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab",
 "budget": "1m0s",
 "query_bound": "15s",
 "started": "2026-10-15T13:12:01.700Z",
 "ended": "2026-10-15T13:12:03.100Z",
 "tested_rejection": {"cycle": 2, "attempt": 3, "at": "2026-10-15T13:12:01.630Z", "target": "mgr.a", "target_gid": 4191, "client_id": 4464,
  "service": 16, "secret_id": 2, "ticket_label": "80c2aeafe59a9bda4b5b8f4ce5b90b074e3a77843e217c855a5a68511f6aa8a0", "result": -13,
  "error": "server refused authentication method 2 with code -13"},
 "held_session": {"client_id": 4434, "usable": true},
 "tickets": [{"cycle": 2, "service": 16, "secret_id": 2, "label": "80c2aeafe59a9bda4b5b8f4ce5b90b074e3a77843e217c855a5a68511f6aa8a0"}],
 "monitors": [
  {"name": "a", "rank": 0, "state": "leader", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"},
  {"name": "b", "rank": 1, "state": "peon", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"},
  {"name": "c", "rank": 2, "state": "peon", "election_epoch": 0, "quorum": [0, 1, 2], "monmap_epoch": 5, "auth_epoch": 11, "fsid": "3f991852-459f-46a3-bd7b-c55161b0bfab"}],
 "managers": [{"name": "a", "gid": 4191, "active": true, "container_id": "", "started_at": "", "beacon_global_ids": null, "module_global_ids": null}],
 "managers_after": [{"name": "a", "gid": 4191, "active": true, "container_id": "", "started_at": "", "beacon_global_ids": null, "module_global_ids": null}],
 "controls": [{"target": "mgr.a", "command": "version", "exit_code": 0, "complete": true}],
 "queries": [
  {"name": "mon.a mon_status", "kind": "observation", "started": "2026-10-15T13:12:01.500Z", "elapsed": "12ms"},
  {"name": "mon.b mon_status", "kind": "observation", "started": "2026-10-15T13:12:01.512Z", "elapsed": "11ms"},
  {"name": "mon.c mon_status", "kind": "observation", "started": "2026-10-15T13:12:01.523Z", "elapsed": "10ms"},
  {"name": "mgr dump", "kind": "observation", "started": "2026-10-15T13:12:01.533Z", "elapsed": "9ms"},
  {"name": "tell mgr.a version", "kind": "behavioral-control", "started": "2026-10-15T13:12:01.540Z", "elapsed": "6ms"}],
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

// The retained failure classifies as complete, with its target still
// registered and successful controls, and leaves generation equality and
// the cause unverified: the classification draws no conclusion from the
// controls or from the epochs.
func TestAuthBundleRetainedFailure(t *testing.T) {
	verdict := classifyAuthBundle(retainedBundle(t))
	if !verdict.Complete || verdict.Target != "registered" || verdict.RefusedClient != "unknown" || !verdict.ControlsSucceeded ||
		len(verdict.ReusedSecretIDs) != 0 || verdict.GenerationEquality != authGenerationUnavailable {
		t.Fatalf("verdict %+v", verdict)
	}
}

func TestAuthBundleClassification(t *testing.T) {
	two := uint64(2)
	for name, test := range map[string]struct {
		edit    func(*authFirstFailureBundle)
		check   func(authBundleVerdict) bool
		problem string
	}{
		"missing MON query": {func(b *authFirstFailureBundle) {
			b.Monitors = b.Monitors[:2]
			b.Queries[2].Error = "context deadline exceeded"
		}, nil, "mon.c mon_status failed: context deadline exceeded"},
		"truncated control": {func(b *authFirstFailureBundle) {
			b.Queries[4].Truncated = true
			b.Controls[0].Complete = false
		}, func(v authBundleVerdict) bool { return !v.ControlsSucceeded }, "tell mgr.a version was truncated"},
		"failed control": {func(b *authFirstFailureBundle) { b.Controls[0].ExitCode = 13 },
			func(v authBundleVerdict) bool { return v.Complete && !v.ControlsSucceeded }, ""},
		"target failed over": {func(b *authFirstFailureBundle) {
			b.Managers[0].GID, b.ManagersAfter[0].GID = 4300, 4300
		}, func(v authBundleVerdict) bool { return v.Complete && v.Target == "changed" }, ""},
		"target absent": {func(b *authFirstFailureBundle) { b.Rejection.Target = "mgr.b" },
			func(v authBundleVerdict) bool { return v.Target == "absent" }, ""},
		"MGR restarted during the collection": {func(b *authFirstFailureBundle) {
			b.Managers[0].StartedAt, b.ManagersAfter[0].StartedAt = "start-1", "start-2"
		}, nil, "mgr.a changed during the collection"},
		"no MGR":              {func(b *authFirstFailureBundle) { b.Managers, b.ManagersAfter = nil, nil }, nil, "no MGR observed"},
		"no MON":              {func(b *authFirstFailureBundle) { b.Monitors = nil }, nil, "no MON observed"},
		"another FSID":        {func(b *authFirstFailureBundle) { b.Monitors[1].FSID = "other" }, nil, `mon.b reports FSID "other"`},
		"no tested rejection": {func(b *authFirstFailureBundle) { b.Rejection = authTestedRejection{} }, nil, "no tested rejection"},
		"claimed equality":    {func(b *authFirstFailureBundle) { b.GenerationEquality = "equal" }, nil, `generation equality "equal"`},
		"other schema":        {func(b *authFirstFailureBundle) { b.Schema = "v0" }, nil, `schema "v0"`},
		// A wipe recreates the MGR key set, and its secret IDs start again:
		// ID 2 of cycle 1 and ID 2 of cycle 2 are different keys.
		"secret ID reused across cycles": {func(b *authFirstFailureBundle) {
			b.Tickets = append(b.Tickets, authTicketRecord{Cycle: 1, Service: 16, SecretID: 2, Label: "cycle-1-label"})
		}, func(v authBundleVerdict) bool {
			return v.Complete && slices.Equal(v.ReusedSecretIDs, []uint64{2}) && v.GenerationEquality == authGenerationUnavailable
		}, ""},
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
		"rejection without a secret ID": {func(b *authFirstFailureBundle) {
			b.Rejection.SecretID, b.Tickets = nil, nil
		}, func(v authBundleVerdict) bool { return v.Complete && len(v.ReusedSecretIDs) == 0 }, ""},
	} {
		bundle := retainedBundle(t)
		bundle.Rejection.SecretID = &two
		test.edit(&bundle)
		verdict := classifyAuthBundle(bundle)
		if verdict.GenerationEquality != authGenerationUnavailable {
			t.Errorf("%s: generation equality %q", name, verdict.GenerationEquality)
		}
		if test.problem != "" && (verdict.Complete || !slices.ContainsFunc(verdict.Problems, func(p string) bool { return strings.Contains(p, test.problem) })) {
			t.Errorf("%s: problems %q, want %q", name, verdict.Problems, test.problem)
		}
		if test.check != nil && !test.check(verdict) {
			t.Errorf("%s: verdict %+v", name, verdict)
		}
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
	extra := strings.Replace(retainedAuthFailureBundle, `"fsid": "3f99`, `"ticket": "AAAA", "fsid": "3f99`, 1)
	if err := checkAuthBundleSecrecy([]byte(extra)); err == nil {
		t.Fatal("a bundle with a field beyond its schema passed")
	}
}
