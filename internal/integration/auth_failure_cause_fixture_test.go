//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// MON and client lines in the format that Squid 19.2.5 and Tentacle 20.2.4
// print them at their default levels, taken from runs of
// TestAuthFailureCauses. The derived key values of the wrong-key line are
// replaced with synthetic ones.
const (
	causeUnknownLine  = `2026-10-31T00:12:25.242+0000 ffff75466ac0  0 cephx server client.auth-cause-missing: couldn't find entity name: client.auth-cause-missing`
	causeKeyLine      = `2026-10-31T00:12:24.238+0000 ffff75466ac0  0 cephx server client.admin:  unexpected key: req.key=0123456789abcdef expected_key=fedcba9876543210`
	causeDeniedLine   = `2026-10-31T00:12:27.357+0000 ffff76c96ac0  1 mon.a@0(leader) e1 handle_command access denied`
	causeOtherLine    = `2026-10-31T00:12:24.481+0000 ffff9a627a80  0 mon.a@0(leader) e1 handle_command mon_command({"prefix": "status"} v 0)`
	tentacleNoMethods = `2026-10-31T00:11:57.319+0000 ffffa77ef120 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] and I support [1]`
	squidNoMethods    = `2026-10-31T00:12:26.252+0000 ffffaad67120 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] but i only support [1]`
	// squidWrongKey is what the Squid client prints for a wrong key: the
	// methods overlap, so the line names no mismatch.
	squidWrongKey = `2026-10-31T00:12:24.238+0000 ffffaa557120 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] but i only support [2]`
)

// causeRecord builds a record with one capture per MON, each holding the
// cause lines about entity among lines.
func causeRecord(entity, stderr string, lines ...string) authCauseRecord {
	record := authCauseRecord{Entity: entity, ClientMethods: authClientMethods(stderr)}
	for _, name := range []string{"a", "b", "c"} {
		record.Captures = append(record.Captures, authCauseCapture{Monitor: "mon." + name, Generation: "container-" + name + "@2026-10-31T00:12:00Z",
			From: 10, To: 10 + len(lines), Lines: authCauseLines(entity, lines)})
	}
	return record
}

func TestAuthCauseLines(t *testing.T) {
	lines := authCauseLines("client.admin", []string{causeOtherLine, causeKeyLine, causeUnknownLine, causeDeniedLine})
	if len(lines) != 2 || lines[0].Cause != authCauseWrongKey || lines[1].Cause != authCauseCommandDenied {
		t.Fatalf("lines %+v", lines)
	}
	if strings.Contains(lines[0].Text, "0123456789abcdef") || strings.Contains(lines[0].Text, "fedcba9876543210") ||
		!strings.HasSuffix(lines[0].Text, "unexpected key: req.key=<masked> expected_key=<masked>") {
		t.Fatalf("the key line is not masked: %q", lines[0].Text)
	}
	data, _ := json.Marshal(lines)
	if err := checkAuthCauseSecrecy(data); err != nil {
		t.Fatal(err)
	}
	if err := checkAuthCauseSecrecy([]byte(causeKeyLine)); err == nil {
		t.Fatal("the secrecy check passed an unmasked key line")
	}
	// A line about another entity is not this entity's cause.
	if lines := authCauseLines("client.other", []string{causeKeyLine, causeUnknownLine}); len(lines) != 0 {
		t.Fatalf("another entity's lines: %+v", lines)
	}
	if !disjointMethods(authClientMethods(tentacleNoMethods)) || !disjointMethods(authClientMethods(squidNoMethods)) || disjointMethods(authClientMethods(squidWrongKey)) {
		t.Fatal("method sets misread")
	}
}

func TestClassifyAuthCause(t *testing.T) {
	monitors := []string{"a", "b", "c"}
	for name, c := range map[string]struct {
		record  authCauseRecord
		cause   string
		problem string
	}{
		"wrong key":                    {causeRecord("client.admin", squidWrongKey, causeOtherLine, causeKeyLine), authCauseWrongKey, ""},
		"unknown entity":               {causeRecord("client.auth-cause-missing", squidWrongKey, causeUnknownLine), authCauseUnknownEntity, ""},
		"no common method, Tentacle":   {causeRecord("client.admin", tentacleNoMethods, causeOtherLine), authCauseMethodMismatch, ""},
		"no common method, Squid":      {causeRecord("client.admin", squidNoMethods), authCauseMethodMismatch, ""},
		"command beyond caps":          {causeRecord("client.reader", "Error EACCES: access denied", causeDeniedLine), authCauseCommandDenied, ""},
		"nothing logged":               {causeRecord("client.admin", "", causeOtherLine), authCauseUnavailable, ""},
		"overlapping methods only":     {causeRecord("client.admin", squidWrongKey), authCauseUnavailable, ""},
		"two MON causes":               {causeRecord("client.admin", "", causeKeyLine, causeDeniedLine), authCauseConflicting, "conflicting causes"},
		"MON cause and no common path": {causeRecord("client.admin", tentacleNoMethods, causeKeyLine), authCauseConflicting, "conflicting causes"},
		"a MON not captured": {func() authCauseRecord {
			r := causeRecord("client.admin", "", causeKeyLine)
			r.Captures = r.Captures[:2]
			return r
		}(), authCauseUnavailable, "no log capture of mon.c"},
		"an unbound capture": {func() authCauseRecord {
			r := causeRecord("client.admin", "", causeKeyLine)
			r.Captures[1].Generation = ""
			return r
		}(), authCauseUnavailable, "unbound"},
		"no entity": {causeRecord("", "", causeKeyLine), authCauseUnavailable, "names no entity"},
	} {
		cause, problems := classifyAuthCause(c.record, monitors)
		if cause != c.cause || c.problem == "" && len(problems) != 0 ||
			c.problem != "" && !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.problem) }) {
			t.Errorf("%s: cause %q, problems %v, want %q with %q", name, cause, problems, c.cause, c.problem)
		}
	}
}

// The retained bundle has no cause record and states no cause; with the
// record of its MONs' wrong-key lines it states wrong-key, and a record that
// misses a MON or conflicts makes it incomplete without changing its -13.
func TestAuthBundleCause(t *testing.T) {
	bundle := retainedBundle(t)
	if verdict := classifyAuthBundle(bundle); verdict.Cause != authCauseUnavailable || !verdict.Complete {
		t.Fatalf("without a record: %+v", verdict)
	}
	record := authCauseRecord{Entity: "client.admin", ClientMethods: authClientMethods(squidWrongKey)}
	for _, name := range bundle.Expected.Monitors {
		record.Captures = append(record.Captures, authCauseCapture{Monitor: "mon." + name, Generation: "container-" + name + "@2026-10-15T13:11:00Z",
			From: 100, To: 104, Lines: authCauseLines("client.admin", []string{causeOtherLine, causeKeyLine})})
	}
	bundle.Cause = &record
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAuthBundleSecrecy(data); err != nil {
		t.Fatal(err)
	}
	if err := checkAuthCauseSecrecy(data); err != nil {
		t.Fatal(err)
	}
	if verdict := classifyAuthBundle(bundle); verdict.Cause != authCauseWrongKey || !verdict.Complete || verdict.Result != -13 {
		t.Fatalf("with a wrong-key record: %+v", verdict)
	}
	partial := record
	partial.Captures = record.Captures[1:]
	bundle.Cause = &partial
	if verdict := classifyAuthBundle(bundle); verdict.Cause != authCauseUnavailable || verdict.Complete || verdict.Result != -13 {
		t.Fatalf("with a MON missing: %+v", verdict)
	}
	conflicting := record
	conflicting.Captures = slices.Clone(record.Captures)
	conflicting.Captures[0].Lines = authCauseLines("client.admin", []string{causeDeniedLine})
	bundle.Cause = &conflicting
	if verdict := classifyAuthBundle(bundle); verdict.Cause != authCauseConflicting || verdict.Complete || verdict.Result != -13 {
		t.Fatalf("with conflicting lines: %+v", verdict)
	}
}
