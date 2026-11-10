//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_multisite_object_lock_sync_test

import (
	"net/http"
	"strings"
	"testing"
)

func TestParseLockConfig(t *testing.T) {
	governance := lockConfig{Enabled: true, Mode: "GOVERNANCE", Days: 1}
	for name, test := range map[string]struct {
		body    string
		want    lockConfig
		wantErr bool
	}{
		"what the case sends": {string(governance.document()), governance, false},
		"years":               {string(lockConfig{Mode: "COMPLIANCE", Years: 2}.document()), lockConfig{Enabled: true, Mode: "COMPLIANCE", Years: 2}, false},
		// Formatting is not meaning.
		"indented, with a declaration": {"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <ObjectLockEnabled>Enabled</ObjectLockEnabled>\n  <Rule>\n    <DefaultRetention>\n      <Mode>GOVERNANCE</Mode>\n      <Days>1</Days>\n    </DefaultRetention>\n  </Rule>\n</ObjectLockConfiguration>\n", governance, false},
		"no namespace":                 {"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Days>1</Days><Mode>GOVERNANCE</Mode></DefaultRetention></Rule></ObjectLockConfiguration>", governance, false},
		"enabled without a rule":       {"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>", lockConfig{Enabled: true}, false},
		"empty rule":                   {"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule></Rule></ObjectLockConfiguration>", lockConfig{Enabled: true}, false},
		"nothing enabled":              {"<ObjectLockConfiguration></ObjectLockConfiguration>", lockConfig{}, false},
		"another state":                {"<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>", lockConfig{}, true},
		"an error document":            {"<Error><Code>AccessDenied</Code></Error>", lockConfig{}, true},
		"not XML":                      {"{}", lockConfig{}, true},
		"empty":                        {"", lockConfig{}, true},
	} {
		got, err := parseLockConfig([]byte(test.body))
		if (err != nil) != test.wantErr || got != test.want {
			t.Errorf("%s: %+v, %v; want %+v, error %t", name, got, err, test.want, test.wantErr)
		}
	}
}

func TestJudgeLockReads(t *testing.T) {
	want := lockConfig{Enabled: true, Mode: "GOVERNANCE", Days: 1}
	answer := func(config lockConfig) lockRead {
		return lockRead{Zone: "b", Bucket: "tc-lock", Status: http.StatusOK, Config: config}
	}
	absent := lockRead{Zone: "b", Bucket: "tc-lock", Status: http.StatusNotFound, Code: lockAbsent}
	reads := map[string]lockRead{
		"the configuration":      answer(want),
		"absent":                 absent,
		"enabled without a rule": answer(lockConfig{Enabled: true}),
		"another mode":           answer(lockConfig{Enabled: true, Mode: "COMPLIANCE", Days: 1}),
		"another period":         answer(lockConfig{Enabled: true, Mode: "GOVERNANCE", Days: 2}),
		"years for days":         answer(lockConfig{Enabled: true, Mode: "GOVERNANCE", Years: 1}),
		"not enabled":            answer(lockConfig{Mode: "GOVERNANCE", Days: 1}),
		"unparsed body":          {Zone: "b", Bucket: "tc-lock", Status: http.StatusOK, Unparsed: "EOF"},
		"no such bucket":         {Zone: "b", Bucket: "tc-lock", Status: http.StatusNotFound, Code: "NoSuchBucket"},
		"404 without a code":     {Zone: "b", Bucket: "tc-lock", Status: http.StatusNotFound},
		"access denied":          {Zone: "b", Bucket: "tc-lock", Status: http.StatusForbidden, Code: "AccessDenied"},
		"not implemented":        {Zone: "b", Bucket: "tc-lock", Status: http.StatusNotImplemented, Code: "NotImplemented"},
		"method not allowed":     {Zone: "b", Bucket: "tc-lock", Status: http.StatusMethodNotAllowed, Code: "MethodNotAllowed"},
		"server error":           {Zone: "b", Bucket: "tc-lock", Status: http.StatusInternalServerError, Code: "InternalError"},
		"transport":              {Zone: "b", Bucket: "tc-lock", Err: "context deadline exceeded"},
		"transport, stale 200":   {Zone: "b", Bucket: "tc-lock", Err: "reset", Status: http.StatusOK, Config: want},
	}
	// Per read: the prefix of the destination, source and control verdicts.
	// The destination names a defect only for an answer about the
	// configuration; the source and the control accept one answer each.
	const defect, fault, condition, control = "observed defect: ", "another fault: ", "condition not established: ", "control failed: "
	expect := map[string][3]string{
		"the configuration":      {"", "", control},
		"absent":                 {defect + "the destination zone has no object lock configuration", condition, ""},
		"enabled without a rule": {defect + "the destination zone answers another object lock configuration", condition, control},
		"another mode":           {defect, condition, control},
		"another period":         {defect, condition, control},
		"years for days":         {defect, condition, control},
		"not enabled":            {defect, condition, control},
		"unparsed body":          {fault, condition, control},
		"no such bucket":         {fault, condition, control},
		"404 without a code":     {fault, condition, control},
		"access denied":          {fault, condition, control},
		"not implemented":        {fault, condition, control},
		"method not allowed":     {fault, condition, control},
		"server error":           {fault, condition, control},
		"transport":              {fault, condition, control},
		"transport, stale 200":   {fault, condition, control},
	}
	if len(expect) != len(reads) {
		t.Fatalf("%d expectations for %d reads", len(expect), len(reads))
	}
	for name, read := range reads {
		got := [3]string{judgeDestination(read, want), judgeSource(read, want), judgeUnlocked(read)}
		for i, verdict := range got {
			if prefix := expect[name][i]; prefix == "" && verdict != "" || prefix != "" && !strings.HasPrefix(verdict, prefix) {
				t.Errorf("%s, judge %d: %q, want prefix %q", name, i, verdict, prefix)
			}
		}
	}
}
