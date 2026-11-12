//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_multisite_object_lock_sync_test

import (
	"errors"
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
	// Per read: the destination judgement (absent, symptom: the start of
	// its detail, or the prefix of another fault), and the prefix of the
	// source and control verdicts. The destination names the symptom only
	// for an answer about the configuration; the source and the control
	// accept one answer each.
	const defect, fault, condition, control = "symptom: ", "another fault: ", "condition not established: ", "control failed: "
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
		symptom, detail, err := judgeDestination(read, want)
		destination := ""
		switch {
		case err != nil:
			destination = err.Error()
		case symptom:
			destination = defect + detail
		case detail == "":
			destination = "absent without a detail"
		}
		got := [3]string{destination, judgeSource(read, want), judgeUnlocked(read)}
		for i, verdict := range got {
			if prefix := expect[name][i]; prefix == "" && verdict != "" || prefix != "" && !strings.HasPrefix(verdict, prefix) {
				t.Errorf("%s, judge %d: %q, want prefix %q", name, i, verdict, prefix)
			}
		}
	}
}

func TestJudgeVersioning(t *testing.T) {
	for name, test := range map[string]struct {
		status int
		body   string
		err    error
		ok     bool
	}{
		"enabled":           {http.StatusOK, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`, nil, true},
		"enabled, indented": {http.StatusOK, "<?xml version=\"1.0\"?>\n<VersioningConfiguration>\n  <Status>Enabled</Status>\n  <MfaDelete>Disabled</MfaDelete>\n</VersioningConfiguration>", nil, true},
		"suspended":         {http.StatusOK, `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`, nil, false},
		"never versioned":   {http.StatusOK, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`, nil, false},
		"another document":  {http.StatusOK, `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`, nil, false},
		"empty":             {http.StatusOK, "", nil, false},
		"denied":            {http.StatusForbidden, `<Error><Code>AccessDenied</Code></Error>`, nil, false},
		"enabled in a 500":  {http.StatusInternalServerError, `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`, nil, false},
		"transport":         {0, "", errors.New("reset"), false},
	} {
		got := judgeVersioning(test.status, []byte(test.body), test.err)
		if (got == "") != test.ok || got != "" && !strings.HasPrefix(got, "condition not established: ") {
			t.Errorf("%s: %q, want ok %t", name, got, test.ok)
		}
	}
}

// The body kept as evidence is bounded and tells apart bodies that parse to
// the same configuration.
func TestRawBody(t *testing.T) {
	enabledOnly := []byte("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>")
	otherLayout := []byte("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rules><Retention><Mode>GOVERNANCE</Mode></Retention></Rules></ObjectLockConfiguration>")
	a, errA := parseLockConfig(enabledOnly)
	b, errB := parseLockConfig(otherLayout)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("the two bodies should parse alike: %+v %v, %+v %v", a, errA, b, errB)
	}
	if rawBody(enabledOnly) == rawBody(otherLayout) || !strings.Contains(rawBody(otherLayout), "<Rules>") {
		t.Fatalf("raw bodies do not tell the layouts apart: %s", rawBody(otherLayout))
	}
	long := []byte(strings.Repeat("x", 3*lockRawBound))
	if got := rawBody(long); len(got) > lockRawBound+64 || !strings.HasSuffix(got, "(cut, 3072 bytes in all)") {
		t.Fatalf("long body kept as %d characters: …%s", len(got), got[len(got)-40:])
	}
	if got := rawBody([]byte("line one\nline two")); strings.Contains(got, "\n") {
		t.Fatalf("raw body spans lines: %s", got)
	}
}
