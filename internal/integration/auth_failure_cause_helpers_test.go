//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The causes that classifyAuthCause can state. Each but method-mismatch rests
// on a line that a MON logs at its default levels; the client's own messages
// differ between releases and name no cause.
const (
	// authCauseUnknownEntity: cephx found no key for the entity.
	authCauseUnknownEntity = "unknown-entity"
	// authCauseWrongKey: the entity exists, and the client proved another key.
	authCauseWrongKey = "wrong-key"
	// authCauseMethodMismatch: the MON offered only methods the client does
	// not support, so no cephx exchange ran.
	authCauseMethodMismatch = "method-mismatch"
	// authCauseCommandDenied: authentication succeeded and the MON refused
	// the command under the entity's caps.
	authCauseCommandDenied = "command-denied"
	// authCauseConflicting: the captures hold evidence of more than one cause.
	authCauseConflicting = "conflicting"
)

var (
	authCauseUnknownLine = regexp.MustCompile(`^\S+ \S+ +0 cephx server (\S+): +couldn't find entity name: (\S+)$`)
	authCauseKeyLine     = regexp.MustCompile(`^(\S+ \S+ +0 cephx server (\S+): +unexpected key: )req\.key=[0-9a-f]+ expected_key=[0-9a-f]+$`)
	authCauseDeniedLine  = regexp.MustCompile(`^\S+ \S+ +1 mon\.\S+ e\d+ handle_command access denied$`)
	authCauseMethodsLine = regexp.MustCompile(`handle_auth_bad_method server allowed_methods \[([0-9, ]*)\] (?:and I|but i only) support \[([0-9, ]*)\]`)
)

// authCauseLine is one MON log line that names a cause, with the key values
// that cephx derives masked.
type authCauseLine struct {
	Cause string `json:"cause"`
	Text  string `json:"text"`
}

// authCauseCapture is the part of one MON's log that an attempt produced:
// lines From to To, before and after the attempt, of the MON container's
// generation, and the cause lines among them that concern the attempted
// entity. A command refusal names no entity; the capture bounds it.
type authCauseCapture struct {
	Monitor    string          `json:"monitor"`
	Generation string          `json:"generation"`
	From       int             `json:"from"`
	To         int             `json:"to"`
	Lines      []authCauseLine `json:"lines"`
}

// authCauseRecord is the evidence of one refusal's cause: the attempt's
// operation identity, the attempted entity, the time from before the first
// MON log was counted to after the last was read again, a capture of every
// MON's log around the attempt, and the client's line about the
// authentication methods, if it printed one.
type authCauseRecord struct {
	Operation     string             `json:"operation"`
	Entity        string             `json:"entity"`
	Start         time.Time          `json:"start"`
	End           time.Time          `json:"end"`
	Captures      []authCauseCapture `json:"captures"`
	ClientMethods string             `json:"client_methods,omitempty"`
}

// authCauseLines picks the cause lines about entity out of a MON log
// capture, masking derived key values.
func authCauseLines(entity string, lines []string) []authCauseLine {
	var result []authCauseLine
	for _, line := range lines {
		switch {
		case authCauseUnknownLine.MatchString(line):
			if m := authCauseUnknownLine.FindStringSubmatch(line); m[1] == entity && m[2] == entity {
				result = append(result, authCauseLine{Cause: authCauseUnknownEntity, Text: line})
			}
		case authCauseKeyLine.MatchString(line):
			if m := authCauseKeyLine.FindStringSubmatch(line); m[2] == entity {
				result = append(result, authCauseLine{Cause: authCauseWrongKey, Text: m[1] + "req.key=<masked> expected_key=<masked>"})
			}
		case authCauseDeniedLine.MatchString(line):
			result = append(result, authCauseLine{Cause: authCauseCommandDenied, Text: line})
		}
	}
	return result
}

// authClientMethods returns the client's line about the methods the MON
// offered and the ones the client supports, if its output holds one.
func authClientMethods(stderr string) string {
	return authCauseMethodsLine.FindString(stderr)
}

// disjointMethods reports whether a client methods line names two method
// sets with no method in common.
func disjointMethods(line string) bool {
	m := authCauseMethodsLine.FindStringSubmatch(line)
	if m == nil {
		return false
	}
	split := func(s string) []string {
		var methods []string
		for _, method := range strings.Split(s, ",") {
			if method = strings.TrimSpace(method); method != "" {
				methods = append(methods, method)
			}
		}
		return methods
	}
	server, client := split(m[1]), split(m[2])
	return len(server) > 0 && len(client) > 0 && !slices.ContainsFunc(server, func(method string) bool { return slices.Contains(client, method) })
}

// classifyAuthCause states the cause that a record establishes, and what
// makes the record unusable. Every expected MON needs a capture with a
// generation and a range. One cause among all MON lines is that cause; MON
// lines of two causes, or MON lines beside disjoint client methods, conflict.
// No MON line and disjoint client methods is a method mismatch. Anything
// else is unavailable.
func classifyAuthCause(record authCauseRecord, monitors []string) (string, []string) {
	var problems []string
	if record.Entity == "" {
		problems = append(problems, "the cause record names no entity")
	}
	for _, name := range monitors {
		index := slices.IndexFunc(record.Captures, func(c authCauseCapture) bool { return c.Monitor == "mon."+name })
		switch {
		case index < 0:
			problems = append(problems, fmt.Sprintf("no log capture of mon.%s", name))
		case record.Captures[index].Generation == "" || record.Captures[index].From > record.Captures[index].To:
			problems = append(problems, fmt.Sprintf("the log capture of mon.%s is unbound: %q lines %d-%d", name,
				record.Captures[index].Generation, record.Captures[index].From, record.Captures[index].To))
		}
	}
	causes := map[string]bool{}
	for _, capture := range record.Captures {
		for _, line := range capture.Lines {
			causes[line.Cause] = true
		}
	}
	disjoint := disjointMethods(record.ClientMethods)
	switch {
	case len(problems) != 0:
		return authCauseUnavailable, problems
	case len(causes) > 1 || len(causes) == 1 && disjoint:
		found := slices.Sorted(func(yield func(string) bool) {
			for cause := range causes {
				if !yield(cause) {
					return
				}
			}
		})
		return authCauseConflicting, []string{fmt.Sprintf("conflicting causes %v, client methods %q", found, record.ClientMethods)}
	case len(causes) == 1:
		for cause := range causes {
			return cause, nil
		}
	case disjoint:
		return authCauseMethodMismatch, nil
	}
	return authCauseUnavailable, nil
}

// checkAuthCauseBinding requires a cause record to be the rejected
// attempt's: the same operation identity, the entity the rejection
// authenticated as, and a capture window around the rejection's time. A
// control, a retry or another entity's attempt never supplies the first
// failure's cause.
func checkAuthCauseBinding(record authCauseRecord, rejection authTestedRejection) error {
	var problems []string
	if want := authRejectionOperation(rejection); record.Operation != want {
		problems = append(problems, fmt.Sprintf("record operation %q, not %q", record.Operation, want))
	}
	if rejection.Entity == "" || record.Entity != rejection.Entity {
		problems = append(problems, fmt.Sprintf("record entity %q, rejection entity %q", record.Entity, rejection.Entity))
	}
	if record.Start.IsZero() || record.End.IsZero() || rejection.At.Before(record.Start) || rejection.At.After(record.End) {
		problems = append(problems, fmt.Sprintf("capture window %s to %s does not hold the rejection at %s",
			record.Start.Format(time.RFC3339Nano), record.End.Format(time.RFC3339Nano), rejection.At.Format(time.RFC3339Nano)))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// authLogMark is where one MON's log stood before an attempt, and when it
// was counted.
type authLogMark struct {
	monitor    *ceph.MonitorContainer
	generation string
	from       int
	at         time.Time
}

// markMonitorLogs records how many lines each MON's log holds now.
func markMonitorLogs(t *testing.T, ctx context.Context, cluster *ceph.Container) []authLogMark {
	t.Helper()
	var marks []authLogMark
	for _, monitor := range clusteraccess.Monitors(t, cluster) {
		at := time.Now().UTC()
		state, err := monitor.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		lines := harness.ContainerLogLines(t, ctx, monitor.Container)
		marks = append(marks, authLogMark{monitor: monitor, generation: monitor.GetContainerID() + "@" + state.StartedAt, from: len(lines) - 1, at: at})
	}
	return marks
}

// captureAuthCause reads each marked MON's log again and records, for the
// attempt named operation, the cause lines about entity that it gained since
// the mark, with the client's methods line from stderr. A MON that restarted
// since its mark fails the test: its new log would not be the attempt's.
func captureAuthCause(t *testing.T, ctx context.Context, operation string, marks []authLogMark, entity, stderr string) authCauseRecord {
	t.Helper()
	record := authCauseRecord{Operation: operation, Entity: entity, ClientMethods: authClientMethods(stderr)}
	for _, mark := range marks {
		if record.Start.IsZero() || mark.at.Before(record.Start) {
			record.Start = mark.at
		}
		state, err := mark.monitor.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if generation := mark.monitor.GetContainerID() + "@" + state.StartedAt; generation != mark.generation {
			t.Fatalf("mon.%s restarted during the attempt: %s, was %s", mark.monitor.DaemonName, generation, mark.generation)
		}
		lines := harness.ContainerLogLines(t, ctx, mark.monitor.Container)
		to := len(lines) - 1
		record.Captures = append(record.Captures, authCauseCapture{Monitor: "mon." + mark.monitor.DaemonName, Generation: mark.generation,
			From: mark.from, To: to, Lines: authCauseLines(entity, lines[max(mark.from, 0):to])})
	}
	record.End = time.Now().UTC()
	return record
}

// authCauseAttempt is one native command of the cause recipe: what it
// tries, how it ended, and the cause its record establishes.
type authCauseAttempt struct {
	Name     string          `json:"name"`
	Entity   string          `json:"entity"`
	ExitCode int             `json:"exit_code"`
	Error    string          `json:"error"`
	Cause    string          `json:"cause"`
	Problems []string        `json:"problems,omitempty"`
	Record   authCauseRecord `json:"record"`
}

// testAuthFailureCauses refuses four native ceph commands for four reasons
// and classifies each refusal from the MON log lines that the attempt
// produced: a key for client.admin that is not client.admin's, a key for an
// entity that does not exist, a client that requires no authentication of a
// cluster that requires cephx, and an authenticated entity with "allow r"
// that creates a pool. A command that succeeds is the control: its capture
// establishes no cause. Each refusal keeps its own exit code and last line.
func testAuthFailureCauses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(slices.Clone(roles), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))...)
	reader, err := cluster.CreateClient(ctx, "auth-cause-reader", ceph.ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	config, err := reader.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	section := []byte("[" + reader.Name() + "]")
	keyrings := map[string][]byte{
		"/tmp/auth-cause-reader.keyring":  config.Keyring,
		"/tmp/auth-cause-admin.keyring":   bytes.ReplaceAll(config.Keyring, section, []byte("[client.admin]")),
		"/tmp/auth-cause-missing.keyring": bytes.ReplaceAll(config.Keyring, section, []byte("[client.auth-cause-missing]")),
	}
	for path, keyring := range keyrings {
		if !bytes.Contains(keyring, []byte("key = ")) {
			t.Fatalf("keyring %s holds no key", path)
		}
		if err := client.CopyToContainer(ctx, keyring, path, 0o600); err != nil {
			t.Fatalf("copy keyring %s failed", path)
		}
	}
	adminKey, err := cluster.Ceph(ctx, "auth", "get-key", "client.admin")
	if err != nil {
		t.Fatal(err)
	}
	var attempts []authCauseAttempt
	defer func() {
		data, err := json.Marshal(attempts)
		if err != nil {
			t.Error(err)
			return
		}
		if err := checkAuthCauseSecrecy(data, strings.TrimSpace(string(adminKey)), string(config.Keyring)); err != nil {
			t.Error(err)
			return
		}
		t.Logf("AUTH_FAILURE_CAUSE_RECEIPT %s", data)
	}()
	var monitors []string
	for _, monitor := range clusteraccess.Monitors(t, cluster) {
		monitors = append(monitors, monitor.DaemonName)
	}
	for _, c := range []struct {
		name, entity string
		argv         []string
		exit         int
		final        string
		cause        string
	}{
		{"wrong key", "client.admin", []string{"--keyring", "/tmp/auth-cause-admin.keyring", "-s"}, 13, "[errno 13] RADOS permission denied", authCauseWrongKey},
		{"unknown entity", "client.auth-cause-missing", []string{"--keyring", "/tmp/auth-cause-missing.keyring", "--name", "client.auth-cause-missing", "-s"}, 13, "[errno 13] RADOS permission denied", authCauseUnknownEntity},
		{"no common method", "client.admin", []string{"--auth-client-required", "none", "-s"}, 13, "[errno 13] RADOS permission denied", authCauseMethodMismatch},
		{"command beyond caps", reader.Name(), []string{"--keyring", "/tmp/auth-cause-reader.keyring", "--name", reader.Name(), "osd", "pool", "create", "auth-cause-denied"}, 13, "Error EACCES: access denied", authCauseCommandDenied},
		{"control", reader.Name(), []string{"--keyring", "/tmp/auth-cause-reader.keyring", "--name", reader.Name(), "-s"}, 0, "", authCauseUnavailable},
	} {
		marks := markMonitorLogs(t, ctx, cluster)
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    append([]string{"ceph", "--connect-timeout", "10"}, c.argv...),
			Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "auth cause " + c.name,
		})
		if err != nil || !result.Complete() {
			t.Fatalf("%s: %v, complete %t", c.name, err, result.Complete())
		}
		stderr := strings.TrimSpace(string(result.Stderr))
		attempt := authCauseAttempt{Name: c.name, Entity: c.entity, ExitCode: result.ExitCode, Error: stderr[strings.LastIndex(stderr, "\n")+1:],
			Record: captureAuthCause(t, ctx, t.Name()+"/"+c.name, marks, c.entity, string(result.Stderr))}
		redacted, err := diagnostics.Redact(ctx, cluster, []byte(attempt.Error))
		if err != nil {
			t.Fatal(err)
		}
		attempt.Error = string(redacted)
		attempt.Cause, attempt.Problems = classifyAuthCause(attempt.Record, monitors)
		attempts = append(attempts, attempt)
		if result.ExitCode != c.exit || !strings.HasPrefix(attempt.Error, c.final) {
			t.Fatalf("%s: exit %d, last line %q, want %d and %q", c.name, result.ExitCode, attempt.Error, c.exit, c.final)
		}
		if attempt.Cause != c.cause || len(attempt.Problems) != 0 {
			t.Fatalf("%s: cause %q, problems %v, want %q: %+v", c.name, attempt.Cause, attempt.Problems, c.cause, attempt.Record)
		}
	}
}

// checkAuthCauseSecrecy fails when data holds a secret or a key value that
// cephx derives, which cause lines must mask.
func checkAuthCauseSecrecy(data []byte, secrets ...string) error {
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(data, []byte(secret)) {
			return errors.New("the cause evidence holds a secret")
		}
	}
	if regexp.MustCompile(`(?:req\.key|expected_key)=[0-9a-f]`).Match(data) {
		return errors.New("the cause evidence holds a derived key value")
	}
	return nil
}
