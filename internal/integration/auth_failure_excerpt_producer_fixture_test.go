//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// The stderr below is synthetic: the log statements are the pinned Tentacle
// 20.2.4 ones used by the excerpt fixtures, and the secret is a placeholder.
// None of it is a saved native observation.
const producerFinalLine = "[errno 13] RADOS permission denied (error connecting to the cluster)"

var producerClientBinding = authExcerptBinding{Daemon: "client.admin", Generation: "container c1 started t1", Capture: "run/cycle-1/attempt-1/stderr/t0"}

func producerOperation(stderr ...string) authFailedOperation {
	return authFailedOperation{Operation: "run/cycle-1/attempt-1", Binding: producerClientBinding, Start: excerptStart, End: excerptEnd,
		Stderr: []byte(strings.Join(stderr, "\n") + "\n"), StderrComplete: true}
}

func producerRejection() authTestedRejection {
	return authTestedRejection{Run: "run", Cycle: 1, Attempt: 1, At: excerptStart, Target: "mgr.a", TargetGID: 4191, Result: -13, Error: producerFinalLine}
}

type producerSource struct {
	calls int
	fn    func(ctx context.Context, data []byte) ([]byte, error)
}

func (p *producerSource) RedactSecrets(ctx context.Context, data []byte, _ ...string) ([]byte, error) {
	p.calls++
	return p.fn(ctx, data)
}

// The original operation's own stderr yields a client-bound, windowed,
// redacted excerpt next to the unchanged numeric refusal and final line.
func TestAuthFirstExcerptProducer(t *testing.T) {
	mask := func(text string) (string, error) {
		return strings.ReplaceAll(text, "SYNTHETICSECRETVALUE", "<redacted>"), nil
	}
	op := producerOperation(excerptNoise, excerptMonRefusal+" key=SYNTHETICSECRETVALUE", excerptRetryLine+" RETRY-MARKER", excerptNoTimestamp+" REJECTED-MARKER", producerFinalLine)
	excerpt, record := produceAuthFirstExcerpt(op, mask, authFailureExcerptLimit)
	if record.Status != "complete" || record.Stage != string(authStageMon) || record.Daemon != "client.admin" || record.Generation == "" || record.Capture == "" ||
		record.Operation != "run/cycle-1/attempt-1" || !record.WindowStart.Equal(excerptStart) || !record.WindowEnd.Equal(excerptEnd) || !record.CommandStderrComplete ||
		record.RejectedLines != 2 || len(record.Lines) != 1 || record.RetainedBytes != len(record.Text) || strings.Contains(record.Text, "SYNTHETIC") || !strings.Contains(record.Text, "<redacted>") {
		t.Fatalf("record %+v", record)
	}
	if excerpt.Status != authExcerptComplete || excerpt.Binding != producerClientBinding {
		t.Fatalf("excerpt %+v", excerpt)
	}
	// Attached to the bundle, the refusal and the final error are unchanged
	// and the rest of the verdict does not move.
	bundle := retainedBundle(t)
	bundle.Rejection = producerRejection()
	bundle.Controls = nil
	before := bundle.Rejection
	bundle.FirstExcerpt = &record
	verdict := classifyAuthBundle(bundle)
	if bundle.Rejection != before || verdict.Result != -13 || verdict.ExcerptStatus != "complete" || verdict.Cause != authCauseUnavailable ||
		verdict.GenerationEquality != authGenerationUnavailable || bundle.GenerationEquality != authGenerationUnavailable {
		t.Fatalf("verdict %+v rejection %+v", verdict, bundle.Rejection)
	}
	for _, problem := range verdict.Problems {
		if strings.Contains(problem, "excerpt") {
			t.Fatalf("a valid excerpt was a problem: %v", verdict.Problems)
		}
	}
	// The record serializes within the bundle schema, without raw text, the
	// secret, or the rejected lines.
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAuthBundleSecrecy(data, "SYNTHETICSECRETVALUE"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"RETRY-MARKER", "REJECTED-MARKER", "unrelated line"} {
		if strings.Contains(string(data), raw) {
			t.Fatalf("serialized bundle holds %q", raw)
		}
	}
	var decoded authFirstFailureBundle
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.FirstExcerpt == nil || decoded.FirstExcerpt.Text != record.Text {
		t.Fatalf("round trip: %+v %v", decoded.FirstExcerpt, err)
	}
}

// The same final errno 13 with a missing prefix, a prefix outside the
// window, or a prefix of another capture never gains a stage or text, and
// the final line stays the refusal.
func TestAuthFirstExcerptNegativeControls(t *testing.T) {
	for name, test := range map[string]struct {
		op     authFailedOperation
		status string
	}{
		"final line alone":      {producerOperation(producerFinalLine), "no-lines"},
		"command-level EACCES":  {producerOperation("Error EACCES: access denied"), "no-lines"},
		"prefix after window":   {producerOperation(excerptRetryLine, producerFinalLine), "no-lines"},
		"timestamp not parsed":  {producerOperation(excerptBadTimestamp, producerFinalLine), "no-lines"},
		"prefix without a time": {producerOperation(excerptNoTimestamp, producerFinalLine), "no-lines"},
	} {
		_, record := produceAuthFirstExcerpt(test.op, excerptIdentity, authFailureExcerptLimit)
		if record.Status != test.status || record.Text != "" || record.RetainedBytes != 0 || len(record.Lines) != 0 || record.Stage != string(authStageUnknown) ||
			record.Daemon != "client.admin" {
			t.Fatalf("%s: %+v", name, record)
		}
	}
	// An unbound capture or a bad window gives an unavailable excerpt with its
	// reason, never a panic or a failure that hides the refusal.
	for name, mutate := range map[string]func(*authFailedOperation){
		"no generation": func(o *authFailedOperation) { o.Binding.Generation = "" },
		"no capture":    func(o *authFailedOperation) { o.Binding.Capture = "" },
		"no daemon":     func(o *authFailedOperation) { o.Binding.Daemon = "" },
		"no start":      func(o *authFailedOperation) { o.Start = time.Time{} },
		"inverted":      func(o *authFailedOperation) { o.Start, o.End = o.End, o.Start },
		"no operation":  func(o *authFailedOperation) { o.Operation = "" },
	} {
		op := producerOperation(excerptMonRefusal, producerFinalLine)
		mutate(&op)
		_, record := produceAuthFirstExcerpt(op, excerptIdentity, authFailureExcerptLimit)
		if record.Status != "unavailable" || record.Text != "" || len(record.Lines) != 0 || record.Stage != string(authStageUnknown) {
			t.Fatalf("%s: %+v", name, record)
		}
		if name != "no operation" && record.Reason == "" {
			t.Fatalf("%s: no reason: %+v", name, record)
		}
		if name == "no daemon" && record.Daemon != "" {
			t.Fatalf("%s: an incomplete binding was retained: %+v", name, record)
		}
	}
}

// Command stderr completeness and the excerpt's own completeness are
// independent: a complete read can hold a clipped excerpt, and a truncated
// read can hold a complete excerpt of what it did read.
func TestAuthFirstExcerptCompletenessIsSeparate(t *testing.T) {
	op := producerOperation(excerptMonRefusal, producerFinalLine)
	_, clipped := produceAuthFirstExcerpt(op, excerptIdentity, 40)
	if clipped.Status != "clipped" || !clipped.CommandStderrComplete || clipped.RetainedBytes != 40 {
		t.Fatalf("clipped excerpt of complete stderr: %+v", clipped)
	}
	op.StderrComplete = false
	_, partial := produceAuthFirstExcerpt(op, excerptIdentity, authFailureExcerptLimit)
	if partial.Status != "complete" || partial.CommandStderrComplete {
		t.Fatalf("complete excerpt of truncated stderr: %+v", partial)
	}
	// Exactly at the limit is complete and over it is clipped.
	exact := len(excerptMonRefusal)
	if _, r := produceAuthFirstExcerpt(producerOperation(excerptMonRefusal), excerptIdentity, exact); r.Status != "complete" {
		t.Fatalf("exact limit: %+v", r)
	}
	if _, r := produceAuthFirstExcerpt(producerOperation(excerptMonRefusal), excerptIdentity, exact-1); r.Status != "clipped" {
		t.Fatalf("over limit: %+v", r)
	}
	// Redaction before clipping: a secret across the boundary leaves no fragment.
	secretLine := excerptMonRefusal + " token=SYNTHETICSECRETVALUE"
	mask := func(text string) (string, error) {
		return strings.ReplaceAll(text, "SYNTHETICSECRETVALUE", "<redacted>"), nil
	}
	cut := strings.Index(secretLine, "SYNTHETICSECRETVALUE") + 5
	if _, r := produceAuthFirstExcerpt(producerOperation(secretLine), mask, cut); strings.Contains(r.Text, "SYNTH") || r.Status != "clipped" {
		t.Fatalf("secret fragment: %+v", r)
	}
}

// The real adapter bounds each redaction by its own budget, withholds the
// text on an error or a late result, treats nil sources as unavailable, and
// hands back a copy.
func TestAuthFirstExcerptRedactorAdapter(t *testing.T) {
	op := producerOperation(excerptMonRefusal, producerFinalLine)
	source := &producerSource{fn: func(_ context.Context, data []byte) ([]byte, error) {
		return []byte(strings.ReplaceAll(string(data), "allowed_methods", "allowed_methods <expanded redaction marker>")), nil
	}}
	_, record := produceAuthFirstExcerpt(op, newAuthExcerptRedactor(t.Context(), source, time.Second), authFailureExcerptLimit)
	if source.calls != 1 || record.Status != "complete" || !strings.Contains(record.Text, "<expanded redaction marker>") {
		t.Fatalf("adapter: %d calls, %+v", source.calls, record)
	}
	// Post-redaction expansion past the limit is visible as clipping.
	_, record = produceAuthFirstExcerpt(op, newAuthExcerptRedactor(t.Context(), source, time.Second), len(excerptMonRefusal)+5)
	if record.Status != "clipped" {
		t.Fatalf("expansion: %+v", record)
	}
	// The input to the source is the selected lines only, never the whole stderr.
	var seen string
	spy := &producerSource{fn: func(_ context.Context, data []byte) ([]byte, error) { seen = string(data); return data, nil }}
	produceAuthFirstExcerpt(op, newAuthExcerptRedactor(t.Context(), spy, time.Second), authFailureExcerptLimit)
	if seen != excerptMonRefusal {
		t.Fatalf("redactor saw %q", seen)
	}
	var typedNil *producerSource
	for name, redactor := range map[string]func(string) (string, error){
		"nil source": newAuthExcerptRedactor(t.Context(), nil, time.Second),
		"typed nil":  newAuthExcerptRedactor(t.Context(), typedNil, time.Second),
		"error": newAuthExcerptRedactor(t.Context(), &producerSource{fn: func(context.Context, []byte) ([]byte, error) {
			return []byte("raw " + excerptMonRefusal), errors.New("snapshot failed")
		}}, time.Second),
		"late": newAuthExcerptRedactor(t.Context(), &producerSource{fn: func(ctx context.Context, data []byte) ([]byte, error) {
			<-ctx.Done()
			return data, nil
		}}, 10*time.Millisecond),
	} {
		_, r := produceAuthFirstExcerpt(op, redactor, authFailureExcerptLimit)
		if r.Status != "redaction-unavailable" || r.Text != "" || r.RetainedBytes != 0 || r.Stage != string(authStageMon) || len(r.Lines) != 1 {
			t.Fatalf("%s: %+v", name, r)
		}
	}
	// The parent's cancellation does not cancel the redaction: the failed
	// test's own context may have ended.
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	if _, r := produceAuthFirstExcerpt(op, newAuthExcerptRedactor(parent, source, time.Second), authFailureExcerptLimit); r.Status != "complete" {
		t.Fatalf("cancelled parent: %+v", r)
	}
	// The returned text is a copy of the source's bytes.
	shared := []byte(excerptMonRefusal)
	redact := newAuthExcerptRedactor(t.Context(), &producerSource{fn: func(context.Context, []byte) ([]byte, error) { return shared, nil }}, time.Second)
	text, err := redact("x")
	if err != nil {
		t.Fatal(err)
	}
	shared[0] = 'X'
	if text != excerptMonRefusal {
		t.Fatalf("redacted text aliases the source's buffer: %q", text)
	}
}

// A bundle refuses an excerpt of another attempt, one bound to the MGR it
// addressed instead of the client, or one whose status disagrees with its
// text, and a control may not reuse the refused attempt's identity. None of
// these changes the refusal or the unavailable cause.
func TestAuthFirstExcerptAssociation(t *testing.T) {
	_, valid := produceAuthFirstExcerpt(producerOperation(excerptMonRefusal, producerFinalLine), excerptIdentity, authFailureExcerptLimit)
	for name, test := range map[string]struct {
		edit    func(*authFirstFailureBundle, *authFirstExcerptRecord)
		problem string
	}{
		"valid":                    {func(*authFirstFailureBundle, *authFirstExcerptRecord) {}, ""},
		"another attempt":          {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Operation = "run/cycle-1/attempt-2" }, "excerpt operation"},
		"bound to the MGR target":  {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Daemon = "mgr.a" }, "intended target"},
		"unknown status":           {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Status = "ok" }, "excerpt status"},
		"complete without text":    {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Text, r.RetainedBytes = "", 0 }, "disagrees"},
		"byte count disagrees":     {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.RetainedBytes++ }, "disagrees"},
		"complete without binding": {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Daemon = "" }, "disagrees"},
		"withheld with text":       {func(_ *authFirstFailureBundle, r *authFirstExcerptRecord) { r.Status = "redaction-unavailable" }, "holds text"},
		"control reuses the attempt": {func(b *authFirstFailureBundle, _ *authFirstExcerptRecord) {
			b.Controls = []authControl{{Target: "mgr.a", Operation: "run/cycle-1/attempt-1", Command: "version", Complete: true}}
		}, "reuses the rejected attempt"},
	} {
		bundle := retainedBundle(t)
		bundle.Rejection = producerRejection()
		record := valid
		test.edit(&bundle, &record)
		bundle.FirstExcerpt = &record
		verdict := classifyAuthBundle(bundle)
		found := ""
		for _, problem := range verdict.Problems {
			if test.problem != "" && strings.Contains(problem, test.problem) {
				found = problem
			}
			if test.problem == "" && (strings.Contains(problem, "excerpt") || strings.Contains(problem, "control")) {
				t.Fatalf("%s: %v", name, verdict.Problems)
			}
		}
		if test.problem != "" && found == "" || verdict.Result != -13 || bundle.Rejection != producerRejection() || verdict.Cause != authCauseUnavailable ||
			verdict.GenerationEquality != authGenerationUnavailable {
			t.Fatalf("%s: verdict %+v", name, verdict)
		}
	}
	// A retained bundle without any excerpt stays complete and reports it absent.
	if v := classifyAuthBundle(retainedBundle(t)); !v.Complete || v.ExcerptStatus != "absent" {
		t.Fatalf("no excerpt: %+v", v)
	}
	// A clipped or empty excerpt is visible and is not a problem of the bundle.
	for _, status := range []string{"clipped", "no-lines", "redaction-unavailable", "unavailable"} {
		bundle := retainedBundle(t)
		record := authFirstExcerptRecord{Operation: authRejectionOperation(bundle.Rejection), Status: status, Stage: string(authStageUnknown)}
		if status == "clipped" {
			record.Text, record.RetainedBytes, record.Daemon = "x", 1, "client.admin"
		}
		bundle.FirstExcerpt = &record
		if v := classifyAuthBundle(bundle); !v.Complete || v.ExcerptStatus != status || v.Cause != authCauseUnavailable {
			t.Fatalf("%s: %+v", status, v)
		}
	}
}

// The first excerpt is recorded once per attempt; a control's own attempt is
// kept apart and does not replace it.
func TestAuthFirstExcerptLedgerKeepsControlApart(t *testing.T) {
	rejection := producerRejection()
	first, _ := produceAuthFirstExcerpt(producerOperation(excerptMonRefusal, producerFinalLine), excerptIdentity, authFailureExcerptLimit)
	ledger := newAuthExcerptLedger()
	if ok, err := ledger.record(first); err != nil || !ok {
		t.Fatalf("first: %v %v", ok, err)
	}
	control := producerOperation(excerptNoise)
	control.Operation = authControlOperation(rejection, rejection.Target)
	controlExcerpt, _ := produceAuthFirstExcerpt(control, excerptIdentity, authFailureExcerptLimit)
	if ok, err := ledger.record(controlExcerpt); err != nil || !ok {
		t.Fatalf("control did not get its own first: %v %v", ok, err)
	}
	// A retry of the refused attempt is kept after it.
	retry, _ := produceAuthFirstExcerpt(producerOperation(excerptNoise), excerptIdentity, authFailureExcerptLimit)
	if ok, err := ledger.record(retry); err != nil || ok {
		t.Fatalf("retry replaced the first: %v %v", ok, err)
	}
	got, ok := ledger.First(authRejectionOperation(rejection))
	if !ok || got.Status != authExcerptComplete || got.Text != excerptMonRefusal || ledger.Later(authRejectionOperation(rejection)) != 1 {
		t.Fatalf("first excerpt changed: %+v", got)
	}
	if authControlOperation(rejection, "mgr.a") == authRejectionOperation(rejection) {
		t.Fatal("a control shares the rejected attempt's identity")
	}
}

// controlSources answers a collection with one successful control and no
// other observation.
type controlSources struct{}

func (controlSources) AdminSocket(context.Context, string, ...string) ([]byte, error) {
	return nil, nil
}
func (controlSources) ManagerStatus(context.Context) (ceph.ManagerStatus, error) {
	return ceph.ManagerStatus{}, nil
}
func (controlSources) Managers(context.Context) ([]*ceph.ManagerContainer, error) { return nil, nil }
func (controlSources) QuorumStatus(context.Context) (ceph.QuorumStatus, error) {
	return ceph.QuorumStatus{}, nil
}
func (controlSources) RunNativeCommand(context.Context, testcontainers.Container, ceph.NativeCommand) (ceph.NativeCommandResult, error) {
	return ceph.NativeCommandResult{Stdout: []byte(`{"version":"v","release":"r"}`)}, nil
}
func (controlSources) RedactSecrets(_ context.Context, data []byte, _ ...string) ([]byte, error) {
	return data, nil
}

// The collector gives each control its own attempt identity, and the
// rejection it was given is unchanged.
func TestAuthCollectorControlsHaveOwnAttempt(t *testing.T) {
	retained := retainedBundle(t)
	rejection := producerRejection()
	bundle := collectAuthFirstFailureBundle(t.Context(), controlSources{}, nil, retained.Provenance, retained.Images, retained.Expected,
		rejection, nil, nil, time.Second, time.Second, 4096)
	if bundle.Rejection != rejection || len(bundle.Controls) != 1 || bundle.Controls[0].Operation != authControlOperation(rejection, "mgr.a") ||
		bundle.Controls[0].Operation == authRejectionOperation(rejection) || bundle.Controls[0].ExitCode != 0 || !bundle.Controls[0].Complete {
		t.Fatalf("controls %+v rejection %+v", bundle.Controls, bundle.Rejection)
	}
}
