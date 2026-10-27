package ceph

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// VerificationConfig selects what Finalization.Verify checks after the test:
// the log lines and the new crashes of the cluster's daemons since
// activation, each against its own expectations. Zero fields select a two
// minute timeout for the activation and for each evaluation (at most five),
// 20 seconds per Docker operation (at most one minute), 32 MiB of log stream
// per container (at most 256 MiB) and 256 retained findings per check (at
// most 4096).
type VerificationConfig struct {
	// Name labels the verification, its check and its report. Empty selects
	// "post-test". Names are unique within one Finalization.
	Name string
	// Logs and Crashes are the expected events. A log expectation never
	// matches a crash, nor a crash expectation a log line.
	Logs    []LogExpectation
	Crashes []CrashExpectation

	Timeout          time.Duration
	OperationTimeout time.Duration
	MaxLogBytes      int64
	MaxFindings      int
	// AdditionalContainers are caller-owned daemon containers, such as
	// mirror daemons, that are verified like the cluster's own. Verification
	// never adopts them.
	AdditionalContainers []DiagnosticsContainer
	// RedactValues removes additional opaque secrets from the report.
	RedactValues []string
}

// LogEvent is the kind of a log line that verification evaluates. Lines of
// no kind are not events.
type LogEvent string

const (
	// LogAssertion is a failed Ceph assertion or abort ("FAILED ceph_assert",
	// "ceph_abort").
	LogAssertion LogEvent = "assertion"
	// LogFatalSignal is a daemon's report of a fatal signal ("Caught
	// signal").
	LogFatalSignal LogEvent = "fatal-signal"
	// LogClusterError, LogClusterSecurity and LogClusterWarning are cluster
	// log entries of level ERR, SEC and WRN, which every daemon writes to
	// its output and the MON writes again as the cluster log. The level is
	// the one of the entry's own log envelope (see LogExpectation.Pattern);
	// a marker in the message of a lower level entry makes no event, and a
	// line with a marker but no recognized envelope is a gap, not an event.
	// The entry's channel (such as cluster or audit) does not select the
	// kind: a level ERR, SEC or WRN entry of any channel is an event.
	LogClusterError    LogEvent = "cluster-error"
	LogClusterSecurity LogEvent = "cluster-security"
	LogClusterWarning  LogEvent = "cluster-warning"
)

// verificationLogEvents lists the log event kinds. Assertion and fatal
// signatures are found by their own patterns anywhere in a line, since the
// daemon prints them outside the cluster log. The cluster log kinds are never
// found by a pattern: they come from the structural severity of a recognized
// log envelope (see parseVerificationEnvelope).
var verificationLogEvents = []struct {
	event   LogEvent
	pattern *regexp.Regexp
}{
	{LogAssertion, regexp.MustCompile(`FAILED (ceph_)?assert|ceph_abort`)},
	{LogFatalSignal, regexp.MustCompile(`Caught signal`)},
	{LogClusterError, nil},
	{LogClusterSecurity, nil},
	{LogClusterWarning, nil},
}

// verificationEnvelope is the producer, channel and severity of a cluster
// log entry, parsed from the structure of the line and not from its message.
type verificationEnvelope struct {
	channel  string
	priority string
}

// The supported envelopes, as the pinned Ceph source (7f793731f1b3, Tentacle
// 20.2.4) prints them:
//
//   - A daemon's own log of a cluster log entry (LogChannel::do_log with the
//     dout prefix of LogClient.cc, Log::_flush_logbuf):
//     "<stamp> <thread> <level> log_channel(<channel>) log [<PRIO>] : <msg>".
//   - A cluster log entry (operator<< of LogEntry, as the MON writes it with
//     mon_cluster_log_to_stderr, which LogMonitor::log_external prefixes
//     with the channel, and to its channel file):
//     "[<channel> ]<stamp> <name> (<rank>) <seq> : <channel> [<PRIO>] <msg>".
//
// PRIO is one of DBG, INF, SEC, WRN and ERR (operator<< of clog_type).
// Anything else is not an envelope.
var (
	daemonEnvelope  = regexp.MustCompile(`^(\S+) +[0-9a-f]+ +-?\d+ +log_channel\(([A-Za-z0-9_.-]+)\) log \[(DBG|INF|SEC|WRN|ERR)\] : `)
	clusterEnvelope = regexp.MustCompile(`^(?:([A-Za-z0-9_.-]+) )?(\S+) \S+ \(\S+\) \d+ : ([A-Za-z0-9_.-]+) \[(DBG|INF|SEC|WRN|ERR)\](?: |$)`)
	// severityMarker finds a severity marker anywhere in a line. A line
	// that carries one without a supported envelope is not classified; it is
	// counted and reported as a gap.
	severityMarker = regexp.MustCompile(`\[(ERR|SEC|WRN)\]`)
)

// parseVerificationEnvelope returns the envelope at the start of a line, or
// false when the line has none of the supported forms. A cluster log line
// whose leading channel differs from its own channel is ambiguous and has no
// envelope.
func parseVerificationEnvelope(line string) (verificationEnvelope, bool) {
	if m := daemonEnvelope.FindStringSubmatch(line); m != nil && validCephStamp(m[1]) {
		return verificationEnvelope{channel: m[2], priority: m[3]}, true
	}
	if m := clusterEnvelope.FindStringSubmatch(line); m != nil && validCephStamp(m[2]) && (m[1] == "" || m[1] == m[3]) {
		return verificationEnvelope{channel: m[3], priority: m[4]}, true
	}
	return verificationEnvelope{}, false
}

// cephStampShape is the stamp as the pinned producer writes it
// (log/LogClock.h append_time, and utime_t localtime for a LogEntry): a
// calendar date and time, three (coarse) or six fractional digits and a
// numeric timezone.
var cephStampShape = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.(\d{3}|\d{6})[+-]\d{4}$`)

// validCephStamp reports whether s has that shape and real calendar and
// clock values.
func validCephStamp(s string) bool {
	if !cephStampShape.MatchString(s) {
		return false
	}
	if _, err := time.Parse("2006-01-02T15:04:05.999999999-0700", s); err != nil {
		return false
	}
	// time.Parse accepts offset minutes of 60 and more, which strftime's %z
	// cannot print. Real offsets lie between -1200 and +1400.
	zone := s[len(s)-5:]
	hours, _ := strconv.Atoi(zone[1:3])
	minutes, _ := strconv.Atoi(zone[3:])
	return minutes < 60 && (zone[0] == '+' && hours <= 14 || zone[0] == '-' && hours <= 12)
}

// classifyVerificationLine returns the event kind of a log line ("" for no
// event) and its envelope when it has one. unclassified is true when the line
// carries a severity marker without a supported envelope: its kind is not
// guessed from the marker.
func classifyVerificationLine(line string) (kind LogEvent, envelope verificationEnvelope, hasEnvelope, unclassified bool) {
	envelope, hasEnvelope = parseVerificationEnvelope(line)
	for _, known := range verificationLogEvents[:2] {
		if known.pattern.MatchString(line) {
			return known.event, envelope, hasEnvelope, false
		}
	}
	if !hasEnvelope {
		return "", envelope, false, severityMarker.MatchString(line)
	}
	switch envelope.priority {
	case "ERR":
		kind = LogClusterError
	case "SEC":
		kind = LogClusterSecurity
	case "WRN":
		kind = LogClusterWarning
	}
	return kind, envelope, true, false
}

// LogExpectation permits, or with Required requires, log events that match
// all of its non-empty criteria. Pattern is required.
type LogExpectation struct {
	// Name identifies the expectation in the report.
	Name string
	// Required makes the expectation fail the verification unless at least
	// MinCount events (at least one) match it. Without Required, MinCount
	// must be zero.
	Required bool
	MinCount int
	// MaxCount limits how many events it permits; zero is no limit. Further
	// events go on to the next expectations and are unexpected when none
	// permits them.
	MaxCount int
	// Daemon is the source as the report names it, such as "osd.0".
	Daemon string
	// Event is the kind of line.
	Event LogEvent
	// Pattern is a regular expression (RE2 syntax) that must match the
	// whole redacted line without its Docker timestamp, including the log
	// envelope. Event, not Pattern, selects the structural kind.
	Pattern string
}

// CrashExpectation permits, or with Required requires, new crashes that
// match all of its non-empty criteria; at least one of Daemon, Process and
// Signature is required.
type CrashExpectation struct {
	Name     string
	Required bool
	MinCount int
	MaxCount int
	// Daemon is the source whose crash directory held the crash, such as
	// "osd.0".
	Daemon string
	// Process is the crashed process, such as "ceph-osd".
	Process string
	// Signature is a regular expression (RE2 syntax) that must match one
	// line of the crash's stack_sig, assert_condition, assert_func,
	// assert_msg or backtrace fields.
	Signature string
}

// VerificationOutcome is the result of one check or of a whole
// verification.
type VerificationOutcome string

const (
	// VerificationClean means that no unexpected event was observed and
	// every required expectation was satisfied within the reported sources
	// and window, all of which were observed.
	VerificationClean VerificationOutcome = "clean"
	// VerificationUnexpected means that an observed event was not permitted
	// or that a required expectation was not satisfied.
	VerificationUnexpected VerificationOutcome = "unexpected"
	// VerificationIncomplete means that no unexpected event was found, but a
	// requested observation could not be completed; Gaps say which.
	VerificationIncomplete VerificationOutcome = "incomplete"
)

// VerificationReport is the JSON-serializable result of one evaluation.
// Outcome is unexpected when a check is, else incomplete when a check is,
// else clean.
type VerificationReport struct {
	SchemaVersion int                 `json:"schema_version"`
	Name          string              `json:"name"`
	Cluster       string              `json:"cluster"`
	Outcome       VerificationOutcome `json:"outcome"`
	Window        VerificationWindow  `json:"window"`
	// Images are the cluster's role images; a daemon started with WithDaemonImage
	// is not reflected.
	Images  ImageSet             `json:"images"`
	Sources []VerificationSource `json:"sources"`
	Logs    VerificationCheck    `json:"logs"`
	Crashes VerificationCheck    `json:"crashes"`
	// LogExpectations and CrashExpectations are the effective policy.
	LogExpectations   []LogExpectation   `json:"log_expectations"`
	CrashExpectations []CrashExpectation `json:"crash_expectations"`
	Notes             []string           `json:"notes"`
}

// VerificationWindow is the observed interval, on the Docker engine's clock.
// Phase is "test": bootstrap before the activation and the cleanups and
// termination after the evaluation are outside it.
type VerificationWindow struct {
	Phase     string    `json:"phase"`
	StartedAt time.Time `json:"started_at"`
	// EndedAt is zero when the engine's clock could not be read.
	EndedAt     time.Time `json:"ended_at"`
	EvaluatedAt time.Time `json:"evaluated_at"`
}

// VerificationSource is one container generation that was selected at
// activation. StartedAt and RestartCount identify the generation.
type VerificationSource struct {
	Source       string `json:"source"`
	Role         string `json:"role"`
	Name         string `json:"name"`
	ContainerID  string `json:"container_id,omitempty"`
	ImageID      string `json:"image_id,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	RestartCount int    `json:"restart_count"`
	State        string `json:"state,omitempty"`
	LogDriver    string `json:"log_driver,omitempty"`
	// LogBytes counts the log stream bytes read in the window.
	LogBytes int64 `json:"log_bytes"`
	// BaselineCrashes are the crash IDs that were present at activation.
	BaselineCrashes []string `json:"baseline_crashes"`
}

// VerificationCheck is the result of the log or the crash check. Findings
// keep the unexpected events first and then the permitted ones, up to
// MaxFindings; OmittedFindings counts the rest. Permitted and Unexpected
// count all of them.
type VerificationCheck struct {
	Outcome         VerificationOutcome   `json:"outcome"`
	Permitted       int                   `json:"permitted"`
	Unexpected      int                   `json:"unexpected"`
	Findings        []VerificationFinding `json:"findings"`
	OmittedFindings int                   `json:"omitted_findings"`
	Expectations    []ExpectationResult   `json:"expectations"`
	Gaps            []VerificationGap     `json:"gaps"`
}

// VerificationFinding is one evaluated event. Expectation names the
// expectation that permitted it; it is empty for an unexpected event.
type VerificationFinding struct {
	Source      string `json:"source"`
	ContainerID string `json:"container_id,omitempty"`
	// Event is a LogEvent, or "crash".
	Event string `json:"event"`
	Time  string `json:"time,omitempty"`
	// Text is the redacted line, or a summary of the crash, at most 1 KiB.
	Text        string `json:"text"`
	Truncated   bool   `json:"truncated,omitempty"`
	CrashID     string `json:"crash_id,omitempty"`
	Expectation string `json:"expectation,omitempty"`
}

// ExpectationResult is how many events one expectation matched.
type ExpectationResult struct {
	Name      string `json:"name"`
	Required  bool   `json:"required"`
	MinCount  int    `json:"min_count"`
	MaxCount  int    `json:"max_count"`
	Matched   int    `json:"matched"`
	Satisfied bool   `json:"satisfied"`
}

// VerificationGap is an observation that could not be completed.
type VerificationGap struct {
	Source string `json:"source,omitempty"`
	Reason string `json:"reason"`
}

// Verification is one activated post-test verification of a dedicated
// cluster. It keeps the baseline that its activation recorded.
type Verification struct {
	name    string
	config  VerificationConfig
	logs    []*verificationRule
	crashes []*verificationRule
	cluster string
	images  ImageSet

	// The cluster's operations, replaced in tests.
	admit    func() error
	snapshot func(context.Context) (diagnosticsSnapshot, error)
	docker   func(context.Context) (verificationDocker, func(), error)
	now      func() time.Time

	mu       sync.Mutex
	baseline *verificationBaseline
	// evaluating admits one evaluation at a time, since evaluations count
	// matches in the shared rules; waiting for it is bounded by the
	// evaluation's own timeout and context.
	evaluating chan struct{}
}

const (
	defaultVerificationName = "post-test"
	maxVerificationRules    = 256
	maxFindingText          = 1024
)

type verificationRule struct {
	name            string
	required        bool
	minCount, max   int
	daemon, process string
	event           LogEvent
	pattern         *regexp.Regexp
	matched         int
}

func normalizeVerificationConfig(config VerificationConfig) (VerificationConfig, []*verificationRule, []*verificationRule, error) {
	if config.Name == "" {
		config.Name = defaultVerificationName
	}
	if config.Timeout == 0 {
		config.Timeout = 2 * time.Minute
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 20 * time.Second
	}
	if config.MaxLogBytes == 0 {
		config.MaxLogBytes = 32 << 20
	}
	if config.MaxFindings == 0 {
		config.MaxFindings = 256
	}
	fail := func(format string, args ...any) (VerificationConfig, []*verificationRule, []*verificationRule, error) {
		return config, nil, nil, invalid(fmt.Errorf("verification: "+format, args...))
	}
	switch {
	case !verificationName(config.Name):
		return fail("name must be one line of at most 128 bytes")
	case config.Timeout < 0 || config.Timeout > 5*time.Minute || config.OperationTimeout < 0 || config.OperationTimeout > time.Minute:
		return fail("timeouts out of range")
	case config.MaxLogBytes < 1 || config.MaxLogBytes > 256<<20 || config.MaxFindings < 1 || config.MaxFindings > 4096:
		return fail("limits out of range")
	case len(config.Logs) > maxVerificationRules || len(config.Crashes) > maxVerificationRules:
		return fail("more than %d expectations of one kind", maxVerificationRules)
	case len(config.AdditionalContainers) > 64 || len(config.RedactValues) > 128:
		return fail("too many additional containers or redaction values")
	}
	for _, target := range config.AdditionalContainers {
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return fail("invalid container description")
		}
	}
	for _, value := range config.RedactValues {
		if len(value) > 4096 {
			return fail("redaction value exceeds 4096 bytes")
		}
	}
	names := map[string]bool{}
	rule := func(kind, name string, required bool, minCount, maxCount int, daemon, pattern string) (*verificationRule, error) {
		switch {
		case !verificationName(name) || name == "":
			return nil, fmt.Errorf("%s expectation name must be one non-empty line of at most 128 bytes", kind)
		case names[name]:
			return nil, fmt.Errorf("expectation name %q is used twice", name)
		case minCount < 0 || maxCount < 0:
			return nil, fmt.Errorf("expectation %q has a negative count", name)
		case !required && minCount != 0:
			return nil, fmt.Errorf("expectation %q sets MinCount without Required", name)
		case !verificationName(daemon) || len(pattern) > 1024:
			return nil, fmt.Errorf("expectation %q has an invalid criterion", name)
		}
		names[name] = true
		if required && minCount == 0 {
			minCount = 1
		}
		if maxCount != 0 && minCount > maxCount {
			return nil, fmt.Errorf("expectation %q requires more events than it permits", name)
		}
		r := &verificationRule{name: name, required: required, minCount: minCount, max: maxCount, daemon: daemon}
		if pattern != "" {
			compiled, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("expectation %q: %w", name, err)
			}
			r.pattern = compiled
		}
		return r, nil
	}
	var logs, crashes []*verificationRule
	for _, expectation := range config.Logs {
		r, err := rule("log", expectation.Name, expectation.Required, expectation.MinCount, expectation.MaxCount, expectation.Daemon, expectation.Pattern)
		if err != nil {
			return fail("%v", err)
		}
		if expectation.Pattern == "" {
			return fail("log expectation %q has no pattern", expectation.Name)
		}
		if expectation.Event != "" && !slices.ContainsFunc(verificationLogEvents, func(known struct {
			event   LogEvent
			pattern *regexp.Regexp
		}) bool {
			return known.event == expectation.Event
		}) {
			return fail("log expectation %q has unknown event %q", expectation.Name, expectation.Event)
		}
		r.event = expectation.Event
		logs = append(logs, r)
	}
	for _, expectation := range config.Crashes {
		r, err := rule("crash", expectation.Name, expectation.Required, expectation.MinCount, expectation.MaxCount, expectation.Daemon, expectation.Signature)
		if err != nil {
			return fail("%v", err)
		}
		if !verificationName(expectation.Process) {
			return fail("crash expectation %q has an invalid process", expectation.Name)
		}
		if expectation.Daemon == "" && expectation.Process == "" && expectation.Signature == "" {
			return fail("crash expectation %q needs a daemon, a process or a signature", expectation.Name)
		}
		r.process = expectation.Process
		crashes = append(crashes, r)
	}
	config.Logs, config.Crashes = slices.Clone(config.Logs), slices.Clone(config.Crashes)
	return config, logs, crashes, nil
}

func verificationName(name string) bool {
	return len(name) <= 128 && !strings.ContainsFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f })
}

// Verify activates a post-test verification of the cluster and adds a check
// that evaluates it, so that a verification that is not clean fails the
// test and, with FinalizationConfig.Archive, triggers the failure archive
// before any cleanup. Activation records the baseline: the selected
// container generations (the cluster's daemon containers and
// config.AdditionalContainers), the crash IDs already in their crash
// directories, and the start of the window on the Docker engine's clock.
// It returns an error when the configuration is invalid, when the engine
// cannot be reached, after finalization has started, and with
// ErrSharedCluster for a cluster that a SharedCluster shares: cluster-wide
// events cannot be attributed to one tenant.
//
// The check fails the test both for an unexpected and for an incomplete
// verification. It never changes the cluster: it restarts, archives and
// removes nothing, and a failure never skips the cleanups or Terminate.
func (f *Finalization) Verify(ctx context.Context, config VerificationConfig) (*Verification, error) {
	if f == nil || f.activate == nil {
		return nil, invalid(errors.New("Verify needs a Finalization from TerminateOnCleanup"))
	}
	if config.Name == "" {
		config.Name = defaultVerificationName
	}
	f.mu.Lock()
	switch {
	case f.started:
		f.mu.Unlock()
		return nil, invalid(errors.New("verification activated after finalization started"))
	case f.verificationNames[config.Name]:
		f.mu.Unlock()
		return nil, invalid(fmt.Errorf("verification %q is already active", config.Name))
	}
	if f.verificationNames == nil {
		f.verificationNames = map[string]bool{}
	}
	f.verificationNames[config.Name] = true
	f.mu.Unlock()
	v, err := f.activate(ctx, config)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil && f.started {
		err = invalid(errors.New("verification activated after finalization started"))
	}
	if err != nil {
		delete(f.verificationNames, config.Name)
		return nil, err
	}
	f.checks = append(f.checks, finalizationStep{name: "verification " + v.name, run: f.verificationCheck(v)})
	return v, nil
}

// verificationCheck evaluates v, keeps its report for the archive and fails
// unless the verification is clean.
func (f *Finalization) verificationCheck(v *Verification) func(context.Context) error {
	return func(ctx context.Context) error {
		report, err := v.Evaluate(ctx)
		if report != nil {
			f.mu.Lock()
			f.verificationReports = append(f.verificationReports, report)
			f.mu.Unlock()
			f.t.Logf("ceph post-test verification %s: %s", v.name, report.summary())
		}
		if err != nil {
			return err
		}
		if report.Outcome != VerificationClean {
			return fmt.Errorf("post-test verification %s: %s", v.name, report.summary())
		}
		return nil
	}
}

func (c *Container) startVerification(ctx context.Context, config VerificationConfig) (*Verification, error) {
	v, err := newVerification(config)
	if err != nil {
		return nil, err
	}
	v.cluster, v.images = c.owner, c.staticFacts().Images
	v.admit = func() error { return c.admitShared("Finalization.Verify", sharedRefused) }
	v.snapshot = func(ctx context.Context) (diagnosticsSnapshot, error) {
		return c.diagnosticsSnapshot(ctx, DiagnosticsConfig{MaxContainers: 128, AdditionalContainers: v.config.AdditionalContainers, RedactValues: v.config.RedactValues})
	}
	v.docker = dialVerificationDocker
	return v, v.activate(ctx)
}

func newVerification(config VerificationConfig) (*Verification, error) {
	config, logs, crashes, err := normalizeVerificationConfig(config)
	if err != nil {
		return nil, err
	}
	return &Verification{name: config.Name, config: config, logs: logs, crashes: crashes, now: func() time.Time { return time.Now().UTC() },
		evaluating: make(chan struct{}, 1)}, nil
}

// activate records the baseline.
func (v *Verification) activate(ctx context.Context) error {
	if err := v.admit(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, v.config.Timeout)
	defer cancel()
	snapshot, err := v.snapshot(ctx)
	if err != nil {
		return fmt.Errorf("verification baseline: %w", err)
	}
	docker, closeDocker, err := v.docker(ctx)
	if err != nil {
		return fmt.Errorf("verification baseline: connect to Docker: %w", err)
	}
	defer closeDocker()
	baseline, err := recordVerificationBaseline(ctx, v.config, snapshot, docker)
	if err != nil {
		return err
	}
	v.mu.Lock()
	v.baseline = baseline
	v.mu.Unlock()
	return nil
}

// Evaluate evaluates the window from the activation until now and returns
// its report. It changes nothing and can be repeated; each evaluation starts
// from the activation's baseline. Gaps make a report incomplete instead of
// returning an error: Evaluate returns an error only when it cannot evaluate
// at all, as for a cluster that a SharedCluster shares now.
func (v *Verification) Evaluate(ctx context.Context) (*VerificationReport, error) {
	if v == nil {
		return nil, invalid(errors.New("verification is nil"))
	}
	if err := v.admit(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, v.config.Timeout)
	defer cancel()
	// A free slot is taken even with an ended context, whose evaluation is
	// then incomplete; only waiting for a busy one is bounded by ctx.
	select {
	case v.evaluating <- struct{}{}:
	default:
		select {
		case v.evaluating <- struct{}{}:
		case <-ctx.Done():
			return nil, fmt.Errorf("verification %s: another evaluation was still running: %w", v.name, ctx.Err())
		}
	}
	defer func() { <-v.evaluating }()
	v.mu.Lock()
	baseline := v.baseline
	v.mu.Unlock()
	if baseline == nil {
		return nil, invalid(errors.New("verification was not activated"))
	}
	observation := &verificationObservation{}
	if snapshot, err := v.snapshot(ctx); err != nil {
		observation.gap("", "list the cluster's containers: "+err.Error())
	} else {
		observation.current = &snapshot
	}
	docker, closeDocker, err := v.docker(ctx)
	if err != nil {
		observation.gap("", "connect to Docker: "+err.Error())
	} else {
		defer closeDocker()
		observeVerification(ctx, v.config, baseline, docker, observation)
	}
	return v.evaluate(observation), nil
}

// verificationBaseline is what the activation recorded.
type verificationBaseline struct {
	start   time.Time
	targets []verificationTarget
	secrets []string
	// gaps are observations that the activation could not complete; they
	// make every evaluation's checks incomplete.
	logGaps, crashGaps []VerificationGap
}

type verificationTarget struct {
	DiagnosticsContainer
	source, id string
	generation *verificationGeneration
	// crashes holds the crash IDs at activation; nil when the crash
	// directory could not be read completely.
	crashes map[string]bool
}

type verificationGeneration struct {
	image, startedAt, state string
	restartCount            int
	tty                     bool
	logDriver               string
	logRotation             string
}

func (g *verificationGeneration) same(other *verificationGeneration) bool {
	return g != nil && other != nil && g.startedAt == other.startedAt && g.restartCount == other.restartCount
}

// verificationObservation is what one evaluation observed.
type verificationObservation struct {
	end      time.Time
	current  *diagnosticsSnapshot
	sources  []VerificationSource
	logs     []verificationEvent
	crashes  []verificationEvent
	logGaps  []VerificationGap
	crashGap []VerificationGap
	// gaps apply to both checks.
	gaps []VerificationGap
}

func (o *verificationObservation) gap(source, reason string) {
	o.gaps = append(o.gaps, VerificationGap{Source: source, Reason: reason})
}

type verificationEvent struct {
	source, containerID string
	event               string
	time                string
	// text is the bounded text a finding keeps; match is the whole redacted
	// text that expectations are matched against.
	text, match      string
	truncated        bool
	crashID, process string
	// signature holds the lines a crash signature is matched against.
	signature []string
}

func (r *verificationRule) matches(e verificationEvent) bool {
	if r.daemon != "" && r.daemon != e.source {
		return false
	}
	if r.event != "" && string(r.event) != e.event {
		return false
	}
	if r.process != "" && r.process != e.process {
		return false
	}
	if r.pattern == nil {
		return true
	}
	if e.event != "crash" {
		return r.pattern.MatchString(e.match)
	}
	return slices.ContainsFunc(e.signature, r.pattern.MatchString)
}

// evaluateVerificationCheck matches each event, in order, with the first
// expectation that matches it and still permits one more.
func evaluateVerificationCheck(events []verificationEvent, rules []*verificationRule, gaps []VerificationGap, maxFindings int) VerificationCheck {
	check := VerificationCheck{Findings: []VerificationFinding{}, Expectations: []ExpectationResult{}, Gaps: append([]VerificationGap{}, gaps...)}
	for _, r := range rules {
		r.matched = 0
	}
	var unexpected, permitted []VerificationFinding
	for _, e := range events {
		finding := VerificationFinding{Source: e.source, ContainerID: e.containerID, Event: e.event, Time: e.time, Text: e.text, Truncated: e.truncated, CrashID: e.crashID}
		for _, r := range rules {
			if (r.max == 0 || r.matched < r.max) && r.matches(e) {
				r.matched++
				finding.Expectation = r.name
				break
			}
		}
		if finding.Expectation == "" {
			unexpected = append(unexpected, finding)
		} else {
			permitted = append(permitted, finding)
		}
	}
	check.Unexpected, check.Permitted = len(unexpected), len(permitted)
	unsatisfied := false
	for _, r := range rules {
		result := ExpectationResult{Name: r.name, Required: r.required, MinCount: r.minCount, MaxCount: r.max, Matched: r.matched, Satisfied: !r.required || r.matched >= r.minCount}
		unsatisfied = unsatisfied || !result.Satisfied
		check.Expectations = append(check.Expectations, result)
	}
	retained := append(unexpected, permitted...)
	if len(retained) > maxFindings {
		check.OmittedFindings = len(retained) - maxFindings
		retained = retained[:maxFindings]
	}
	check.Findings = append(check.Findings, retained...)
	switch {
	case check.Unexpected > 0 || unsatisfied:
		check.Outcome = VerificationUnexpected
	case len(check.Gaps) > 0:
		check.Outcome = VerificationIncomplete
	default:
		check.Outcome = VerificationClean
	}
	return check
}

var verificationNotes = []string{
	"The window starts at the activation and ends at the evaluation, on the Docker engine's clock; bootstrap before it and the cleanups and termination after it are outside the window.",
	"Log coverage is the Docker log stream of each container generation selected at activation, read only with the json-file driver without rotation; lines that a daemon did not write to its output, such as those below its log level, are not covered. A cluster log entry is an event by the severity of its log envelope, and only the daemon log and cluster log forms of the pinned Tentacle source are recognized; a line with a severity marker in another form is a gap.",
	"A crash is new when its crash ID was not in that container's crash directory at activation. Crashes of removed containers and of containers added after the activation are not covered, and are reported as gaps.",
	"Clean is evidence for these sources and this window, not proof that no daemon failed. Shared-cluster tenants, scrub results and recovery waits are not verified.",
	"The bug compatibility selection is a build property that the library does not see.",
}

func (v *Verification) evaluate(o *verificationObservation) *VerificationReport {
	b := v.baseline
	report := &VerificationReport{SchemaVersion: 1, Name: v.name, Cluster: v.cluster, Images: v.images,
		Window:          VerificationWindow{Phase: "test", StartedAt: b.start, EndedAt: o.end, EvaluatedAt: v.now()},
		Sources:         o.sources,
		LogExpectations: slices.Clone(v.config.Logs), CrashExpectations: slices.Clone(v.config.Crashes), Notes: slices.Clone(verificationNotes)}
	if report.Sources == nil {
		report.Sources = []VerificationSource{}
	}
	if report.LogExpectations == nil {
		report.LogExpectations = []LogExpectation{}
	}
	if report.CrashExpectations == nil {
		report.CrashExpectations = []CrashExpectation{}
	}
	logGaps := slices.Concat(b.logGaps, o.gaps, o.logGaps)
	crashGaps := slices.Concat(b.crashGaps, o.gaps, o.crashGap)
	report.Logs = evaluateVerificationCheck(o.logs, v.logs, logGaps, v.config.MaxFindings)
	report.Crashes = evaluateVerificationCheck(o.crashes, v.crashes, crashGaps, v.config.MaxFindings)
	switch {
	case report.Logs.Outcome == VerificationUnexpected || report.Crashes.Outcome == VerificationUnexpected:
		report.Outcome = VerificationUnexpected
	case report.Logs.Outcome == VerificationIncomplete || report.Crashes.Outcome == VerificationIncomplete:
		report.Outcome = VerificationIncomplete
	default:
		report.Outcome = VerificationClean
	}
	return report
}

// summary is one bounded line about the report.
func (r *VerificationReport) summary() string {
	parts := []string{string(r.Outcome)}
	for _, check := range []struct {
		name  string
		check VerificationCheck
	}{{"logs", r.Logs}, {"crashes", r.Crashes}} {
		part := fmt.Sprintf("%s %s (%d unexpected, %d permitted, %d gaps", check.name, check.check.Outcome, check.check.Unexpected, check.check.Permitted, len(check.check.Gaps))
		var unsatisfied []string
		for _, result := range check.check.Expectations {
			if !result.Satisfied {
				unsatisfied = append(unsatisfied, fmt.Sprintf("%s matched %d of %d", result.Name, result.Matched, result.MinCount))
			}
		}
		if len(unsatisfied) > 0 {
			part += "; required but missing: " + strings.Join(unsatisfied, ", ")
		}
		if check.check.Unexpected > 0 {
			first := check.check.Findings[0]
			part += fmt.Sprintf("; first unexpected: %s %s %q", first.Source, first.Event, first.Text)
		}
		if len(check.check.Gaps) > 0 {
			first := check.check.Gaps[0]
			part += fmt.Sprintf("; first gap: %s %s", first.Source, first.Reason)
		}
		parts = append(parts, part+")")
	}
	text, _ := diagnosticBoundText(strings.Join(parts, "; "), 4096)
	return text
}
