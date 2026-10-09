package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const healthDetailsFixtureFSID = "9b9b782d-a094-4b2a-b883-87ea6ea6c5f8"
const healthDetailsFixtureHealthy = `{"status":"HEALTH_OK","checks":{},"mutes":[]}`
const healthDetailsFixtureWarning = `{"status":"HEALTH_WARN","checks":{"FUTURE_CODE":{"severity":"HEALTH_WARN","summary":{"message":"native summary","count":9007199254740993},"detail":[{"message":"detail one"},{"message":"detail two"}],"muted":false}},"mutes":[]}`

type healthDetailsFixtureContainer struct {
	testcontainers.Container
	calls   [][]string
	outputs []string
	exits   map[int]int
	readers map[int]io.Reader
	hook    func(context.Context, int) error
}

func healthDetailsFixtureStream(output string) io.Reader {
	return healthDetailsFixtureChannelStream(byte(stdcopy.Stdout), output)
}

func healthDetailsFixtureChannelStream(channel byte, output string) io.Reader {
	var header [8]byte
	header[0] = channel
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return io.MultiReader(bytes.NewReader(header[:]), strings.NewReader(output))
}

func (ctr *healthDetailsFixtureContainer) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.calls = append(ctr.calls, slices.Clone(args))
	call := len(ctr.calls)
	if ctr.hook != nil {
		if err := ctr.hook(ctx, call); err != nil {
			return 0, nil, err
		}
	}
	if len(args) < 3 || !slices.Equal(args[:3], []string{"ceph", "--connect-timeout", "5"}) || call > len(ctr.outputs) {
		return 0, nil, errors.New("unexpected health fixture command")
	}
	if reader := ctr.readers[call]; reader != nil {
		return ctr.exits[call], reader, nil
	}
	return ctr.exits[call], healthDetailsFixtureStream(ctr.outputs[call-1]), nil
}

func newHealthDetailsFixture() (*Container, *healthDetailsFixtureContainer) {
	ctr := &healthDetailsFixtureContainer{
		outputs: []string{healthDetailsFixtureFSID + "\n", healthDetailsFixtureWarning, healthDetailsFixtureFSID + "\n"},
		exits:   make(map[int]int), readers: make(map[int]io.Reader),
	}
	cluster := &Container{Container: ctr,
		config: []byte("[global]\nfsid = " + healthDetailsFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	return cluster, ctr
}

func assertHealthDetailsFailure(t *testing.T, cluster *Container, ctx context.Context) error {
	t.Helper()
	result, err := cluster.HealthDetails(ctx)
	if err == nil || !reflect.DeepEqual(result, HealthSnapshot{}) {
		t.Fatalf("unusable health snapshot published: %+v error=%v", result, err)
	}
	return err
}

func TestHealthDetailsReportsNativeMessagesAndReadOnlyScope(t *testing.T) {
	cluster, ctr := newHealthDetailsFixture()
	result, err := cluster.HealthDetails(t.Context())
	want := HealthSnapshot{FSID: healthDetailsFixtureFSID, Status: "HEALTH_WARN",
		Checks: map[string]HealthCheck{"FUTURE_CODE": {Severity: "HEALTH_WARN", Summary: "native summary",
			Count: 9007199254740993, Details: []string{"detail one", "detail two"}}}, Mutes: []HealthMute{}}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("native count/messages changed: %+v error=%v", result, err)
	}
	wantCalls := [][]string{
		{"ceph", "--connect-timeout", "5", "fsid"},
		{"ceph", "--connect-timeout", "5", "health", "detail", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "fsid"},
	}
	if !reflect.DeepEqual(ctr.calls, wantCalls) {
		t.Fatalf("unexpected mutation, handle query or missing identity check: %v", ctr.calls)
	}
	// A healthy MON-only fixture needs no manager, OSD, pool or service map.
	cluster, ctr = newHealthDetailsFixture()
	ctr.outputs[1] = healthDetailsFixtureHealthy
	result, err = cluster.HealthDetails(t.Context())
	if err != nil || result.Status != "HEALTH_OK" || result.FSID != healthDetailsFixtureFSID ||
		result.Checks == nil || len(result.Checks) != 0 || result.Mutes == nil || len(result.Mutes) != 0 || len(ctr.calls) != 3 {
		t.Fatalf("cold healthy fixture rejected: %+v error=%v", result, err)
	}
}

func TestHealthDetailsUsesOneCapturedControlHandle(t *testing.T) {
	cluster, embedded := newHealthDetailsFixture()
	_, control := newHealthDetailsFixture()
	_, replacement := newHealthDetailsFixture()
	cluster.controlPlane = control
	control.hook = func(_ context.Context, call int) error {
		if call == 1 {
			cluster.controlMu.Lock()
			cluster.controlPlane = replacement
			cluster.controlMu.Unlock()
		}
		return nil
	}
	result, err := cluster.HealthDetails(t.Context())
	if err != nil || result.FSID != healthDetailsFixtureFSID || len(control.calls) != 3 || len(embedded.calls) != 0 || len(replacement.calls) != 0 {
		t.Fatalf("observation changed control handle: result=%+v error=%v calls=%v/%v/%v", result, err, embedded.calls, control.calls, replacement.calls)
	}
}

func TestHealthDetailsRejectsUnavailableBootstrapBeforeQuery(t *testing.T) {
	for _, point := range []string{"closed", "control", "config", "keyring", "ambiguous FSID", "zero FSID", "malformed FSID"} {
		t.Run(point, func(t *testing.T) {
			cluster, ctr := newHealthDetailsFixture()
			switch point {
			case "closed":
				cluster.closed = true
			case "control":
				cluster.Container = nil
			case "config":
				cluster.config = nil
			case "keyring":
				cluster.keyring = nil
			case "ambiguous FSID":
				cluster.config = append(cluster.config, []byte("fsid = "+healthDetailsFixtureFSID+"\n")...)
			case "zero FSID":
				cluster.config = []byte("[global]\nfsid = 00000000-0000-0000-0000-000000000000\n")
			case "malformed FSID":
				cluster.config = []byte("[global]\nfsid = SECRET_BOOTSTRAP_VALUE\n")
			}
			err := assertHealthDetailsFailure(t, cluster, t.Context())
			if len(ctr.calls) != 0 || strings.Contains(err.Error(), "SECRET_BOOTSTRAP_VALUE") {
				t.Fatalf("unavailable fixture was queried or exposed: %v calls=%v", err, ctr.calls)
			}
		})
	}
	assertHealthDetailsFailure(t, nil, t.Context())
}

func TestHealthDetailsRejectsFSIDDriftBeforeAndAfterHealth(t *testing.T) {
	for _, call := range []int{1, 3} {
		for _, fsid := range []string{"61098f7f-474e-4403-ad31-a976f7c4e731", "", "00000000-0000-0000-0000-000000000000", strings.ToUpper(healthDetailsFixtureFSID), "SECRET_FOREIGN_FSID"} {
			t.Run(fmt.Sprintf("query-%d/%s", call, fsid), func(t *testing.T) {
				cluster, ctr := newHealthDetailsFixture()
				ctr.outputs[call-1] = fsid
				err := assertHealthDetailsFailure(t, cluster, t.Context())
				if len(ctr.calls) != call || strings.Contains(err.Error(), "SECRET_FOREIGN_FSID") {
					t.Fatalf("FSID guard continued or exposed identity: %v calls=%v", err, ctr.calls)
				}
			})
		}
	}
}

type healthDetailsErrorReader struct{ err error }

func (reader healthDetailsErrorReader) Read([]byte) (int, error) { return 0, reader.err }

func TestHealthDetailsRedactsEveryNativeFailure(t *testing.T) {
	const secret = "SECRET_NATIVE_HEALTH_OUTPUT"
	for call := 1; call <= 3; call++ {
		for _, mode := range []string{"exec", "read", "exit", "exit stdout and stderr", "wrapped cancellation", "wrapped deadline"} {
			t.Run(fmt.Sprintf("%d/%s", call, mode), func(t *testing.T) {
				cluster, ctr := newHealthDetailsFixture()
				cluster.keyring = []byte(secret)
				cause := errors.New(secret)
				var canonical error
				switch mode {
				case "exec":
					ctr.hook = func(_ context.Context, current int) error {
						if current == call {
							return cause
						}
						return nil
					}
				case "read":
					ctr.readers[call] = healthDetailsErrorReader{cause}
				case "exit":
					ctr.exits[call], ctr.outputs[call-1] = 1, secret
				case "exit stdout and stderr":
					ctr.exits[call] = 1
					ctr.readers[call] = io.MultiReader(healthDetailsFixtureStream(secret),
						healthDetailsFixtureChannelStream(byte(stdcopy.Stderr), "stderr "+secret))
				case "wrapped cancellation", "wrapped deadline":
					canonical = context.Canceled
					if mode == "wrapped deadline" {
						canonical = context.DeadlineExceeded
					}
					cause = fmt.Errorf(secret+": %w", canonical)
					ctr.hook = func(_ context.Context, current int) error {
						if current == call {
							return cause
						}
						return nil
					}
				}
				err := assertHealthDetailsFailure(t, cluster, t.Context())
				if strings.Contains(fmt.Sprintf("%+v", err), secret) || errors.Is(err, cause) || len(ctr.calls) != call {
					t.Fatalf("native output/cause exposed or queries continued: %v calls=%v", err, ctr.calls)
				}
				if canonical != nil && !errors.Is(err, canonical) {
					t.Fatalf("canonical context cause hidden: %v", err)
				}
			})
		}
	}
	cluster, ctr := newHealthDetailsFixture()
	ctr.outputs[1] = `{"status":"SECRET_NATIVE_HEALTH_OUTPUT"}`
	err := assertHealthDetailsFailure(t, cluster, t.Context())
	if strings.Contains(err.Error(), secret) || len(ctr.calls) != 2 {
		t.Fatalf("malformed native data exposed or final query continued: %v calls=%v", err, ctr.calls)
	}
}

func TestHealthDetailsCancellationAtEveryQuery(t *testing.T) {
	for call := 0; call <= 3; call++ {
		t.Run(fmt.Sprintf("query-%d", call), func(t *testing.T) {
			cluster, ctr := newHealthDetailsFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if call == 0 {
				cancel()
			}
			ctr.hook = func(_ context.Context, current int) error {
				if current == call {
					cancel() // Even valid native output cannot publish success.
				}
				return nil
			}
			err := assertHealthDetailsFailure(t, cluster, ctx)
			if !errors.Is(err, context.Canceled) || len(ctr.calls) != call {
				t.Fatalf("cancellation hidden or native work continued: %v calls=%v", err, ctr.calls)
			}
		})
	}
	cluster, ctr := newHealthDetailsFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctr.hook = func(_ context.Context, call int) error {
		if call == 2 {
			cancel()
			return fmt.Errorf("SECRET_CONTEXT_OUTPUT: %w", context.DeadlineExceeded)
		}
		return nil
	}
	err := assertHealthDetailsFailure(t, cluster, ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "SECRET_CONTEXT_OUTPUT") || len(ctr.calls) != 2 {
		t.Fatalf("canonical causes not retained or secret exposed: %v calls=%v", err, ctr.calls)
	}
}

type healthDetailsFinalContext struct {
	context.Context
	cancel   context.CancelFunc
	armed    bool
	checks   int
	cancelAt int
}

func (ctx *healthDetailsFinalContext) Err() error {
	if ctx.armed {
		ctx.checks++
		if ctx.checks == ctx.cancelAt {
			ctx.cancel()
		}
	}
	return ctx.Context.Err()
}

func TestHealthDetailsChecksContextAfterFinalIdentity(t *testing.T) {
	cluster, ctr := newHealthDetailsFixture()
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &healthDetailsFinalContext{Context: base, cancel: cancel, cancelAt: 3}
	ctr.hook = func(_ context.Context, call int) error {
		if call == 3 {
			ctx.armed = true
		}
		return nil
	}
	err := assertHealthDetailsFailure(t, cluster, ctx)
	if !errors.Is(err, context.Canceled) || len(ctr.calls) != 3 || ctx.checks != 3 {
		t.Fatalf("last admission guard missing: %v calls=%v checks=%d", err, ctr.calls, ctx.checks)
	}
}

func TestHealthDetailsRetainsCancellationDuringFSIDRejection(t *testing.T) {
	for _, call := range []int{1, 3} {
		t.Run(fmt.Sprintf("identity-query-%d", call), func(t *testing.T) {
			cluster, ctr := newHealthDetailsFixture()
			ctr.outputs[call-1] = "SECRET_INVALID_FSID"
			base, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &healthDetailsFinalContext{Context: base, cancel: cancel, cancelAt: 2}
			ctr.hook = func(_ context.Context, current int) error {
				if current == call {
					ctx.armed = true
				}
				return nil
			}
			err := assertHealthDetailsFailure(t, cluster, ctx)
			if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "SECRET_INVALID_FSID") || len(ctr.calls) != call || ctx.checks != 2 {
				t.Fatalf("identity rejection lost cancellation or exposed output: %v calls=%v checks=%d", err, ctr.calls, ctx.checks)
			}
		})
	}
}

func TestHealthDetailsBoundsOwnerControlAndConfigAdmission(t *testing.T) {
	for _, gate := range []string{"owner", "control", "config"} {
		t.Run(gate, func(t *testing.T) {
			cluster, ctr := newHealthDetailsFixture()
			var unlock func()
			switch gate {
			case "owner":
				cluster.mu.Lock()
				unlock = cluster.mu.Unlock
			case "control":
				cluster.controlMu.Lock()
				unlock = cluster.controlMu.Unlock
			case "config":
				cluster.configMu.Lock()
				unlock = cluster.configMu.Unlock
			}
			var once sync.Once
			release := func() { once.Do(unlock) }
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
			done, joined := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { cancel(); release(); <-joined })
			go func() {
				defer close(joined)
				result, err := cluster.HealthDetails(ctx)
				if !reflect.DeepEqual(result, HealthSnapshot{}) {
					err = errors.Join(err, errors.New("blocked observation published a snapshot"))
				}
				done <- err
			}()
			select {
			case err := <-done:
				<-joined
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("busy gate lost deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("HealthDetails remained queued after deadline")
			}
			if len(ctr.calls) != 0 {
				t.Fatal("unadmitted observation queried native state")
			}
		})
	}
}

func healthDetailsNativeShape() (map[string]any, map[string]map[string]any) {
	summary := map[string]any{"message": "summary value", "count": int64(9007199254740993)}
	detail := map[string]any{"message": "detail value"}
	check := map[string]any{"severity": "HEALTH_WARN", "summary": summary, "detail": []any{detail}, "muted": false}
	mute := map[string]any{"code": "ABSENT_CODE", "summary": "", "count": int64(0), "sticky": true}
	root := map[string]any{"status": "HEALTH_WARN", "checks": map[string]any{"FUTURE_CODE": check}, "mutes": []any{mute}}
	return root, map[string]map[string]any{"root": root, "check": check, "summary": summary, "detail": detail, "mute": mute}
}

func healthDetailsTestJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertHealthDetailsDecodeFailure(t *testing.T, data string) {
	t.Helper()
	result, err := decodeHealthDetails([]byte(data), healthDetailsFixtureFSID)
	if err == nil || !reflect.DeepEqual(result, HealthSnapshot{}) {
		t.Fatalf("malformed health data published: %+v error=%v", result, err)
	}
}

func TestHealthDetailsRequiresEveryNativeFieldWithoutAdoptingAliases(t *testing.T) {
	fields := map[string][]string{
		"root": {"status", "checks", "mutes"}, "check": {"severity", "summary", "detail", "muted"},
		"summary": {"message", "count"}, "detail": {"message"}, "mute": {"code", "summary", "count", "sticky"},
	}
	for section, names := range fields {
		for _, name := range names {
			for _, mode := range []string{"missing", "null", "wrong type", "alias only", "alias overwrite", "duplicate"} {
				t.Run(section+"/"+name+"/"+mode, func(t *testing.T) {
					root, sections := healthDetailsNativeShape()
					object := sections[section]
					original := object[name]
					if mode == "duplicate" {
						component := healthDetailsTestJSON(t, object)
						pair := `"` + name + `":` + healthDetailsTestJSON(t, original)
						duplicate := strings.Replace(component, pair, pair+","+pair, 1)
						data := strings.Replace(healthDetailsTestJSON(t, root), component, duplicate, 1)
						assertHealthDetailsDecodeFailure(t, data)
						return
					}
					switch mode {
					case "missing":
						delete(object, name)
					case "null":
						object[name] = nil
					case "wrong type":
						object[name] = "wrong type"
						if _, stringValue := original.(string); stringValue {
							object[name] = true
						}
					case "alias only":
						delete(object, name)
						object[strings.ToUpper(name)] = original
					case "alias overwrite":
						object[strings.ToUpper(name)] = original
					}
					assertHealthDetailsDecodeFailure(t, healthDetailsTestJSON(t, root))
				})
			}
		}
	}
	for _, section := range []string{"root", "check", "summary", "detail", "mute"} {
		t.Run(section+"/Unicode alias", func(t *testing.T) {
			root, sections := healthDetailsNativeShape()
			field := map[string]string{"root": "status", "check": "severity", "summary": "message", "detail": "message", "mute": "sticky"}[section]
			alias := strings.Replace(field, "s", "ſ", 1)
			sections[section][alias] = sections[section][field]
			assertHealthDetailsDecodeFailure(t, healthDetailsTestJSON(t, root))
		})
	}
}

func TestHealthDetailsRejectsAmbiguousCollectionsAndHealthRelations(t *testing.T) {
	for _, name := range []string{"duplicate check", "duplicate mute", "empty check code", "empty mute code", "muted without mute", "mute without muted flag", "healthy unmuted warning", "wrong worst severity", "unknown status", "unknown severity", "null check", "null detail", "null mute"} {
		t.Run(name, func(t *testing.T) {
			root, sections := healthDetailsNativeShape()
			checks := root["checks"].(map[string]any)
			switch name {
			case "duplicate check":
				component := healthDetailsTestJSON(t, checks)
				check := healthDetailsTestJSON(t, sections["check"])
				duplicate := `{"FUTURE_CODE":` + check + `,"FUTURE_CODE":` + check + `}`
				assertHealthDetailsDecodeFailure(t, strings.Replace(healthDetailsTestJSON(t, root), component, duplicate, 1))
				return
			case "duplicate mute":
				root["mutes"] = []any{sections["mute"], sections["mute"]}
			case "empty check code":
				delete(checks, "FUTURE_CODE")
				checks[""] = sections["check"]
			case "empty mute code":
				sections["mute"]["code"] = ""
			case "muted without mute":
				sections["check"]["muted"], root["status"] = true, "HEALTH_OK"
			case "mute without muted flag":
				sections["mute"]["code"] = "FUTURE_CODE"
			case "healthy unmuted warning":
				root["status"] = "HEALTH_OK"
			case "wrong worst severity":
				root["status"] = "HEALTH_ERR"
			case "unknown status":
				root["status"] = "HEALTH_FUTURE"
			case "unknown severity":
				sections["check"]["severity"] = "HEALTH_FUTURE"
			case "null check":
				checks["FUTURE_CODE"] = nil
			case "null detail":
				sections["check"]["detail"] = []any{nil}
			case "null mute":
				root["mutes"] = []any{nil}
			}
			assertHealthDetailsDecodeFailure(t, healthDetailsTestJSON(t, root))
		})
	}
}

func TestHealthDetailsPreservesMutedAndAbsentChecksWithSignedCounts(t *testing.T) {
	root, sections := healthDetailsNativeShape()
	sections["summary"]["count"] = int64(math.MinInt64)
	sections["mute"]["count"] = int64(math.MaxInt64)
	sections["mute"]["summary"] = "earlier summary"
	sections["mute"]["code"] = "FUTURE_CODE"
	sections["check"]["muted"], sections["check"]["severity"] = true, "HEALTH_ERR"
	root["status"] = "HEALTH_OK"
	result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
	if err != nil || result.Status != "HEALTH_OK" || !result.Checks["FUTURE_CODE"].Muted || result.Checks["FUTURE_CODE"].Count != math.MinInt64 || result.Mutes[0].Count != math.MaxInt64 || result.Mutes[0].Summary != "earlier summary" {
		t.Fatalf("native muted signed counters or old summary rejected: %+v error=%v", result, err)
	}
	// Exact code case is meaningful; unknown codes need no hardcoded allowlist.
	_, other := healthDetailsNativeShape()
	root["checks"].(map[string]any)["future_code"] = other["check"]
	root["status"] = "HEALTH_WARN"
	result, err = decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
	if err != nil || len(result.Checks) != 2 || result.Checks["future_code"].Muted || result.Checks["future_code"].Count != 9007199254740993 || !result.Checks["FUTURE_CODE"].Muted {
		t.Fatalf("case-sensitive codes were folded: %+v error=%v", result, err)
	}
	for _, sticky := range []bool{false, true} {
		root, sections := healthDetailsNativeShape()
		root["checks"], root["status"] = map[string]any{}, "HEALTH_OK"
		sections["mute"]["sticky"] = sticky
		result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
		if err != nil || len(result.Mutes) != 1 || result.Mutes[0].Sticky != sticky || result.Mutes[0].Count != 0 || result.Mutes[0].Summary != "" || result.Mutes[0].ExpiresAt != "" {
			t.Fatalf("absent check with retained mute rejected: %+v error=%v", result, err)
		}
	}
	// Native empty summary/detail strings and a zero count are still present.
	root, sections = healthDetailsNativeShape()
	sections["summary"]["message"], sections["summary"]["count"] = "", int64(0)
	sections["detail"]["message"] = ""
	result, err = decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
	if err != nil || result.Checks["FUTURE_CODE"].Summary != "" || !slices.Equal(result.Checks["FUTURE_CODE"].Details, []string{""}) {
		t.Fatalf("present empty native messages rejected: %+v error=%v", result, err)
	}
}

func TestHealthDetailsRejectsInvalidSignedCounters(t *testing.T) {
	for _, section := range []string{"summary", "mute"} {
		for _, value := range []string{"9223372036854775808", "-9223372036854775809", "1.5", "1e2", `"1"`, "true"} {
			t.Run(section+"/"+value, func(t *testing.T) {
				root, sections := healthDetailsNativeShape()
				object := sections[section]
				component := healthDetailsTestJSON(t, object)
				pair := `"count":` + healthDetailsTestJSON(t, object["count"])
				invalid := strings.Replace(component, pair, `"count":`+value, 1)
				assertHealthDetailsDecodeFailure(t, strings.Replace(healthDetailsTestJSON(t, root), component, invalid, 1))
			})
		}
	}
}

func TestHealthDetailsPreservesAllSignedCounterBoundaries(t *testing.T) {
	for _, count := range []int64{math.MinInt64, math.MaxInt64, 9007199254740993} {
		root, sections := healthDetailsNativeShape()
		sections["summary"]["count"], sections["mute"]["count"] = count, count
		result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
		if err != nil || result.Checks["FUTURE_CODE"].Count != count || result.Mutes[0].Count != count {
			t.Fatalf("signed counter boundary changed: count=%d snapshot=%+v error=%v", count, result, err)
		}
	}
}

func TestHealthDetailsReportsWorstUnmutedSeverity(t *testing.T) {
	root, sections := healthDetailsNativeShape()
	_, second := healthDetailsNativeShape()
	root["checks"].(map[string]any)["OTHER_CODE"] = second["check"]
	sections["check"]["severity"], root["status"] = "HEALTH_ERR", "HEALTH_ERR"
	result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
	if err != nil || result.Status != "HEALTH_ERR" || result.Checks["OTHER_CODE"].Severity != "HEALTH_WARN" {
		t.Fatalf("worst native error severity lost: %+v error=%v", result, err)
	}
}

func TestHealthDetailsPreservesNativeExpiryForms(t *testing.T) {
	for _, expiry := range []string{"2026-10-08T18:00:00.123456+0900", "2026-10-08T01:00:00.000001-0530", "2000-01-01T00:00:00.000000+0000", "1.000000", "0.000001", "315359999.999999"} {
		t.Run(expiry, func(t *testing.T) {
			root, sections := healthDetailsNativeShape()
			sections["mute"]["ttl"] = expiry
			result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
			if err != nil || result.Mutes[0].ExpiresAt != expiry {
				t.Fatalf("native expiry normalized or rejected: %+v error=%v", result, err)
			}
		})
	}
	for _, expiry := range []any{nil, "", 123, "2026-10-08T18:00:00.123456Z", "2026-10-08T18:00:00.123456+09:00", "2026-02-30T00:00:00.000000+0000", "2026-10-08T18:00:00.123+0900", "2026-10-08T18:00:00.123456+2400", "2026-10-08T18:00:00.123456+0060", "1.00000", "315360000.000000", "-1.000000", "01.000000", " 1.000000", "1.000000 "} {
		t.Run(fmt.Sprint(expiry), func(t *testing.T) {
			root, sections := healthDetailsNativeShape()
			sections["mute"]["ttl"] = expiry
			assertHealthDetailsDecodeFailure(t, healthDetailsTestJSON(t, root))
		})
	}
	for _, mode := range []string{"alias only", "alias overwrite", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			root, sections := healthDetailsNativeShape()
			mute := sections["mute"]
			mute["ttl"] = "1.000000"
			if mode == "duplicate" {
				component := healthDetailsTestJSON(t, mute)
				invalid := strings.Replace(component, `"ttl":"1.000000"`, `"ttl":"1.000000","ttl":"1.000000"`, 1)
				assertHealthDetailsDecodeFailure(t, strings.Replace(healthDetailsTestJSON(t, root), component, invalid, 1))
				return
			}
			mute["TTL"] = mute["ttl"]
			if mode == "alias only" {
				delete(mute, "ttl")
			}
			assertHealthDetailsDecodeFailure(t, healthDetailsTestJSON(t, root))
		})
	}
}

func TestHealthDetailsAllowsUnrelatedUnknownFields(t *testing.T) {
	root, sections := healthDetailsNativeShape()
	for _, section := range sections {
		section["future"] = map[string]any{"Count": json.Number("1e9999"), "count": nil, "MESSAGE": "future data"}
		section["Future"] = nil
	}
	result, err := decodeHealthDetails([]byte(healthDetailsTestJSON(t, root)), healthDetailsFixtureFSID)
	if err != nil || result.Checks["FUTURE_CODE"].Count != 9007199254740993 || result.Mutes[0].Summary != "" {
		t.Fatalf("unrelated unknown data changed observation: %+v error=%v", result, err)
	}
}

func TestHealthDetailsRejectsMalformedTrailingAndExcessiveJSON(t *testing.T) {
	for _, data := range []string{"", "null", "[]", "true", "{}", healthDetailsFixtureHealthy + "{}", healthDetailsFixtureHealthy + " null", healthDetailsFixtureWarning[:len(healthDetailsFixtureWarning)-1], strings.Repeat(" ", 4<<20) + healthDetailsFixtureHealthy,
		strings.Replace(healthDetailsFixtureHealthy, `"checks":{}`, `"future":`+strings.Repeat("[", 260)+"0"+strings.Repeat("]", 260)+`,"checks":{}`, 1)} {
		assertHealthDetailsDecodeFailure(t, data)
	}
}
