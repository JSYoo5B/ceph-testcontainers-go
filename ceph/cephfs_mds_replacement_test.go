package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This wraps the existing Q native fixture, adding only control-role fsid and
// names-only auth replies. Real starter publication and owner gates are used.
type replacementMDSControl struct {
	*stoppedMDSControl
	auth                    []any
	authRaw, authFail, fsid string
	mutations               int
	before                  func(string)
}

func (n *replacementMDSControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(monitorQuorumTestModuleArgs(args), " ")
	if n.before != nil {
		n.before(call)
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if strings.HasPrefix(call, "auth get-or-create ") {
		n.mutations++
	}
	if call != "fsid" && call != "auth ls --format json" {
		return n.stoppedMDSControl.Exec(ctx, args, opts...)
	}
	n.calls = append(n.calls, call)
	output := n.fsid + "\n"
	code := 0
	if call == "auth ls --format json" {
		output = coldMDSTestJSON(map[string]any{"auth_dump": n.auth})
		if n.authRaw != "" {
			output = n.authRaw
		}
		if n.authFail != "" {
			code, output = 1, n.authFail
		}
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return code, bytes.NewReader(append(header[:], []byte(output)...)), nil
}

type replacementMDSFixture struct {
	fs                     *CephFSContainer
	n                      *replacementMDSControl
	old                    *MDSContainer
	oldCID, newCID         *stoppedMDSDaemon
	starts                 int
	startErr               error
	nilResult, noPromotion bool
	afterStart             func(context.Context)
}

func newReplacementMDSFixture(t *testing.T) *replacementMDSFixture {
	t.Helper()
	previous, n, _, _, _ := stoppedMDSFixture(t)
	c := previous.cluster
	r := &replacementMDSFixture{n: &replacementMDSControl{stoppedMDSControl: n, fsid: coldMDSFixtureFSID, auth: []any{map[string]any{"entity": "mds.target-0"}}}}
	c.Container = r.n
	c.services = map[string]testcontainers.Container{}
	c.settings.startupTimeout = 2 * time.Second
	c.settings.mdsImage = "selected-existing-mds-image"
	config, err := normalizeCephFSConfig(CephFSConfig{Name: "target"})
	if err != nil {
		t.Fatal(err)
	}
	r.fs = &CephFSContainer{cluster: c, config: config, FilesystemName: "target", MetadataPool: config.MetadataPool.Name, DataPool: config.DataPool.Name}
	c.filesystems = map[string]*CephFSContainer{"target": r.fs}
	if err := r.fs.captureNativePoolIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	r.oldCID = &stoppedMDSDaemon{cid: strings.Repeat("a", 64), state: container.State{Status: "exited"}}
	r.newCID = &stoppedMDSDaemon{cid: strings.Repeat("c", 64), state: container.State{Status: "running", Running: true, Pid: 13}}
	stoppedMDSPublish(t, r.fs, r.oldCID, nil)
	r.old = r.fs.mdss[0]
	m := n.target()
	m["standby_count_wanted"], m["info"], m["up"], m["in"], m["failed"] = 0, map[string]any{}, map[string]any{}, []int{0}, []int{0}
	n.calls = nil
	r.n.mutations = 0
	return r
}

func (r *replacementMDSFixture) promote(state string) {
	m := r.n.target()
	m["info"] = map[string]any{"gid_33": coldMDSTestRow("target-1", state, 33, 0, 7)}
	m["up"], m["failed"] = map[string]any{"mds_0": uint64(33)}, []int{}
}

func (r *replacementMDSFixture) start(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	r.starts++
	if name != "mds.target-1" || image != "selected-existing-mds-image" {
		return nil, errors.New("wrong reserved name or selected image")
	}
	// The actual startService publishes under this same owner mutex. Reaching
	// it proves the new admission hook released the lock before service start.
	if err := r.fs.cluster.lockTopology(ctx); err != nil {
		return nil, err
	}
	if !r.nilResult {
		r.fs.cluster.services[name] = r.newCID
	}
	r.fs.cluster.mu.Unlock()
	if !r.noPromotion {
		r.promote("up:active")
	}
	if r.afterStart != nil {
		r.afterStart(ctx)
	}
	if r.nilResult {
		return nil, r.startErr
	}
	return r.newCID, r.startErr
}

func (r *replacementMDSFixture) add(ctx context.Context) (*MDSContainer, error) {
	return r.fs.addMDSReplacement(ctx, r.old, r.start)
}
func (r *replacementMDSFixture) assertNoAttempt(t *testing.T) {
	t.Helper()
	if r.starts != 0 || r.n.mutations != 0 || r.old.identity.replacement != nil || r.fs.nextMDSIndex != 1 || len(r.fs.mdss) != 1 {
		t.Fatal("rejected replacement attempted auth/start/reservation or changed owned cohort")
	}
}

func TestMDSReplacementActualPublicationAndCompletedCopyRetry(t *testing.T) {
	r := newReplacementMDSFixture(t)
	beforeConfig, beforeNative := r.fs.config, r.fs.nativeIdentity
	newDaemon, err := r.add(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if newDaemon == nil || newDaemon != r.fs.mdss[1] || newDaemon.Container != r.newCID || !newDaemon.identity.confirmed || !r.old.identity.replacement.completed || r.old.identity.replacement.daemon != newDaemon || r.fs.Container != r.oldCID || r.fs.nextMDSIndex != 2 || r.n.mutations != 1 || r.starts != 1 || r.oldCID.removes != 0 || !reflect.DeepEqual(r.fs.config, beforeConfig) || r.fs.nativeIdentity != beforeNative {
		t.Fatal("replacement lost canonical publication, old embed, monotonic name or desired policy")
	}
	copy := *r.old
	again, err := r.fs.addMDSReplacement(t.Context(), &copy, r.start)
	if err != nil || again != newDaemon || r.starts != 1 || r.n.mutations != 1 {
		t.Fatal("completed copied-handle retry launched or adopted a new daemon", err)
	}
	if err := r.fs.RemoveStoppedMDS(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	again, err = r.fs.addMDSReplacement(t.Context(), &copy, r.start)
	if err != nil || again != newDaemon || r.fs.Container != r.newCID || len(r.fs.mdss) != 1 || r.oldCID.removes != 1 || r.starts != 1 || r.n.mutations != 1 {
		t.Fatal("completed own Q removal/404 lost exact replacement or created another", err)
	}
	for _, call := range r.n.calls {
		if strings.HasPrefix(call, "mds fail") || strings.HasPrefix(call, "auth del") || strings.HasPrefix(call, "fs set") {
			t.Fatal("replacement changed native failure/auth/policy", call)
		}
	}
}

func TestMDSReplacementOwnerAndPublicAdmissionRejectsWithoutMutation(t *testing.T) {
	if _, err := (*CephFSContainer)(nil).AddMDSReplacement(t.Context(), nil); err == nil {
		t.Fatal("nil filesystem accepted")
	}
	for _, fault := range []string{"nil", "copy-export", "foreign-embed", "edited-original", "active-two", "standby-one", "replay", "partial-original", "closed", "registry", "duplicate", "fs-name", "metadata", "default-pool", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			target := r.old
			ctx := t.Context()
			switch fault {
			case "nil":
				target = nil
			case "copy-export":
				copy := *target
				copy.Container = r.newCID
				target = &copy
			case "foreign-embed":
				r.fs.Container = r.newCID
			case "edited-original":
				r.old.ID = "changed"
			case "active-two":
				r.fs.config.ActiveMDS = 2
			case "standby-one":
				r.fs.config.StandbyMDS = 1
			case "replay":
				r.fs.config.StandbyReplay = true
			case "partial-original":
				r.old.identity.confirmed = false
			case "closed":
				r.fs.cluster.closed = true
			case "registry":
				delete(r.fs.cluster.services, "mds.target-0")
			case "duplicate":
				r.fs.mdss = append(r.fs.mdss, r.old)
			case "fs-name":
				r.fs.FilesystemName = "other"
			case "metadata":
				r.fs.MetadataPool = "other"
			case "default-pool":
				r.fs.DataPool = "other"
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if _, err := r.fs.addMDSReplacement(ctx, target, r.start); err == nil {
				t.Fatal("invalid owner admitted")
			}
			if r.starts != 0 || r.n.mutations != 0 || r.old.identity.replacement != nil || r.fs.nextMDSIndex != 1 || len(r.n.calls) != 0 {
				t.Fatal("owner rejection reached native allocation")
			}
		})
	}
}

func TestMDSReplacementOriginalStoppedCIDRequired(t *testing.T) {
	for _, fault := range []string{"missing", "running", "restarting", "paused", "dead", "pid", "created", "wrong-cid", "mutable-cid", "nil-info", "query-error"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			switch fault {
			case "missing":
				r.oldCID.missing = true
			case "running":
				r.oldCID.state.Running = true
			case "restarting":
				r.oldCID.state.Restarting = true
			case "paused":
				r.oldCID.state.Paused = true
			case "dead":
				r.oldCID.state.Dead = true
			case "pid":
				r.oldCID.state.Pid = 1
			case "created":
				r.oldCID.state.Status = "created"
			case "wrong-cid":
				r.oldCID.returnedCID = strings.Repeat("d", 64)
			case "mutable-cid":
				r.oldCID.cid = strings.Repeat("d", 64)
			case "nil-info":
				r.oldCID.nilInfo = true
			case "query-error":
				r.oldCID.inspectErr = errors.New("never-log-original-inspect-secret")
			}
			_, err := r.add(t.Context())
			if err == nil || strings.Contains(err.Error(), "never-log") {
				t.Fatal("bad original inspect accepted or leaked cause", err)
			}
			r.assertNoAttempt(t)
		})
	}
}

func TestMDSReplacementRequiresExactFailedRankAndGlobalAbsence(t *testing.T) {
	for _, fault := range []string{"still-registered", "global-old", "foreign-unqualified", "foreign-target", "foreign-unknown", "missing-failed", "different-failed", "in-empty", "up-without-info", "damaged", "stopped", "not-joinable", "replay", "unprotected", "wanted", "max", "fsid", "metadata", "default", "attached-duplicate", "reserved-sibling", "malformed-sibling", "duplicate-fs", "raw-null", "raw-trailing", "raw-duplicate", "native-query"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			m := r.n.target()
			switch fault {
			case "still-registered":
				m["info"] = map[string]any{"gid_5": coldMDSTestRow("target-0", "up:active", 5, 0, 7)}
				m["up"] = map[string]any{"mds_0": uint64(5)}
			case "global-old":
				r.n.native["standbys"] = []any{coldMDSTestRow("target-0", "up:standby", 5, -1, 7)}
			case "foreign-unqualified":
				r.n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 5, -1, -1)}
			case "foreign-target":
				r.n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 5, -1, 7)}
			case "foreign-unknown":
				r.n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 5, -1, 8)}
			case "missing-failed":
				delete(m, "failed")
			case "different-failed":
				m["failed"] = []int{1}
			case "in-empty":
				m["in"] = []int{}
			case "up-without-info":
				m["up"] = map[string]any{"mds_0": uint64(33)}
			case "damaged":
				m["damaged"] = []int{0}
			case "stopped":
				m["stopped"] = []int{0}
			case "not-joinable":
				m["flags_state"].(map[string]any)["joinable"] = false
			case "replay":
				m["flags_state"].(map[string]any)["allow_standby_replay"] = true
			case "unprotected":
				m["flags_state"].(map[string]any)["refuse_standby_for_another_fs"] = false
			case "wanted":
				m["standby_count_wanted"] = 1
			case "max":
				m["max_mds"] = 2
			case "fsid":
				r.n.quorum["monmap"].(map[string]any)["fsid"] = "0b4a987e-f450-4f00-aa7f-0f5230ae4bca"
			case "metadata":
				m["metadata_pool"] = 2
			case "default":
				m["data_pools"] = []int64{2}
			case "attached-duplicate":
				m["data_pools"] = []int64{1, 1}
			case "reserved-sibling", "malformed-sibling":
				sibling := coldMDSTestMap("sibling", 0, 1)
				sibling["up"] = map[string]any{"mds_0": uint64(44)}
				if fault == "reserved-sibling" {
					sibling["info"] = map[string]any{"gid_44": coldMDSTestRow("target-1", "up:active", 44, 0, 8)}
				}
				r.n.native["filesystems"] = append(r.n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
			case "duplicate-fs":
				r.n.native["filesystems"] = append(r.n.native["filesystems"].([]any), r.n.native["filesystems"].([]any)[0])
			case "raw-null":
				r.n.raw = `{"filesystems":null,"standbys":[]}`
			case "raw-trailing":
				r.n.raw = coldMDSTestJSON(r.n.native) + ` {}`
			case "raw-duplicate":
				r.n.raw = `{"standbys":[],"standbys":[],"filesystems":[]}`
			case "native-query":
				r.n.fail = "fs dump --format json"
			}
			_, err := r.add(t.Context())
			if err == nil || strings.Contains(err.Error(), "never-log") {
				t.Fatal("invalid failed/global map reached replacement", err)
			}
			r.assertNoAttempt(t)
			if slices.Contains([]string{"still-registered", "global-old"}, fault) && err.Error() != "original stopped MDS name is still registered" {
				t.Fatal("completed name-presence classification changed", err)
			}
		})
	}
}

func TestMDSReplacementPreservesSiblingAndCurrentAttachments(t *testing.T) {
	r := newReplacementMDSFixture(t)
	r.n.target()["data_pools"] = []int64{1, 2}
	sibling := coldMDSTestMap("sibling", 0, 1)
	sibling["info"] = map[string]any{"gid_44": coldMDSTestRow("sibling-active", "up:stopping", 44, 0, 8)}
	sibling["up"] = map[string]any{"mds_0": uint64(44)}
	r.n.native["filesystems"] = append(r.n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
	r.n.native["standbys"] = []any{coldMDSTestRow("sibling-standby", "up:standby", 45, -1, 8)}
	before := coldMDSTestJSON(sibling)
	got, err := r.add(t.Context())
	if err != nil || got == nil || before != coldMDSTestJSON(sibling) || !slices.Equal(r.old.identity.replacement.scope.dataPools, []int64{1, 2}) {
		t.Fatal("independent sibling or current attachment was destroyed or adopted", err)
	}
}

func TestMDSReplacementNamesOnlyAuthGuard(t *testing.T) {
	for _, fault := range []string{"collision", "missing", "null", "empty", "whitespace", "duplicate", "raw", "query"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			switch fault {
			case "collision":
				r.n.auth = append(r.n.auth, map[string]any{"entity": "mds.target-1"})
			case "missing":
				r.n.auth = []any{map[string]any{}}
			case "null":
				r.n.auth = []any{map[string]any{"entity": nil}}
			case "empty":
				r.n.auth = []any{map[string]any{"entity": ""}}
			case "whitespace":
				r.n.auth = []any{map[string]any{"entity": " mds.other"}}
			case "duplicate":
				r.n.auth = append(r.n.auth, r.n.auth[0])
			case "raw":
				r.n.authRaw = `{"auth_dump":null}`
			case "query":
				r.n.authFail = "never-log-auth-secret"
			}
			_, err := r.add(t.Context())
			if err == nil || strings.Contains(err.Error(), "never-log") {
				t.Fatal("bad auth names accepted or secret leaked", err)
			}
			r.assertNoAttempt(t)
		})
	}
}

func TestMDSReplacementReadOnlyRejectionRetainsFirstAttempt(t *testing.T) {
	for _, fault := range []string{"late-auth", "late-pool", "selected-fsid", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			r.n.before = func(call string) {
				if call == "auth ls --format json" {
					reads++
					if reads == 2 {
						switch fault {
						case "late-auth":
							r.n.auth = append(r.n.auth, map[string]any{"entity": "mds.target-1"})
						case "late-pool":
							r.n.target()["data_pools"] = []int64{1, 2}
						case "selected-fsid":
							r.n.fsid = "0b4a987e-f450-4f00-aa7f-0f5230ae4bca"
						case "cancel":
							cancel()
						}
					}
				}
			}
			// The late-pool drift happens after this admission's first map read;
			// force it at its second fsid/quorum boundary instead.
			if fault == "late-pool" {
				calls := 0
				r.n.before = func(call string) {
					if call == "quorum_status --format json" {
						calls++
						if calls == 2 {
							r.n.target()["data_pools"] = []int64{1, 2}
						}
					}
				}
			}
			_, err := r.add(ctx)
			if err == nil {
				t.Fatal("late preflight drift accepted")
			}
			r.assertNoAttempt(t)
		})
	}
}

func TestMDSReplacementAttemptedAuthFailureIsCleanupOnly(t *testing.T) {
	r := newReplacementMDSFixture(t)
	r.n.fail = "auth get-or-create mds.target-1 mon allow profile mds mgr allow profile mds osd allow rw tag cephfs *=* mds allow"
	got, err := r.add(t.Context())
	if got != nil || err == nil || strings.Contains(err.Error(), "never-log") || r.old.identity.replacement == nil || r.fs.nextMDSIndex != 2 || r.n.mutations != 1 || r.starts != 0 {
		t.Fatal("auth attempt failure lost receipt or leaked error", err)
	}
	r.n.fail = ""
	got, err = r.add(t.Context())
	if got != nil || err == nil || err.Error() != "attempted MDS replacement is cleanup-only" || r.n.mutations != 1 || r.starts != 0 || r.fs.nextMDSIndex != 2 {
		t.Fatal("auth uncertainty implicitly retried", err)
	}
}

func TestMDSReplacementActualPartialStartupRetainedOnRetryAndCleanup(t *testing.T) {
	for _, fault := range []string{"starter-error", "canceled-publication", "nil-result"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "starter-error":
				r.startErr = errors.New("never-log-service-secret")
			case "canceled-publication":
				r.afterStart = func(context.Context) { cancel() }
			case "nil-result":
				r.nilResult = true
			}
			got, err := r.add(ctx)
			if err == nil || strings.Contains(err.Error(), "never-log") || r.starts != 1 || r.n.mutations != 1 || r.fs.nextMDSIndex != 2 || r.old.identity.replacement.completed {
				t.Fatal("partial attempt was completed or lost", err)
			}
			if fault != "nil-result" {
				if got == nil || got != r.fs.mdss[1] || got != r.old.identity.replacement.daemon || got.identity.confirmed || got.identity.fsid != "" || r.fs.cluster.services["mds.target-1"] != r.newCID {
					t.Fatal("actual partial publication lost canonical unconfirmed handle")
				}
			} else if got != nil || len(r.fs.mdss) != 1 {
				t.Fatal("nil service acquired a descriptor")
			}
			copy := *r.old
			again, retryErr := r.fs.addMDSReplacement(t.Context(), &copy, r.start)
			if again != got || retryErr == nil || retryErr.Error() != "attempted MDS replacement is cleanup-only" || r.starts != 1 || r.n.mutations != 1 {
				t.Fatal("failed receipt retry lost useful partial or resent native start", retryErr)
			}
			if err := r.fs.cluster.Terminate(t.Context()); err != nil {
				t.Fatal(err)
			}
			if r.oldCID.removes != 1 || fault != "nil-result" && r.newCID.removes != 1 {
				t.Fatal("cluster cleanup lost attempted owned containers")
			}
		})
	}
}

func TestMDSReplacementServiceSuccessIsNotOperationCompletion(t *testing.T) {
	for _, fault := range []string{"native-name", "native-mode", "laggy", "new-cid", "zero-pid", "new-state-error", "attached-drift", "native-query", "context-after-publication"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r.afterStart = func(context.Context) {
				switch fault {
				case "native-name":
					r.n.target()["info"].(map[string]any)["gid_33"].(map[string]any)["name"] = "foreign"
				case "native-mode":
					r.n.target()["flags_state"].(map[string]any)["allow_standby_replay"] = true
				case "laggy":
					r.n.target()["info"].(map[string]any)["gid_33"].(map[string]any)["laggy_since"] = nil
				case "new-cid":
					r.newCID.returnedCID = strings.Repeat("d", 64)
				case "zero-pid":
					r.newCID.state.Pid = 0
				case "new-state-error":
					r.newCID.state.Error = "never-log-daemon-error"
				case "attached-drift":
					r.n.target()["data_pools"] = []int64{1, 2}
				case "native-query":
					r.n.fail = "fs dump --format json"
				case "context-after-publication":
					r.newCID.inspectHook = func(int) { cancel() }
				}
			}
			got, err := r.add(ctx)
			if got == nil || err == nil || !got.identity.confirmed || r.old.identity.replacement.completed || strings.Contains(err.Error(), "never-log") || got != r.fs.mdss[1] || r.fs.cluster.services["mds.target-1"] != r.newCID {
				t.Fatal("service success/final observation error lost useful canonical creation or completed request", err)
			}
			again, retryErr := r.add(t.Context())
			if again != got || retryErr == nil || retryErr.Error() != "attempted MDS replacement is cleanup-only" || r.starts != 1 || r.n.mutations != 1 {
				t.Fatal("logical failure was implicitly resumed", retryErr)
			}
		})
	}
}

func TestMDSReplacementRecognizedRecoveryProgressAndPollOwnerRelease(t *testing.T) {
	r := newReplacementMDSFixture(t)
	r.afterStart = func(context.Context) { r.promote("up:replay") }
	observations := 0
	r.n.hook = func(call string) {
		if call == "fs dump --format json" && r.starts != 0 {
			observations++
			if observations == 3 {
				r.promote("up:active")
			}
		}
	}
	got, err := r.add(t.Context())
	if err != nil || got == nil || !r.old.identity.replacement.completed || observations < 3 {
		t.Fatal("recognized replay did not reach exact active completion", err)
	}
	// A pending recovery releases the actual owner gate between polls. A new
	// admission can read its own registry without waiting for full readiness.
	r = newReplacementMDSFixture(t)
	r.noPromotion = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := make(chan struct{}, 1)
	result := make(chan error, 1)
	joined := make(chan struct{})
	r.n.hook = func(call string) {
		if call == "fs dump --format json" && r.starts != 0 {
			select {
			case observed <- struct{}{}:
			default:
			}
		}
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("replacement poll did not exit")
		}
	})
	go func() { defer close(joined); _, err := r.add(ctx); result <- err }()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("pending recovery never observed")
	}
	admitCtx, admitCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer admitCancel()
	if err := r.fs.cluster.lockTopology(admitCtx); err != nil {
		t.Fatal("poll retained owner lock", err)
	}
	ownedCount := len(r.fs.mdss)
	r.fs.cluster.mu.Unlock()
	if ownedCount != 2 {
		t.Fatal("pending owned partial disappeared")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatal("pending wait lost canonical cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending wait ignored cancellation")
	}
}

func TestMDSReplacementCompletedRetryReattestsOriginalIdentities(t *testing.T) {
	for _, fault := range []string{"new-handle", "new-cid", "new-export", "new-registry", "extra-owned", "pool", "native-name", "old-running", "old-arbitrary-missing", "pending-Q-receipt", "removed-without-Q", "same-CID-new-GID"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			got, err := r.add(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "new-handle":
				got.Container = r.oldCID
			case "new-cid":
				r.newCID.returnedCID = strings.Repeat("d", 64)
			case "new-export":
				got.ID = "foreign"
			case "new-registry":
				delete(r.fs.cluster.services, "mds.target-1")
			case "extra-owned":
				r.fs.mdss = append(r.fs.mdss, got)
			case "pool":
				r.n.target()["data_pools"] = []int64{1, 2}
			case "native-name":
				r.n.target()["info"].(map[string]any)["gid_33"].(map[string]any)["name"] = "foreign"
			case "old-running":
				r.oldCID.state.Running = true
			case "old-arbitrary-missing":
				r.oldCID.missing = true
			case "pending-Q-receipt":
				r.old.identity.removalAttempted = true
			case "removed-without-Q":
				r.old.identity.removed = true
				r.oldCID.missing = true
			case "same-CID-new-GID":
				r.n.target()["info"] = map[string]any{"gid_34": coldMDSTestRow("target-1", "up:active", 34, 0, 7)}
				r.n.target()["up"] = map[string]any{"mds_0": uint64(34)}
			}
			again, err := r.add(t.Context())
			if fault == "same-CID-new-GID" {
				if err != nil || again != got {
					t.Fatal("logical GID change invented process binding", err)
				}
			} else if err == nil {
				t.Fatal("completed retry adopted changed original authority")
			}
			if r.starts != 1 || r.n.mutations != 1 {
				t.Fatal("completed retry allocated/authenticated another daemon")
			}
		})
	}
}

func TestMDSReplacementRealAdmissionGatesRespectDeadline(t *testing.T) {
	for _, gate := range []string{"setup", "owner", "control", "config"} {
		t.Run(gate, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			c := r.fs.cluster
			var lock, unlock func()
			switch gate {
			case "setup":
				lock, unlock = c.cephfsSetupMu.Lock, c.cephfsSetupMu.Unlock
			case "owner":
				lock, unlock = c.mu.Lock, c.mu.Unlock
			case "control":
				lock, unlock = c.controlMu.Lock, c.controlMu.Unlock
			case "config":
				lock, unlock = c.configMu.Lock, c.configMu.Unlock
			}
			coldMDSTestGate(t, lock, unlock, func(ctx context.Context) error { _, err := r.add(ctx); return err })
			r.assertNoAttempt(t)
		})
	}
}

func TestMDSReplacementLateOriginalOwnerDriftStaysCleanupOnly(t *testing.T) {
	r := newReplacementMDSFixture(t)
	r.startErr = errors.New("starter error")
	got, err := r.add(t.Context())
	if got == nil || err == nil {
		t.Fatal("fixture did not retain real partial")
	}
	copy := *r.old
	copy.Container = r.newCID
	again, err := r.fs.addMDSReplacement(t.Context(), &copy, r.start)
	if again != got || err == nil || r.starts != 1 || r.n.mutations != 1 {
		t.Fatal("partial receipt owner rejection lost useful cleanup result or launched again", err)
	}
}

func TestMDSReplacementIncompleteCreationCannotGainTypedAuthority(t *testing.T) {
	for _, failure := range []string{"logical-ready", "final-context"} {
		t.Run(failure, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r.afterStart = func(context.Context) {
				if failure == "logical-ready" {
					r.n.target()["info"].(map[string]any)["gid_33"].(map[string]any)["laggy_since"] = nil
				} else {
					r.newCID.inspectHook = func(int) { cancel() }
				}
			}
			partial, err := r.add(ctx)
			if err == nil || partial == nil || !partial.identity.confirmed || partial.identity.createdByReplacement != r.old.identity.replacement || r.old.identity.replacement.completed {
				t.Fatal("fixture lacks confirmed-but-incomplete publication", err)
			}
			r.newCID.inspectHook = nil
			r.promote("up:active")
			again, err := r.add(t.Context())
			if again != partial || err == nil || err.Error() != "attempted MDS replacement is cleanup-only" {
				t.Fatal("late active registration resumed failed Add", err)
			}
			before := len(r.n.calls)
			if err := r.fs.ScaleMDS(t.Context(), 1, 0); err == nil || err.Error() != "attempted MDS replacement is cleanup-only" || len(r.n.calls) != before {
				t.Fatal("ordinary Scale adopted incomplete replacement authority", err)
			}
			if err := r.fs.RemoveStoppedMDS(t.Context(), r.old); err == nil || r.oldCID.removes != 0 || r.old.identity.removalAttempted {
				t.Fatal("incomplete replacement became Q survivor", err)
			}
			// A failed new member cannot itself be a Q retirement target, even
			// with unrelated healthy original members later available.
			r.newCID.state = container.State{Status: "exited"}
			before = len(r.n.calls)
			if err := r.fs.RemoveStoppedMDS(t.Context(), partial); err == nil || err.Error() != "attempted MDS replacement is cleanup-only" || len(r.n.calls) != before || r.newCID.removes != 0 {
				t.Fatal("incomplete replacement gained typed retirement authority", err)
			}
			if r.starts != 1 || r.n.mutations != 1 {
				t.Fatal("failed request resent auth/start")
			}
			if err := r.fs.cluster.Terminate(t.Context()); err != nil || r.oldCID.removes != 1 || r.newCID.removes != 1 {
				t.Fatal("typed rejection lost cleanup-owned exact containers", err)
			}
		})
	}
}

func TestMDSReplacementAttemptWithoutNewHandleRemainsCleanupOnlyAfterRestart(t *testing.T) {
	for _, failure := range []string{"auth-failure", "nil-service"} {
		t.Run(failure, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			if failure == "auth-failure" {
				r.n.fail = "auth get-or-create mds.target-1 mon allow profile mds mgr allow profile mds osd allow rw tag cephfs *=* mds allow"
			} else {
				r.nilResult = true
			}
			partial, err := r.add(t.Context())
			if partial != nil || err == nil || r.old.identity.replacement == nil || r.old.identity.replacement.daemon != nil || len(r.fs.mdss) != 1 {
				t.Fatal("fixture lacks attempted receipt without a new handle", err)
			}
			r.n.fail = ""
			// External recovery of the old worker never completes the failed
			// replacement request or grants its typed fixture new authority.
			r.oldCID.state = container.State{Status: "running", Running: true, Pid: 21}
			m := r.n.target()
			m["info"] = map[string]any{"gid_31": coldMDSTestRow("target-0", "up:active", 31, 0, 7)}
			m["up"], m["failed"] = map[string]any{"mds_0": uint64(31)}, []int{}
			before := len(r.n.calls)
			if err := r.fs.ScaleMDS(t.Context(), 1, 0); err == nil || err.Error() != "attempted MDS replacement is cleanup-only" || len(r.n.calls) != before {
				t.Fatal("restarted original escaped attempted fixture guard", err)
			}
			if err := r.fs.RemoveStoppedMDS(t.Context(), r.old); err == nil || err.Error() != "attempted MDS replacement is cleanup-only" || len(r.n.calls) != before || r.oldCID.removes != 0 {
				t.Fatal("original receipt gained typed retirement authority", err)
			}
			if again, err := r.add(t.Context()); again != nil || err == nil || err.Error() != "attempted MDS replacement is cleanup-only" {
				t.Fatal("restarted original implicitly completed or retried replacement", err)
			}
			wantedStarts := 0
			if failure == "nil-service" {
				wantedStarts = 1
			}
			if r.starts != wantedStarts || r.n.mutations != 1 || r.fs.nextMDSIndex != 2 {
				t.Fatal("restarted failed request allocated/authenticated another member")
			}
			if err := r.fs.cluster.Terminate(t.Context()); err != nil || r.oldCID.removes != 1 || r.newCID.removes != 0 {
				t.Fatal("cleanup lost exact original or adopted nonexistent new handle", err)
			}
		})
	}
}

func TestMDSReplacementLegacyStarterReceiptUnchanged(t *testing.T) {
	for _, fault := range []string{"nil-success", "partial-error"} {
		t.Run(fault, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			var cause error
			var ctr testcontainers.Container
			if fault == "partial-error" {
				cause = fmt.Errorf("legacy starter error")
				ctr = r.newCID
			}
			err := r.fs.startMDSWithService(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				if ctr != nil {
					r.fs.cluster.services["mds.target-1"] = ctr
				}
				return ctr, cause
			})
			if !errors.Is(err, cause) || r.old.identity.replacement != nil || r.fs.nextMDSIndex != 2 {
				t.Fatal("nil-hook legacy return or reservation changed", err)
			}
			if fault == "partial-error" && (len(r.fs.mdss) != 2 || r.fs.mdss[1].identity.confirmed || r.fs.mdss[1].Container != ctr) {
				t.Fatal("legacy partial no longer owned")
			}
		})
	}
}
