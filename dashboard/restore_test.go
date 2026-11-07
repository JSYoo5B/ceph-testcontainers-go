package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// restoreCLI stores the account display name and config-key values that
// survive an attempted Restore. Secret-bearing writes use inputCommand.
type restoreCLI struct {
	testcontainers.Container
	accountName     string
	keys            map[string]string
	loseRemoveReply bool
}

func (f *restoreCLI) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	command := clustertest.ModuleArgs(args)
	switch {
	case slices.Equal(command, []string{"config", "dump", "--format", "json"}):
		entries := make([]map[string]string, 0, len(f.keys))
		for name, value := range f.keys {
			entries = append(entries, map[string]string{"section": "mgr", "name": name, "value": value})
		}
		data, _ := json.Marshal(entries)
		return clustertest.Reply(args, 0, data)
	case slices.Equal(command, []string{"dashboard", "reset-rgw-api-access-key"}):
		delete(f.keys, "mgr/dashboard/RGW_API_ACCESS_KEY")
		return clustertest.Reply(args, 0, nil)
	case slices.Equal(command, []string{"dashboard", "reset-rgw-api-secret-key"}):
		delete(f.keys, "mgr/dashboard/RGW_API_SECRET_KEY")
		return clustertest.Reply(args, 0, nil)
	case slices.Equal(command, []string{"dashboard", "ac-user-show", "tester"}):
		if f.accountName != "" {
			data, _ := json.Marshal(map[string]string{"username": "tester", "name": f.accountName})
			return clustertest.Reply(args, 0, data)
		}
	case slices.Equal(command, []string{"dashboard", "ac-user-delete", "tester"}):
		f.accountName = ""
		return clustertest.Reply(args, 0, nil)
	case len(command) == 3 && command[0] == "config-key" && command[1] == "get":
		if value, ok := f.keys[command[2]]; ok {
			return clustertest.Reply(args, 0, []byte(value))
		}
	case len(command) == 3 && command[0] == "config-key" && command[1] == "rm":
		delete(f.keys, command[2])
		if f.loseRemoveReply {
			f.loseRemoveReply = false
			return 0, nil, errors.New("lost remove reply")
		}
		return clustertest.Reply(args, 0, nil)
	default:
		return 0, nil, fmt.Errorf("unexpected command %v", command)
	}
	// NativeFailure reads Ceph's symbolic errno on stderr.
	missing := clustertest.CannedCommandContainer{Fail: strings.Join(command, " "), FailCode: 2, FailReason: "Error ENOENT: absent"}
	return missing.Exec(context.Background(), args)
}

func dashboardRestoreState(f *restoreCLI) *state {
	s := &state{cluster: testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second}), registry: &registry{},
		username: "tester", role: "read-only", password: "fixture-password", cert: []byte("new-cert"), key: []byte("new-key")}
	s.registry.active = s
	return s
}

func TestRestoreVerifiesAccountAfterCompetingCreateOrReplacement(t *testing.T) {
	for _, competing := range []bool{true, false} {
		t.Run(fmt.Sprintf("competing=%t", competing), func(t *testing.T) {
			f := &restoreCLI{}
			s := dashboardRestoreState(f)
			err := s.createAccount(t.Context(), func(_ context.Context, _ *ceph.Container, _ string, password []byte, args ...string) error {
				if !slices.Equal(args[:len(args)-1], []string{"dashboard", "ac-user-create", "tester", "-i", "-", "read-only"}) || string(password) != s.password || args[len(args)-1] == "" {
					t.Fatalf("unexpected create arguments %v", args)
				}
				f.accountName = args[len(args)-1]
				if competing {
					// Ceph can exit successfully reporting the account already exists.
					f.accountName = "foreign"
				}
				return nil
			})
			if competing && !errors.Is(err, ceph.ErrInvalidRequest) || !competing && err != nil {
				t.Fatalf("creation returned %v", err)
			}
			f.accountName = "foreign"
			if err := s.restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || f.accountName != "foreign" || s.registry.active != s {
				t.Fatalf("replacement after restore: name=%q active=%t err=%v", f.accountName, s.registry.active == s, err)
			}
			f.accountName = ""
			if err := s.restore(t.Context()); err != nil || s.registry.active != nil {
				t.Fatalf("retry after external removal: %v", err)
			}
		})
	}
}

func TestRestoreRecognizesAccountAfterLostCreateReply(t *testing.T) {
	f := &restoreCLI{}
	s := dashboardRestoreState(f)
	lost := errors.New("create reply lost")
	err := s.createAccount(t.Context(), func(_ context.Context, _ *ceph.Container, _ string, _ []byte, args ...string) error {
		f.accountName = args[len(args)-1]
		return lost
	})
	if !errors.Is(err, lost) || f.accountName == "" {
		t.Fatalf("create result: account=%q err=%v", f.accountName, err)
	}
	if err := s.restore(t.Context()); err != nil || f.accountName != "" || s.registry.active != nil {
		t.Fatalf("owned account after cleanup: account=%q err=%v", f.accountName, err)
	}
}

func TestRestorePartialCertificateInstall(t *testing.T) {
	for _, existed := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior=%t", existed), func(t *testing.T) {
			f := &restoreCLI{keys: map[string]string{}}
			if existed {
				f.keys["mgr/dashboard/crt"] = "old-cert"
				f.keys["mgr/dashboard/key"] = "old-key"
			}
			s := dashboardRestoreState(f)
			failed := errors.New("certificate was refused")
			err := s.installCertificate(t.Context(), func(_ context.Context, _ *ceph.Container, _ string, _ []byte, args ...string) error {
				if args[1] != "set-ssl-certificate" {
					t.Fatalf("unexpected write %v", args)
				}
				return failed
			})
			if !errors.Is(err, failed) {
				t.Fatalf("install: %v", err)
			}
			f.keys["mgr/dashboard/key"] = "outside-key" // this write was never attempted by Start
			if err := s.restore(t.Context()); err != nil || f.keys["mgr/dashboard/key"] != "outside-key" || s.registry.active != nil {
				t.Fatalf("keys after partial restore: %v, %v", f.keys, err)
			}
		})
	}
}

func TestRestoreCertificateRetriesLostReplyAndPreservesOutsideValue(t *testing.T) {
	f := &restoreCLI{keys: map[string]string{"mgr/dashboard/crt": "outside-cert"}, loseRemoveReply: true}
	s := dashboardRestoreState(f)
	s.certificate = &configKey{name: "mgr/dashboard/crt", installed: s.cert}
	if err := s.restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || f.keys["mgr/dashboard/crt"] != "outside-cert" {
		t.Fatalf("foreign certificate after restore: %v, %v", f.keys, err)
	}
	f.keys["mgr/dashboard/crt"] = string(s.cert)
	if err := s.restore(t.Context()); err == nil || len(f.keys) != 0 || s.registry.active != s {
		t.Fatalf("lost remove result: keys=%v err=%v", f.keys, err)
	}
	if err := s.restore(t.Context()); err != nil || s.registry.active != nil {
		t.Fatalf("retry after remove took effect: %v", err)
	}
}

// lateWriteCLI accepts a native input command and its stdin, then loses the
// Docker reply while the command is still pending. The test controls when
// that command changes Ceph state and when the canceler can reach it.
type lateWriteCLI struct {
	*restoreCLI
	delayed      func()
	stopFailures int
}

func (f *lateWriteCLI) GetContainerID() string { return "dashboard-control" }

func (f *lateWriteCLI) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

type lateCommandInput struct {
	bytes.Buffer
	output   *io.PipeWriter
	accepted func(string)
}

func (in *lateCommandInput) CloseWrite() error {
	in.accepted(in.String())
	return in.output.CloseWithError(errors.New("lost Docker reply after stdin reached the command"))
}

func (in *lateCommandInput) Close() error { return in.output.Close() }

func (f *lateWriteCLI) AttachExec(_ context.Context, cmd, _ []string) (supervised.Attachment, error) {
	at := slices.IndexFunc(cmd, func(arg string) bool { return arg == "dashboard" || arg == "config-key" })
	if at < 0 || at+1 >= len(cmd) {
		return supervised.Attachment{}, errors.New("expected a Dashboard or config-key mutation")
	}
	args := cmd[at:]
	read, output := io.Pipe()
	in := &lateCommandInput{output: output, accepted: func(value string) {
		f.delayed = func() {
			switch {
			case slices.Equal(args[:2], []string{"dashboard", "ac-user-create"}):
				f.accountName = args[len(args)-1]
			case slices.Equal(args[:2], []string{"dashboard", "set-ssl-certificate"}):
				f.keys["mgr/dashboard/crt"] = value
			case slices.Equal(args[:2], []string{"dashboard", "set-ssl-certificate-key"}):
				f.keys["mgr/dashboard/key"] = value
			case slices.Equal(args[:2], []string{"dashboard", "set-rgw-api-access-key"}):
				f.keys["mgr/dashboard/RGW_API_ACCESS_KEY"] = value
			case slices.Equal(args[:2], []string{"dashboard", "set-rgw-api-secret-key"}):
				f.keys["mgr/dashboard/RGW_API_SECRET_KEY"] = value
			case slices.Equal(args[:2], []string{"config-key", "set"}) && len(args) == 5:
				f.keys[args[2]] = value
			}
		}
	}}
	return supervised.Attachment{Input: in, Output: read, ExitCode: func(context.Context) (int, error) {
		return 0, errors.New("exec lookup unavailable")
	}, Release: func() { _ = read.Close() }}, nil
}

func (f *lateWriteCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 2 && args[2] == supervised.CancelScript {
		if f.stopFailures > 0 {
			f.stopFailures--
			return 0, nil, errors.New("canceler unavailable")
		}
		f.delayed = nil
		return 0, nil, nil
	}
	return f.restoreCLI.Exec(ctx, args, opts...)
}

func TestRestoreSettlesLateDashboardWrites(t *testing.T) {
	for _, mutation := range []string{"ac-user-create", "set-ssl-certificate", "set-ssl-certificate-key"} {
		t.Run(mutation, func(t *testing.T) {
			f := &lateWriteCLI{restoreCLI: &restoreCLI{keys: make(map[string]string)}, stopFailures: 2}
			s := dashboardRestoreState(f.restoreCLI)
			s.cluster = testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second})
			var err error
			if mutation == "ac-user-create" {
				err = s.createAccount(t.Context(), runWithInput)
			} else {
				err = s.installCertificate(t.Context(), func(ctx context.Context, c *ceph.Container, label string, data []byte, args ...string) error {
					if args[1] == mutation {
						return runWithInput(ctx, c, label, data, args...)
					}
					f.keys["mgr/dashboard/crt"] = string(data)
					return nil
				})
			}
			var pending *ceph.NativeCommandUnstoppedError
			if !errors.As(err, &pending) || f.delayed == nil {
				t.Fatalf("expected an accepted write whose stop failed: %v", err)
			}
			if err := s.restore(t.Context()); err == nil || s.registry.active != s || s.restored || f.delayed == nil {
				t.Fatalf("restore released a pending write: %v", err)
			}
			f.delayed()
			switch mutation {
			case "ac-user-create":
				if f.accountName == "" || f.accountName != s.accountMarker {
					t.Fatal("late account was not published with its creation marker")
				}
			case "set-ssl-certificate":
				if f.keys["mgr/dashboard/crt"] != string(s.cert) {
					t.Fatal("late certificate was not published")
				}
			case "set-ssl-certificate-key":
				if f.keys["mgr/dashboard/key"] != string(s.key) {
					t.Fatal("late private key was not published")
				}
			}
			if err := s.restore(t.Context()); err != nil || f.accountName != "" || len(f.keys) != 0 || s.registry.active != nil || f.delayed != nil {
				t.Fatalf("retry left account=%q, keys=%d, active=%t: %v", f.accountName, len(f.keys), s.registry.active != nil, err)
			}
		})
	}
}

func TestRestoreSettlesLateTLSRollback(t *testing.T) {
	const key = "mgr/dashboard/crt"
	f := &lateWriteCLI{restoreCLI: &restoreCLI{keys: map[string]string{key: "installed"}}, stopFailures: 2}
	s := dashboardRestoreState(f.restoreCLI)
	s.cluster = testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second})
	s.certificate = &configKey{name: key, existed: true, previous: []byte("original"), installed: []byte("installed")}

	var pending *ceph.NativeCommandUnstoppedError
	if err := s.restore(t.Context()); !errors.As(err, &pending) || f.delayed == nil || f.keys[key] != "installed" || s.registry.active != s {
		t.Fatalf("rollback before delayed write: value=%q active=%t err=%v", f.keys[key], s.registry.active == s, err)
	}
	f.delayed()
	if f.keys[key] != "original" {
		t.Fatalf("late rollback wrote %q", f.keys[key])
	}
	// The key already reads as its prior value, but the command has not
	// been confirmed stopped. The matching value alone cannot free the slot.
	if err := s.restore(t.Context()); err == nil || s.registry.active != s || s.restored {
		t.Fatalf("retry released an unsettled rollback: %v", err)
	}
	if err := s.restore(t.Context()); err != nil || f.keys[key] != "original" || f.delayed != nil || s.registry.active != nil {
		t.Fatalf("settled rollback: value=%q active=%t err=%v", f.keys[key], s.registry.active != nil, err)
	}
}

func TestRestoreSettlesLateGatewayKeyWrites(t *testing.T) {
	for _, option := range []string{"RGW_API_ACCESS_KEY", "RGW_API_SECRET_KEY"} {
		for _, stage := range []string{"install", "rollback"} {
			t.Run(option+"/"+stage, func(t *testing.T) {
				f := &lateWriteCLI{restoreCLI: &restoreCLI{keys: map[string]string{}}, stopFailures: 2}
				s := dashboardRestoreState(f.restoreCLI)
				s.cluster = testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second})
				key := &rgwKeyOption{option: option, command: strings.ToLower(strings.ReplaceAll(option, "_", "-"))}
				s.link = &GatewayLink{&linkState{dashboard: s, keys: []*rgwKeyOption{key}}}
				var err error
				var wantAfterWrite string
				switch stage {
				case "install":
					wantAfterWrite = "fixture-value"
					err = key.install(t.Context(), s.cluster, wantAfterWrite)
				case "rollback":
					key.existed, key.previous, key.installed = true, "prior-value", "fixture-value"
					f.keys[key.name()] = key.installed
					wantAfterWrite = key.previous
					err = s.restore(t.Context())
				}
				var pending *ceph.NativeCommandUnstoppedError
				if !errors.As(err, &pending) || f.delayed == nil {
					t.Fatalf("write was not left pending: %v", err)
				}
				if stage == "rollback" {
					// Restoration has taken effect, but its writer can still run.
					f.delayed()
				}
				if err := s.restore(t.Context()); err == nil || s.registry.active != s || s.link == nil || s.restored {
					t.Fatalf("Restore released the link before the command stopped: %v", err)
				}
				if stage == "install" {
					// The old (absent) value was visible throughout the failed Restore.
					f.delayed()
				}
				if f.keys[key.name()] != wantAfterWrite {
					t.Fatal("late command did not write the requested value")
				}
				if err := s.restore(t.Context()); err != nil || s.registry.active != nil || s.link != nil || f.delayed != nil {
					t.Fatalf("Restore did not settle the pending link: %v", err)
				}
				if actual, found := f.keys[key.name()]; found != key.existed || actual != key.previous {
					t.Fatal("settled link did not restore the original gateway API key option")
				}
			})
		}
	}
}
