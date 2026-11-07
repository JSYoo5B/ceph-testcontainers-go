package dashboard

import (
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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
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
