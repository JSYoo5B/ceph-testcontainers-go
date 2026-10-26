package ceph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeCLI answers each command line from a table and records every query, so
// a test sees exactly which commands an observation sent.
type fakeCLI struct {
	replies map[string][]byte
	errs    map[string]error
	calls   []string
	ctxSeen []context.Context
}

func (f *fakeCLI) run(ctx context.Context, args ...string) ([]byte, error) {
	key := strings.Join(args, " ")
	f.calls = append(f.calls, key)
	f.ctxSeen = append(f.ctxSeen, ctx)
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	return f.replies[key], nil
}

func TestReadStatusCommandAndDecode(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{"status --format json": []byte(`{"fsid":"f","health":{"status":"HEALTH_OK"},"mgrmap":{"available":true},"osdmap":{"num_osds":3,"num_up_osds":3,"num_in_osds":2}}`)}}
	got, err := readStatus(context.Background(), cli.run)
	if err != nil || got.FSID != "f" || got.Health.Status != "HEALTH_OK" || !got.MgrMap.Available || got.OSDMap.NumInOSDs != 2 {
		t.Fatalf("status = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(cli.calls, []string{"status --format json"}) {
		t.Fatalf("calls = %q", cli.calls)
	}
}

func TestReadStatusErrors(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeCLI{errs: map[string]error{"status --format json": boom}}
	if got, err := readStatus(context.Background(), cli.run); !errors.Is(err, boom) || !reflect.DeepEqual(got, Status{}) {
		t.Fatalf("command error = %+v, %v", got, err)
	}
	cli = &fakeCLI{replies: map[string][]byte{"status --format json": []byte(`{`)}}
	if _, err := readStatus(context.Background(), cli.run); err == nil || !strings.HasPrefix(err.Error(), "decode ceph status: ") {
		t.Fatalf("decode error = %v", err)
	}
}

func TestReadManagerStatusKeepsPartialResultOnDecodeError(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{"mgr dump --format json": []byte(`{"available":true,"active_name":"a","active_gid":7,"standbys":[{"name":"b","gid":9}]}`)}}
	got, err := readManagerStatus(context.Background(), cli.run)
	if err != nil || !got.Available || got.ActiveName != "a" || got.ActiveGID != 7 || len(got.Standbys) != 1 || got.Standbys[0].Name != "b" {
		t.Fatalf("manager status = %+v, %v", got, err)
	}
	boom := errors.New("boom")
	cli = &fakeCLI{errs: map[string]error{"mgr dump --format json": boom}}
	if got, err := readManagerStatus(context.Background(), cli.run); !errors.Is(err, boom) || !reflect.DeepEqual(got, ManagerStatus{}) {
		t.Fatalf("command error = %+v, %v", got, err)
	}
	cli = &fakeCLI{replies: map[string][]byte{"mgr dump --format json": []byte(`{"available":true,"active_name":5}`)}}
	if got, err := readManagerStatus(context.Background(), cli.run); err == nil || !got.Available {
		t.Fatalf("decode error result = %+v, %v", got, err)
	}
}

func daemonAddressReplies(t *testing.T) map[string][]byte {
	t.Helper()
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join("testdata", "daemon-addresses", "20.2.4", name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	return map[string][]byte{
		"mon dump --format json": read("mon-dump.json"),
		"mgr dump --format json": read("mgr-dump.json"),
		"osd dump --format json": read("osd-dump.json"),
	}
}

func TestReadDaemonAddressesQueriesThreeMapsInOrder(t *testing.T) {
	cli := &fakeCLI{replies: daemonAddressReplies(t)}
	got, err := readDaemonAddresses(context.Background(), cli.run)
	if err != nil || len(got.Monitors) != 2 || got.ActiveManager == nil || len(got.OSDs) != 1 {
		t.Fatalf("addresses = %+v, %v", got, err)
	}
	want := []string{"mon dump --format json", "mgr dump --format json", "osd dump --format json"}
	if !reflect.DeepEqual(cli.calls, want) {
		t.Fatalf("calls = %q, want %q", cli.calls, want)
	}
}

func TestReadDaemonAddressesErrorsStopAtFirstFailureWithMapName(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		failing, prefix string
		calls           int
	}{
		{"mon dump --format json", "read monmap: ", 1},
		{"mgr dump --format json", "read mgrmap: ", 2},
		{"osd dump --format json", "read osdmap: ", 3},
	} {
		t.Run(tc.failing, func(t *testing.T) {
			cli := &fakeCLI{replies: daemonAddressReplies(t), errs: map[string]error{tc.failing: boom}}
			got, err := readDaemonAddresses(context.Background(), cli.run)
			if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), tc.prefix) || len(cli.calls) != tc.calls {
				t.Fatalf("err = %v, calls = %q", err, cli.calls)
			}
			if got.MonMapEpoch != 0 || got.Monitors != nil {
				t.Fatalf("partial result returned: %+v", got)
			}
		})
	}
}

func TestReadDaemonAddressesParsesAfterAllReads(t *testing.T) {
	// A malformed monmap is reported only after all three reads, as before.
	replies := daemonAddressReplies(t)
	replies["mon dump --format json"] = []byte(`{}`)
	cli := &fakeCLI{replies: replies}
	got, err := readDaemonAddresses(context.Background(), cli.run)
	if err == nil || !strings.Contains(err.Error(), "decode monmap") || len(cli.calls) != 3 || got.MonMapEpoch != 0 {
		t.Fatalf("err = %v, calls = %q, got = %+v", err, cli.calls, got)
	}
}

func TestCLIObservationsPassTheCallerContext(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	cli := &fakeCLI{replies: daemonAddressReplies(t)}
	cli.replies["status --format json"] = []byte(`{}`)
	_, _ = readStatus(ctx, cli.run)
	_, _ = readManagerStatus(ctx, cli.run)
	_, _ = readDaemonAddresses(ctx, cli.run)
	for i, seen := range cli.ctxSeen {
		if seen != ctx {
			t.Fatalf("query %d (%s) did not receive the caller context", i, cli.calls[i])
		}
	}
}

// The exported methods keep selecting the control container on every call, so
// a cluster without one refuses before any command runs.
func TestCLIObservationsRefuseWithoutControlContainer(t *testing.T) {
	c := &Container{}
	if _, err := c.Status(context.Background()); err == nil || !strings.Contains(err.Error(), "ceph control container is unavailable") {
		t.Fatalf("Status err = %v", err)
	}
	if _, err := c.ManagerStatus(context.Background()); err == nil || !strings.Contains(err.Error(), "ceph control container is unavailable") {
		t.Fatalf("ManagerStatus err = %v", err)
	}
	if _, err := c.DaemonAddresses(context.Background()); err == nil || !strings.Contains(err.Error(), "read monmap: ") {
		t.Fatalf("DaemonAddresses err = %v", err)
	}
}
