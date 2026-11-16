package rbd

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"slices"
	"strings"
	"testing"
)

func TestParseRBDImageClients(t *testing.T) {
	const watcher = `{"watchers":[{"address":"10.0.0.1:0/2","client":4204,"cookie":92}]}`
	const record = `{"name":"rbd_lock","type":%q,"tag":%q,"lockers":[{"cookie":%q,"name":"client.4204","addr":"10.0.0.1:0/2"}]}`
	for _, tc := range []struct {
		mode, tag, cookie string
		managed           bool
	}{
		{"exclusive", "internal", "auto 92", true},
		{"exclusive", "", "auto backup", false},
		{"exclusive", "", "auto 92", false},
		{"shared", "internal", "auto 92", false},
		{"exclusive", "internal", "auto backup", false},
		{"exclusive", "internal", "auto 0", false},
	} {
		status, err := parseImageClients([]byte(watcher), []byte(fmt.Sprintf(record, tc.mode, tc.tag, tc.cookie)))
		if err != nil {
			t.Fatal(err)
		}
		if want := []ImageWatcher{{Address: "10.0.0.1:0/2", ClientID: 4204, Cookie: 92}}; !slices.Equal(status.Watchers, want) {
			t.Fatalf("watchers = %+v", status.Watchers)
		}
		want := ImageLock{ID: tc.cookie, Locker: "client.4204", Address: "10.0.0.1:0/2", Managed: tc.managed}
		if !slices.Equal(status.Locks, []ImageLock{want}) || (status.ExclusiveOwner() != nil) != tc.managed {
			t.Fatalf("%+v: locks = %+v, owner = %+v", tc, status.Locks, status.ExclusiveOwner())
		}
	}
	const emptyLock = `{"name":"rbd_lock","type":"none","tag":"","lockers":[]}`
	empty, err := parseImageClients([]byte(`{"watchers":[]}`), []byte(emptyLock))
	if err != nil || empty.Watchers == nil || empty.Locks == nil || empty.ExclusiveOwner() != nil {
		t.Fatalf("empty = %+v, %v", empty, err)
	}
	for _, bad := range [][2]string{
		{`{}`, emptyLock},
		{watcher, `null`},
		{`{"watchers":[{"address":"","client":1}]}`, emptyLock},
		{watcher, `{"name":"rbd_lock","type":"exclusive","lockers":[]}`},
		{watcher, `{"name":"rbd_lock","type":"shared","tag":"","lockers":[{"name":"client.1"}]}`},
		{watcher, `{"name":"rbd_lock","type":"unexpected","tag":"","lockers":[]}`},
	} {
		if _, err := parseImageClients([]byte(bad[0]), []byte(bad[1])); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}

func TestImageLockHeader(t *testing.T) {
	for info, want := range map[string]string{
		`{"format":1}`:            "disk.rbd",
		`{"format":2,"id":"abc"}`: "rbd_header.abc",
		`{"format":2}`:            "",
		`{}`:                      "",
	} {
		got, err := imageLockHeader([]byte(info), "disk")
		if got != want || (err != nil) != (want == "") {
			t.Fatalf("%s: got %q, %v", info, got, err)
		}
	}
}

func TestRBDImageClientsRefusesInvalidNamesBeforeCommands(t *testing.T) {
	// An empty Container has no control CLI, so any command attempt would fail
	// with a different error than the name validation checked here.
	cluster := testhelper.TestCluster(nil, testhelper.TestConfig{})
	for _, args := range [][3]string{{"", "", "disk"}, {"data", "bad ns", "disk"}, {"data", "", "a/b"}} {
		if _, err := ImageClients(t.Context(), cluster, args[0], args[1], args[2]); err == nil || !strings.Contains(err.Error(), "name must use") {
			t.Fatalf("%q: error=%v", args, err)
		}
	}
	if _, err := ImageClients(t.Context(), nil, "data", "", "disk"); err == nil {
		t.Fatal("nil cluster was accepted")
	}
}
