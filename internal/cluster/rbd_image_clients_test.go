package cluster

import (
	"slices"
	"strings"
	"testing"
)

func TestParseRBDImageClients(t *testing.T) {
	status, err := parseRBDImageClients(
		[]byte(`{"watchers":[{"address":"172.19.0.6:0/3437308921","client":4204,"cookie":281472896547024}]}`),
		[]byte(`[{"id":"auto 281472896547024","locker":"client.4204","address":"172.19.0.6:0/3437308921"},`+
			`{"id":"backup","locker":"client.4300","address":"172.19.0.9:0/11"}]`))
	if err != nil {
		t.Fatal(err)
	}
	if want := []RBDImageWatcher{{Address: "172.19.0.6:0/3437308921", ClientID: 4204, Cookie: 281472896547024}}; !slices.Equal(status.Watchers, want) {
		t.Fatalf("watchers = %+v", status.Watchers)
	}
	owner := status.ExclusiveOwner()
	if owner == nil || owner.Locker != "client.4204" || !owner.Managed || status.Locks[1].Managed {
		t.Fatalf("locks = %+v", status.Locks)
	}
	empty, err := parseRBDImageClients([]byte(`{"watchers":[]}`), []byte(`[]`))
	if err != nil || empty.Watchers == nil || empty.Locks == nil || empty.ExclusiveOwner() != nil {
		t.Fatalf("empty = %+v, %v", empty, err)
	}
	for _, bad := range [][2]string{
		{`{}`, `[]`},
		{`{"watchers":[]}`, `null`},
		{`{"watchers":[{"address":"","client":1}]}`, `[]`},
		{`{"watchers":[]}`, `[{"id":"auto 1","locker":"client.1"}]`},
	} {
		if _, err := parseRBDImageClients([]byte(bad[0]), []byte(bad[1])); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}

func TestRBDImageClientsRefusesInvalidNamesBeforeCommands(t *testing.T) {
	// An empty Container has no control CLI, so any command attempt would fail
	// with a different error than the name validation checked here.
	cluster := &Container{}
	for _, args := range [][3]string{{"", "", "disk"}, {"data", "bad ns", "disk"}, {"data", "", "a/b"}} {
		if _, err := RBDImageClients(t.Context(), cluster, args[0], args[1], args[2]); err == nil || !strings.Contains(err.Error(), "name must use") {
			t.Fatalf("%q: error=%v", args, err)
		}
	}
	if _, err := RBDImageClients(t.Context(), nil, "data", "", "disk"); err == nil {
		t.Fatal("nil cluster was accepted")
	}
}
