package cluster

import (
	"slices"
	"strings"
	"testing"
)

func TestParseObjectWatchers(t *testing.T) {
	// Output captured from rados listwatchers on Ceph 20.2.4.
	watchers, err := parseObjectWatchers("watcher=172.19.0.6:0/3461995546 client.4193 cookie=187650986454688\n")
	if err != nil {
		t.Fatal(err)
	}
	if want := []ObjectWatcher{{Address: "172.19.0.6:0/3461995546", ClientID: 4193, Cookie: 187650986454688}}; !slices.Equal(watchers, want) {
		t.Fatalf("watchers = %+v", watchers)
	}
	if watchers, err := parseObjectWatchers(""); err != nil || watchers == nil || len(watchers) != 0 {
		t.Fatalf("empty output = %+v, %v", watchers, err)
	}
	for _, bad := range []string{"watcher=a client.x cookie=1", "error: something", "watcher= client.1 cookie=1", "watcher=a client.1"} {
		if _, err := parseObjectWatchers(bad); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}

func TestObjectWatchersRefusesUnsafeNamesBeforeCommands(t *testing.T) {
	// An empty Container has no control CLI, so a command attempt would fail
	// with a different error than the validation checked here.
	cluster := &Container{}
	for _, args := range [][3]string{{"bad pool", "", "obj"}, {"pool", "-ns", "obj"}, {"pool", "", ""}, {"pool", "", "--all"}, {"pool", "", "a\nb"}} {
		if _, err := cluster.ObjectWatchers(t.Context(), args[0], args[1], args[2]); err == nil || !(strings.Contains(err.Error(), "name") || strings.Contains(err.Error(), "empty")) {
			t.Fatalf("%q: error=%v", args, err)
		}
	}
}
