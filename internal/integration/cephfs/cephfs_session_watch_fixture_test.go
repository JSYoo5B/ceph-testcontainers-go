//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"testing"

	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
)

func TestListsOpenSessions(t *testing.T) {
	session := func(rank int, id uint64, address string) cephfssession.Session {
		return cephfssession.Session{Rank: rank, ID: id, Address: address, State: "open"}
	}
	retained0, retained1 := session(0, 4101, "192.0.2.10:0/1"), session(1, 4101, "192.0.2.10:0/1")
	departing0, departing1 := session(0, 4102, "192.0.2.11:0/2"), session(1, 4102, "192.0.2.11:0/2")
	want := map[sessionWatchKey]cephfssession.Session{}
	for _, s := range []cephfssession.Session{retained0, retained1, departing0, departing1} {
		want[sessionWatchKeyOf(s)] = s
	}
	closing := departing1
	closing.State = "closing"
	moved := departing1
	moved.Address = "192.0.2.12:0/3"
	for _, c := range []struct {
		name     string
		sessions []cephfssession.Session
		want     bool
	}{
		{"all four in any order", []cephfssession.Session{departing1, retained0, departing0, retained1}, true},
		// Four rows, but rank 1 lists the retained session twice and the
		// departing one not at all.
		{"duplicate row with a missing identity", []cephfssession.Session{retained0, retained1, departing0, retained1}, false},
		{"duplicate row beyond the four", []cephfssession.Session{retained0, retained1, departing0, departing1, retained1}, false},
		{"missing identity", []cephfssession.Session{retained0, retained1, departing0}, false},
		{"not open", []cephfssession.Session{retained0, retained1, departing0, closing}, false},
		{"another address", []cephfssession.Session{retained0, retained1, departing0, moved}, false},
		{"none", nil, false},
	} {
		if got := listsOpenSessions(want, c.sessions); got != c.want {
			t.Errorf("%s: listsOpenSessions = %t, want %t", c.name, got, c.want)
		}
	}
}
