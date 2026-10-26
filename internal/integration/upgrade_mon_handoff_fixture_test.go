//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"net/netip"
	"testing"
)

// These are Docker-free controls of the retained MON address handoff model
// with synthetic identities and addresses. They run no mon-join.sh, Ceph or
// Docker and establish no native upgrade behavior.

func monAddr(ip string, v2, v1 uint16) upgradeMonAddr {
	return upgradeMonAddr{IP: netip.MustParseAddr(ip), V2: v2, V1: v1}
}

func retainedMon() upgradeMonRetained {
	return upgradeMonRetained{Name: "a", FSID: "fsid-1", StoreID: "store-a", Addrs: upgradeMonVector{monAddr("10.0.0.2", 3300, 6789)}}
}

func launchFor(r upgradeMonRetained) upgradeMonLaunch {
	return upgradeMonLaunch{Transition: "a-to-dst", Name: r.Name, FSID: r.FSID, StoreID: r.StoreID, HasStore: true, Reserved: append(upgradeMonVector(nil), r.Addrs...)}
}

func TestUpgradeMonHandoffAdmitsMatchingEndpoint(t *testing.T) {
	h, err := newUpgradeMonHandoff(retainedMon())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := h.admit(launchFor(retainedMon()))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mkfs || plan.MonmapUpdate || plan.MembershipChange {
		t.Fatalf("plan changes store or membership: %+v", plan)
	}
	for k, want := range map[string]string{
		"CEPH_MON_ID": "a", "CEPH_ADDRESS_FAMILY": "ipv4", "CEPH_PUBLIC_ADDRESS": "10.0.0.2",
		"CEPH_MON_PORT_V2": "3300", "CEPH_MON_PORT_V1": "6789",
	} {
		if plan.Env[k] != want {
			t.Errorf("env %s = %q, want %q", k, plan.Env[k], want)
		}
	}
	// A msgr2-only MON keeps its single listener and does not invent v1.
	r := retainedMon()
	r.Addrs = upgradeMonVector{monAddr("10.0.0.2", 3300, 0)}
	h2, err := newUpgradeMonHandoff(r)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := h2.admit(launchFor(r))
	if err != nil || p2.Env["CEPH_MSGR2_ONLY"] != "true" || p2.Env["CEPH_MON_PORT_V1"] != "" {
		t.Fatalf("msgr2-only plan = %+v, %v", p2, err)
	}
}

func TestUpgradeMonHandoffRejectsBeforeLaunch(t *testing.T) {
	cases := map[string]func(*upgradeMonLaunch){
		"different address": func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("10.0.0.3", 3300, 6789)} },
		"different v2 port": func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("10.0.0.2", 3301, 6789)} },
		"different v1 port": func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("10.0.0.2", 3300, 6790)} },
		"v1 dropped":        func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("10.0.0.2", 3300, 0)} },
		"different family":  func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("fd00::2", 3300, 6789)} },
		"mapped family":     func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("::ffff:10.0.0.2", 3300, 6789)} },
		"extra entry":       func(l *upgradeMonLaunch) { l.Reserved = append(l.Reserved, monAddr("fd00::2", 3300, 6789)) },
		"nil vector":        func(l *upgradeMonLaunch) { l.Reserved = nil },
		"empty vector":      func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{} },
		"zero address":      func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{{V2: 3300, V1: 6789}} },
		"zero v2 port":      func(l *upgradeMonLaunch) { l.Reserved = upgradeMonVector{monAddr("10.0.0.2", 0, 6789)} },
		"duplicate entry":   func(l *upgradeMonLaunch) { l.Reserved = append(l.Reserved, l.Reserved[0]) },
		"other MON name":    func(l *upgradeMonLaunch) { l.Name = "b" },
		"other FSID":        func(l *upgradeMonLaunch) { l.FSID = "fsid-2" },
		"missing FSID":      func(l *upgradeMonLaunch) { l.FSID = "" },
		"other store":       func(l *upgradeMonLaunch) { l.StoreID = "store-b" },
		"missing store id":  func(l *upgradeMonLaunch) { l.StoreID = "" },
		"store.db absent":   func(l *upgradeMonLaunch) { l.HasStore = false },
		"no transition":     func(l *upgradeMonLaunch) { l.Transition = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h, err := newUpgradeMonHandoff(retainedMon())
			if err != nil {
				t.Fatal(err)
			}
			req := launchFor(retainedMon())
			mutate(&req)
			if plan, err := h.admit(req); err == nil {
				t.Fatalf("admitted %+v as %+v", req, plan)
			}
			// A refused request records nothing: the corrected request is
			// still admitted for the same transition.
			if len(h.admitted) != 0 {
				t.Fatalf("rejected request recorded %v", h.admitted)
			}
			if _, err := h.admit(launchFor(retainedMon())); err != nil {
				t.Fatalf("corrected request refused: %v", err)
			}
		})
	}
}

func TestUpgradeMonHandoffComparesVectorsAsSets(t *testing.T) {
	a, b := monAddr("10.0.0.2", 3300, 6789), monAddr("10.0.0.3", 3300, 6789)
	if !(upgradeMonVector{a, b}).sameSet(upgradeMonVector{b, a}) {
		t.Fatal("order changed the comparison")
	}
	if (upgradeMonVector{a, a}).sameSet(upgradeMonVector{a, b}) {
		t.Fatal("repeated entry matched a different one")
	}
}

func TestUpgradeMonHandoffRejectsDuplicateTransition(t *testing.T) {
	h, err := newUpgradeMonHandoff(retainedMon())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.admit(launchFor(retainedMon())); err != nil {
		t.Fatal(err)
	}
	if plan, err := h.admit(launchFor(retainedMon())); err == nil {
		t.Fatalf("second writer for one transition admitted: %+v", plan)
	}
	again := launchFor(retainedMon())
	again.Transition = "a-to-dst-2"
	if _, err := h.admit(again); err != nil {
		t.Fatalf("a distinct transition was refused: %v", err)
	}
}

func TestUpgradeMonHandoffRetainedRecordGuards(t *testing.T) {
	cases := map[string]func(*upgradeMonRetained){
		"no name":        func(r *upgradeMonRetained) { r.Name = "" },
		"no FSID":        func(r *upgradeMonRetained) { r.FSID = "" },
		"no store":       func(r *upgradeMonRetained) { r.StoreID = "" },
		"no vector":      func(r *upgradeMonRetained) { r.Addrs = nil },
		"zero port":      func(r *upgradeMonRetained) { r.Addrs = upgradeMonVector{monAddr("10.0.0.2", 0, 0)} },
		"ipv6 endpoint":  func(r *upgradeMonRetained) { r.Addrs = upgradeMonVector{monAddr("fd00::2", 3300, 6789)} },
		"mapped address": func(r *upgradeMonRetained) { r.Addrs = upgradeMonVector{monAddr("::ffff:10.0.0.2", 3300, 6789)} },
		"dual stack": func(r *upgradeMonRetained) {
			r.Addrs = upgradeMonVector{monAddr("10.0.0.2", 3300, 6789), monAddr("fd00::2", 3300, 6789)}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := retainedMon()
			mutate(&r)
			if _, err := newUpgradeMonHandoff(r); err == nil {
				t.Fatalf("accepted retained record %+v", r)
			}
		})
	}
}

func TestUpgradeMonHandoffBaselineIsNotAliased(t *testing.T) {
	// Changing the constructor input after construction.
	for name, mutate := range map[string]func(*upgradeMonAddr){
		"address": func(a *upgradeMonAddr) { a.IP = netip.MustParseAddr("10.0.0.3") },
		"v2 port": func(a *upgradeMonAddr) { a.V2 = 3301 },
		"v1 port": func(a *upgradeMonAddr) { a.V1 = 6790 },
	} {
		t.Run("constructor input "+name, func(t *testing.T) {
			r := retainedMon()
			h, err := newUpgradeMonHandoff(r)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&r.Addrs[0])
			if plan, err := h.admit(launchFor(r)); err == nil {
				t.Fatalf("launch reserving the mutated endpoint admitted: %+v", plan)
			}
			plan, err := h.admit(launchFor(retainedMon()))
			if err != nil || plan.Env["CEPH_PUBLIC_ADDRESS"] != "10.0.0.2" || plan.Env["CEPH_MON_PORT_V2"] != "3300" || plan.Env["CEPH_MON_PORT_V1"] != "6789" {
				t.Fatalf("original endpoint plan = %+v, %v", plan, err)
			}
		})
	}

	t.Run("returned record", func(t *testing.T) {
		h, err := newUpgradeMonHandoff(retainedMon())
		if err != nil {
			t.Fatal(err)
		}
		got := h.record()
		got.Addrs[0] = monAddr("10.0.0.3", 3301, 6790)
		got.Name = "z"
		if again := h.record(); !again.Addrs.sameSet(retainedMon().Addrs) || again.Name != "a" {
			t.Fatalf("record changed through its copy: %+v", again)
		}
		if _, err := h.admit(launchFor(retainedMon())); err != nil {
			t.Fatalf("baseline moved: %v", err)
		}
	})

	t.Run("returned plan", func(t *testing.T) {
		h, err := newUpgradeMonHandoff(retainedMon())
		if err != nil {
			t.Fatal(err)
		}
		first, err := h.admit(launchFor(retainedMon()))
		if err != nil {
			t.Fatal(err)
		}
		first.Env["CEPH_PUBLIC_ADDRESS"] = "10.0.0.9"
		delete(first.Env, "CEPH_MON_PORT_V2")
		again := launchFor(retainedMon())
		again.Transition = "second"
		second, err := h.admit(again)
		if err != nil || second.Env["CEPH_PUBLIC_ADDRESS"] != "10.0.0.2" || second.Env["CEPH_MON_PORT_V2"] != "3300" {
			t.Fatalf("plan shared state with an earlier plan: %+v, %v", second, err)
		}
	})

	t.Run("launch request vector", func(t *testing.T) {
		// The request's vector is only read: admission must not store it or
		// change it, and a later edit of it cannot affect an admitted plan.
		h, err := newUpgradeMonHandoff(retainedMon())
		if err != nil {
			t.Fatal(err)
		}
		req := launchFor(retainedMon())
		plan, err := h.admit(req)
		if err != nil {
			t.Fatal(err)
		}
		req.Reserved[0].IP = netip.MustParseAddr("10.0.0.9")
		if plan.Env["CEPH_PUBLIC_ADDRESS"] != "10.0.0.2" || !h.record().Addrs.sameSet(retainedMon().Addrs) {
			t.Fatalf("request edit reached the plan or baseline: %+v", plan)
		}
	})
}
