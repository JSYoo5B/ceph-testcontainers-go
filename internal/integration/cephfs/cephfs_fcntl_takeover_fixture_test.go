//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"encoding/json"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestCephFSFcntlTakeoverIdentity(t *testing.T) {
	first := harness.CephFSActorReceipt{Token: "owned-actor", PID: 10, Inode: 123,
		Ops: json.RawMessage(`{"start_ticks":"1000","fd":3,"mount_id":20,"device":40}`)}
	if err := sameFcntlTakeoverFD(first, first); err != nil {
		t.Fatal(err)
	}
	for name, modify := range map[string]func(*harness.CephFSActorReceipt){
		"new process": func(r *harness.CephFSActorReceipt) { r.PID++ },
		"new inode":   func(r *harness.CephFSActorReceipt) { r.Inode++ },
		"other run":   func(r *harness.CephFSActorReceipt) { r.Token = "other" },
		"PID reused": func(r *harness.CephFSActorReceipt) {
			r.Ops = json.RawMessage(`{"start_ticks":"1001","fd":3,"mount_id":20,"device":40}`)
		},
		"new fd": func(r *harness.CephFSActorReceipt) {
			r.Ops = json.RawMessage(`{"start_ticks":"1000","fd":4,"mount_id":20,"device":40}`)
		},
		"new mount": func(r *harness.CephFSActorReceipt) {
			r.Ops = json.RawMessage(`{"start_ticks":"1000","fd":3,"mount_id":21,"device":40}`)
		},
		"new device": func(r *harness.CephFSActorReceipt) {
			r.Ops = json.RawMessage(`{"start_ticks":"1000","fd":3,"mount_id":20,"device":41}`)
		},
		"missing fd": func(r *harness.CephFSActorReceipt) {
			r.Ops = json.RawMessage(`{"start_ticks":"1000","mount_id":20,"device":40}`)
		},
		"missing identity": func(r *harness.CephFSActorReceipt) { r.Ops = json.RawMessage(`{}`) },
	} {
		t.Run(name, func(t *testing.T) {
			next := first
			modify(&next)
			if err := sameFcntlTakeoverFD(first, next); err == nil {
				t.Fatal("replacement identity accepted as retained fd")
			}
		})
	}
}

func TestCephFSFcntlTakeoverConflict(t *testing.T) {
	before := harness.CephFSActorReceipt{Errno: 11}
	after := harness.CephFSActorReceipt{Errno: 13}
	if err := fcntlTakeoverConflict(before, after); err != nil {
		t.Fatal(err)
	}
	for _, code := range []int{0, 5, 28, 110} {
		if err := fcntlTakeoverConflict(before, harness.CephFSActorReceipt{Errno: code}); err == nil {
			t.Fatalf("errno %d accepted as lock conflict", code)
		}
	}
	lock := &harness.FcntlLockView{Type: 1, Start: 0, Len: 4096, PID: 10}
	if err := sameFcntlTakeoverLock(lock, lock); err != nil {
		t.Fatal(err)
	}
	for name, next := range map[string]*harness.FcntlLockView{
		"missing":         nil,
		"unlocked":        {Type: 2, Start: 0, Len: 4096, PID: 10},
		"different range": {Type: 1, Start: 4096, Len: 4096, PID: 10},
		"short range":     {Type: 1, Start: 0, Len: 1, PID: 10},
		"different owner": {Type: 1, Start: 0, Len: 4096, PID: 11},
	} {
		t.Run(name, func(t *testing.T) {
			if err := sameFcntlTakeoverLock(lock, next); err == nil {
				t.Fatal("missing or changed lock accepted")
			}
		})
	}
}
