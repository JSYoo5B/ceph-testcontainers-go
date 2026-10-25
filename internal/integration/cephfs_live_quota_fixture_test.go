//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

// These are receipt/model controls, not claims of native quota propagation.
func TestCephFSLiveQuotaReceipt(t *testing.T) {
	base, created := []byte("acknowledged"), []byte("after relaxation")
	first := cephFSActorReceipt{Token: "one-run", PID: 123, Client: 45, Inode: 67, DirInode: 89}
	current := first
	current.Size, current.Full = int64(len(base)), hex.EncodeToString(base)
	limited := `{"limit":2,"files":1,"directories":1,"entries":2,"samples":3,"elapsed_ms":400,"create_errno":122}`
	relaxed := `{"limit":3,"files":1,"directories":1,"entries":2,"samples":1,"elapsed_ms":0,"create_errno":0,"created_inode":68,"created_size":16,"created_data":"` + hex.EncodeToString(created) + `"}`
	t.Run("observed limit and EDQUOT", func(t *testing.T) {
		current.Ops = json.RawMessage(limited)
		if _, err := validateLiveQuotaStep(first, current, 2, base, nil, 122); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("relaxation preserves old fd and new data", func(t *testing.T) {
		current.Ops = json.RawMessage(relaxed)
		if _, err := validateLiveQuotaStep(first, current, 3, base, created, 0); err != nil {
			t.Fatal(err)
		}
	})
	for name, mutate := range map[string]func(*cephFSActorReceipt){
		"new client":      func(r *cephFSActorReceipt) { r.Client++ },
		"new process":     func(r *cephFSActorReceipt) { r.PID++ },
		"new fd inode":    func(r *cephFSActorReceipt) { r.Inode++ },
		"new directory":   func(r *cephFSActorReceipt) { r.DirInode++ },
		"other run":       func(r *cephFSActorReceipt) { r.Token = "another-run" },
		"wrong old bytes": func(r *cephFSActorReceipt) { r.Full = hex.EncodeToString([]byte("changed")) },
		"wrong old size":  func(r *cephFSActorReceipt) { r.Size++ },
	} {
		t.Run(name, func(t *testing.T) {
			r := current
			r.Ops = json.RawMessage(limited)
			mutate(&r)
			if _, err := validateLiveQuotaStep(first, r, 2, base, nil, 122); err == nil {
				t.Fatal("changed identity/data accepted")
			}
		})
	}
	for name, data := range map[string]string{
		"no observations":    `{}`,
		"missing errno":      `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1}`,
		"null limit":         `{"limit":null,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122}`,
		"not yet enforced":   `{"limit":0,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122}`,
		"not at file limit":  `{"limit":2,"files":0,"samples":1,"create_errno":122}`,
		"no actual sample":   `{"limit":2,"files":1,"directories":1,"entries":2,"samples":0,"create_errno":122}`,
		"wrong errno ENOSPC": `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":28}`,
		"create succeeded":   `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":0}`,
		"denied but created": `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122,"created_inode":68}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := current
			r.Ops = json.RawMessage(data)
			if _, err := validateLiveQuotaStep(first, r, 2, base, nil, 122); err == nil {
				t.Fatal("incomplete or contradictory quota evidence accepted")
			}
		})
	}
	t.Run("new bytes must match selected payload", func(t *testing.T) {
		current.Ops = json.RawMessage(relaxed)
		if _, err := validateLiveQuotaStep(first, current, 3, base, []byte("wrong payload"), 0); err == nil {
			t.Fatal("wrong created bytes accepted")
		}
	})
	t.Run("file-only headroom ignores the directory contribution", func(t *testing.T) {
		// Native admission compares rfiles+rsubdirs, not rfiles. With one
		// file and one directory, max_files=2 is still full, not relaxed.
		r := current
		r.Ops = json.RawMessage(`{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":0,"created_inode":68,"created_size":16,"created_data":"` + hex.EncodeToString(created) + `"}`)
		if _, err := validateLiveQuotaStep(first, r, 2, base, created, 0); err == nil {
			t.Fatal("a full recursive entry quota was accepted as relaxed")
		}
	})
}
