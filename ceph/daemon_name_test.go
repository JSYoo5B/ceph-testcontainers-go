package ceph

import (
	"errors"
	"strings"
	"testing"
)

// TestDaemonNameRuleIsOneRuleForCreationAndReaders runs the names that
// AddMonitor and AddManager admitted in v1.0.2, and names they refused,
// through creation admission, the admin-socket parser and the debug target.
// v1.0.2 admitted every name of naming.DaemonName at any length; a name of
// more than 79 bytes then never became ready, because its admin socket path
// is longer than the 107 bytes Ceph binds, so creation now refuses it.
func TestDaemonNameRuleIsOneRuleForCreationAndReaders(t *testing.T) {
	long := func(n int) string { return "n" + strings.Repeat("x", n-1) }
	for _, tc := range []struct {
		name   string
		accept bool
	}{
		{"a", true}, {"b", true}, {"mon-1", true}, {"0", true}, {"A_b.c-d", true},
		{long(64), true}, {long(65), true}, {long(maxDaemonNameBytes), true},
		{long(maxDaemonNameBytes + 1), false}, {long(200), false},
		{"", false}, {"-a", false}, {".a", false}, {"_a", false}, {"a b", false}, {"a/b", false}, {"é", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closed := &Container{closed: true}
			_, monErr := closed.AddMonitor(t.Context(), tc.name)
			_, mgrErr := closed.AddManager(t.Context(), tc.name)
			for role, err := range map[string]error{"AddMonitor": monErr, "AddManager": mgrErr} {
				if tc.accept && !errors.Is(err, errClusterTerminated) {
					t.Errorf("%s refused %q at admission: %v", role, tc.name, err)
				}
				if !tc.accept && !errors.Is(err, ErrInvalidRequest) {
					t.Errorf("%s refusal of %q does not match ErrInvalidRequest: %v", role, tc.name, err)
				}
			}
			for _, kind := range []string{"mon", "mgr"} {
				_, id, err := parseAdminSocketDaemon(kind + "." + tc.name)
				if tc.accept && (err != nil || id != tc.name) {
					t.Errorf("admin socket parser refused %s.%s: %v", kind, tc.name, err)
				}
				if !tc.accept && !errors.Is(err, ErrInvalidRequest) {
					t.Errorf("admin socket parser refusal of %s.%s does not match ErrInvalidRequest: %v", kind, tc.name, err)
				}
			}
			_, name, err := parseDebugDaemon("mgr." + tc.name)
			if tc.accept && (err != nil || name != tc.name) {
				t.Errorf("debug target refused mgr.%s: %v", tc.name, err)
			}
			if !tc.accept {
				if _, err := closed.openDebugTarget(t.Context(), "mgr."+tc.name); !errors.Is(err, ErrInvalidRequest) {
					t.Errorf("debug target refusal of mgr.%s does not match ErrInvalidRequest: %v", tc.name, err)
				}
			}
		})
	}
	if maxDaemonNameBytes != 79 {
		t.Fatalf("maxDaemonNameBytes = %d, want 79", maxDaemonNameBytes)
	}
}
