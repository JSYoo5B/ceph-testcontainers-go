//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_mirror_daemon_redistribution_test

import "testing"

func TestRedistributionCandidate(t *testing.T) {
	prior := map[string]bool{"1": true, "2": true}
	for _, tc := range []struct {
		name    string
		current map[string]bool
		want    string
	}{
		{"only survivor", map[string]bool{"2": true}, ""},
		{"former owner", map[string]bool{"1": true, "2": true}, ""},
		{"survivor lost", map[string]bool{"1": true, "3": true}, ""},
		{"new registered member", map[string]bool{"2": true, "3": true}, "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := redistributionCandidate(prior, tc.current, "2"); got != tc.want {
				t.Fatalf("candidate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedistributionRequiresCompleteCurrentEvidence(t *testing.T) {
	paired := map[string]bool{"2": true, "3": true}
	stuck := `{"mapping":{"2":"4 directories","3":"0 directories"}}`
	for _, tc := range []struct {
		name, raw string
		after     map[string]bool
		spread    bool
		invalid   bool
	}{
		{"all remain on survivor", stuck, paired, false, false},
		{"both hold directories", `{"mapping":{"2":"2 directories","3":"2 directories"}}`, paired, true, false},
		{"missing mapping", `{}`, paired, false, true},
		{"null mapping", `{"mapping":null}`, paired, false, true},
		{"one reported member", `{"mapping":{"2":"4 directories"}}`, paired, false, true},
		{"malformed count", `{"mapping":{"2":"unknown","3":"0 directories"}}`, paired, false, true},
		{"negative count", `{"mapping":{"2":"5 directories","3":"-1 directories"}}`, paired, false, true},
		{"unlabelled count", `{"mapping":{"2":"4","3":"0 directories"}}`, paired, false, true},
		{"incomplete tally", `{"mapping":{"2":"3 directories","3":"0 directories"}}`, paired, false, true},
		{"another distribution member", `{"mapping":{"2":"4 directories","4":"0 directories"}}`, paired, false, true},
		{"another registered member", stuck, map[string]bool{"2": true, "4": true}, false, true},
		{"replacement lost after registering", stuck, map[string]bool{"2": true}, false, true},
		{"everything moved to replacement", `{"mapping":{"2":"0 directories","3":"4 directories"}}`, paired, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeRedistribution([]byte(tc.raw), paired, tc.after, "2", "3", 4)
			if got != tc.spread || (err != nil) != tc.invalid {
				t.Fatalf("spread=%t error=%v, want spread=%t invalid=%t", got, err, tc.spread, tc.invalid)
			}
		})
	}
}
