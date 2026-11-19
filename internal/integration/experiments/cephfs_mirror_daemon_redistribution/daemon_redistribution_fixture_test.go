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
	admitted := redistributionMembership{services: map[string]bool{"42": true, "43": true}, watchers: paired}
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
			after := redistributionMembership{services: admitted.services, watchers: tc.after}
			got, err := judgeRedistribution([]byte(tc.raw), admitted, admitted, after, "2", "3", 4)
			if got != tc.spread || (err != nil) != tc.invalid {
				t.Fatalf("spread=%t error=%v, want spread=%t invalid=%t", got, err, tc.spread, tc.invalid)
			}
		})
	}
}

func TestRedistributionJoinsSeparateServiceAndWatcherIdentities(t *testing.T) {
	dump := []byte(`{"services":{"cephfs-mirror":{"daemons":{"4273":{"metadata":{"id":"owned"}},"4301":{"metadata":{"id":"owned"}},"5000":{"metadata":{"id":"other"}}}}}}`)
	status := []byte(`[{"daemon_id":4273,"filesystems":[{"name":"source","peers":[{"uuid":"peer"}]}]},{"daemon_id":4301,"filesystems":[{"name":"source","peers":[{"uuid":"peer"}]}]}]`)
	services, err := redistributionRegisteredServices(dump, status, "owned", "source", "peer")
	if err != nil || len(services) != 2 || !services["4273"] || !services["4301"] {
		t.Fatalf("registered services %v: %v", services, err)
	}
	watchers, err := redistributionWatchers(map[string]string{"4288": "address-a", "4311": "address-b"}, map[string][]byte{
		"survivor":    []byte(`{"rados_inst":"address-a","peers":{"peer":{}}}`),
		"replacement": []byte(`{"rados_inst":"address-b","peers":{"peer":{}}}`),
	}, "peer")
	if err != nil || !watchers["4288"] || !watchers["4311"] {
		t.Fatalf("filesystem watchers %v: %v", watchers, err)
	}
	admitted := redistributionMembership{services: services, watchers: watchers}
	raw := []byte(`{"mapping":{"4288":"4 directories","4311":"0 directories"}}`)
	if spread, err := judgeRedistribution(raw, admitted, admitted, admitted, "4288", "4311", 4); err != nil || spread {
		t.Fatalf("four directories at the survivor: spread=%t, %v", spread, err)
	}
	changedService := redistributionMembership{watchers: watchers, services: map[string]bool{"4273": true, "4302": true}}
	if _, err := judgeRedistribution(raw, admitted, admitted, changedService, "4288", "4311", 4); err == nil {
		t.Fatal("admitted a different service session with the same watchers")
	}
}
