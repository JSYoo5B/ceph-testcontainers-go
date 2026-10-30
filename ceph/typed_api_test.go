package ceph

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

func TestOSDFlagsReportsFlagsOutsideTheConstantSet(t *testing.T) {
	ctr := &osdPolicyFixture{flags: []string{"sortbitwise", "noout", "purged_snapdirs", "pglog_hardlimit"}}
	c := osdPolicyCluster(ctr)
	flags, err := (&Observer{target: c}).OSDFlags(t.Context())
	want := []OSDFlag{"noout", "pglog_hardlimit", "purged_snapdirs", "sortbitwise"}
	if err != nil || !slices.Equal(flags, want) {
		t.Fatalf("flags %q, want %q: %v", flags, want, err)
	}
	if !slices.Contains(flags, OSDFlagNoOut) {
		t.Fatal("a reported flag does not compare equal to its constant")
	}
	flags[0] = "changed"
	again, err := (&Observer{target: c}).OSDFlags(t.Context())
	if err != nil || !slices.Equal(again, want) {
		t.Fatalf("returned flags alias cluster state: %q %v", again, err)
	}
	empty := osdPolicyCluster(&osdPolicyFixture{})
	if flags, err := (&Observer{target: empty}).OSDFlags(t.Context()); err != nil || len(flags) != 0 {
		t.Fatalf("no flags: %q %v", flags, err)
	}
	failing := osdPolicyCluster(&osdPolicyFixture{raw: `null`})
	if flags, err := (&Observer{target: failing}).OSDFlags(t.Context()); err == nil || flags != nil {
		t.Fatalf("malformed dump returned flags %q with err %v", flags, err)
	}
}

func TestParsePGIDAcceptsExactlyWhatValidatePGIDAccepts(t *testing.T) {
	for _, s := range []string{
		"1.0", "7.0", "3.1f", "12.ab", "0.0", "10.ffff", "1.00",
		"", "1", "1.", ".1", "a.1", "1.G", "1.1F", "1.0\n", " 1.0", "1.0 ", "1.0; rm", "-1.0", "1.0.1", "1..0", "1.-1", "１.0",
	} {
		want := validatePGID(s)
		got, err := ParsePGID(s)
		if (err == nil) != (want == nil) {
			t.Fatalf("%q: ParsePGID err %v, validatePGID err %v", s, err, want)
		}
		if err != nil {
			if got != "" || !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("%q: rejected with %q and %v", s, got, err)
			}
			continue
		}
		if string(got) != s || got.String() != s {
			t.Fatalf("%q parsed as %q", s, got)
		}
	}
	for _, s := range []string{"1.0", "3.1f"} {
		if _, err := ParsePGID(s); err != nil {
			t.Fatalf("grammar example %q refused: %v", s, err)
		}
	}
	for _, s := range []string{"", "1.", "1.1F", "1.0\n"} {
		if _, err := ParsePGID(s); err == nil {
			t.Fatalf("invalid example %q accepted", s)
		}
	}
}

func TestScrubOperationsRefuseAnInvalidTypedPGIDWithoutANativeCall(t *testing.T) {
	for _, pgid := range []PGID{"", "7", "7.0; rm", "7.G", "PG"} {
		ctr := scrubFixture()
		c := poolFixtureCluster(ctr, 2)
		if err := c.DeepScrubPG(t.Context(), pgid); err == nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("DeepScrubPG(%q): %v", pgid, err)
		}
		if err := c.RepairPG(t.Context(), pgid); err == nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("RepairPG(%q): %v", pgid, err)
		}
		if objects, err := (&Observer{target: c}).PGInconsistencies(t.Context(), pgid); err == nil || objects != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("PGInconsistencies(%q): %v %v", pgid, objects, err)
		}
		if len(ctr.Calls) != 0 {
			t.Fatalf("invalid PG %q reached native CLI: %v", pgid, ctr.Calls)
		}
	}
}

func TestReportedPGIDsParseAsPGIDs(t *testing.T) {
	ready, pgs, err := decodePoolPGReports([]byte(`{"pg_ready":true,"pg_stats":[`+
		`{"pgid":"7.1f","state":"active+clean","up":[0],"acting":[0],"up_primary":0,"acting_primary":0,"reported_epoch":1,"reported_seq":1,"mapping_epoch":1,"last_epoch_clean":1,"stats_invalid":false,`+
		`"stat_sum":{"num_objects":0,"num_bytes":0,"num_object_copies":0,"num_objects_degraded":0,"num_objects_misplaced":0,"num_objects_unfound":0}}]}`), 7)
	if err != nil || !ready || len(pgs) != 1 {
		t.Fatalf("decode: %v %v %v", ready, pgs, err)
	}
	parsed, err := ParsePGID(pgs[0].PGID.String())
	if err != nil || parsed != pgs[0].PGID || parsed != "7.1f" {
		t.Fatalf("reported PG %q does not round-trip: %q %v", pgs[0].PGID, parsed, err)
	}
}

func TestConnectionConfigNamesTheFilesAndReturnsTheZeroValueOnError(t *testing.T) {
	cluster := &Container{bootstrap: bootstrapRecord{config: []byte("conf-bytes"), keyring: []byte("keyring-bytes")}}
	cfg, err := cluster.ConnectionConfig(t.Context())
	if err != nil || string(cfg.Conf) != "conf-bytes" || string(cfg.Keyring) != "keyring-bytes" {
		t.Fatalf("container fields swapped or missing: %q %q %v", cfg.Conf, cfg.Keyring, err)
	}
	cfg.Conf[0], cfg.Keyring[0] = 'X', 'X'
	again, err := cluster.ConnectionConfig(t.Context())
	if err != nil || string(again.Conf) != "conf-bytes" || string(again.Keyring) != "keyring-bytes" {
		t.Fatalf("container copies alias cluster state: %q %q %v", again.Conf, again.Keyring, err)
	}
	for name, broken := range map[string]*Container{
		"terminated": {bootstrap: bootstrapRecord{config: []byte("c"), keyring: []byte("k")}, closed: true},
		"no config":  {bootstrap: bootstrapRecord{keyring: []byte("k")}},
		"no keyring": {bootstrap: bootstrapRecord{config: []byte("c")}},
	} {
		got, err := broken.ConnectionConfig(t.Context())
		if err == nil || got.Conf != nil || got.Keyring != nil {
			t.Fatalf("%s: returned %+v with err %v", name, got, err)
		}
	}

	client := &ClientConfig{name: "client.app", ready: true, config: []byte("client-conf"), keyring: []byte("client-key")}
	got, err := client.ConnectionConfig()
	if err != nil || !bytes.Equal(got.Conf, []byte("client-conf")) || !bytes.Equal(got.Keyring, []byte("client-key")) {
		t.Fatalf("client fields swapped or missing: %q %q %v", got.Conf, got.Keyring, err)
	}
	got.Conf[0], got.Keyring[0] = 'X', 'X'
	if string(client.config) != "client-conf" || string(client.keyring) != "client-key" {
		t.Fatal("client copies alias descriptor state")
	}
	var nilClient *ClientConfig
	for name, unavailable := range map[string]*ClientConfig{
		"nil":        nilClient,
		"not ready":  {config: []byte("c"), keyring: []byte("k")},
		"no config":  {ready: true, keyring: []byte("k")},
		"no keyring": {ready: true, config: []byte("c")},
	} {
		got, err := unavailable.ConnectionConfig()
		if err == nil || got.Conf != nil || got.Keyring != nil {
			t.Fatalf("%s: returned %+v with err %v", name, got, err)
		}
	}
}
