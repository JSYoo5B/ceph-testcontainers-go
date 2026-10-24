//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"strings"
	"testing"
)

// These are Docker-free controls of the eligible-fragment model. Every report
// below is synthetic: it follows the field names and string formats that the
// pinned Tentacle dump code writes (PeeringState::dump_peering_state,
// pg_info_t::dump, pg_log_entry_t::dump, PrimaryLogPG::do_command), and none
// of it was recorded from a running cluster. They prove the join and its
// rejections, not what a cluster reports.

const (
	ecFixtureWriter = 4100
	ecFixtureSnap   = 3
	ecFixtureHead   = "2:5e0f5bc0:::eligible:head"
	ecFixtureClone  = "2:5e0f5bc0:::eligible:3"
)

// ecFixtureLog is a PG log of one object: the first write at 12'7, then a
// write under snapshot 3 by a second client incarnation, which logs the clone
// at 14'9 and the head modify at 14'10.
func ecFixtureLog() string {
	return `{"pg_log_t": {"head": "14'10", "tail": "0'0", "log": [` +
		`{"op": "modify", "object": "` + ecFixtureHead + `", "version": "12'7", "prior_version": "0'0", "reqid": "client.4100.0:1"},` +
		`{"op": "clone", "object": "` + ecFixtureClone + `", "version": "14'9", "prior_version": "12'7", "reqid": "unknown.0:0"},` +
		`{"op": "modify", "object": "` + ecFixtureHead + `", "version": "14'10", "prior_version": "12'7", "reqid": "client.4100.0:3"}` +
		`], "dups": []}}`
}

type ecFixtureShard struct {
	osd, shard           int
	update, complete, lb string
}

func ecFixtureInfo(s ecFixtureShard) string {
	return fmt.Sprintf(`"pgid": "2.0s%d", "shard": "%d", "last_update": "%s", "last_complete": "%s", "log_tail": "0'0", "last_backfill": "%s", `+
		`"partial_writes_last_complete": [], "stats": {"stat_sum": {"num_objects_recovered": 0}}`, s.shard, s.shard, s.update, s.complete, s.lb)
}

// ecFixtureQuery is a "pg query" whose first shard is the primary.
func ecFixtureQuery(state string, epoch int, acting []int, shards ...ecFixtureShard) string {
	var peers []string
	for _, s := range shards[1:] {
		peers = append(peers, fmt.Sprintf(`{"peer": "%d(%d)", %s}`, s.osd, s.shard, ecFixtureInfo(s)))
	}
	text := func(values []int) string {
		var out []string
		for _, v := range values {
			out = append(out, fmt.Sprint(v))
		}
		return strings.Join(out, ", ")
	}
	return fmt.Sprintf(`{"snap_trimq": "[]", "state": "%s", "epoch": %d, "up": [%s], "acting": [%s], "info": {%s}, "peer_info": [%s], "recovery_state": []}`,
		state, epoch, text(acting), text(acting), ecFixtureInfo(shards[0]), strings.Join(peers, ", "))
}

func ecFixtureComplete(osd, shard int) ecFixtureShard {
	return ecFixtureShard{osd, shard, "14'10", "14'10", "MAX"}
}

const (
	ecFixtureNoUnfound   = `{"num_missing": 0, "num_unfound": 0, "objects": [], "more": false}`
	ecFixtureMissingOnly = `{"num_missing": 2, "num_unfound": 0, "objects": [], "more": false}`
)

func ecFixtureObservation(t *testing.T, primary int, before, after, log, unfound string) ecObservation {
	t.Helper()
	obs := ecObservation{Primary: primary}
	var err error
	if obs.Before, err = parseECPGReport([]byte(before)); err != nil {
		t.Fatal(err)
	}
	if obs.After, err = parseECPGReport([]byte(after)); err != nil {
		t.Fatal(err)
	}
	if obs.Log, err = parseECPGLog([]byte(log)); err != nil {
		t.Fatal(err)
	}
	if obs.Unfound, err = parseECUnfound([]byte(unfound)); err != nil {
		t.Fatal(err)
	}
	return obs
}

// ecFixtureClassify classifies the head and the clone of the fixture object.
func ecFixtureClassify(t *testing.T, obs ecObservation, k int) (head, clone ecClassification) {
	t.Helper()
	headTarget, cloneTarget := ecTarget{Name: "eligible"}, ecTarget{Name: "eligible", Snap: ecFixtureSnap}
	need, err := ecRequiredVersion(obs.Log, headTarget, ecFixtureWriter)
	head = classifyECObject(obs, k, headTarget, need, err)
	need, err = ecRequiredVersion(obs.Log, cloneTarget, ecFixtureWriter)
	clone = classifyECObject(obs, k, cloneTarget, need, err)
	return head, clone
}

func ecWant(t *testing.T, c ecClassification, verdict ecVerdict, status string, indices ...int) {
	t.Helper()
	got := c.indices(status)
	if c.Verdict != verdict || fmt.Sprint(got) != fmt.Sprint(indices) {
		t.Errorf("%v: verdict %s with %s %v (%s), want %s with %v\n%+v", c.Target, c.Verdict, status, got, c.Detail, verdict, indices, c.Fragments)
	}
}

func TestECEligibleFragmentVerdicts(t *testing.T) {
	// Three OSDs, fragment i on acting[i]; osd.1 is the primary.
	clean := []int{1, 2, 0}
	query := func(state string, epoch int, acting []int, shards ...ecFixtureShard) string {
		return ecFixtureQuery(state, epoch, acting, shards...)
	}
	observe := func(t *testing.T, state string, acting []int, unfound string, shards ...ecFixtureShard) ecObservation {
		q := query(state, 31, acting, shards...)
		return ecFixtureObservation(t, 1, q, q, ecFixtureLog(), unfound)
	}
	t.Run("clean", func(t *testing.T) {
		head, clone := ecFixtureClassify(t, observe(t, "active+clean", clean, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)), 2)
		ecWant(t, head, ecSufficient, ecEligible, 0, 1, 2)
		ecWant(t, clone, ecSufficient, ecEligible, 0, 1, 2)
		if head.Need != (eversion{14, 10}) || clone.Need != (eversion{14, 9}) {
			t.Errorf("required versions: head %v, clone %v", head.Need, clone.Need)
		}
	})
	t.Run("missing fragment is not observed", func(t *testing.T) {
		head, clone := ecFixtureClassify(t, observe(t, "active+undersized+degraded", []int{1, ecFragmentNone, 0}, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureComplete(0, 2)), 2)
		for _, c := range []ecClassification{head, clone} {
			ecWant(t, c, ecSufficient, ecEligible, 0, 2)
			ecWant(t, c, ecSufficient, ecUnobserved, 1)
		}
	})
	t.Run("lagging fragment with an empty unfound list", func(t *testing.T) {
		lagging := ecFixtureShard{2, 1, "14'10", "12'7", "MAX"}
		head, clone := ecFixtureClassify(t, observe(t, "active+recovery_wait+degraded", clean, ecFixtureMissingOnly,
			ecFixtureComplete(1, 0), lagging, ecFixtureComplete(0, 2)), 2)
		// Two missing objects and an empty, complete list: absence from the list
		// is neither "unfound" nor a lack of eligible fragments.
		for _, c := range []ecClassification{head, clone} {
			ecWant(t, c, ecSufficient, ecEligible, 0, 2)
			ecWant(t, c, ecSufficient, ecBehind, 1)
		}
	})
	t.Run("a clean PG outranks a stale peer last_complete", func(t *testing.T) {
		// Shaped after what a recovered PG reported natively: active+clean
		// while the primary still held the peer's pre-failure last_complete.
		stale := ecFixtureShard{2, 1, "14'10", "12'7", "MAX"}
		head, _ := ecFixtureClassify(t, observe(t, "active+clean", clean, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), stale, ecFixtureComplete(0, 2)), 2)
		ecWant(t, head, ecSufficient, ecEligible, 0, 1, 2)
		head, _ = ecFixtureClassify(t, observe(t, "active+clean+scrubbing", clean, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureShard{2, 1, "12'7", "12'7", "MAX"}, ecFixtureComplete(0, 2)), 2)
		ecWant(t, head, ecSufficient, ecBehind, 1)
	})
	t.Run("each snapshot has its own required version", func(t *testing.T) {
		// The restored fragment is complete through the clone but not the head.
		between := ecFixtureShard{2, 1, "14'10", "14'9", "MAX"}
		head, clone := ecFixtureClassify(t, observe(t, "active+recovery_wait+degraded", clean, ecFixtureMissingOnly,
			ecFixtureComplete(1, 0), between, ecFixtureComplete(0, 2)), 2)
		ecWant(t, head, ecSufficient, ecEligible, 0, 2)
		ecWant(t, clone, ecSufficient, ecEligible, 0, 1, 2)
	})
	t.Run("a stray copy outside the acting set supplies nothing", func(t *testing.T) {
		lagging := ecFixtureShard{2, 1, "14'10", "12'7", "MAX"}
		head, _ := ecFixtureClassify(t, observe(t, "active+recovery_wait+degraded", clean, ecFixtureMissingOnly,
			ecFixtureComplete(1, 0), lagging, ecFixtureComplete(0, 2), ecFixtureComplete(3, 1)), 2)
		ecWant(t, head, ecSufficient, ecEligible, 0, 2)
		ecWant(t, head, ecSufficient, ecBehind, 1)
	})
	t.Run("duplicate locations of one index count once", func(t *testing.T) {
		acting := []int{1, 2, 3, 4, 5, 6}
		head, _ := ecFixtureClassify(t, observe(t, "active+undersized+degraded", acting, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(2, 1), ecFixtureComplete(3, 2)), 4)
		ecWant(t, head, ecInsufficient, ecEligible, 0, 1, 2)
	})
	t.Run("too few fragments", func(t *testing.T) {
		head, _ := ecFixtureClassify(t, observe(t, "active+undersized+degraded", []int{1, ecFragmentNone, ecFragmentNone}, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0)), 2)
		ecWant(t, head, ecInsufficient, ecEligible, 0)
	})
	t.Run("lagging fragments leave the result undecided", func(t *testing.T) {
		lagging := ecFixtureShard{2, 1, "14'10", "12'7", "MAX"}
		head, _ := ecFixtureClassify(t, observe(t, "active+recovery_wait+degraded", []int{1, 2, ecFragmentNone}, ecFixtureMissingOnly,
			ecFixtureComplete(1, 0), lagging), 2)
		if head.Verdict != ecIncomplete {
			t.Errorf("verdict %s (%s)", head.Verdict, head.Detail)
		}
	})
	t.Run("a finite backfill cursor is not a verdict", func(t *testing.T) {
		// Pinned MissingLoc rejects a source only when the object is at or
		// beyond the cursor; the model does not compare them.
		acting := []int{1, 2, ecFragmentNone}
		cursor := ecFixtureShard{2, 1, "14'10", "14'10", "1:2e0f5bc0:::zzz:head"}
		head, _ := ecFixtureClassify(t, observe(t, "active+undersized+degraded+remapped+backfilling", acting, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), cursor), 2)
		if head.Verdict != ecIncomplete || len(head.indices(ecEligible)) != 1 || len(head.indices(ecCursor)) != 1 {
			t.Errorf("finite cursor: verdict %s (%s) %+v", head.Verdict, head.Detail, head.Fragments)
		}
		head, _ = ecFixtureClassify(t, observe(t, "active+undersized+degraded", acting, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1)), 2)
		ecWant(t, head, ecSufficient, ecEligible, 0, 1)
		// MIN excludes the fragment for certain.
		head, _ = ecFixtureClassify(t, observe(t, "active+undersized+degraded+remapped+backfilling", acting, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureShard{2, 1, "14'10", "14'10", "MIN"}), 2)
		ecWant(t, head, ecInsufficient, ecUnbackfilled, 1)
	})
	t.Run("an unbackfilled fragment is not eligible", func(t *testing.T) {
		backfill := ecFixtureShard{2, 1, "14'10", "14'10", "MIN"}
		head, _ := ecFixtureClassify(t, observe(t, "active+remapped+backfill_wait", clean, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), backfill, ecFixtureComplete(0, 2)), 2)
		ecWant(t, head, ecSufficient, ecUnbackfilled, 1)
		ecWant(t, head, ecSufficient, ecEligible, 0, 2)
	})
	t.Run("unfound is per object", func(t *testing.T) {
		lagging := ecFixtureShard{2, 1, "14'10", "12'7", "MAX"}
		listed := `{"num_missing": 2, "num_unfound": 1, "objects": [{"oid": {"oid": "eligible", "key": "", "snapid": -2, "hash": 1, "max": 0, "pool": 2, "namespace": ""}, "need": "14'10", "have": "12'7"}], "more": false}`
		head, clone := ecFixtureClassify(t, observe(t, "active+recovering+degraded", clean, listed,
			ecFixtureComplete(1, 0), lagging, ecFixtureComplete(0, 2)), 2)
		if head.Verdict != ecUnfoundObserved {
			t.Errorf("head verdict %s (%s)", head.Verdict, head.Detail)
		}
		ecWant(t, clone, ecSufficient, ecEligible, 0, 2)
	})
	t.Run("an unfound clone is not the head", func(t *testing.T) {
		listed := `{"num_missing": 1, "num_unfound": 1, "objects": [{"oid": {"oid": "eligible", "snapid": 3}}], "more": false}`
		head, clone := ecFixtureClassify(t, observe(t, "active+recovering+degraded", clean, listed,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)), 2)
		if clone.Verdict != ecUnfoundObserved || head.Verdict != ecSufficient {
			t.Errorf("clone %s, head %s", clone.Verdict, head.Verdict)
		}
	})
	t.Run("unfound counters must agree with the list", func(t *testing.T) {
		head, _ := ecFixtureClassify(t, observe(t, "active+recovering+degraded", clean, `{"num_missing": 2, "num_unfound": 2, "objects": [], "more": false}`,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)), 2)
		if head.Verdict != ecIncomplete {
			t.Errorf("verdict %s", head.Verdict)
		}
		head, _ = ecFixtureClassify(t, observe(t, "active+recovering+degraded", clean, `{"num_missing": 2, "num_unfound": 2, "objects": [], "more": true}`,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)), 2)
		if head.Verdict != ecSufficient || !strings.Contains(head.Detail, "absence is not claimed") {
			t.Errorf("a paged list: verdict %s (%s)", head.Verdict, head.Detail)
		}
	})
	t.Run("peering blocks every statement", func(t *testing.T) {
		for _, state := range []string{"peering", "down+peering", "remapped+peering", "incomplete", "stale+active+clean", "unknown", "creating+peering"} {
			head, _ := ecFixtureClassify(t, observe(t, state, clean, ecFixtureNoUnfound,
				ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)), 2)
			if head.Verdict != ecPeeringBlocked || len(head.Fragments) != 0 {
				t.Errorf("%s: verdict %s with %d fragments", state, head.Verdict, len(head.Fragments))
			}
		}
	})
	t.Run("a PG that moved is incomplete", func(t *testing.T) {
		shards := []ecFixtureShard{ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)}
		for name, after := range map[string]string{
			"epoch":  query("active+clean", 32, clean, shards...),
			"state":  query("active+clean+scrubbing", 31, clean, shards...),
			"acting": query("active+clean", 31, []int{1, 0, 2}, shards...),
		} {
			obs := ecFixtureObservation(t, 1, query("active+clean", 31, clean, shards...), after, ecFixtureLog(), ecFixtureNoUnfound)
			head, _ := ecFixtureClassify(t, obs, 2)
			if head.Verdict != ecIncomplete {
				t.Errorf("%s: verdict %s", name, head.Verdict)
			}
		}
	})
	t.Run("a shifted fragment vector is rejected", func(t *testing.T) {
		// osd.2 holds fragment 1 in the acting vector but reports fragment 2.
		head, _ := ecFixtureClassify(t, observe(t, "active+clean", clean, ecFixtureNoUnfound,
			ecFixtureComplete(1, 0), ecFixtureComplete(2, 2), ecFixtureComplete(0, 1)), 2)
		if head.Verdict != ecIncomplete {
			t.Errorf("verdict %s (%s)", head.Verdict, head.Detail)
		}
	})
	t.Run("the primary must sit at its own fragment", func(t *testing.T) {
		q := query("active+clean", 31, clean, ecFixtureComplete(2, 1), ecFixtureComplete(1, 0), ecFixtureComplete(0, 2))
		head, _ := ecFixtureClassify(t, ecFixtureObservation(t, 1, q, q, ecFixtureLog(), ecFixtureNoUnfound), 2)
		if head.Verdict != ecIncomplete {
			t.Errorf("verdict %s (%s)", head.Verdict, head.Detail)
		}
	})
	t.Run("partial-write positions are not described by last_complete", func(t *testing.T) {
		q := strings.Replace(query("active+clean", 31, clean, ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2)),
			`"partial_writes_last_complete": []`, `"partial_writes_last_complete": [{"id": 1, "from": "12'7", "to": "14'10"}]`, 1)
		head, _ := ecFixtureClassify(t, ecFixtureObservation(t, 1, q, q, ecFixtureLog(), ecFixtureNoUnfound), 2)
		if head.Verdict != ecIncomplete {
			t.Errorf("verdict %s (%s)", head.Verdict, head.Detail)
		}
	})
}

func TestECRequiredVersion(t *testing.T) {
	entries := func(items ...string) ecPGLog {
		log, err := parseECPGLog([]byte(`{"head": "14'10", "tail": "0'0", "log": [` + strings.Join(items, ",") + `]}`))
		if err != nil {
			t.Fatal(err)
		}
		return log
	}
	entry := func(op, object, version, reqid string) string {
		return fmt.Sprintf(`{"op": "%s", "object": "%s", "version": "%s", "prior_version": "0'0", "reqid": "%s"}`, op, object, version, reqid)
	}
	head, clone := ecTarget{Name: "eligible"}, ecTarget{Name: "eligible", Snap: 3}
	good := entries(entry("modify", ecFixtureHead, "12'7", "client.4100.0:1"), entry("clone", ecFixtureClone, "14'9", "unknown.0:0"),
		entry("modify", ecFixtureHead, "14'10", "client.4100.0:3"))
	for name, test := range map[string]struct {
		log    ecPGLog
		target ecTarget
		writer uint64
		want   eversion
		fails  string
	}{
		"head":               {good, head, 4100, eversion{14, 10}, ""},
		"clone":              {good, clone, 4100, eversion{14, 9}, ""},
		"another client":     {good, head, 4101, eversion{}, "not a modify by client.4101"},
		"clone of another":   {good, clone, 4101, eversion{}, "not a modify by client.4101"},
		"a different snap":   {good, ecTarget{Name: "eligible", Snap: 4}, 4100, eversion{}, "no entry"},
		"a different object": {good, ecTarget{Name: "other"}, 4100, eversion{}, "no entry"},
		"empty log":          {entries(), head, 4100, eversion{}, "no entry"},
		"head deleted": {entries(entry("modify", ecFixtureHead, "12'7", "client.4100.0:1"), entry("delete", ecFixtureHead, "14'9", "client.4100.0:2")),
			head, 4100, eversion{}, "not a modify"},
		"clone without the writer's modify": {entries(entry("modify", ecFixtureHead, "12'7", "client.4100.0:1"), entry("clone", ecFixtureClone, "14'9", "unknown.0:0")),
			clone, 4100, eversion{}, "last log entry"},
		"clone not adjacent": {entries(entry("clone", ecFixtureClone, "14'9", "unknown.0:0"), entry("modify", ecFixtureHead, "14'11", "client.4100.0:3")),
			clone, 4100, eversion{}, "not followed directly"},
		"clone is the head": {entries(entry("modify", ecFixtureClone, "14'9", "client.4100.0:3")), clone, 4100, eversion{}, "not a clone"},
	} {
		got, err := ecRequiredVersion(test.log, test.target, test.writer)
		if test.fails == "" && (err != nil || got != test.want) {
			t.Errorf("%s: %v, %v; want %v", name, got, err, test.want)
		}
		if test.fails != "" && (err == nil || !strings.Contains(err.Error(), test.fails)) {
			t.Errorf("%s: %v, %v; want an error with %q", name, got, err, test.fails)
		}
	}
	// A required version that cannot be derived is an incomplete observation,
	// never a verdict about the fragments.
	q := ecFixtureQuery("active+clean", 31, []int{1, 2, 0}, ecFixtureComplete(1, 0), ecFixtureComplete(2, 1), ecFixtureComplete(0, 2))
	obs := ecFixtureObservation(t, 1, q, q, ecFixtureLog(), ecFixtureNoUnfound)
	need, err := ecRequiredVersion(obs.Log, head, 9)
	if got := classifyECObject(obs, 2, head, need, err); got.Verdict != ecIncomplete {
		t.Errorf("unbridged writer: verdict %s", got.Verdict)
	}
}

func TestECObjectIDParsing(t *testing.T) {
	for text, want := range map[string]ecObjectID{
		"2:5e0f5bc0:::eligible:head": {Pool: 2, Hash: "5e0f5bc0", Name: "eligible", Snap: "head"},
		"2:5e0f5bc0:::eligible:1a":   {Pool: 2, Hash: "5e0f5bc0", Name: "eligible", Snap: "1a", hexSnap: 0x1a, hasSnapID: true},
		"2:00000001:ns:k%3a:a%3ab:3": {Pool: 2, Hash: "00000001", Nspace: "ns", Key: "k:", Name: "a:b", Snap: "3", hexSnap: 3, hasSnapID: true},
	} {
		got, err := parseECObjectID(text)
		if err != nil || got != want {
			t.Errorf("%s: %+v, %v; want %+v", text, got, err, want)
		}
	}
	for _, text := range []string{"MIN", "MAX", "2:5e0f5bc0:::eligible", "x:5e0f5bc0:::eligible:head", "2:5e0f5bc0:::eligible:zz", "2:5e0f5bc0:::a%3:head"} {
		if got, err := parseECObjectID(text); err == nil {
			t.Errorf("%s parsed as %+v", text, got)
		}
	}
	// 0x1a is the client's snapshot 26.
	if !(ecTarget{Name: "eligible", Snap: 26}).matches(ecObjectID{Name: "eligible", Snap: "1a", hexSnap: 0x1a, hasSnapID: true}) ||
		(ecTarget{Name: "eligible", Snap: 26}).matches(ecObjectID{Name: "eligible", Snap: "head"}) {
		t.Error("snapshot IDs are hexadecimal in the log and decimal for the client")
	}
}

func TestECReportParsing(t *testing.T) {
	for name, test := range map[string]func() error{
		"query without epoch": func() error {
			_, err := parseECPGReport([]byte(strings.Replace(ecFixtureQuery("active+clean", 31, []int{1}, ecFixtureComplete(1, 0)), `"epoch": 31,`, "", 1)))
			return err
		},
		"query without a shard": func() error {
			_, err := parseECPGReport([]byte(strings.Replace(ecFixtureQuery("active+clean", 31, []int{1}, ecFixtureComplete(1, 0)), `2.0s0`, `2.0`, 1)))
			return err
		},
		"query with a bad position": func() error {
			_, err := parseECPGReport([]byte(strings.Replace(ecFixtureQuery("active+clean", 31, []int{1}, ecFixtureComplete(1, 0)), `"14'10"`, `"14-10"`, 1)))
			return err
		},
		"log without a head": func() error { _, err := parseECPGLog([]byte(`{"log": []}`)); return err },
		"unfound without objects": func() error {
			_, err := parseECUnfound([]byte(`{"num_missing": 0, "num_unfound": 0}`))
			return err
		},
	} {
		if err := test(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// The log and the unfound report are accepted with or without the section
	// name that the dump code opens first.
	for _, text := range []string{
		`{"head": "1'2", "tail": "0'0", "log": []}`,
		`{"pg_log_t": {"head": "1'2", "tail": "0'0", "log": []}}`,
		`{"op_log": {"pg_log_t": {"head": "1'2", "tail": "0'0", "log": []}}}`,
	} {
		if log, err := parseECPGLog([]byte(text)); err != nil || log.Head != "1'2" {
			t.Errorf("%s: %+v, %v", text, log, err)
		}
	}
	for _, text := range []string{
		`{"num_missing": 3, "num_unfound": 0, "objects": [], "more": false}`,
		`{"missing": {"num_missing": 3, "num_unfound": 0, "objects": [], "more": false}}`,
	} {
		if report, err := parseECUnfound([]byte(text)); err != nil || report.NumMissing != 3 {
			t.Errorf("%s: %+v, %v", text, report, err)
		}
	}
}
