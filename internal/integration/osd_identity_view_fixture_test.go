//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// osdIdentityDocs renders "osd dump" and "osd tree" for three OSDs, with edit
// applied to each OSD's dump entry and CRUSH weight. extra adds unrelated map
// fields.
func osdIdentityDocs(epoch int, extra string, edit func(id int, entry map[string]any, crush *float64)) [2][]byte {
	var osds, nodes []string
	for id := range 3 {
		entry := map[string]any{"uuid": fmt.Sprintf("uuid-%d", id), "up": 1, "in": 1, "weight": 1.0, "primary_affinity": 1.0}
		crush := 0.0098
		if edit != nil {
			edit(id, entry, &crush)
		}
		if entry["uuid"] == nil {
			continue
		}
		osds = append(osds, fmt.Sprintf(`{"osd":%d,"uuid":%q,"up":%v,"in":%v,"weight":%v,"primary_affinity":%v,"last_clean_begin":%d}`,
			id, entry["uuid"], entry["up"], entry["in"], entry["weight"], entry["primary_affinity"], epoch))
		if crush >= 0 {
			nodes = append(nodes, fmt.Sprintf(`{"id":%d,"name":"osd.%d","type":"osd","crush_weight":%v,"reweight":%v}`, id, id, crush, entry["weight"]))
		}
	}
	nodes = append(nodes, `{"id":-1,"name":"default","type":"root","crush_weight":0.0294}`)
	dump := fmt.Sprintf(`{"epoch":%d,"osds":[%s]%s}`, epoch, strings.Join(osds, ","), extra)
	tree := fmt.Sprintf(`{"nodes":[%s],"stray":[]}`, strings.Join(nodes, ","))
	return [2][]byte{[]byte(dump), []byte(tree)}
}

func mustOSDIdentity(t *testing.T, docs [2][]byte) osdIdentityView {
	t.Helper()
	v, err := parseOSDIdentityView(docs[0], docs[1])
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestOSDIdentityChangesIgnoreUnrelatedMapEvolution(t *testing.T) {
	before := mustOSDIdentity(t, osdIdentityDocs(40, "", nil))
	// A later epoch with other per-OSD fields and a PG temp mapping changed:
	// the raw documents differ, the guarded identity does not.
	after := mustOSDIdentity(t, osdIdentityDocs(42, `,"pg_temp":[{"pgid":"1.0","osds":[0,2]}]`, nil))
	if changes := osdIdentityChanges(before, after); len(changes) != 0 {
		t.Fatalf("unrelated map evolution reported as a side effect: %v", changes)
	}
	if before.Epoch != 40 || after.Epoch != 42 {
		t.Fatalf("epochs %d -> %d", before.Epoch, after.Epoch)
	}
}

func TestOSDIdentityChangesCatchEveryGuardedSideEffect(t *testing.T) {
	before := mustOSDIdentity(t, osdIdentityDocs(40, "", nil))
	for name, test := range map[string]struct {
		edit func(id int, entry map[string]any, crush *float64)
		want string
	}{
		"removal":         {func(id int, e map[string]any, _ *float64) { osdDrop(id, e) }, "osd.1 removed"},
		"identity change": {func(id int, e map[string]any, _ *float64) { osdEdit(id, e, "uuid", "uuid-other") }, "osd.1 uuid uuid-1 -> uuid-other"},
		"out":             {func(id int, e map[string]any, _ *float64) { osdEdit(id, e, "in", 0) }, "osd.1 in 1 -> 0"},
		"down":            {func(id int, e map[string]any, _ *float64) { osdEdit(id, e, "up", 0) }, "osd.1 up 1 -> 0"},
		"reweight":        {func(id int, e map[string]any, _ *float64) { osdEdit(id, e, "weight", 0.5) }, "osd.1 reweight 1 -> 0.5"},
		"primary affinity": {func(id int, e map[string]any, _ *float64) { osdEdit(id, e, "primary_affinity", 0) },
			"osd.1 primary_affinity 1 -> 0"},
		"crush reweight": {func(id int, _ map[string]any, c *float64) {
			if id == 1 {
				*c = 0
			}
		}, "osd.1 crush_weight 0.0098 -> 0"},
		"crush removal": {func(id int, _ map[string]any, c *float64) {
			if id == 1 {
				*c = -1
			}
		}, "osd.1 in_crush true -> false"},
	} {
		changes := osdIdentityChanges(before, mustOSDIdentity(t, osdIdentityDocs(41, "", test.edit)))
		if len(changes) == 0 || !strings.Contains(strings.Join(changes, "; "), test.want) {
			t.Errorf("%s: changes %v, want %q", name, changes, test.want)
		}
	}
	added := mustOSDIdentity(t, osdIdentityDocs(41, "", nil))
	added.OSDs[3] = osdIdentityEntry{UUID: "uuid-3", Up: 1, In: 1, Weight: 1}
	if changes := osdIdentityChanges(before, added); len(changes) != 1 || changes[0] != "osd.3 added" {
		t.Errorf("an added OSD: %v", changes)
	}
}

func TestOSDRejectionSideEffectsReportEachDisjunct(t *testing.T) {
	if p := (osdRejectionSideEffects{EpochBefore: 40, EpochAfter: 43}).problems(); len(p) != 0 {
		t.Fatalf("an epoch move alone is a side effect: %v", p)
	}
	p := osdRejectionSideEffects{MapChanges: []string{"osd.1 in 1 -> 0"}, Mutations: 1, Stops: 1, Terminations: 2}.problems()
	if len(p) != 4 {
		t.Fatalf("each side effect must be reported on its own: %v", p)
	}
	for i, want := range []string{"OSD map or CRUSH changes", "1 native mutations", "1 container stops", "2 container terminations"} {
		if !strings.Contains(p[i], want) {
			t.Errorf("problem %d %q, want %q", i, p[i], want)
		}
	}
	if _, err := parseOSDIdentityView([]byte(`{"osds":[]}`), []byte(`{"nodes":[]}`)); err == nil {
		t.Error("a dump without an epoch was accepted")
	}
}

func osdEdit(id int, entry map[string]any, key string, value any) {
	if id == 1 {
		entry[key] = value
	}
}

func osdDrop(id int, entry map[string]any) {
	if id == 1 {
		delete(entry, "uuid")
	}
}

func TestOSDIdentityRejectsIncompleteForeignRejectionEvidence(t *testing.T) {
	// These replies retain the foreign UUID whenever the record exists, so the
	// scenario's UUID precondition alone cannot justify the unchanged verdict.
	const entry = `{"osd":1,"uuid":"foreign","up":0,"in":0,"weight":0,"primary_affinity":0}`
	const node = `{"id":1,"type":"osd","crush_weight":0}`
	valid := [2]string{`{"epoch":40,"osds":[` + entry + `]}`, `{"nodes":[` + node + `]}`}
	cases := map[string][2]string{}
	for _, field := range []string{"osd", "uuid", "up", "in", "weight", "primary_affinity"} {
		for _, mode := range []string{"missing", "null"} {
			var record map[string]any
			if err := json.Unmarshal([]byte(entry), &record); err != nil {
				t.Fatal(err)
			}
			delete(record, field)
			if mode == "null" {
				record[field] = nil
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			cases["dump/"+field+"/"+mode] = [2]string{`{"epoch":40,"osds":[` + string(encoded) + `]}`, valid[1]}
		}
	}
	for _, field := range []string{"id", "type", "crush_weight"} {
		for _, mode := range []string{"missing", "null"} {
			var record map[string]any
			if err := json.Unmarshal([]byte(node), &record); err != nil {
				t.Fatal(err)
			}
			delete(record, field)
			if mode == "null" {
				record[field] = nil
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			cases["tree/"+field+"/"+mode] = [2]string{valid[0], `{"nodes":[` + string(encoded) + `]}`}
		}
	}
	for name, dump := range map[string]string{
		"missing osds": `{"epoch":40}`, "null osds": `{"epoch":40,"osds":null}`,
		"null dump": `null`, "missing epoch": `{"osds":[]}`, "null epoch": `{"epoch":null,"osds":[]}`,
		"duplicate ID": `{"epoch":40,"osds":[` + entry + `,` + entry + `]}`,
	} {
		cases["dump/"+name] = [2]string{dump, valid[1]}
	}
	for name, tree := range map[string]string{
		"missing nodes": `{}`, "null nodes": `{"nodes":null}`, "null tree": `null`,
		"duplicate ID":     `{"nodes":[` + node + `,` + node + `]}`,
		"unregistered OSD": `{"nodes":[{"id":2,"type":"osd","crush_weight":0}]}`,
		"OSD ID as bucket": `{"nodes":[{"id":1,"type":"host","crush_weight":0}]}`,
		"bucket ID as OSD": `{"nodes":[{"id":-1,"type":"osd","crush_weight":0}]}`,
	} {
		cases["tree/"+name] = [2]string{valid[0], tree}
	}
	for name, docs := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseOSDIdentityView([]byte(docs[0]), []byte(docs[1])); err == nil {
				t.Error("incomplete identity evidence decoded without an error")
			}
			for _, observations := range [][2][2]string{{docs, valid}, {valid, docs}, {docs, docs}} {
				if err := foreignRejectionVerdict(observations[0], observations[1]); err == nil {
					t.Fatal("incomplete observations reached the unchanged foreign-OSD verdict")
				}
			}
		})
	}
	if err := foreignRejectionVerdict(valid, valid); err != nil {
		t.Fatalf("explicit zero fields rejected: %v", err)
	}
	valid[1] = `{"nodes":[],"stray":[{"id":1}]}`
	if err := foreignRejectionVerdict(valid, valid); err != nil {
		t.Fatalf("registered OSD absent from CRUSH rejected: %v", err)
	}
	if _, err := parseOSDIdentityView([]byte(`{"epoch":0,"osds":[]}`), []byte(`{"nodes":[]}`)); err != nil {
		t.Fatalf("explicit empty observations rejected: %v", err)
	}
}

// foreignRejectionVerdict follows the removal scenario's observation, foreign
// UUID precondition and unchanged-state verdict with supplied before/after
// replies. The independent call counters are zero for this pure oracle test.
func foreignRejectionVerdict(beforeDocs, afterDocs [2]string) error {
	before, err := parseOSDIdentityView([]byte(beforeDocs[0]), []byte(beforeDocs[1]))
	if err != nil {
		return err
	}
	if before.OSDs[1].UUID != "foreign" {
		return fmt.Errorf("foreign UUID was not observed")
	}
	after, err := parseOSDIdentityView([]byte(afterDocs[0]), []byte(afterDocs[1]))
	if err != nil {
		return err
	}
	problems := (osdRejectionSideEffects{MapChanges: osdIdentityChanges(before, after), EpochBefore: before.Epoch, EpochAfter: after.Epoch}).problems()
	if len(problems) != 0 {
		return fmt.Errorf("rejection side effects: %v", problems)
	}
	return nil
}
