//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// The entries below are synthetic. Their descriptions have the shape of the
// MOSDOp descriptions that a Tentacle 20.2.4 OSD records for a rados cp with
// osd_copyfrom_max_chunk lowered to 32768: the client's copy-from on the
// destination and the OSD's own copy-get reads of the source, the second and
// later ones behind an assert-version. Request ids and hashes are placeholders.
const (
	copyEvidenceSource      = "copy-src"
	copyEvidenceDestination = "copy-chunked"
	copyEvidenceChunk       = 32768
	copyEvidenceWant        = 3
)

type syntheticOp struct {
	description string
	initiated   string
	duration    float64
}

func copyFromOp(reqid, object, initiated string, duration float64) syntheticOp {
	return syntheticOp{fmt.Sprintf("osd_op(%s 2.7 2:e1a63049:::%s:head [copy-from ver 0] snapc 0=[] ondisk+write+known_if_redirected+supports_pool_eio e12)", reqid, object), initiated, duration}
}

func copyGetOp(reqid, object, ops, initiated string) syntheticOp {
	return syntheticOp{fmt.Sprintf("osd_op(%s 2.3 2:cebe0020:::%s:head [%s] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e12)", reqid, object, ops), initiated, 0.0002}
}

const (
	copyFromAt = "2026-11-06T02:08:24.421401+0000"
	chunk1At   = "2026-11-06T02:08:24.421822+0000"
	chunk2At   = "2026-11-06T02:08:24.429798+0000"
	chunk3At   = "2026-11-06T02:08:24.430227+0000"
	beforeAt   = "2026-11-06T02:08:20.000000+0000"
	afterAt    = "2026-11-06T02:08:30.000000+0000"
)

var (
	syntheticCopyFrom = copyFromOp("client.4172.0:1", copyEvidenceDestination, copyFromAt, 0.019388)
	syntheticChunks   = []syntheticOp{
		copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 32768", chunk1At),
		copyGetOp("osd.0.6:2", copyEvidenceSource, "assert-version v1,copy-get max 32768", chunk2At),
		copyGetOp("osd.0.6:3", copyEvidenceSource, "assert-version v1,copy-get max 32768", chunk3At),
	}
	syntheticNoise = syntheticOp{"osd_op(client.4112.0:20 1.0 1:30306672:devicehealth::main.db.0000000000000000:head [write 0~65536] snapc 0=[] ondisk+write+known_if_redirected+supports_pool_eio e9)", beforeAt, 0.0019}
)

func syntheticDump(t *testing.T, ops ...syntheticOp) []byte {
	t.Helper()
	type entry struct {
		Description string  `json:"description"`
		Initiated   string  `json:"initiated_at"`
		Age         float64 `json:"age"`
		Duration    float64 `json:"duration"`
		TypeData    struct {
			FlagPoint string `json:"flag_point"`
		} `json:"type_data"`
	}
	doc := struct {
		Size     int     `json:"size"`
		Duration int     `json:"duration"`
		Ops      []entry `json:"ops"`
	}{Size: 1000, Duration: 600, Ops: []entry{}}
	for _, op := range ops {
		e := entry{Description: op.description, Initiated: op.initiated, Age: 1, Duration: op.duration}
		e.TypeData.FlagPoint = "started"
		doc.Ops = append(doc.Ops, e)
	}
	data, err := json.MarshalIndent(doc, "", "    ")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func attributeSynthetic(t *testing.T, ops ...syntheticOp) (copyGetEvidence, error) {
	t.Helper()
	dump, err := parseHistoricOps(syntheticDump(t, ops...))
	if err != nil {
		t.Fatal(err)
	}
	return attributeCopyGets(dump, copyEvidenceSource, copyEvidenceDestination, copyEvidenceChunk, copyEvidenceWant)
}

func TestCopyGetEvidenceAttributesTheCopy(t *testing.T) {
	ops := append([]syntheticOp{syntheticNoise, syntheticCopyFrom}, syntheticChunks...)
	evidence, err := attributeSynthetic(t, ops...)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.CopyFrom == nil || evidence.CopyFrom.ReqID != "client.4172.0:1" || len(evidence.Chunks) != 3 {
		t.Fatalf("evidence %s", evidence)
	}
	if h := evidence.History; h.Total != 5 || h.Parsed != 5 || h.Undecoded != 0 || h.Opaque != 0 {
		t.Fatalf("parse counts %+v", h)
	}
	if len(evidence.Relevant) != 4 {
		t.Fatalf("relevant entries %+v", evidence.Relevant)
	}
}

// Every case below holds at least three "copy-get max 32768" substrings, so
// the daemon-wide count that the test used to make accepts each of them; none
// of them has three chunks of the copy itself.
func TestCopyGetEvidenceRefusesUnrelatedEntries(t *testing.T) {
	foreign := []syntheticOp{
		copyGetOp("osd.0.6:7", "other-src", "copy-get max 32768", chunk1At),
		copyGetOp("osd.0.6:8", "other-src", "assert-version v1,copy-get max 32768", chunk2At),
		copyGetOp("osd.0.6:9", "other-src", "assert-version v1,copy-get max 32768", chunk3At),
	}
	cases := []struct {
		name string
		ops  []syntheticOp
		want string
	}{
		{"chunks without the copy-from", append([]syntheticOp{syntheticNoise}, syntheticChunks...), "no copy-from on copy-chunked"},
		{"another copy's copy-from", append([]syntheticOp{copyFromOp("client.4180.0:1", "copy-restored", copyFromAt, 0.02)}, syntheticChunks...), "no copy-from on copy-chunked"},
		{"chunks of another object", append([]syntheticOp{syntheticCopyFrom}, foreign...), "has 0 copy-get chunks"},
		{"chunks outside the copy", []syntheticOp{
			copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 32768", beforeAt),
			syntheticCopyFrom,
			copyGetOp("osd.0.6:2", copyEvidenceSource, "copy-get max 32768", afterAt),
			copyGetOp("osd.0.6:3", copyEvidenceSource, "copy-get max 32768", afterAt),
		}, "has 0 copy-get chunks"},
		{"client copy-gets", []syntheticOp{
			syntheticCopyFrom,
			copyGetOp("client.4190.0:1", copyEvidenceSource, "copy-get max 32768", chunk1At),
			copyGetOp("client.4190.0:2", copyEvidenceSource, "copy-get max 32768", chunk2At),
			copyGetOp("client.4190.0:3", copyEvidenceSource, "copy-get max 32768", chunk3At),
		}, "has 0 copy-get chunks"},
		{"one chunk repeated", []syntheticOp{
			syntheticCopyFrom,
			copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 32768", chunk1At),
			copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 32768", chunk2At),
			copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 32768", chunk3At),
		}, "has 1 copy-get chunks"},
		{"two copy-froms on the destination", append([]syntheticOp{syntheticCopyFrom, copyFromOp("client.4173.0:1", copyEvidenceDestination, copyFromAt, 0.019388)}, syntheticChunks...), "2 copy-from requests"},
		{"undecoded entries during the copy", []syntheticOp{
			syntheticCopyFrom,
			{"osd_op(osd.0.6:1 2.3 2.4007d73 (undecoded) ondisk+read+known_if_redirected+supports_pool_eio e12)", chunk1At, 0.0002},
			{"osd_op(osd.0.6:2 2.3 2.4007d73 (undecoded) ondisk+read+known_if_redirected+supports_pool_eio e12)", chunk2At, 0.0002},
			copyGetOp("osd.0.6:3", copyEvidenceSource, "assert-version v1,copy-get max 32768", chunk3At),
			copyGetOp("osd.0.6:4", "other-src", "copy-get max 32768", chunk3At),
			copyGetOp("osd.0.6:5", "other-src", "copy-get max 32768", chunk3At),
		}, "(2 undecoded entries during it)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if raw := syntheticDump(t, c.ops...); strings.Count(string(raw), "copy-get max 32768") < copyEvidenceWant {
				t.Fatalf("the case does not satisfy the daemon-wide count it is meant to defeat")
			}
			evidence, err := attributeSynthetic(t, c.ops...)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error with %q: %s", err, c.want, evidence)
			}
		})
	}
}

func TestCopyGetEvidenceRefusesAnotherChunkSize(t *testing.T) {
	_, err := attributeSynthetic(t, syntheticCopyFrom, copyGetOp("osd.0.6:1", copyEvidenceSource, "copy-get max 8388608", chunk1At))
	if err == nil || !strings.Contains(err.Error(), `reads "copy-get max 8388608"`) {
		t.Fatalf("got %v", err)
	}
}

func TestParseHistoricOpsRefusesIncompleteDumps(t *testing.T) {
	whole := syntheticDump(t, syntheticCopyFrom)
	cases := map[string][]byte{
		"truncated":      whole[:len(whole)/2],
		"trailing data":  append(append([]byte{}, whole...), []byte("{}")...),
		"no ops":         []byte(`{"size": 1000, "duration": 600}`),
		"no description": []byte(`{"size": 1000, "duration": 600, "ops": [{"initiated_at": "2026-11-06T02:08:24.421401+0000"}]}`),
		"bad time":       []byte(`{"size": 1000, "duration": 600, "ops": [{"description": "osd_op(x)", "initiated_at": "yesterday"}]}`),
	}
	for name, raw := range cases {
		if _, err := parseHistoricOps(raw); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
	dump, err := parseHistoricOps([]byte(`{"size": 1000, "duration": 600, "ops": [{"description": "osd_repop(client.1.0:1 2.3 e12/10)", "initiated_at": "2026-11-06T02:08:24.421401+0000"}]}`))
	if err != nil || dump.Total != 1 || dump.Opaque != 1 || dump.Parsed != 0 {
		t.Fatalf("an opaque entry: %+v %v", dump, err)
	}
}
