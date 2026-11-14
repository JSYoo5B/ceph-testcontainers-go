//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_slo_head_segment_reads_test

import (
	"reflect"
	"strings"
	"testing"
)

// The descriptions below have the form the OSD prints for a client
// operation. They are synthetic inputs of the readers, not native evidence
// of what a gateway sends.
const (
	opManifest = `osd_op(client.4567.0:10 7.3 7:c1a2b3d4:::m.1_slo:head [call version.read in=11b,getxattrs,stat] snapc 0=[] ondisk+read+known_if_redirected e45)`
	opSegment  = `osd_op(client.4567.0:11 7.1 7:81a2b3d4:::s.2_segment-0000:head [call version.read in=11b,getxattrs,stat,read 0~3145728] snapc 0=[] ondisk+read+known_if_redirected e45)`
	opMarker   = `osd_op(client.9000.0:1 7.0 7:01a2b3d4:::tc-slo-window-1:head [create] snapc 0=[] ondisk+write+known_if_redirected e45)`
	opIndex    = `osd_op(client.4567.0:12 6.2 6:41a2b3d4:::.dir.s.2.0:head [call rgw.bucket_list in=40b] snapc 0=[] ondisk+read+known_if_redirected e45)`
)

func historyReport(size string, descriptions ...string) []byte {
	entries := make([]string, len(descriptions))
	for i, description := range descriptions {
		entries[i] = `{"description":"` + description + `","initiated_at":"2026-01-01T00:00:00.000000+0000"}`
	}
	return []byte(`{"size":` + size + `,"duration":3600,"ops":[` + strings.Join(entries, ",") + `]}`)
}

func TestParseOpHistoryReadsClientOperations(t *testing.T) {
	got, err := parseOpHistory(historyReport("20000", opManifest, "osd_repop(client.1.0:2 7.3 e45/40)", opSegment))
	if err != nil {
		t.Fatal(err)
	}
	want := opHistory{Size: 20000, Total: 3, Ops: []osdOp{
		{ReqID: "client.4567.0:10", Pool: 7, Object: "m.1_slo", Subs: []string{"call version.read in=11b", "getxattrs", "stat"}},
		{ReqID: "client.4567.0:11", Pool: 7, Object: "s.2_segment-0000", Subs: []string{"call version.read in=11b", "getxattrs", "stat", "read 0~3145728"}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("history = %+v, want %+v", got, want)
	}
	// An empty history is an observation.
	if got, err := parseOpHistory([]byte(`{"size":20,"duration":600,"ops":[]}`)); err != nil || got.Total != 0 || got.Size != 20 {
		t.Fatalf("empty history = %+v, %v", got, err)
	}
}

func TestParseOpHistoryRefusesWhatItCannotRead(t *testing.T) {
	for name, report := range map[string]string{
		"not JSON":                 `osd.0 is not running`,
		"no size":                  `{"duration":600,"ops":[]}`,
		"null size":                `{"size":null,"ops":[]}`,
		"no op list":               `{"size":20}`,
		"null op list":             `{"size":20,"ops":null}`,
		"entry without a text":     `{"size":20,"ops":[{"initiated_at":"x"}]}`,
		"entry with a null text":   `{"size":20,"ops":[{"description":null}]}`,
		"operation without a PG":   string(historyReport("20", "osd_op(client.4567.0:10)")),
		"operation without a pool": string(historyReport("20", "osd_op(client.4567.0:10 x.3 7:c1a2b3d4:::m:head [stat] snapc 0=[] e45)")),
		"operation without a text": string(historyReport("20", "osd_op()")),
		"operation that ends late": string(historyReport("20", "osd_op(client.4567.0:10 7.3 7:c1a2b3d4:::m:head [stat] snapc 0=[] e45")),
	} {
		if got, err := parseOpHistory([]byte(report)); err == nil {
			t.Errorf("%s: read as %+v", name, got)
		}
	}
}

// An operation on a PG has no object, and an operation whose object or
// sub-operations have another form is kept as unread with its pool.
func TestParseOSDOpKeepsWhatItCannotRead(t *testing.T) {
	listing, err := parseOSDOp("osd_op(client.4150.0:4 2.0 2:00000000::::head [pgnls start_epoch 9] snapc 0=[] ondisk+read+ignore_overlay e9)")
	if want := (osdOp{ReqID: "client.4150.0:4", Pool: 2, Subs: []string{"pgnls start_epoch 9"}}); err != nil || !reflect.DeepEqual(listing, want) {
		t.Fatalf("PG operation = %+v, %v, want %+v", listing, err, want)
	}
	for name, description := range map[string]string{
		"undecoded operation":    "osd_op(client.4567.0:10 7.3 7.c1a2b3d4 (undecoded) ondisk+read e45)",
		"no sub-operations":      "osd_op(client.4567.0:10 7.3 7:c1a2b3d4:::m.1_slo:head snapc 0=[] ondisk+read e45)",
		"unended sub-operations": "osd_op(client.4567.0:10 7.3 7:c1a2b3d4:::m.1_slo:head [stat ondisk+read e45)",
		"object of another pool": "osd_op(client.4567.0:10 7.3 8:c1a2b3d4:::m.1_slo:head [stat] snapc 0=[] ondisk+read e45)",
		"object in another form": "osd_op(client.4567.0:10 7.3 MIN [stat] snapc 0=[] ondisk+read e45)",
		"empty sub-operation":    "osd_op(client.4567.0:10 7.3 7:c1a2b3d4:::m:head [stat,] snapc 0=[] e45)",
	} {
		got, err := parseOSDOp(description)
		if err != nil || got.ReqID != "client.4567.0:10" || got.Pool != 7 || got.Unread == "" || got.Object != "" || got.Subs != nil {
			t.Errorf("%s: read as %+v, %v, want an unread operation of pool 7", name, got, err)
		}
	}
}

func TestParseSubClassifiesReads(t *testing.T) {
	for sub, want := range map[string]extent{
		"read 0~3145728":       {Off: 0, Len: 3145728},
		"read 0~4194304":       {Off: 0, Len: 4194304},
		"read 4194304~1048576": {Off: 4194304, Len: 1048576},
	} {
		if got, other := parseSub(sub); other || got == nil || *got != want {
			t.Errorf("%q = %v, other %t, want %v", sub, got, other, want)
		}
	}
	for _, sub := range []string{"call version.read in=11b", "call version.read", "getxattrs", "stat"} {
		if got, other := parseSub(sub); other || got != nil {
			t.Errorf("%q = %v, other %t, want a metadata sub-operation", sub, got, other)
		}
	}
	// Everything else is unclassified: a call of another method, of a
	// method the history does not name, or with anything but the size of
	// its input after it; a metadata verb with anything after it; a read
	// in another form or with anything after its extent; other data
	// operations and writes.
	for _, sub := range []string{
		"", "call", "call in=11b", "call rgw.obj_store_pg_ver in=20b", "call version.read_all in=11b", "call version.read out=48b",
		"call version.read in=11b out=48b", "call version.read in=x", "call version.set in=30b", "call refcount.read in=2b",
		"getxattr user.rgw.acl", "getxattrs in=4b", "stat out=16b", "statfs", "getxattrsx",
		"read", "read 0", "read x~1", "read 0~y", "read -1~4", "read 0~-4", "read 0~4 retry", "read 0~4096 [1@0]",
		"read 0~4096 [fadvise_sequential]", "read 0~3145728 out=3145728b", "read 0~4096 in=0b", "readx 0~4",
		"sparse-read 0~4096", "sync_read 0~4096", "mapext 0~4096", "cmpext 0~4096", "checksum", "writefull 0~4096", "create", "setxattr user.rgw.acl (10)",
	} {
		if got, other := parseSub(sub); !other || got != nil {
			t.Errorf("%q = %v, other %t, want an unclassified sub-operation", sub, got, other)
		}
	}
}

func TestObjectOpsCovers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reads []extent
		size  int64
		want  bool
		bytes int64
	}{
		{"one read of the object", []extent{{0, 100}}, 100, true, 100},
		{"a read beyond the end", []extent{{0, 4096}}, 100, true, 100},
		{"adjacent reads in any order", []extent{{50, 50}, {0, 50}}, 100, true, 100},
		{"overlapping reads", []extent{{0, 60}, {40, 60}}, 100, true, 120},
		{"no read", nil, 100, false, 0},
		{"a read of the first bytes", []extent{{0, 99}}, 100, false, 99},
		{"a read that starts late", []extent{{1, 99}}, 100, false, 99},
		{"a hole between reads", []extent{{0, 40}, {60, 40}}, 100, false, 80},
		{"a read that starts beyond the end", []extent{{100, 50}}, 100, false, 0},
		{"an object without a size", []extent{{0, 100}}, 0, false, 0},
	} {
		ops := objectOps{Reads: tc.reads}
		if got, bytes := ops.covers(tc.size), ops.readBytes(tc.size); got != tc.want || bytes != tc.bytes {
			t.Errorf("%s: covers = %t with %d bytes, want %t with %d", tc.name, got, bytes, tc.want, tc.bytes)
		}
	}
}

func TestWindowOpsReturnsTheNewOperationsOfTheDataPool(t *testing.T) {
	seen := map[string]bool{"client.4567.0:10": true}
	history, err := parseOpHistory(historyReport("20000", opManifest, opSegment, opIndex, opMarker))
	if err != nil {
		t.Fatal(err)
	}
	got, err := windowOps(history, seen, 7, "tc-slo-window-1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]objectOps{"s.2_segment-0000": {Requests: 1, Reads: []extent{{0, 3145728}}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("window = %+v, want %+v", got, want)
	}
	// The same history has nothing new, and so no marker of a window.
	if got, err := windowOps(history, seen, 7, "tc-slo-window-1"); err == nil {
		t.Fatalf("a history without a new marker was read as the window %+v", got)
	}
	// An unread operation of another pool does not concern the window, and
	// an operation on a PG of the data pool is kept without an object name.
	held := []string{opManifest, opSegment, opIndex, opMarker,
		"osd_op(client.4567.0:20 6.3 6.c1a2b3d4 (undecoded) ondisk+read e45)",
		"osd_op(client.4567.0:21 7.0 7:00000000::::head [pgnls start_epoch 9] snapc 0=[] ondisk+read e45)",
		strings.ReplaceAll(strings.ReplaceAll(opMarker, "window-1", "window-9"), "client.9000.0:1", "client.9009.0:1")}
	history, err = parseOpHistory(historyReport("20000", held...))
	if err != nil {
		t.Fatal(err)
	}
	got, err = windowOps(history, seen, 7, "tc-slo-window-9")
	if want := (map[string]objectOps{"": {Requests: 1, Other: []string{"pgnls start_epoch 9"}}}); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("window = %+v, %v, want %+v", got, err, want)
	}
	// A sub-operation that is neither a read nor a known metadata read is
	// kept: a write, and a call of a method the case does not know.
	held = append(held,
		`osd_op(client.4567.0:13 7.3 7:c1a2b3d4:::m.1_slo:head [writefull 0~10,setxattr user.rgw.acl (10)] snapc 0=[] ondisk+write e45)`,
		`osd_op(client.4567.0:14 7.1 7:81a2b3d4:::s.2_segment-0000:head [call refcount.read in=2b,getxattrs,stat] snapc 0=[] ondisk+read e45)`,
		`osd_op(client.4567.0:15 7.1 7:81a2b3d4:::s.2_segment-0001:head [call,getxattrs,stat] snapc 0=[] ondisk+read e45)`,
		strings.ReplaceAll(strings.ReplaceAll(opMarker, "window-1", "window-2"), "client.9000.0:1", "client.9001.0:1"))
	history, err = parseOpHistory(historyReport("20000", held...))
	if err != nil {
		t.Fatal(err)
	}
	got, err = windowOps(history, seen, 7, "tc-slo-window-2")
	want = map[string]objectOps{
		"m.1_slo":          {Requests: 1, Other: []string{"writefull 0~10", "setxattr user.rgw.acl (10)"}},
		"s.2_segment-0000": {Requests: 1, Other: []string{"call refcount.read in=2b"}},
		"s.2_segment-0001": {Requests: 1, Other: []string{"call"}},
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("window = %+v, %v, want %+v", got, err, want)
	}
	// Such a call on a segment is never read as a HEAD without data reads.
	segments := []segment{{"s.2_segment-0000", 3145728}}
	full := sloWindow{Ops: map[string]objectOps{"m.1_slo": {Requests: 1}, "s.2_segment-0000": {Requests: 1, Reads: []extent{{0, 3145728}}}}, OutBytes: 3145728}
	head := sloWindow{Ops: map[string]objectOps{"m.1_slo": {Requests: 1}, "s.2_segment-0000": got["s.2_segment-0000"]}}
	if symptoms, err := judgeSLOHead("m.1_slo", segments, full, head); err == nil || !strings.Contains(err.Error(), "window not observed") {
		t.Fatalf("a call of an unknown method on a segment was judged as %q, %v", symptoms, err)
	}
}

func TestWindowOpsRefusesAnIncompleteHistory(t *testing.T) {
	for name, tc := range map[string]struct {
		size         string
		descriptions []string
	}{
		"no marker":      {"20000", []string{opManifest, opSegment}},
		"two markers":    {"20000", []string{opMarker, strings.ReplaceAll(opMarker, "client.9000.0:1", "client.9001.0:1")}},
		"a full history": {"3", []string{opManifest, opSegment, opMarker}},
		"marker of another pool": {"20000", []string{opManifest,
			`osd_op(client.9000.0:1 6.0 6:01a2b3d4:::tc-slo-window-1:head [create] snapc 0=[] ondisk+write e45)`}},
		"an unread operation of the data pool": {"20000", []string{opMarker, "osd_op(client.4567.0:14 7.3 7.c1a2b3d4 (undecoded) ondisk+read e45)"}},
	} {
		history, err := parseOpHistory(historyReport(tc.size, tc.descriptions...))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got, err := windowOps(history, map[string]bool{}, 7, "tc-slo-window-1"); err == nil {
			t.Errorf("%s: read as the window %+v", name, got)
		}
	}
}

func TestRequireMetadataOnly(t *testing.T) {
	head := objectOps{Requests: 1}
	if err := requireMetadataOnly(map[string]objectOps{"b.1_single": head}, "b.1_single"); err != nil {
		t.Fatal(err)
	}
	for name, window := range map[string]map[string]objectOps{
		"no operation at all":            {},
		"no operation on the head":       {"b.1__shadow_single_1": head},
		"an object of another bucket":    {"b.1_single": head, "x.9_other": head},
		"another object of the bucket":   {"b.1_single": head, "b.1_multipart": head},
		"an object the head's name ends": {"b.1_single": head, "b.1_single2": head},
		"a tail object of the head":      {"b.1_single": head, "b.1__shadow_single_1": head},
		"an operation on a PG":           {"b.1_single": head, "": {Requests: 1, Other: []string{"pgnls start_epoch 9"}}},
		"a data read of the head":        {"b.1_single": {Requests: 1, Reads: []extent{{0, 4194304}}}},
		"an empty read of the head":      {"b.1_single": {Requests: 1, Reads: []extent{{0, 0}}}},
		"an unclassified read":           {"b.1_single": {Requests: 1, Other: []string{"sparse-read 0~4096"}}},
		"a call of an unknown method":    {"b.1_single": {Requests: 1, Other: []string{"call rgw.obj_store_pg_ver in=20b"}}},
		"a write on the head object":     {"b.1_single": {Requests: 1, Other: []string{"writefull 0~10"}}},
	} {
		if err := requireMetadataOnly(window, "b.1_single"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// These cases feed synthetic windows to judgeSLOHead. They are not native
// evidence of what a gateway reads.
func TestJudgeSLOHead(t *testing.T) {
	const manifest = "m.1_slo"
	segments := []segment{{"s.2_segment-0000", 100}, {"s.2_segment-0001", 100}, {"s.2_segment-0002", 50}}
	stat := objectOps{Requests: 1}
	full := func(size int64) objectOps { return objectOps{Requests: 1, Reads: []extent{{0, size}}} }
	// rest is a read that starts at offset and has no length.
	rest := func(offset int64) objectOps { return objectOps{Requests: 1, Reads: []extent{{offset, 0}}} }
	read := map[string]objectOps{manifest: stat, "s.2_segment-0000": full(100), "s.2_segment-0001": full(100), "s.2_segment-0002": full(50)}
	get := sloWindow{Ops: read, OutBytes: 260}
	with := func(base map[string]objectOps, object string, ops objectOps) map[string]objectOps {
		changed := map[string]objectOps{}
		for name, o := range base {
			changed[name] = o
		}
		changed[object] = ops
		return changed
	}
	for _, tc := range []struct {
		name      string
		get, head sloWindow
		symptoms  int
		want      string
	}{
		{name: "the HEAD reads every segment", get: get, head: sloWindow{Ops: read, OutBytes: 255}, symptoms: 4},
		{name: "the HEAD reads every segment in chunks", get: get,
			head: sloWindow{Ops: with(read, "s.2_segment-0000", objectOps{Requests: 2, Reads: []extent{{0, 64}, {64, 36}}}), OutBytes: 250}, symptoms: 4},
		{name: "the HEAD asks for more than every segment holds", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": full(4096), "s.2_segment-0001": full(4096), "s.2_segment-0002": full(4096)}, OutBytes: 255}, symptoms: 4},
		{name: "the HEAD reads only the manifest", get: get, head: sloWindow{Ops: map[string]objectOps{manifest: stat}, OutBytes: 0}},
		{name: "the HEAD reads the attributes of the segments", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": stat, "s.2_segment-0001": stat, "s.2_segment-0002": stat}, OutBytes: 49}},

		// The request did not reach the OSD: nothing was observed.
		{name: "an empty HEAD window", get: get, head: sloWindow{}, want: "condition not established"},
		{name: "a HEAD window without the manifest", get: get, head: sloWindow{Ops: map[string]objectOps{"s.2_segment-0000": full(100)}, OutBytes: 100}, want: "condition not established"},
		{name: "a GET window without the manifest", get: sloWindow{Ops: with(read, manifest, objectOps{}), OutBytes: 260}, head: sloWindow{Ops: read, OutBytes: 255}, want: "condition not established"},
		// The observation does not see data reads when they happen.
		{name: "the GET shows no read of a segment", get: sloWindow{Ops: with(read, "s.2_segment-0001", stat), OutBytes: 260}, head: sloWindow{Ops: map[string]objectOps{manifest: stat}}, want: "control not established"},
		{name: "the GET shows a partial read of a segment", get: sloWindow{Ops: with(read, "s.2_segment-0001", full(99)), OutBytes: 260}, head: sloWindow{Ops: read, OutBytes: 255}, want: "control not established"},
		{name: "the GET returned less than the payload", get: sloWindow{Ops: read, OutBytes: 249}, head: sloWindow{Ops: read, OutBytes: 255}, want: "control not established"},
		// Neither none nor all of the payload.
		{name: "the HEAD reads the first bytes of every segment", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": full(10), "s.2_segment-0001": full(10), "s.2_segment-0002": full(10)}, OutBytes: 30}, want: "another state"},
		{name: "the HEAD reads one segment of three", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": full(100)}, OutBytes: 100}, want: "another state"},
		{name: "the HEAD reads every segment from beyond its end", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": rest(200), "s.2_segment-0001": rest(200), "s.2_segment-0002": rest(200)}}, want: "another state"},
		// The operations and the byte counter disagree.
		{name: "full reads, but fewer bytes returned", get: get, head: sloWindow{Ops: read, OutBytes: 249}, want: "outcome not observed"},
		{name: "no read, but a segment's worth of bytes returned", get: get, head: sloWindow{Ops: map[string]objectOps{manifest: stat}, OutBytes: 50}, want: "outcome not observed"},
		// Operations the window cannot attribute or classify.
		{name: "an operation on a PG", get: get, head: sloWindow{Ops: with(read, "", objectOps{Requests: 1, Other: []string{"pgnls start_epoch 9"}}), OutBytes: 255}, want: "window not observed"},
		{name: "an operation on a foreign object", get: get, head: sloWindow{Ops: with(read, "x.9_other", stat), OutBytes: 255}, want: "window not observed"},
		{name: "an unclassified sub-operation on a segment", get: get,
			head: sloWindow{Ops: map[string]objectOps{manifest: stat, "s.2_segment-0000": {Requests: 1, Other: []string{"sparse-read 0~100"}}}}, want: "window not observed"},
		{name: "an unclassified sub-operation in the GET", get: sloWindow{Ops: with(read, manifest, objectOps{Requests: 1, Other: []string{"checksum"}}), OutBytes: 260}, head: sloWindow{Ops: read, OutBytes: 255}, want: "window not observed"},
	} {
		symptoms, err := judgeSLOHead(manifest, segments, tc.get, tc.head)
		switch {
		case tc.want == "" && (err != nil || len(symptoms) != tc.symptoms):
			t.Errorf("%s: symptoms %q, error %v, want %d symptoms", tc.name, symptoms, err, tc.symptoms)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || symptoms != nil):
			t.Errorf("%s: symptoms %q, error %v, want an error with %q", tc.name, symptoms, err, tc.want)
		}
	}
	for name, tc := range map[string]struct {
		manifest string
		segments []segment
	}{
		"no manifest":              {"", segments},
		"no segment":               {manifest, nil},
		"a segment without a size": {manifest, []segment{{"s.2_segment-0000", 0}}},
		"a segment without a name": {manifest, []segment{{"", 100}}},
	} {
		if symptoms, err := judgeSLOHead(tc.manifest, tc.segments, get, sloWindow{Ops: read, OutBytes: 255}); err == nil {
			t.Errorf("%s: judged as %q", name, symptoms)
		}
	}
}

func TestParseReadOutBytesTellsZeroFromMissing(t *testing.T) {
	for report, want := range map[string]int64{
		`{"osd":{"op_r":3,"op_r_out_bytes":0}}`:       0,
		`{"osd":{"op_r":3,"op_r_out_bytes":9437184}}`: 9437184,
	} {
		if got, err := parseReadOutBytes([]byte(report)); err != nil || got != want {
			t.Errorf("%s = %d, %v, want %d", report, got, err, want)
		}
	}
	for _, report := range []string{``, `{}`, `{"osd":null}`, `{"osd":{}}`, `{"osd":{"op_r_out_bytes":null}}`, `{"osd":{"op_r_out_bytes":-1}}`, `{"osd":{"op_r_out_bytes":"9"}}`, `{"op_r_out_bytes":9}`} {
		if got, err := parseReadOutBytes([]byte(report)); err == nil {
			t.Errorf("%s read as %d", report, got)
		}
	}
}

func TestParseBucketMarker(t *testing.T) {
	if got, err := parseBucketMarker([]byte(`{"bucket":"tc-slo","id":"a.1.2","marker":"a.1.1"}`)); err != nil || got != "a.1.1" {
		t.Fatalf("marker = %q, %v", got, err)
	}
	for _, report := range []string{``, `{}`, `{"marker":null}`, `{"marker":""}`, `{"id":"a.1.2"}`, `[]`} {
		if got, err := parseBucketMarker([]byte(report)); err == nil {
			t.Errorf("%s read as %q", report, got)
		}
	}
}
