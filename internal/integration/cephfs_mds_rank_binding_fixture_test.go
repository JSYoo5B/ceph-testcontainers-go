//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// These controls feed synthetic FSMap dumps, daemon status answers and session
// lists to the rank binding reader and judge. They test parsing and the
// binding policy only. They do not reproduce a native MDS, and the shapes
// follow the pinned source rather than a captured run.

const (
	rankFS      = "fs0"
	rankGID     = 5001
	rankStandby = 5002
	rankClientA = 4101
	rankClientB = 4102
)

type rankInfoSpec struct {
	gid    uint64
	name   string
	rank   int
	state  string
	laggy  bool
	rawKey string
}

func rankInfo(s rankInfoSpec) string {
	key := s.rawKey
	if key == "" {
		key = fmt.Sprintf("gid_%d", s.gid)
	}
	laggy := ""
	if s.laggy {
		laggy = `"laggy_since":"2026-01-01T00:00:00.000000+0000",`
	}
	return fmt.Sprintf(`%q:{"gid":%d,"name":%q,"rank":%d,"incarnation":3,"state":%q,%s"state_seq":9}`, key, s.gid, s.name, s.rank, s.state, laggy)
}

// rankMapJSON builds one filesystem's mdsmap. The pieces are replaceable so a
// control can change exactly one field.
type rankMapSpec struct {
	epoch, maxMDS, in, up, failed, damaged, infos, extraFS string
	fsName                                                 string
}

func rankMapDefault() rankMapSpec {
	return rankMapSpec{
		epoch: "40", maxMDS: "1", in: "0", up: `"mds_0":5001`, failed: "", damaged: "",
		infos: rankInfo(rankInfoSpec{gid: rankGID, name: "a", rank: 0, state: "up:active"}) + "," +
			rankInfo(rankInfoSpec{gid: rankStandby, name: "b", rank: 0, state: "up:standby-replay"}),
		fsName: rankFS,
	}
}

func (s rankMapSpec) json() []byte {
	return []byte(fmt.Sprintf(`{"epoch":7,"filesystems":[%s{"mdsmap":{"epoch":%s,"max_mds":%s,"in":[%s],"up":{%s},"failed":[%s],"damaged":[%s],"stopped":[],"info":{%s},"fs_name":%q},"id":1}]}`,
		s.extraFS, s.epoch, s.maxMDS, s.in, s.up, s.failed, s.damaged, s.infos, s.fsName))
}

func rankSnapshot(t *testing.T, s rankMapSpec) mdsMapSnapshot {
	t.Helper()
	m, err := parseMDSMapSnapshot(s.json(), rankFS)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return m
}

func rankStatusJSON(fs string, whoami, id int64, state string, epoch int) []byte {
	return []byte(fmt.Sprintf(`{"cluster_fsid":"x","whoami":%d,"id":%d,"want_state":%q,"state":%q,"fs_name":%q,"rank_uptime":3.5,"mdsmap_epoch":%d,"osdmap_epoch":4}`, whoami, id, state, state, fs, epoch))
}

func rankSessionJSON(rows ...string) []byte {
	return []byte("[" + strings.Join(rows, ",") + "]")
}

func rankSessionRow(id uint64, typ, state string) string {
	return fmt.Sprintf(`{"id":%d,"auth_name":{"entity_type":"client","entity_id":"x"},"entity":{"name":{"type":%q,"num":%d},"addr":{"type":"v1","addr":"10.0.0.2:0","nonce":77}},"state":%q,"num_leases":0,"num_caps":2,"inst":"client.%d v1:10.0.0.2:0/77"}`, id, typ, id, state, id)
}

func TestMDSRankMapSnapshotReader(t *testing.T) {
	m, err := parseMDSMapSnapshot(rankMapDefault().json(), rankFS)
	if err != nil || m.Epoch != 40 || m.MaxMDS != 1 || len(m.In) != 1 || m.Up[0] != rankGID || len(m.Info) != 2 || m.Info[rankGID].Name != "a" || m.Info[rankStandby].State != "up:standby-replay" || m.Info[rankGID].Laggy {
		t.Fatalf("good dump: %+v %v", m, err)
	}
	laggy := rankMapDefault()
	laggy.infos = rankInfo(rankInfoSpec{gid: rankGID, name: "a", rank: 0, state: "up:active", laggy: true})
	if m := rankSnapshot(t, laggy); !m.Info[rankGID].Laggy {
		t.Fatal("laggy_since was not read")
	}
	other := rankMapDefault()
	other.extraFS = `{"mdsmap":{"fs_name":"other","max_mds":3},"id":2},`
	if _, err := parseMDSMapSnapshot(other.json(), rankFS); err != nil {
		t.Fatalf("another filesystem with a name is not an error: %v", err)
	}

	mut := func(f func(*rankMapSpec)) []byte {
		s := rankMapDefault()
		f(&s)
		return s.json()
	}
	rejects := map[string][]byte{
		"not an object":            []byte(`[]`),
		"null document":            []byte(`null`),
		"no filesystems":           []byte(`{"epoch":1}`),
		"filesystems null":         []byte(`{"filesystems":null}`),
		"filesystems not an array": []byte(`{"filesystems":{}}`),
		"filesystem absent":        mut(func(s *rankMapSpec) { s.fsName = "elsewhere" }),
		"filesystem twice":         mut(func(s *rankMapSpec) { s.extraFS = `{"mdsmap":{"fs_name":"fs0"},"id":2},` }),
		"other filesystem unnamed": mut(func(s *rankMapSpec) { s.extraFS = `{"mdsmap":{},"id":2},` }),
		"other filesystem no map":  mut(func(s *rankMapSpec) { s.extraFS = `{"id":2},` }),
		"epoch missing":            []byte(`{"filesystems":[{"mdsmap":{"fs_name":"fs0"}}]}`),
		"epoch string":             mut(func(s *rankMapSpec) { s.epoch = `"40"` }),
		"epoch negative":           mut(func(s *rankMapSpec) { s.epoch = `-1` }),
		"epoch null":               mut(func(s *rankMapSpec) { s.epoch = `null` }),
		"max_mds fractional":       mut(func(s *rankMapSpec) { s.maxMDS = `1.0` }),
		"in element string":        mut(func(s *rankMapSpec) { s.in = `"0"` }),
		"failed element null":      mut(func(s *rankMapSpec) { s.failed = `null` }),
		"up key not a rank":        mut(func(s *rankMapSpec) { s.up = `"rank0":5001` }),
		"up key negative rank":     mut(func(s *rankMapSpec) { s.up = `"mds_-1":5001` }),
		"up gid string":            mut(func(s *rankMapSpec) { s.up = `"mds_0":"5001"` }),
		"up gid null":              mut(func(s *rankMapSpec) { s.up = `"mds_0":null` }),
		"info key disagrees": mut(func(s *rankMapSpec) {
			s.infos = rankInfo(rankInfoSpec{gid: rankGID, name: "a", state: "up:active", rawKey: "gid_9"})
		}),
		"info name null":        mut(func(s *rankMapSpec) { s.infos = `"gid_5001":{"gid":5001,"name":null,"rank":0,"state":"up:active"}` }),
		"info rank string":      mut(func(s *rankMapSpec) { s.infos = `"gid_5001":{"gid":5001,"name":"a","rank":"0","state":"up:active"}` }),
		"info state missing":    mut(func(s *rankMapSpec) { s.infos = `"gid_5001":{"gid":5001,"name":"a","rank":0}` }),
		"info entry not object": mut(func(s *rankMapSpec) { s.infos = `"gid_5001":7` }),
	}
	for name, doc := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			if m, err := parseMDSMapSnapshot(doc, rankFS); err == nil {
				t.Fatalf("accepted: %+v", m)
			}
		})
	}
	for _, field := range []string{"max_mds", "in", "up", "failed", "damaged", "info"} {
		t.Run("rejects missing "+field, func(t *testing.T) {
			doc := strings.Replace(string(rankMapDefault().json()), fmt.Sprintf("%q:", field), fmt.Sprintf("%q:", "x_"+field), 1)
			if m, err := parseMDSMapSnapshot([]byte(doc), rankFS); err == nil {
				t.Fatalf("accepted without %s: %+v", field, m)
			}
		})
	}
}

func TestMDSRankSingleRankSelection(t *testing.T) {
	sel := selectSingleRank(rankSnapshot(t, rankMapDefault()))
	if sel.Kind != rankSelectionOne || sel.Rank != 0 || sel.GID != rankGID || sel.Name != "a" {
		t.Fatalf("good map: %+v", sel)
	}
	active := func(gid uint64, name string, rank int) string {
		return rankInfo(rankInfoSpec{gid: gid, name: name, rank: rank, state: "up:active"})
	}
	cases := []struct {
		name string
		edit func(*rankMapSpec)
		want rankSelectionKind
	}{
		{"max_mds two", func(s *rankMapSpec) { s.maxMDS = "2" }, rankSelectionMultiRank},
		{"max_mds zero", func(s *rankMapSpec) { s.maxMDS = "0" }, rankSelectionMultiRank},
		{"two ranks in", func(s *rankMapSpec) { s.in = "0,1" }, rankSelectionMultiRank},
		{"two ranks up", func(s *rankMapSpec) {
			s.up = `"mds_0":5001,"mds_1":5003`
			s.infos += "," + active(5003, "c", 1)
		}, rankSelectionMultiRank},
		{"in set is rank one only", func(s *rankMapSpec) { s.in = "1" }, rankSelectionIneligible},
		{"in set empty", func(s *rankMapSpec) { s.in = "" }, rankSelectionIneligible},
		{"failed rank", func(s *rankMapSpec) { s.failed = "0" }, rankSelectionIneligible},
		{"damaged rank", func(s *rankMapSpec) { s.damaged = "0" }, rankSelectionIneligible},
		{"no up daemon", func(s *rankMapSpec) { s.up = "" }, rankSelectionIneligible},
		{"up gid zero", func(s *rankMapSpec) { s.up = `"mds_0":0` }, rankSelectionIneligible},
		{"up gid without info", func(s *rankMapSpec) { s.up = `"mds_0":9999` }, rankSelectionIneligible},
		{"up daemon reports another rank", func(s *rankMapSpec) { s.infos = active(rankGID, "a", 1) }, rankSelectionIneligible},
		{"replaying", func(s *rankMapSpec) {
			s.infos = rankInfo(rankInfoSpec{gid: rankGID, name: "a", rank: 0, state: "up:replay"})
		}, rankSelectionIneligible},
		{"laggy", func(s *rankMapSpec) {
			s.infos = rankInfo(rankInfoSpec{gid: rankGID, name: "a", rank: 0, state: "up:active", laggy: true})
		}, rankSelectionIneligible},
		{"second daemon claims rank", func(s *rankMapSpec) { s.infos += "," + active(5003, "c", 0) }, rankSelectionIneligible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := rankMapDefault()
			tc.edit(&s)
			got := selectSingleRank(rankSnapshot(t, s))
			if got.Kind != tc.want || got.Reason == "" {
				t.Fatalf("got %+v want kind %d", got, tc.want)
			}
		})
	}
	t.Run("standby-replay does not take the rank", func(t *testing.T) {
		s := rankMapDefault()
		s.infos = rankInfo(rankInfoSpec{gid: rankStandby, name: "b", rank: 0, state: "up:standby-replay"}) + "," + rankInfo(rankInfoSpec{gid: rankGID, name: "a", rank: 0, state: "up:active"})
		if got := selectSingleRank(rankSnapshot(t, s)); got.Kind != rankSelectionOne || got.GID != rankGID {
			t.Fatalf("%+v", got)
		}
	})
}

func TestMDSRankStatusReader(t *testing.T) {
	st, err := parseMDSDaemonStatus(rankStatusJSON(rankFS, 0, rankGID, "up:active", 40))
	if err != nil || st != (mdsDaemonStatus{FS: rankFS, Whoami: 0, ID: rankGID, State: "up:active", MapEpoch: 40}) {
		t.Fatalf("good status: %+v %v", st, err)
	}
	if st, err := parseMDSDaemonStatus(rankStatusJSON(rankFS, -1, rankGID, "up:standby", 40)); err != nil || st.Whoami != -1 {
		t.Fatalf("rankless status is data, not an error: %+v %v", st, err)
	}
	rejects := map[string]string{
		"not an object":    `[]`,
		"null":             `null`,
		"no fs_name":       `{"whoami":-1,"id":5001,"state":"up:standby","mdsmap_epoch":4}`,
		"fs_name null":     `{"fs_name":null,"whoami":0,"id":5001,"state":"up:active","mdsmap_epoch":4}`,
		"fs_name empty":    `{"fs_name":"","whoami":0,"id":5001,"state":"up:active","mdsmap_epoch":4}`,
		"whoami missing":   `{"fs_name":"fs0","id":5001,"state":"up:active","mdsmap_epoch":4}`,
		"whoami string":    `{"fs_name":"fs0","whoami":"0","id":5001,"state":"up:active","mdsmap_epoch":4}`,
		"id missing":       `{"fs_name":"fs0","whoami":0,"state":"up:active","mdsmap_epoch":4}`,
		"id null":          `{"fs_name":"fs0","whoami":0,"id":null,"state":"up:active","mdsmap_epoch":4}`,
		"id negative":      `{"fs_name":"fs0","whoami":0,"id":-5,"state":"up:active","mdsmap_epoch":4}`,
		"state missing":    `{"fs_name":"fs0","whoami":0,"id":5001,"mdsmap_epoch":4}`,
		"state empty":      `{"fs_name":"fs0","whoami":0,"id":5001,"state":"","mdsmap_epoch":4}`,
		"epoch missing":    `{"fs_name":"fs0","whoami":0,"id":5001,"state":"up:active"}`,
		"epoch fractional": `{"fs_name":"fs0","whoami":0,"id":5001,"state":"up:active","mdsmap_epoch":4.5}`,
	}
	for name, doc := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			if st, err := parseMDSDaemonStatus([]byte(doc)); err == nil {
				t.Fatalf("accepted: %+v", st)
			}
		})
	}
}

func TestMDSRankSessionListReader(t *testing.T) {
	good := rankSessionJSON(rankSessionRow(rankClientA, "client", "open"), rankSessionRow(rankClientB, "client", "stale"))
	got, err := parseRankSessionList(good)
	if err != nil || len(got.States) != 2 || got.States[rankClientA] != "open" || got.States[rankClientB] != "stale" {
		t.Fatalf("good list: %+v %v", got, err)
	}
	if got, err := parseRankSessionList([]byte(`[]`)); err != nil || got.States == nil || len(got.States) != 0 {
		t.Fatalf("an empty array is a real empty list: %+v %v", got, err)
	}
	rejects := map[string][]byte{
		"object, not array":     []byte(`{"sessions":[]}`),
		"null":                  []byte(`null`),
		"row not an object":     []byte(`[7]`),
		"id missing":            []byte(`[{"entity":{"name":{"type":"client","num":4101}},"state":"open"}]`),
		"id zero":               rankSessionJSON(rankSessionRow(0, "client", "open")),
		"id string":             []byte(`[{"id":"4101","entity":{"name":{"type":"client","num":4101}},"state":"open"}]`),
		"entity missing":        []byte(`[{"id":4101,"state":"open"}]`),
		"entity name missing":   []byte(`[{"id":4101,"entity":{},"state":"open"}]`),
		"entity number differs": []byte(`[{"id":4101,"entity":{"name":{"type":"client","num":4102}},"state":"open"}]`),
		"entity not a client":   rankSessionJSON(rankSessionRow(4101, "mds", "open")),
		"state missing":         []byte(`[{"id":4101,"entity":{"name":{"type":"client","num":4101}}}]`),
		"state null":            []byte(`[{"id":4101,"entity":{"name":{"type":"client","num":4101}},"state":null}]`),
		"state empty":           rankSessionJSON(rankSessionRow(4101, "client", "")),
		"duplicate id":          rankSessionJSON(rankSessionRow(4101, "client", "open"), rankSessionRow(4101, "client", "open")),
	}
	for name, doc := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			if got, err := parseRankSessionList(doc); err == nil {
				t.Fatalf("accepted: %+v", got)
			}
		})
	}
}

// rankEvidence returns a window in which one rank held by one daemon was asked
// four ways: two maps and two statuses around one session list.
func rankEvidence(t *testing.T) rankBindingEvidence {
	t.Helper()
	base := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	snap := rankSnapshot(t, rankMapDefault())
	later := snap
	later.Epoch++
	list, err := parseRankSessionList(rankSessionJSON(rankSessionRow(rankClientA, "client", "open"), rankSessionRow(rankClientB, "client", "open")))
	if err != nil {
		t.Fatal(err)
	}
	status := func(epoch uint64) mdsDaemonStatus {
		return mdsDaemonStatus{FS: rankFS, Whoami: 0, ID: rankGID, State: "up:active", MapEpoch: epoch}
	}
	return rankBindingEvidence{
		FS: rankFS, Clients: []uint64{rankClientA, rankClientB}, MetricsRank: 0,
		WindowStart: at(0), Deadline: at(1000),
		Maps:     []rankMapProbe{{at(10), at(20), snap}, {at(80), at(90), later}},
		Statuses: []rankStatusProbe{{at(30), at(40), "mds.fs0:0", status(40)}, {at(60), at(70), "mds.fs0:0", status(41)}},
		Sessions: []rankSessionProbe{{at(45), at(55), "mds.fs0:0", "session ls", list}},
	}
}

func TestMDSRankBindingJudge(t *testing.T) {
	good := judgeRankBinding(rankEvidence(t))
	if good.Verdict != rankBindingBound || good.Rank != 0 || good.GID != rankGID || good.Name != "a" {
		t.Fatalf("good evidence: %+v", good)
	}
	at := func(ms int) time.Time { return time.Unix(1_700_000_000, 0).Add(time.Duration(ms) * time.Millisecond) }
	swapped := func(e *rankBindingEvidence, edit func(*mdsMapSnapshot)) {
		m := e.Maps[1].Map
		edit(&m)
		e.Maps[1].Map = m
	}
	cases := []struct {
		name string
		edit func(*rankBindingEvidence)
		want rankBindingVerdict
	}{
		{"client ls is the same command", func(e *rankBindingEvidence) { e.Sessions[0].Command = "client ls" }, rankBindingBound},
		{"daemon addressed by name", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.a" }, rankBindingBound},
		{"extra unrelated session is fine", func(e *rankBindingEvidence) { e.Sessions[0].List.States[9] = "open" }, rankBindingBound},
		{"probes outside the window are ignored", func(e *rankBindingEvidence) {
			e.Statuses = append(e.Statuses, rankStatusProbe{at(1500), at(1510), "mds.fs0:0", mdsDaemonStatus{FS: rankFS, Whoami: 0, ID: 9999, State: "up:active", MapEpoch: 50}})
		}, rankBindingBound},

		// Several ranks.
		{"second map has two ranks", func(e *rankBindingEvidence) { swapped(e, func(m *mdsMapSnapshot) { m.MaxMDS = 2 }) }, rankBindingRejected},
		{"first map has two ranks in", func(e *rankBindingEvidence) { e.Maps[0].Map.In = []int64{0, 1} }, rankBindingRejected},
		{"rank one up", func(e *rankBindingEvidence) {
			swapped(e, func(m *mdsMapSnapshot) {
				m.Up = map[int64]uint64{0: rankGID, 1: 5003}
			})
		}, rankBindingRejected},
		{"list asked of rank one", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.fs0:1" }, rankBindingRejected},
		{"status asked of rank one", func(e *rankBindingEvidence) { e.Statuses[0].Target = "mds.fs0:1" }, rankBindingRejected},
		{"list asked of every daemon", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.*" }, rankBindingRejected},
		{"list asked of another filesystem", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.fs1:0" }, rankBindingRejected},
		{"list asked of the standby daemon", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.b" }, rankBindingRejected},
		{"metrics rows of another rank", func(e *rankBindingEvidence) { e.MetricsRank = 1 }, rankBindingRejected},
		{"status says rank one", func(e *rankBindingEvidence) { e.Statuses[1].Status.Whoami = 1 }, rankBindingRejected},
		{"status says no rank", func(e *rankBindingEvidence) { e.Statuses[1].Status.Whoami = -1 }, rankBindingRejected},
		{"status of another filesystem", func(e *rankBindingEvidence) { e.Statuses[0].Status.FS = "fs1" }, rankBindingRejected},
		{"map of another filesystem", func(e *rankBindingEvidence) { e.Maps[0].Map.FS = "fs1" }, rankBindingRejected},

		// Several generations.
		{"rank 0 daemon replaced between maps", func(e *rankBindingEvidence) {
			swapped(e, func(m *mdsMapSnapshot) {
				m.Up = map[int64]uint64{0: 5003}
				m.Info = map[uint64]mdsDaemonInfo{5003: {GID: 5003, Name: "a", Rank: 0, State: "up:active"}}
			})
		}, rankBindingRejected},
		{"replacement still replaying", func(e *rankBindingEvidence) {
			swapped(e, func(m *mdsMapSnapshot) {
				m.Up = map[int64]uint64{0: 5003}
				m.Info = map[uint64]mdsDaemonInfo{5003: {GID: 5003, Name: "a", Rank: 0, State: "up:replay"}}
			})
		}, rankBindingRejected},
		{"status from a restarted daemon", func(e *rankBindingEvidence) { e.Statuses[1].Status.ID = 5003 }, rankBindingRejected},
		{"status from the standby", func(e *rankBindingEvidence) { e.Statuses[0].Status.ID = rankStandby }, rankBindingRejected},

		// Evidence that is missing, unusable or not usable as evidence.
		{"bare integer target", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.0" }, rankBindingInconclusive},
		{"gid target", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.5001" }, rankBindingInconclusive},
		{"unknown daemon name", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.zz" }, rankBindingInconclusive},
		{"target without the mds prefix", func(e *rankBindingEvidence) { e.Sessions[0].Target = "fs0:0" }, rankBindingInconclusive},
		{"target with empty filesystem", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.:0" }, rankBindingInconclusive},
		{"target with bad rank", func(e *rankBindingEvidence) { e.Sessions[0].Target = "mds.fs0:x" }, rankBindingInconclusive},
		{"status target unusable", func(e *rankBindingEvidence) { e.Statuses[0].Target = "mds.0" }, rankBindingInconclusive},
		{"other command", func(e *rankBindingEvidence) { e.Sessions[0].Command = "session evict" }, rankBindingInconclusive},
		{"no command", func(e *rankBindingEvidence) { e.Sessions[0].Command = "" }, rankBindingInconclusive},
		{"daemon not active", func(e *rankBindingEvidence) { e.Statuses[1].Status.State = "up:stopping" }, rankBindingInconclusive},
		{"daemon map epoch decreases", func(e *rankBindingEvidence) { e.Statuses[1].Status.MapEpoch = 3 }, rankBindingInconclusive},
		{"map epoch decreases", func(e *rankBindingEvidence) { e.Maps[1].Map.Epoch = 3 }, rankBindingInconclusive},
		{"map without active daemon", func(e *rankBindingEvidence) {
			e.Maps[1].Map.Info = map[uint64]mdsDaemonInfo{rankGID: {GID: rankGID, Name: "a", Rank: 0, State: "up:replay"}}
		}, rankBindingInconclusive},
		{"map with laggy daemon", func(e *rankBindingEvidence) {
			d := e.Maps[0].Map.Info[rankGID]
			d.Laggy = true
			e.Maps[0].Map.Info = map[uint64]mdsDaemonInfo{rankGID: d}
		}, rankBindingInconclusive},
		{"map with failed rank", func(e *rankBindingEvidence) { e.Maps[0].Map.Failed = []int64{0} }, rankBindingInconclusive},
		{"map without up daemon", func(e *rankBindingEvidence) { e.Maps[0].Map.Up = map[int64]uint64{} }, rankBindingInconclusive},
		{"no maps", func(e *rankBindingEvidence) { e.Maps = nil }, rankBindingInconclusive},
		{"one map", func(e *rankBindingEvidence) { e.Maps = e.Maps[:1] }, rankBindingInconclusive},
		{"one status", func(e *rankBindingEvidence) { e.Statuses = e.Statuses[:1] }, rankBindingInconclusive},
		{"no lists", func(e *rankBindingEvidence) { e.Sessions = nil }, rankBindingInconclusive},
		{"no map before the list", func(e *rankBindingEvidence) { e.Maps[0].End = at(46) }, rankBindingInconclusive},
		{"no map after the list", func(e *rankBindingEvidence) { e.Maps[1].Start = at(54) }, rankBindingInconclusive},
		{"no status before the list", func(e *rankBindingEvidence) { e.Statuses[0].End = at(46) }, rankBindingInconclusive},
		{"no status after the list", func(e *rankBindingEvidence) { e.Statuses[1].Start = at(54) }, rankBindingInconclusive},
		{"list ends after the deadline", func(e *rankBindingEvidence) { e.Sessions[0].End = at(1001) }, rankBindingInconclusive},
		{"closing status ends after the deadline", func(e *rankBindingEvidence) { e.Statuses[1].End = at(1001) }, rankBindingInconclusive},
		{"opening map starts before the window", func(e *rankBindingEvidence) {
			e.WindowStart = at(15)
		}, rankBindingInconclusive},
		{"empty list", func(e *rankBindingEvidence) { e.Sessions[0].List.States = map[uint64]string{} }, rankBindingInconclusive},
		{"nil list", func(e *rankBindingEvidence) { e.Sessions[0].List.States = nil }, rankBindingInconclusive},
		{"client absent", func(e *rankBindingEvidence) { delete(e.Sessions[0].List.States, rankClientB) }, rankBindingInconclusive},
		{"client stale", func(e *rankBindingEvidence) { e.Sessions[0].List.States[rankClientB] = "stale" }, rankBindingInconclusive},
		{"client opening", func(e *rankBindingEvidence) { e.Sessions[0].List.States[rankClientA] = "opening" }, rankBindingInconclusive},

		// Admission.
		{"no filesystem", func(e *rankBindingEvidence) { e.FS = "" }, rankBindingInconclusive},
		{"no clients", func(e *rankBindingEvidence) { e.Clients = nil }, rankBindingInconclusive},
		{"zero client", func(e *rankBindingEvidence) { e.Clients = []uint64{0, rankClientB} }, rankBindingInconclusive},
		{"repeated client", func(e *rankBindingEvidence) { e.Clients = []uint64{rankClientA, rankClientA} }, rankBindingInconclusive},
		{"negative metrics rank", func(e *rankBindingEvidence) { e.MetricsRank = -1 }, rankBindingInconclusive},
		{"no window start", func(e *rankBindingEvidence) { e.WindowStart = time.Time{} }, rankBindingInconclusive},
		{"deadline before start", func(e *rankBindingEvidence) { e.Deadline = at(-1) }, rankBindingInconclusive},
		{"zero list time", func(e *rankBindingEvidence) { e.Sessions[0].Start = time.Time{} }, rankBindingInconclusive},
		{"inverted list interval", func(e *rankBindingEvidence) { e.Sessions[0].End = at(44) }, rankBindingInconclusive},
		{"inverted status interval", func(e *rankBindingEvidence) { e.Statuses[0].End = at(29) }, rankBindingInconclusive},
		{"maps out of order", func(e *rankBindingEvidence) { e.Maps[0], e.Maps[1] = e.Maps[1], e.Maps[0] }, rankBindingInconclusive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := rankEvidence(t)
			// Copy what the edits mutate in place.
			states := map[uint64]string{}
			for k, v := range e.Sessions[0].List.States {
				states[k] = v
			}
			e.Sessions[0].List.States = states
			tc.edit(&e)
			got := judgeRankBinding(e)
			if got.Verdict != tc.want || got.Reason == "" {
				t.Fatalf("verdict %d (%s), want %d", got.Verdict, got.Reason, tc.want)
			}
			if got.Verdict == rankBindingBound && (got.GID != rankGID || got.Rank != 0) {
				t.Fatalf("bound outcome: %+v", got)
			}
			if got.Verdict != rankBindingBound && (got.GID != 0 || got.Name != "") {
				t.Fatalf("a non-bound outcome carries an identity: %+v", got)
			}
		})
	}

	t.Run("a stale list does not outvote an open one", func(t *testing.T) {
		e := rankEvidence(t)
		stale := rankSessionProbe{at(46), at(54), "mds.fs0:0", "client ls", rankSessionList{States: map[uint64]string{rankClientA: "stale", rankClientB: "stale"}}}
		e.Sessions = append(e.Sessions, stale)
		if got := judgeRankBinding(e); got.Verdict != rankBindingBound {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("an unbracketed list is not evidence even when it is open", func(t *testing.T) {
		e := rankEvidence(t)
		e.Sessions[0].Start, e.Sessions[0].End = at(15), at(25)
		if got := judgeRankBinding(e); got.Verdict != rankBindingInconclusive {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("an unbracketed list for another rank is still rejected", func(t *testing.T) {
		e := rankEvidence(t)
		other := rankSessionProbe{at(15), at(25), "mds.fs0:1", "session ls", e.Sessions[0].List}
		e.Sessions = append([]rankSessionProbe{other}, e.Sessions...)
		if got := judgeRankBinding(e); got.Verdict != rankBindingRejected {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("rejection outranks a missing piece", func(t *testing.T) {
		e := rankEvidence(t)
		e.Maps[1].Map.MaxMDS = 2
		e.Sessions = nil
		if got := judgeRankBinding(e); got.Verdict != rankBindingRejected {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("the zero value is inconclusive", func(t *testing.T) {
		if got := judgeRankBinding(rankBindingEvidence{}); got.Verdict != rankBindingInconclusive || got.Reason == "" {
			t.Fatalf("%+v", got)
		}
	})
}

func TestMDSRankBindingFromParsedDocuments(t *testing.T) {
	// The same policy end to end from raw documents instead of structs.
	base := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	snap := func(epoch string) mdsMapSnapshot {
		s := rankMapDefault()
		s.epoch = epoch
		return rankSnapshot(t, s)
	}
	status := func(epoch int) mdsDaemonStatus {
		st, err := parseMDSDaemonStatus(rankStatusJSON(rankFS, 0, rankGID, "up:active", epoch))
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	list, err := parseRankSessionList(rankSessionJSON(rankSessionRow(rankClientA, "client", "open")))
	if err != nil {
		t.Fatal(err)
	}
	e := rankBindingEvidence{
		FS: rankFS, Clients: []uint64{rankClientA}, WindowStart: at(0), Deadline: at(100),
		Maps:     []rankMapProbe{{at(1), at(2), snap("40")}, {at(9), at(10), snap("40")}},
		Statuses: []rankStatusProbe{{at(3), at(4), "mds.fs0:0", status(40)}, {at(7), at(8), "mds.fs0:0", status(40)}},
		Sessions: []rankSessionProbe{{at(5), at(6), "mds.fs0:0", "session ls", list}},
	}
	if got := judgeRankBinding(e); got.Verdict != rankBindingBound {
		t.Fatalf("%+v", got)
	}
}
