//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// eversion is Ceph's "epoch'version" position in a PG log.
type eversion struct{ Epoch, Version uint64 }

func parseEversion(text string) (eversion, error) {
	epoch, version, found := strings.Cut(text, "'")
	if !found {
		return eversion{}, fmt.Errorf("eversion %q", text)
	}
	e, err := strconv.ParseUint(epoch, 10, 64)
	if err != nil {
		return eversion{}, fmt.Errorf("eversion %q: %w", text, err)
	}
	v, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		return eversion{}, fmt.Errorf("eversion %q: %w", text, err)
	}
	return eversion{e, v}, nil
}

func (a eversion) less(b eversion) bool {
	return a.Epoch < b.Epoch || a.Epoch == b.Epoch && a.Version < b.Version
}

// pgQuery is the part of a replicated PG's "pg query" that tells log-based
// recovery from backfill.
type pgQuery struct {
	State  string `json:"state"`
	Up     []int  `json:"up"`
	Acting []int  `json:"acting"`
	Info   struct {
		LastUpdate   string `json:"last_update"`
		LastComplete string `json:"last_complete"`
		LogTail      string `json:"log_tail"`
		Stats        struct {
			StatSum struct {
				Objects   int64 `json:"num_objects"`
				Recovered int64 `json:"num_objects_recovered"`
				Degraded  int64 `json:"num_objects_degraded"`
			} `json:"stat_sum"`
		} `json:"stats"`
	} `json:"info"`
	PeerInfo []struct {
		Peer         string `json:"peer"`
		LastUpdate   string `json:"last_update"`
		LastComplete string `json:"last_complete"`
		LastBackfill string `json:"last_backfill"`
	} `json:"peer_info"`
	RecoveryState []struct {
		Name     string `json:"name"`
		Progress *struct {
			BackfillTargets []string `json:"backfill_targets"`
		} `json:"recovery_progress"`
	} `json:"recovery_state"`
}

func parsePGQuery(data []byte) (pgQuery, error) {
	var query pgQuery
	if err := json.Unmarshal(data, &query); err != nil {
		return query, fmt.Errorf("pg query: %w", err)
	}
	if query.State == "" || query.Info.LastUpdate == "" || query.Info.LogTail == "" {
		return query, fmt.Errorf("pg query without state or log positions: %.256s", data)
	}
	return query, nil
}

// backfillTargets returns the backfill targets of the primary's active state.
func (q pgQuery) backfillTargets() []string {
	for _, state := range q.RecoveryState {
		if state.Progress != nil {
			return state.Progress.BackfillTargets
		}
	}
	return nil
}

// pgShardOSD returns the OSD of a PG shard as "pg query" names it: "1" in a
// replicated pool, "1(2)" for shard 2 in an erasure-coded one.
func pgShardOSD(name string) (int, error) {
	osd, _, _ := strings.Cut(name, "(")
	return strconv.Atoi(osd)
}

// pgRecoveryPath tells, from one query of the primary while recovery is held,
// how it brings replica up to date. stoppedAt is the primary's last_update
// when replica stopped, the last entry replica has. Log recovery keeps
// replica in the acting set and replays the log, which still reaches back to
// stoppedAt; backfill takes replica out of the acting set as a backfill
// target, from the beginning of the PG (last_backfill MIN), because the
// trimmed log starts after stoppedAt. In an erasure-coded pool the replica is
// the OSD of one shard. Anything else is an error.
func pgRecoveryPath(q pgQuery, replica int, stoppedAt eversion) (string, error) {
	tail, err := parseEversion(q.Info.LogTail)
	if err != nil {
		return "", err
	}
	last, err := parseEversion(q.Info.LastUpdate)
	if err != nil {
		return "", err
	}
	if !stoppedAt.less(last) {
		return "", fmt.Errorf("the primary's last_update %s is not past %v: nothing to recover", q.Info.LastUpdate, stoppedAt)
	}
	isReplica := func(name string) bool {
		osd, err := pgShardOSD(name)
		return err == nil && osd == replica
	}
	index := slices.IndexFunc(q.PeerInfo, func(peer struct {
		Peer         string `json:"peer"`
		LastUpdate   string `json:"last_update"`
		LastComplete string `json:"last_complete"`
		LastBackfill string `json:"last_backfill"`
	}) bool {
		return isReplica(peer.Peer)
	})
	if index < 0 {
		return "", fmt.Errorf("osd.%d is not a peer: %+v", replica, q.PeerInfo)
	}
	peer := q.PeerInfo[index]
	targets := q.backfillTargets()
	inUp, inActing := slices.Contains(q.Up, replica), slices.Contains(q.Acting, replica)
	switch {
	case inUp && inActing && len(targets) == 0 && peer.LastBackfill == "MAX" && !stoppedAt.less(tail) &&
		(strings.Contains(q.State, "recovering") || strings.Contains(q.State, "recovery_wait")):
		complete, err := parseEversion(peer.LastComplete)
		if err != nil {
			return "", err
		}
		if !complete.less(last) {
			return "", fmt.Errorf("osd.%d is complete at %s, the primary at %s", replica, peer.LastComplete, q.Info.LastUpdate)
		}
		return "log", nil
	case inUp && !inActing && len(targets) == 1 && isReplica(targets[0]) && peer.LastBackfill == "MIN" && stoppedAt.less(tail) &&
		(strings.Contains(q.State, "backfilling") || strings.Contains(q.State, "backfill_wait")):
		return "backfill", nil
	}
	return "", fmt.Errorf("neither log recovery nor backfill of osd.%d: state %s, up %v, acting %v, backfill targets %v, peer %+v, log tail %s, stopped at %v",
		replica, q.State, q.Up, q.Acting, targets, peer, q.Info.LogTail, stoppedAt)
}

// recoveryObjects writes or reads objects prefix-0 to prefix-(count-1) of
// tc-paths. Reading prints the count and the SHA-256 of the objects'
// contents, concatenated in name order.
const recoveryObjects = `import hashlib, json, rados, sys
mode, prefixes = sys.argv[1], sys.argv[2:]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_osd_op_timeout": "60"}) as cluster:
    with cluster.open_ioctx("tc-paths") as io:
        names = []
        for spec in prefixes:
            prefix, count = spec.split(":")
            names += ["%s-%d" % (prefix, i) for i in range(int(count))]
        if mode == "write":
            for name in names:
                io.write_full(name, name.encode() * 64)
            print(json.dumps({"count": len(names)}))
        else:
            digest = hashlib.sha256()
            for name in sorted(names):
                digest.update(io.read(name, 1 << 20, 0))
            print(json.dumps({"count": len(names), "sha256": digest.hexdigest(), "client": cluster.get_instance_id()}))`

// expectedRecoveryDigest is what recoveryObjects reads back for prefixes.
func expectedRecoveryDigest(prefixes ...string) string {
	var names []string
	for _, spec := range prefixes {
		prefix, count, _ := strings.Cut(spec, ":")
		n, _ := strconv.Atoi(count)
		for i := range n {
			names = append(names, fmt.Sprintf("%s-%d", prefix, i))
		}
	}
	slices.Sort(names)
	digest := sha256.New()
	for _, name := range names {
		digest.Write([]byte(strings.Repeat(name, 64)))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func recoveryRun(t *testing.T, ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, ctr, ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", recoveryObjects}, args...), Timeout: 3 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "recovery objects " + args[0],
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("%s objects: exit %d: %v: %.2048s", args[0], result.ExitCode, err, result.Stderr)
	}
	return lastJSONLine(string(result.Stdout))
}

// readRecoveryObjects reads every object of prefixes with a client of its own
// in the control container and requires the expected bytes.
func readRecoveryObjects(t *testing.T, ctx context.Context, cluster *ceph.Container, prefixes ...string) {
	t.Helper()
	var read struct {
		Count  int    `json:"count"`
		SHA256 string `json:"sha256"`
	}
	out := recoveryRun(t, ctx, cluster, toolkitContainer(t, cluster), append([]string{"read"}, prefixes...)...)
	if err := json.Unmarshal([]byte(out), &read); err != nil || read.SHA256 != expectedRecoveryDigest(prefixes...) {
		t.Fatalf("independent read of %v: %s, %v", prefixes, out, err)
	}
}

func queryPG(t *testing.T, ctx context.Context, cluster *ceph.Container, pgid string) (pgQuery, []byte) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "pg", pgid, "query", "--format", "json")
	if err != nil {
		t.Fatal("pg query:", err)
	}
	query, err := parsePGQuery(out)
	if err != nil {
		t.Fatal(err)
	}
	return query, out
}

// waitPG polls the pool's only PG until ok accepts it.
func waitPG(t *testing.T, parent context.Context, cluster *ceph.Container, what string, ok func(ceph.PGState) bool) ceph.PGState {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		snapshot, err := poolHandle(t, ctx, cluster, "tc-paths").PGs(ctx)
		if err == nil && len(snapshot.PGs) == 1 {
			if ok(snapshot.PGs[0]) {
				return snapshot.PGs[0]
			}
			last = fmt.Sprintf("%s up %v acting %v", snapshot.PGs[0].State, snapshot.PGs[0].Up, snapshot.PGs[0].Acting)
		} else {
			last = fmt.Sprintf("%v, %d PGs", err, len(snapshot.PGs))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PG never %s: %s", what, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// recoveryPhase is the receipt of one held recovery.
type recoveryPhase struct {
	Path            string   `json:"path"`
	Written         int      `json:"written"`
	StoppedAt       string   `json:"stopped_at"`
	HeldState       string   `json:"held_state"`
	HeldUp          []int    `json:"held_up"`
	HeldActing      []int    `json:"held_acting"`
	LogTail         string   `json:"log_tail"`
	PeerBackfill    string   `json:"peer_last_backfill"`
	BackfillTargets []string `json:"backfill_targets"`
	HeldFor         string   `json:"held_for"`
	Recovered       int64    `json:"recovered"`
}

// testOSDRecoveryAndBackfill brings one replica of a one-PG pool up to date
// twice and records which native path the primary took: a two-replica pool,
// or with erasure a K=2, M=1 pool whose "replica" is the OSD of one shard.
// The replica stops while writes continue; with recovery held by a flag, the
// primary's own report decides the path: log recovery while its PG log still
// reaches back to the replica's last entry, backfill once a short log has
// been trimmed past it. The hold must stop progress, the per-PG
// num_objects_recovered count must grow by exactly the objects written while
// the replica was down, and an independent client reads every object. At the
// end the primary stops, and the PG serves every object from the remaining
// OSDs: the replica alone, or the recovered shard with the third one, which
// cannot rebuild an object without it.
func testOSDRecoveryAndBackfill(t *testing.T, erasure bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	width := 2
	pool := ceph.PoolConfig{Name: "tc-paths", PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados"}
	var opts []testcontainers.ContainerCustomizer
	if erasure {
		width = 3
		pool = ceph.PoolConfig{Name: "tc-paths", PGNum: 1, MinSize: 2, Application: "rados", ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}}
		opts = append(opts, ceph.WithOSDCount(3))
	}
	cluster, client := newServiceCluster(t, opts...)
	if _, err := cluster.CreatePool(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryRun(t, ctx, cluster, client, "write", "seed:20")
	clean := waitPG(t, ctx, cluster, "clean on every OSD", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == width && !slices.Contains(pg.Acting, 2147483647)
	})
	pgid, primary := clean.PGID.String(), clean.ActingPrimary
	var victim *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID != primary && slices.Contains(clean.Acting, osd.ID) {
			victim = osd
		}
	}
	if victim == nil {
		t.Fatalf("no replica besides primary osd.%d: %+v", primary, clean)
	}
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := noout.Restore(restoreCtx); err != nil {
			t.Errorf("restore noout: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	stopTimeout := 10 * time.Second
	prefixes := []string{"seed:20"}
	var phases []recoveryPhase
	for _, phase := range []struct {
		path     string
		flag     ceph.OSDFlag
		prefix   string
		count    int
		shortLog bool
	}{
		{"log", ceph.OSDFlagNoRecover, "recovery", 5, false},
		{"backfill", ceph.OSDFlagNoBackfill, "backfill", 60, true},
	} {
		if phase.shortLog {
			// A short log that is trimmed at every write leaves the replica's
			// last entry behind the log tail.
			for name, value := range map[string]string{"osd_min_pg_log_entries": "10", "osd_max_pg_log_entries": "10", "osd_pg_log_trim_min": "1"} {
				overridden, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: name, Value: value})
				t.Cleanup(func() {
					restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
					defer stop()
					if err := overridden.Restore(restoreCtx); err != nil {
						t.Errorf("restore %s: %v", name, err)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		before, _ := queryPG(t, ctx, cluster, pgid)
		stoppedAt, err := parseEversion(before.Info.LastUpdate)
		if err != nil {
			t.Fatal(err)
		}
		if err := victim.Stop(ctx, &stopTimeout); err != nil {
			t.Fatal(err)
		}
		waitPG(t, ctx, cluster, "degraded without the replica", func(pg ceph.PGState) bool {
			return strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Acting, victim.ID)
		})
		spec := fmt.Sprintf("%s:%d", phase.prefix, phase.count)
		recoveryRun(t, ctx, cluster, client, "write", spec)
		prefixes = append(prefixes, spec)
		hold, err := override.SetOSDFlag(ctx, cluster, phase.flag, true)
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := hold.Restore(restoreCtx); err != nil {
				t.Errorf("restore %s: %v", phase.flag, err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := victim.Start(ctx); err != nil {
			t.Fatal(err)
		}
		waitPG(t, ctx, cluster, "held in "+phase.path, func(pg ceph.PGState) bool {
			return slices.Contains(pg.Up, victim.ID) && (strings.Contains(pg.State, "recover") || strings.Contains(pg.State, "backfill"))
		})
		heldAt := time.Now()
		held, raw := queryPG(t, ctx, cluster, pgid)
		path, err := pgRecoveryPath(held, victim.ID, stoppedAt)
		if err != nil || path != phase.path {
			t.Fatalf("held %s: path %q: %v\n%.16384s", phase.path, path, err, raw)
		}
		t.Logf("OSD_RECOVERY_RAW %s pg query: %.16384s", phase.path, raw)
		// The flag holds: no object is recovered while it is set.
		time.Sleep(5 * time.Second)
		still, _ := queryPG(t, ctx, cluster, pgid)
		if still.Info.Stats.StatSum.Recovered != held.Info.Stats.StatSum.Recovered {
			t.Fatalf("%s set, but num_objects_recovered went from %d to %d", phase.flag, held.Info.Stats.StatSum.Recovered, still.Info.Stats.StatSum.Recovered)
		}
		heldFor := time.Since(heldAt)
		if err := hold.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		waitPG(t, ctx, cluster, "clean again", func(pg ceph.PGState) bool {
			return pg.State == "active+clean" && slices.Contains(pg.Acting, victim.ID)
		})
		after, _ := queryPG(t, ctx, cluster, pgid)
		recovered := after.Info.Stats.StatSum.Recovered - held.Info.Stats.StatSum.Recovered
		if recovered != int64(phase.count) {
			t.Fatalf("%s recovered %d objects, want the %d written while osd.%d was down", phase.path, recovered, phase.count, victim.ID)
		}
		readRecoveryObjects(t, ctx, cluster, prefixes...)
		phases = append(phases, recoveryPhase{
			Path: path, Written: phase.count, StoppedAt: before.Info.LastUpdate, HeldState: held.State, HeldUp: held.Up, HeldActing: held.Acting,
			LogTail: held.Info.LogTail, BackfillTargets: held.backfillTargets(), HeldFor: heldFor.Round(time.Second).String(),
			Recovered: recovered,
		})
		for _, peer := range held.PeerInfo {
			if osd, err := pgShardOSD(peer.Peer); err == nil && osd == victim.ID {
				phases[len(phases)-1].PeerBackfill = peer.LastBackfill
			}
		}
	}

	// The replica now holds every object: with the primary stopped, the PG
	// serves them without it.
	var first *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID == primary {
			first = osd
		}
	}
	if err := first.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	waitPG(t, ctx, cluster, "active without the primary", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active") && slices.Contains(pg.Acting, victim.ID) && !slices.Contains(pg.Acting, primary)
	})
	readRecoveryObjects(t, ctx, cluster, prefixes...)
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitPG(t, ctx, cluster, "clean on every OSD again", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == width && !slices.Contains(pg.Acting, 2147483647)
	})
	kind := "replicated size 2"
	if erasure {
		kind = "erasure k=2 m=1"
	}
	receipt, err := json.Marshal(map[string]any{"pool": kind, "pg": pgid, "primary": primary, "replica": victim.ID, "phases": phases})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_RECOVERY_RECEIPT %s", receipt)
}
