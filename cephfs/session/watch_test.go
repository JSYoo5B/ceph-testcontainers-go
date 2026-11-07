package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// watchFake is the native side of the filesystem "app" (FSMap ID 7, pools 1
// and 2) as the session watch reads it: a scripted sequence of active-rank
// reads, the session list of each rank and the FSMap and pools that the
// identity check reads after a failed rank read. The test changes it while
// the watch goroutine reads it.
type watchFake struct {
	*clustertest.CannedCommandContainer
	mu sync.Mutex
	// rosters answers successive rank reads; the last one repeats.
	rosters []rosterReply
	// sessions are the session rows of each rank.
	sessions map[int][]string
	// failRank fails the session listing of that rank; -1 fails none.
	failRank int
	// fsDump is the FSMap of the identity check.
	fsDump string
	core   *fsbase.Core
	// unregistered makes the registry no longer hold the core.
	unregistered bool
}

type rosterReply struct {
	ranks *fsbase.ActiveRanks
	err   error
}

const appFSDump = `{"filesystems":[{"id":7,"mdsmap":{"fs_name":"app","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{}}}],"standbys":[]}`

func newWatchFake(t *testing.T) *watchFake {
	t.Helper()
	f := &watchFake{CannedCommandContainer: &clustertest.CannedCommandContainer{Output: map[string]string{
		"osd pool ls detail --format json": `[{"pool_id":1,"pool_name":"metadata","type":1,"size":1,"min_size":1,"pg_num":8},{"pool_id":2,"pool_name":"data","type":1,"size":1,"min_size":1,"pg_num":8}]`,
	}}, sessions: map[int][]string{}, failRank: -1, fsDump: appFSDump}
	cluster := testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	descriptor := "app"
	f.core = fsbase.NewCore(fsbase.Params{Cluster: cluster, Name: "app", DescriptorName: &descriptor, SetupMu: &sync.Mutex{},
		MetadataPool: "metadata", DataPool: "data",
		Live: func(*fsbase.Core) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return !f.unregistered
		},
		ActiveRanks: f.activeRanks})
	f.core.ReplaceNative(&fsbase.NativeIdentity{ID: 7, MetadataPool: 1, DefaultPool: 2})
	return f
}

func (f *watchFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Output["fs dump --format json"] = f.fsDump
	f.Fail = ""
	for rank, rows := range f.sessions {
		f.Output[listCommand(rank)] = "[" + strings.Join(rows, ",") + "]"
	}
	if f.failRank >= 0 {
		f.Fail = listCommand(f.failRank)
	}
	return f.CannedCommandContainer.Exec(ctx, args, opts...)
}

func listCommand(rank int) string {
	return fmt.Sprintf("tell mds.app:%d session ls --format json", rank)
}

func (f *watchFake) activeRanks(context.Context) (*fsbase.ActiveRanks, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reply := f.rosters[0]
	if len(f.rosters) > 1 {
		f.rosters = f.rosters[1:]
	}
	if reply.err != nil {
		return nil, reply.err
	}
	ranks := *reply.ranks
	ranks.Active = slices.Clone(reply.ranks.Active)
	return &ranks, nil
}

func (f *watchFake) update(change func(*watchFake)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f)
}

// set makes every later rank read return ranks and rank r list rows.
func (f *watchFake) set(ranks *fsbase.ActiveRanks, sessions map[int][]string) {
	f.update(func(f *watchFake) {
		f.rosters = []rosterReply{{ranks: ranks}}
		f.sessions = sessions
	})
}

// roster is the FSMap ID 7 topology with max_mds maxMDS and the given
// holders, each "rank:GID".
func roster(maxMDS int, holders ...string) *fsbase.ActiveRanks {
	ranks := &fsbase.ActiveRanks{FilesystemID: 7, MaxMDS: maxMDS}
	for _, holder := range holders {
		var rank int
		var gid uint64
		if _, err := fmt.Sscanf(holder, "%d:%d", &rank, &gid); err != nil {
			panic(err)
		}
		ranks.Active = append(ranks.Active, fsbase.ActiveRank{Rank: rank, Name: fmt.Sprintf("mds%d", gid), GID: gid, State: "up:active", Owned: true})
	}
	return ranks
}

// row is the session ls entry of client id at nonce in state.
func row(id uint64, nonce int, state string) string {
	return fmt.Sprintf(`{"id":%d,"state":%q,"num_caps":3,"entity":{"addr":{"addr":"172.19.0.7:0","nonce":%d}},"client_metadata":{"entity_id":"admin","hostname":"client","pid":"41","root":"/"}}`, id, state, nonce)
}

func address(nonce int) string { return fmt.Sprintf("172.19.0.7:0/%d", nonce) }

func keys(sessions []Session) []string {
	var out []string
	for _, s := range sessions {
		out = append(out, fmt.Sprintf("%d/%d/%s/%s", s.Rank, s.ID, s.Address, s.State))
	}
	return out
}

func mustRead(t *testing.T, s *sampler) sample {
	t.Helper()
	out, err := s.read(t.Context())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return out
}

func gapRead(t *testing.T, s *sampler, want string) sample {
	t.Helper()
	out, err := s.read(t.Context())
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("read error = %v, want a gap containing %q", err, want)
	}
	var ended watchEnded
	if errors.As(err, &ended) {
		t.Fatalf("a gap ended the watch: %v", err)
	}
	if out.sessions != nil || out.removed != nil || out.added != nil {
		t.Fatalf("a gap reported sessions: %+v", out)
	}
	return out
}

func TestSessionWatchReportsAdmissionStateChangeAndRemovalOnAStableRoster(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open")}})
	s := newSampler(f.core, nil)
	base := mustRead(t, s)
	if got := keys(base.sessions); !slices.Equal(got, []string{"0/10/" + address(1) + "/open"}) || base.added != nil || base.rebaselined != nil {
		t.Fatalf("baseline = %+v", base)
	}
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "stale"), row(11, 2, "opening")}})
	next := mustRead(t, s)
	if got := keys(next.added); !slices.Equal(got, []string{"0/11/" + address(2) + "/opening"}) {
		t.Fatalf("added = %q", got)
	}
	if len(next.changed) != 1 || next.changed[0].PreviousState != "open" || next.changed[0].Session.State != "stale" || next.removed != nil {
		t.Fatalf("changed = %+v, removed = %+v", next.changed, next.removed)
	}
	// The MDS evicts client 10; the same daemon no longer lists it.
	f.set(roster(1, "0:101"), map[int][]string{0: {row(11, 2, "open")}})
	evicted := mustRead(t, s)
	if got := keys(evicted.removed); !slices.Equal(got, []string{"0/10/" + address(1) + "/stale"}) || evicted.rebaselined != nil {
		t.Fatalf("removed = %q, rebaselined = %v", got, evicted.rebaselined)
	}
	// A reconnect under a new nonce is another session.
	f.set(roster(1, "0:101"), map[int][]string{0: {row(11, 3, "open")}})
	reconnected := mustRead(t, s)
	if !slices.Equal(keys(reconnected.removed), []string{"0/11/" + address(2) + "/open"}) || !slices.Equal(keys(reconnected.added), []string{"0/11/" + address(3) + "/open"}) {
		t.Fatalf("reconnect: removed %q, added %q", keys(reconnected.removed), keys(reconnected.added))
	}
}

func TestSessionWatchTreatsARosterChangeDuringTheSampleAsAGap(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open"), row(11, 2, "open")}})
	s := newSampler(f.core, nil)
	mustRead(t, s)
	// The takeover happens between the two rank reads: the old daemon is
	// still named before, the replacement after, and the listing in between
	// reaches the replacement while it has reconnected only one client.
	f.update(func(f *watchFake) {
		f.rosters = []rosterReply{{ranks: roster(1, "0:101")}, {ranks: roster(1, "0:202")}}
		f.sessions = map[int][]string{0: {row(10, 1, "open")}}
	})
	gap := gapRead(t, s, "active ranks changed")
	if !slices.Equal(gap.before, []RankHolder{{Rank: 0, Name: "mds101", GID: 101}}) || !slices.Equal(gap.after, []RankHolder{{Rank: 0, Name: "mds202", GID: 202}}) {
		t.Fatalf("gap rosters = %+v / %+v", gap.before, gap.after)
	}
	// The replacement's first coherent sample rebaselines rank 0: client 11,
	// which has not reconnected yet, is not removed.
	f.set(roster(1, "0:202"), map[int][]string{0: {row(10, 1, "open")}})
	resumed := mustRead(t, s)
	if !slices.Equal(resumed.rebaselined, []int{0}) || resumed.removed != nil || resumed.added != nil || resumed.changed != nil {
		t.Fatalf("resumed = %+v", resumed)
	}
	// From then on the replacement's own list is diffed.
	f.set(roster(1, "0:202"), map[int][]string{0: {row(10, 1, "open"), row(11, 2, "open")}})
	if added := mustRead(t, s).added; !slices.Equal(keys(added), []string{"0/11/" + address(2) + "/open"}) {
		t.Fatalf("added after the rebaseline = %q", keys(added))
	}
}

func TestSessionWatchTreatsAnEmptyRosterAsAGapNotADeparture(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open")}})
	s := newSampler(f.core, nil)
	mustRead(t, s)
	// During a takeover no rank is active and nothing lists the session.
	f.set(roster(1), map[int][]string{})
	gapRead(t, s, "no rank is active")
	f.set(roster(0), map[int][]string{})
	gapRead(t, s, "no rank is active")
	// The same daemon comes back: nothing changed across the gaps.
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open")}})
	if resumed := mustRead(t, s); resumed.removed != nil || resumed.added != nil || resumed.rebaselined != nil {
		t.Fatalf("resumed = %+v", resumed)
	}
}

func TestSessionWatchRequiresEveryRankOfAMultiRankFilesystem(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(2, "0:101", "1:102"), map[int][]string{0: {row(10, 1, "open")}, 1: {row(10, 1, "open"), row(11, 2, "open")}})
	s := newSampler(f.core, nil)
	if base := mustRead(t, s); len(base.sessions) != 3 {
		t.Fatalf("baseline = %q", keys(base.sessions))
	}
	// Rank 1 fails: only rank 0 can be listed.
	f.set(roster(2, "0:101"), map[int][]string{0: {row(10, 1, "open")}})
	gapRead(t, s, "rank 1 of max_mds 2 is not active")
	// Rank 1 is replaced between the two rank reads; rank 0 stays.
	f.update(func(f *watchFake) {
		f.rosters = []rosterReply{{ranks: roster(2, "0:101")}, {ranks: roster(2, "0:101", "1:303")}}
	})
	gapRead(t, s, "active ranks changed")
	// A coherent sample with the replacement: rank 1 is rebaselined, but
	// rank 0, held by the same daemon throughout, still reports its removal.
	f.set(roster(2, "0:101", "1:303"), map[int][]string{0: {}, 1: {row(11, 2, "open")}})
	resumed := mustRead(t, s)
	if !slices.Equal(resumed.rebaselined, []int{1}) || !slices.Equal(keys(resumed.removed), []string{"0/10/" + address(1) + "/open"}) || resumed.added != nil {
		t.Fatalf("resumed = rebaselined %v, removed %q, added %q", resumed.rebaselined, keys(resumed.removed), keys(resumed.added))
	}
	// A failed listing of one rank is a gap that removes nothing.
	f.update(func(f *watchFake) { f.failRank = 1; f.sessions = map[int][]string{0: {}} })
	gapRead(t, s, "rank 1")
	f.update(func(f *watchFake) { f.failRank = -1; f.sessions = map[int][]string{0: {}, 1: {row(11, 2, "open")}} })
	if again := mustRead(t, s); again.removed != nil || again.added != nil {
		t.Fatalf("after a failed listing = %+v", again)
	}
	// max_mds lowered to 1: rank 1 stops being active and is rebaselined.
	f.set(roster(1, "0:101"), map[int][]string{0: {}})
	if stopped := mustRead(t, s); !slices.Equal(stopped.rebaselined, []int{1}) || stopped.removed != nil {
		t.Fatalf("stopped rank = %+v", stopped)
	}
	f.update(func(f *watchFake) {
		f.rosters = []rosterReply{{ranks: roster(1, "0:101")}, {ranks: roster(2, "0:101")}}
	})
	gapRead(t, s, "max_mds changed")
}

func TestSessionWatchSelectsClientsByID(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open"), row(11, 2, "open")}})
	s := newSampler(f.core, []uint64{11, 12})
	if base := mustRead(t, s); !slices.Equal(keys(base.sessions), []string{"0/11/" + address(2) + "/open"}) {
		t.Fatalf("selected baseline = %q", keys(base.sessions))
	}
	f.set(roster(1, "0:101"), map[int][]string{0: {row(12, 3, "open")}})
	next := mustRead(t, s)
	if !slices.Equal(keys(next.added), []string{"0/12/" + address(3) + "/open"}) || !slices.Equal(keys(next.removed), []string{"0/11/" + address(2) + "/open"}) {
		t.Fatalf("selected diff: added %q, removed %q", keys(next.added), keys(next.removed))
	}
}

func TestSessionWatchEndsOnAReplacedOrRemovedFilesystem(t *testing.T) {
	ended := func(t *testing.T, f *watchFake, want string) {
		t.Helper()
		_, err := newSampler(f.core, nil).read(t.Context())
		var e watchEnded
		if !errors.As(err, &e) || !strings.Contains(err.Error(), want) {
			t.Fatalf("read error = %v, want the end of the watch with %q", err, want)
		}
	}
	t.Run("recreated", func(t *testing.T) {
		f := newWatchFake(t)
		replaced := roster(1, "0:101")
		replaced.FilesystemID = 8
		f.set(replaced, nil)
		ended(t, f, "FSMap ID 8 instead of 7")
	})
	t.Run("absent", func(t *testing.T) {
		f := newWatchFake(t)
		f.update(func(f *watchFake) {
			f.rosters = []rosterReply{{err: errors.New(`cephfs filesystem "app" is absent from FSMap`)}}
			f.fsDump = `{"filesystems":[],"standbys":[]}`
		})
		ended(t, f, "absent from FSMap")
	})
	t.Run("unregistered", func(t *testing.T) {
		f := newWatchFake(t)
		f.set(roster(1, "0:101"), nil)
		f.update(func(f *watchFake) { f.unregistered = true })
		ended(t, f, "active filesystem created by this cluster")
	})
	// A failed rank read with the filesystem still in the FSMap is a gap.
	f := newWatchFake(t)
	f.update(func(f *watchFake) { f.rosters = []rosterReply{{err: errors.New("fs dump timed out")}} })
	gapRead(t, newSampler(f.core, nil), "fs dump timed out")
}

// receive waits for the next event of w.
func receive(t *testing.T, w *Watcher) WatchEvent {
	t.Helper()
	select {
	case e, ok := <-w.Events():
		if !ok {
			t.Fatalf("the watch ended: %v", w.Stop())
		}
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	return WatchEvent{}
}

func TestSessionWatchStreamsGapsAcrossATakeoverWithoutRemovals(t *testing.T) {
	f := newWatchFake(t)
	f.set(roster(1, "0:101"), map[int][]string{0: {row(10, 1, "open"), row(11, 2, "open")}})
	settings, err := watch.Validate(watch.MinInterval, time.Second, 0, watchDefaultInterval)
	if err != nil {
		t.Fatal(err)
	}
	w, err := startWatch(t.Context(), settings, "watch CephFS sessions of app", newSampler(f.core, nil).read)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()
	base := receive(t, w)
	if base.Kind != ceph.WatchBaseline || base.Sequence != 1 || len(base.Sessions) != 2 || base.MaxMDS != 1 {
		t.Fatalf("baseline = %+v", base)
	}
	base.Sessions[0].State = "edited"
	f.set(roster(1), map[int][]string{})
	gap := receive(t, w)
	if gap.Kind != ceph.WatchGap || gap.Err == nil || gap.Sessions != nil || gap.Removed != nil || gap.Before == nil {
		t.Fatalf("gap = %+v", gap)
	}
	f.set(roster(1, "0:202"), map[int][]string{0: {row(10, 1, "open"), row(11, 2, "reconnecting")}})
	var resumed WatchEvent
	for resumed = receive(t, w); resumed.Kind == ceph.WatchGap; resumed = receive(t, w) {
	}
	if resumed.Kind != ceph.WatchResumed || !slices.Equal(resumed.Rebaselined, []int{0}) || resumed.Removed != nil || resumed.Changed != nil || len(resumed.Sessions) != 2 {
		t.Fatalf("resumed = %+v", resumed)
	}
	f.set(roster(1, "0:202"), map[int][]string{0: {row(10, 1, "open")}})
	removed := receive(t, w)
	if removed.Kind != ceph.WatchChanged || !slices.Equal(keys(removed.Removed), []string{"0/11/" + address(2) + "/reconnecting"}) || removed.Sequence != resumed.Sequence+1 {
		t.Fatalf("removal = %+v", removed)
	}
	// Caps alone cause no event; the next one is the replaced filesystem.
	replaced := roster(1, "0:202")
	replaced.FilesystemID = 9
	f.set(replaced, map[int][]string{0: {strings.Replace(row(10, 1, "open"), `"num_caps":3`, `"num_caps":7`, 1)}})
	select {
	case e, ok := <-w.Events():
		if ok {
			t.Fatalf("unexpected event %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced filesystem did not end the watch")
	}
	if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "FSMap ID 9") {
		t.Fatalf("Stop = %v", err)
	}
}

func TestSessionWatchRefusesInvalidRequests(t *testing.T) {
	if _, err := Watch(t.Context(), nil, ceph.WatchConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil filesystem: %v", err)
	}
	fs := &cephfs.Filesystem{}
	if _, err := List(t.Context(), fs); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("list with unconstructed filesystem: %v", err)
	}
	if _, err := Watch(t.Context(), fs, ceph.WatchConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("watch with unconstructed filesystem: %v", err)
	}
	if _, err := Watch(t.Context(), fs, ceph.WatchConfig{Interval: time.Millisecond}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("short interval: %v", err)
	}
	if _, err := Watch(t.Context(), fs, ceph.WatchConfig{}, 4, 0); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("client 0: %v", err)
	}
	var unstarted *Watcher
	if err := unstarted.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Stop of a nil watcher: %v", err)
	}
	if err := (&Watcher{}).Stop(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Stop of an unstarted watcher: %v", err)
	}
	// A baseline that is not coherent starts nothing.
	f := newWatchFake(t)
	f.set(roster(1), nil)
	settings, _ := watch.Validate(0, time.Second, 0, watchDefaultInterval)
	if w, err := startWatch(t.Context(), settings, "watch", newSampler(f.core, nil).read); w != nil || err == nil {
		t.Fatalf("incoherent baseline = %v, %v", w, err)
	}
}
