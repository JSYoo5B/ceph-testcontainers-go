package cephfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func shutdownMap(id int64, maxMDS int, up string) string {
	return fmt.Sprintf(`{"id":%d,"mdsmap":{"fs_name":"app","max_mds":%d,"up":%s}}`, id, maxMDS, up)
}

func TestRemovalShutdownMap(t *testing.T) {
	// MDSMap::dump uses mds_<rank> keys, not numeric keys or a num_up_mds
	// scalar. Keep these controls shaped like the native producer.
	for _, up := range []string{`{}`, `{"mds_0":4114}`, `{"mds_0":4114,"mds_1":4115}`} {
		if _, err := parseRemovalShutdownState([]byte(shutdownMap(41, 0, up)), "app"); err != nil {
			t.Fatal(err)
		}
	}
	for _, input := range []string{
		`{}`, `null`, `{"id":41,"mdsmap":null}`,
		`{"id":41,"mdsmap":{"fs_name":"app","max_mds":0}}`,
		shutdownMap(41, 0, `null`), shutdownMap(41, 0, `[]`),
		shutdownMap(41, 0, `{"mds_0":"4114"}`), shutdownMap(41, 0, `{"mds_-1":4114}`),
		shutdownMap(41, 0, `{"0":4114}`), shutdownMap(41, 0, `{"mds_00":4114}`),
		shutdownMap(41, 0, `{"bad":4114}`), shutdownMap(41, 0, `{"mds_0":0}`),
		shutdownMap(-1, 0, `{}`), shutdownMap(41, -1, `{}`),
		`{"id":41,"mdsmap":{"fs_name":"app","up":{}}}`,
		`{"mdsmap":{"fs_name":"app","max_mds":0,"up":{}}}`,
		strings.ReplaceAll(shutdownMap(41, 0, `{}`), `"app"`, `"other"`),
	} {
		if _, err := parseRemovalShutdownState([]byte(input), "app"); err == nil {
			t.Errorf("accepted incomplete shutdown map: %s", input)
		}
	}
}

func TestRemoveWaitsForAllUpRanksBeforeCleanup(t *testing.T) {
	var sequence *shutdownSequenceCLI
	cluster, f := cephFSRemovalClusterWithControl(t, true, func(f *cephStateFake) *shutdownSequenceCLI {
		sequence = &shutdownSequenceCLI{cephStateFake: f, maps: []string{
			shutdownMap(41, 1, `{"mds_0":4114}`),
			// A rank can be stopping while no rank is up:active. The explicit
			// up map, not that state's name, must still prevent removal.
			shutdownMap(41, 0, `{"mds_0":4114}`), shutdownMap(41, 0, `{}`),
		}}
		return sequence
	})
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if sequence.reads != 3 || len(f.Filesystems) != 0 || !f.HasPool("attached") {
		t.Fatalf("drain/cleanup state: reads=%d, %s", sequence.reads, f)
	}
	assertNoForcedRemoval(t, f)
}

func TestRemovePreservesOwnedResourcesWhenDrainCannotFinish(t *testing.T) {
	for name, after := range map[string]string{
		"stopping":            shutdownMap(41, 0, `{"mds_0":4114}`),
		"replaced filesystem": shutdownMap(42, 0, `{}`),
		"unset down":          shutdownMap(41, 1, `{}`),
		"absent up map":       `{"id":41,"mdsmap":{"fs_name":"app","max_mds":0}}`,
		"null up map":         shutdownMap(41, 0, `null`),
	} {
		t.Run(name, func(t *testing.T) {
			cluster, f := cephFSRemovalClusterWithControl(t, true, func(f *cephStateFake) *shutdownSequenceCLI {
				return &shutdownSequenceCLI{cephStateFake: f, maps: []string{shutdownMap(41, 1, `{"mds_0":4114}`), after}}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			app := ownedFilesystem(t, cluster, "app")
			if err := app.Remove(ctx); err == nil {
				t.Fatal("unfinished or invalid drain succeeded")
			}
			assertRemovalNotCommitted(t, cluster, f)
		})
	}
}

func TestRemoveCancelledDrainCanBeRetriedWithoutForce(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var sequence *shutdownSequenceCLI
	cluster, f := cephFSRemovalClusterWithControl(t, true, func(f *cephStateFake) *shutdownSequenceCLI {
		sequence = &shutdownSequenceCLI{cephStateFake: f, maps: []string{shutdownMap(41, 1, `{"mds_0":4114}`)}, onDown: cancel}
		return sequence
	})
	defer cancel()
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled removal: %v", err)
	}
	assertRemovalNotCommitted(t, cluster, f)
	sequence.maps, sequence.reads, sequence.onDown = []string{shutdownMap(41, 0, `{}`)}, 0, nil
	if err := app.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertNoForcedRemoval(t, f)
}

func TestRemoveDownFailureDoesNotFallBack(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.FailNext("fs set app down true", 1)
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err == nil {
		t.Fatal("failed shutdown succeeded")
	}
	assertRemovalNotCommitted(t, cluster, f)
}

func TestRemoveReadFailurePreservesOwnershipBeforeAndAfterDown(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			cluster, f := cephFSRemovalClusterWithControl(t, true, func(f *cephStateFake) *shutdownSequenceCLI {
				return &shutdownSequenceCLI{cephStateFake: f, maps: []string{shutdownMap(41, 1, `{"mds_0":4114}`), shutdownMap(41, 0, `{}`)}, onRead: func(n int) {
					if n == failAt {
						f.FailNext("fs get app --format json", 1)
					}
				}}
			})
			app := ownedFilesystem(t, cluster, "app")
			if err := app.Remove(t.Context()); err == nil {
				t.Fatal("failed native map query succeeded")
			}
			assertRemovalNotCommitted(t, cluster, f)
			down := 0
			for _, args := range f.Calls {
				if strings.Join(args, " ") == "fs set app down true" {
					down++
				}
			}
			if down != failAt-1 {
				t.Fatalf("shutdown requests=%d at failed read %d", down, failAt)
			}
		})
	}
}

func TestRemoveRefusesReplacedFilesystemBeforeShutdown(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	filesystems(cluster).byName["app"].nativeIdentity = &cephFSNativeIdentity{id: 99}
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err == nil {
		t.Fatal("replaced filesystem was admitted")
	}
	for _, args := range f.Calls {
		if strings.Join(args, " ") == "fs set app down true" {
			t.Fatal("shutdown dispatched against a replaced filesystem")
		}
	}
	assertRemovalNotCommitted(t, cluster, f)
}

func assertNoForcedRemoval(t *testing.T, f *cephStateFake) {
	t.Helper()
	for _, args := range f.Calls {
		command := strings.Join(args, " ")
		if strings.HasPrefix(command, "fs fail ") || strings.HasPrefix(command, "mds fail ") ||
			command == "fs set app down false" || strings.Contains(command, "down true --yes") {
			t.Fatalf("unexpected force/rollback path: %s", command)
		}
	}
}

func assertRemovalNotCommitted(t *testing.T, cluster *ceph.Container, f *cephStateFake) {
	t.Helper()
	if filesystems(cluster).byName["app"] == nil || len(f.Filesystems) != 1 || !f.HasPool("app-data") || !f.HasPool("app-metadata") || !f.HasPool("attached") {
		t.Fatalf("failed drain lost ownership or storage: %s", f)
	}
	for _, args := range f.Calls {
		command := strings.Join(args, " ")
		if strings.HasPrefix(command, "fs rm ") || strings.HasPrefix(command, "auth del ") || strings.HasPrefix(command, "osd pool rm ") {
			t.Fatalf("cleanup ran before a confirmed drain: %s", command)
		}
	}
	assertNoForcedRemoval(t, f)
}
