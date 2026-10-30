package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// monStoreState is the owned MON store volumes of one cluster, by MON name: the
// ownership primitive of docs/internals/mon-volume-storage.md. It sits beside
// osdStorageState and is shaped like it.
//
// In this slice nothing publishes a record: no option, no AddMonitor or Run
// path calls begin, so a cluster holds none and every method that Terminate
// calls returns without a Docker call. The primitive, its tests and its guards
// exist ahead of the first caller.
//
// The type has no mutex. Container.mu guards every field, at the position
// osdStorageState has in the lock order: callers hold it, no method takes a
// lock or calls into Container, and Docker calls run with it held, as
// registerOSD does for the OSD volume. monDocker is written only by tests,
// before the cluster is used.
//
// Terminate order: releaseStores runs after the added and the primary MON
// containers are terminated, which are the volume users; retryStores runs once
// more after the labeled containers are removed, because one of those can be a
// user whose create reply was lost.
//
//testdouble:nil-default-hook
type monStoreState struct {
	// records is published before the volume is created, so a lost create reply
	// stays owned for cleanup, and a record leaves it only when its release or
	// discard succeeded.
	records map[string]*monStoreVolume

	// monDocker replaces the Docker client of the store volumes in tests.
	//testdouble:nil-default-hook
	monDocker func(context.Context) (monStoreDocker, error)
}

// begin allocates and publishes the record of mon before its volume exists.
func (s *monStoreState) begin(mon string) (*monStoreVolume, error) {
	if mon == "" {
		return nil, errors.New("MON store needs a MON name")
	}
	if s.records[mon] != nil {
		return nil, fmt.Errorf("MON %s already has a store; a second one is never allocated", mon)
	}
	r := newMonStoreVolume(mon)
	r.newClient = s.monDocker
	if s.records == nil {
		s.records = map[string]*monStoreVolume{}
	}
	s.records[mon] = r
	return r, nil
}

// store returns the record of mon, or nil.
func (s *monStoreState) store(mon string) *monStoreVolume { return s.records[mon] }

// count returns the number of published records.
func (s *monStoreState) count() int { return len(s.records) }

// names returns the MON names with a record, sorted.
func (s *monStoreState) names() []string {
	return slices.Sorted(func(yield func(string) bool) {
		for name := range s.records {
			if !yield(name) {
				return
			}
		}
	})
}

// volumeName returns the volume the MON mounts, or "" without a record.
func (s *monStoreState) volumeName(mon string) string {
	if r := s.records[mon]; r != nil {
		return r.name
	}
	return ""
}

// discardFresh discards the fresh store of mon after its first generation is
// gone, and forgets the record only when the volume removal succeeded.
func (s *monStoreState) discardFresh(ctx context.Context, mon string) error {
	r := s.records[mon]
	if r == nil {
		return fmt.Errorf("MON %s has no store", mon)
	}
	if err := r.discardFresh(ctx); err != nil {
		return err
	}
	delete(s.records, mon)
	return nil
}

// releaseStores is the first Terminate stage: it releases every store whose
// attachment is confirmed gone, in MON name order, and forgets a record only
// when its release succeeded. A failure does not stop the other stores.
func (s *monStoreState) releaseStores(ctx context.Context) error {
	return s.releaseEach(ctx, false)
}

// retryStores is the second Terminate stage, after the labeled-container scan:
// it repeats the release only when the first stage failed (previous), and
// returns previous otherwise.
func (s *monStoreState) retryStores(ctx context.Context, previous error) error {
	if previous == nil || len(s.records) == 0 {
		return previous
	}
	return s.releaseEach(ctx, true)
}

func (s *monStoreState) releaseEach(ctx context.Context, afterScan bool) error {
	var errs []error
	for _, name := range s.names() {
		if err := s.records[name].release(ctx, afterScan); err != nil {
			errs = append(errs, err)
			continue
		}
		delete(s.records, name)
	}
	return errors.Join(errs...)
}
