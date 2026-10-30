//go:build all || integration

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// mdsSnapshotID finds the ID of the snapshot called name on directory inode in
// the MDS's snap table cache ("dump snaps"), where IDs print as decimal
// integers. A table without exactly one such snapshot is an error.
func mdsSnapshotID(data []byte, dirInode uint64, name string) (uint64, error) {
	var table struct {
		Snaps *[]struct {
			SnapID *uint64 `json:"snapid"`
			Ino    *uint64 `json:"ino"`
			Name   *string `json:"name"`
		} `json:"snaps"`
	}
	if err := json.Unmarshal(data, &table); err != nil || table.Snaps == nil {
		return 0, fmt.Errorf("dump snaps has no snaps array (%v): %.256s", err, data)
	}
	var found []uint64
	for _, s := range *table.Snaps {
		if s.SnapID == nil || s.Ino == nil || s.Name == nil {
			return 0, fmt.Errorf("dump snaps entry lacks snapid, ino or name: %.256s", data)
		}
		if *s.Ino == dirInode && *s.Name == name {
			found = append(found, *s.SnapID)
		}
	}
	if len(found) != 1 || found[0] == 0 {
		return 0, fmt.Errorf("dump snaps lists %v for %q on inode 0x%x", found, name, dirInode)
	}
	return found[0], nil
}

// snapRealmShape renders what one dump_cache carries about realms, for the
// log: the raw snaprealm object of the selected HEAD inode, then every parsed
// realm record. It holds only inode numbers, snapshot IDs and sequences, and
// never fails: a parse error is part of the shape.
func snapRealmShape(data []byte, inode uint64) string {
	var entries []json.RawMessage
	if json.Unmarshal(data, &entries) != nil {
		var wrapped struct {
			Cache []json.RawMessage `json:"cache"`
		}
		_ = json.Unmarshal(data, &wrapped)
		entries = wrapped.Cache
	}
	withRealm, inodes := 0, 0
	rawSelected := "none"
	want := fmt.Sprintf("0x%x", inode)
	for _, entry := range entries {
		var obj struct {
			Ino       string          `json:"ino"`
			SnapID    *string         `json:"snapid"`
			SnapRealm json.RawMessage `json:"snaprealm"`
		}
		if json.Unmarshal(entry, &obj) != nil || obj.SnapID == nil {
			continue
		}
		inodes++
		if len(obj.SnapRealm) > 0 && !bytes.Equal(bytes.TrimSpace(obj.SnapRealm), []byte("null")) {
			withRealm++
			if obj.Ino == want && *obj.SnapID == "head" {
				var compact bytes.Buffer
				if json.Compact(&compact, obj.SnapRealm) == nil {
					rawSelected = compact.String()
				}
			}
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "inodes=%d with_snaprealm=%d selected_raw=%s", inodes, withRealm, rawSelected)
	view, err := parseClientSnapRealms(data, inode)
	if err != nil {
		fmt.Fprintf(&out, "; parse: %v", err)
		return out.String()
	}
	ids := make([]uint64, 0, len(view.Records))
	for id := range view.Records {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	fmt.Fprintf(&out, "; selected realm=0x%x records:", view.Realm)
	for _, id := range ids {
		r := view.Records[id]
		fmt.Fprintf(&out, " [0x%x seq=%x parent=0x%x since=%x prior=%x mine=%x]", id, r.Seq, r.Parent, r.ParentSince, r.PriorParentSnaps, r.MySnaps)
	}
	return out.String()
}

// classifySnapRealm names the kind of an observation error.
func classifySnapRealm(err error) string {
	switch {
	case err == nil:
		return "complete"
	case errors.Is(err, errSnapRealmIncomplete):
		return "incomplete"
	case errors.Is(err, errSnapRealmInvalid):
		return "invalid"
	}
	return "unclassified"
}

// errPollDeadline means a bounded observation ran out of time. A response
// that arrives after the deadline is not accepted, even a ready one.
var errPollDeadline = errors.New("bounded observation deadline passed")

// pollClock is the clock of a bounded observation; tests replace it.
type pollClock struct {
	Now  func() time.Time
	Wait func(ctx context.Context, d time.Duration) error
}

var realPollClock = pollClock{
	Now: time.Now,
	Wait: func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	},
}

// boundedPoll calls attempt until it reports ready, fails, or the phase
// deadline (timeout from now) passes. Each attempt gets a context that
// expires at the deadline, so a query cannot outlive it, and the deadline is
// checked again after the attempt returns: a ready response that arrives
// after it is rejected with errPollDeadline. The wait between attempts never
// extends past the deadline. last is the value of the final attempt, also
// when the poll ends in error, for the caller's log.
func boundedPoll[T any](ctx context.Context, clock pollClock, timeout, interval time.Duration, attempt func(context.Context) (T, bool, error)) (last T, looks int, err error) {
	deadline := clock.Now().Add(timeout)
	for {
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 {
			return last, looks, errPollDeadline
		}
		attemptCtx, stop := context.WithTimeout(ctx, remaining)
		value, ready, attemptErr := attempt(attemptCtx)
		stop()
		looks++
		last = value
		if !clock.Now().Before(deadline) {
			return last, looks, errPollDeadline
		}
		if attemptErr != nil {
			return last, looks, attemptErr
		}
		if ready {
			return last, looks, nil
		}
		if err := clock.Wait(ctx, min(interval, deadline.Sub(clock.Now()))); err != nil {
			return last, looks, err
		}
	}
}
