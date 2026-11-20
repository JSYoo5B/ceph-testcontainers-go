//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_mirror_demote_restart_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestDemotionListingChecksIdentityAndCompletion(t *testing.T) {
	const original = `[{"id":7,"name":"copy","namespace":{"type":"mirror","state":"demoted","complete":false,"primary_mirror_uuid":"src","primary_snap_id":6}}]`
	snaps, err := decodeDemotionListing([]byte(original))
	if err != nil || len(snaps) != 1 {
		t.Fatalf("decode control: %v, %v", snaps, err)
	}
	for _, tc := range []struct {
		name, listing, wantError string
		complete                 bool
	}{
		{name: "incomplete", listing: original},
		{name: "complete", listing: strings.Replace(original, "false", "true", 1), complete: true},
		{name: "transport text", listing: "rbd: connection lost", wantError: "decode snapshot listing"},
		{name: "null", listing: "null", wantError: "listing is null"},
		{name: "missing", listing: "[]", wantError: "copy 7 is gone"},
		{name: "changed primary snapshot", listing: strings.Replace(original, `"primary_snap_id":6`, `"primary_snap_id":8`, 1), wantError: "changed identity"},
		{name: "changed source mirror", listing: strings.Replace(original, `"src"`, `"other-src"`, 1), wantError: "changed identity"},
		{name: "missing completion", listing: strings.Replace(original, "false", "null", 1), wantError: "no completion field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listing, err := decodeDemotionListing([]byte(tc.listing))
			complete := false
			if err == nil {
				complete, err = demotionComplete(listing, snaps[0])
			}
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || complete {
					t.Fatalf("complete %t, error %v, want %q", complete, err, tc.wantError)
				}
			} else if err != nil || complete != tc.complete {
				t.Fatalf("complete %t, error %v, want complete %t", complete, err, tc.complete)
			}
		})
	}
}

func TestDemotedReplicaAbsenceRequiresAnImageList(t *testing.T) {
	for _, tc := range []struct {
		listing    string
		absent     bool
		unreadable bool
	}{
		{listing: `["other"]`, absent: true},
		{listing: `[]`, absent: true},
		{listing: `["other","removed"]`},
		{listing: `null`, unreadable: true},
		{listing: `[null]`, unreadable: true},
		{listing: `{"images":[]}`, unreadable: true},
	} {
		absent, err := demotedReplicaAbsent([]byte(tc.listing), "removed")
		if absent != tc.absent || (err != nil) != tc.unreadable {
			t.Errorf("listing %s: absent %t, error %v", tc.listing, absent, err)
		}
	}
}

func TestObserveDemotionStateRequiresFullWindowAndCurrentEvidence(t *testing.T) {
	failed := errors.New("transport lost")
	const observation = time.Minute
	for _, tc := range []struct {
		name      string
		parent    time.Duration
		read      func(fullWindow bool) (string, bool, error)
		wantError error
		complete  bool
		full      bool
	}{
		{name: "copy completes", read: func(bool) (string, bool, error) { return "complete", true, nil }, complete: true},
		{name: "incomplete copy positively observed", read: func(bool) (string, bool, error) { return "incomplete", false, nil }, full: true},
		{name: "complete on the confirming observation", read: func(full bool) (string, bool, error) {
			return "latest", full, nil
		}, complete: true, full: true},
		{name: "transport unavailable after restart", read: func(bool) (string, bool, error) { return "", false, failed }, wantError: failed},
		{name: "confirming observation unavailable", read: func(full bool) (string, bool, error) {
			if full {
				return "", false, failed
			}
			return "observed incomplete earlier", false, nil
		}, wantError: failed, full: true},
		{name: "the parent deadline shortened the phase", parent: observation / 2, read: func(bool) (string, bool, error) {
			return "incomplete", false, nil
		}, wantError: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tc.parent != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.parent)
					defer cancel()
				}
				started := time.Now()
				_, got, err := observeDemotionState(ctx, observation, time.Second, 5*time.Second, func(context.Context) (string, bool, error) {
					return tc.read(time.Since(started) >= observation)
				})
				if got != tc.complete || !errors.Is(err, tc.wantError) || (time.Since(started) >= observation) != tc.full {
					t.Fatalf("complete %t, error %v after %s", got, err, time.Since(started))
				}
			})
		})
	}
}

func TestObserveResumedBytesDistinguishesExportFailureFromMismatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const observation = time.Minute
		exportErr := errors.New("destination export failed")
		want := []byte("new bytes")
		for _, tc := range []struct {
			name  string
			data  []byte
			err   error
			match bool
		}{
			{name: "export unavailable", err: exportErr},
			{name: "read stale bytes", data: []byte("old bytes")},
			{name: "read exact bytes", data: want, match: true},
		} {
			got, match, err := observeDemotionState(t.Context(), observation, time.Second, time.Second, func(context.Context) ([]byte, bool, error) {
				return tc.data, tc.err == nil && bytes.Equal(tc.data, want), tc.err
			})
			if match != tc.match || !errors.Is(err, tc.err) || !bytes.Equal(got, tc.data) {
				t.Errorf("%s: bytes %q, match %t, error %v", tc.name, got, match, err)
			}
		}
	})
}
