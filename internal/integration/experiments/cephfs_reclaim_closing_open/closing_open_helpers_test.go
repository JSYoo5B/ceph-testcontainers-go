//go:build all || integration

package cephfs_reclaim_closing_open_test

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"syscall"
)

// closingSession is one entry of the MDS's session listing.
type closingSession struct {
	ID    uint64 `json:"id"`
	State string `json:"state"`
	UUID  string `json:"uuid"`
}

// closingHeld is what the reopening mount had done when the hold ended,
// with the OSD writes still paused.
type closingHeld struct {
	Returned bool             `json:"returned"`
	RC       *int             `json:"rc"`
	Sessions []closingSession `json:"sessions"`
}

// closingFlow is the actor's report of one reclaim flow: a target handle
// mounts with the UUID, a reclaimer RESETs it, adopts the UUID (which
// closes the reclaimer's first session) and mounts again. The overlap flow
// pauses the OSDs before the close and holds the mount while they are
// paused; the control flow does neither.
type closingFlow struct {
	Target            uint64           `json:"target"`
	Reclaimer         uint64           `json:"reclaimer"`
	UUID              string           `json:"uuid"`
	TargetSessions    []closingSession `json:"target_sessions"`
	ReclaimRC         int              `json:"reclaim_rc"`
	ReclaimedSessions []closingSession `json:"reclaimed_sessions"`
	PausedFlags       []string         `json:"paused_flags"`
	CloseSeconds      float64          `json:"close_seconds"`
	ClosingSessions   []closingSession `json:"closing_sessions"`
	Held              *closingHeld     `json:"held"`
	MountRC           int              `json:"mount_rc"`
	SettledSessions   []closingSession `json:"settled_sessions"`
	ClientLog         []string         `json:"client_log"`
}

// closingReport is the actor's whole output.
type closingReport struct {
	Control closingFlow `json:"control"`
	Overlap closingFlow `json:"overlap"`
}

// closingSymptom names the defect when the then step observed it.
const closingSymptom = "an open on the client's own closing session was rejected as a duplicated session uuid"

func closingFind(sessions []closingSession, id uint64) (closingSession, bool) {
	i := slices.IndexFunc(sessions, func(s closingSession) bool { return s.ID == id })
	if i < 0 {
		return closingSession{}, false
	}
	return sessions[i], true
}

func closingHas(sessions []closingSession, id uint64, state, uuid string) bool {
	s, ok := closingFind(sessions, id)
	return ok && s.State == state && s.UUID == uuid
}

func closingLogHas(lines []string, text string) bool {
	return slices.ContainsFunc(lines, func(line string) bool { return strings.Contains(line, text) })
}

// closingReclaimed checks the given steps shared by both flows: the target
// session carried the UUID, the RESET returned 0 and removed it, and the
// reclaimer's first session was open without a UUID.
func closingReclaimed(name string, f closingFlow, uuid string) error {
	if f.UUID != uuid || f.Target == 0 || f.Reclaimer == 0 || f.Target == f.Reclaimer {
		return fmt.Errorf("%s: the flow reports uuid %q, target %d and reclaimer %d", name, f.UUID, f.Target, f.Reclaimer)
	}
	if !closingHas(f.TargetSessions, f.Target, "open", uuid) {
		return fmt.Errorf("%s: the target session is not open with the UUID: %+v", name, f.TargetSessions)
	}
	if f.ReclaimRC != 0 {
		return fmt.Errorf("%s: the RESET returned %d", name, f.ReclaimRC)
	}
	if _, ok := closingFind(f.ReclaimedSessions, f.Target); ok || !closingHas(f.ReclaimedSessions, f.Reclaimer, "open", "") {
		return fmt.Errorf("%s: after the RESET the target session is not gone or the reclaimer's is not open without a UUID: %+v", name, f.ReclaimedSessions)
	}
	return nil
}

// judgeClosingControl checks the control flow: without a paused journal the
// close completes before ceph_set_uuid gives up on it, and the mount opens
// a session that carries the UUID.
func judgeClosingControl(f closingFlow, uuid string, shutdownTimeout float64) error {
	if err := closingReclaimed("control", f, uuid); err != nil {
		return err
	}
	if len(f.PausedFlags) > 0 || f.Held != nil {
		return fmt.Errorf("control: the flow paused the OSDs or held the mount")
	}
	if f.CloseSeconds >= shutdownTimeout-1 || closingLogHas(f.ClientLog, "did not respond to session close") {
		return fmt.Errorf("control: the session close was not answered (%.3f s)", f.CloseSeconds)
	}
	if f.MountRC != 0 || !closingHas(f.SettledSessions, f.Reclaimer, "open", uuid) {
		return fmt.Errorf("control: the mount returned %d with sessions %+v", f.MountRC, f.SettledSessions)
	}
	return nil
}

// judgeClosingCondition checks the condition of the overlap flow: the OSD
// writes were paused, ceph_set_uuid gave up waiting for the close after the
// shutdown timeout and dropped the client's session entry, and the MDS
// still had the reclaimer's session closing.
func judgeClosingCondition(f closingFlow, uuid string, shutdownTimeout float64) error {
	if err := closingReclaimed("overlap", f, uuid); err != nil {
		return err
	}
	if !slices.Contains(f.PausedFlags, "pauserd") || !slices.Contains(f.PausedFlags, "pausewr") {
		return fmt.Errorf("condition not established: the OSD flags are %v", f.PausedFlags)
	}
	if f.CloseSeconds < shutdownTimeout-1 || !closingLogHas(f.ClientLog, "did not respond to session close -- timing out") {
		return fmt.Errorf("condition not established: the session close was answered or did not time out (%.3f s)", f.CloseSeconds)
	}
	if !closingHas(f.ClosingSessions, f.Reclaimer, "closing", "") {
		return fmt.Errorf("condition not established: the reclaimer's session is not closing on the MDS: %+v", f.ClosingSessions)
	}
	if f.Held == nil {
		return errors.New("condition not established: the flow reports no hold")
	}
	return nil
}

var (
	closingRequestClose = regexp.MustCompile(`handle_client_session client_session\(request_close\) from client\.(\d+)\b`)
	closingRequestOpen  = regexp.MustCompile(`handle_client_session client_session\(request_open\) from client\.(\d+)\b`)
	closingCloseLogged  = regexp.MustCompile(`_session_logged client\.(\d+) \S+ state_seq \d+ close \d+`)
	closingReject       = regexp.MustCompile(`client session with duplicated session uuid '([^']*)' denied \(client\.(\d+) `)
)

// closingIndex returns the index of the first line at or after from that re
// matches for client id, or -1.
func closingIndex(lines []string, from int, re *regexp.Regexp, id uint64) int {
	want := fmt.Sprint(id)
	for i := max(from, 0); i < len(lines); i++ {
		if m := re.FindStringSubmatch(lines[i]); m != nil && m[len(m)-1] == want {
			return i
		}
	}
	return -1
}

// closingRejected reports whether the MDS log has a duplicated-UUID refusal
// of the reclaimer with the case's UUID after its session close.
func closingRejected(lines []string, id uint64, uuid string) bool {
	closed := closingIndex(lines, 0, closingRequestClose, id)
	if closed < 0 {
		return false
	}
	for _, line := range lines[closed:] {
		if m := closingReject.FindStringSubmatch(line); m != nil && m[1] == uuid && m[2] == fmt.Sprint(id) {
			return true
		}
	}
	return false
}

// closingRetried reports whether the MDS handled the reclaimer's open after
// its close once while the close was pending and again after the close was
// logged: request_close, request_open, the close's _session_logged and
// request_open, in that order.
func closingRetried(lines []string, id uint64) bool {
	closed := closingIndex(lines, 0, closingRequestClose, id)
	if closed < 0 {
		return false
	}
	first := closingIndex(lines, closed+1, closingRequestOpen, id)
	if first < 0 {
		return false
	}
	logged := closingIndex(lines, first+1, closingCloseLogged, id)
	return logged >= 0 && closingIndex(lines, logged+1, closingRequestOpen, id) >= 0
}

// judgeClosingOverlap decides the then step of the overlap flow from the
// actor's report and the MDS log. The defect is a mount that failed with
// EPERM while the OSDs were paused, with the MDS refusing the open as a
// duplicated session uuid and the client logging that refusal. The contract
// is a mount still waiting when the hold ended, with the session still
// closing, that then succeeded, with the MDS handling the open again after
// the close was logged and no refusal. Anything else is an error: neither
// outcome was observed.
func judgeClosingOverlap(f closingFlow, uuid string, mdsLog []string) (reproduced bool, err error) {
	rejected := closingRejected(mdsLog, f.Reclaimer, uuid)
	h := f.Held
	if h.Returned && h.RC != nil && *h.RC == -int(syscall.EPERM) && f.MountRC == *h.RC &&
		rejected && closingLogHas(f.ClientLog, "rejected us (duplicated session uuid)") {
		return true, nil
	}
	if !h.Returned && closingHas(h.Sessions, f.Reclaimer, "closing", "") && f.MountRC == 0 && !rejected &&
		closingRetried(mdsLog, f.Reclaimer) && closingHas(f.SettledSessions, f.Reclaimer, "open", uuid) {
		return false, nil
	}
	rc := "none"
	if h.RC != nil {
		rc = fmt.Sprint(*h.RC)
	}
	return false, fmt.Errorf("neither the contract nor the defect: held returned=%t rc=%s, mount rc %d, refusal logged %t, open retried %t, held sessions %+v, settled sessions %+v",
		h.Returned, rc, f.MountRC, rejected, closingRetried(mdsLog, f.Reclaimer), h.Sessions, f.SettledSessions)
}
