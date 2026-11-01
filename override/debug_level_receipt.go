package override

import "time"

// DebugLevelSetting selects one runtime debug level of one running daemon.
// Daemon is an owned OSD such as "osd.0"; Name is "debug_ms" or "debug_osd";
// Level is "N" or "N/M", each from 0 to 99, as Ceph's config set accepts it.
// Other roles and options are not supported yet.
type DebugLevelSetting struct {
	Daemon, Name, Level string
}

// DebugLevelState is the progress of a DebugLevelHandle. A window
// moves from DebugLevelPriorRead through DebugLevelApplyAttempted to
// DebugLevelApplied or DebugLevelApplyUncertain, and Restore moves it through
// DebugLevelRestoreAttempted to DebugLevelRestored, DebugLevelConflict,
// DebugLevelTargetChanged or DebugLevelUnconfirmed.
type DebugLevelState string

const (
	// DebugLevelPriorRead: the daemon's effective prior level was read.
	DebugLevelPriorRead DebugLevelState = "prior-read"
	// DebugLevelApplyAttempted: the set was issued; its outcome is not
	// known yet.
	DebugLevelApplyAttempted DebugLevelState = "apply-attempted"
	// DebugLevelApplied: the effective readback equals the requested level.
	DebugLevelApplied DebugLevelState = "applied-verified"
	// DebugLevelApplyUncertain: the set or its readback failed or differs;
	// the level may have changed, and Restore reconciles it.
	DebugLevelApplyUncertain DebugLevelState = "apply-uncertain"
	// DebugLevelRestoreAttempted: Restore has started and not settled yet.
	DebugLevelRestoreAttempted DebugLevelState = "restore-attempted"
	// DebugLevelRestored: the effective readback equals the prior level.
	// Terminal.
	DebugLevelRestored DebugLevelState = "restored-verified"
	// DebugLevelConflict: the daemon showed a level that is neither the
	// prior nor the window's own, so it was left as it is. Terminal.
	DebugLevelConflict DebugLevelState = "conflict"
	// DebugLevelTargetChanged: the daemon's container generation was
	// stopped, restarted, replaced or removed, or the cluster terminated, so
	// the window's process is gone and nothing was restored. Terminal.
	DebugLevelTargetChanged DebugLevelState = "target-changed"
	// DebugLevelUnconfirmed: restoration could not be verified, for example
	// with a paused daemon or an unavailable socket; Restore may be retried.
	DebugLevelUnconfirmed DebugLevelState = "unconfirmed"
	// DebugLevelNotApplied: the request was refused before any native
	// step, so nothing was applied and there is nothing to restore. Reason
	// holds the refusal. A window that may have been applied never reports
	// this state.
	DebugLevelNotApplied DebugLevelState = "not-applied"
)

// DebugLevelReceipt is the JSON-serializable record of one window. Requested
// is the caller's text and Applied, Restored and each transition's Readback
// are the daemon's normalized effective values; an empty value was not read.
type DebugLevelReceipt struct {
	Daemon string `json:"daemon"`
	Name   string `json:"name"`
	// Generation is the daemon container's ID and start time that the
	// window was opened on.
	Generation string          `json:"generation"`
	Requested  string          `json:"requested"`
	Prior      string          `json:"prior"`
	Applied    string          `json:"applied,omitempty"`
	Restored   string          `json:"restored,omitempty"`
	State      DebugLevelState `json:"state"`
	// Reason is the refusal text of a DebugLevelNotApplied window and is
	// empty for every other state.
	Reason      string                 `json:"reason,omitempty"`
	Transitions []DebugLevelTransition `json:"transitions"`
}

// DebugLevelTransition is one recorded state change of a window.
type DebugLevelTransition struct {
	State    DebugLevelState `json:"state"`
	At       time.Time       `json:"at"`
	Readback string          `json:"readback,omitempty"`
	Error    string          `json:"error,omitempty"`
}
