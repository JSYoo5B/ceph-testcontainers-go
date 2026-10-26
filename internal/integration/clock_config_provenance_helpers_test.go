//go:build all || integration

package integration_test

import (
	"fmt"
	"strings"
)

// This file models, without a clock, a process or Docker, which configuration
// source a libfaketime v0.9.12 target (3ccdd344aadf1e317156fa5fb7c881f2c4404778)
// reads, and when a controller may treat a timestamp file as the live,
// owned control channel of one target process. It qualifies configuration
// provenance only. It does not measure an offset, does not show that the
// interposer reaches a clock route and is not evidence of restoration.
//
// Source facts the model follows (src/libfaketime.c at the pinned commit):
//
//   - Initialization calls getenv("FAKETIME"). When the variable is present,
//     even with an empty value, it parses that text and disables file parsing.
//     Otherwise it reads the configuration file.
//   - The file reader tries FAKETIME_TIMESTAMP_FILE, then $HOME/.faketimerc,
//     then /etc/faketimerc, and uses the first path that opens. An unavailable
//     selected path therefore falls back to a non-owned source.
//   - A read error on the opened file yields empty text, not a failure.

// faketimeFallbackSource names the file that supplied the configuration text.
type faketimeFallbackSource string

const (
	faketimeSourceNone     faketimeFallbackSource = ""
	faketimeSourceSelected faketimeFallbackSource = "selected"
	faketimeSourceUser     faketimeFallbackSource = "user"
	faketimeSourceSystem   faketimeFallbackSource = "system"
)

// faketimeFileContent is one file as the target's open and read would see it.
// A nil entry in faketimeInputs.Files means the open fails.
type faketimeFileContent struct {
	Text      string
	ReadError bool
}

// faketimeInputs are the facts that decide the source. EnvFaketime is a
// pointer so that a present-but-empty value differs from an absent one.
type faketimeInputs struct {
	EnvFaketime  *string
	SelectedPath string
	UserPath     string
	SystemPath   string
	Files        map[string]*faketimeFileContent
}

// faketimeResolution is what the pinned initialization hands to the parser.
type faketimeResolution struct {
	// EnvMode is true when FAKETIME is present; file parsing is then off.
	EnvMode bool
	// FileParsingEnabled mirrors parse_config_file after initialization.
	FileParsingEnabled bool
	// Text is the configuration text delivered to the parser.
	Text string
	// Source is the file that supplied Text when EnvMode is false.
	Source faketimeFallbackSource
}

// resolveFaketimeSource reproduces the source selection of ftpl_really_init and
// read_config_file for ordinary files.
func resolveFaketimeSource(in faketimeInputs) faketimeResolution {
	if in.EnvFaketime != nil {
		return faketimeResolution{EnvMode: true, Text: *in.EnvFaketime}
	}
	res := faketimeResolution{FileParsingEnabled: true}
	for _, c := range []struct {
		path string
		src  faketimeFallbackSource
	}{
		{in.SelectedPath, faketimeSourceSelected},
		{in.UserPath, faketimeSourceUser},
		{in.SystemPath, faketimeSourceSystem},
	} {
		f := in.Files[c.path]
		if c.path == "" || f == nil {
			continue
		}
		res.Source = c.src
		if !f.ReadError {
			res.Text = f.Text
		}
		return res
	}
	return res
}

// clockFileIdentity is the device and inode of the controller's owned file as
// captured at creation. HasIdentity false means the capture failed.
type clockFileIdentity struct {
	HasIdentity bool
	Device      uint64
	Inode       uint64
}

// clockOwnedFile is the controller's record of the file it created for one
// target. ExpectedText is the text the controller last wrote.
type clockOwnedFile struct {
	Path         string
	Identity     clockFileIdentity
	ExpectedText string
}

// clockTargetEnvironment is the environment of the target process itself, read
// from the target and not from the launcher's intent. Known false means it
// could not be read.
type clockTargetEnvironment struct {
	Known         bool
	Faketime      *string
	TimestampFile *string
}

// clockObservedFile is the owned path as observed now. KnownPresence false and
// KnownContent false keep an unreadable observation out of the evidence.
type clockObservedFile struct {
	KnownPresence bool
	Present       bool
	Identity      clockFileIdentity
	KnownContent  bool
	Text          string
}

type clockProvenanceVerdict string

const (
	clockProvenanceValid        clockProvenanceVerdict = "valid"
	clockProvenanceInvalid      clockProvenanceVerdict = "invalid"
	clockProvenanceInconclusive clockProvenanceVerdict = "inconclusive"
)

// clockProvenanceResult is a configuration-provenance verdict. It carries no
// claim about the clock: even a valid result at "+0" says only that the target
// reads the owned file and that file holds the text the controller wrote.
type clockProvenanceResult struct {
	Verdict clockProvenanceVerdict
	Reason  string
	// FallbackInEffect is set when the owned file is gone and the target's
	// next read would use a non-owned source.
	FallbackInEffect bool
}

func provenanceResult(v clockProvenanceVerdict, format string, args ...any) clockProvenanceResult {
	return clockProvenanceResult{Verdict: v, Reason: fmt.Sprintf(format, args...)}
}

// admitClockLiveFileMode decides whether a target may be used as a live-file
// clock fixture. The target environment must be read from the target, must not
// contain FAKETIME (even empty), must name exactly the owned file, and the
// owned file must start with exactly "+0" and a captured identity.
func admitClockLiveFileMode(env clockTargetEnvironment, owned clockOwnedFile, initial clockObservedFile) clockProvenanceResult {
	if owned.Path == "" || !owned.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file path or identity was not captured")
	}
	if !env.Known {
		return provenanceResult(clockProvenanceInconclusive, "target environment was not read")
	}
	if env.Faketime != nil {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME is present in the target environment, which disables file parsing")
	}
	if env.TimestampFile == nil {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME_TIMESTAMP_FILE is absent from the target environment")
	}
	if *env.TimestampFile != owned.Path {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME_TIMESTAMP_FILE does not name the owned file")
	}
	if owned.ExpectedText != "+0" {
		return provenanceResult(clockProvenanceInvalid, "controller ledger does not start at +0")
	}
	if r, done := checkOwnedFileState(owned, initial); done {
		return r
	}
	return provenanceResult(clockProvenanceValid, "target reads the owned file, which holds %q", owned.ExpectedText)
}

// verifyClockLiveFileMode re-checks a previously admitted target. The expected
// text comes from the controller's ledger, so a legitimate update is not
// reported as drift. An unlinked or replaced file invalidates the fixture and
// is never described as restoration.
func verifyClockLiveFileMode(owned clockOwnedFile, now clockObservedFile) clockProvenanceResult {
	if owned.Path == "" || !owned.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file path or identity was not captured")
	}
	if r, done := checkOwnedFileState(owned, now); done {
		return r
	}
	return provenanceResult(clockProvenanceValid, "owned file is present with the ledger text %q", owned.ExpectedText)
}

// checkOwnedFileState returns a terminal result when the observation is
// missing, shows removal, replacement or drift.
func checkOwnedFileState(owned clockOwnedFile, o clockObservedFile) (clockProvenanceResult, bool) {
	if !o.KnownPresence {
		return provenanceResult(clockProvenanceInconclusive, "owned file presence was not observed"), true
	}
	if !o.Present {
		r := provenanceResult(clockProvenanceInvalid, "owned file is gone; unlinking is not restoration and the target may read a non-owned source")
		r.FallbackInEffect = true
		return r, true
	}
	if !o.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file identity was not observed"), true
	}
	if o.Identity != owned.Identity {
		return provenanceResult(clockProvenanceInvalid, "owned file was replaced (device or inode changed)"), true
	}
	if !o.KnownContent {
		return provenanceResult(clockProvenanceInconclusive, "owned file content was not read"), true
	}
	if strings.TrimSuffix(o.Text, "\n") != owned.ExpectedText {
		return provenanceResult(clockProvenanceInvalid, "owned file text %q differs from ledger text %q", o.Text, owned.ExpectedText), true
	}
	return clockProvenanceResult{}, false
}
