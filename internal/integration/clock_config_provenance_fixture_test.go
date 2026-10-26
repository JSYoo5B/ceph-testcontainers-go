//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

func strPtr(s string) *string { return &s }

const (
	cfgSelected = "owned-faketime.txt"
	cfgUser     = "home-faketimerc"
	cfgSystem   = "system-faketimerc"
)

func cfgFiles(selected, user, system string) map[string]*faketimeFileContent {
	m := map[string]*faketimeFileContent{}
	if selected != "" {
		m[cfgSelected] = &faketimeFileContent{Text: selected}
	}
	if user != "" {
		m[cfgUser] = &faketimeFileContent{Text: user}
	}
	if system != "" {
		m[cfgSystem] = &faketimeFileContent{Text: system}
	}
	return m
}

// TestFaketimeConfigSourceResolution pins the four offline-control rows from
// the research notes plus the remaining fallback order.
func TestFaketimeConfigSourceResolution(t *testing.T) {
	base := func(env *string, files map[string]*faketimeFileContent) faketimeInputs {
		return faketimeInputs{EnvFaketime: env, SelectedPath: cfgSelected, UserPath: cfgUser, SystemPath: cfgSystem, Files: files}
	}
	cases := []struct {
		name string
		in   faketimeInputs
		want faketimeResolution
	}{
		{"owned file, environment unset", base(nil, cfgFiles("+30", "", "")),
			faketimeResolution{FileParsingEnabled: true, Text: "+30", Source: faketimeSourceSelected}},
		{"same file, FAKETIME=+0", base(strPtr("+0"), cfgFiles("+30", "", "")),
			faketimeResolution{EnvMode: true, Text: "+0"}},
		{"empty FAKETIME still disables files", base(strPtr(""), cfgFiles("+30", "", "")),
			faketimeResolution{EnvMode: true, Text: ""}},
		{"selected removed, fallback present", base(nil, cfgFiles("", "", "+120")),
			faketimeResolution{FileParsingEnabled: true, Text: "+120", Source: faketimeSourceSystem}},
		{"selected removed, user file wins over system", base(nil, cfgFiles("", "+60", "+120")),
			faketimeResolution{FileParsingEnabled: true, Text: "+60", Source: faketimeSourceUser}},
		{"owned file restored to +0", base(nil, cfgFiles("+0", "+60", "+120")),
			faketimeResolution{FileParsingEnabled: true, Text: "+0", Source: faketimeSourceSelected}},
		{"no source at all", base(nil, cfgFiles("", "", "")),
			faketimeResolution{FileParsingEnabled: true}},
	}
	for _, c := range cases {
		if got := resolveFaketimeSource(c.in); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}

	// An opened but unreadable file selects that source with empty text; it
	// does not fall through to a later source.
	in := base(nil, cfgFiles("+30", "", "+120"))
	in.Files[cfgSelected].ReadError = true
	want := faketimeResolution{FileParsingEnabled: true, Text: "", Source: faketimeSourceSelected}
	if got := resolveFaketimeSource(in); got != want {
		t.Errorf("read error: got %+v, want %+v", got, want)
	}
}

func cfgOwned() clockOwnedFile {
	return clockOwnedFile{Path: cfgSelected, Identity: clockFileIdentity{HasIdentity: true, Device: 7, Inode: 42}, ExpectedText: "+0"}
}

func cfgObserved(text string) clockObservedFile {
	return clockObservedFile{KnownPresence: true, Present: true, Identity: cfgOwned().Identity, KnownContent: true, Text: text}
}

func cfgEnv() clockTargetEnvironment {
	return clockTargetEnvironment{Known: true, TimestampFile: strPtr(cfgSelected)}
}

func wantVerdict(t *testing.T, name string, got clockProvenanceResult, want clockProvenanceVerdict) {
	t.Helper()
	if got.Verdict != want {
		t.Errorf("%s: verdict %q (%s), want %q", name, got.Verdict, got.Reason, want)
	}
}

func TestClockLiveFileAdmission(t *testing.T) {
	wantVerdict(t, "baseline", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+0")), clockProvenanceValid)
	wantVerdict(t, "trailing newline", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+0\n")), clockProvenanceValid)

	// Environment mode, including the empty value, is invalid.
	for _, v := range []string{"+0", ""} {
		env := cfgEnv()
		env.Faketime = strPtr(v)
		wantVerdict(t, "FAKETIME="+v, admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)
	}

	env := cfgEnv()
	env.TimestampFile = nil
	wantVerdict(t, "selector absent", admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)
	env.TimestampFile = strPtr("other-file.txt")
	wantVerdict(t, "selector names another file", admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)

	// Initial text must be exactly +0: empty, nonzero and wrong ledger fail.
	wantVerdict(t, "empty initial", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("")), clockProvenanceInvalid)
	wantVerdict(t, "nonzero initial", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+30")), clockProvenanceInvalid)
	nz := cfgOwned()
	nz.ExpectedText = "+30"
	wantVerdict(t, "ledger not at zero", admitClockLiveFileMode(cfgEnv(), nz, cfgObserved("+30")), clockProvenanceInvalid)

	// Missing evidence is inconclusive and never decodes into a pass.
	wantVerdict(t, "environment unread", admitClockLiveFileMode(clockTargetEnvironment{}, cfgOwned(), cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without identity", admitClockLiveFileMode(cfgEnv(), clockOwnedFile{Path: cfgSelected, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without path", admitClockLiveFileMode(cfgEnv(), clockOwnedFile{Identity: cfgOwned().Identity, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "presence unobserved", admitClockLiveFileMode(cfgEnv(), cfgOwned(), clockObservedFile{}), clockProvenanceInconclusive)
	noContent := cfgObserved("")
	noContent.KnownContent = false
	wantVerdict(t, "content unread", admitClockLiveFileMode(cfgEnv(), cfgOwned(), noContent), clockProvenanceInconclusive)
	noID := cfgObserved("+0")
	noID.Identity = clockFileIdentity{}
	wantVerdict(t, "identity unobserved", admitClockLiveFileMode(cfgEnv(), cfgOwned(), noID), clockProvenanceInconclusive)
}

func TestClockLiveFileVerification(t *testing.T) {
	wantVerdict(t, "unchanged", verifyClockLiveFileMode(cfgOwned(), cfgObserved("+0")), clockProvenanceValid)

	// A controller update moves the ledger with the file and stays valid.
	upd := cfgOwned()
	upd.ExpectedText = "+30"
	wantVerdict(t, "ledger updated", verifyClockLiveFileMode(upd, cfgObserved("+30")), clockProvenanceValid)
	// A file that did not take the update is drift, not success.
	wantVerdict(t, "update not applied", verifyClockLiveFileMode(upd, cfgObserved("+0")), clockProvenanceInvalid)

	gone := verifyClockLiveFileMode(cfgOwned(), clockObservedFile{KnownPresence: true, Present: false})
	wantVerdict(t, "unlinked", gone, clockProvenanceInvalid)
	if !gone.FallbackInEffect {
		t.Error("unlinked file must report that a non-owned source may apply")
	}
	if r := verifyClockLiveFileMode(cfgOwned(), cfgObserved("+0")); r.FallbackInEffect {
		t.Error("present owned file must not report a fallback")
	}

	swapped := cfgObserved("+0")
	swapped.Identity = clockFileIdentity{HasIdentity: true, Device: 7, Inode: 43}
	wantVerdict(t, "replaced with same text", verifyClockLiveFileMode(cfgOwned(), swapped), clockProvenanceInvalid)

	wantVerdict(t, "presence unobserved", verifyClockLiveFileMode(cfgOwned(), clockObservedFile{}), clockProvenanceInconclusive)
	noContent := cfgObserved("")
	noContent.KnownContent = false
	wantVerdict(t, "content unread", verifyClockLiveFileMode(cfgOwned(), noContent), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without identity", verifyClockLiveFileMode(clockOwnedFile{Path: cfgSelected, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
}

// TestClockUnlinkIsNotRestoration ties the model to the resolver: once the
// owned file is gone, the target reads the fallback text, and the verifier must
// not report a valid fixture even when the fallback text happens to be "+0".
func TestClockUnlinkIsNotRestoration(t *testing.T) {
	in := faketimeInputs{SelectedPath: cfgSelected, UserPath: cfgUser, SystemPath: cfgSystem, Files: cfgFiles("", "", "+0")}
	res := resolveFaketimeSource(in)
	if res.Source != faketimeSourceSystem || res.Text != "+0" {
		t.Fatalf("fallback resolution = %+v", res)
	}
	got := verifyClockLiveFileMode(cfgOwned(), clockObservedFile{KnownPresence: true, Present: false})
	wantVerdict(t, "fallback +0", got, clockProvenanceInvalid)
}

// TestFaketimeReaderParity pins the pinned reader boundaries: the text ends at
// 255 bytes or at the first NUL, carriage returns are dropped and comment
// lines are skipped. prepare_config_contents tests *write_position, which
// holds not-yet-overwritten input, against a line feed. Expectations for the
// quirky cases come from tracing the pinned function by hand and were not
// compiled or run.
func TestFaketimeReaderParity(t *testing.T) {
	long := strings.Repeat("1", 301)
	cases := []struct{ name, raw, want string }{
		{"trailing newline", "+30\n", "+30"},
		{"crlf keeps the line feed the in-place write test lets through", "+30\r\n", "+30\n"},
		{"comment line leaves its line feed", "# note\n+30\n", "\n+30\n"},
		{"in-place write test drops a later byte", ";x\n+5", "\n+"},
		{"comment only after first column kept", "+1 # x", "+1 # x"},
		{"301 bytes truncated to 255", long, long[:255]},
		{"255 bytes kept", long[:255], long[:255]},
		{"NUL ends the text", "+7\x00+9", "+7"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		in := faketimeInputs{SelectedPath: cfgSelected, Files: map[string]*faketimeFileContent{cfgSelected: {Text: c.raw}}}
		got := resolveFaketimeSource(in)
		if got.Text != c.want {
			t.Errorf("%s: parser text %q, want %q", c.name, got.Text, c.want)
		}
	}
	// The environment value is delivered unchanged.
	in := faketimeInputs{EnvFaketime: strPtr("+30\n")}
	if got := resolveFaketimeSource(in); got.Text != "+30\n" {
		t.Errorf("environment text altered: %q", got.Text)
	}
}
