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

// TestClockUnlinkFallsBackToANonOwnedSource pins the resolver half of the
// unlink rule that fault's live-file verification enforces: once the owned
// file is gone, the target reads the fallback text, even when that text
// happens to be "+0".
func TestClockUnlinkFallsBackToANonOwnedSource(t *testing.T) {
	in := faketimeInputs{SelectedPath: cfgSelected, UserPath: cfgUser, SystemPath: cfgSystem, Files: cfgFiles("", "", "+0")}
	res := resolveFaketimeSource(in)
	if res.Source != faketimeSourceSystem || res.Text != "+0" {
		t.Fatalf("fallback resolution = %+v", res)
	}
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
