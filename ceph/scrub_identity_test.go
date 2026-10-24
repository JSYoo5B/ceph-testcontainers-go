package ceph

import (
	"slices"
	"strings"
	"testing"
)

// objectIdentity is the (namespace, object) pair a native command addresses.
type objectIdentity struct{ namespace, object string }

// osdMapIdentity mirrors the pinned OSDMonitor.cc "osd map" handler
// (7f793731f1b39eb4f465e960113d2363c311b964): the object word is the literal
// object and the namespace is a separate argument, which this package does not
// supply.
func osdMapIdentity(word string) objectIdentity {
	return objectIdentity{namespace: "", object: word}
}

// injectDataErrIdentity mirrors the pinned OSD.cc injectdataerr handler: the
// first slash splits the word into namespace and object.
func injectDataErrIdentity(word string) objectIdentity {
	if i := strings.IndexByte(word, '/'); i >= 0 {
		return objectIdentity{namespace: word[:i], object: word[i+1:]}
	}
	return objectIdentity{object: word}
}

var injectIdentityNames = []string{
	"victim", "ns/victim", "/victim", "dir/sub/victim",
	"victim/", "ns/", "/", "//", "a//b", "a/b/",
}

// TestInjectObjectNameIdentityMatchesNativeParsers reproduces the identity
// table of the native parsers and requires every name accepted by the given
// validator to address the same identity in the preflight and the injection.
func TestInjectObjectNameIdentityMatchesNativeParsers(t *testing.T) {
	// Characterization of the divergence for the names of the issue table.
	for word, want := range map[string][2]objectIdentity{
		"victim":         {{"", "victim"}, {"", "victim"}},
		"ns/victim":      {{"", "ns/victim"}, {"ns", "victim"}},
		"/victim":        {{"", "/victim"}, {"", "victim"}},
		"dir/sub/victim": {{"", "dir/sub/victim"}, {"dir", "sub/victim"}},
		"victim/":        {{"", "victim/"}, {"victim", ""}},
		"ns/":            {{"", "ns/"}, {"ns", ""}},
	} {
		if got := osdMapIdentity(word); got != want[0] {
			t.Fatalf("%q: osd map identity %v, want %v", word, got, want[0])
		}
		if got := injectDataErrIdentity(word); got != want[1] {
			t.Fatalf("%q: injectdataerr identity %v, want %v", word, got, want[1])
		}
	}
	if err := acceptedIdentitiesMatch(validateInjectObjectName); err != "" {
		t.Fatal(err)
	}
}

// acceptedIdentitiesMatch returns a description of the first name that the
// validator accepts although the two native commands would address different
// identities, or "" when none does.
func acceptedIdentitiesMatch(validate func(string) error) string {
	for _, name := range injectIdentityNames {
		if validate(name) == nil && osdMapIdentity(name) != injectDataErrIdentity(name) {
			return "accepted " + name + " with diverging preflight and injection identity"
		}
	}
	return ""
}

// TestInjectObjectNameIdentityControlFailsForTheGeneralValidator is the
// negative control: the unchanged general validator accepts diverging names,
// so the identity check above would fail if the slash rule were removed.
func TestInjectObjectNameIdentityControlFailsForTheGeneralValidator(t *testing.T) {
	if acceptedIdentitiesMatch(validateObjectName) == "" {
		t.Fatal("control did not detect the diverging identities of the general validator")
	}
}

func TestInjectObjectNameRefusesEverySlashFormAndKeepsPlainNames(t *testing.T) {
	for _, name := range injectIdentityNames {
		err := validateInjectObjectName(name)
		if strings.Contains(name, "/") != (err != nil) {
			t.Fatalf("%q: unexpected validation result %v", name, err)
		}
	}
}

func TestInjectObjectDataErrorRefusesSlashNamesBeforeAnyNativeCommand(t *testing.T) {
	for _, name := range injectIdentityNames {
		if !strings.Contains(name, "/") {
			continue
		}
		ctr := scrubFixture()
		ctr.Output["osd map fixture "+name+" --format json"] = ctr.Output["osd map fixture victim --format json"]
		err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", name, 0)
		if err == nil || len(ctr.Calls) != 0 || len(scrubCommands(ctr)) != 0 {
			t.Fatalf("%q: slash name reached the native CLI: %v %v", name, err, ctr.Calls)
		}
	}
	ctr := scrubFixture()
	if err := poolFixtureCluster(ctr, 2).InjectObjectDataError(t.Context(), "fixture", "victim", 0); err != nil {
		t.Fatal(err)
	}
	want := []string{"tell osd.0 config set bluestore_debug_inject_read_err true", "tell osd.0 injectdataerr fixture victim"}
	if !slices.Equal(scrubCommands(ctr), want) {
		t.Fatalf("plain name changed the injection commands: %v", scrubCommands(ctr))
	}
}
