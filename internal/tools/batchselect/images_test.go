package main

import (
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func imagesOf(t *testing.T, tests ...string) map[string]imageUse {
	t.Helper()
	root, err := filepath.Abs("testdata/images")
	if err != nil {
		t.Fatal(err)
	}
	out, err := testImages(options{root: root, pattern: "./pkg", with: []string{"harness"}, goos: runtime.GOOS,
		goarch: runtime.GOARCH}, "", tests)
	if err != nil {
		t.Fatal(err)
	}
	if out.Schema != imagesSchema || !slices.Equal(out.Tests, tests) {
		t.Fatalf("result = %+v", out)
	}
	found := map[string]imageUse{}
	for _, use := range out.Images {
		found[use.Kind+" "+use.Reference] = use
	}
	return found
}

func TestImagesFollowTheTestsDeclarations(t *testing.T) {
	old := "image quay.io/ceph/ceph:v20.2.3@sha256:d195020de02512030118e772cef7859e92904e91eb4cb21acb503f8b94118137"
	for name, test := range map[string]struct {
		tests []string
		want  []string
	}{
		// A constant of the shared package.
		"constant": {tests: []string{"TestOld"}, want: []string{old}},
		// A prefix completed at run time, and the pinned role file.
		"run time": {tests: []string{"TestRelease"}, want: []string{"prefix quay.io/ceph/ceph:v", "role-pins "}},
		// A constant of a library imported from export data, and a function
		// of the package itself that its external tests import; no
		// placeholder counts.
		"library and own package": {tests: []string{"TestLibrary"},
			want: []string{"image ghcr.io/example-org/helper:extension-v1", "image hashicorp/vault:1.21.4"}},
		// A test of the package's own test files.
		"own test": {tests: []string{"TestOwn"}, want: []string{"image busybox:1.37.0"}},
	} {
		t.Run(name, func(t *testing.T) {
			found := imagesOf(t, test.tests...)
			got := []string{}
			for key, use := range found {
				got = append(got, key)
				if !slices.Equal(use.Tests, test.tests) || use.Position == "" || use.Declaration == "" {
					t.Errorf("%s: %+v", key, use)
				}
			}
			slices.Sort(got)
			slices.Sort(test.want)
			if !slices.Equal(got, test.want) {
				t.Fatalf("images = %v, want %v", got, test.want)
			}
		})
	}
}

func TestImagesAreMergedAcrossTestsAndRefuseUnknownTests(t *testing.T) {
	found := imagesOf(t, "TestOld", "TestLibrary")
	if len(found) != 3 || !strings.HasPrefix(found["image hashicorp/vault:1.21.4"].Position, "pkg/pkg.go:") {
		t.Fatalf("images = %+v", found)
	}
	root, _ := filepath.Abs("testdata/images")
	if _, err := testImages(options{root: root, pattern: "./pkg", goos: runtime.GOOS, goarch: runtime.GOARCH}, "",
		[]string{"TestMissing"}); err == nil || !strings.Contains(err.Error(), "TestMissing") {
		t.Fatalf("err = %v", err)
	}
}

func TestImageReferenceShapes(t *testing.T) {
	for text, want := range map[string]bool{
		"busybox:1.37.0": true, "hashicorp/vault:1.21.4": true, "ghcr.io/a/b:tag": true,
		"ghcr.io/a/b@sha256:" + strings.Repeat("a", 64): true, "localhost:5000/a:b": false,
		"registry.example/ceph:20.2.4": false, "example.invalid/x:y": false, "example/ceph:version": false,
		"sys/dev/block/253:4": false, "essential:1": false, "busybox": false, "quay.io/ceph/ceph": false,
		"a b:1.0": false, "": false,
	} {
		if got := isImage(text); got != want {
			t.Errorf("isImage(%q) = %t, want %t", text, got, want)
		}
	}
	for text, want := range map[string]bool{"quay.io/ceph/ceph:v": true, "ghcr.io/a/b:": true,
		"quay.io/ceph/ceph": false, "registry.example/ceph:": false, "v": false} {
		if got := isPrefix(text); got != want {
			t.Errorf("isPrefix(%q) = %t, want %t", text, got, want)
		}
	}
}
