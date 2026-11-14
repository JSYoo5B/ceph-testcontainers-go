package apiinventory_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The repository names Ceph images by release or role tag and keeps no
// digest of one (AGENTS.md, "Image references"): no file holds a complete
// sha256 or sha512 digest value, as a digest reference (name@sha256:...), as
// an entry of a digest list or as a local image ID. A compatible rebuild
// under the same tag then needs no change here.
//
// What stays is not a digest value in that form: a content hash written as
// bare hex (source, payload and tool checksums, Git object IDs), a stand-in
// that is no hash at all ("sha256:helper"), a truncated ID in a recorded
// observation ("sha256:2e57651a5aa7…"), a placeholder ("@sha256:...") and a
// synthetic digest of one repeated digit, which tests of digest handling
// use.
func TestRepositoryKeepsNoCephImageDigest(t *testing.T) {
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// The repository's own metadata and the ignored output
			// directories; everything else, dot-directories included.
			if rel == ".git" || rel == "artifacts" || entry.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || strings.HasSuffix(rel, ".test") || rel == "coverage.out" {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.IndexByte(src, 0) >= 0 {
			return nil // not text
		}
		scanned++
		for _, problem := range imageDigestProblems(filepath.ToSlash(rel), src) {
			t.Error(problem)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 1000 {
		t.Fatalf("scanned %d files; the walk did not reach the repository", scanned)
	}
}

// A known-defect experiment case runs on its package-level image variable.
// Its default is the release tag of an image that reproduces the defect, so
// that the case follows a rebuild of that release; a caller injects any
// other reference at build time.
func TestExperimentCaseDefaultsAreReleaseTags(t *testing.T) {
	directory := filepath.Join(root, "internal", "integration", "experiments")
	cases, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, entry := range cases {
		if !entry.IsDir() {
			continue
		}
		files, err := filepath.Glob(filepath.Join(directory, entry.Name(), "*_test.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			src, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			defaults, err := imageDefaults(file, src)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range defaults {
				found++
				if d.problem != "" {
					t.Errorf("%s:%d: the default of image %s", filepath.ToSlash(strings.TrimPrefix(file, root+string(filepath.Separator))), d.line, d.problem)
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no experiment case declares an image variable; the check found nothing to check")
	}
}

// digestValue is a complete digest value wherever it stands: after an "@"
// in a reference, alone as an image ID, or as a value of a list. The digits
// are matched in either case, and a longer run of digits is not a digest.
var digestValue = regexp.MustCompile(`(?i)sha(?:256:[0-9a-f]{64}|512:[0-9a-f]{128})`)

// digestValues returns the complete digest values of text that are not a
// synthetic one of a single repeated digit.
func digestValues(text string) []string {
	var found []string
	for _, at := range digestValue.FindAllStringIndex(text, -1) {
		if at[1] < len(text) && isHex(text[at[1]]) {
			continue // more digits follow: no sha256 or sha512 value
		}
		value := text[at[0]:at[1]]
		digits := value[strings.IndexByte(value, ':')+1:]
		if strings.Count(digits, digits[:1]) == len(digits) {
			continue
		}
		found = append(found, value)
	}
	return found
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// imageDigestProblems reports the digest values of one file. A Go file is
// read twice: as text, which covers its comments, and as the string
// constants its expressions build, so that a value split over a
// concatenation of literals counts like one written in one piece.
func imageDigestProblems(name string, src []byte) []string {
	var problems []string
	text := string(src)
	seen := map[string]bool{}
	for n, line := range strings.Split(text, "\n") {
		for _, value := range digestValues(line) {
			seen[value] = true
			problems = append(problems, name+":"+strconv.Itoa(n+1)+": holds the digest "+value+
				"; name the image by its release or role tag (AGENTS.md, Image references)")
		}
	}
	if !strings.HasSuffix(name, ".go") {
		return problems
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return problems // not Go source that compiles; the text was read
	}
	ast.Inspect(file, func(n ast.Node) bool {
		expr, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		value, ok := stringConstant(expr)
		if !ok {
			return true
		}
		for _, digest := range digestValues(value) {
			if !seen[digest] {
				seen[digest] = true
				problems = append(problems, name+":"+strconv.Itoa(fset.Position(expr.Pos()).Line)+": builds the digest "+digest+
					"; name the image by its release or role tag (AGENTS.md, Image references)")
			}
		}
		return false // the largest constant expression is the value
	})
	return problems
}

// stringConstant evaluates an expression made only of string literals,
// concatenation and parentheses.
func stringConstant(expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(e.Value)
		return value, err == nil
	case *ast.ParenExpr:
		return stringConstant(e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, ok := stringConstant(e.X)
		if !ok {
			return "", false
		}
		right, ok := stringConstant(e.Y)
		return left + right, ok
	}
	return "", false
}

type imageDefault struct {
	line    int
	problem string
}

// imageDefaults returns the package-level image variables of one Go file,
// each with what is wrong with its default, if anything.
func imageDefaults(name string, src []byte) ([]imageDefault, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var defaults []imageDefault
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, ident := range vs.Names {
				if ident.Name != "image" {
					continue
				}
				d := imageDefault{line: fset.Position(ident.Pos()).Line}
				// One value per name; a missing value, or one call that
				// initializes several names, is no string constant.
				if len(vs.Values) != len(vs.Names) {
					d.problem = "is not a string constant"
				} else if value, ok := stringConstant(vs.Values[i]); !ok {
					d.problem = "is not a string constant"
				} else {
					d.problem = releaseTagProblem(value)
				}
				defaults = append(defaults, d)
			}
		}
	}
	return defaults, nil
}

// releaseTag is the tag of a Ceph release image: the upstream v<release>,
// or the <variant>-<release>-<role> of a published role image.
var releaseTag = regexp.MustCompile(`^(?:v[0-9]+\.[0-9]+\.[0-9]+|[a-z]+-[0-9]+\.[0-9]+\.[0-9]+-[a-z]+(?:-[a-z]+)*)$`)

// releaseTagProblem says why reference is not an image named by a release
// tag, or returns "" when it is.
func releaseTagProblem(reference string) string {
	if reference == "" {
		return "is empty"
	}
	if name, digest, ok := strings.Cut(reference, "@"); ok {
		return strconv.Quote(reference) + " names the digest " + strconv.Quote(digest) + " of " + strconv.Quote(name) +
			"; use the release tag (AGENTS.md, Image references)"
	}
	// The tag follows the last colon of the last path component; a colon
	// before a slash belongs to a registry port.
	slash := strings.LastIndexByte(reference, '/')
	if slash < 0 {
		return strconv.Quote(reference) + " is not an image of a registry: it is a local image or an image ID"
	}
	repository, tag, ok := strings.Cut(reference[slash+1:], ":")
	if !ok || repository == "" {
		return strconv.Quote(reference) + " has no tag"
	}
	if !releaseTag.MatchString(tag) {
		return strconv.Quote(reference) + " has the tag " + strconv.Quote(tag) + ", which is not a release tag (v<release> or <variant>-<release>-<role>)"
	}
	return ""
}

func TestImageDigestProblems(t *testing.T) {
	real := strings.Repeat("0123456789abcdef", 4)
	long := strings.Repeat("0123456789abcdef", 8)
	for name, tc := range map[string]struct {
		file string
		src  string
		want int
	}{
		// What the repository must not hold.
		"pinned default":           {"case_test.go", "package p\n\nvar image = \"ghcr.io/o/r@sha256:" + real + "\"\n", 1},
		"tag and digest":           {"README.md", "run `quay.io/ceph/ceph:v20.2.4@sha256:" + real + "` first", 1},
		"digest list":              {"images.json", `{"linux/amd64": "sha256:` + real + `", "linux/arm64": "sha256:` + real[1:] + `0"}`, 2},
		"image ID":                 {"x_test.go", "package p\n\nconst id = \"sha256:" + real + "\"\n", 1},
		"upper case":               {"notes.md", "SHA256:" + strings.ToUpper(real), 1},
		"two on a line":            {"w.yml", "a: r@sha256:" + real + " b: r@sha256:" + real, 2},
		"at the end of the file":   {"w.yml", "image: r@sha256:" + real, 1},
		"sha512":                   {"w.yml", "image: r@sha512:" + long, 1},
		"comment in Go":            {"x.go", "package p\n\n// ran on r@sha256:" + real + "\n", 1},
		"concatenated literals":    {"x_test.go", "package p\n\nvar image = \"ghcr.io/o/r@\" + \"sha256:\" + \"" + real[:32] + "\" + (\"" + real[32:] + "\")\n", 1},
		"split after the prefix":   {"x_test.go", "package p\n\nconst id = \"sha256:\" + `" + real + "`\n", 1},
		"script":                   {"run.py", `IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:` + real + `"`, 1},
		"almost one repeated char": {"x.md", "sha256:" + strings.Repeat("a", 63) + "b", 1},
		// What stays.
		"release tag":             {"case_test.go", "package p\n\nvar image = \"ghcr.io/o/r:official-20.2.4-all\"\n", 0},
		"bare content hash":       {"runner-storage.json", `{"sources_sha256": "` + real + `"}`, 0},
		"checksum input":          {"action.yml", "sha256: " + real, 0},
		"Git object ID":           {"w.yml", "ref: " + real[:40], 0},
		"stand-in":                {"x_test.go", "package p\n\nconst id = \"sha256:helper\"\n", 0},
		"truncated observation":   {"README.md", "`ghcr.io/o/r@sha256:" + real[:12] + "…` (the default image when recorded)", 0},
		"placeholder":             {"guide.md", "pass `ghcr.io/o/r@sha256:...` to Run", 0},
		"synthetic digest":        {"x_test.go", "package p\n\nconst ref = \"ghcr.io/example-org/ceph@sha256:" + strings.Repeat("a", 64) + "\"\n", 0},
		"synthetic concatenation": {"x_test.go", "package p\n\nconst id = \"sha256:\" + \"" + strings.Repeat("1", 64) + "\"\n", 0},
		"built at run time":       {"x_test.go", "package p\n\nimport \"strings\"\n\nvar id = \"sha256:\" + strings.Repeat(\"1\", 64)\n", 0},
		"too short":               {"x.md", "sha256:" + real[:63], 0},
		"too long":                {"x.md", "sha256:" + real + "0", 0},
		"sha512 of sha256 length": {"x.md", "sha512:" + real, 0},
		"validating pattern":      {"x.py", `IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")`, 0},
		"not Go after all":        {"broken.go", "package p\n\nfunc {", 0},
	} {
		if got := imageDigestProblems(tc.file, []byte(tc.src)); len(got) != tc.want {
			t.Errorf("%s: %d problems, want %d: %q", name, len(got), tc.want, got)
		}
	}
	// A problem names the file, the line and the value.
	got := imageDigestProblems("a/b.md", []byte("one\ntwo r@sha256:"+real+"\n"))
	if want := "a/b.md:2: holds the digest sha256:" + real + "; name the image by its release or role tag (AGENTS.md, Image references)"; !slices.Equal(got, []string{want}) {
		t.Errorf("problem = %q, want %q", got, want)
	}
}

func TestImageDefaults(t *testing.T) {
	real := strings.Repeat("0123456789abcdef", 4)
	for name, tc := range map[string]struct {
		decl string
		want string // a part of the problem; "" for an accepted default
	}{
		"published aggregate":  {`var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"`, ""},
		"upstream release":     {`var image = "quay.io/ceph/ceph:v19.1.0"`, ""},
		"role with a hyphen":   {`var image = "ghcr.io/o/r:official-20.2.4-rbd-aux"`, ""},
		"registry with a port": {`var image = "registry.example:5000/ceph/ceph:v20.2.4"`, ""},
		"concatenated tag":     {`var image = "quay.io/ceph/ceph:" + "v20.2.5"`, ""},
		"grouped declaration":  {"var (\n\tother = 1\n\timage = \"quay.io/ceph/ceph:v20.2.5\"\n)", ""},
		"digest":               {`var image = "ghcr.io/o/r@sha256:` + real + `"`, "names the digest"},
		"tag and digest":       {`var image = "quay.io/ceph/ceph:v20.2.4@sha256:` + real + `"`, "names the digest"},
		"split digest":         {`var image = "ghcr.io/o/r@sha256:" + "` + real + `"`, "names the digest"},
		"synthetic digest":     {`var image = "ghcr.io/o/r@sha256:` + strings.Repeat("a", 64) + `"`, "names the digest"},
		"truncated digest":     {`var image = "ghcr.io/o/r@sha256:2e57651a5aa7"`, "names the digest"},
		"image ID":             {`var image = "sha256:` + real + `"`, "not an image of a registry"},
		"short image ID":       {`var image = "2e57651a5aa7"`, "not an image of a registry"},
		"no tag":               {`var image = "quay.io/ceph/ceph"`, "has no tag"},
		"port but no tag":      {`var image = "registry.example:5000/ceph/ceph"`, "has no tag"},
		"local image":          {`var image = "ceph:v20.2.4"`, "not an image of a registry"},
		"moving tag":           {`var image = "quay.io/ceph/ceph:latest"`, "not a release tag"},
		"major only":           {`var image = "quay.io/ceph/ceph:v20"`, "not a release tag"},
		"empty tag":            {`var image = "quay.io/ceph/ceph:"`, "not a release tag"},
		"empty":                {`var image = ""`, "is empty"},
		"no default":           {`var image string`, "not a string constant"},
		"computed":             {"var image = defaultImage()\n\nfunc defaultImage() string { return \"\" }", "not a string constant"},
		"named constant":       {"const tag = \"quay.io/ceph/ceph:v20.2.5\"\n\nvar image = tag", "not a string constant"},
		"two names, one call":  {"var image, other = pair()\n\nfunc pair() (string, string) { return \"\", \"\" }", "not a string constant"},
	} {
		defaults, err := imageDefaults("case_test.go", []byte("package p\n\n"+tc.decl+"\n"))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(defaults) != 1 {
			t.Errorf("%s: %d image variables, want 1", name, len(defaults))
			continue
		}
		if got := defaults[0].problem; tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
			t.Errorf("%s: problem %q, want %q", name, got, tc.want)
		}
	}
	// Only the package-level variable is the case's input.
	local := "package p\n\nfunc f() { var image = \"sha256:x\"; _ = image }\n\nconst image2 = \"r@sha256:x\"\n"
	if defaults, err := imageDefaults("case_test.go", []byte(local)); err != nil || len(defaults) != 0 {
		t.Errorf("a local variable and another name: %v, %v", defaults, err)
	}
}
