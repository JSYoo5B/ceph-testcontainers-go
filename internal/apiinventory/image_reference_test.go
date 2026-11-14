package apiinventory_test

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// The repository names Ceph images by release or role tag and keeps no
// digest of one (AGENTS.md, "Image references"): no tracked file holds a
// complete sha256 or sha512 digest of an image, as a digest reference
// (name@sha256:...), as an entry of a digest list or as a local image ID. A
// compatible rebuild under the same tag then needs no change here.
//
// A digest value is an image's when it is written as one: after the "@" of
// a reference, or under a label that names an image (image, digest,
// manifest, an ID, a platform such as linux/amd64). The same spelling under
// another label is a checksum of something else (a payload, a tool download,
// a source file), which the rule leaves alone. What also stays is a content
// hash written as bare hex, a stand-in that is no hash at all
// ("sha256:helper"), a truncated ID in a recorded observation
// ("sha256:2e57651a5aa7…"), a placeholder ("@sha256:...") and a synthetic
// digest of one repeated digit, which tests of digest handling use.
//
// Only the repository's inputs are read: the files Git tracks. A log or a
// Docker inspection saved beside the sources is not one of them.
func TestRepositoryKeepsNoCephImageDigest(t *testing.T) {
	files, err := trackedFiles(root)
	if err != nil {
		if os.Getenv("CI") == "" {
			t.Skipf("not a Git checkout, so there are no repository inputs to read: %v", err)
		}
		t.Fatal(err)
	}
	scanned := 0
	for _, rel := range files {
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if errors.Is(err, fs.ErrNotExist) {
			continue // tracked, and deleted in this working tree
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.IndexByte(src, 0) >= 0 {
			continue // not text
		}
		scanned++
		for _, problem := range imageDigestProblems(rel, src) {
			t.Error(problem)
		}
	}
	if scanned < 1000 {
		t.Fatalf("scanned %d files; the listing did not reach the repository", scanned)
	}
}

// trackedFiles lists the files Git tracks under directory, as slash paths
// relative to it. Untracked and ignored files are not repository inputs.
func trackedFiles(directory string) ([]string, error) {
	out, err := exec.Command("git", "-C", directory, "ls-files", "-z", "--cached").Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, fmt.Errorf("git ls-files in %s: %w: %s", directory, err, bytes.TrimSpace(exit.Stderr))
		}
		return nil, fmt.Errorf("git ls-files in %s: %w", directory, err)
	}
	var files []string
	for _, name := range strings.Split(string(out), "\x00") {
		if name != "" {
			files = append(files, name)
		}
	}
	return files, nil
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

// imageLabel matches a label that names an image or a part of its identity.
var imageLabel = regexp.MustCompile(`(?i)image|digest|manifest|\bids?\b|\b(?:linux|windows|darwin)/[a-z0-9]+`)

// digestValues returns the complete digest values of line that are an
// image's and not a synthetic one of a single repeated digit. label is what
// the line stands under when it carries no label of its own (the key of a
// list whose entries are one per line); it may be empty.
func digestValues(line, label string) []string {
	var found []string
	for _, at := range digestValue.FindAllStringIndex(line, -1) {
		if at[1] < len(line) && isHex(line[at[1]]) {
			continue // more digits follow: no sha256 or sha512 value
		}
		value := line[at[0]:at[1]]
		digits := value[strings.IndexByte(value, ':')+1:]
		if strings.Count(digits, digits[:1]) == len(digits) {
			continue
		}
		if !namesImage(line[:at[0]], label) {
			continue
		}
		found = append(found, value)
	}
	return found
}

// namesImage reports whether a digest value that follows before on its line
// is written as an image's. An "@" directly in front makes it the digest of
// a reference. Otherwise its own label decides: the text between the value
// and the separator in front of it (a comma, a brace or a semicolon). A
// value with no label of its own, an entry of a list, takes the label the
// line stands under.
func namesImage(before, label string) bool {
	if strings.HasSuffix(before, "@") {
		return true
	}
	if cut := strings.LastIndexAny(before, ",{;"); cut >= 0 {
		before = before[cut+1:]
	}
	if strings.ContainsFunc(before, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }) {
		return imageLabel.MatchString(before)
	}
	return imageLabel.MatchString(label)
}

// parentLabels returns, for each line, the nearest line above it that is
// indented less and is not blank: the key a list entry or a nested value
// stands under in YAML, JSON written one entry per line, or a composite
// literal. A line with no such line above has an empty label.
func parentLabels(lines []string) []string {
	indent := func(line string) int { return len(line) - len(strings.TrimLeft(line, " \t")) }
	labels := make([]string, len(lines))
	for n, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		for up := n - 1; up >= 0; up-- {
			if strings.TrimSpace(lines[up]) != "" && indent(lines[up]) < indent(line) {
				labels[n] = lines[up]
				break
			}
		}
	}
	return labels
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// imageDigestProblems reports the image digests of one file. A Go file is
// read twice: as text, which covers its comments, and as the string
// constants its expressions build, so that a value split over a
// concatenation of literals counts like one written in one piece.
func imageDigestProblems(name string, src []byte) []string {
	var problems []string
	text := string(src)
	seen := map[string]bool{}
	lines := strings.Split(text, "\n")
	labels := parentLabels(lines)
	for n, line := range lines {
		for _, value := range digestValues(line, labels[n]) {
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
		// The constant's label is what stands in front of it on its line:
		// the name it is assigned to, or the key of its entry.
		at := fset.Position(expr.Pos())
		label := ""
		if at.Line >= 1 && at.Line <= len(lines) && at.Column >= 1 && at.Column-1 <= len(lines[at.Line-1]) {
			label = lines[at.Line-1][:at.Column-1]
		}
		for _, digest := range digestValues(label+value, labels[at.Line-1]) {
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
		"upper case":               {"notes.md", "Image ID: SHA256:" + strings.ToUpper(real), 1},
		"upper case reference":     {"notes.md", "ran R@SHA256:" + strings.ToUpper(real), 1},
		"two on a line":            {"w.yml", "a: r@sha256:" + real + " b: r@sha256:" + real, 2},
		"at the end of the file":   {"w.yml", "image: r@sha256:" + real, 1},
		"sha512":                   {"w.yml", "image: r@sha512:" + long, 1},
		"comment in Go":            {"x.go", "package p\n\n// ran on r@sha256:" + real + "\n", 1},
		"concatenated literals":    {"x_test.go", "package p\n\nvar image = \"ghcr.io/o/r@\" + \"sha256:\" + \"" + real[:32] + "\" + (\"" + real[32:] + "\")\n", 1},
		"split after the prefix":   {"x_test.go", "package p\n\nconst id = \"sha256:\" + `" + real + "`\n", 1},
		"script":                   {"run.py", `IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:` + real + `"`, 1},
		"almost one repeated char": {"x.md", "r@sha256:" + strings.Repeat("a", 63) + "b", 1},
		"entry of a digest list":   {"images.yml", "digests:\n  - sha256:" + real + "\n  - \"sha256:" + real + "\"\n", 2},
		"entry under a platform":   {"images.json", "{\n  \"linux/amd64\": [\n    \"sha256:" + real + "\"\n  ]\n}\n", 1},
		"image ID in a log line":   {"notes.md", "docker inspect reported Id sha256:" + real, 1},
		"image field after others": {"row.json", `{"payload_sha256": "sha256:` + real + `", "image_id": "sha256:` + real + `"}`, 1},
		"list of image IDs in Go":  {"x_test.go", "package p\n\nvar imageIDs = []string{\n\t\"sha256:" + real + "\",\n}\n", 1},
		"split ID under a label":   {"x_test.go", "package p\n\nvar images = map[string]string{\n\t\"linux/amd64\": \"sha256:\" + \"" + real + "\",\n}\n", 1},
		// What stays.
		"release tag":              {"case_test.go", "package p\n\nvar image = \"ghcr.io/o/r:official-20.2.4-all\"\n", 0},
		"bare content hash":        {"runner-storage.json", `{"sources_sha256": "` + real + `"}`, 0},
		"checksum input":           {"action.yml", "sha256: " + real, 0},
		"payload checksum":         {"notes.md", "payload checksum: sha256:" + real, 0},
		"tool checksum":            {"action.yml", "    tool-checksum: \"sha256:" + real + "\"", 0},
		"checksum constant":        {"x_test.go", "package p\n\nconst payloadSum = \"sha256:\" + \"" + real + "\"\n", 0},
		"entry of a checksum list": {"sums.yml", "checksums:\n  - sha256:" + real + "\n", 0},
		"checksum after an image":  {"row.json", `{"image": "quay.io/ceph/ceph:v20.2.4", "payload": "sha256:` + real + `"}`, 0},
		"unlabelled value":         {"notes.md", "sha256:" + real, 0},
		"Git object ID":            {"w.yml", "ref: " + real[:40], 0},
		"stand-in":                 {"x_test.go", "package p\n\nconst id = \"sha256:helper\"\n", 0},
		"truncated observation":    {"README.md", "`ghcr.io/o/r@sha256:" + real[:12] + "…` (the default image when recorded)", 0},
		"placeholder":              {"guide.md", "pass `ghcr.io/o/r@sha256:...` to Run", 0},
		"synthetic digest":         {"x_test.go", "package p\n\nconst ref = \"ghcr.io/example-org/ceph@sha256:" + strings.Repeat("a", 64) + "\"\n", 0},
		"synthetic concatenation":  {"x_test.go", "package p\n\nconst id = \"sha256:\" + \"" + strings.Repeat("1", 64) + "\"\n", 0},
		"built at run time":        {"x_test.go", "package p\n\nimport \"strings\"\n\nvar id = \"sha256:\" + strings.Repeat(\"1\", 64)\n", 0},
		"too short":                {"x.md", "sha256:" + real[:63], 0},
		"too long":                 {"x.md", "sha256:" + real + "0", 0},
		"sha512 of sha256 length":  {"x.md", "sha512:" + real, 0},
		"validating pattern":       {"x.py", `IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")`, 0},
		"not Go after all":         {"broken.go", "package p\n\nfunc {", 0},
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

// The guard reads what Git tracks and nothing else: a tracked pin is a
// problem, a tracked payload checksum is none, and an untracked diagnostic
// with a real image digest in it is not read at all.
func TestTrackedFilesAreTheRepositoryInputs(t *testing.T) {
	real := strings.Repeat("0123456789abcdef", 4)
	directory := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(directory, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) {
		t.Helper()
		if out, err := exec.Command("git", append([]string{"-C", directory}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	write("case/pinned_test.go", "package p\n\nvar image = \"ghcr.io/o/r@sha256:"+real+"\"\n")
	write("docs/payload.md", "payload checksum: sha256:"+real+"\n")
	write(".hidden/list.json", `{"linux/amd64": "sha256:`+real+`"}`+"\n")
	git("init", "-q")
	git("add", "-A")
	write("inspect.log", `"Id": "sha256:`+real+`", "RepoDigests": ["ghcr.io/o/r@sha256:`+real+`"]`+"\n")
	write("case/scratch/notes.txt", "image r@sha256:"+real+"\n")

	files, err := trackedFiles(directory)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(files)
	if want := []string{".hidden/list.json", "case/pinned_test.go", "docs/payload.md"}; !slices.Equal(files, want) {
		t.Fatalf("tracked files = %q, want %q", files, want)
	}
	problems := map[string]int{}
	for _, rel := range files {
		src, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		problems[rel] = len(imageDigestProblems(rel, src))
	}
	if problems[".hidden/list.json"] != 1 || problems["case/pinned_test.go"] != 1 || problems["docs/payload.md"] != 0 {
		t.Errorf("problems per tracked file = %v, want the pin and the list entry only", problems)
	}

	// Outside a checkout there is nothing to list, and that is an error
	// rather than an empty repository.
	if files, err := trackedFiles(t.TempDir()); err == nil {
		t.Errorf("a directory that is no checkout listed %q", files)
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
