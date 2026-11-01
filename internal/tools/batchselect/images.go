package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const imagesSchema = "ceph-test-images/v1"

// value is one string constant that a declaration uses. prefix marks the
// constant left operand of a concatenation with a value only known at run
// time, such as "quay.io/ceph/ceph:v" + version.
type value struct {
	text   string
	prefix bool
	pos    token.Pos
}

// collectValues records, for every node, the string constants it uses: each
// literal, each named constant, also of another package, and each constant
// prefix of a run-time concatenation.
func (g *graph) collectValues(packages [][]*ast.File, info *types.Info) {
	order := make([]int, len(g.nodes))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return g.nodes[order[i]].pos < g.nodes[order[j]].pos })
	g.values = make([][]value, len(g.nodes))
	constantString := func(expr ast.Expr) (string, bool) {
		tv, ok := info.Types[expr]
		if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
			return "", false
		}
		return constant.StringVal(tv.Value), true
	}
	record := func(v value) {
		if n := g.at(order, v.pos); n >= 0 {
			g.values[n] = append(g.values[n], v)
		}
	}
	for _, files := range packages {
		for _, file := range files {
			var visit func(ast.Node) bool
			visit = func(n ast.Node) bool {
				e, ok := n.(ast.Expr)
				if !ok {
					return true
				}
				// The largest constant expression is the value; its operands
				// are parts of it.
				if text, ok := constantString(e); ok {
					record(value{text: text, pos: e.Pos()})
					return false
				}
				if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD {
					if text, ok := constantString(b.X); ok {
						// The constant operand is no value of its own.
						record(value{text: text, prefix: true, pos: b.Pos()})
						ast.Inspect(b.Y, visit)
						return false
					}
				}
				return true
			}
			ast.Inspect(file, visit)
		}
	}
}

// imageReference is a reference that Docker resolves: an optional registry
// host, a repository path, and a tag or a digest. A name without a slash
// (Docker Hub's library images) needs a version tag, so that words like
// "essential:1" never count.
var (
	imageRepository = `[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*`
	imageReference  = regexp.MustCompile(`^(?:(` + `[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)*(?::[0-9]+)?` + `)/)?` +
		`(` + imageRepository + `(?:/` + imageRepository + `)*)` +
		`(?::([A-Za-z0-9_][A-Za-z0-9_.-]{0,127}))?(?:@(sha256:[a-f0-9]{64}))?$`)
	versionTag = regexp.MustCompile(`^v?[0-9]+(?:\.[0-9]+)+$`)
	// A reference prefix is a repository and the start of its tag.
	referencePrefix = regexp.MustCompile(`^[a-z0-9.-]+(?::[0-9]+)?/` + imageRepository + `(?:/` + imageRepository + `)*:[A-Za-z0-9_.-]*$`)
)

// reservedHost reports a host that never serves images: documentation and
// test domains.
func reservedHost(host string) bool {
	host = strings.ToLower(strings.Split(host, ":")[0])
	for _, suffix := range []string{".invalid", ".example", ".test", ".localhost"} {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return host == "localhost" || strings.HasPrefix(host, "example.") || strings.Contains(host, ".example.")
}

// isImage reports whether text is a pullable image reference.
func isImage(text string) bool {
	m := imageReference.FindStringSubmatch(text)
	if m == nil || m[3] == "" && m[4] == "" {
		return false
	}
	host := m[1]
	if host != "" && !strings.ContainsAny(host, ".:") && host != "localhost" {
		// "a/b:c": the first part is a Docker Hub namespace, not a host.
		host = ""
	}
	if host != "" && reservedHost(host) {
		return false
	}
	if host != "" {
		return true
	}
	// Docker Hub: [namespace/]name with a version tag or a digest. Deeper
	// paths and word tags are file paths and placeholders.
	repository := m[2]
	if m[1] != "" {
		repository = m[1] + "/" + repository
	}
	return strings.Count(repository, "/") <= 1 && (m[4] != "" || versionTag.MatchString(m[3]))
}

// isPrefix reports whether text, completed at run time, names an image.
func isPrefix(text string) bool {
	if !referencePrefix.MatchString(text) {
		return false
	}
	return !reservedHost(strings.SplitN(text, "/", 2)[0])
}

// rolePins is the file of pinned role images of every supported release: a
// test that names it reads role images of other releases than its own.
const rolePins = ".github/ceph-role-images.json"

type imageUse struct {
	// Kind is image (Reference is complete), prefix (Reference is completed at
	// run time) or role-pins (the test reads the pinned role images).
	Kind        string   `json:"kind"`
	Reference   string   `json:"reference,omitempty"`
	Declaration string   `json:"declaration"`
	Position    string   `json:"position"`
	Tests       []string `json:"tests"`
}

type imagesResult struct {
	Schema  string     `json:"schema"`
	Package string     `json:"package"`
	Tags    string     `json:"tags"`
	Tests   []string   `json:"tests"`
	Images  []imageUse `json:"images"`
}

// testImages reports the images that the declarations reachable from tests
// name. Images that a library builds from its inputs are the caller's role
// images and are not reported.
func testImages(opts options, tags string, tests []string) (imagesResult, error) {
	out := imagesResult{Schema: imagesSchema, Package: opts.pattern, Tags: tags, Tests: tests,
		Images: []imageUse{}}
	if len(tests) == 0 {
		return out, fmt.Errorf("no tests named")
	}
	pkgDir := path.Clean(strings.TrimPrefix(opts.pattern, "./"))
	g, err := load(opts.root, opts.pattern, opts.with, tags, opts.goos, opts.goarch,
		loadOptions{mixed: true, values: true})
	if err != nil {
		return out, err
	}
	forward := make([][]int, len(g.nodes))
	for to, froms := range g.reverse {
		for _, from := range froms {
			forward[from] = append(forward[from], to)
		}
	}
	uses := map[string]*imageUse{}
	for _, name := range tests {
		root := -1
		for i, n := range g.nodes {
			if n.isEntry && n.display == "func "+name && path.Dir(n.file) == pkgDir {
				root = i
				break
			}
		}
		if root < 0 {
			return out, fmt.Errorf("test %s is not declared in %s under tags %s", name, opts.pattern, tags)
		}
		seen := map[int]bool{root: true}
		queue := []int{root}
		for len(queue) > 0 {
			n := queue[0]
			queue = queue[1:]
			for _, v := range g.values[n] {
				kind := ""
				switch {
				case v.prefix && isPrefix(v.text):
					kind = "prefix"
				case !v.prefix && isImage(v.text):
					kind = "image"
				case !v.prefix && strings.HasSuffix(v.text, rolePins):
					kind = "role-pins"
				default:
					continue
				}
				reference := v.text
				if kind == "role-pins" {
					reference = ""
				}
				key := kind + "\x00" + reference
				use := uses[key]
				if use == nil {
					position := g.fset.Position(v.pos)
					use = &imageUse{Kind: kind, Reference: reference, Declaration: g.nodes[n].display,
						Position: fmt.Sprintf("%s:%d", g.nodes[n].file, position.Line), Tests: []string{}}
					uses[key] = use
				}
				if !slices.Contains(use.Tests, name) {
					use.Tests = append(use.Tests, name)
				}
			}
			for _, to := range forward[n] {
				if !seen[to] {
					seen[to] = true
					queue = append(queue, to)
				}
			}
		}
	}
	for _, use := range uses {
		out.Images = append(out.Images, *use)
	}
	sort.Slice(out.Images, func(i, j int) bool {
		a, b := out.Images[i], out.Images[j]
		return a.Kind+"\x00"+a.Reference < b.Kind+"\x00"+b.Reference
	})
	return out, nil
}
