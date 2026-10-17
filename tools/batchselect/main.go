// Command batchselect reports which test batches of one package a change
// can affect. It maps the diff since a base commit to the top-level
// declarations of the package's test build, and keeps a batch when one of
// its roots (its tests, and every init function and package-level var
// initializer) reaches a changed declaration in a reference graph that
// go/types resolves. Anything it cannot attribute to a declaration selects
// every batch, with the reason.
//
// By default the checkout under -root is the head, and the diff is taken
// from the merge base of -base and HEAD to the working tree. With -head,
// -root holds that commit's tree, exported from the repository -repo.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"slices"
	"sort"
	"strings"
)

const schema = "ceph-batch-selection/v1"

type batchSpec struct {
	ID   string `json:"id"`
	Tags string `json:"tags"`
}

type batchResult struct {
	ID       string `json:"id"`
	Selected bool   `json:"selected"`
	Reason   string `json:"reason"`
}

type result struct {
	Schema    string `json:"schema"`
	Package   string `json:"package"`
	Base      string `json:"base"`
	MergeBase string `json:"merge_base"`
	// Fallback lists why every batch is selected; it is empty when the
	// selection comes from the graph.
	Fallback []string      `json:"fallback"`
	Ignored  []string      `json:"ignored_files"`
	Analysed []string      `json:"analysed_files"`
	Changed  []string      `json:"changed_declarations"`
	Batches  []batchResult `json:"batches"`
	Selected int           `json:"selected"`
	Total    int           `json:"total"`
}

// options are what one selection reads besides the repository.
type options struct {
	root, base, pattern, unionTags, goos, goarch string
	// repo is the git repository; head, when set, is the commit whose tree
	// root holds, which then need not be a checkout.
	repo, head string
	batches    []batchSpec
}

func git(root string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", root, "-c", "core.quotePath=false"}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// classify sorts a changed path: ignored (no batch reads it), analysed (a
// Go file of the package), or a reason to select every batch.
func classify(pkgDir string, file fileDiff) (ignored, analysed bool, reason string) {
	p := file.path
	switch {
	case strings.HasSuffix(p, ".md") || strings.HasPrefix(p, "docs/") || p == "LICENSE":
		return true, false, ""
	case p == "go.mod" || p == "go.sum":
		return false, false, p + " changed"
	case strings.HasPrefix(p, ".github/"):
		return false, false, p + ": CI scripts, workflows and actions are read at run time"
	case path.Dir(p) == pkgDir && strings.HasSuffix(p, ".go"):
		if file.status != 'M' {
			return false, false, p + ": added or deleted file"
		}
		return false, true, ""
	case strings.HasPrefix(p, pkgDir+"/"):
		return false, false, p + ": non-Go or nested file of the package, read at run time or embedded"
	}
	return false, false, p + ": outside " + pkgDir + ", which this stage does not analyse"
}

func matchFiles(dir string, names []string, tags, goos, goarch string) (map[string]bool, error) {
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = goos, goarch, false
	ctx.BuildTags = strings.Split(tags, ",")
	set := map[string]bool{}
	for _, name := range names {
		ok, err := ctx.MatchFile(dir, name)
		if err != nil {
			return nil, err
		}
		if ok {
			set[name] = true
		}
	}
	return set, nil
}

func subset(a, b map[string]bool) bool {
	for name := range a {
		if !b[name] {
			return false
		}
	}
	return true
}

func setKey(set map[string]bool) string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, "\n")
}

// changes maps the hunks of the analysed files that g compiles to its
// nodes. Deleted lines are mapped through the base file's declarations to
// the head declarations of the same name.
type changes struct {
	g        *graph
	base     func(string) ([]byte, error)
	changed  map[int]bool
	reasons  []string
	outlines map[string]*outline
}

func (c *changes) kind(file string, l int, k lineKind) {
	switch k {
	case lineHeader:
		c.reasons = append(c.reasons, fmt.Sprintf("%s:%d: file header (build constraint or package clause)", file, l))
	case lineImport:
		c.reasons = append(c.reasons, fmt.Sprintf("%s:%d: import block", file, l))
	case lineDirective:
		c.reasons = append(c.reasons, fmt.Sprintf("%s:%d: directive comment outside a declaration", file, l))
	case lineCode:
		c.reasons = append(c.reasons, fmt.Sprintf("%s:%d: outside any declaration", file, l))
	}
}

func (c *changes) markKey(keys []string) bool {
	found := false
	for _, key := range keys {
		for _, n := range c.g.keys[key] {
			c.changed[n], found = true, true
		}
	}
	return found
}

func (c *changes) baseOutline(file string) (*outline, error) {
	if o, ok := c.outlines[file]; ok {
		return o, nil
	}
	src, err := c.base(file)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	o := newOutline(fset, f, src)
	c.outlines[file] = o
	return o, nil
}

func (c *changes) add(file fileDiff) {
	head := c.g.files[file.path]
	nodes := c.g.fileNodes[file.path]
	for _, h := range file.hunks {
		for l := h.newStart; l < h.newStart+h.newLines; l++ {
			if l > len(head.lines) {
				c.kind(file.path, l, lineCode)
				continue
			}
			for _, index := range head.lines[l-1] {
				c.changed[nodes[index]] = true
			}
			if len(head.lines[l-1]) == 0 {
				c.kind(file.path, l, head.kinds[l-1])
			}
		}
		if h.oldLines == 0 {
			continue
		}
		base, err := c.baseOutline(file.path)
		if err != nil {
			c.reasons = append(c.reasons, file.path+": base version: "+err.Error())
			continue
		}
		for l := h.oldStart; l < h.oldStart+h.oldLines; l++ {
			if l > len(base.lines) {
				c.kind(file.path, l, lineCode)
				continue
			}
			if len(base.lines[l-1]) == 0 {
				c.kind(file.path+" (base)", l, base.kinds[l-1])
			}
			for _, index := range base.lines[l-1] {
				d := base.decls[index]
				switch {
				case c.markKey(d.keys):
				case d.isInit:
					c.reasons = append(c.reasons, file.path+": an init function was removed")
				case d.initializer:
					c.reasons = append(c.reasons, file.path+": a package-level initializer was removed: "+d.display)
				case d.recv != "":
					// A removed method changes its type's method set.
					c.markKey([]string{"type:" + d.recv})
				}
				// A removed function, type or const that nothing in
				// the head names cannot change what the head runs.
			}
		}
	}
}

func selectBatches(opts options) (result, error) {
	out := result{Schema: schema, Package: opts.pattern, Base: opts.base, Fallback: []string{}, Ignored: []string{},
		Analysed: []string{}, Changed: []string{}, Batches: []batchResult{}, Total: len(opts.batches)}
	pkgDir := path.Clean(strings.TrimPrefix(opts.pattern, "./"))
	if opts.repo == "" {
		opts.repo = opts.root
	}
	if prefix, err := git(opts.repo, "rev-parse", "--show-prefix"); err != nil || strings.TrimSpace(prefix) != "" {
		return out, fmt.Errorf("the repository must be given by its top level (%v)", err)
	}
	head := opts.head
	if head == "" {
		head = "HEAD"
	}
	mergeBase, err := git(opts.repo, "merge-base", opts.base, head)
	if err != nil {
		return out, err
	}
	out.MergeBase = strings.TrimSpace(mergeBase)
	// Without -head the diff ends at the working tree, which is what the
	// type check reads.
	args := []string{"diff", "-U0", "--no-renames", "--no-ext-diff", "--no-textconv", "--no-color",
		"--no-relative", "--src-prefix=a/", "--dst-prefix=b/", out.MergeBase}
	if opts.head != "" {
		args = append(args, opts.head)
	}
	text, err := git(opts.repo, append(args, "--")...)
	if err != nil {
		return out, err
	}
	files, err := parseDiff(text)
	if err != nil {
		out.Fallback = append(out.Fallback, err.Error())
	}
	if opts.head == "" {
		status, err := git(opts.repo, "status", "--porcelain", "--untracked-files=all", "--", pkgDir)
		if err != nil {
			return out, err
		}
		for _, row := range strings.Split(status, "\n") {
			if strings.HasPrefix(row, "?? ") {
				out.Fallback = append(out.Fallback, row[3:]+": untracked file")
			}
		}
	}
	var analysed []fileDiff
	for _, file := range files {
		ignored, ok, reason := classify(pkgDir, file)
		switch {
		case ignored:
			out.Ignored = append(out.Ignored, file.path)
		case ok:
			out.Analysed = append(out.Analysed, file.path)
			analysed = append(analysed, file)
		default:
			out.Fallback = append(out.Fallback, reason)
		}
	}
	if len(out.Fallback) == 0 && len(analysed) > 0 {
		out.Fallback = append(out.Fallback, graphSelection(opts, pkgDir, out.MergeBase, analysed, &out)...)
	} else if len(out.Fallback) == 0 {
		for _, b := range opts.batches {
			out.Batches = append(out.Batches, batchResult{ID: b.ID, Reason: "no analysed file changed"})
		}
	}
	if len(out.Fallback) > 0 {
		out.Batches = []batchResult{}
		for _, b := range opts.batches {
			out.Batches = append(out.Batches, batchResult{ID: b.ID, Selected: true, Reason: "every batch: " + out.Fallback[0]})
		}
	}
	for _, b := range out.Batches {
		if b.Selected {
			out.Selected++
		}
	}
	return out, nil
}

// graphSelection fills out.Batches from the reference graphs, or returns
// the reasons to select every batch.
func graphSelection(opts options, pkgDir, mergeBase string, analysed []fileDiff, out *result) []string {
	dir := path.Join(opts.root, pkgDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{err.Error()}
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			names = append(names, entry.Name())
		}
	}
	base := func(file string) ([]byte, error) {
		text, err := git(opts.repo, "show", mergeBase+":"+file)
		return []byte(text), err
	}
	union, err := matchFiles(dir, names, opts.unionTags, opts.goos, opts.goarch)
	if err != nil {
		return []string{err.Error()}
	}
	type built struct {
		g        *graph
		affected map[int]int
	}
	graphs := map[string]*built{}
	build := func(tags string, files map[string]bool) (*built, []string) {
		key := setKey(files)
		if b, ok := graphs[key]; ok {
			return b, nil
		}
		g, err := load(opts.root, opts.pattern, tags, opts.goos, opts.goarch)
		if err != nil {
			return nil, []string{"type-based analysis failed: " + err.Error()}
		}
		listed := map[string]bool{}
		for file := range g.files {
			listed[path.Base(file)] = true
		}
		if setKey(listed) != key {
			return nil, []string{"go list and go/build disagree on the files of tags " + tags}
		}
		c := &changes{g: g, base: base, changed: map[int]bool{}, outlines: map[string]*outline{}}
		for _, file := range analysed {
			if g.files[file.path] != nil {
				c.add(file)
			}
		}
		if len(c.reasons) > 0 {
			return nil, c.reasons
		}
		changed := []int{}
		for n := range c.changed {
			changed = append(changed, n)
		}
		sort.Ints(changed)
		for _, n := range changed {
			label := g.nodes[n].file + ": " + g.nodes[n].display
			if !slices.Contains(out.Changed, label) {
				out.Changed = append(out.Changed, label)
			}
		}
		b := &built{g: g, affected: g.affected(changed)}
		graphs[key] = b
		return b, nil
	}
	if _, reasons := build(opts.unionTags, union); reasons != nil {
		return reasons
	}
	for _, spec := range opts.batches {
		files, err := matchFiles(dir, names, spec.Tags, opts.goos, opts.goarch)
		if err != nil {
			return []string{err.Error()}
		}
		b, reasons := graphs[setKey(union)], []string(nil)
		if !subset(files, union) {
			b, reasons = build(spec.Tags, files)
			if reasons != nil {
				return reasons
			}
		}
		res := batchResult{ID: spec.ID}
		roots := 0
		for n, item := range b.g.nodes {
			if !files[path.Base(item.file)] || !(item.isEntry || item.isInit || item.initializer) {
				continue
			}
			roots++
			if origin, ok := b.affected[n]; ok {
				target := b.g.nodes[origin]
				res.Selected = true
				res.Reason = item.display + " reaches " + target.display + " (" + target.file + ")"
				break
			}
		}
		if !res.Selected {
			res.Reason = fmt.Sprintf("none of %d roots reaches a changed declaration", roots)
		}
		out.Batches = append(out.Batches, res)
	}
	sort.Strings(out.Changed)
	return nil
}

func main() {
	var opts options
	var batches string
	flag.StringVar(&opts.root, "root", ".", "module root, checked out at the head")
	flag.StringVar(&opts.base, "base", "", "base commit; the diff starts at its merge base with the head")
	flag.StringVar(&opts.repo, "repo", "", "git repository top level (default: -root)")
	flag.StringVar(&opts.head, "head", "", "head commit whose tree -root holds (default: the working tree of -root)")
	flag.StringVar(&opts.pattern, "package", "./internal/integration", "the analysed test package")
	flag.StringVar(&opts.unionTags, "union-tags", "all", "tags of the build that compiles every batch's files")
	flag.StringVar(&opts.goos, "goos", "linux", "GOOS of the batch builds")
	flag.StringVar(&opts.goarch, "goarch", "amd64", "GOARCH of the batch builds")
	flag.StringVar(&batches, "batches", "", `JSON file of [{"id": ..., "tags": "a,b"}]`)
	flag.Parse()
	if opts.base == "" || batches == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	data, err := os.ReadFile(batches)
	if err == nil {
		err = json.Unmarshal(data, &opts.batches)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out, err := selectBatches(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
