// Command apinaming maintains the package-scoped API catalogues under
// docs/design/api-naming.
//
//	apinaming regenerate [package...]    rewrite declarations.json and members.json
//	apinaming approve [-review note] package...   write review.json only
//	apinaming resolve [-base rev] [-ours rev] [-theirs rev] [package...]
//	                                     after a merge, re-derive the catalogues and receipts of packages
//	                                     that both parents changed, if the merge only composes approved rows
//	apinaming aggregate                  print the computed totals (nothing is committed)
//	apinaming check                      verify layout, catalogues and receipts like the tests
//
// Regenerate never touches review.json and approve never touches the
// catalogues. CI runs neither: it only runs the tests.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "apinaming:", err)
		os.Exit(1)
	}
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("run inside the module")
		}
		dir = parent
	}
}

func gitShow(root, rev, path string) ([]byte, error) {
	out, err := exec.Command("git", "-C", root, "show", rev+":"+path).Output()
	if err != nil {
		return nil, fmt.Errorf("git show %s:%s: %w", rev, path, err)
	}
	return out, nil
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: apinaming regenerate|approve|resolve|aggregate|check")
	}
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	cfg := catalogue.Config{Root: root, Proposals: catalogue.Proposals}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	review := flags.String("review", catalogue.DefaultReview, "approve: review note below docs/design")
	base := flags.String("base", "", "resolve: the merge base (default: the merge base of -ours and -theirs)")
	ours := flags.String("ours", "HEAD", "resolve: the branch being merged into")
	theirs := flags.String("theirs", "MERGE_HEAD", "resolve: the branch being merged in")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "regenerate":
		written, err := catalogue.Regenerate(cfg, flags.Args())
		for _, path := range written {
			fmt.Println("wrote", path)
		}
		return err
	case "approve":
		written, err := catalogue.Approve(cfg, flags.Args(), *review)
		for _, path := range written {
			fmt.Println("wrote", path)
		}
		return err
	case "resolve":
		return resolve(cfg, *base, *ours, *theirs, flags.Args())
	case "aggregate":
		rows, total, err := catalogue.Summarize(root)
		if err != nil {
			return err
		}
		fmt.Print(catalogue.Markdown(rows, total))
		return nil
	case "check":
		return check(cfg)
	}
	return fmt.Errorf("unknown command %q", args[0])
}

func check(cfg catalogue.Config) error {
	if err := catalogue.CheckLayout(cfg.Root); err != nil {
		return err
	}
	packages, err := catalogue.Discover(cfg.Root)
	if err != nil {
		return err
	}
	surfaces, err := catalogue.Generate(cfg, packages)
	if err != nil {
		return err
	}
	var problems []error
	for _, pkg := range packages {
		problems = append(problems, catalogue.CheckCatalogue(cfg.Root, pkg, surfaces[pkg]), catalogue.CheckReceipt(cfg.Root, pkg, surfaces[pkg]))
	}
	return errors.Join(problems...)
}

func gitRead(root string) catalogue.Reader {
	return func(rev, path string) ([]byte, error) {
		if err := exec.Command("git", "-C", root, "cat-file", "-e", rev+":"+path).Run(); err != nil {
			return nil, nil
		}
		return gitShow(root, rev, path)
	}
}

func gitOutput(root string, args ...string) (string, error) {
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
}

// resolve implements `apinaming resolve`. Revisions are resolved to commit
// ids first, so a name that is not a commit fails instead of reading nothing.
// The merge base defaults to the merge base of the two parents.
func resolve(cfg catalogue.Config, base, ours, theirs string, packages []string) error {
	commit := func(label, rev string) (string, error) {
		id, err := gitOutput(cfg.Root, "rev-parse", "--verify", "--end-of-options", rev+"^{commit}")
		if err != nil {
			return "", fmt.Errorf("%s revision %q is not a commit (is a merge in progress?): %w", label, rev, err)
		}
		return id, nil
	}
	var err error
	if ours, err = commit("ours", ours); err != nil {
		return err
	}
	if theirs, err = commit("theirs", theirs); err != nil {
		return err
	}
	if base == "" {
		if base, err = gitOutput(cfg.Root, "merge-base", ours, theirs); err != nil {
			return fmt.Errorf("no merge base of the two parents: %w", err)
		}
	} else if base, err = commit("base", base); err != nil {
		return err
	}
	results, err := catalogue.Resolve(cfg, catalogue.ResolveOptions{Base: base, Ours: ours, Theirs: theirs, Read: gitRead(cfg.Root)}, packages)
	for _, res := range results {
		for _, path := range res.Files {
			fmt.Println("wrote", path)
		}
		fmt.Printf("resolved %s: %d declaration rows only in ours, %d only in theirs; both receipts were approved states and the merged rows are their composition\n", res.Package, res.Comp.OursOnly, res.Comp.TheirsOnly)
	}
	if err == nil && len(results) == 0 {
		fmt.Println("resolve: no package was changed by both parents; nothing to do")
	}
	return err
}
