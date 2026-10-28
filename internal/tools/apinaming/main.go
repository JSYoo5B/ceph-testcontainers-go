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
//	apinaming moves [-base rev] [-require-base]   check move records and account for removed/added rows against the merge base
//	apinaming migrate-split -base rev    one-time split of the global catalogues
//	apinaming migrate-verify -base rev   one-time byte-equality proof against the global files
//
// Regenerate never touches review.json and approve never touches the
// catalogues. CI runs neither: it only runs the tests and the moves check. The
// moves check prints OK, SKIPPED (no base available: the comparison did not
// run) or FAILED; SKIPPED is never a pass, and -require-base fails it.
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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/moves"
)

const legacyDir = "docs/design/"

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
		return errors.New("usage: apinaming regenerate|approve|resolve|aggregate|check|moves|migrate-split|migrate-verify")
	}
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	cfg := catalogue.Config{Root: root, Proposals: catalogue.Proposals}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	review := flags.String("review", catalogue.DefaultReview, "approve: review note below docs/design")
	base := flags.String("base", "", "migrate-*: Git revision that holds the global catalogues; moves: revision to compare with (its merge base with HEAD is used)")
	ours := flags.String("ours", "HEAD", "resolve: the branch being merged into")
	theirs := flags.String("theirs", "MERGE_HEAD", "resolve: the branch being merged in")
	requireBase := flags.Bool("require-base", false, "moves: fail instead of reporting SKIPPED when no base is available")
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
	case "moves":
		if flags.NArg() != 0 {
			return errors.New("moves takes no arguments")
		}
		return checkMoves(root, *base, *requireBase)
	case "migrate-split", "migrate-verify":
		if *base == "" {
			return errors.New("-base is required")
		}
		files := make([][]byte, 3)
		for i, name := range []string{"api-naming-inventory.json", "api-naming-members.json", "api-naming-review.json"} {
			if files[i], err = gitShow(root, *base, legacyDir+name); err != nil {
				return err
			}
		}
		if args[0] == "migrate-verify" {
			if err := catalogue.VerifyLegacy(root, files[0], files[1], files[2]); err != nil {
				return err
			}
			fmt.Println("per-package catalogues reproduce the global catalogues byte for byte")
			return nil
		}
		surfaces, err := catalogue.SplitLegacy(files[0], files[1])
		if err != nil {
			return err
		}
		for pkg, surface := range surfaces {
			for name, value := range map[string]any{catalogue.DeclarationsFile: surface.Declarations, catalogue.MembersFile: surface.Members} {
				data, err := catalogue.Marshal(value)
				if err != nil {
					return err
				}
				dir := catalogue.PackageDir(root, pkg)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
					return err
				}
			}
			fmt.Println("split", pkg)
		}
		return nil
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

// checkMoves prints the outcome of the moves check on stdout. A skipped
// comparison is reported as SKIPPED with its reason and is not an error
// unless requireBase is set.
func checkMoves(root, base string, requireBase bool) error {
	result := moves.Check(moves.Options{Root: root, Base: base, RequireBase: requireBase})
	for _, problem := range result.Problems {
		fmt.Println("problem:", problem)
	}
	switch result.Status() {
	case "FAILED":
		return fmt.Errorf("moves: FAILED with %d problem(s)", len(result.Problems))
	case "SKIPPED":
		fmt.Printf("moves: SKIPPED (%s); record checks ran, the base comparison did not\n", result.Reason)
	default:
		fmt.Printf("moves: OK (compared with merge base %s)\n", result.Base)
	}
	return nil
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
