package catalogue

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// DefaultReview is the review note a new receipt names unless told otherwise.
// It is resolved relative to docs/design.
const DefaultReview = "api-naming.md"

// PackageDir returns the catalogue directory of a package.
func PackageDir(root, pkg string) string {
	return filepath.Join(root, filepath.FromSlash(Dir), filepath.FromSlash(pkg))
}

// DefaultPage is the API reference page a new receipt names:
// docs/<package with slashes replaced by dashes>.md.
func DefaultPage(pkg string) string {
	return "docs/" + strings.ReplaceAll(pkg, "/", "-") + ".md"
}

// Directories returns the packages that have a catalogue directory: every
// directory below Dir that holds at least one of the three files.
func Directories(root string) ([]string, error) {
	base := filepath.Join(root, filepath.FromSlash(Dir))
	var found []string
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && path == base {
				return fs.SkipAll
			}
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		for _, name := range []string{DeclarationsFile, MembersFile, ReceiptFile} {
			if _, err := os.Stat(filepath.Join(path, name)); err == nil {
				rel, err := filepath.Rel(base, path)
				if err != nil {
					return err
				}
				found = append(found, filepath.ToSlash(rel))
				break
			}
		}
		return nil
	})
	slices.Sort(found)
	return found, err
}

// CheckLayout requires the catalogue directories to match the discovered
// packages exactly: no public package without its three files, and no
// catalogue directory for a package that does not exist.
func CheckLayout(root string) error {
	packages, err := Discover(root)
	if err != nil {
		return err
	}
	directories, err := Directories(root)
	if err != nil {
		return err
	}
	var problems []error
	for _, pkg := range packages {
		if !slices.Contains(directories, pkg) {
			problems = append(problems, fmt.Errorf("public package %s has no catalogue directory %s/%s", pkg, Dir, pkg))
			continue
		}
		for _, name := range []string{DeclarationsFile, MembersFile, ReceiptFile} {
			if info, err := os.Stat(filepath.Join(PackageDir(root, pkg), name)); err != nil || !info.Mode().IsRegular() {
				problems = append(problems, fmt.Errorf("public package %s is missing %s/%s/%s", pkg, Dir, pkg, name))
			}
		}
	}
	for _, directory := range directories {
		if !slices.Contains(packages, directory) {
			problems = append(problems, fmt.Errorf("catalogue directory %s/%s has no public package", Dir, directory))
		}
	}
	return errors.Join(problems...)
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func selectPackages(discovered, named []string) ([]string, error) {
	if len(named) == 0 {
		return discovered, nil
	}
	for _, pkg := range named {
		if !slices.Contains(discovered, pkg) {
			return nil, fmt.Errorf("%s is not a public package", pkg)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(named))), nil
}

// Regenerate rewrites declarations.json and members.json of the named
// packages (all when none is named) from source. It never reads or writes
// review.json, so regenerating cannot approve a changed API. Nothing is
// written when the derivation is refused (conflicting duplicate, name
// collision, stale proposal).
func Regenerate(c Config, named []string) ([]string, error) {
	discovered, err := Discover(c.Root)
	if err != nil {
		return nil, err
	}
	selected, err := selectPackages(discovered, named)
	if err != nil {
		return nil, err
	}
	surfaces, err := Generate(c, discovered)
	if err != nil {
		return nil, err
	}
	var written []string
	for _, pkg := range selected {
		for _, file := range []struct {
			name  string
			value any
		}{{DeclarationsFile, surfaces[pkg].Declarations}, {MembersFile, surfaces[pkg].Members}} {
			name := file.name
			data, err := Marshal(file.value)
			if err != nil {
				return written, err
			}
			path := filepath.Join(PackageDir(c.Root, pkg), name)
			if err := writeFile(path, data); err != nil {
				return written, err
			}
			written = append(written, filepath.ToSlash(filepath.Join(Dir, pkg, name)))
		}
	}
	slices.Sort(written)
	return written, nil
}

func checkFile(root, pkg, name string, value any) error {
	want, err := Marshal(value)
	if err != nil {
		return err
	}
	got, err := os.ReadFile(filepath.Join(PackageDir(root, pkg), name))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%s/%s/%s is stale; inspect the API change, then regenerate it", Dir, pkg, name)
	}
	return nil
}

// CheckDeclarations requires the committed declarations.json of a package to
// equal the source-derived bytes. A missing or stale file, a missing row and a
// changed signature all fail.
func CheckDeclarations(root, pkg string, surface Surface) error {
	return checkFile(root, pkg, DeclarationsFile, surface.Declarations)
}

// CheckMembers requires the committed members.json of a package to equal the
// source-derived bytes. A missing type, a missing member and a changed tag all
// fail.
func CheckMembers(root, pkg string, surface Surface) error {
	return checkFile(root, pkg, MembersFile, surface.Members)
}

// CheckCatalogue runs CheckDeclarations and CheckMembers.
func CheckCatalogue(root, pkg string, surface Surface) error {
	return errors.Join(CheckDeclarations(root, pkg, surface), CheckMembers(root, pkg, surface))
}

// Receipt is the reviewed receipt of one package. It pins the exact catalogue
// serialization that a human review accepted.
type Receipt struct {
	Schema             string `json:"schema"`
	Package            string `json:"package"`
	DeclarationsSHA256 string `json:"declarations_sha256"`
	MembersSHA256      string `json:"members_sha256"`
	Declarations       int    `json:"declarations"`
	Members            int    `json:"members"`
	Review             string `json:"review"`
	Page               string `json:"page"`
}

// LoadReceipt reads a package's receipt strictly: unknown fields and trailing
// data are refused.
func LoadReceipt(root, pkg string) (Receipt, error) {
	var receipt Receipt
	data, err := os.ReadFile(filepath.Join(PackageDir(root, pkg), ReceiptFile))
	if err != nil {
		return receipt, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("%s/%s/%s: %w", Dir, pkg, ReceiptFile, err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, fmt.Errorf("%s/%s/%s: %w", Dir, pkg, ReceiptFile, err)
	}
	return receipt, nil
}

func digest(value any) (string, error) {
	data, err := Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// requireFile accepts a repository-local regular file below base.
func requireFile(root, base, name, suffix string) error {
	if name == "" || !filepath.IsLocal(name) || !strings.HasSuffix(name, suffix) {
		return fmt.Errorf("%q is not a local %s file", name, suffix)
	}
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(base), filepath.FromSlash(name)))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", name)
	}
	return nil
}

func checkReferences(root string, receipt Receipt) error {
	var problems []error
	if err := requireFile(root, "docs/design", receipt.Review, ".md"); err != nil {
		problems = append(problems, fmt.Errorf("review note: %w", err))
	}
	if !strings.HasPrefix(receipt.Page, "docs/") {
		problems = append(problems, fmt.Errorf("page %q is not below docs/", receipt.Page))
	} else if err := requireFile(root, "docs", strings.TrimPrefix(receipt.Page, "docs/"), ".md"); err != nil {
		problems = append(problems, fmt.Errorf("page: %w", err))
	}
	return errors.Join(problems...)
}

// CheckReceipt requires the package's receipt to bind the current
// source-derived surface: schema, package, both hashes, both counts, no
// pending proposal, and an existing review note and reference page. A source
// change in another package never makes this receipt stale.
func CheckReceipt(root, pkg string, surface Surface) error {
	receipt, err := LoadReceipt(root, pkg)
	if err != nil {
		return err
	}
	if receipt.Schema != ReceiptSchema || receipt.Package != pkg {
		return fmt.Errorf("unknown naming review receipt for %s", pkg)
	}
	for _, entry := range surface.Declarations.Entries {
		if entry.Decision == "proposal" {
			return fmt.Errorf("review cannot finish with a pending proposal: %s", entry.Current)
		}
	}
	declarations, err := digest(surface.Declarations)
	if err != nil {
		return err
	}
	members, err := digest(surface.Members)
	if err != nil {
		return err
	}
	if receipt.DeclarationsSHA256 != declarations || receipt.MembersSHA256 != members ||
		receipt.Declarations != surface.Declarations.Count || receipt.Members != surface.Members.Count {
		return fmt.Errorf("public naming surface of %s changed since the recorded review; review the change and explicitly approve it, not only regenerate the catalogues", pkg)
	}
	return checkReferences(root, receipt)
}

// Approve writes review.json for the named packages, and only that file. It
// refuses unless every named package's committed catalogue equals the source
// (no stale catalogue), no row is a pending proposal, and the review note and
// reference page exist. Nothing is written when any package is refused. The
// approval stays a deliberate edit that a reviewer reads as its own diff; the
// command only removes the hash arithmetic.
func Approve(c Config, named []string, review string) ([]string, error) {
	if len(named) == 0 {
		return nil, errors.New("name the packages to approve")
	}
	discovered, err := Discover(c.Root)
	if err != nil {
		return nil, err
	}
	selected, err := selectPackages(discovered, named)
	if err != nil {
		return nil, err
	}
	surfaces, err := Generate(c, discovered)
	if err != nil {
		return nil, err
	}
	if review == "" {
		review = DefaultReview
	}
	receipts := map[string]Receipt{}
	var problems []error
	for _, pkg := range selected {
		surface := surfaces[pkg]
		if err := CheckCatalogue(c.Root, pkg, surface); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", pkg, err))
			continue
		}
		var pending []string
		for _, entry := range surface.Declarations.Entries {
			if entry.Decision == "proposal" {
				pending = append(pending, entry.Current)
			}
		}
		if len(pending) > 0 {
			problems = append(problems, fmt.Errorf("%s: pending proposals: %s", pkg, strings.Join(pending, ", ")))
			continue
		}
		page := DefaultPage(pkg)
		if prior, err := LoadReceipt(c.Root, pkg); err == nil && prior.Page != "" {
			page = prior.Page
		}
		declarations, err := digest(surface.Declarations)
		if err != nil {
			return nil, err
		}
		members, err := digest(surface.Members)
		if err != nil {
			return nil, err
		}
		receipt := Receipt{
			Schema: ReceiptSchema, Package: pkg, DeclarationsSHA256: declarations, MembersSHA256: members,
			Declarations: surface.Declarations.Count, Members: surface.Members.Count, Review: review, Page: page,
		}
		if err := checkReferences(c.Root, receipt); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", pkg, err))
			continue
		}
		receipts[pkg] = receipt
	}
	if err := errors.Join(problems...); err != nil {
		return nil, err
	}
	var written []string
	for _, pkg := range selected {
		data, err := Marshal(receipts[pkg])
		if err != nil {
			return written, err
		}
		if err := writeFile(filepath.Join(PackageDir(c.Root, pkg), ReceiptFile), data); err != nil {
			return written, err
		}
		written = append(written, filepath.ToSlash(filepath.Join(Dir, pkg, ReceiptFile)))
	}
	return written, nil
}

// requireEOF refuses anything after the first JSON value. Decoder.More is not
// an end-of-input check: a stray closing delimiter makes it report false.
// Decoding again must return io.EOF exactly, which rejects a second value and
// any trailing syntax error alike. Whitespace after the value is accepted.
func requireEOF(dec *json.Decoder) error {
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}
