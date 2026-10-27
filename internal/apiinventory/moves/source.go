package moves

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// snapshot is the set of files this check reads, keyed by repository path.
type snapshot struct {
	files    map[string][]byte
	problems []string
}

func newSnapshot() *snapshot { return &snapshot{files: map[string][]byte{}} }

var objectID = regexp.MustCompile(`\A([0-9a-f]{40}|[0-9a-f]{64})\z`)

// git runs a Git command in root without a shell and with a fixed
// environment, so a hostile revision string can only ever be an argument.
func git(root string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0", "LC_ALL=C")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// resolution is the outcome of looking for a base.
type resolution struct {
	sha     string
	skip    string // non-empty: the comparison cannot run
	problem string // non-empty: the input is invalid
}

// resolveBase turns a revision into the merge base with HEAD. An empty
// revision, a revision the repository does not have (a shallow clone) and
// unrelated histories skip the comparison. A revision that cannot be a
// revision name (a leading dash, whitespace, a trailing newline) is an error:
// it is never handed to Git.
func resolveBase(root, rev string) resolution {
	if rev == "" {
		return resolution{skip: "no base revision was given"}
	}
	if !revision.MatchString(rev) {
		return resolution{problem: fmt.Sprintf("base revision %q is not a plain revision name", rev)}
	}
	out, err := git(root, "rev-parse", "--verify", "--quiet", "--end-of-options", rev+"^{commit}")
	if err != nil {
		return resolution{skip: fmt.Sprintf("base revision %q is not available in this clone (a shallow clone may lack it)", rev)}
	}
	tip := strings.TrimSpace(string(out))
	if !objectID.MatchString(tip) {
		return resolution{problem: fmt.Sprintf("base revision %q resolved to %q, not a commit id", rev, tip)}
	}
	out, err = git(root, "merge-base", tip, "HEAD")
	if err != nil {
		return resolution{skip: fmt.Sprintf("no merge base between %s and HEAD", tip[:12])}
	}
	base := strings.TrimSpace(string(out))
	if !objectID.MatchString(base) {
		return resolution{problem: fmt.Sprintf("merge base %q is not a commit id", base)}
	}
	return resolution{sha: base}
}

// readBase loads the relevant files of a commit. Only regular blobs count; a
// symbolic link or submodule at a relevant path is ignored, so it cannot make
// a row appear. Paths are taken from NUL-separated output, so a name with a
// newline stays one path and is then rejected by classify.
func readBase(root, sha string) (*snapshot, error) {
	snap := newSnapshot()
	out, err := git(root, "ls-tree", "-r", "-z", "--full-tree", sha, "--", catalogue.Dir, "docs/changes")
	if err != nil {
		return nil, err
	}
	for _, entry := range bytes.Split(out, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		tab := bytes.IndexByte(entry, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("unexpected ls-tree entry %q", entry)
		}
		meta := strings.Fields(string(entry[:tab]))
		path := string(entry[tab+1:])
		if len(meta) != 3 || meta[1] != "blob" || (meta[0] != "100644" && meta[0] != "100755") || !objectID.MatchString(meta[2]) {
			continue
		}
		kind, _ := classify(path)
		if kind == kindNone {
			continue
		}
		if kind == kindStray || kind == kindReserved {
			// An odd name in the base is the base's own problem. It is not
			// read, so it cannot create or hide a row.
			continue
		}
		size, err := git(root, "cat-file", "-s", meta[2])
		if err != nil {
			return nil, err
		}
		var n int64
		if _, err := fmt.Sscanf(strings.TrimSpace(string(size)), "%d", &n); err != nil || n > maxFile {
			snap.problems = append(snap.problems, fmt.Sprintf("base file %q is larger than %d bytes", path, maxFile))
			continue
		}
		data, err := git(root, "cat-file", "blob", meta[2])
		if err != nil {
			return nil, err
		}
		snap.files[path] = data
	}
	return snap, nil
}

// readHead loads the relevant files of the working tree. Symbolic links and
// irregular files at relevant paths are problems, not data.
func readHead(root string) (*snapshot, error) {
	snap := newSnapshot()
	for _, top := range []string{catalogue.Dir, "docs/changes"} {
		base := filepath.Join(root, filepath.FromSlash(top))
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) && path == base {
					return fs.SkipAll
				}
				return err
			}
			if entry.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			kind, _ := classify(rel)
			switch kind {
			case kindNone:
				return nil
			case kindStray, kindReserved:
				snap.problems = append(snap.problems, fmt.Sprintf("unexpected file %q in %s", rel, RecordsDir))
				return nil
			}
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				snap.problems = append(snap.problems, fmt.Sprintf("%q is not a regular file", rel))
				return nil
			}
			if info.Size() > maxFile {
				snap.problems = append(snap.problems, fmt.Sprintf("%q is larger than %d bytes", rel, maxFile))
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			snap.files[rel] = data
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return snap, nil
}
