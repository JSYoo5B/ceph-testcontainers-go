package main

import (
	"fmt"
	"strconv"
	"strings"
)

// hunk is one change of a -U0 unified diff: oldLines lines from oldStart
// were replaced by newLines lines from newStart.
type hunk struct {
	oldStart, oldLines, newStart, newLines int
}

// fileDiff is one changed path. Status is 'A' for an added file, 'D' for a
// deleted one and 'M' otherwise; renames are reported as both.
type fileDiff struct {
	path   string
	status byte
	hunks  []hunk
}

func parseRange(text string) (int, int, error) {
	start, count, found := strings.Cut(text, ",")
	first, err := strconv.Atoi(start)
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return first, 1, nil
	}
	n, err := strconv.Atoi(count)
	return first, n, err
}

// diffPath reads the path of a "diff --git a/P b/P" line. Without renames
// both sides name the same path; a quoted path is rejected.
func diffPath(header string) (string, error) {
	rest := strings.TrimPrefix(header, "diff --git ")
	if len(rest) < 7 || (len(rest)-5)%2 != 0 || strings.HasPrefix(rest, `"`) {
		return "", fmt.Errorf("unsupported diff header %q", header)
	}
	n := (len(rest) - 5) / 2
	a, b := rest[:2+n], rest[3+n:]
	if !strings.HasPrefix(a, "a/") || !strings.HasPrefix(b, "b/") || a[2:] != b[2:] || rest[2+n] != ' ' {
		return "", fmt.Errorf("unsupported diff header %q", header)
	}
	return a[2:], nil
}

// parseDiff reads the output of git diff -U0 --no-renames.
func parseDiff(text string) ([]fileDiff, error) {
	var files []fileDiff
	for _, row := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(row, "diff --git "):
			path, err := diffPath(row)
			if err != nil {
				return nil, err
			}
			files = append(files, fileDiff{path: path, status: 'M'})
		case len(files) == 0:
			continue
		case strings.HasPrefix(row, "new file mode"):
			files[len(files)-1].status = 'A'
		case strings.HasPrefix(row, "deleted file mode"):
			files[len(files)-1].status = 'D'
		case strings.HasPrefix(row, "@@ "):
			fields := strings.Fields(row)
			if len(fields) < 4 || fields[3] != "@@" || !strings.HasPrefix(fields[1], "-") || !strings.HasPrefix(fields[2], "+") {
				return nil, fmt.Errorf("malformed hunk header %q", row)
			}
			var h hunk
			var err error
			if h.oldStart, h.oldLines, err = parseRange(fields[1][1:]); err != nil {
				return nil, fmt.Errorf("malformed hunk header %q", row)
			}
			if h.newStart, h.newLines, err = parseRange(fields[2][1:]); err != nil {
				return nil, fmt.Errorf("malformed hunk header %q", row)
			}
			files[len(files)-1].hunks = append(files[len(files)-1].hunks, h)
		}
	}
	return files, nil
}
