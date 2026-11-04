package main

import (
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"slices"
	"strings"
)

// summaryPriority orders inspection, not severity. Numeric values of different
// metrics are not comparable. Unchanged messages remain in the raw delta.
func summaryPriority(e deltaEntry) int {
	switch e.State {
	case stateNew:
		return 0
	case stateBoth:
		if e.Base.Message != e.Head.Message {
			return 1
		}
		return 4
	case stateUnmatched:
		return 2
	default:
		return 3
	}
}

func markdownCell(s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s)
	// Encode Markdown punctuation too: source names and linter messages are
	// data, not links, HTML or formatting supplied by the report author.
	s = html.EscapeString(s)
	return strings.NewReplacer("|", "&#124;", "`", "&#96;", "[", "&#91;", "]", "&#93;", "*", "&#42;", "_", "&#95;", "\\", "&#92;").Replace(s)
}

func writeDeltaMarkdown(out io.Writer, d *delta, limit int) error {
	var b strings.Builder
	fmt.Fprintf(&b, "### Structural metric triage\n\nBase: %s; head: %s.\n\n", markdownCell(d.Base.Ref), markdownCell(d.Head.Ref))
	fmt.Fprintf(&b, "golangci-lint %s (CI pin %s; match %t); config SHA-256 %s; patterns %s.\n\n",
		markdownCell(d.Tool.Version), markdownCell(d.Tool.CIPinned), d.Tool.MatchesCIPin, markdownCell(d.Config.SHA256), markdownCell(strings.Join(d.Patterns, ", ")))
	fmt.Fprintf(&b, "Raw SHA-256: base %s; head %s. Snapshot source indexed: base %t, head %t; unparsable files: base %d, head %d.\n\n",
		d.Base.RawSHA256, d.Head.RawSHA256, d.Base.SnapshotIndexed, d.Head.SnapshotIndexed, d.Base.UnparsableFiles, d.Head.UnparsableFiles)
	for _, state := range deltaStates {
		fmt.Fprintf(&b, "- %s: %d\n", state, d.Counts[state])
	}
	entries := slices.Clone(d.Entries)
	slices.SortStableFunc(entries, func(a, c deltaEntry) int { return summaryPriority(a) - summaryPriority(c) })
	unchanged := 0
	for _, e := range entries {
		if summaryPriority(e) == 4 {
			unchanged++
		}
	}
	candidates := len(entries) - unchanged
	shown := min(limit, candidates)
	fmt.Fprintf(&b, "\nShowing %d of %d inspection candidates; %d unchanged messages omitted; %d candidates beyond row limit.\n\n", shown, candidates, unchanged, candidates-shown)
	b.WriteString("Order: newly reported, changed messages, unmatched, no longer reported. This is inspection order, not a severity or improvement ranking. Missing diagnostics are not zero. Dupl blocks stay unmatched.\n\n")
	b.WriteString("| State / identity | Metric | Base location / message | Head location / message | Reason / caveats |\n| --- | --- | --- | --- | --- |\n")
	cell := func(s *site) string {
		if s == nil {
			return "no paired diagnostic; see state/reason"
		}
		return markdownCell(fmt.Sprintf("%s:%d:%d (test=%t): %s", s.File, s.Line, s.Column, s.Test, s.Message))
	}
	for _, e := range entries[:shown] {
		identity := strings.Join([]string{e.PkgDir, e.PkgName, e.Receiver, e.Function}, "/")
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", markdownCell(e.State+" / "+identity), markdownCell(e.Linter), cell(e.Base), cell(e.Head), markdownCell(strings.Join(append([]string{e.Reason}, e.Caveats...), ", ")))
	}
	b.WriteString("\nCollection scope:\n\n")
	for _, s := range d.RawScope {
		fmt.Fprintf(&b, "- %s\n", markdownCell(s))
	}
	b.WriteString("\nRetain the full JSON/TSV delta and both raw collections beside this summary. Source indexing does not prove a tagged configuration was measured. Metrics do not establish behavioral correctness or agent-cost savings.\n")
	_, err := io.WriteString(out, b.String())
	return err
}

// Filtering never turns a missing finding into an improvement: full counts and
// raw evidence stay attached, and rows outside the focus are counted explicitly.
func writeFocusedDeltaMarkdown(out io.Writer, d *delta, limit int, files []string) error {
	focus := map[string]bool{}
	for _, file := range files {
		if !fs.ValidPath(file) || strings.ContainsAny(file, "\\\r\n\x00") {
			return errors.New("changed-files contains an invalid module-relative path")
		}
		focus[file] = true
	}
	filtered := *d
	filtered.Entries = nil
	for _, e := range d.Entries {
		if e.Base != nil && focus[e.Base.File] || e.Head != nil && focus[e.Head.File] {
			filtered.Entries = append(filtered.Entries, e)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Findings focus: changed files on either side. %d entries outside focus omitted. State counts below describe the full comparison; JSON/TSV retain all findings.\n\n", len(d.Entries)-len(filtered.Entries))
	if err := writeDeltaMarkdown(&b, &filtered, limit); err != nil {
		return err
	}
	_, err := io.WriteString(out, b.String())
	return err
}
