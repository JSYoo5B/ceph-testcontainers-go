package catalogue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Row is the committed state of one package.
type Row struct {
	Package      string
	Declarations int
	Members      int
	Types        int
	Review       string
	Page         string
}

// Summarize reads the committed per-package files and returns one row per
// catalogue directory in package order, and the totals. It is a computed view
// for reviewers: nothing aggregated is committed. It does not read the source
// and does not judge staleness; the tests do.
func Summarize(root string) ([]Row, Row, error) {
	directories, err := Directories(root)
	if err != nil {
		return nil, Row{}, err
	}
	var rows []Row
	total := Row{Package: "Total"}
	for _, pkg := range directories {
		var declarations Declarations
		var members Members
		if err := readStrict(filepath.Join(PackageDir(root, pkg), DeclarationsFile), &declarations); err != nil {
			return nil, Row{}, err
		}
		if err := readStrict(filepath.Join(PackageDir(root, pkg), MembersFile), &members); err != nil {
			return nil, Row{}, err
		}
		receipt, err := LoadReceipt(root, pkg)
		if err != nil {
			return nil, Row{}, err
		}
		if declarations.Package != pkg || members.Package != pkg || receipt.Package != pkg {
			return nil, Row{}, fmt.Errorf("%s/%s files name a different package", Dir, pkg)
		}
		if declarations.Count != len(declarations.Entries) {
			return nil, Row{}, fmt.Errorf("%s/%s/%s count %d does not match its %d entries", Dir, pkg, DeclarationsFile, declarations.Count, len(declarations.Entries))
		}
		row := Row{Package: pkg, Declarations: declarations.Count, Members: members.Count, Types: len(members.Types), Review: receipt.Review, Page: receipt.Page}
		rows = append(rows, row)
		total.Declarations += row.Declarations
		total.Members += row.Members
		total.Types += row.Types
	}
	return rows, total, nil
}

func readStrict(path string, into any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return nil
}

// Markdown renders the aggregate as a table.
func Markdown(rows []Row, total Row) string {
	var out strings.Builder
	out.WriteString("| Package | Declaration entries | Member/embedding entries | Types with members | Review note | Page |\n")
	out.WriteString("| --- | ---: | ---: | ---: | --- | --- |\n")
	for _, row := range rows {
		fmt.Fprintf(&out, "| `%s` | %d | %d | %d | `%s` | `%s` |\n", row.Package, row.Declarations, row.Members, row.Types, row.Review, row.Page)
	}
	fmt.Fprintf(&out, "| %s | %d | %d | %d | | |\n", total.Package, total.Declarations, total.Members, total.Types)
	return out.String()
}
