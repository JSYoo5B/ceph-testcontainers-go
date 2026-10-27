// Package apiinventory checks the package-scoped API catalogues under
// docs/design/api-naming and that each public package's page in docs names
// every identifier the package exports: functions, types, constants,
// variables and the methods of its types.
//
// The public package list is discovered from the source tree; the page of a
// package is named by its reviewed receipt.
package apiinventory_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

const root = "../.."

// A method counts as documented when its name appears on the page, usually
// as its own heading under the type's section.
func TestPackagePagesNameEveryExport(t *testing.T) {
	for _, pkg := range publicPackages(t) {
		receipt, err := catalogue.LoadReceipt(root, pkg)
		if err != nil {
			t.Errorf("%s: %v", pkg, err)
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, receipt.Page))
		if err != nil {
			t.Errorf("%s: %v", pkg, err)
			continue
		}
		text := string(data)
		names, err := catalogue.ExportedNames(root, pkg)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			word := name[strings.LastIndex(name, ".")+1:]
			if !regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`).MatchString(text) {
				t.Errorf("%s.%s is not documented in %s", pkg, name, receipt.Page)
			}
		}
	}
}
