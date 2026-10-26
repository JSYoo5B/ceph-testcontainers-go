//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// These checks cover the shared receipt decoder (issue 63). Decoder.More is an
// iteration predicate that is false before a closing delimiter, so a dangling
// "}" or "]" after a valid receipt must be refused by a second Decode that
// returns io.EOF. They run the Python producers on an ordinary temporary
// directory plus synthetic text; they exercise no tmpfs, no Ceph daemon and no
// Docker.

type decodeStrictProbe struct {
	Version *int `json:"version"`
}

func TestDecodeStrictRequiresEndOfDocument(t *testing.T) {
	const doc = `{"version": 1}`
	for name, ok := range map[string]string{
		"plain":               doc,
		"surrounding spaces":  "  " + doc + " \t",
		"trailing newline":    doc + "\n",
		"leading newline":     "\n" + doc,
		"whitespace only gap": doc + "   ",
	} {
		var p decodeStrictProbe
		if err := decodeStrict([]byte(ok), &p); err != nil || p.Version == nil || *p.Version != 1 {
			t.Errorf("%s: refused a valid document: %+v, %v", name, p, err)
		}
	}
	for name, bad := range map[string]string{
		"dangling brace":           doc + "}",
		"dangling bracket":         doc + "]",
		"dangling brace and space": doc + " }",
		"extra value":              doc + ` {"version": 1}`,
		"extra scalar":             doc + " 7",
		"extra word":               doc + " garbage",
		"two lines":                doc + "\n" + doc,
		"unknown field":            `{"version": 1, "extra": true}`,
	} {
		var p decodeStrictProbe
		if err := decodeStrict([]byte(bad), &p); err == nil {
			t.Errorf("%s: accepted %q as %+v", name, bad, p)
		}
	}
}

// TestSharedReceiptParsersRefuseDanglingDelimiters appends one unmatched
// delimiter to a valid producer receipt for every consumer of decodeStrict and
// pairs it with the unmodified and whitespace-padded receipt.
func TestSharedReceiptParsersRefuseDanglingDelimiters(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	fill := runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "4096", "30", "0")
	r := mustScratchReceipt(t, fill)
	if err := os.WriteFile(filepath.Join(dir, "seed"), []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	release := runScratchScript(t, scratchFillerScript, "release", dir, "filler",
		strconv.FormatUint(*r.File.Ino, 10), strconv.FormatUint(*r.File.Dev, 10), "seed")
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	backing := runScratchScript(t, scratchBlockBackingScript, block, dir)
	marker := filepath.Join(dir, "marker")
	markerOut := runScratchScript(t, scratchStoreMarkerScript, marker, storeToken)
	view := runScratchScript(t, scratchStoreViewScript, marker)

	parsers := map[string]struct {
		valid []byte
		parse func([]byte) error
	}{
		"fill receipt":    {fill, func(b []byte) error { _, err := parseScratchFillReceipt(b); return err }},
		"release receipt": {release, func(b []byte) error { _, err := parseScratchRelease(b); return err }},
		"block backing":   {backing, func(b []byte) error { _, err := parseBlockBacking(b); return err }},
		"store marker":    {markerOut, func(b []byte) error { _, err := parseStoreMarker(b); return err }},
		"store view":      {view, func(b []byte) error { _, err := parseStoreView(b); return err }},
	}
	for name, p := range parsers {
		if err := p.parse(p.valid); err != nil {
			t.Fatalf("%s: producer receipt refused: %v", name, err)
		}
		if err := p.parse(append([]byte("  "), append(append([]byte{}, p.valid...), " \n\t"...)...)); err != nil {
			t.Errorf("%s: whitespace-padded receipt refused: %v", name, err)
		}
		for _, tail := range []string{"}", "]", " }", "]\n", ` {"version": 1}`} {
			if err := p.parse(append(append([]byte{}, p.valid...), tail...)); err == nil {
				t.Errorf("%s: accepted a receipt followed by %q", name, tail)
			}
		}
	}
}
