//go:build all || integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// The admission rule for an offline cephfs-journal export (a "backup") and the
// classification of one captured export attempt. The pure fixtures
// (cephfs_journal_backup_admission_fixture_test.go,
// cephfs_journal_export_availability_fixture_test.go) exercise them on
// synthetic files; the native header-restore scenario feeds them a real
// export.

// jbHeaderLen is the length of the textual header of Dumper::dump.
const jbHeaderLen = 4096

// jbClass is the outcome of the admission of one backup.
type jbClass string

const (
	// jbAdmitted: complete, hash-matching, one-stripe, equal ends.
	jbAdmitted jbClass = "admitted"
	// jbInconclusive: a required header field is missing, null, duplicated,
	// or of the wrong type, so no verdict on the backup is possible.
	jbInconclusive jbClass = "inconclusive"
	// jbShortOrCorrupt: the transfer is incomplete or altered (size, magic
	// or checksum), whatever the header says.
	jbShortOrCorrupt jbClass = "short-or-corrupt-transfer"
	// jbOutsideLayout: readable and intact, but not the one-stripe layout.
	jbOutsideLayout jbClass = "outside-first-experiment-layout"
	// jbOutsideDivergentEnd: readable and intact, but the persisted header
	// end differs from the checked payload end.
	jbOutsideDivergentEnd jbClass = "outside-first-experiment-divergent-end"
)

// jbRecord is the admission evidence. Both ends are kept whenever they were
// parsed, including for a refused divergent-end backup.
type jbRecord struct {
	Class             jbClass
	Reason            string
	Start             uint64
	Length            uint64
	PersistedWritePos uint64
	PayloadEnd        uint64 // start+length, checked for overflow
	StripeUnit        uint64
	StripeCount       uint64
	ObjectSize        uint64
	FileSize          uint64
	PayloadSHA256     string
	WholeSHA256       string
}

// jbManifest is what the exporter recorded before transfer.
type jbManifest struct {
	FileSize      uint64
	WholeSHA256   string
	PayloadSHA256 string
}

// jbBreak deliberately breaks one rule of the admission; the negative
// controls use it to show that the matching test fails without that rule.
type jbBreak string

const (
	jbIntact          jbBreak = ""
	jbRelaxEquality   jbBreak = "relax-equality"
	jbMissingAsZero   jbBreak = "missing-as-zero"
	jbDivergentIsCorr jbBreak = "divergent-as-short-or-corrupt"
	jbRewriteHeader   jbBreak = "rewrite-saved-header"
	jbSkipHash        jbBreak = "skip-hash"
	jbSkipSizeCheck   jbBreak = "skip-size-check"
)

var jbFieldPatterns = []struct {
	name   string
	re     *regexp.Regexp
	hexToo bool
}{
	{"start offset", regexp.MustCompile(`(?m)^ start offset (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"length", regexp.MustCompile(`(?m)^ +length (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"write_pos", regexp.MustCompile(`(?m)^ +write_pos (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"format", regexp.MustCompile(`(?m)^ +format (\d+)$`), false},
	{"trimmed_pos", regexp.MustCompile(`(?m)^ +trimmed_pos (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"stripe_unit", regexp.MustCompile(`(?m)^ +stripe_unit (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"stripe_count", regexp.MustCompile(`(?m)^ +stripe_count (\d+) \(0x([0-9a-f]+)\)$`), true},
	{"object_size", regexp.MustCompile(`(?m)^ +object_size (\d+) \(0x([0-9a-f]+)\)$`), true},
}

var jbNamePattern = map[string]*regexp.Regexp{
	"start offset": regexp.MustCompile(`(?m)^ start offset `),
	"length":       regexp.MustCompile(`(?m)^ +length `),
	"write_pos":    regexp.MustCompile(`(?m)^ +write_pos `),
	"format":       regexp.MustCompile(`(?m)^ +format `),
	"trimmed_pos":  regexp.MustCompile(`(?m)^ +trimmed_pos `),
	"stripe_unit":  regexp.MustCompile(`(?m)^ +stripe_unit `),
	"stripe_count": regexp.MustCompile(`(?m)^ +stripe_count `),
	"object_size":  regexp.MustCompile(`(?m)^ +object_size `),
}

const jbBound = 1 << 50

func jbSum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// jbParseHeader decodes the textual header. Any field that is absent, appears
// twice, does not match its exact format (for example null or text where an
// integer is required), whose decimal and hexadecimal spellings disagree, or
// that exceeds the bound, is an error: it is never decoded as zero.
func jbParseHeader(hdr []byte, missingAsZero bool) (map[string]uint64, error) {
	end := bytes.IndexByte(hdr, 4)
	if end < 0 {
		return nil, fmt.Errorf("header terminator 0x04 absent")
	}
	text := hdr[:end]
	fields := map[string]uint64{}
	for _, p := range jbFieldPatterns {
		if n := len(jbNamePattern[p.name].FindAll(text, -1)); n > 1 {
			return nil, fmt.Errorf("field %q appears %d times", p.name, n)
		}
		m := p.re.FindSubmatch(text)
		if m == nil {
			if missingAsZero {
				fields[p.name] = 0
				continue
			}
			return nil, fmt.Errorf("field %q missing or not an integer", p.name)
		}
		v, err := strconv.ParseUint(string(m[1]), 10, 64)
		if err != nil || v > jbBound {
			return nil, fmt.Errorf("field %q out of bounds", p.name)
		}
		if p.hexToo {
			h, err := strconv.ParseUint(string(m[2]), 16, 64)
			if err != nil || h != v {
				return nil, fmt.Errorf("field %q decimal and hexadecimal disagree", p.name)
			}
		}
		fields[p.name] = v
	}
	return fields, nil
}

// jbAdmit classifies one backup. It never writes to the backup: the saved
// header and the payload stay byte-for-byte as transferred. When the
// deliberate breakage rewrites or truncates, it does so on the input slice so
// that the immutability control can observe it.
func jbAdmit(file []byte, m jbManifest, brk jbBreak) jbRecord {
	rec := jbRecord{FileSize: uint64(len(file))}
	if len(file) < jbHeaderLen {
		rec.Class, rec.Reason = jbShortOrCorrupt, "file shorter than the 4096-byte header"
		return rec
	}
	if !bytes.HasPrefix(file, []byte("Ceph mds")) {
		rec.Class, rec.Reason = jbShortOrCorrupt, "header magic absent"
		return rec
	}
	f, err := jbParseHeader(file[:jbHeaderLen], brk == jbMissingAsZero)
	if err != nil {
		rec.Class, rec.Reason = jbInconclusive, err.Error()
		return rec
	}
	rec.Start, rec.Length, rec.PersistedWritePos = f["start offset"], f["length"], f["write_pos"]
	rec.StripeUnit, rec.StripeCount, rec.ObjectSize = f["stripe_unit"], f["stripe_count"], f["object_size"]
	rec.PayloadEnd = rec.Start + rec.Length // both are bounded by 2^50
	if brk != jbSkipSizeCheck && (rec.FileSize != m.FileSize || rec.FileSize != rec.PayloadEnd) {
		rec.Class, rec.Reason = jbShortOrCorrupt, fmt.Sprintf("file size %d, manifest %d, start+length %d", rec.FileSize, m.FileSize, rec.PayloadEnd)
		return rec
	}
	if rec.PayloadEnd > uint64(len(file)) {
		rec.Class, rec.Reason = jbShortOrCorrupt, "payload range exceeds the file"
		return rec
	}
	rec.PayloadSHA256 = jbSum(file[rec.Start:rec.PayloadEnd])
	rec.WholeSHA256 = jbSum(file)
	if brk != jbSkipHash && (rec.PayloadSHA256 != m.PayloadSHA256 || rec.WholeSHA256 != m.WholeSHA256) {
		rec.Class, rec.Reason = jbShortOrCorrupt, "checksum differs from the manifest"
		return rec
	}
	if rec.StripeCount != 1 || rec.StripeUnit != rec.ObjectSize || rec.ObjectSize == 0 {
		rec.Class, rec.Reason = jbOutsideLayout, "not the one-stripe layout"
		return rec
	}
	if rec.PersistedWritePos != rec.PayloadEnd && brk != jbRelaxEquality {
		rec.Class, rec.Reason = jbOutsideDivergentEnd, fmt.Sprintf("persisted write_pos %d, payload end %d", rec.PersistedWritePos, rec.PayloadEnd)
		if brk == jbDivergentIsCorr {
			rec.Class = jbShortOrCorrupt
		}
		if brk == jbRewriteHeader {
			copy(file[:jbHeaderLen], jbHeader(rec.Start, rec.Length, rec.PayloadEnd, rec.StripeUnit, rec.StripeCount, rec.ObjectSize))
		}
		return rec
	}
	rec.Class = jbAdmitted
	return rec
}

// jeOutcome classifies one captured export attempt before the admission model
// sees any bytes.
type jeOutcome string

const (
	jeObsInconclusive jeOutcome = "inconclusive"
	jeObsToolFailed   jeOutcome = "tool-failed"
	jeObsAdmission    jeOutcome = "admission-recorded"
)

// jeObserve decodes one export capture and, only for a zero exit status with
// intact observation fields, runs the existing jbAdmit on the untouched
// export. Any classification of jbAdmit is a valid recorded result. Fields
// must be present with the exact JSON type; nothing is defaulted to zero.
func jeObserve(obs []byte, export []byte) (jeOutcome, string, jbRecord) {
	var raw map[string]any
	if err := json.Unmarshal(obs, &raw); err != nil || raw == nil {
		return jeObsInconclusive, "observation is not a JSON object", jbRecord{}
	}
	integer := func(name string) (uint64, bool) {
		f, ok := raw[name].(float64)
		if !ok || f < 0 || f != math.Trunc(f) || f > 1<<50 {
			return 0, false
		}
		return uint64(f), true
	}
	text := func(name string) (string, bool) { s, ok := raw[name].(string); return s, ok }
	exit, ok1 := raw["exit_code"].(float64)
	_, ok2 := text("stdout")
	_, ok3 := text("stderr")
	size, ok4 := integer("export_size")
	whole, ok5 := text("export_sha256")
	if !ok1 || exit != math.Trunc(exit) || !ok2 || !ok3 || !ok4 || !ok5 || len(whole) != 64 || export == nil {
		return jeObsInconclusive, "observation field missing, null or wrong-typed", jbRecord{}
	}
	if exit != 0 {
		return jeObsToolFailed, fmt.Sprintf("exit code %d", int(exit)), jbRecord{}
	}
	// The payload hash is derived from the header-declared start of the same
	// bytes; the whole-file hash and size are what the capture recorded.
	manifest := jbManifest{FileSize: size, WholeSHA256: whole}
	if fields, err := jbParseHeader(export[:min(len(export), jbHeaderLen)], false); err == nil && fields["start offset"] <= uint64(len(export)) {
		manifest.PayloadSHA256 = jbSum(export[fields["start offset"]:])
	}
	rec := jbAdmit(append([]byte(nil), export...), manifest, jbIntact)
	if rec.Class == jbInconclusive {
		return jeObsInconclusive, rec.Reason, rec
	}
	return jeObsAdmission, string(rec.Class), rec
}

// jbHeader renders the Dumper::dump header text for the given positions.
func jbHeader(start, length, writePos, unit, count, objSize uint64) []byte {
	buf := make([]byte, jbHeaderLen)
	txt := fmt.Sprintf("Ceph mds%d journal dump\n start offset %d (0x%x)\n"+
		"       length %d (0x%x)\n    write_pos %d (0x%x)\n    format %d\n"+
		"       trimmed_pos %d (0x%x)\n    stripe_unit %d (0x%x)\n    stripe_count %d (0x%x)\n"+
		"       object_size %d (0x%x)\n    fsid %s\n%c",
		0, start, start, length, length, writePos, writePos, 1,
		start, start, unit, unit, count, count, objSize, objSize,
		"00000000-0000-0000-0000-000000000000", 4)
	copy(buf, txt)
	return buf
}

// journalPayload reads the journal range [start, end) of a one-stripe layout
// straight from the metadata pool's 200.* objects, and whether each object
// after the one holding end exists, up to the two import purges. It prints the
// payload digest, final-object length and the existence of those objects. Arguments: pool, start,
// end, object size.
const journalPayload = `import hashlib, json, rados, sys
pool, start, end, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
out = {}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(pool) as io:
        data, pos = b"", start
        while pos < end:
            obj, off = pos // size, pos % size
            n = min(size - off, end - pos)
            chunk = io.read("200.%08x" % obj, n, off)
            if len(chunk) != n:
                out["short_object"] = obj
                break
            data, pos = data + chunk, pos + n
        out["payload_sha256"] = hashlib.sha256(data).hexdigest()
        last = (end - 1) // size
        out["last_object_size"] = io.stat("200.%08x" % last)[0]
        beyond = {}
        for obj in range(last + 1, (end // size) + 2):
            try:
                io.stat("200.%08x" % obj)
                beyond["200.%08x" % obj] = True
            except rados.ObjectNotFound:
                beyond["200.%08x" % obj] = False
        out["beyond_end"] = beyond
print(json.dumps(out))`

// journalPayloadView is the offline RADOS observation after journal import.
type journalPayloadView struct {
	SHA256         string          `json:"payload_sha256"`
	Short          *int            `json:"short_object"`
	LastObjectSize *uint64         `json:"last_object_size"`
	Beyond         map[string]bool `json:"beyond_end"`
}

// checkJournalPayload requires the imported payload and its complete physical
// tail to match the admitted one-stripe backup before an MDS may start.
func checkJournalPayload(v journalPayloadView, rec jbRecord) error {
	if rec.ObjectSize == 0 || rec.PayloadEnd <= rec.Start {
		return fmt.Errorf("journal payload has no nonempty bounded range")
	}
	if v.Short != nil || v.SHA256 != rec.PayloadSHA256 {
		return fmt.Errorf("the restored journal payload differs from the backup: %+v", v)
	}
	last := (rec.PayloadEnd - 1) / rec.ObjectSize
	wantSize := (rec.PayloadEnd-1)%rec.ObjectSize + 1
	if v.LastObjectSize == nil {
		return fmt.Errorf("journal final object 200.%08x length was not observed", last)
	}
	if *v.LastObjectSize != wantSize {
		return fmt.Errorf("journal final object 200.%08x has stale/truncated tail: got %d bytes, want %d", last, *v.LastObjectSize, wantSize)
	}
	for obj := last + 1; obj < rec.PayloadEnd/rec.ObjectSize+2; obj++ {
		name := fmt.Sprintf("200.%08x", obj)
		exists, observed := v.Beyond[name]
		if !observed || exists {
			return fmt.Errorf("journal object %s beyond the restored end exists or was not observed", name)
		}
	}
	return nil
}
