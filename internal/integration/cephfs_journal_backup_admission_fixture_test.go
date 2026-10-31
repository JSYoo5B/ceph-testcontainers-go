//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"strings"
	"testing"
)

// These are Docker-free controls of the admission rule for an offline
// cephfs-journal export (a "backup") before any damage or import. The backups
// are synthetic files laid out the way the pinned Tentacle 20.2.4 Dumper::dump
// writes them: a 4096-byte textual header, then a seek to the absolute journal
// start offset and the payload [start, start+length). The header text follows
// the snprintf format of that function. Nothing here was recorded from a
// running cluster, no journal tool was executed, and the controls make no
// claim about tool availability, recovery or losslessness.
//
// Dumper::dump takes start and the payload length from the recovered
// Journaler (read_pos and the probed write_pos), but prints write_pos from
// get_last_committed(), the saved header. Probing can advance the working
// write_pos beyond the saved one, so a readable export can carry
// persisted write_pos != start+length. The first experiment admits only
// equal ends. A divergent pair is classified separately from a short or
// corrupt transfer, and the backup is neither rewritten nor truncated to make
// the ends agree.

const (
	jbStart     = 32768
	jbPersisted = 49152
	jbProbed    = 57344
	jbObjSize   = 16384
)

func jbBackup(start, length, writePos uint64) ([]byte, jbManifest) {
	file := make([]byte, start+length)
	copy(file, jbHeader(start, length, writePos, jbObjSize, 1, jbObjSize))
	for i := start; i < start+length; i++ {
		file[i] = byte(i%251) + 1
	}
	return file, jbManifest{
		FileSize:      uint64(len(file)),
		WholeSHA256:   jbSum(file),
		PayloadSHA256: jbSum(file[start:]),
	}
}

func jbRehash(file []byte, start uint64) jbManifest {
	return jbManifest{FileSize: uint64(len(file)), WholeSHA256: jbSum(file), PayloadSHA256: jbSum(file[start:])}
}

func jbEqualBackup() ([]byte, jbManifest) {
	return jbBackup(jbStart, jbPersisted-jbStart, jbPersisted)
}

// divergent: payload length from the probed end, header from the saved end.
func jbDivergentBackup() ([]byte, jbManifest) {
	return jbBackup(jbStart, jbProbed-jbStart, jbPersisted)
}

func TestJournalBackupAdmissionEqualEndIsAdmitted(t *testing.T) {
	file, m := jbEqualBackup()
	rec := jbAdmit(file, m, jbIntact)
	if rec.Class != jbAdmitted {
		t.Fatalf("equal ends: class %s (%s), want %s", rec.Class, rec.Reason, jbAdmitted)
	}
	if rec.PersistedWritePos != jbPersisted || rec.PayloadEnd != jbPersisted || rec.Start != jbStart || rec.Length != 16384 {
		t.Fatalf("record lost positions: %+v", rec)
	}
	if rec.PayloadSHA256 == "" || rec.WholeSHA256 == "" || rec.ObjectSize != jbObjSize {
		t.Fatalf("record lost layout/hash evidence: %+v", rec)
	}
}

func TestJournalBackupAdmissionDivergentEndIsRefusedWithBothEnds(t *testing.T) {
	file, m := jbDivergentBackup()
	before := jbSum(file)
	rec := jbAdmit(file, m, jbIntact)
	if rec.Class != jbOutsideDivergentEnd {
		t.Fatalf("divergent ends: class %s (%s), want %s", rec.Class, rec.Reason, jbOutsideDivergentEnd)
	}
	if rec.Start != 32768 || rec.Length != 24576 || rec.PersistedWritePos != 49152 || rec.PayloadEnd != 57344 {
		t.Fatalf("record must keep both ends for a refused backup: %+v", rec)
	}
	if rec.PayloadSHA256 == "" || rec.StripeCount != 1 {
		t.Fatalf("refusal must retain layout and hash evidence: %+v", rec)
	}
	if after := jbSum(file); after != before {
		t.Fatalf("admission modified the saved backup")
	}
	if rec.Class == jbShortOrCorrupt {
		t.Fatalf("divergent ends must not be filed as a short or corrupt transfer")
	}
}

func TestJournalBackupAdmissionUnreadableFieldsAreInconclusive(t *testing.T) {
	cases := map[string]func(h []byte) []byte{
		"missing write_pos": func(h []byte) []byte {
			return bytes.Replace(h, []byte("    write_pos 49152 (0xc000)\n"), nil, 1)
		},
		"null write_pos": func(h []byte) []byte {
			return bytes.Replace(h, []byte("write_pos 49152 (0xc000)"), []byte("write_pos null"), 1)
		},
		"wrong-type length": func(h []byte) []byte {
			return bytes.Replace(h, []byte("length 16384 (0x4000)"), []byte("length \"16384\" (0x4000)"), 1)
		},
		"empty stripe_count": func(h []byte) []byte {
			return bytes.Replace(h, []byte("stripe_count 1 (0x1)"), []byte("stripe_count  (0x1)"), 1)
		},
		"hex disagrees": func(h []byte) []byte {
			return bytes.Replace(h, []byte("(0xc000)"), []byte("(0xd000)"), 1)
		},
		"negative start": func(h []byte) []byte {
			return bytes.Replace(h, []byte("start offset 32768"), []byte("start offset -32768"), 1)
		},
		"duplicate write_pos": func(h []byte) []byte {
			return bytes.Replace(h, []byte("    format 1\n"), []byte("    format 1\n    write_pos 0 (0x0)\n"), 1)
		},
		"terminator absent": func(h []byte) []byte {
			return bytes.Replace(h, []byte{4}, []byte{' '}, 1)
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			file, _ := jbEqualBackup()
			hdr := mut(append([]byte(nil), file[:jbHeaderLen]...))
			if len(hdr) > jbHeaderLen {
				hdr = hdr[:jbHeaderLen]
			}
			for len(hdr) < jbHeaderLen {
				hdr = append(hdr, 0)
			}
			copy(file, hdr)
			rec := jbAdmit(file, jbRehash(file, jbStart), jbIntact)
			if rec.Class != jbInconclusive {
				t.Fatalf("class %s (%s), want %s", rec.Class, rec.Reason, jbInconclusive)
			}
		})
	}
}

func TestJournalBackupAdmissionShortAndCorruptStayTheirOwnClass(t *testing.T) {
	good, m := jbEqualBackup()
	div, dm := jbDivergentBackup()

	short := good[:len(good)-1]
	altered := append([]byte(nil), good...)
	altered[jbStart+5] ^= 0xff
	headerOnly := good[:jbHeaderLen]
	tiny := good[:100]
	badMagic := append([]byte(nil), good...)
	copy(badMagic, "Xeph")
	shortDiv := div[:len(div)-1]

	for name, c := range map[string]struct {
		file []byte
		m    jbManifest
	}{
		"one byte short":             {short, m},
		"same-size altered payload":  {altered, m},
		"header only":                {headerOnly, m},
		"below header size":          {tiny, m},
		"bad magic":                  {badMagic, m},
		"short divergent-end backup": {shortDiv, dm},
	} {
		t.Run(name, func(t *testing.T) {
			rec := jbAdmit(c.file, c.m, jbIntact)
			if rec.Class != jbShortOrCorrupt {
				t.Fatalf("class %s (%s), want %s", rec.Class, rec.Reason, jbShortOrCorrupt)
			}
		})
	}
}

func TestJournalBackupAdmissionStripedLayoutIsOutsideLayout(t *testing.T) {
	file, _ := jbBackup(jbStart, jbPersisted-jbStart, jbPersisted)
	copy(file, jbHeader(jbStart, jbPersisted-jbStart, jbPersisted, 4096, 2, jbObjSize))
	rec := jbAdmit(file, jbRehash(file, jbStart), jbIntact)
	if rec.Class != jbOutsideLayout {
		t.Fatalf("class %s (%s), want %s", rec.Class, rec.Reason, jbOutsideLayout)
	}
}

// The negative controls run the same checks against a deliberately broken
// admission and require that the check then fails.
func TestJournalBackupAdmissionNegativeControls(t *testing.T) {
	t.Run("relaxed equality admits divergent ends", func(t *testing.T) {
		file, m := jbDivergentBackup()
		if rec := jbAdmit(file, m, jbRelaxEquality); rec.Class != jbAdmitted {
			t.Fatalf("broken rule should have admitted the divergent backup, got %s", rec.Class)
		}
	})
	t.Run("divergent filed as corrupt loses the distinct class", func(t *testing.T) {
		file, m := jbDivergentBackup()
		if rec := jbAdmit(file, m, jbDivergentIsCorr); rec.Class == jbOutsideDivergentEnd {
			t.Fatalf("broken rule should not report the divergent class")
		}
	})
	t.Run("missing field decoded as zero is no longer inconclusive", func(t *testing.T) {
		file, _ := jbEqualBackup()
		hdr := bytes.Replace(append([]byte(nil), file[:jbHeaderLen]...), []byte("    write_pos 49152 (0xc000)\n"), nil, 1)
		hdr = append(hdr, make([]byte, jbHeaderLen-len(hdr))...)
		copy(file, hdr)
		m := jbRehash(file, jbStart)
		if rec := jbAdmit(file, m, jbIntact); rec.Class != jbInconclusive {
			t.Fatalf("intact rule: class %s, want inconclusive", rec.Class)
		}
		if rec := jbAdmit(file, m, jbMissingAsZero); rec.Class == jbInconclusive {
			t.Fatalf("broken rule should have decoded the missing field into a value")
		}
	})
	t.Run("skipped hash admits an altered payload", func(t *testing.T) {
		file, m := jbEqualBackup()
		file[jbStart+9] ^= 0xff
		if rec := jbAdmit(file, m, jbIntact); rec.Class != jbShortOrCorrupt {
			t.Fatalf("intact rule: class %s", rec.Class)
		}
		if rec := jbAdmit(file, m, jbSkipHash); rec.Class != jbAdmitted {
			t.Fatalf("broken rule should have admitted the altered payload, got %s", rec.Class)
		}
	})
	t.Run("skipped size check admits a short file", func(t *testing.T) {
		file, m := jbEqualBackup()
		short := file[:len(file)-1]
		if rec := jbAdmit(short, m, jbSkipSizeCheck); strings.Contains(rec.Reason, "manifest") {
			t.Fatalf("broken rule should not report the size mismatch")
		}
	})
	t.Run("rewriting the saved header is observable", func(t *testing.T) {
		file, m := jbDivergentBackup()
		before := jbSum(file)
		jbAdmit(file, m, jbRewriteHeader)
		if jbSum(file) == before {
			t.Fatalf("broken rule should have modified the saved backup, so the immutability control can detect it")
		}
	})
}

func TestJournalBackupAdmissionKeepsTruncationNegativeControl(t *testing.T) {
	// Truncating the payload to the persisted end and rewriting the header
	// would make the ends agree. Only the pre-damage manifest refuses such a
	// file; a self-consistent truncated file passes the local rules, which
	// is why the admission never truncates and never accepts one.
	file, m := jbDivergentBackup()
	truncated := append([]byte(nil), file[:jbPersisted]...)
	copy(truncated, jbHeader(jbStart, jbPersisted-jbStart, jbPersisted, jbObjSize, 1, jbObjSize))
	if rec := jbAdmit(truncated, m, jbIntact); rec.Class != jbShortOrCorrupt {
		t.Fatalf("a truncated divergent backup must stay short-or-corrupt against its manifest, got %s", rec.Class)
	}
	if rec := jbAdmit(truncated, jbRehash(truncated, jbStart), jbIntact); rec.Class != jbAdmitted {
		t.Fatalf("control: a self-consistent truncated file passes the local rules (%s); only the pre-damage manifest refuses it", rec.Class)
	}
}
