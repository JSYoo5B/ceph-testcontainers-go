package ceph

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"reflect"
	"slices"
	"strings"
)

// This file is the read-only observation side of the MON store identity
// binding of docs/design/mon-volume-storage.md. It defines two ports and the
// computation that turns what they report into a monStoreIdentity:
//
//   - monStoreInspector: a short-lived read-only inspector of the volume (design
//     decision D6, the recommendation; the owner has not answered it). It reports
//     the entries of the data directory and nothing else.
//   - monStoreMonmap: the live monmap view of one member while its MON is down.
//
// Nothing implements either port in production and nothing calls the functions
// of this file: a Docker-free fake in the tests stands in for both. A later
// slice adds the inspector container, the monmap adapter and the caller. That
// the adapters decode their sources into the shapes below, and that an absent
// or null source field becomes an absent value here, is theirs to prove; this
// file only refuses what is absent or malformed.

// monStoreKind is the kind of one directory entry as the inspector saw it
// without following links.
type monStoreKind string

const (
	monStoreKindFile    monStoreKind = "file"
	monStoreKindDir     monStoreKind = "dir"
	monStoreKindSymlink monStoreKind = "symlink"
	monStoreKindOther   monStoreKind = "other"
)

// monStoreEntry is one entry below the data directory. path is slash separated
// and relative to the data directory. digest is the lowercase hex SHA-256 of a
// regular file's content and is empty for every other kind.
type monStoreEntry struct {
	path    string
	kind    monStoreKind
	mode    uint32
	inode   uint64
	size    int64
	mtimeNs int64
	digest  string
}

// monStoreFiles is what the inspector reports for one volume: every entry
// below the data directory, and the content of the kv_backend marker. A nil
// kvBackend means the inspector did not read it; it is never read as an empty
// marker.
type monStoreFiles struct {
	entries   []monStoreEntry
	kvBackend *string
}

// monStoreInspector observes the data directory of the named volume without
// writing. An implementation mounts the volume read-only for the duration of
// the call and is gone when it returns, so it is a volume user only while it
// runs.
type monStoreInspector interface {
	Inspect(ctx context.Context, volume string) (monStoreFiles, error)
}

// monStoreQuorum is the quorum fact of a member. The zero value is unknown and
// is never read as "out of quorum".
type monStoreQuorum int

const (
	monStoreQuorumIn monStoreQuorum = iota + 1
	monStoreQuorumOut
)

// monStoreMember is one member of the live monmap: its name, the cluster FSID
// the map belongs to, its address vector and whether it is in quorum.
type monStoreMember struct {
	name      string
	fsid      string
	addresses []string
	quorum    monStoreQuorum
}

// monStoreMonmap reads the live monmap. A nil member with a nil error means
// the name is not in the map.
type monStoreMonmap interface {
	Member(ctx context.Context, mon string) (*monStoreMember, error)
}

// monStoreFingerprintDomain separates this encoding from any other use of
// SHA-256 and versions it.
const monStoreFingerprintDomain = "tc-ceph-mon-store-fingerprint-v1\n"

// isNilPort reports a nil interface or an interface that holds a nil pointer,
// map, slice, channel or function.
func isNilPort(v any) bool {
	if v == nil {
		return true
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// check validates the shape of one entry. It does not look at its neighbors.
func (e monStoreEntry) check() error {
	clean := path.Clean(e.path)
	switch {
	case e.path == "" || clean != e.path || strings.HasPrefix(e.path, "/") || e.path == "." || e.path == ".." || strings.HasPrefix(e.path, "../"):
		return fmt.Errorf("entry %q is not a clean relative path below the data directory", e.path)
	case e.size < 0:
		return fmt.Errorf("entry %q has a negative size", e.path)
	}
	switch e.kind {
	case monStoreKindFile:
		if !isHexSHA256(e.digest) {
			return fmt.Errorf("file %q has no lowercase hex SHA-256 content digest", e.path)
		}
	case monStoreKindDir:
		if e.digest != "" {
			return fmt.Errorf("directory %q carries a content digest", e.path)
		}
	case monStoreKindSymlink, monStoreKindOther:
		return fmt.Errorf("entry %q is a %s: a store holds only files and directories", e.path, e.kind)
	default:
		return fmt.Errorf("entry %q has an unknown kind", e.path)
	}
	return nil
}

// monStoreFingerprint is the lowercase hex SHA-256 over the entries in path order: the
// domain line, then for each entry its path (length prefixed), kind, mode,
// inode, size, modification time in nanoseconds and content digest (length
// prefixed). Every field is length-delimited or fixed width, so no two
// different listings encode alike. It is a change detector for the fixture's
// own store, not a statement that ceph-mon accepts the bytes.
func monStoreFingerprint(entries []monStoreEntry) string {
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b monStoreEntry) int { return strings.Compare(a.path, b.path) })
	h := sha256.New()
	h.Write([]byte(monStoreFingerprintDomain))
	text := func(s string) {
		_ = binary.Write(h, binary.BigEndian, uint32(len(s)))
		h.Write([]byte(s))
	}
	for _, e := range sorted {
		text(e.path)
		text(string(e.kind))
		_ = binary.Write(h, binary.BigEndian, e.mode)
		_ = binary.Write(h, binary.BigEndian, e.inode)
		_ = binary.Write(h, binary.BigEndian, e.size)
		_ = binary.Write(h, binary.BigEndian, e.mtimeNs)
		text(e.digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// storeFacts checks a listing against the shape of the store the fixture binds
// and returns its keyring digest, shape and fingerprint. Every refusal names
// the entry or the field and never a digest. Unlike the entrypoint's
// require-store check, which accepts an absent kv_backend or keyring because
// ceph-mon recreates them, a binding needs both: ceph-mon writes kv_backend
// whenever it opens a store, and a bound store has run a generation to ready.
func (f monStoreFiles) storeFacts() (keyringDigest, shape, fingerprint string, err error) {
	byPath := make(map[string]monStoreEntry, len(f.entries))
	for _, e := range f.entries {
		if err := e.check(); err != nil {
			return "", "", "", err
		}
		if _, dup := byPath[e.path]; dup {
			return "", "", "", fmt.Errorf("entry %q is listed twice", e.path)
		}
		byPath[e.path] = e
	}
	inStoreDB := false
	for p := range byPath {
		if parent := path.Dir(p); parent != "." {
			if byPath[parent].kind != monStoreKindDir {
				return "", "", "", fmt.Errorf("entry %q has no listed parent directory", p)
			}
		}
		inStoreDB = inStoreDB || strings.HasPrefix(p, "store.db/")
	}
	if byPath["store.db"].kind != monStoreKindDir {
		return "", "", "", errors.New("the store has no store.db directory")
	}
	if !inStoreDB {
		return "", "", "", errors.New("the store.db directory is empty")
	}
	if byPath["kv_backend"].kind != monStoreKindFile {
		return "", "", "", errors.New("the store has no kv_backend file")
	}
	if f.kvBackend == nil {
		return "", "", "", errors.New("the kv_backend marker was not read")
	}
	if strings.TrimRight(*f.kvBackend, "\n") != monStoreShapeRocksDB {
		return "", "", "", errors.New("the kv_backend marker is not rocksdb")
	}
	keyring := byPath["keyring"]
	if keyring.kind != monStoreKindFile {
		return "", "", "", errors.New("the store has no keyring regular file")
	}
	return keyring.digest, monStoreShapeRocksDB, monStoreFingerprint(f.entries), nil
}

// observeStopped builds the identity of the stopped MON mon from one live
// monmap answer and one inspection of the volume. The member must be in the
// map, in no quorum, and report a valid FSID and address vector; the listing
// must have the shape of storeFacts. Any refusal returns before a second
// source is read when the first already refuses, and nothing is written.
func observeStopped(ctx context.Context, mon, volume string, inspector monStoreInspector, monmap monStoreMonmap) (monStoreIdentity, error) {
	if isNilPort(inspector) || isNilPort(monmap) {
		return monStoreIdentity{}, errors.New("MON store observation needs an inspector and a monmap view")
	}
	member, err := monmap.Member(ctx, mon)
	if err != nil {
		return monStoreIdentity{}, fmt.Errorf("read the monmap entry of MON %s: %w", mon, err)
	}
	switch {
	case member == nil:
		return monStoreIdentity{}, fmt.Errorf("MON %s is not a member of the live monmap", mon)
	case member.name != mon:
		return monStoreIdentity{}, fmt.Errorf("the monmap entry returned for MON %s names another member", mon)
	case member.quorum == monStoreQuorumIn:
		return monStoreIdentity{}, fmt.Errorf("MON %s is in quorum: a running member's store is not inspected", mon)
	case member.quorum != monStoreQuorumOut:
		return monStoreIdentity{}, fmt.Errorf("the quorum state of MON %s is unknown", mon)
	}
	files, err := inspector.Inspect(ctx, volume)
	if err != nil {
		return monStoreIdentity{}, fmt.Errorf("inspect the store of MON %s: %w", mon, err)
	}
	keyringDigest, shape, fingerprint, err := files.storeFacts()
	if err != nil {
		return monStoreIdentity{}, fmt.Errorf("MON %s store: %w", mon, err)
	}
	observed := monStoreIdentity{keyringDigest: keyringDigest, shape: shape, fingerprint: fingerprint, fsid: member.fsid, addresses: member.addresses}.clone()
	if err := observed.validate(); err != nil {
		return monStoreIdentity{}, fmt.Errorf("MON %s store observation: %w", mon, err)
	}
	return observed, nil
}

// verifyStopped is the read-only preflight of a retained generation: it checks
// the volume metadata, observes the stopped store and the live monmap, and
// compares the result with the recorded identity through verify. A refusal at
// any step leaves the record unverified, names fields and never values, and
// writes nothing: no volume is created, no container is dispatched and no
// membership changes. It admits an inspection only while no generation is
// attached, so the inspector never reads a store a MON may be writing.
func (s *monStoreVolume) verifyStopped(ctx context.Context, inspector monStoreInspector, monmap monStoreMonmap) error {
	s.verified = false
	if err := s.requireRetained(); err != nil {
		return err
	}
	if err := s.inspect(ctx); err != nil {
		return err
	}
	observed, err := observeStopped(ctx, s.mon, s.name, inspector, monmap)
	if err != nil {
		return err
	}
	return s.verify(observed)
}
