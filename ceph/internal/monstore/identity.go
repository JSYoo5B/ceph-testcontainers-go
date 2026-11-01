package monstore

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// monStoreShapeRocksDB is the store shape of a MON data directory that holds
// kv_backend "rocksdb" and a store.db directory. It is the only shape the
// fixture binds.
const monStoreShapeRocksDB = "rocksdb"

// monStoreIdentity is what a MON store volume is bound to once its first
// generation is ready, besides the volume's own metadata (name, owner label,
// creation timestamp) and the MON name of its record. The values are inputs:
// a later slice observes them (a read-only inspector for the files, the live
// monmap for the FSID and the addresses) and passes them in. Nothing here reads
// Docker or Ceph.
//
// The digests are of secrets or of the store contents. They are compared and
// never printed: a mismatch error names the field, not its value.
type monStoreIdentity struct {
	// keyringDigest is the lowercase hex SHA-256 of the keyring file.
	keyringDigest string
	// shape is monStoreShapeRocksDB.
	shape string
	// fingerprint is the lowercase hex SHA-256 over the entries of the store
	// directory (mode, inode, size, modification time, file digests).
	fingerprint string
	// fsid is the cluster FSID the member serves.
	fsid string
	// addresses is the member's address vector from the live monmap. Order is
	// not significant.
	addresses []string
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

// validate rejects an identity with a missing, empty or malformed field. An
// absent value is never treated as an empty value that matches.
func (i monStoreIdentity) validate() error {
	var errs []error
	if !isHexSHA256(i.keyringDigest) {
		errs = append(errs, errors.New("keyring digest is missing or not a lowercase hex SHA-256"))
	}
	if i.shape != monStoreShapeRocksDB {
		errs = append(errs, fmt.Errorf("store shape must be %q", monStoreShapeRocksDB))
	}
	if !isHexSHA256(i.fingerprint) {
		errs = append(errs, errors.New("store fingerprint is missing or not a lowercase hex SHA-256"))
	}
	if parsed, err := uuid.Parse(i.fsid); err != nil || parsed.String() != i.fsid {
		errs = append(errs, errors.New("cluster FSID is missing or not a canonical UUID"))
	}
	if len(i.addresses) == 0 {
		errs = append(errs, errors.New("address vector is empty"))
	}
	seen := map[string]bool{}
	for _, address := range i.addresses {
		switch {
		case address == "":
			errs = append(errs, errors.New("address vector holds an empty address"))
		case seen[address]:
			errs = append(errs, errors.New("address vector holds a duplicate address"))
		}
		seen[address] = true
	}
	return errors.Join(errs...)
}

// clone returns an identity that shares no slice with i.
func (i monStoreIdentity) clone() monStoreIdentity {
	i.addresses = slices.Clone(i.addresses)
	slices.Sort(i.addresses)
	return i
}

// differences names the fields in which observed differs from i. Values are
// never reported.
func (i monStoreIdentity) differences(observed monStoreIdentity) []string {
	var out []string
	if i.keyringDigest != observed.keyringDigest {
		out = append(out, "keyring digest")
	}
	if i.shape != observed.shape {
		out = append(out, "store shape")
	}
	if i.fingerprint != observed.fingerprint {
		out = append(out, "store fingerprint")
	}
	if i.fsid != observed.fsid {
		out = append(out, "cluster FSID")
	}
	if !slices.Equal(i.clone().addresses, observed.clone().addresses) {
		out = append(out, "address vector")
	}
	return out
}
