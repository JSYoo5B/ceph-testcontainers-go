package diagnostics

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
)

// Redact returns a copy of data in which the secrets that cluster knows, and
// values, are replaced by [REDACTED], as in Collect: keyring keys, service
// credentials, every client key the cluster generated, and the private keys,
// secret assignments and bearer tokens that it recognizes. It is meant for
// output that a test logs or reports; an oracle compares the raw bytes.
// Redaction is best effort.
//
//purpose:evidence
func Redact(ctx context.Context, cluster *ceph.Container, data []byte, values ...string) ([]byte, error) {
	snapshot, err := takeSnapshot(ctx, cluster, Config{RedactValues: values})
	if err != nil {
		return nil, err
	}
	return []byte(evidence.Redactor(snapshot.secrets)(string(data))), nil
}
