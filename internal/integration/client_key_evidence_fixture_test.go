//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

package integration_test

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"testing"
)

func TestReplacementAttemptEvidence(t *testing.T) {
	attempted, resolved := &ceph.ClientConfig{}, &ceph.ClientConfig{}
	// Both uncertain native outcomes yield distinct immutable descriptors at
	// this public boundary. No synthetic credential is needed to distinguish them.
	for _, outcome := range []string{"uncertain applied", "uncertain not applied"} {
		got, err := replacementAttemptEvidence(attempted, resolved, context.DeadlineExceeded)
		if err != nil || got != "resolved/current; initial import application unverified" {
			t.Fatalf("%s: %q %v", outcome, got, err)
		}
	}
	if got, err := replacementAttemptEvidence(resolved, resolved, nil); err != nil || got != "confirmed replacement" {
		t.Fatalf("confirmed: %q %v", got, err)
	}
	if _, err := replacementAttemptEvidence(attempted, resolved, nil); err == nil {
		t.Fatal("changed confirmed descriptor accepted")
	}
}
