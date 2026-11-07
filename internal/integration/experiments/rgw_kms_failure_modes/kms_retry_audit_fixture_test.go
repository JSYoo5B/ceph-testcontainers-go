//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

// ci: timeout=20m job-timeout=30
package rgw_kms_failure_modes_test

import "testing"

func TestTransitRetryAuditDetectsADispatchedDeleteWithoutAReply(t *testing.T) {
	const path = "tctransit/keys/orphan"
	audit := []byte(`{"type":"response","request":{"path":"tctransit/keys/orphan/config","operation":"update"}}`)
	before := rgwSSES3KeyAudit(t, audit, path)
	if before.configured != 1 || before.attempts != 0 {
		t.Fatal("unexpected initial key audit")
	}
	audit = append(audit, []byte("\n"+`{"type":"request","request":{"path":"tctransit/keys/orphan","operation":"delete"}}`)...)
	after := rgwSSES3KeyAudit(t, audit, path)
	if after == before || after.attempts != 1 || after.deleted != 0 {
		t.Fatal("missed the new in-flight deletion")
	}
}
