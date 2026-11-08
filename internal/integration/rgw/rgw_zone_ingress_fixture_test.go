//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"net/http"
	"testing"
	"time"
)

func TestIngressAcceptsOnlyCompletedGET(t *testing.T) {
	for _, tc := range []struct {
		status, exit int
		want         bool
	}{
		{http.StatusOK, 0, true},
		{http.StatusOK, 18, false}, // the payload arrived but Content-Length promised more
		{http.StatusServiceUnavailable, 0, false},
	} {
		a := ingressAttempt{Status: tc.status, CurlExit: tc.exit, SHA256: "expected", Served: "zone-a"}
		if got := a.succeeded(); got != tc.want {
			t.Errorf("attempt %+v accepted=%t, want %t", a, got, tc.want)
		}
	}
}

func TestIngressRejectsDelayOnUnselectedCrossZonePath(t *testing.T) {
	fast := harness.ZoneRTT{Requested: 20, OK: 20, Median: 0.2, P10: 0.1, P90: 0.3}
	slow := harness.ZoneRTT{Requested: 20, OK: 20, Median: 40.2, P10: 40.1, P90: 40.3}
	base := ingressRTTs{local: fast, selected: fast, unselected: fast}
	shaped := ingressRTTs{local: fast, selected: slow, unselected: fast}
	if err := shaped.check(base, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	shaped.unselected = slow
	if err := shaped.check(base, 40*time.Millisecond); err == nil {
		t.Fatal("accepted delay on all cross-zone traffic")
	}
}
