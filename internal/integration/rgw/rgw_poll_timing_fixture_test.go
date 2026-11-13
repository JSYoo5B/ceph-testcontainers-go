//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"errors"
	"fmt"
	"testing"
)

func TestPollTimingProfileOutcomeFollowsTheGatewayBinary(t *testing.T) {
	refusal := func(observed string) error {
		return fmt.Errorf("verify RGW zone source timing: incremental RGW timing is qualified only for Ceph 20.2.4 (observed %s)", observed)
	}
	for name, test := range map[string]struct {
		gateway string
		err     error
		refused bool
		problem bool
	}{
		"qualified gateway accepted":                {gateway: "20.2.4"},
		"qualified gateway refused":                 {gateway: "20.2.4", err: refusal("20.2.4"), problem: true},
		"qualified gateway, other error":            {gateway: "20.2.4", err: errors.New("run RGW zone source: boom"), problem: true},
		"unqualified gateway refused":               {gateway: "19.2.5", err: refusal("19.2.5"), refused: true},
		"newer unqualified gateway refused":         {gateway: "21.2.0", err: refusal("21.2.0"), refused: true},
		"unqualified gateway accepted":              {gateway: "19.2.5", problem: true},
		"refusal names another release":             {gateway: "19.2.5", err: refusal("21.2.0"), problem: true},
		"refusal names the cluster, not the binary": {gateway: "19.2.5", err: refusal("20.2.4"), problem: true},
		"unqualified gateway, other error":          {gateway: "19.2.5", err: errors.New("run RGW zone source: boom"), problem: true},
		"patch of the qualified minor":              {gateway: "20.2.40", err: refusal("20.2.4"), problem: true},
		"release not observed":                      {gateway: "", err: refusal(""), problem: true},
	} {
		t.Run(name, func(t *testing.T) {
			refused, problem := pollTimingProfileOutcome(test.gateway, test.err)
			if refused != test.refused || (problem != nil) != test.problem {
				t.Fatalf("outcome(%q, %v) = refused %t, problem %v", test.gateway, test.err, refused, problem)
			}
			if refused && problem != nil {
				t.Fatal("a refusal that is a problem")
			}
		})
	}
}
