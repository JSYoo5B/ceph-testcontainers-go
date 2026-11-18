//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

func TestMessengerDelayNeedsReceiverEvidence(t *testing.T) {
	queued := "ms queue_received will delay after 2279.231 on 0x987 osd_op(client.7.0:3 pg)"
	anotherClient := "ms queue_received will delay after 2279.231 on 0x987 osd_op(client.70.0:3 pg)"
	for _, test := range []struct {
		name      string
		seconds   float64
		lines     []string
		wantDelay bool
		wantOK    bool
	}{
		{"slow host without injection", 4, []string{anotherClient}, true, false},
		{"small random delay", 0.0002, []string{queued}, true, true},
		{"slow after restore", 7, []string{anotherClient}, false, true},
		{"delay remains after restore", 0.0002, []string{queued}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := messengerFaultRun{ClientID: 7, Results: []messengerFaultResult{{Name: "example", Seconds: test.seconds, Verified: true}}}
			if _, err := run.checkDelay(test.lines, test.wantDelay); (err == nil) != test.wantOK {
				t.Fatalf("delay verdict: %v, want success=%t", err, test.wantOK)
			}
		})
	}
}
