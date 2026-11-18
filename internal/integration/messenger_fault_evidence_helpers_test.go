//go:build all || integration

package integration_test

import (
	"fmt"
	"strings"
)

type messengerFaultRun struct {
	ClientID uint64                 `json:"client_id"`
	Results  []messengerFaultResult `json:"results"`
}

type messengerFaultResult struct {
	Name     string  `json:"name"`
	Errno    int     `json:"errno"`
	Seconds  float64 `json:"seconds"`
	Verified bool    `json:"verified"`
}

func (run messengerFaultRun) checkWrites() error {
	if len(run.Results) == 0 {
		return fmt.Errorf("client.%d reported no write/readback results", run.ClientID)
	}
	for _, result := range run.Results {
		if result.Errno != 0 || !result.Verified {
			return fmt.Errorf("write/readback of %s: %+v", result.Name, result)
		}
	}
	return nil
}

// checkDelay uses the receiving OSD's queue decision for this phase's fresh
// client. The total write/readback time includes storage and scheduling.
func (run messengerFaultRun) checkDelay(lines []string, wantDelay bool) (int, error) {
	if err := run.checkWrites(); err != nil {
		return 0, err
	}
	if run.ClientID == 0 {
		return 0, fmt.Errorf("missing client instance ID")
	}
	operation := fmt.Sprintf("osd_op(client.%d.", run.ClientID)
	var count int
	for _, line := range lines {
		if _, decision, ok := strings.Cut(line, "queue_received will delay after "); ok && strings.Contains(decision, operation) {
			count++
		}
	}
	if (count > 0) != wantDelay {
		return count, fmt.Errorf("client.%d matched %d delayed OSD operations, want delay=%t", run.ClientID, count, wantDelay)
	}
	return count, nil
}
