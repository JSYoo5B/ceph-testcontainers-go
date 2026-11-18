package dashboard

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var errLoginUnsettled = errors.New("dashboard login has no definitive server completion; fixture ownership is retained; terminate the disposable cluster because stopping the client cannot establish server completion")

// trackProbe is called while the Dashboard lifetime lock is held. Concurrent
// observations may finish independently, so their settlement records have a
// separate mutex. A stopped HTTP client does not prove that its server handler
// has finished writing the Dashboard's multi-cluster configuration.
func (s *state) trackProbe(report probeReport, err error) {
	s.probesMu.Lock()
	defer s.probesMu.Unlock()
	s.loginUncertain = s.loginUncertain || report.LoginPending
	var command *ceph.NativeCommandUnstoppedError
	if errors.As(err, &command) {
		// An unconfirmed process takes precedence over any admission error
		// joined to its failure; it may already have sent the login.
		s.loginUncertain = true
		s.probeCommands = append(s.probeCommands, command)
	}
}

// settleProbes requires the exclusive lifetime lock, after admitted
// observations have returned. Stop remains retryable, but never substitutes
// for a missing server response.
func (s *state) settleProbes(ctx context.Context) error {
	var errs []error
	remaining := s.probeCommands[:0]
	for _, command := range s.probeCommands {
		if err := command.Stop(ctx); err != nil {
			remaining = append(remaining, command)
			errs = append(errs, err)
		}
	}
	s.probeCommands = remaining
	if s.loginUncertain {
		errs = append(errs, errLoginUnsettled)
	}
	return errors.Join(errs...)
}
