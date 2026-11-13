//go:build all || integration

package rgw_test

import (
	"errors"
	"fmt"
	"strings"
)

// pollTimingQualifiedRelease is the one radosgw release that the incremental
// startup timing profile is qualified for.
const pollTimingQualifiedRelease = "20.2.4"

// pollTimingProfileOutcome judges what Run answered for the incremental
// startup timing profile. gatewayRelease is what the radosgw binary of the
// selected gateway image reports: the profile is gated on that binary, so
// neither the cluster's release nor an image name takes part. On the
// qualified release Run must succeed. On any other release Run must refuse,
// naming the profile's qualification and the release it observed, which must
// be the gateway's; refused is then true and the caller ends the test as
// passed. Every other answer is a problem: a refusal of the qualified
// release, a refusal that names another release than the gateway's, another
// error, and above all a success on a release the profile is not qualified
// for.
func pollTimingProfileOutcome(gatewayRelease string, err error) (refused bool, problem error) {
	if gatewayRelease == "" {
		return false, errors.New("the gateway's radosgw release was not observed")
	}
	if gatewayRelease == pollTimingQualifiedRelease {
		if err != nil {
			return false, fmt.Errorf("startup timing profile on the qualified radosgw %s: %w", gatewayRelease, err)
		}
		return false, nil
	}
	if err == nil {
		return false, fmt.Errorf("Run accepted the startup timing profile on radosgw %s, which it is not qualified for", gatewayRelease)
	}
	if !strings.Contains(err.Error(), "qualified only") || !strings.Contains(err.Error(), "(observed "+gatewayRelease+")") {
		return false, fmt.Errorf("startup timing profile on the unqualified radosgw %s failed otherwise than by its refusal: %w", gatewayRelease, err)
	}
	return true, nil
}
