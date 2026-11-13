package expect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// The scopes of an rgw.UserUsage observation.
const (
	// UsageScopeUser: the counters aggregate the selected ordinary user, who
	// is its own owner.
	UsageScopeUser = "user"
	// UsageScopeAccount: the counters aggregate the account the selected user
	// belongs to, which owns the buckets of all its users and roles.
	UsageScopeAccount = "account"
)

// UserUsage states what a test expects of one rgw.UserUsage observation, read
// by rgw.User.Usage after the test's own stats synchronization.
//
// UserID is the selected user and is required. Tenant, when set, is the
// user's tenant. Scope is UsageScopeUser or UsageScopeAccount. OwnerID is the
// effective owner the counters aggregate: for UsageScopeUser it is the user
// itself and may be left empty; for UsageScopeAccount it is the account ID,
// which is required and differs from UserID. Distinct users of one account
// therefore state the same OwnerID with their own UserID.
//
// Objects and SizeBytes are always compared, so zero objects and zero bytes
// are expectations of their own. SizeBytes is the native logical size, the
// sum of object sizes; the rounded accounting size (SizeActualBytes) is not
// compared.
//
// A match proves only what that one native stats read reported. Native
// accounting is asynchronous: synchronize the stats or poll before
// evaluating, and verify quota enforcement and object content separately.
type UserUsage struct {
	UserID, Tenant, Scope, OwnerID string
	Objects, SizeBytes             uint64
}

// UserUsageMismatchReason names why an observation does not satisfy a
// UserUsage expectation.
type UserUsageMismatchReason string

// The reasons a UserUsageEvaluation reports.
const (
	// UserUsageMismatchUser: the observation is of another user.
	UserUsageMismatchUser UserUsageMismatchReason = "user"
	// UserUsageMismatchTenant: the user's tenant differs.
	UserUsageMismatchTenant UserUsageMismatchReason = "tenant"
	// UserUsageMismatchScope: the counters aggregate the user where an
	// account is expected, or the reverse.
	UserUsageMismatchScope UserUsageMismatchReason = "scope"
	// UserUsageMismatchOwner: the counters aggregate another owner.
	UserUsageMismatchOwner UserUsageMismatchReason = "owner"
	// UserUsageMismatchObjects: the object count differs.
	UserUsageMismatchObjects UserUsageMismatchReason = "objects"
	// UserUsageMismatchSize: the logical size in bytes differs.
	UserUsageMismatchSize UserUsageMismatchReason = "size"
)

// UserUsageMismatch is one reason an observation does not satisfy a
// UserUsage expectation. Want and Observed are the compared values as text.
type UserUsageMismatch struct {
	Reason         UserUsageMismatchReason
	Want, Observed string
}

// String names the reason and the evaluated values.
func (m UserUsageMismatch) String() string {
	return fmt.Sprintf("%s: want %s, observed %s", m.Reason, quoteEmpty(m.Want), quoteEmpty(m.Observed))
}

// UserUsageEvaluation is the result of evaluating one observation.
// Expectation and Observed are copies of what was evaluated; Expectation
// carries the resolved OwnerID. Mismatches are in the order of the reason
// constants, and every field is compared, so a wrong owner or scope is
// reported even when the counters match. Matched means there are none.
type UserUsageEvaluation struct {
	Expectation UserUsage
	Observed    rgw.UserUsage
	Mismatches  []UserUsageMismatch
	Matched     bool
}

// String summarizes the expectation and at most eight mismatches.
func (e UserUsageEvaluation) String() string {
	var b strings.Builder
	w := e.Expectation
	fmt.Fprintf(&b, "usage of user %s", w.UserID)
	if w.Tenant != "" {
		fmt.Fprintf(&b, " tenant=%s", w.Tenant)
	}
	fmt.Fprintf(&b, " want scope=%s owner=%s objects=%d size=%d: ", w.Scope, w.OwnerID, w.Objects, w.SizeBytes)
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == evaluationShown {
			fmt.Fprintf(&b, "; %d more", len(e.Mismatches)-i)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// Evaluate checks one rgw.UserUsage observation against the expectation. It
// reads only its arguments: no query, stats synchronization, wait or retry,
// so a test composes it with its own polling or assertion helpers. A mismatch
// is a result; an error means no verdict. An expectation without a UserID,
// with an unknown Scope, with an OwnerID other than UserID in the user scope,
// or without an OwnerID distinct from UserID in the account scope is
// ceph.ErrInvalidRequest. An observation without a user or owner, with an
// unknown scope, or whose owner contradicts its scope is a malformed
// observation, never a mismatch.
//
//purpose:observation
func (e UserUsage) Evaluate(observed rgw.UserUsage) (UserUsageEvaluation, error) {
	want, err := e.resolve()
	if err != nil {
		return UserUsageEvaluation{}, err
	}
	if err := validateUsageObservation(observed); err != nil {
		return UserUsageEvaluation{}, err
	}
	result := UserUsageEvaluation{Expectation: want, Observed: observed}
	result.Mismatches = want.mismatches(observed)
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e UserUsage) resolve() (UserUsage, error) {
	if e.UserID == "" {
		return UserUsage{}, fmt.Errorf("%w: usage expectation needs a user ID", ceph.ErrInvalidRequest)
	}
	switch e.Scope {
	case UsageScopeUser:
		if e.OwnerID == "" {
			e.OwnerID = e.UserID
		}
		if e.OwnerID != e.UserID {
			return UserUsage{}, fmt.Errorf("%w: user-scope usage expectation names owner %q other than its user", ceph.ErrInvalidRequest, e.OwnerID)
		}
	case UsageScopeAccount:
		if e.OwnerID == "" || e.OwnerID == e.UserID {
			return UserUsage{}, fmt.Errorf("%w: account-scope usage expectation needs an account ID other than its user", ceph.ErrInvalidRequest)
		}
	default:
		return UserUsage{}, fmt.Errorf("%w: usage expectation selects unknown scope %q", ceph.ErrInvalidRequest, e.Scope)
	}
	return e, nil
}

func validateUsageObservation(observed rgw.UserUsage) error {
	if observed.UserID == "" || observed.OwnerID == "" {
		return fmt.Errorf("malformed usage observation: no user or owner")
	}
	switch observed.Scope {
	case UsageScopeUser:
		if observed.OwnerID != observed.UserID {
			return fmt.Errorf("malformed usage observation: user scope owned by another identity")
		}
	case UsageScopeAccount:
		if observed.OwnerID == observed.UserID {
			return fmt.Errorf("malformed usage observation: account scope owned by the user itself")
		}
	default:
		return fmt.Errorf("malformed usage observation: unknown scope %q", observed.Scope)
	}
	return nil
}

func (e UserUsage) mismatches(observed rgw.UserUsage) []UserUsageMismatch {
	var result []UserUsageMismatch
	compare := func(reason UserUsageMismatchReason, want, got string) {
		if got != want {
			result = append(result, UserUsageMismatch{Reason: reason, Want: want, Observed: got})
		}
	}
	compare(UserUsageMismatchUser, e.UserID, observed.UserID)
	if e.Tenant != "" {
		compare(UserUsageMismatchTenant, e.Tenant, observed.Tenant)
	}
	compare(UserUsageMismatchScope, e.Scope, observed.Scope)
	compare(UserUsageMismatchOwner, e.OwnerID, observed.OwnerID)
	compare(UserUsageMismatchObjects, strconv.FormatUint(e.Objects, 10), strconv.FormatUint(observed.NumObjects, 10))
	compare(UserUsageMismatchSize, strconv.FormatUint(e.SizeBytes, 10), strconv.FormatUint(observed.SizeBytes, 10))
	return result
}

// evaluationShown bounds the mismatches that an evaluation's String lists.
const evaluationShown = 8

func quoteEmpty(s string) string {
	if s == "" {
		return `""`
	}
	return s
}
