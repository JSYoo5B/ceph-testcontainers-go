package multicluster

import "errors"

// EveryBranch reports whether every branch of err ends in an error that leaf
// accepts. It follows Unwrap() error to the innermost error, and into each
// non-nil child of a joined error. A nil err, and a join without a non-nil
// child, have no branch and are not accepted.
func EveryBranch(err error, leaf func(error) bool) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			found = true
			if !EveryBranch(child, leaf) {
				return false
			}
		}
		return found
	}
	if inner := errors.Unwrap(err); inner != nil {
		return EveryBranch(inner, leaf)
	}
	return leaf(err)
}

// AnyNode reports whether match accepts err or an error that it wraps or
// joins, at any depth. Unlike errors.As it keeps looking after a node of the
// wanted type that match refuses.
func AnyNode(err error, match func(error) bool) bool {
	if err == nil {
		return false
	}
	if match(err) {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if AnyNode(child, match) {
				return true
			}
		}
		return false
	}
	return AnyNode(errors.Unwrap(err), match)
}
