// Package naming holds the name patterns the cluster and service packages
// accept for Ceph resources before passing them to a CLI.
package naming

import "regexp"

// PoolResource matches pool, RBD namespace and image names: letters, digits,
// underscores, dots or dashes, not starting with a dot or dash.
var PoolResource = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

// ClientID matches a Cephx client ID without its "client." prefix.
var ClientID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

// DaemonName matches a daemon or service instance name.
var DaemonName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// ExistingPool matches a native pool name that a fixture may reference but
// did not create, including Ceph's own dotted pools such as .mgr.
var ExistingPool = regexp.MustCompile(`^[A-Za-z0-9_.][A-Za-z0-9_.-]{0,127}$`)

// MirrorDaemonRule is what a mirror fixture's daemon names may contain
// besides ASCII letters, digits, underscores and hyphens. Each mirror package
// keeps the rule it published, so the rules differ only here.
type MirrorDaemonRule struct {
	// Dots admits '.' anywhere in the name.
	Dots bool
	// LeadingHyphen admits a name that starts with '-'.
	LeadingHyphen bool
}

// ValidMirrorDaemonName reports whether name has 1 to 63 ASCII letters,
// digits, underscores and hyphens, and the characters rule admits.
func ValidMirrorDaemonName(name string, rule MirrorDaemonRule) bool {
	if name == "" || len(name) > 63 || (name[0] == '-' && !rule.LeadingHyphen) {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		case c == '.' && rule.Dots:
		default:
			return false
		}
	}
	return true
}
