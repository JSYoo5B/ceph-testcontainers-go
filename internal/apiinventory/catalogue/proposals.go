package catalogue

// Proposals are review proposals, not compatibility aliases or an API rewrite.
// Only catalogue rows change when a proposal is edited, and a row with decision
// "proposal" blocks a finished review. A proposal key is a package-qualified
// name or a package-qualified receiver type.
var Proposals = map[string]string{}
