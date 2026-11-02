package harness

import "example.com/images/lib"

// Only the fallback function, rather than the image reference itself, is
// replaceable by roles. The constant comes from a dependency's export data.
func IntegrationImage() string { return lib.Published }

func ExplicitImage() string { return lib.Published }
