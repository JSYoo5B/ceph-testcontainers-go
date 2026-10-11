//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_auth_ciphers))))

//ci: timeout=25m job-timeout=35

package integration_test

import "testing"

// tentacleWithoutKeyTypes is the last Tentacle release whose ceph-authtool
// has no --key-type and whose monmaptool has no cipher options.
const tentacleWithoutKeyTypes = "quay.io/ceph/ceph:v20.2.3@sha256:d195020de02512030118e772cef7859e92904e91eb4cb21acb503f8b94118137"

func TestAuthCiphersAndTicketLifetimes(t *testing.T) {
	parallelWhenEnabled(t)
	image, imageOpts := integrationImages(t)
	testAuthCiphers(t, image, imageOpts)
}

// TestAuthCiphersWithoutKeyTypeOption runs the official Tentacle 20.2.3 image
// for every role, independently of the release under test.
func TestAuthCiphersWithoutKeyTypeOption(t *testing.T) {
	parallelWhenEnabled(t)
	testAuthCiphers(t, tentacleWithoutKeyTypes, nil)
}
