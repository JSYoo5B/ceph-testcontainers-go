//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_client_cohorts))))

//ci: timeout=50m job-timeout=60

package integration_test

import (
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CRC profile with a MemStore OSD, on a bridge network with AES keys and
// an AES256K service cipher, and on the host network with AES256K.
func TestMessengerClientProfileCRC(t *testing.T) {
	parallelWhenEnabled(t)
	for _, mode := range networkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) {
			if mode.Host {
				testMessengerClientCRCProfile(t, ceph.AuthConfig{KeyType: ceph.CipherAES256K,
					MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second}, ceph.WithHostNetwork())
				return
			}
			testMessengerClientCRCProfile(t, ceph.AuthConfig{KeyType: ceph.CipherAES, ServiceCipher: ceph.CipherAES256K,
				MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
		})
	}
}

// The secure profile on IPv6 and on IPv4-mapped IPv6 addresses.
func TestMessengerClientProfileAddressFamilies(t *testing.T) {
	parallelWhenEnabled(t)
	t.Run("ipv6", func(t *testing.T) {
		testMessengerClientAddressProfile(t, ceph.AddressIPv6)
	})
	t.Run("ipv4-mapped", func(t *testing.T) {
		testMessengerClientAddressProfile(t, ceph.AddressIPv4MappedIPv6)
	})
}

// The secure profile on official Tentacle 20.2.3 for every role, whose
// ceph-authtool has no --key-type.
func TestMessengerClientProfileWithoutKeyTypes(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientWithoutKeyTypes(t, tentacleWithoutKeyTypes)
}
