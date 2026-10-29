package ceph

import (
	"context"
	"errors"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// temporaryRoot matches the test directory that runEmbeddedScript redirects
// container paths into.
var temporaryRoot = regexp.MustCompile(`/\S*?/\d{3}/`)

var updateAddressFamilyGolden = flag.Bool("update-address-family-golden", false, "rewrite testdata/address-family golden files")

// TestAddressFamilyBootstrapRenderingGolden runs mon.sh and mon-join.sh for
// every address family, Messenger mode and bridge layout, and compares the
// generated ceph.conf and native monmap admissions with golden files. The
// IPv4, IPv6 and IPv4-mapped files were recorded before dual-stack support
// and must stay byte-identical.
func TestAddressFamilyBootstrapRenderingGolden(t *testing.T) {
	subnets := map[AddressFamily][2]string{
		AddressIPv4:      {"10.10.0.0/24", "10.20.0.0/24"},
		AddressIPv6:      {"fd00:10::/64", "fd00:20::/64"},
		AddressDualStack: {"10.10.0.0/24,fd00:10::/64", "10.20.0.0/24,fd00:20::/64"},
	}
	modes := map[string]MessengerMode{"default": MessengerDefault, "v2only": MessengerV2Only, "secure": MessengerV2Secure}
	for _, family := range []AddressFamily{AddressIPv4, AddressIPv6, AddressIPv4MappedIPv6, AddressDualStack} {
		for modeName, mode := range modes {
			for _, layout := range []string{"single", "separate"} {
				name := family.String() + "-" + modeName + "-" + layout
				t.Run(name, func(t *testing.T) {
					settings := options{addressFamily: family, messengerMode: mode, separateClusterNetwork: layout == "separate",
						blockSize: 1 << 30, poolReplicas: 2, poolMinSize: 1}
					var public, cluster string
					switch {
					case layout == "separate":
						pair, ok := subnets[family]
						if !ok {
							// A cancelled context prevents Docker allocation even if the
							// admission guard regresses. Require the specific invalid request,
							// rather than treating cancellation as proof of configuration refusal.
							ctx, cancel := context.WithCancel(t.Context())
							cancel()
							refused, err := Run(ctx, RunOptions{}.WithOptions(WithAddressFamily(family), WithSeparateClusterNetwork(), WithMessengerMode(mode)))
							if refused != nil || !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "cannot be combined with WithSeparateClusterNetwork") {
								t.Fatalf("unsupported address-family/layout admission: container=%v error=%v", refused != nil, err)
							}
							return
						}
						public, cluster = pair[0], pair[1]
					case family == AddressDualStack:
						// A single dual-stack bridge serves both planes.
						public = subnets[family][0]
						cluster = public
					}
					c := &Container{settings: settings, nets: networkState{publicSubnet: public, clusterSubnet: cluster}}
					env := c.monitorEnvironment("fixture-fsid")
					maps.Copy(env, settings.messengerEnvironment())
					env["TC_HOSTNAME_I"] = "192.0.2.12 fd00:10::12 fe80::12"
					config, calls := messengerRunScript(t, "mon", env, nil)
					joinEnv := c.monitorJoinEnvironment("b")
					joinEnv["TC_HOSTNAME_I"] = "192.0.2.14 fd00:10::14"
					maps.Copy(joinEnv, map[string]string{"CEPH_FSID": "fixture-fsid"})
					joined, joinCalls := messengerRunScript(t, "mon-join", joinEnv, []byte(config))
					if joined != config {
						t.Fatal("late monitor replaced its inherited configuration")
					}
					got := "## ceph.conf\n" + config + "## mon.sh native commands\n" + strings.Join(calls, "\n") +
						"\n## mon-join.sh native commands\n" + strings.Join(joinCalls, "\n") + "\n"
					got = temporaryRoot.ReplaceAllString(got, "<root>/")
					path := filepath.Join("testdata", "address-family", name+".golden")
					if *updateAddressFamilyGolden {
						if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
							t.Fatal(err)
						}
					}
					want, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if got != string(want) {
						t.Fatalf("rendered bootstrap differs from %s:\n%s", path, got)
					}
				})
			}
		}
	}
}
