package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRBDMirrorScopeAndNamespaceDefaults(t *testing.T) {
	for _, tc := range []struct {
		config RBDMirrorConfig
		scope  RBDMirrorScope
		mode   RBDMirrorMode
	}{
		{RBDMirrorConfig{Pool: "images"}, RBDMirrorScopeImage, RBDMirrorModeSnapshot},
		{RBDMirrorConfig{Pool: "images", Scope: RBDMirrorScopePool}, RBDMirrorScopePool, RBDMirrorModeJournal},
		{RBDMirrorConfig{Pool: "images", Scope: RBDMirrorScopeImage, Mode: RBDMirrorModeJournal}, RBDMirrorScopeImage, RBDMirrorModeJournal},
		{RBDMirrorConfig{Pool: "images", Scope: RBDMirrorScopePool, SourceNamespace: "ns-a"}, RBDMirrorScopePool, RBDMirrorModeJournal},
		{RBDMirrorConfig{Pool: "images", DestinationNamespace: "ns-b"}, RBDMirrorScopeImage, RBDMirrorModeSnapshot},
	} {
		got, err := normalizeRBDMirrorConfig(tc.config)
		if err != nil || got.Scope != tc.scope || got.Mode != tc.mode || got.SourceNamespace != tc.config.SourceNamespace || got.DestinationNamespace != tc.config.DestinationNamespace {
			t.Fatalf("normalization changed explicit default namespace semantics: got=%+v error=%v", got, err)
		}
	}
	for _, scope := range []RBDMirrorScope{"namespace", " pool", "pool ", "--force"} {
		if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "images", Scope: scope}); err == nil {
			t.Errorf("invalid scope %q accepted", scope)
		}
	}
	if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "images", Scope: RBDMirrorScopePool, Mode: RBDMirrorModeSnapshot}); err == nil {
		t.Fatal("pool scope accepted snapshot mode")
	}
	for _, namespace := range []string{"--force", ".hidden", "n/s", "n@s", " ns", "ns ", "n\x00", strings.Repeat("n", 129)} {
		for _, source := range []bool{true, false} {
			config := RBDMirrorConfig{Pool: "images"}
			if source {
				config.SourceNamespace = namespace
			} else {
				config.DestinationNamespace = namespace
			}
			if _, err := normalizeRBDMirrorConfig(config); err == nil {
				t.Errorf("invalid namespace %q accepted", namespace)
			}
		}
	}
}

func TestRBDMirrorNamespacePolicyPlannerPreservesDefaultAndExistingScope(t *testing.T) {
	disabled := nativeRBDMirrorPolicy{Mode: "disabled"}
	remote := "ns-b"
	for _, mode := range []string{"disabled", "init-only", "image", "pool"} {
		base := disabled
		if mode != "disabled" {
			base = nativeRBDMirrorPolicy{Mode: mode, MirrorUUID: "base-uuid", RemoteNamespace: stringPointer(""), SiteName: "source"}
		}
		steps, err := planRBDMirrorSite(nil, "ns-a", remote, "source", RBDMirrorScopePool, base, disabled)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if mode == "disabled" {
			want = 2
			if steps[0].mode != "init-only" || steps[0].namespace != "" {
				t.Fatal("disabled default namespace was not prepared as init-only")
			}
		}
		if len(steps) != want || steps[len(steps)-1].namespace != "ns-a" || steps[len(steps)-1].remote != remote || steps[len(steps)-1].mode != "pool" {
			t.Fatalf("default mode %q was unnecessarily changed: %+v", mode, steps)
		}
	}
	base := nativeRBDMirrorPolicy{Mode: "init-only", MirrorUUID: "base-uuid", RemoteNamespace: stringPointer(""), SiteName: "source"}
	selected := nativeRBDMirrorPolicy{Mode: "pool", MirrorUUID: "namespace-uuid", RemoteNamespace: &remote}
	steps, err := planRBDMirrorSite(nil, "ns-a", remote, "source", RBDMirrorScopePool, base, selected)
	if err != nil || len(steps) != 0 {
		t.Fatalf("matching policy must be unchanged: %+v %v", steps, err)
	}
	for _, tc := range []struct{ mode, remote, site string }{{"image", "ns-b", "source"}, {"pool", "other", "source"}, {"pool", "ns-b", "other-site"}} {
		selected.Mode, selected.RemoteNamespace, base.SiteName = tc.mode, stringPointer(tc.remote), tc.site
		if _, err := planRBDMirrorSite(nil, "ns-a", "ns-b", "source", RBDMirrorScopePool, base, selected); err == nil {
			t.Fatalf("existing scope/mapping/site was implicitly reconfigured: %+v", tc)
		}
	}
}

func TestRBDMirrorNamespaceProvisionUsesExplicitEmptyRemote(t *testing.T) {
	source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
	link := &RBDMirror{sourceClient: source, destinationClient: destination, config: RBDMirrorConfig{
		Pool: "images", Scope: RBDMirrorScopePool, SourceNamespace: "ns-a", SourceSite: "source", DestinationSite: "destination",
	}}
	if err := link.provisionRBDMirrorPolicies(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(source.mutations) != 2 || len(destination.mutations) != 1 {
		t.Fatalf("unexpected scope mutations: source=%v destination=%v", source.mutations, destination.mutations)
	}
	if !slices.Equal(source.mutations[0], []string{"rbd", "mirror", "pool", "enable", "--site-name", "source", "images", "init-only"}) || !slices.Equal(source.mutations[1], []string{"rbd", "mirror", "pool", "enable", "--site-name", "source", "images/ns-a", "pool", "--remote-namespace", ""}) || !slices.Equal(destination.mutations[0], []string{"rbd", "mirror", "pool", "enable", "--site-name", "destination", "images", "pool", "--remote-namespace", "ns-a"}) {
		t.Fatalf("default namespace was treated as an omitted mapping: source=%v destination=%v", source.mutations, destination.mutations)
	}
	// Repeating composition preserves matching policy, UUIDs and enrollment.
	if err := link.provisionRBDMirrorPolicies(t.Context()); err != nil || len(source.mutations) != 2 || len(destination.mutations) != 1 {
		t.Fatalf("matching policies were rewritten: error=%v", err)
	}
}

func TestRBDMirrorNamespaceProvisionRejectsBothEndpointsBeforeMutation(t *testing.T) {
	for _, scenario := range []string{"destination scope", "destination mapping", "site on another pool", "outside edit", "bad readback"} {
		t.Run(scenario, func(t *testing.T) {
			source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
			link := &RBDMirror{sourceClient: source, destinationClient: destination, config: RBDMirrorConfig{
				Pool: "images", Scope: RBDMirrorScopePool, SourceNamespace: "ns-a", DestinationNamespace: "ns-b", SourceSite: "source", DestinationSite: "destination",
			}}
			switch scenario {
			case "destination scope":
				destination.policies["images/ns-b"] = nativeRBDMirrorPolicy{Mode: "image", MirrorUUID: "uuid", RemoteNamespace: stringPointer("ns-a")}
			case "destination mapping":
				destination.policies["images/ns-b"] = nativeRBDMirrorPolicy{Mode: "pool", MirrorUUID: "uuid", RemoteNamespace: stringPointer("other")}
			case "site on another pool":
				destination.site = "existing-other-site"
			case "outside edit":
				source.beforeInfo = func(c *rbdNamespacePolicyFixture, spec string, count int) {
					if spec == "images" && count == 2 {
						c.policies[spec] = nativeRBDMirrorPolicy{Mode: "image", MirrorUUID: "outside-uuid", RemoteNamespace: stringPointer(""), SiteName: "source"}
					}
				}
			case "bad readback":
				source.falseSuccess = true
			}
			if err := link.provisionRBDMirrorPolicies(t.Context()); err == nil {
				t.Fatal("incompatible or changed native policy accepted")
			}
			wantSource := 0
			if scenario == "bad readback" {
				wantSource = 1
			}
			if len(source.mutations) != wantSource || len(destination.mutations) != 0 {
				t.Fatalf("preflight/guard failed to contain mutation: source=%v destination=%v", source.mutations, destination.mutations)
			}
		})
	}
}

func TestRBDMirrorNamespacePartialSetupCanBeReadAfterLostReply(t *testing.T) {
	source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
	source.lostReply = true
	link := &RBDMirror{sourceClient: source, destinationClient: destination, config: RBDMirrorConfig{Pool: "images", Scope: RBDMirrorScopePool, SourceSite: "source", DestinationSite: "destination"}}
	if err := link.provisionRBDMirrorPolicies(t.Context()); err == nil {
		t.Fatal("lost native reply unexpectedly succeeded")
	}
	status, err := link.PolicyStatus(t.Context())
	if err != nil || status.Source.Mode != "pool" || status.Source.RemoteNamespace != "" || status.Source.MirrorUUID == "" || status.Destination.Mode != "disabled" {
		t.Fatalf("partial native state was hidden or rolled back: %+v %v", status, err)
	}
	source.lostReply = false
	if err := link.provisionRBDMirrorPolicies(t.Context()); err != nil || len(source.mutations) != 1 || len(destination.mutations) != 1 {
		t.Fatalf("reconciliation rewrote completed native policy: source=%v destination=%v error=%v", source.mutations, destination.mutations, err)
	}
}

func TestRBDMirrorNamespaceReadRequiresNativeModeUUIDAndRemote(t *testing.T) {
	for _, data := range []string{`{`, `{}`, `{"mode":"unexpected"}`, `{"mode":"pool","mirror_uuid":"uuid"}`, `{"mode":"image","remote_namespace":""}`} {
		client := newRBDNamespacePolicyFixture()
		client.infoOverride = data
		if _, err := readRBDMirrorNamespacePolicy(t.Context(), client, "images", "ns-a"); err == nil {
			t.Errorf("incomplete native policy accepted: %s", data)
		}
	}
	client := newRBDNamespacePolicyFixture()
	client.infoOverride = `{"mode":"init-only","mirror_uuid":"uuid","remote_namespace":""}`
	if _, err := readRBDMirrorNamespacePolicy(t.Context(), client, "images", "ns-a"); err == nil {
		t.Fatal("named init-only policy accepted")
	}
}

func TestRBDMirrorNamespaceImageEnrollmentAndPoolIdentity(t *testing.T) {
	client := &rbdEnableFixture{info: `{"features":["exclusive-lock"]}`}
	link := &RBDMirror{sourceClient: client, config: RBDMirrorConfig{Pool: "images", SourceNamespace: "ns-a", Mode: RBDMirrorModeSnapshot}}
	if err := link.EnableImage(t.Context(), "volume"); err != nil || len(client.calls) != 2 || client.calls[0][2] != "images/ns-a/volume" || client.calls[1][4] != "images/ns-a/volume" {
		t.Fatalf("enrollment lost source namespace: %v error=%v", client.calls, err)
	}
	client.calls = nil
	link.config.Scope = RBDMirrorScopePool
	if err := link.EnableImage(t.Context(), "volume"); err == nil || len(client.calls) != 0 {
		t.Fatal("pool enrollment should require journal features, without per-image enable")
	}
	link.closed = true
	if err := link.EnableImage(t.Context(), "volume"); err == nil || len(client.calls) != 0 {
		t.Fatal("terminated fixture performed enrollment")
	}
	if _, err := link.PolicyStatus(t.Context()); err == nil {
		t.Fatal("terminated fixture reported policy")
	}
	source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
	link = &RBDMirror{config: RBDMirrorConfig{Pool: "images", Source: &ceph.Container{Container: source}, Destination: &ceph.Container{Container: destination}}, poolIdentities: &rbdMirrorPoolIdentities{source: 42, destination: 42}}
	if err := link.checkRBDMirrorPools(t.Context()); err != nil {
		t.Fatal(err)
	}
	destination.poolID = 43
	if err := link.checkRBDMirrorPools(t.Context()); err == nil {
		t.Fatal("same-name replacement pool was adopted")
	}
}

func TestRBDMirrorNativePolicyIdentityGuardsLaterMutations(t *testing.T) {
	for _, scenario := range []string{"source base UUID", "source namespace UUID", "destination namespace UUID", "namespace scope", "namespace mapping", "default mapping", "site identity"} {
		t.Run(scenario, func(t *testing.T) {
			source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
			link := &RBDMirror{sourceClient: source, destinationClient: destination, config: RBDMirrorConfig{
				Pool: "images", Scope: RBDMirrorScopeImage, SourceNamespace: "ns-a", DestinationNamespace: "ns-b", SourceSite: "source", DestinationSite: "destination",
				Source: &ceph.Container{Container: source}, Destination: &ceph.Container{Container: destination},
			}}
			if err := link.provisionRBDMirrorPolicies(t.Context()); err != nil {
				t.Fatal(err)
			}
			captured := link.policyIdentities
			client, spec := source, "images/ns-a"
			if scenario == "source base UUID" || scenario == "default mapping" {
				spec = "images"
			}
			if scenario == "destination namespace UUID" {
				client, spec = destination, "images/ns-b"
			}
			policy := client.policies[spec]
			switch scenario {
			case "source base UUID", "source namespace UUID", "destination namespace UUID":
				policy.MirrorUUID = "same-name-native-replacement"
			case "namespace scope":
				policy.Mode = "pool"
			case "namespace mapping", "default mapping":
				policy.RemoteNamespace = stringPointer("outside-mapping")
			case "site identity":
				source.site = "outside-site"
			}
			client.policies[spec] = policy
			for _, operation := range []struct {
				name string
				call func() error
			}{
				{"EnableImage", func() error { return link.EnableImage(t.Context(), "volume") }},
				{"Rebootstrap", func() error { return link.Rebootstrap(t.Context()) }},
				{"AddDaemon", func() error { _, err := link.AddDaemon(t.Context(), "new"); return err }},
			} {
				if err := operation.call(); err == nil || !strings.Contains(err.Error(), "identity changed") {
					t.Fatalf("%s failed to guard %s before mutation: %v", operation.name, scenario, err)
				}
			}
			status, err := link.PolicyStatus(t.Context())
			if err != nil || status.Source.MirrorUUID == "" || link.policyIdentities != captured || len(source.mutations) != 2 || len(destination.mutations) != 2 {
				t.Fatalf("read-only status adopted replacement policy or hid native state: %+v error=%v", status, err)
			}
			if err := link.checkRBDMirrorPolicyIdentities(t.Context()); err == nil {
				t.Fatal("PolicyStatus adopted an outside policy change")
			}
		})
	}
}

func stringPointer(value string) *string { return &value }

type rbdNamespacePolicyFixture struct {
	testcontainers.Container
	policies     map[string]nativeRBDMirrorPolicy
	infoCounts   map[string]int
	mutations    [][]string
	site         string
	poolID       int64
	infoOverride string
	falseSuccess bool
	lostReply    bool
	beforeInfo   func(*rbdNamespacePolicyFixture, string, int)
}

func newRBDNamespacePolicyFixture() *rbdNamespacePolicyFixture {
	return &rbdNamespacePolicyFixture{policies: make(map[string]nativeRBDMirrorPolicy), infoCounts: make(map[string]int), poolID: 42}
}

func (c *rbdNamespacePolicyFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	var data []byte
	switch {
	case slices.Equal(args, []string{"ceph", "config-key", "ls", "--format", "json"}):
		data = []byte(`[]`)
		if c.site != "" {
			data = []byte(`["rbd/mirror/site_name"]`)
		}
	case slices.Equal(args, []string{"ceph", "config-key", "get", "rbd/mirror/site_name"}):
		data = []byte(c.site)
	case len(args) > 5 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}):
		spec := args[4]
		c.infoCounts[spec]++
		if c.beforeInfo != nil {
			c.beforeInfo(c, spec, c.infoCounts[spec])
		}
		policy := c.policies[spec]
		if policy.Mode == "" {
			policy.Mode = "disabled"
		}
		if policy.Mode != "disabled" && !strings.Contains(spec, "/") {
			policy.SiteName = c.site
		}
		data, _ = json.Marshal(policy)
		if c.infoOverride != "" {
			data = []byte(c.infoOverride)
		}
	case len(args) >= 8 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "enable"}):
		c.mutations = append(c.mutations, slices.Clone(args))
		if !c.falseSuccess {
			c.site = args[5]
			remote := ""
			if len(args) == 10 {
				remote = args[9]
			}
			c.policies[args[6]] = nativeRBDMirrorPolicy{Mode: args[7], MirrorUUID: "native-" + args[6], RemoteNamespace: &remote}
		}
		if c.lostReply {
			return 0, nil, errors.New("lost native mutation reply")
		}
	case slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"}):
		data = fmt.Appendf(nil, `[{"pool_id":%d,"pool_name":"images","type":1,"size":2,"min_size":1,"pg_num":8}]`, c.poolID)
	default:
		return 0, nil, fmt.Errorf("unexpected command %v", args)
	}
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.Write(data)
	return 0, bytes.NewReader(stream.Bytes()), nil
}
