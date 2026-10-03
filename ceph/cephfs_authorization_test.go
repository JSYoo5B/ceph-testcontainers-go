package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type cephFSAuthFixtureEntry struct {
	Entity string            `json:"entity"`
	Key    string            `json:"key"`
	Caps   map[string]string `json:"caps"`
}

type cephFSAuthFixtureContainer struct {
	*poolFixtureContainer
	auth       map[string]*cephFSAuthFixtureEntry
	grants     map[string]string
	copied     []byte
	applyFail  bool
	corruptCap bool
}

func (ctr *cephFSAuthFixtureContainer) CopyToContainer(_ context.Context, content []byte, _ string, _ int64) error {
	ctr.copied = bytes.Clone(content)
	return nil
}

func (ctr *cephFSAuthFixtureContainer) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	args := argv
	if argv[0] == "ceph" {
		args = argv[3:]
	}
	key := strings.Join(args, " ")
	custom := args[0] == "auth" || args[0] == "ceph-authtool" || args[0] == "rm" || (len(args) > 2 && args[0] == "fs" && args[1] == "subvolume" && slices.Contains([]string{"authorize", "deauthorize", "authorized_list", "evict"}, args[2]))
	if !custom {
		return ctr.poolFixtureContainer.Exec(ctx, argv, opts...)
	}
	ctr.calls = append(ctr.calls, slices.Clone(args))
	apply := key != ctr.fail || ctr.applyFail
	output := ""
	switch {
	case args[0] == "ceph-authtool":
		output = "PRIVATE-KEY\n"
	case args[0] == "auth" && args[1] == "ls":
		entries := make([]*cephFSAuthFixtureEntry, 0, len(ctr.auth))
		for _, entry := range ctr.auth {
			entries = append(entries, entry)
		}
		data, _ := json.Marshal(map[string]any{"auth_dump": entries})
		output = string(data)
	case args[0] == "auth" && args[1] == "add" && apply:
		caps := make(map[string]string)
		for i := 5; i+1 < len(args); i += 2 {
			caps[args[i]] = args[i+1]
		}
		ctr.auth[args[2]] = &cephFSAuthFixtureEntry{Entity: args[2], Key: clientKey(ctr.copied, args[2]), Caps: caps}
	case args[0] == "auth" && args[1] == "get":
		if entry := ctr.auth[args[2]]; entry != nil {
			output = "[" + args[2] + "]\nkey = " + entry.Key + "\n"
			if slices.Contains(args, "json") {
				data, _ := json.Marshal([]*cephFSAuthFixtureEntry{entry})
				output = string(data)
			}
		}
	case args[0] == "auth" && args[1] == "caps" && apply:
		entry := ctr.auth[args[2]]
		entry.Caps = make(map[string]string)
		for i := 3; i+1 < len(args); i += 2 {
			entry.Caps[args[i]] = args[i+1]
		}
	case args[0] == "auth" && args[1] == "del" && apply:
		delete(ctr.auth, args[2])
	case len(args) > 2 && args[2] == "authorized_list":
		entries := make([]map[string]string, 0, len(ctr.grants))
		for id, access := range ctr.grants {
			entries = append(entries, map[string]string{id: access})
		}
		data, _ := json.Marshal(entries)
		output = string(data)
	case len(args) > 2 && args[2] == "authorize" && apply:
		access := args[slices.Index(args, "--access_level")+1]
		entry := ctr.auth["client."+args[5]]
		entry.Caps["mds"] = "allow " + access + " path=/volumes/group/volume/unique-id"
		entry.Caps["osd"] = "allow " + access + " pool=additional namespace=fsvolumens_group_volume"
		if ctr.corruptCap {
			entry.Caps["osd"] = "allow *"
		}
		ctr.grants[args[5]] = access
	case len(args) > 2 && args[2] == "deauthorize" && apply:
		id := args[5]
		entry := ctr.auth["client."+id]
		if entry != nil {
			for _, access := range []string{"r", "rw"} {
				for service, clause := range map[string]string{
					"mds": "allow " + access + " path=/volumes/group/volume/unique-id",
					"osd": "allow " + access + " pool=additional namespace=fsvolumens_group_volume",
				} {
					tokens := authorizationCapTokens(entry.Caps[service])
					tokens = slices.DeleteFunc(tokens, func(token string) bool { return token == clause })
					if len(tokens) == 0 {
						delete(entry.Caps, service)
					} else {
						entry.Caps[service] = strings.Join(tokens, ", ")
					}
				}
			}
			if entry.Caps["mds"] == "" && entry.Caps["osd"] == "" && entry.Caps["mon"] == "allow r" {
				delete(entry.Caps, "mon")
			}
			if len(entry.Caps) == 0 {
				delete(ctr.auth, "client."+id)
			}
		}
		delete(ctr.grants, id)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	code := 0
	if key == ctr.fail {
		header[0], code, output = byte(stdcopy.Stderr), 1, "PRIVATE-KEY injected failure"
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}

func cephFSAuthorizationFixture(t *testing.T) (*CephFSContainer, *cephFSAuthFixtureContainer, *CephFSSubvolume) {
	t.Helper()
	fs, pool := subvolumeFixture()
	volume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	ctr := &cephFSAuthFixtureContainer{poolFixtureContainer: pool, auth: make(map[string]*cephFSAuthFixtureEntry), grants: make(map[string]string)}
	fs.cluster.Container = ctr
	fs.cluster.config = []byte("[global]\nmon_host = fixture\n")
	ctr.calls = nil
	return fs, ctr, volume
}

func TestCephFSSubvolumeAuthorizationFreshKeyAndCopiedRevocation(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err != nil {
		t.Fatal(err)
	}
	entry := ctr.auth["client.writer"]
	if entry.Key != "PRIVATE-KEY" || entry.Caps["mon"] != "allow r fsname=fixture" || !grant.identity.authorized {
		t.Fatal("grant did not preserve a fresh scoped principal")
	}
	if _, _, err := grant.Client.ConnectionConfig(); err != nil {
		t.Fatal("confirmed principal has no credentials", err)
	}
	listing, err := fs.SubvolumeAuthorizedClients(t.Context(), volume.Name, volume.GroupName)
	if err != nil || !slices.Equal(listing, []CephFSSubvolumeAuthorizedClient{{AuthID: "writer", Access: "rw"}}) {
		t.Fatal("native authorized_list shape was not decoded", listing, err)
	}
	if err := fs.RemoveSubvolume(t.Context(), volume); err == nil {
		t.Fatal("subvolume removed before authorization cleanup")
	}
	if err := fs.EvictSubvolumeClients(t.Context(), grant); err == nil {
		t.Fatal("active grant allowed premature eviction")
	}
	// Native deauthorization must preserve unrelated service and path/pool caps.
	entry.Caps["mgr"] = "allow r"
	entry.Caps["mds"] += ", allow r path=/unrelated"
	entry.Caps["osd"] += ", allow r pool=unrelated namespace=other"
	copy := *grant
	copy.Client, copy.AuthID, copy.Access, copy.SubvolumeName, copy.GroupName, copy.Path = nil, "foreign", "r", "foreign", "foreign", "/foreign"
	if err := fs.DeauthorizeSubvolume(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"mon": "allow r fsname=fixture", "mgr": "allow r", "mds": "allow r path=/unrelated", "osd": "allow r pool=unrelated namespace=other"}
	if entry.Key != "PRIVATE-KEY" || !equalAuthorizationCaps(entry.Caps, want) {
		t.Fatal("deauthorization deleted key or unrelated grants", entry.Caps)
	}
	before := len(ctr.calls)
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil {
		t.Fatal("copied revoke state was not shared", err)
	}
	for _, call := range ctr.calls[before:] {
		if slices.Contains(call, "deauthorize") {
			t.Fatal("deauthorization repeated")
		}
	}
	if err := fs.EvictSubvolumeClients(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ctr.calls[len(ctr.calls)-1], []string{"fs", "subvolume", "evict", "fixture", "volume", "writer", "--group_name", "group"}) {
		t.Fatal("copied fields redirected eviction")
	}
	if err := fs.RemoveSubvolume(t.Context(), volume); err != nil {
		t.Fatal("deauthorized volume could not be removed", err)
	}
}

func TestCephFSSubvolumeAuthorizationRefusesExistingAndUnisolatedPrincipals(t *testing.T) {
	for _, test := range []string{"existing", "namespace"} {
		t.Run(test, func(t *testing.T) {
			fs, ctr, volume := cephFSAuthorizationFixture(t)
			if test == "existing" {
				ctr.auth["client.writer"] = &cephFSAuthFixtureEntry{Entity: "client.writer", Key: "EXTERNAL-KEY", Caps: map[string]string{"mon": "allow *"}}
			} else {
				key := "fs subvolume info fixture volume --group_name group --format json"
				ctr.output[key] = strings.ReplaceAll(ctr.output[key], `"pool_namespace":"fsvolumens_group_volume"`, `"pool_namespace":""`)
			}
			if grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"}); err == nil || grant != nil {
				t.Fatal("existing principal or pool-wide layout was adopted")
			}
			for _, call := range ctr.calls {
				if slices.Contains(call, "add") || slices.Contains(call, "authorize") || slices.Contains(call, "del") {
					t.Fatal("unsafe preflight issued auth mutation", call)
				}
			}
		})
	}
}

func TestCephFSSubvolumeAuthorizationPartialReplyAndDeauthorizeRetry(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	ctr.fail, ctr.applyFail = "fs subvolume authorize fixture volume writer --group_name group --access_level rw --allow_existing_id", true
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err == nil || grant == nil || grant.identity.authorized || !grant.identity.authorizeAttempted || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatal("uncertain grant was adopted or leaked a key")
	}
	if err := fs.RemoveSubvolume(t.Context(), volume); err == nil {
		t.Fatal("partial grant was discarded during volume cleanup")
	}
	ctr.fail = "fs subvolume deauthorize fixture volume writer --group_name group"
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil || !grant.identity.deauthAttempted {
		t.Fatal("lost revoke reply did not retain retry state")
	}
	ctr.fail, ctr.calls = "", nil
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil || !grant.identity.deauthorized {
		t.Fatal("uncertain successful revocation did not reconcile", err)
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "deauthorize") {
			t.Fatal("already applied revoke was sent again")
		}
	}
}

func TestCephFSSubvolumeAuthorizationOutsideIdentityEditsRefuseMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*CephFSContainer, *cephFSAuthFixtureContainer)
	}{
		{"key", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) { c.auth["client.writer"].Key = "REPLACED" }},
		{"namespace", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) {
			key := "fs subvolume info fixture volume --group_name group --format json"
			c.output[key] = strings.ReplaceAll(c.output[key], "fsvolumens_group_volume", "replacement")
		}},
		{"UUID", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) {
			key := "fs subvolume info fixture volume --group_name group --format json"
			c.output[key] = strings.ReplaceAll(c.output[key], "unique-id", "replacement")
		}},
		{"pool ID", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) {
			key := "osd pool ls detail --format json"
			c.output[key] = strings.ReplaceAll(c.output[key], `"pool_id":3`, `"pool_id":4`)
		}},
		{"metadata access", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) { c.grants["writer"] = "r" }},
		{"caps", func(_ *CephFSContainer, c *cephFSAuthFixtureContainer) {
			c.auth["client.writer"].Caps["osd"] = "allow *"
		}},
		{"closed", func(fs *CephFSContainer, _ *cephFSAuthFixtureContainer) { fs.cluster.closed = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, c, volume := cephFSAuthorizationFixture(t)
			grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
			if err != nil {
				t.Fatal(err)
			}
			test.edit(fs, c)
			c.calls = nil
			before := maps.Clone(c.auth["client.writer"].Caps)
			if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil {
				t.Fatal("outside replacement allowed revoke")
			}
			if !equalAuthorizationCaps(before, c.auth["client.writer"].Caps) {
				t.Fatal("guard changed outside capabilities")
			}
			for _, call := range c.calls {
				if slices.Contains(call, "deauthorize") || slices.Contains(call, "evict") || slices.Contains(call, "del") {
					t.Fatal("outside edit reached mutation", call)
				}
			}
		})
	}
}

func TestCephFSSubvolumeAuthorizationReadbackMismatchIsNotConfirmed(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	ctr.corruptCap = true
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err == nil || grant == nil || grant.identity.authorized {
		t.Fatal("native pool-wide capability was accepted")
	}
	if err := fs.EvictSubvolumeClients(t.Context(), grant); err == nil {
		t.Fatal("partial grant authorized eviction")
	}
	before := len(ctr.calls)
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil {
		t.Fatal("unpaired partial native caps allowed deauthorization")
	}
	for _, call := range ctr.calls[before:] {
		if slices.Contains(call, "deauthorize") {
			t.Fatal("unsafe native deny_access was sent")
		}
	}
}

func TestCephFSSubvolumeAuthorizationLostReplyPreservesLaterOutsideCaps(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fail, ctr.applyFail = "fs subvolume deauthorize fixture volume writer --group_name group", true
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil {
		t.Fatal("lost reply was not reported")
	}
	ctr.fail, ctr.calls = "", nil
	ctr.auth["client.writer"].Caps["mgr"] = "allow r"
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil {
		t.Fatal("outside state change after lost reply was silently accepted")
	}
	if ctr.auth["client.writer"].Caps["mgr"] != "allow r" {
		t.Fatal("recovery overwrote outside rights")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "deauthorize") || slices.Contains(call, "caps") {
			t.Fatal("uncertain recovery rewrote changed outside state", call)
		}
	}
}

func TestCephFSSubvolumeAuthorizationDecoderAndCanceledValidation(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `[{}]`, `[{"writer":"r","reader":"r"}]`, `[{"writer":"r"},{"writer":"rw"}]`, `[{"--unsafe":"r"}]`, `[{"writer":"*"}]`} {
		if _, err := decodeSubvolumeAuthorizedClients([]byte(data)); err == nil {
			t.Fatal("invalid authorized_list accepted", data)
		}
	}
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if grant, err := fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"}); err == nil || grant != nil {
		t.Fatal("canceled context created a principal")
	}
	if len(ctr.calls) != 0 {
		t.Fatal("canceled operation reached native commands")
	}
	for _, access := range []string{"", "w", "rws"} {
		if _, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: access}); err == nil {
			t.Fatal("invalid access accepted", access)
		}
	}
	if err := fs.DeauthorizeSubvolume(t.Context(), &CephFSSubvolumeAuthorization{}); err == nil {
		t.Fatal("external descriptor allowed deauthorization")
	}
}

func TestCephFSSubvolumeAuthorizationNativeRawIDKeepsClientPrefix(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "client.client.writer", Access: "r"})
	if err != nil || grant.AuthID != "client.writer" || grant.Client.Name() != "client.client.writer" {
		t.Fatal("native raw auth ID was normalized as a qualified entity", grant, err)
	}
	listing, err := fs.SubvolumeAuthorizedClients(t.Context(), volume.Name, volume.GroupName)
	if err != nil || len(listing) != 1 || listing[0].AuthID != "client.writer" {
		t.Fatal("native raw auth ID prefix was discarded", listing, err)
	}
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	if ctr.auth["client.client.writer"] == nil || ctr.auth["client.writer"] != nil {
		t.Fatal("raw ID revocation selected the wrong principal")
	}
}

func TestCephFSSubvolumeAuthorizationPartialPrincipalDoesNotGrantOrEraseData(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	ctr.fail = "auth get client.writer"
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err == nil || grant == nil || grant.Client == nil || !grant.Client.created || grant.Client.ready || grant.identity.authorizeAttempted || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatal("unconfirmed principal was granted or leaked a key")
	}
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil {
		t.Fatal("unconfirmed principal was adopted for native grant cleanup")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "authorize") || slices.Contains(call, "deauthorize") || slices.Contains(call, "del") || slices.Contains(call, "rm") && call[0] == "fs" {
			t.Fatal("partial principal automatically changed grants/data", call)
		}
	}
	if len(volume.identity.authorizations) != 0 {
		t.Fatal("unattempted grant permanently blocked volume cleanup")
	}
}

func TestCephFSSubvolumeEvictionRetryAndReplacementKeyRefusal(t *testing.T) {
	fs, ctr, volume := cephFSAuthorizationFixture(t)
	grant, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	ctr.fail = "fs subvolume evict fixture volume writer --group_name group"
	if err := fs.EvictSubvolumeClients(t.Context(), grant); err == nil || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatal("uncertain native eviction was accepted or leaked auth output")
	}
	ctr.fail, ctr.calls = "", nil
	if err := fs.EvictSubvolumeClients(t.Context(), grant); err != nil {
		t.Fatal("same scoped native eviction could not be retried", err)
	}
	ctr.auth["client.writer"].Key, ctr.calls = "REPLACEMENT", nil
	if err := fs.EvictSubvolumeClients(t.Context(), grant); err == nil {
		t.Fatal("replacement principal allowed owned-session eviction")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "evict") {
			t.Fatal("replacement principal reached eviction")
		}
	}
}
