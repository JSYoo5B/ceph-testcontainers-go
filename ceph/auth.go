package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// ClientCaps contains Cephx capability expressions, passed as individual CLI
// arguments. For example, Mon: "allow r" and OSD: "allow rw pool=data
// namespace=tenant-a" restrict RADOS I/O to one pool and namespace. Empty fields
// grant no capabilities for that service. Capabilities are never broadened by
// this helper; Ceph validates their syntax.
type ClientCaps struct {
	Mon string
	OSD string
	MGR string
	MDS string
}

// ClientConfig describes a client identity created by this cluster. Credentials
// remain private; ConnectionConfig returns copies for an external native client.
// The cluster owns the auth database, while callers own attached containers.
type ClientConfig struct {
	owner   *Container
	name    string
	config  []byte
	keyring []byte
	created bool
	ready   bool
	revoked bool
}

// Name returns the fully qualified Cephx entity, for example client.reader.
func (client *ClientConfig) Name() string { return client.name }

// User returns the ID without the client. prefix, for APIs such as
// librados rados_create and go-ceph rados.NewConnWithUser.
func (client *ClientConfig) User() string { return strings.TrimPrefix(client.name, "client.") }

// KeyringPath returns the per-identity path used in an attached client container.
func (client *ClientConfig) KeyringPath() string { return "/etc/ceph/ceph." + client.name + ".keyring" }

// String deliberately excludes credentials, including when a descriptor is logged.
func (client ClientConfig) String() string { return "Cephx client " + client.name }

// GoString excludes credentials from formatted Go representations as well.
func (client ClientConfig) GoString() string { return client.String() }

// ConnectionConfig returns independent ceph.conf and keyring copies. The config
// is a snapshot at creation; WithClientIdentity uses the current MON addresses.
// Revocation does not erase already copied credentials or invalidate established
// sessions immediately. Test revoked credentials with a fresh connection.
func (client *ClientConfig) ConnectionConfig() ([]byte, []byte, error) {
	if client == nil || !client.ready || len(client.config) == 0 || len(client.keyring) == 0 {
		return nil, nil, errors.New("Cephx client credentials are unavailable")
	}
	return bytes.Clone(client.config), bytes.Clone(client.keyring), nil
}

var clientIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

func clientEntity(name string) (string, error) {
	id := strings.TrimPrefix(name, "client.")
	if !clientIDPattern.MatchString(id) {
		return "", errors.New("client name must be an ID or client.ID using letters, digits, dots, underscores or hyphens")
	}
	return "client." + id, nil
}

func clientCapArgs(caps ClientCaps) ([]string, error) {
	var args []string
	for _, cap := range []struct{ service, expression string }{
		{"mon", caps.Mon}, {"osd", caps.OSD}, {"mgr", caps.MGR}, {"mds", caps.MDS},
	} {
		if strings.IndexFunc(cap.expression, unicode.IsControl) != -1 {
			return nil, fmt.Errorf("%s capabilities must not contain control characters", cap.service)
		}
		if cap.expression != "" {
			args = append(args, cap.service, cap.expression)
		}
	}
	return args, nil
}

// CreateClient creates a fresh client with exactly the supplied capabilities.
// Existing identities are rejected before mutation; their keys and caps are
// never updated. A non-nil result with an error identifies an attempted or partial
// creation; inspect auth state or terminate the cluster rather than retrying with
// broader caps. DeleteClient can revoke a confirmed successful creation.
func (c *Container) CreateClient(ctx context.Context, name string, caps ClientCaps) (*ClientConfig, error) {
	entity, err := clientEntity(name)
	if err != nil {
		return nil, err
	}
	capArgs, err := clientCapArgs(caps)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if len(c.config) == 0 || c.cliContainer() == nil {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	data, err := c.clientAuthCommand(ctx, "check existing identity", "auth", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	var listing struct {
		AuthDump []struct {
			Entity string `json:"entity"`
		} `json:"auth_dump"`
	}
	if err := json.Unmarshal(data, &listing); err != nil || listing.AuthDump == nil {
		return nil, errors.New("decode Cephx identity listing")
	}
	for _, existing := range listing.AuthDump {
		if existing.Entity == entity {
			return nil, fmt.Errorf("Cephx identity %q already exists", entity)
		}
	}
	client := &ClientConfig{owner: c, name: entity}
	client.config = clientConfigBytes(c.config, client)
	control := c.cliContainer()
	secret, err := command(ctx, control, "ceph-authtool", "--gen-print-key")
	if err != nil || len(bytes.TrimSpace(secret)) == 0 {
		return client, errors.New("generate fresh Cephx key: command failed")
	}
	client.keyring = []byte("[" + entity + "]\nkey = " + string(bytes.TrimSpace(secret)) + "\n")
	keyringPath := "/tmp/tc-client-" + uuid.NewString() + ".keyring"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _ = command(cleanupCtx, control, "rm", "-f", keyringPath)
	}()
	if err := control.CopyToContainer(ctx, client.keyring, keyringPath, 0o600); err != nil {
		return client, errors.New("copy fresh Cephx keyring to control container failed")
	}
	// Supplying our fresh key makes Ceph reject a concurrently created identity
	// with another key. An auth-add command without -i can silently reuse it.
	args := append([]string{"auth", "add", entity, "-i", keyringPath}, capArgs...)
	if _, err := c.clientAuthCommand(ctx, "create "+entity, args...); err != nil {
		return client, err
	}
	client.created = true
	keyring, err := c.clientAuthCommand(ctx, "retrieve "+entity, "auth", "get", entity)
	if err != nil {
		return client, err
	}
	if clientKey(keyring, entity) != clientKey(client.keyring, entity) {
		return client, fmt.Errorf("Cephx keyring for %s does not contain the created key", entity)
	}
	client.keyring = bytes.Clone(keyring)
	client.ready = true
	return client, nil
}

func clientConfigBytes(base []byte, client *ClientConfig) []byte {
	config := bytes.Clone(base)
	config = append(config, []byte("\n["+client.name+"]\nkeyring = "+client.KeyringPath()+"\n")...)
	return config
}

// WithClientIdentity attaches a container with only the selected client keyring.
// It rejects descriptors from another cluster, incomplete creation and revocation.
// Caller owns the attached container and must terminate it before the cluster.
func (c *Container) WithClientIdentity(client *ClientConfig) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return errors.New("ceph cluster is terminated")
		}
		if client == nil || client.owner != c || !client.ready || client.revoked || len(client.keyring) == 0 {
			return errors.New("Cephx client must be an active identity created by this cluster")
		}
		if c.settings.hostNetwork {
			if err := hostContainerCustomizer(c.settings.publicAddress).Customize(req); err != nil {
				return err
			}
		} else if err := network.WithNetworkName(nil, c.NetworkName())(req); err != nil {
			return err
		}
		return testcontainers.WithFiles(
			textFile("/etc/ceph/ceph.conf", clientConfigBytes(c.config, client), 0o644),
			textFile(client.KeyringPath(), bytes.Clone(client.keyring), 0o600),
		)(req)
	}
}

// DeleteClient revokes an identity created by this cluster without deleting data.
// It checks for an externally replaced key before deleting; external auth edits
// must not race this operation. A repeat call after success is a no-op; existing
// sessions may retain issued tickets.
func (c *Container) DeleteClient(ctx context.Context, client *ClientConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if client == nil || client.owner != c || !client.created {
		return errors.New("Cephx client was not created successfully by this cluster")
	}
	if client.revoked {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	keyring, err := c.clientAuthCommand(ctx, "check "+client.name+" before revocation", "auth", "get", client.name)
	if err != nil {
		return err
	}
	if len(client.keyring) != 0 && clientKey(keyring, client.name) != clientKey(client.keyring, client.name) {
		return fmt.Errorf("Cephx identity %s has a different key; refusing revocation", client.name)
	}
	if _, err := c.clientAuthCommand(ctx, "revoke "+client.name, "auth", "del", client.name); err != nil {
		return err
	}
	client.revoked = true
	return nil
}

// Auth command output can contain keys, including on failure. Never include it
// in an error returned to test logs; preserve cancellation without secret output.
func (c *Container) clientAuthCommand(ctx context.Context, operation string, args ...string) ([]byte, error) {
	data, err := c.Ceph(ctx, args...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s: %w", operation, ctx.Err())
		}
		return nil, fmt.Errorf("%s: Ceph command failed", operation)
	}
	return data, nil
}

func clientKey(keyring []byte, entity string) string {
	active := false
	for _, line := range strings.Split(string(keyring), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			active = line == "["+entity+"]"
			continue
		}
		if active {
			key, value, ok := strings.Cut(line, "=")
			if ok && strings.TrimSpace(key) == "key" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}
