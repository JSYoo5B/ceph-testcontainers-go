package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
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
	// replacedBy is the descriptor of the key that ClientConfig.ReplaceKey gave the
	// identity in place of this descriptor's key.
	replacedBy *ClientConfig
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
// On error it returns the zero ConnectionConfig.
func (client *ClientConfig) ConnectionConfig() (ConnectionConfig, error) {
	if client == nil || !client.ready || len(client.config) == 0 || len(client.keyring) == 0 {
		return ConnectionConfig{}, errors.New("cephx client credentials are unavailable")
	}
	return ConnectionConfig{Conf: bytes.Clone(client.config), Keyring: bytes.Clone(client.keyring)}, nil
}

// ConnectionConfig holds the files that an external native client needs to
// connect: the ceph.conf and the keyring. The fields are named so that the
// two byte slices cannot be swapped by a positional assignment. Each value
// returned by a ConnectionConfig method owns independent copies of the bytes.
// The keyring is a credential: it is not redacted, so do not log the value.
type ConnectionConfig struct {
	// Conf is the content of ceph.conf.
	Conf []byte
	// Keyring is the content of the keyring file.
	Keyring []byte
}

var clientIDPattern = naming.ClientID

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
// broader caps. ClientConfig.Delete can revoke a confirmed successful creation.
// The key has the cluster's AuthConfig.KeyType, the release default without
// one, or the type that ClientOptions.WithKeyType selects.
func (c *Container) CreateClient(ctx context.Context, name string, caps ClientCaps, opts ...ClientOptions) (*ClientConfig, error) {
	return c.createClient(ctx, name, caps, opts...)
}

func (c *Container) createClient(ctx context.Context, name string, caps ClientCaps, opts ...ClientOptions) (*ClientConfig, error) {
	entity, err := clientEntity(name)
	if err != nil {
		return nil, err
	}
	keyType, err := resolveClientOptions(c.settings.auth.KeyType, opts)
	if err != nil {
		return nil, err
	}
	generate, err := c.generateKeyArgs(keyType)
	if err != nil {
		return nil, err
	}
	capArgs, err := clientCapArgs(caps)
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return nil, err
	}
	if !c.bootstrap.hasConfig() || control == nil {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.settleClient(ctx, entity); err != nil {
		return nil, err
	}
	if c.authRecords.revocation(entity) != nil {
		return nil, fmt.Errorf("cephx identity %q is revoked temporarily; restore it or delete it first", entity)
	}
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
			return nil, invalid(fmt.Errorf("cephx identity %q already exists", entity))
		}
	}
	client := &ClientConfig{owner: c, name: entity}
	client.config = clientConfigBytes(c.bootstrap.rawConfig(), client)
	control, err = c.controlContainerContext(ctx)
	if err != nil {
		return client, err
	}
	secret, err := command(ctx, control, generate...)
	if err != nil || len(bytes.TrimSpace(secret)) == 0 {
		return client, clientOperationError(ctx, "generate fresh Cephx key: command failed", err)
	}
	c.authRecords.addSecret(string(bytes.TrimSpace(secret)))
	client.keyring = []byte("[" + entity + "]\nkey = " + string(bytes.TrimSpace(secret)) + "\n")
	keyringPath := "/tmp/tc-client-" + uuid.NewString() + ".keyring"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _ = command(cleanupCtx, control, "rm", "-f", keyringPath)
	}()
	if err := control.CopyToContainer(ctx, client.keyring, keyringPath, 0o600); err != nil {
		return client, clientOperationError(ctx, "copy fresh Cephx keyring to control container failed", err)
	}
	// Supplying our fresh key makes Ceph reject a concurrently created identity
	// with another key. An auth-add command without -i can silently reuse it.
	args := append([]string{"auth", "add", entity, "-i", keyringPath}, capArgs...)
	if _, err := c.clientAuthCommand(ctx, "create "+entity, args...); err != nil {
		return client, err
	}
	client.created = true
	c.authRecords.confirmKey(entity, clientKey(client.keyring, entity))
	keyring, err := c.clientAuthCommand(ctx, "retrieve "+entity, "auth", "get", entity)
	if err != nil {
		return client, err
	}
	if clientKey(keyring, entity) != clientKey(client.keyring, entity) {
		return client, fmt.Errorf("cephx keyring for %s does not contain the created key", entity)
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
// The options customize the container's ceph.conf.
func (c *Container) WithClientIdentity(client *ClientConfig, opts ...ClientContainerOptions) testcontainers.CustomizeRequestOption {
	return c.withClientIdentity(client, opts...)
}

func (c *Container) withClientIdentity(client *ClientConfig, opts ...ClientContainerOptions) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return errClusterTerminated
		}
		if client == nil || client.owner != c || !client.ready || client.revoked || len(client.keyring) == 0 {
			return errors.New("cephx client must be an active identity created by this cluster")
		}
		if err := c.checkCurrentClient(client); err != nil {
			return err
		}
		if c.settings.hostNetwork {
			if err := hostContainerCustomizer(c.settings.publicAddress).Customize(req); err != nil {
				return err
			}
		} else if err := network.WithNetworkName(nil, c.staticFacts().NetworkName)(req); err != nil {
			return err
		}
		config, err := clientContainerConfig(clientConfigBytes(c.bootstrap.rawConfig(), client), opts)
		if err != nil {
			return err
		}
		return testcontainers.WithFiles(
			textFile("/etc/ceph/ceph.conf", config, 0o644),
			textFile(client.KeyringPath(), bytes.Clone(client.keyring), 0o600),
		)(req)
	}
}

// Auth command output can contain keys, including on failure. Never include it
// in an error returned to test logs; preserve cancellation without secret output.
func (c *Container) clientAuthCommand(ctx context.Context, operation string, args ...string) ([]byte, error) {
	data, err := c.cephCLI(ctx, args...)
	if err != nil {
		return nil, clientOperationError(ctx, operation+": Ceph command failed", err)
	}
	return data, nil
}

// Native key generation, copies and auth errors can contain credentials. Keep
// only canonical context causes while returning a fixed operation description.
func clientOperationError(ctx context.Context, operation string, native error) error {
	return clusterutil.OperationError(ctx, operation, native)
}

func clientKey(keyring []byte, entity string) string { return clusterutil.KeyringKey(keyring, entity) }
