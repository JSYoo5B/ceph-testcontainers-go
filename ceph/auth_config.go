package ceph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// CephxCipher names a Cephx key type, which is also the cipher of the
// tickets made with such keys.
type CephxCipher string

const (
	// CipherAES is AES-128-CBC, the only key type of Squid and of Tentacle
	// before 20.2.4.
	CipherAES CephxCipher = "aes"
	// CipherAES256K is AES-256 with Kerberos-style key derivation
	// (AES256KRB5). It needs an image whose ceph-authtool has --key-type and
	// whose monmaptool has the cipher options, as in Ceph 20.2.4 and later,
	// where it is the default.
	CipherAES256K CephxCipher = "aes256k"
)

func validCephxCipher(cipher CephxCipher) bool {
	return cipher == CipherAES || cipher == CipherAES256K
}

// AuthConfig selects the Cephx settings of a new cluster. Zero fields keep
// the defaults of the image's Ceph release.
type AuthConfig struct {
	// KeyType is the type of the keys the fixture generates itself: the
	// mon., client.admin and OSD keys and, unless WithClientKeyType selects
	// another, CreateClient keys.
	KeyType CephxCipher
	// ServiceCipher encrypts service tickets. AllowedCiphers are the key
	// types that the MONs accept for authentication, and PreferredCipher is
	// the type the MONs use for keys they generate with auth get-or-create,
	// such as MGR, MDS and RGW keys. These MonMap settings need a KeyType and,
	// for anything but CipherAES, the image support that CipherAES256K
	// describes. ServiceCipher and PreferredCipher
	// default to KeyType, and AllowedCiphers to the distinct types among
	// KeyType, ServiceCipher and PreferredCipher. AllowedCiphers must contain
	// all three.
	ServiceCipher   CephxCipher
	AllowedCiphers  []CephxCipher
	PreferredCipher CephxCipher
	// MonTicketTTL and ServiceTicketTTL set auth_mon_ticket_ttl and
	// auth_service_ticket_ttl, in seconds with fractions kept, such as
	// 10500 * time.Millisecond. MONs rotate service secrets on their ticks, so
	// lifetimes of a few seconds also need a shorter mon_tick_interval, which
	// WithConfigFile can set. Keep MonTicketTTL at ten seconds or more: MON
	// clients renew their tickets only every few seconds, and a daemon whose
	// renewal comes too late is refused and does not authenticate again.
	MonTicketTTL     time.Duration
	ServiceTicketTTL time.Duration
}

// WithAuth selects the Cephx key type, the MonMap ciphers and the ticket
// lifetimes of a new cluster. The settings are part of the bootstrap: the key
// type and ciphers apply from the first MON, and the lifetimes are written to
// every daemon's and WithClient container's ceph.conf, so OverrideConfig
// cannot change them later. An image without key type support, such as Squid
// or Tentacle before 20.2.4, accepts a configuration in which every cipher is
// CipherAES, and Run fails before any daemon starts when one is not.
func WithAuth(config AuthConfig) Option {
	return func(o *options) error {
		normalized, err := normalizeAuthConfig(config)
		if err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		o.auth = normalized
		return nil
	}
}

func normalizeAuthConfig(config AuthConfig) (AuthConfig, error) {
	config.AllowedCiphers = slices.Clone(config.AllowedCiphers)
	if config.MonTicketTTL < 0 || config.ServiceTicketTTL < 0 {
		return config, invalid(errors.New("ticket lifetimes must not be negative"))
	}
	for _, cipher := range []CephxCipher{config.KeyType, config.ServiceCipher, config.PreferredCipher} {
		if cipher != "" && !validCephxCipher(cipher) {
			return config, invalid(fmt.Errorf("unknown Cephx cipher %q; use aes or aes256k", cipher))
		}
	}
	for i, cipher := range config.AllowedCiphers {
		if !validCephxCipher(cipher) {
			return config, invalid(fmt.Errorf("unknown allowed Cephx cipher %q; use aes or aes256k", cipher))
		}
		if slices.Contains(config.AllowedCiphers[:i], cipher) {
			return config, invalid(fmt.Errorf("allowed Cephx cipher %s is listed twice", cipher))
		}
	}
	if config.KeyType == "" {
		if config.ServiceCipher != "" || config.PreferredCipher != "" || len(config.AllowedCiphers) != 0 {
			return config, invalid(errors.New("a KeyType is required for ServiceCipher, AllowedCiphers and PreferredCipher"))
		}
		return config, nil
	}
	if config.ServiceCipher == "" {
		config.ServiceCipher = config.KeyType
	}
	if config.PreferredCipher == "" {
		config.PreferredCipher = config.KeyType
	}
	required := []CephxCipher{config.KeyType, config.ServiceCipher, config.PreferredCipher}
	if len(config.AllowedCiphers) == 0 {
		for _, cipher := range required {
			if !slices.Contains(config.AllowedCiphers, cipher) {
				config.AllowedCiphers = append(config.AllowedCiphers, cipher)
			}
		}
	}
	for _, cipher := range required {
		if !slices.Contains(config.AllowedCiphers, cipher) {
			return config, invalid(fmt.Errorf("allowed ciphers must contain the key type, service cipher and preferred cipher (%s)", cipher))
		}
	}
	return config, nil
}

// environment returns the MON bootstrap variables of the configuration.
func (config AuthConfig) environment() map[string]string {
	env := map[string]string{}
	if config.KeyType != "" {
		allowed := make([]string, len(config.AllowedCiphers))
		for i, cipher := range config.AllowedCiphers {
			allowed[i] = string(cipher)
		}
		env["CEPH_KEY_TYPE"] = string(config.KeyType)
		env["CEPH_AUTH_SERVICE_CIPHER"] = string(config.ServiceCipher)
		env["CEPH_AUTH_ALLOWED_CIPHERS"] = strings.Join(allowed, ",")
		env["CEPH_AUTH_PREFERRED_CIPHER"] = string(config.PreferredCipher)
	}
	if config.MonTicketTTL > 0 {
		env["CEPH_AUTH_MON_TICKET_TTL"] = strconv.FormatFloat(config.MonTicketTTL.Seconds(), 'f', -1, 64)
	}
	if config.ServiceTicketTTL > 0 {
		env["CEPH_AUTH_SERVICE_TICKET_TTL"] = strconv.FormatFloat(config.ServiceTicketTTL.Seconds(), 'f', -1, 64)
	}
	return env
}

// ClientOption customizes CreateClient.
type ClientOption func(*clientOptions) error

type clientOptions struct {
	keyType CephxCipher
}

// WithClientKeyType selects the key type of a new client identity instead of
// the cluster's AuthConfig.KeyType or the release default. The MONs must
// allow the type for the client to authenticate. CipherAES256K needs an
// image with key type support (see CipherAES256K); without it CreateClient
// fails before any change.
func WithClientKeyType(cipher CephxCipher) ClientOption {
	return func(o *clientOptions) error {
		if !validCephxCipher(cipher) {
			return invalidRequest("unknown Cephx cipher %q; use aes or aes256k", cipher)
		}
		o.keyType = cipher
		return nil
	}
}

// generateKeyArgs returns the ceph-authtool command that prints a fresh key
// of keyType, or of the release default when keyType is empty. A
// ceph-authtool without --key-type, as in Squid and Tentacle before 20.2.4,
// generates only AES keys.
func (c *Container) generateKeyArgs(keyType CephxCipher) ([]string, error) {
	args := []string{"ceph-authtool", "--gen-print-key"}
	switch {
	case keyType == "":
		return args, nil
	case !c.keyTypeOption && keyType != CipherAES:
		return nil, &OperationError{Kind: ErrUnsupported, Target: "control", Version: c.CephVersion(), Image: c.controlImageID,
			Capability: "ceph-authtool --key-type",
			Err:        fmt.Errorf("cephx key type %s needs ceph-authtool --key-type, which the control image lacks", keyType)}
	case !c.keyTypeOption:
		return args, nil
	}
	return append(args, "--key-type", string(keyType)), nil
}

// detectKeyTypeOption reports whether ctr's ceph-authtool accepts --key-type.
// Its usage text lists the option when it does.
func detectKeyTypeOption(ctx context.Context, ctr testcontainers.Container) (bool, error) {
	_, reader, err := ctr.Exec(ctx, []string{"ceph-authtool", "--help"}, tcexec.Multiplexed())
	if err != nil {
		return false, fmt.Errorf("inspect ceph-authtool options: %w", err)
	}
	usage, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil {
		return false, fmt.Errorf("inspect ceph-authtool options: %w", err)
	}
	return strings.Contains(string(usage), "--key-type"), nil
}

// containerImageID returns the ID of the image that ctr runs, or "" when it
// cannot be read; it only details errors.
func containerImageID(ctx context.Context, ctr testcontainers.Container) string {
	inspect, err := ctr.Inspect(ctx)
	if err != nil || inspect == nil {
		return ""
	}
	return inspect.Image
}

// bootstrapRefused is what a bootstrap script printed before it refused to
// start, in lines "tc-ceph: KIND: REASON": an "unsupported" Cephx cipher that
// the image's tools cannot generate, an "image" without a tool that the
// bootstrap needs, or an "environment" without an address of the requested
// family.
type bootstrapRefused struct {
	kind    error
	reasons []string
}

// with returns the refusal of the first MON with the error of its start. A
// start that ctx ended keeps the reasons but no category.
func (r *bootstrapRefused) with(ctx context.Context, err error) error {
	text := fmt.Errorf("%s: %w", strings.Join(r.reasons, "; "), err)
	if r.kind == nil || ctx.Err() != nil {
		return text
	}
	return &OperationError{Kind: r.kind, Target: "mon." + daemonName(0), Err: text}
}

// bootstrapRefusal returns the refusal that ctr's bootstrap script printed,
// or nil when it printed none.
func bootstrapRefusal(ctx context.Context, ctr testcontainers.Container) *bootstrapRefused {
	if ctr == nil {
		return nil
	}
	logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	reader, err := ctr.Logs(logCtx)
	if err != nil {
		return nil
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil {
		return nil
	}
	return parseBootstrapRefusal(data)
}

// parseBootstrapRefusal reads the refusal lines of a bootstrap script's log.
func parseBootstrapRefusal(data []byte) *bootstrapRefused {
	refused := &bootstrapRefused{}
	kinds := map[string]error{"unsupported": ErrUnsupported, "image": ErrImageCapability, "environment": ErrEnvironmentUnavailable}
	for _, line := range strings.Split(string(data), "\n") {
		_, reason, ok := strings.Cut(line, "tc-ceph: ")
		if !ok {
			continue
		}
		name, text, tagged := strings.Cut(strings.TrimSpace(reason), ": ")
		kind, known := kinds[name]
		if !tagged || !known {
			kind, text = nil, strings.TrimSpace(reason)
		}
		// Lines of different kinds leave the refusal unclassified.
		if len(refused.reasons) == 0 {
			refused.kind = kind
		} else if refused.kind != kind {
			refused.kind = nil
		}
		refused.reasons = append(refused.reasons, text)
	}
	if len(refused.reasons) == 0 {
		return nil
	}
	return refused
}
