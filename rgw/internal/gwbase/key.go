package gwbase

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// KeyStatus is what the fixture knows about one of its S3 keys in native
// state.
type KeyStatus int

const (
	// KeyActive is a key whose creation was confirmed and that was not
	// revoked: the user must still hold it.
	KeyActive KeyStatus = iota
	// KeyPending is a key whose creation outcome is unknown. It is never
	// handed off; RevokeKey settles and removes it.
	KeyPending
	// KeyRevoking is a key whose removal outcome is unknown; RevokeKey
	// settles it.
	KeyRevoking
	// KeyRevoked is a key that native state is known not to hold: it was
	// removed, or its creation is known not to have applied.
	KeyRevoked
)

// Key is one S3 key pair that the fixture created, or tried to create, for an
// owned user. Every copy of a public key handle shares one Key.
//
// AccessKey is chosen by the fixture before the native command runs, so it
// identifies the key in native state whatever the command's outcome.
// SecretKey is set once, before the key's handle is returned, and only for a
// key whose creation was confirmed then; it never changes afterwards, so it
// is read without a lock. Status, NativeSecret and Unstopped are read and
// written with the owning cluster's mutex held.
type Key struct {
	AccessKey, SecretKey string
	Status               KeyStatus
	// NativeSecret is the secret that native state showed for a pending key
	// when RevokeKey settled it. It is kept for redaction only.
	NativeSecret string
	// Unstopped is a creation or removal command of the key that may still
	// run in the gateway container.
	Unstopped *ceph.NativeCommandUnstoppedError
}

// accessKeyAlphabet and accessKeyLength follow the access keys that
// radosgw-admin generates.
const (
	accessKeyAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	accessKeyLength   = 20
)

// generateAccessKey returns a random access key of the native form.
func generateAccessKey() (string, error) {
	random := make([]byte, accessKeyLength)
	if _, err := rand.Read(random); err != nil {
		return "", errors.New("generate an RGW access key")
	}
	key := make([]byte, accessKeyLength)
	for i, b := range random {
		key[i] = accessKeyAlphabet[int(b)%len(accessKeyAlphabet)]
	}
	return string(key), nil
}

// Keys returns the keys of u that the fixture created or tried to create:
// the creation key, when confirmed, then the issued keys in order. The caller
// holds the owning cluster's mutex.
func (u *User) Keys() []*Key {
	if u == nil {
		return nil
	}
	var keys []*Key
	if u.Creation != nil {
		keys = append(keys, u.Creation)
	}
	if u.State != nil {
		keys = append(keys, u.State.Issued...)
	}
	return keys
}

// Secrets lists every access and secret key of u that the fixture created,
// tried to create or saw, for diagnostic redaction. The caller holds the
// owning cluster's mutex.
func (u *User) Secrets() []string {
	var secrets []string
	for _, key := range u.Keys() {
		for _, value := range []string{key.AccessKey, key.SecretKey, key.NativeSecret} {
			if value != "" {
				secrets = append(secrets, value)
			}
		}
	}
	return secrets
}

func findKey(keys []NativeKey, accessKey string) *NativeKey {
	for i := range keys {
		if keys[i].AccessKey == accessKey {
			return &keys[i]
		}
	}
	return nil
}

// errOtherCredentials refuses a user whose fixture-created keys are not what
// the fixture recorded.
var errOtherCredentials = servicehook.Invalid(errors.New("RGW user has different credentials; refusing owned-user operation"))

// CheckKeys confirms that native keys hold the fixture-created keys of u as
// the fixture recorded them: every active key with its secret, and no revoked
// key. Keys whose outcome is unknown may be present or absent, and keys the
// fixture did not create are ignored. At least one active key must be
// present: it is what identifies the owned user. The caller holds the owning
// cluster's mutex.
func (u *User) CheckKeys(keys []NativeKey) error {
	active, err := u.checkKeys(keys, nil)
	if err == nil && active == 0 {
		err = errOtherCredentials
	}
	return err
}

// checkKeys checks the keys of u other than except as CheckKeys does and
// returns how many active ones it found.
func (u *User) checkKeys(keys []NativeKey, except *Key) (int, error) {
	active := 0
	for _, key := range u.Keys() {
		if key == except {
			continue
		}
		native := findKey(keys, key.AccessKey)
		switch key.Status {
		case KeyActive:
			if native == nil || native.SecretKey != key.SecretKey {
				return 0, errOtherCredentials
			}
			active++
		case KeyRevoked:
			if native != nil {
				return 0, errOtherCredentials
			}
		}
	}
	return active, nil
}

// sameKeys reports whether two native key lists hold the same key pairs.
func sameKeys(a, b []NativeKey) bool {
	sortKeys := func(keys []NativeKey) []NativeKey {
		keys = slices.Clone(keys)
		slices.SortFunc(keys, func(x, y NativeKey) int {
			return cmp.Or(strings.Compare(x.AccessKey, y.AccessKey), strings.Compare(x.SecretKey, y.SecretKey))
		})
		return keys
	}
	return slices.Equal(sortKeys(a), sortKeys(b))
}

// withoutKey returns keys without the key pair of accessKey.
func withoutKey(keys []NativeKey, accessKey string) []NativeKey {
	return slices.DeleteFunc(slices.Clone(keys), func(key NativeKey) bool { return key.AccessKey == accessKey })
}

// supervisedUserCommand runs a radosgw-admin command of u through g as
// UserCommand does, under the gateway's shell supervisor: a call whose ctx
// ends stops the command before it returns, and a stop that cannot be
// confirmed returns a *ceph.NativeCommandUnstoppedError.
func (g *Core) supervisedUserCommand(ctx context.Context, u *User, args ...string) ([]byte, error) {
	if u.State.NativeScope == nil {
		return g.AdminCommand(ctx, args...)
	}
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	if scope != *u.State.NativeScope {
		return nil, errors.New("RGW user's native runtime scope changed")
	}
	return g.SupervisedCommand(ctx, scope, args...)
}

// settle reads the user's keys after a mutation on a fresh settlement budget.
func (g *Core) settle(ctx context.Context, u *User) (*NativeUser, error) {
	settleCtx, cancel := SettlementContext(ctx)
	defer cancel()
	return g.readUser(settleCtx, u)
}

// CreateKey creates a new S3 key pair for the active user u through g and
// records it in u before the native command runs. The fixture chooses the
// access key and RGW generates the secret. The outcome is read back from
// native state, also after a failed call, and the command is never repeated:
//
//   - confirmed: the key is active and has its secret; with a failed call, the
//     key is returned with the call's error;
//   - known not to have applied: the key is recorded as revoked and nil is
//     returned with the error;
//   - unknown: the key is pending and returned with the error; RevokeKey
//     settles and removes it.
//
// The other keys of u, including those the fixture did not create, must be
// unchanged. The caller holds the owning cluster's mutex, has validated g and
// bounds ctx with AdminContext.
func (g *Core) CreateKey(ctx context.Context, u *User) (*Key, error) {
	before, err := g.OwnedUser(ctx, u)
	if err != nil {
		return nil, err
	}
	accessKey, err := generateAccessKey()
	if err != nil {
		return nil, err
	}
	if findKey(before.Keys, accessKey) != nil || slices.ContainsFunc(u.Keys(), func(key *Key) bool { return key.AccessKey == accessKey }) {
		return nil, errors.New("generated RGW access key is already in use")
	}
	key := &Key{AccessKey: accessKey, Status: KeyPending}
	u.State.Issued = append(u.State.Issued, key)
	_, cmdErr := g.supervisedUserCommand(ctx, u, "key", "create", "--uid", u.ID, "--key-type", "s3", "--access-key", accessKey, "--gen-secret")
	errors.As(cmdErr, &key.Unstopped)
	cause := cmdErr
	if cause == nil {
		cause = errors.New("RGW S3 key creation answered")
	}
	after, err := g.settle(ctx, u)
	if err != nil {
		return key, errors.Join(cmdErr, fmt.Errorf("whether RGW created the S3 key is unknown; the key is tracked and its Revoke settles and removes it: %w", err))
	}
	native := findKey(after.Keys, accessKey)
	switch {
	case native == nil && key.Unstopped == nil:
		key.Status = KeyRevoked
		return nil, fmt.Errorf("%w; RGW shows that the S3 key was not created, and it no longer can be", cause)
	case native == nil:
		return key, errors.Join(cmdErr, errors.New("the S3 key creation may still run; the key is tracked and its Revoke settles and removes it"))
	case native.SecretKey == "" || !sameKeys(withoutKey(after.Keys, accessKey), before.Keys):
		return key, errors.Join(cmdErr, errors.New("RGW created the S3 key but changed the user's other keys or omitted its secret; the key is tracked and its Revoke removes it"))
	}
	key.SecretKey, key.Status, key.Unstopped = native.SecretKey, KeyActive, nil
	if cmdErr != nil {
		return key, errors.Join(cmdErr, errors.New("RGW shows that the S3 key was created; it is tracked and its credentials are usable"))
	}
	return key, nil
}

// RevokeKey removes the fixture-created key of the active user u through g,
// unless native state no longer holds it, and records it as revoked. It
// first settles a command of the key that may still run, and refuses to
// remove the user's last active fixture-created key, which identifies the
// owned user, or a key whose secret changed outside the fixture. The other
// keys of u must be unchanged. A removal whose outcome is unknown leaves the
// key revoking; a later RevokeKey settles it. The caller holds the owning
// cluster's mutex, has validated g and bounds ctx with AdminContext.
func (g *Core) RevokeKey(ctx context.Context, u *User, key *Key) error {
	if key.Status == KeyRevoked {
		return nil
	}
	if !u.Active() {
		return errors.New("RGW user must be an active identity created in this cluster")
	}
	if key.Unstopped != nil {
		if err := key.Unstopped.Stop(ctx); err != nil {
			return fmt.Errorf("an earlier command of the RGW S3 key may still run: %w", err)
		}
		key.Unstopped = nil
	}
	before, err := g.readUser(ctx, u)
	if err != nil {
		return err
	}
	active, err := u.checkKeys(before.Keys, key)
	if err != nil {
		return err
	}
	if active == 0 {
		return servicehook.Invalid(errors.New("revoking the user's last active fixture-created S3 key would leave nothing that identifies the owned user; create a replacement key first or remove the user"))
	}
	if err := g.ValidateCreationIdentity(ctx, u, before); err != nil {
		return err
	}
	native := findKey(before.Keys, key.AccessKey)
	if native == nil {
		key.Status = KeyRevoked
		return nil
	}
	expected := key.SecretKey
	if expected == "" {
		expected = key.NativeSecret
	}
	if expected != "" && native.SecretKey != expected {
		return servicehook.Invalid(errors.New("RGW S3 key has a different secret; refusing to revoke a key changed outside the fixture"))
	}
	if key.SecretKey == "" {
		key.NativeSecret = native.SecretKey
	}
	prior := key.Status
	key.Status = KeyRevoking
	_, cmdErr := g.supervisedUserCommand(ctx, u, "key", "rm", "--uid", u.ID, "--key-type", "s3", "--access-key", key.AccessKey)
	errors.As(cmdErr, &key.Unstopped)
	after, err := g.settle(ctx, u)
	if err != nil {
		return errors.Join(cmdErr, fmt.Errorf("whether RGW removed the S3 key is unknown; Revoke settles it: %w", err))
	}
	if findKey(after.Keys, key.AccessKey) != nil {
		if key.Unstopped == nil {
			key.Status = prior
		}
		if cmdErr == nil {
			cmdErr = errors.New("RGW answered without removing the S3 key")
		}
		return cmdErr
	}
	key.Status, key.Unstopped = KeyRevoked, nil
	if !sameKeys(after.Keys, withoutKey(before.Keys, key.AccessKey)) {
		return errors.New("RGW removed the S3 key but changed the user's other keys")
	}
	return nil
}
