package rgw

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// S3Key is one S3 access and secret key pair that the fixture created for an
// owned user: the key pair of the user's creation, from User.CreationKey, or
// one from User.CreateS3Key. A key handle names one key pair and never
// changes which: creating another key does not change what an existing handle,
// or credentials copied from it, mean. Copies of a key handle share its state.
// Formatting a key handle never includes its keys.
//
// Creating a key or revoking one changes what RGW accepts; it does not show
// that the system under test reloaded its configuration or dropped a cached
// credential. Hand the new key to the SUT the way it reads its configuration,
// and assert its behavior with requests it makes after that.
type S3Key struct {
	user *gwbase.User
	key  *gwbase.Key
}

// newS3Key returns the handle of key, a key of user; nil for a nil key.
func newS3Key(user *gwbase.User, key *gwbase.Key) *S3Key {
	if key == nil {
		return nil
	}
	return &S3Key{user: user, key: key}
}

// CreationKey returns the handle of the S3 key pair that this user's
// creation generated; nil when the creation response did not confirm one.
// It names that key pair even after it is revoked or after other keys were
// created.
//
//purpose:handoff
func (u *User) CreationKey() *S3Key {
	user := u.base()
	if user == nil || user.Creation == nil || user.Creation.SecretKey == "" {
		return nil
	}
	return newS3Key(user, user.Creation)
}

// CreateS3Key creates one more S3 key pair for this owned user, keeping the
// user, its data and its other keys, so a test can hand the SUT a replacement
// credential and later revoke the old one with S3Key.Revoke. The fixture
// generates the access key and RGW the secret, and the new key is tracked as
// the fixture's before the native command runs. The user must be active, and
// its fixture-created keys must be as the fixture recorded them.
//
// The outcome is read back from native state, also when the call fails or
// its ctx ends, and the native command is never repeated:
//
//   - a nil error returns the confirmed key;
//   - a non-nil key with an error is tracked: when native state showed the
//     key, its Credentials work and the error keeps the call's failure, such
//     as its context's; otherwise whether the key exists is unknown,
//     Credentials refuses it, and its Revoke settles and removes it;
//   - a nil key with an error means that RGW shows that no key was created.
//
// A new call always creates a different key; it does not retry an earlier
// one. Native key edits of this user from Admin or external clients must not
// race this operation.
//
//purpose:construction,handoff
func (u *User) CreateS3Key(ctx context.Context) (*S3Key, error) {
	user := u.base()
	if !user.Owned() {
		return nil, servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	cluster := user.Origin.Cluster()
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return nil, err
	}
	defer servicehook.Unlock(cluster)
	g, err := user.Origin.ScopeGateway(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	key, err := g.CreateKey(ctx, user)
	return newS3Key(user, key), err
}

// Credentials returns the access and secret keys of this key pair as explicit
// copies. They stay the same after the key is revoked or the user removed,
// so a test can hand a revoked key to the SUT to check that RGW refuses it;
// revocation does not erase copies. A key whose creation was not confirmed
// returns an error.
//
//purpose:handoff
func (k *S3Key) Credentials() (accessKey, secretKey string, err error) {
	if k == nil || k.key == nil || k.key.SecretKey == "" {
		return "", "", errors.New("RGW S3 credentials are unavailable")
	}
	return k.key.AccessKey, k.key.SecretKey, nil
}

// Revoke removes this key pair from its user, irreversibly: RGW refuses new
// requests signed with it once its metadata caches see the removal, so poll
// with new requests. Nothing restores a revoked key; create another with
// User.CreateS3Key. The user, its data and its other keys stay.
//
// Revoke refuses, with ceph.ErrInvalidRequest, the user's last active
// fixture-created key, which identifies the owned user to later operations,
// and a key whose secret changed outside the fixture. A key that native state
// no longer holds, a key of a removed user and a key already revoked return
// nil. A key whose creation outcome was unknown is settled first: a native
// command of it that may still run is stopped, and the key is removed when
// native state holds it.
//
// When the removal fails, its outcome is read back: a key that is gone is
// revoked and Revoke returns nil; a key still present stays tracked and
// usable, and Revoke can be called again; an unknown outcome is settled by
// the next Revoke.
//
//purpose:condition,lifetime
func (k *S3Key) Revoke(ctx context.Context) error {
	if k == nil || k.key == nil || !k.user.Owned() {
		return servicehook.Invalid(errors.New("RGW S3 key is not owned by a Ceph cluster"))
	}
	user := k.user
	cluster := user.Origin.Cluster()
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(cluster)
	if k.key.Status == gwbase.KeyRevoked {
		return nil
	}
	if user.State.Removed {
		k.key.Status = gwbase.KeyRevoked
		return nil
	}
	g, err := user.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	return g.RevokeKey(ctx, user, k.key)
}

// String names the key's user without the keys.
func (k S3Key) String() string {
	if k.user == nil {
		return "RGW S3 key"
	}
	return "RGW S3 key of user " + k.user.ID
}

// GoString formats the key like String, so %#v does not print its keys.
func (k S3Key) GoString() string { return k.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (k S3Key) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, k.String()) }
