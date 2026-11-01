// Package gwbase is the foundation that package rgw and its feature packages
// (rgw/account, rgw/placement and rgw/multisite) share for one gateway and the
// resources created through it: the gateway identity (its cluster, name,
// configured scope, listener and container), the admin command transport, the
// selection of the gateway that runs a resource handle's operations, the
// native runtime scope of a running gateway, and the private state of owned
// users and accounts.
//
// Package rgw owns the gateway registry and constructs each Core with
// NewCore, passing the address of the gateway's exported Container field and
// the reader of the cluster's registered gateway cores in name order. gwbase
// keeps no registry, no global state and no mutex of its own. A Gateway
// embeds a Handle, so *rgw.Gateway satisfies Owner, and Of returns its Core;
// a User embeds a UserHandle and an Account an AccountHandle in the same way,
// and a Placement embeds an Origin, whose OriginOf is the core of the gateway
// that created it. The handles have no exported member, so nothing is
// promoted into the public API.
//
// Every copy of a gateway, user or account handle shares one Core, User or
// Account: copies share lifecycle state, as the public packages document.
// Unlike the cephfs foundation, a value copy is not detached.
//
// Lock contract: the operations of this package that issue native commands or
// read the gateway registry are called with the owning cluster's mutex held
// (servicehook.Lock); the public packages take it. The fields of User and
// Account and their states are read and written with that mutex held. The
// identity of a Core is fixed at construction and needs no lock.
package gwbase
