// Package mdsmap decodes the native global MDS map (`ceph fs dump`) for the
// MDS lifecycle decisions of package cephfs: that a filesystem is still cold,
// that a stopped MDS is gone from it, and how far its replacement got. It also
// holds the strict decoder of the other native documents of that lifecycle and
// the check that an `auth ls` document lists an identity. Each decoder is a
// function of the document and of the plain expectation the caller read from
// its filesystem record: the package takes no lock, starts no command and
// knows no filesystem handle.
package mdsmap
