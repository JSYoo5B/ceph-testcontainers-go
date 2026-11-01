// Package mdsmap decodes the native global MDS map (`ceph fs dump`) for the
// MDS lifecycle decisions of package cephfs: that a filesystem is still cold,
// that a stopped MDS is gone from it, and how far its replacement got. Each
// decoder is a function of the document and of the plain expectation the
// caller read from its filesystem record: the package takes no lock, starts no
// command and knows no filesystem handle.
package mdsmap
