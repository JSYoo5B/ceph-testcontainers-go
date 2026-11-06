// Package dashboard prepares the Ceph Dashboard of an existing cluster as a
// collaborator for an application that consumes its REST API: an owned HTTPS
// listener with the caller's certificate, an owned account with one role, the
// endpoint of the active manager after an authenticated readiness check, and
// optionally the RGW backend of one owned gateway with a dedicated read-only
// RGW identity.
//
// The Dashboard backend runs inside ceph-mgr, so the cluster's managers must
// run an image that ships the dashboard module and its frontend, such as the
// official Ceph image selected through ceph.ImageSet.MGR; the essential role
// image does not. Start acts on a cluster that ceph.Run or a service package's
// Run started; it starts no container. Dashboard requests and application
// workflows remain the SUT's: the package adds no Dashboard client.
package dashboard
