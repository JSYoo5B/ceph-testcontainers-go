// Package placement prepares bucket placement targets and storage classes
// through an existing gateway: a fresh, non-default placement target with its
// index, multipart and per-class data pools, its activation in a standalone
// gateway or a realm period, and a user's default placement.
//
// Create creates an owned placement through a *rgw.Gateway in the gateway's
// actual native zone and zonegroup. An operation on an owned placement is a
// method of its handle, which records the native IDs, pool IDs and zone
// documents it created and refuses a replaced pool or a changed zone. Apply
// and Reload restart the gateway they are given. These functions act on a
// gateway that rgw.Run, rgw.Start or rgw.WithGateways started; they start no
// container.
package placement
