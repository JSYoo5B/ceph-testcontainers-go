// Package harness shares one Ceph cluster between the tests of a test binary.
//
// A SharedCluster lends each test a Tenant: a RADOS namespace of SharedPool and a
// Cephx client limited to it. Exclusive lends the whole cluster to one test.
// The service harness packages rbd/harness, cephfs/harness and rgw/harness
// lease service resources of a tenant; give the cluster their support with
// WithServices. Package ceph keeps the cluster, its operations and the gate and
// does not import this package.
package harness
