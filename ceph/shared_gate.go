package ceph

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// sharedPolicy classifies what a method of *Container may do while a
// SharedCluster shares the cluster.
type sharedPolicy int

const (
	// sharedOpen methods only read state or describe the fixture.
	sharedOpen sharedPolicy = iota + 1
	// sharedTenant methods have a scoped Tenant counterpart; the raw method
	// could act outside one tenant's scope.
	sharedTenant
	// sharedExclusive methods change cluster-wide state or return a raw
	// handle to the cluster's containers or admin credentials.
	sharedExclusive
	// sharedRecycle methods change the topology or damage data, so the
	// cluster would have to be replaced afterwards.
	sharedRecycle
	// sharedRefused methods end the cluster, which only SharedCluster.Close
	// does.
	sharedRefused
)

func (p sharedPolicy) reason() string {
	switch p {
	case sharedTenant:
		return "acts outside one tenant's scope; use the Tenant's method"
	case sharedExclusive:
		return "changes cluster-wide state or exposes raw access, which tenants of a shared cluster cannot have"
	case sharedRecycle:
		return "changes the topology or damages data; use a cluster of the test's own"
	case sharedRefused:
		return "would end the cluster; only SharedCluster.Close ends a shared cluster"
	}
	return "is not classified for a shared cluster"
}

// sharedMethodPolicies classifies every method of *Container, including the
// ones promoted from testcontainers.Container. TestSharedGateClassifiesEveryMethod
// fails for a method that is missing here, and calls every method that is not
// sharedOpen on a shared cluster to check that it is refused, so a new
// method cannot bypass the gate unnoticed.
var sharedMethodPolicies = map[string]sharedPolicy{
	// Inspection of the fixture and read-only views of the cluster.
	"AddressFamily":             sharedOpen,
	"BlocklistEntries":          sharedOpen,
	"CephVersion":               sharedOpen,
	"ClusterNetworkName":        sharedOpen,
	"CollectCrashMetadata":      sharedOpen,
	"CollectDiagnostics":        sharedOpen,
	"Configuration":             sharedOpen,
	"Images":                    sharedOpen,
	"ClientContainer":           sharedExclusive,
	"EssentialImage":            sharedOpen,
	"DaemonAddresses":           sharedOpen,
	"FullRatios":                sharedOpen,
	"HasSeparateClusterNetwork": sharedOpen,
	"HealthDetails":             sharedOpen,
	"ManagerMetrics":            sharedOpen,
	"ManagerServices":           sharedOpen,
	"ManagerStatus":             sharedOpen,
	"MessengerMode":             sharedOpen,
	"MGRModules":                sharedOpen,
	"MonitorBootstrapAddresses": sharedOpen,
	"NetworkName":               sharedOpen,
	"ObjectWatchers":            sharedOpen,
	"OSDFlags":                  sharedOpen,
	"OSDStates":                 sharedOpen,
	"PGInconsistencies":         sharedOpen,
	"Pool":                      sharedOpen,
	"Pools":                     sharedOpen,
	"PublicAddress":             sharedOpen,
	"QuorumStatus":              sharedOpen,
	"RedactSecrets":             sharedOpen,
	"Status":                    sharedOpen,
	"UsesHostNetwork":           sharedOpen,
	"WaitForClean":              sharedOpen,
	"WaitForPGClean":            sharedOpen,
	"WaitForQuorum":             sharedOpen,
	"WaitMGRModuleReady":        sharedOpen,
	// Promoted from the primary MON's testcontainers.Container: inspection.
	"ContainerIP":                  sharedOpen,
	"ContainerIPs":                 sharedOpen,
	"Endpoint":                     sharedOpen,
	"FollowOutput":                 sharedOpen,
	"GetContainerID":               sharedOpen,
	"GetLogProductionErrorChannel": sharedOpen,
	"Host":                         sharedOpen,
	"Inspect":                      sharedOpen,
	"IsRunning":                    sharedOpen,
	"Logs":                         sharedOpen,
	"MappedPort":                   sharedOpen,
	"Name":                         sharedOpen,
	"NetworkAliases":               sharedOpen,
	"Networks":                     sharedOpen,
	"PortEndpoint":                 sharedOpen,
	"Ports":                        sharedOpen,
	"SessionID":                    sharedOpen,
	"StartLogProducer":             sharedOpen,
	"State":                        sharedOpen,
	"StopLogProducer":              sharedOpen,

	// Client identities and native commands, scoped by Tenant.
	"CreateClient":               sharedTenant,
	"RefreshClientMonitorConfig": sharedTenant,
	"RunNativeCommand":           sharedTenant,
	"WithClientIdentity":         sharedTenant,

	// Cluster-wide changes.
	"DeepScrubPG":             sharedExclusive,
	"CreatePool":              sharedExclusive,
	"RefreshMonitorConfig":    sharedExclusive,
	"RepairPG":                sharedExclusive,
	"TemporaryBlocklist":      sharedExclusive,
	"OverrideConfig":          sharedExclusive,
	"OverrideDebugLevel":      sharedExclusive,
	"OverrideFullRatios":      sharedExclusive,
	"OverrideMGRModule":       sharedExclusive,
	"OverrideMessengerPolicy": sharedExclusive,
	"OverrideOSDFlag":         sharedExclusive,
	// Raw access: the admin CLI and keyring, and daemon container handles.
	"Ceph":              sharedExclusive,
	"ConnectionConfig":  sharedExclusive,
	"ControlContainer":  sharedExclusive,
	"ManagerContainer":  sharedExclusive,
	"Managers":          sharedExclusive,
	"Monitors":          sharedExclusive,
	"OSDs":              sharedExclusive,
	"ServiceContainers": sharedExclusive,
	"WithClient":        sharedExclusive,
	// Promoted from the primary MON's container and overridden below.
	"CopyDirToContainer":    sharedExclusive,
	"CopyFileFromContainer": sharedExclusive,
	"CopyFileToContainer":   sharedExclusive,
	"CopyToContainer":       sharedExclusive,
	"Exec":                  sharedExclusive,
	"Start":                 sharedExclusive,
	"Stop":                  sharedExclusive,

	// Topology changes and injected damage.
	"AddManager":       sharedRecycle,
	"AddMonitor":       sharedRecycle,
	"AddOSD":           sharedRecycle,
	"AddOSDWithConfig": sharedRecycle,

	"Terminate":          sharedRefused,
	"TerminateOnCleanup": sharedRefused,
}

// sharingMode is how a SharedCluster lends a cluster.
type sharingMode int32

const (
	// notShared is a cluster of its own test.
	notShared sharingMode = iota
	// sharedWithTenants refuses every method that is not sharedOpen.
	sharedWithTenants
	// sharedExclusively lends the cluster to one exclusive lease, which may
	// use every method but those that end the cluster.
	sharedExclusively
)

// sharedGate refuses method while a SharedCluster shares c. Every method
// that sharedMethodPolicies does not mark sharedOpen calls it first; the
// module's own operations use unexported paths that it does not guard.
func (c *Container) sharedGate(method string) error {
	return c.admitShared(method, sharedMethodPolicies[method])
}

// sharedFaultPolicies classifies the fault operations by the name that
// internal/faulthook admits them with. Each keeps the policy of the
// *Container method that implements it today, so moving an operation out of
// package ceph does not move it out of the gate.
var sharedFaultPolicies = map[string]sharedPolicy{
	"fault.ApplyNetworkProfile":   sharedExclusive,
	"fault.InterruptNetwork":      sharedExclusive,
	"fault.Pause":                 sharedExclusive,
	"fault.InjectObjectDataError": sharedRecycle,
}

// admitFault refuses the fault operation op while a SharedCluster shares c
// as sharedGate refuses its method. An operation without a policy is
// refused even on a cluster of its own, so it cannot run unclassified.
func (c *Container) admitFault(op string) error {
	policy, ok := sharedFaultPolicies[op]
	if !ok {
		return invalidRequest("fault operation %q has no shared-cluster policy", op)
	}
	return c.admitShared(op, policy)
}

// sharedHandlePolicies classifies the operations that live on handles by the
// name that admitHandle admits them with. Each keeps the policy of the
// *Container method that it replaced.
var sharedHandlePolicies = map[string]sharedPolicy{
	"OSDContainer.Remove":                sharedRecycle,
	"OSDContainer.Detach":                sharedRecycle,
	"DetachedOSD.Reopen":                 sharedRecycle,
	"OSDContainer.SetIn":                 sharedExclusive,
	"OSDContainer.AdminSocket":           sharedExclusive,
	"OSDContainer.RotateKey":             sharedExclusive,
	"MonitorContainer.Remove":            sharedRecycle,
	"MonitorContainer.AdminSocket":       sharedExclusive,
	"ManagerContainer.Remove":            sharedRecycle,
	"ManagerContainer.AdminSocket":       sharedExclusive,
	"ManagerContainer.RotateKey":         sharedExclusive,
	"Pool.Remove":                        sharedExclusive,
	"Pool.SetPGCount":                    sharedExclusive,
	"Pool.SetPlacement":                  sharedExclusive,
	"Pool.SetQuota":                      sharedExclusive,
	"Pool.SetReplication":                sharedExclusive,
	"Pool.Status":                        sharedOpen,
	"Pool.Usage":                         sharedOpen,
	"Pool.PGs":                           sharedOpen,
	"Pool.WaitForPGCount":                sharedOpen,
	"ClientConfig.Capabilities":          sharedTenant,
	"ClientConfig.UpdateCaps":            sharedTenant,
	"ClientConfig.ReplaceKey":            sharedTenant,
	"ClientConfig.Revoke":                sharedTenant,
	"ClientConfig.Delete":                sharedTenant,
	"Observer.Status":                    sharedOpen,
	"Observer.ManagerStatus":             sharedOpen,
	"Observer.DaemonAddresses":           sharedOpen,
	"Observer.Configuration":             sharedOpen,
	"Observer.BlocklistEntries":          sharedOpen,
	"Observer.MGRModules":                sharedOpen,
	"Observer.OSDFlags":                  sharedOpen,
	"Observer.OSDStates":                 sharedOpen,
	"Observer.ManagerServices":           sharedOpen,
	"Observer.QuorumStatus":              sharedOpen,
	"Observer.MonitorBootstrapAddresses": sharedOpen,
	"Observer.HealthDetails":             sharedOpen,
	"Observer.FullRatios":                sharedOpen,
}

// admitHandle refuses the handle operation op while a SharedCluster shares c,
// as sharedGate refuses a method of *Container. An operation without a policy
// is refused even on a cluster of its own, so it cannot run unclassified.
func (c *Container) admitHandle(op string) error {
	policy, ok := sharedHandlePolicies[op]
	if !ok {
		return invalidRequest("handle operation %q has no shared-cluster policy", op)
	}
	return c.admitShared(op, policy)
}

func (c *Container) admitShared(method string, policy sharedPolicy) error {
	if c == nil {
		return nil
	}
	mode := sharingMode(c.sharing.Load())
	switch {
	case mode == notShared, policy == sharedOpen:
		return nil
	case mode == sharedExclusively && policy != sharedRefused:
		return nil
	}
	return &OperationError{Kind: ErrSharedCluster, Op: method, Err: fmt.Errorf("%s: it %s", ErrSharedCluster, policy.reason())}
}

// Exec runs a command in the primary MON container. A shared cluster
// refuses it.
func (c *Container) Exec(ctx context.Context, cmd []string, options ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := c.sharedGate("Exec"); err != nil {
		return 0, nil, err
	}
	return c.Container.Exec(ctx, cmd, options...)
}

// CopyToContainer copies content into the primary MON container. A shared
// cluster refuses it.
func (c *Container) CopyToContainer(ctx context.Context, fileContent []byte, containerFilePath string, fileMode int64) error {
	if err := c.sharedGate("CopyToContainer"); err != nil {
		return err
	}
	return c.Container.CopyToContainer(ctx, fileContent, containerFilePath, fileMode)
}

// CopyDirToContainer copies a host directory into the primary MON
// container. A shared cluster refuses it.
func (c *Container) CopyDirToContainer(ctx context.Context, hostDirPath string, containerParentPath string, fileMode int64) error {
	if err := c.sharedGate("CopyDirToContainer"); err != nil {
		return err
	}
	return c.Container.CopyDirToContainer(ctx, hostDirPath, containerParentPath, fileMode)
}

// CopyFileToContainer copies a host file into the primary MON container. A
// shared cluster refuses it.
func (c *Container) CopyFileToContainer(ctx context.Context, hostFilePath string, containerFilePath string, fileMode int64) error {
	if err := c.sharedGate("CopyFileToContainer"); err != nil {
		return err
	}
	return c.Container.CopyFileToContainer(ctx, hostFilePath, containerFilePath, fileMode)
}

// CopyFileFromContainer reads a file of the primary MON container, which
// holds the admin keyring. A shared cluster refuses it.
func (c *Container) CopyFileFromContainer(ctx context.Context, filePath string) (io.ReadCloser, error) {
	if err := c.sharedGate("CopyFileFromContainer"); err != nil {
		return nil, err
	}
	return c.Container.CopyFileFromContainer(ctx, filePath)
}

// Start starts the primary MON container. A shared cluster refuses it.
func (c *Container) Start(ctx context.Context) error {
	if err := c.sharedGate("Start"); err != nil {
		return err
	}
	return c.Container.Start(ctx)
}

// Stop stops the primary MON container. A shared cluster refuses it.
func (c *Container) Stop(ctx context.Context, timeout *time.Duration) error {
	if err := c.sharedGate("Stop"); err != nil {
		return err
	}
	return c.Container.Stop(ctx, timeout)
}

var _ testcontainers.Container = (*Container)(nil)
