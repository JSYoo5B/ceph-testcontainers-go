package ceph

import (
	"go/ast"
	"go/token"
	"slices"
	"testing"
)

func TestDaemonKeyringsRefusesWhatItCannotResolve(t *testing.T) {
	c, control := adminSocketCluster()
	topology := c.monitorTopology()
	for _, ref := range [][2]string{{"mgr", "a"}, {"osd", "0"}} {
		ctr, keyrings, err := topology.daemonKeyrings(ref[0], ref[1])
		want := []string{"/etc/ceph/" + ref[0] + ".keyring", "/var/lib/ceph/" + ref[0] + "/ceph-" + ref[1] + "/keyring"}
		if err != nil || ctr != control || !slices.Equal(keyrings, want) {
			t.Errorf("%s.%s = %v, %q, %v", ref[0], ref[1], ctr, keyrings, err)
		}
	}
	for _, ref := range [][2]string{{"mon", "a"}, {"mds", "a"}, {"osd", "x"}, {"osd", "7"}, {"mgr", "z"}} {
		if ctr, keyrings, err := topology.daemonKeyrings(ref[0], ref[1]); err == nil || ctr != nil || keyrings != nil {
			t.Errorf("%s.%s = %v, %q, %v; want an error", ref[0], ref[1], ctr, keyrings, err)
		}
	}
}

// monitorTopologyBinding reports whether fn is Container.monitorTopology, the
// one function that hands the membership state's addresses to the topology.
// The roster and bootstrap field guards admit its uses there;
// TestMonitorTopologyBindsEachStateOnce pins what it does.
func monitorTopologyBinding(fn *ast.FuncDecl) bool {
	return fn != nil && receiverTypeName(fn) == "Container" && fn.Name.Name == "monitorTopology"
}

// inMonitorTopologyBinding reports whether pos lies in Container.monitorTopology.
func inMonitorTopologyBinding(file *ast.File, pos token.Pos) bool {
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && monitorTopologyBinding(fn) && fn.Pos() <= pos && pos < fn.End() {
			return true
		}
	}
	return false
}

// monitorTopologyBound is the state that Container.monitorTopology binds by
// address: the topology's own state (membershipState, which Container embeds:
// the roster and the first-MON flag) and the bootstrap record. The roster and
// the bootstrap record keep their field guards, which also hold for the
// topology's methods because they use the same field names;
// monitorTerminated is audited by TestLifecycleFlagsHaveAuditedWritersAndReaders.
var monitorTopologyBound = []string{"bootstrap", "membershipState"}

// TestMonitorTopologyBindsEachStateOnce guards the topology's construction
// and back reference: Container.monitorTopology is a single return of one
// monitorTopology literal that binds each state in monitorTopologyBound once,
// as &c.<same field>; no other function builds a monitorTopology; and its
// methods use owner only as the owner of a MON or MGR handle they create.
func TestMonitorTopologyBindsEachStateOnce(t *testing.T) {
	bindingAudit{typeName: "monitorTopology", constructor: monitorTopologyBinding, bound: monitorTopologyBound, singleReturn: true,
		recv: "t", handles: []string{"MonitorContainer", "ManagerContainer"}}.check(t)
}
