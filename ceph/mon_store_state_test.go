package ceph

import (
	"go/ast"
	"go/token"
	"go/types"
	"testing"
)

// monStoreMethods are the methods of monstore.State that package ceph calls;
// the package boundary keeps every other use of its fields out of ceph.
var monStoreMethods = map[string][]string{
	"Begin": nil, "Store": nil, "DiscardFresh": nil, "Retire": nil, "ReleaseStores": nil, "RetryStores": nil,
}

// monStoreCallers lists every function outside the component that calls one
// of its methods in production, and how it holds Container.mu.
var monStoreCallers = map[string]stateCaller{
	"terminate": {"takes", []string{"ReleaseStores", "RetryStores"}},
	// The first generation of a MON's store: Run before the cluster is
	// shared, AddMonitor under the topology lock.
	"startMonitorStore": {"holds", []string{"Begin", "DiscardFresh"}},
	// runMonitor starts the first MON in Run, before the cluster is shared.
	"runMonitor":          {"init", []string{"Store"}},
	"startJoiningMonitor": {"holds", nil},
	"replaceMonitor":      {"takes", []string{"Store"}},
	// A permanent MON removal retires the removed MON's record.
	"retireMonitor": {"holds", []string{"Retire"}},
}

func monStoreUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "monStores" }

func isMonStoreReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "monstore.State" }

// TestMonStoreStateFieldsAreOnlyTouchedByItsMethods guards the ownership
// written on monstore.State: outside it, Container.monStores is used only
// as the receiver of one of its methods, and the lock-free FactsOf source does
// not mention it.
func TestMonStoreStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.monStores", use: monStoreUse, own: isMonStoreReceiver, methods: monStoreMethods, wholeFactsFile: true,
		bound: monitorTopologyBinding}.check(t)
}

// TestMonStoreStateCallersHoldTheTopologyLock is the caller table guard: the
// set of production functions that use the component is exactly monStoreCallers,
// they call only the release stages, and each calls lockTopology before its
// first use. A later slice that adds a caller updates the table in its own diff.
func TestMonStoreStateCallersHoldTheTopologyLock(t *testing.T) {
	callerAudit{label: "the MON store state", use: monStoreUse, own: isMonStoreReceiver, callers: monStoreCallers}.check(t)
}

// TestTerminateReleasesMonStoresInTheirTwoStagesInOrder guards the Terminate
// order of the component: the first stage after the added and the primary MON
// containers are terminated and before the control plane, the second after the
// labeled-container removal and after the OSD retry stages, before the run
// volume.
func TestTerminateReleasesMonStoresInTheirTwoStagesInOrder(t *testing.T) {
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					at[types.ExprString(call)] = append(at[types.ExprString(call)], call.Pos())
				}
				return true
			})
			order := []string{
				"c.lockTopology(ctx)",
				"c.fleet.stores.releaseVolume(ctx, c.fleet.osdHandles.count() == 0)",
				"c.Container.Terminate(ctx, opts...)",
				"c.monStores.ReleaseStores(ctx)",
				"c.controlPlane.terminate(ctx, opts...)",
				"c.startup.removeLabeledContainers(ctx)",
				"c.fleet.stores.retryVolume(ctx, volumeErr, c.fleet.osdHandles.count() == 0)",
				"c.monStores.RetryStores(ctx, monStoreErr)",
				"c.runDir.release(ctx)",
			}
			var problems []string
			for _, name := range order {
				if len(at[name]) != 1 {
					problems = append(problems, name+" must be called exactly once in terminate")
				}
			}
			if len(problems) == 0 {
				for i := 1; i < len(order); i++ {
					if at[order[i-1]][0] >= at[order[i]][0] {
						problems = append(problems, order[i-1]+" must come before "+order[i])
					}
				}
			}
			if len(problems) != 0 {
				t.Fatalf("terminate changed the MON store release order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
