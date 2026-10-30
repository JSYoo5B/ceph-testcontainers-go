package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

// terminateSteps are the per-owner cleanup steps that Container.terminate
// calls, each as the statement that calls it, with the file that declares it,
// its receiver and how that receiver reads from terminate's c.
var terminateSteps = []struct {
	call, file, decl string
	rewrite          [][2]string
}{
	{
		call: "errs = append(errs, c.monitorTopology().terminateServices(ctx, opts)...)",
		file: "monitor_topology.go", decl: "func (t monitorTopology) terminateServices(",
		rewrite: [][2]string{{`\bt\.roster\b`, "c.members.roster"}},
	},
	{
		call: "errs = append(errs, c.osdFleet().terminateOSDs(ctx, opts)...)",
		file: "osd_fleet.go", decl: "func (f osdFleet) terminateOSDs(",
		rewrite: [][2]string{{`\bf\.`, "c.fleet."}},
	},
	{
		call: "errs = append(errs, c.monitorTopology().terminateMembers(ctx, opts)...)",
		file: "monitor_topology.go", decl: "func (t monitorTopology) terminateMembers(",
		rewrite: [][2]string{{`\bt\.roster\b`, "c.members.roster"}, {`\bt\.first\b`, "c.Container"}, {`\bt\.`, "c.monitorTopology()."}},
	},
}

// stepBody returns the statements of a terminate step, without its local
// error slice and its return, with its receiver rewritten to terminate's c.
func stepBody(t *testing.T, src, decl string, rewrite [][2]string) string {
	t.Helper()
	at := strings.Index(src, decl)
	if at < 0 {
		t.Fatalf("terminate step %s not found", decl)
	}
	open := strings.Index(src[at:], "{\n") + at + 2
	end := strings.Index(src[open:], "\n}\n") + open
	body := strings.TrimPrefix(src[open:end], "\tvar errs []error\n")
	body = strings.TrimSuffix(body, "\treturn errs")
	if strings.Contains(body, "var errs") || strings.Contains(body, "return errs") {
		t.Fatalf("terminate step %s must collect into errs and return it once at the end", decl)
	}
	for _, r := range rewrite {
		body = regexp.MustCompile(r[0]).ReplaceAllString(body, r[1])
	}
	return body
}

// parseProductionFilesWithTerminateSteps is parseProductionFiles with
// Container.terminate replaced by its expansion: each per-owner step call is
// replaced, in place, by that step's statements, so the order guards see the
// whole cleanup sequence of terminate across its steps. Each step must be
// called exactly once, by terminate.
func parseProductionFilesWithTerminateSteps(t *testing.T) map[string]*ast.File {
	t.Helper()
	files := parseProductionFiles(t)
	read := func(name string) string {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	src := read("ceph.go")
	at := strings.Index(src, "func (c *Container) terminate(")
	if at < 0 {
		t.Fatal("Container.terminate not found")
	}
	end := strings.Index(src[at:], "\n}\n") + at + 3
	fn := src[at:end]
	for _, step := range terminateSteps {
		if strings.Count(fn, step.call) != 1 || strings.Count(src, step.call) != 1 {
			t.Fatalf("terminate must call %q exactly once", step.call)
		}
		fn = strings.Replace(fn, "\t"+step.call+"\n", stepBody(t, read(step.file), step.decl, step.rewrite), 1)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "terminate_expanded.go", "package ceph\n\n"+fn, 0)
	if err != nil {
		t.Fatalf("expanded terminate does not parse: %v", err)
	}
	for _, decl := range files["ceph.go"].Decls {
		if f, ok := decl.(*ast.FuncDecl); ok && funcKey(f) == "Container.terminate" {
			*f = *parsed.Decls[0].(*ast.FuncDecl)
			return files
		}
	}
	t.Fatal("Container.terminate not found")
	return nil
}

// TestTerminateStepsAreCalledOnlyByTerminate guards that the per-owner
// cleanup steps run only inside terminate, under its Container.mu.
func TestTerminateStepsAreCalledOnlyByTerminate(t *testing.T) {
	steps := map[string]bool{"terminateServices": true, "terminateOSDs": true, "terminateMembers": true}
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && steps[sel.Sel.Name] && funcKey(fn) != "Container.terminate" {
					t.Errorf("%s: %s calls the terminate step %s", file, funcKey(fn), sel.Sel.Name)
				}
				return true
			})
		}
	}
	parseProductionFilesWithTerminateSteps(t)
}
