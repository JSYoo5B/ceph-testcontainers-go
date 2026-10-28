package catalogue

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
)

// State is the committed bytes of one package's three catalogue files at one
// revision. A nil slice means the file does not exist there.
type State struct {
	Declarations []byte
	Members      []byte
	Receipt      []byte
}

// Reader returns the content of a repository path at a revision. A path that
// does not exist there is (nil, nil).
type Reader func(rev, path string) ([]byte, error)

// ReadState loads one package's three files at a revision.
func ReadState(read Reader, rev, pkg string) (State, error) {
	var state State
	for _, file := range []struct {
		name string
		into *[]byte
	}{{DeclarationsFile, &state.Declarations}, {MembersFile, &state.Members}, {ReceiptFile, &state.Receipt}} {
		data, err := read(rev, Dir+"/"+pkg+"/"+file.name)
		if err != nil {
			return State{}, err
		}
		*file.into = data
	}
	return state, nil
}

func decodeStrict(data []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return err
	}
	return requireEOF(dec)
}

type parsed struct {
	declarations Declarations
	members      Members
	receipt      Receipt
}

func parseState(pkg string, state State, needReceipt bool) (parsed, error) {
	var p parsed
	for _, file := range []struct {
		name string
		data []byte
		into any
		need bool
	}{{DeclarationsFile, state.Declarations, &p.declarations, true}, {MembersFile, state.Members, &p.members, true}, {ReceiptFile, state.Receipt, &p.receipt, needReceipt}} {
		if file.data == nil {
			if file.need {
				return p, fmt.Errorf("%s is missing", file.name)
			}
			continue
		}
		if err := decodeStrict(file.data, file.into); err != nil {
			return p, fmt.Errorf("%s: %w", file.name, err)
		}
	}
	if p.declarations.Schema != DeclarationsSchema || p.declarations.Package != pkg || p.declarations.Count != len(p.declarations.Entries) {
		return p, fmt.Errorf("%s is not the v2 declarations catalogue of %s", DeclarationsFile, pkg)
	}
	if p.members.Schema != MembersSchema || p.members.Package != pkg {
		return p, fmt.Errorf("%s is not the v2 members catalogue of %s", MembersFile, pkg)
	}
	return p, nil
}

// requireApproved requires a side's receipt to pin exactly the catalogue
// bytes committed beside it, with matching counts. This is the property that
// makes a parent an approved state; it does not re-derive the surface from
// source (that was the parent's own check).
func requireApproved(pkg string, state State, p parsed) error {
	r := p.receipt
	if r.Schema != ReceiptSchema || r.Package != pkg {
		return fmt.Errorf("%s is not the v2 receipt of %s", ReceiptFile, pkg)
	}
	if r.DeclarationsSHA256 != fmt.Sprintf("%x", sha256.Sum256(state.Declarations)) ||
		r.MembersSHA256 != fmt.Sprintf("%x", sha256.Sum256(state.Members)) {
		return errors.New("the receipt does not pin the committed catalogue bytes, so this state was never approved")
	}
	if r.Declarations != p.declarations.Count || r.Members != p.members.Count {
		return errors.New("the receipt counts do not match the committed catalogue")
	}
	return nil
}

// rows maps a stable row identity to the row's exact content.
func declarationRows(d Declarations) (map[string]Entry, error) {
	rows := make(map[string]Entry, len(d.Entries))
	for _, e := range d.Entries {
		if _, dup := rows[e.Current]; dup {
			return nil, fmt.Errorf("declaration %s appears twice", e.Current)
		}
		rows[e.Current] = e
	}
	return rows, nil
}

// memberKey is the stable identity of a member row. A row reads
// "<Type>.<path> field|method <value>" or "<Type> embed <Type>". The identity
// of a field or method is its path, so changing its type, tag or signature is
// an edit of one row and not a removal plus an addition. An embedding has no
// separate value: the embedded type is its identity.
func memberKey(row string) (key, value string) {
	path, rest, _ := strings.Cut(row, " ")
	kind, _, _ := strings.Cut(rest, " ")
	if kind == "embed" || kind == "" {
		return row, row
	}
	return path, row
}

func memberLabel(key string) string {
	_, label, _ := strings.Cut(key, "\x00")
	return label
}

// memberRows maps (member path or embedding) to the full row value.
func memberRows(m Members) (map[string]string, error) {
	rows := map[string]string{}
	for typ, members := range m.Types {
		for _, member := range members {
			key, value := memberKey(member)
			if prior, dup := rows[typ+"\x00"+key]; dup {
				return nil, fmt.Errorf("member %q of %s appears twice (%q)", key, typ, prior)
			}
			rows[typ+"\x00"+key] = value
		}
	}
	return rows, nil
}

// merge3 applies the three-way rule to one row: equal on both sides keeps it,
// a side that left the row as in the base yields to the other side, and two
// different changes are a conflict.
func merge3[V comparable](base, ours, theirs V, hb, ho, ht bool) (V, bool, bool) {
	same := func(a, b V, ha, hb bool) bool { return ha == hb && (!ha || a == b) }
	switch {
	case same(ours, theirs, ho, ht):
		return ours, ho, true
	case same(ours, base, ho, hb):
		return theirs, ht, true
	case same(theirs, base, ht, hb):
		return ours, ho, true
	}
	var zero V
	return zero, false, false
}

// Composition is the row set that two approved parents jointly allow for a
// merged package, and the receipt fields that follow from them.
type Composition struct {
	Declarations map[string]Entry
	Members      map[string]string
	Review, Page string
	// OursOnly counts composed declaration rows the theirs side does not have
	// in that exact form, and TheirsOnly the same for ours.
	OursOnly, TheirsOnly int
}

// Compose computes the three-way merge of the rows of two approved parents.
// Both ours and theirs must pin their own committed catalogue bytes in their
// receipt. A row that both sides changed differently, a row the merge result
// would have to invent, and receipt fields that both sides changed differently
// are refused. Compose reads no source and writes nothing.
func Compose(pkg string, base, ours, theirs State) (Composition, error) {
	var comp Composition
	pb, err := parseState(pkg, base, false)
	if err != nil {
		return comp, fmt.Errorf("merge base: %w", err)
	}
	po, err := parseState(pkg, ours, true)
	if err != nil {
		return comp, fmt.Errorf("ours: %w", err)
	}
	pt, err := parseState(pkg, theirs, true)
	if err != nil {
		return comp, fmt.Errorf("theirs: %w", err)
	}
	if err := requireApproved(pkg, ours, po); err != nil {
		return comp, fmt.Errorf("ours: %w", err)
	}
	if err := requireApproved(pkg, theirs, pt); err != nil {
		return comp, fmt.Errorf("theirs: %w", err)
	}

	db, e1 := declarationRows(pb.declarations)
	do, e2 := declarationRows(po.declarations)
	dt, e3 := declarationRows(pt.declarations)
	if err := errors.Join(e1, e2, e3); err != nil {
		return comp, err
	}
	comp.Declarations = map[string]Entry{}
	var conflicts []string
	keys := map[string]bool{}
	for _, m := range []map[string]Entry{db, do, dt} {
		for key := range m {
			keys[key] = true
		}
	}
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		b, hb := db[key]
		o, ho := do[key]
		t, ht := dt[key]
		row, present, ok := merge3(b, o, t, hb, ho, ht)
		switch {
		case !ok:
			conflicts = append(conflicts, "declaration "+key)
		case present:
			comp.Declarations[key] = row
			if !ht || t != row {
				comp.OursOnly++
			}
			if !ho || o != row {
				comp.TheirsOnly++
			}
		}
	}

	mb, e1 := memberRows(pb.members)
	mo, e2 := memberRows(po.members)
	mt, e3 := memberRows(pt.members)
	if err := errors.Join(e1, e2, e3); err != nil {
		return comp, err
	}
	comp.Members = map[string]string{}
	keys = map[string]bool{}
	for _, m := range []map[string]string{mb, mo, mt} {
		for key := range m {
			keys[key] = true
		}
	}
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		b, hb := mb[key]
		o, ho := mo[key]
		t, ht := mt[key]
		row, present, ok := merge3(b, o, t, hb, ho, ht)
		switch {
		case !ok:
			conflicts = append(conflicts, "member "+memberLabel(key))
		case present:
			comp.Members[key] = row
		}
	}

	pickText := func(name string, base, ours, theirs string, haveBase bool) string {
		switch {
		case ours == theirs:
			return ours
		case haveBase && ours == base:
			return theirs
		case haveBase && theirs == base:
			return ours
		}
		conflicts = append(conflicts, "receipt field "+name)
		return ""
	}
	haveBase := pb.receipt.Schema != ""
	comp.Review = pickText("review", pb.receipt.Review, po.receipt.Review, pt.receipt.Review, haveBase)
	comp.Page = pickText("page", pb.receipt.Page, po.receipt.Page, pt.receipt.Page, haveBase)
	if len(conflicts) > 0 {
		return comp, fmt.Errorf("%s: both parents changed these differently, so the combination is a new decision that needs a human review and approve: %s", pkg, strings.Join(conflicts, ", "))
	}
	return comp, nil
}

// ProveComposition requires the source-derived surface of the merged tree to
// consist of exactly the composed rows: no row that neither approved parent
// contains, and no approved row missing. The first difference found of each
// kind is reported.
func ProveComposition(pkg string, comp Composition, surface Surface) error {
	var problems []error
	derived, err := declarationRows(surface.Declarations)
	if err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(derived)) {
		want, ok := comp.Declarations[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Errorf("merged source has declaration %s that neither approved parent allows", key))
		case want != derived[key]:
			problems = append(problems, fmt.Errorf("merged source changes declaration %s to a form neither approved parent has", key))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(comp.Declarations)) {
		if _, ok := derived[key]; !ok {
			problems = append(problems, fmt.Errorf("merged source lacks declaration %s that the approved parents keep", key))
		}
	}
	members, err := memberRows(surface.Members)
	if err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(members)) {
		want, ok := comp.Members[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Errorf("merged source has member %s that neither approved parent allows", memberLabel(key)))
		case want != members[key]:
			problems = append(problems, fmt.Errorf("merged source changes member %s to a form neither approved parent has", memberLabel(key)))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(comp.Members)) {
		if _, ok := members[key]; !ok {
			problems = append(problems, fmt.Errorf("merged source lacks member %s that the approved parents keep", memberLabel(key)))
		}
	}
	if err := errors.Join(problems...); err != nil {
		return fmt.Errorf("%s: the merge is not a pure composition of the approved parents; review the API change and run regenerate and approve: %w", pkg, err)
	}
	return nil
}

// ResolveOptions names the three revisions of a merge.
type ResolveOptions struct {
	Base, Ours, Theirs string
	Read               Reader
}

// Resolution is the outcome for one package.
type Resolution struct {
	Package string
	Comp    Composition
	Files   []string
}

// Resolve re-derives the catalogue and receipt of every named package (by
// default every package that both parents changed) after a merge, but only
// when the merged source surface is a pure composition of the two approved
// parents (see Compose and ProveComposition). It writes declarations.json,
// members.json and review.json together and writes nothing at all when any
// selected package is refused. Unlike Approve it needs no prior catalogue to
// equal the source: the committed working-tree files may still carry merge
// conflict markers.
func Resolve(c Config, opts ResolveOptions, named []string) ([]Resolution, error) {
	discovered, err := Discover(c.Root)
	if err != nil {
		return nil, err
	}
	var selected []string
	states := map[string][3]State{}
	load := func(pkg string) error {
		var trio [3]State
		for i, rev := range []string{opts.Base, opts.Ours, opts.Theirs} {
			if trio[i], err = ReadState(opts.Read, rev, pkg); err != nil {
				return err
			}
		}
		states[pkg] = trio
		return nil
	}
	if len(named) == 0 {
		for _, pkg := range discovered {
			if err := load(pkg); err != nil {
				return nil, err
			}
			if changed(states[pkg][0], states[pkg][1]) && changed(states[pkg][0], states[pkg][2]) {
				selected = append(selected, pkg)
			}
		}
	} else {
		if selected, err = selectPackages(discovered, named); err != nil {
			return nil, err
		}
		for _, pkg := range selected {
			if err := load(pkg); err != nil {
				return nil, err
			}
		}
	}
	if len(selected) == 0 {
		return nil, nil
	}
	surfaces, err := Generate(c, discovered)
	if err != nil {
		return nil, fmt.Errorf("the merged source does not derive a catalogue: %w", err)
	}
	var problems []error
	var out []Resolution
	files := map[string]map[string][]byte{}
	for _, pkg := range selected {
		trio := states[pkg]
		comp, err := Compose(pkg, trio[0], trio[1], trio[2])
		if err == nil {
			err = ProveComposition(pkg, comp, surfaces[pkg])
		}
		if err == nil {
			for _, entry := range surfaces[pkg].Declarations.Entries {
				if entry.Decision == "proposal" {
					err = fmt.Errorf("%s: pending proposal %s blocks any approval", pkg, entry.Current)
					break
				}
			}
		}
		if err != nil {
			problems = append(problems, err)
			continue
		}
		decl, err1 := Marshal(surfaces[pkg].Declarations)
		mem, err2 := Marshal(surfaces[pkg].Members)
		dh, err3 := digest(surfaces[pkg].Declarations)
		mh, err4 := digest(surfaces[pkg].Members)
		if err := errors.Join(err1, err2, err3, err4); err != nil {
			return nil, err
		}
		receipt := Receipt{
			Schema: ReceiptSchema, Package: pkg, DeclarationsSHA256: dh, MembersSHA256: mh,
			Declarations: surfaces[pkg].Declarations.Count, Members: surfaces[pkg].Members.Count,
			Review: comp.Review, Page: comp.Page,
		}
		if err := checkReferences(c.Root, receipt); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", pkg, err))
			continue
		}
		rec, err := Marshal(receipt)
		if err != nil {
			return nil, err
		}
		files[pkg] = map[string][]byte{DeclarationsFile: decl, MembersFile: mem, ReceiptFile: rec}
		out = append(out, Resolution{Package: pkg, Comp: comp})
	}
	if err := errors.Join(problems...); err != nil {
		return nil, err
	}
	for i, res := range out {
		for _, name := range []string{DeclarationsFile, MembersFile, ReceiptFile} {
			if err := writeFile(filepath.Join(PackageDir(c.Root, res.Package), name), files[res.Package][name]); err != nil {
				return out, err
			}
			out[i].Files = append(out[i].Files, Dir+"/"+res.Package+"/"+name)
		}
	}
	return out, nil
}

func changed(base, side State) bool {
	return !bytes.Equal(base.Declarations, side.Declarations) || !bytes.Equal(base.Members, side.Members) || !bytes.Equal(base.Receipt, side.Receipt)
}
