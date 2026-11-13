#!/usr/bin/env python3
"""Reproduction catalogues of known-defect experiment cases.

Each case that starts reproduction units (harness.StartReproduction) keeps a
reproductions.json beside its tests: the image roles it consumes, its units
with their network mode, and the image combinations whose verdicts a native
run verified. The Go test selects the expected verdict of a unit from the
combination whose images equal the ones it resolved; this module validates
the catalogues and lets the tag runner require, for a run whose images match
a combination, each expected unit's own REPRODUCTION_VERDICT line, so a skip
or a missing line cannot pass.
"""

import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS = ROOT / "internal/integration/experiments"
CATALOG = "reproductions.json"
SCHEMA = "ceph-reproductions/v1"
ROLES = ("essential", "client", "cephfs", "rbd-aux", "rgw")
VERDICTS = ("REPRODUCED", "NOT_REPRODUCED")
NETWORKS = ("bridge", "host")
# The same syntax as harness.ParseReproductionCatalog.
UNIT = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*(/[a-z0-9]+([_-][a-z0-9]+)*)?\Z")
COMBINATION = re.compile(r"[a-z0-9]+([.-][a-z0-9]+)*\Z")
PLATFORM = re.compile(r"[a-z0-9]+/[a-z0-9]+\Z")
# An expectation is stored only for bytes that cannot change: a local image
# ID, or a reference with a sha256 digest (a tag beside it is kept). The name
# grammar is the distribution reference grammar (v0.6.0) that the harness
# parses with, in ASCII classes as Go's regexp reads \w (Python's \w also
# matches non-ASCII letters and digits); testdata/reproduction_images.json
# holds the cases both check.
_PATH_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
_DOMAIN_COMPONENT = r"(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])"
_HOST = r"(?:%s(?:\.%s)*|\[[a-fA-F0-9:]+\])" % (_DOMAIN_COMPONENT, _DOMAIN_COMPONENT)
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
DIGEST_REFERENCE = re.compile(r"(?:%s(?::[0-9]+)?/)?(?P<path>%s(?:/%s)*)(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?@sha256:[0-9a-f]{64}\Z"
                              % (_HOST, _PATH_COMPONENT, _PATH_COMPONENT))
# The grammar limits the repository path, without the registry host.
PATH_MAX = 255


def immutable_image(image):
    """Whether image names bytes that cannot change, as the harness's
    immutableImage decides."""
    if not isinstance(image, str):
        return False
    if IMAGE_ID.match(image):
        return True
    match = DIGEST_REFERENCE.match(image)
    return bool(match) and len(match["path"]) <= PATH_MAX


IMAGE_ROLES = re.compile(r"\bimage-roles=(\S+)")
DEFAULT_IMAGE = re.compile(r'^const DefaultImage = "([^"]+)"$', re.MULTILINE)
LINE = re.compile(r"REPRODUCTION_VERDICT case=(\S+) verdict=(\S+) expected=(\S+) combination=(\S+) attempts=(\d+) "
                  r"platform=(\S+) network=(\S+) (.*)$")
ROLE_VARIABLES = {"essential": "CEPH_TEST_ESSENTIAL_IMAGE", "client": "CEPH_TEST_CLIENT_IMAGE",
                  "cephfs": "CEPH_TEST_CEPHFS_IMAGE", "rbd-aux": "CEPH_TEST_RBD_AUX_IMAGE", "rgw": "CEPH_TEST_RGW_IMAGE"}


def package_files(catalog, package):
    """The test files of package in the tagcatalog catalog, whatever their tags."""
    return [file for file in catalog if Path(file["path"]).parent == Path(package)]


def started_units(files, tests):
    """The units that the named top-level tests start. files are the
    tagcatalog records of the test files compiled into the run; their
    reproductions say, per package-level declaration, the units its code
    starts and the names it uses (read with go/ast, so comments and string
    contents never count). A declaration reaches the functions and variables
    that it names and the methods that it selects by name; a test also reaches
    the methods of the testify suites that it runs. The walk follows them
    transitively."""
    declarations, methods, receivers, suites = {}, {}, {}, set()
    for file in files:
        for key, reach in file.get("reproductions", {}).items():
            merged = declarations.setdefault(key, {"starts": set(), "names": set(), "selectors": set()})
            for field, values in merged.items():
                values.update(reach[field])
            if "." in key:
                receiver, method = key.split(".", 1)
                methods.setdefault(method, set()).add(key)
                receivers.setdefault(receiver, set()).add(key)
        for test in tests:
            suites.update(file.get("suite_runs", {}).get(test, ()))
    pending = [test for test in tests if test in declarations]
    pending += [method for suite in suites for method in receivers.get(suite, ())]
    reached = set()
    while pending:
        key = pending.pop()
        if key in reached:
            continue
        reached.add(key)
        reach = declarations[key]
        # A method key has a dot, which no identifier has.
        pending += [name for name in reach["names"] if name in declarations]
        pending += [method for name in reach["selectors"] for method in methods.get(name, ())]
    return {unit for key in reached for unit in declarations[key]["starts"]}


def source_units(catalog, package):
    """The units that any test file of the package starts."""
    return {unit for file in package_files(catalog, package)
            for reach in file.get("reproductions", {}).values() for unit in reach["starts"]}


def validate(document, label=CATALOG):
    """A catalogue that a run can use to select one expectation; the same
    rules as harness.ParseReproductionCatalog."""
    if not isinstance(document, dict) or set(document) != {"schema", "roles", "units", "combinations"}:
        raise ValueError(label + ": fields must be schema, roles, units and combinations")
    if document["schema"] != SCHEMA:
        raise ValueError("%s: schema %r, want %r" % (label, document["schema"], SCHEMA))
    roles = document["roles"]
    if not roles or len(set(roles)) != len(roles) or not set(roles) <= set(ROLES):
        raise ValueError("%s: roles %r must be distinct roles of %r" % (label, roles, ROLES))
    units = document["units"]
    if not units:
        raise ValueError(label + ": no units")
    for name, unit in units.items():
        if not UNIT.match(name) or unit != {"network": unit.get("network")} or unit["network"] not in NETWORKS:
            raise ValueError("%s: unit %s must be <case>[/<subtest>] with network bridge or host" % (label, name))
    ids, identities = set(), set()
    for combination in document["combinations"]:
        if set(combination) != {"id", "platform", "images", "verdicts", "evidence"}:
            raise ValueError("%s: combination %r has other fields than id, platform, images, verdicts, evidence"
                             % (label, combination.get("id")))
        name = combination["id"]
        if not COMBINATION.match(name) or name in ids:
            raise ValueError("%s: combination id %r must be unique and lowercase" % (label, name))
        ids.add(name)
        if not PLATFORM.match(combination["platform"]):
            raise ValueError("%s: combination %s platform %r" % (label, name, combination["platform"]))
        images = combination["images"]
        if set(images) != set(roles):
            raise ValueError("%s: combination %s must name an image for exactly the roles %r" % (label, name, roles))
        for role in roles:
            if not immutable_image(images[role]):
                raise ValueError("%s: combination %s role %s image %r is not an image ID or a digest reference"
                                 % (label, name, role, images[role]))
        if not combination["verdicts"] or not combination["evidence"].strip():
            raise ValueError("%s: combination %s needs verdicts and native evidence" % (label, name))
        for unit, verdict in combination["verdicts"].items():
            if unit not in units or verdict not in VERDICTS:
                raise ValueError("%s: combination %s expects %r of %s" % (label, name, verdict, unit))
        identity = (combination["platform"], tuple(images[role] for role in roles))
        if identity in identities:
            raise ValueError("%s: combination %s repeats the images of another one" % (label, name))
        identities.add(identity)
    return document


def load(catalog, package, root=ROOT):
    """The validated catalogue of a package, or None when it starts no unit.
    A package that starts units needs a catalogue listing exactly them.
    catalog is the tagcatalog record of every test file."""
    directory = Path(root) / package
    units = source_units(catalog, package)
    path = directory / CATALOG
    if not units:
        if path.exists():
            raise ValueError("%s: the package starts no reproduction unit" % path)
        return None
    if not path.exists():
        raise ValueError("%s: the package starts %s and has no %s" % (package, sorted(units), CATALOG))
    catalog = validate(json.loads(path.read_text(encoding="utf-8")), str(path))
    if set(catalog["units"]) != units:
        raise ValueError("%s: units %s, but the tests start %s" % (path, sorted(catalog["units"]), sorted(units)))
    return catalog


def default_image(root=ROOT):
    match = DEFAULT_IMAGE.search((Path(root) / "ceph/ceph.go").read_text(encoding="utf-8"))
    if match is None:
        raise ValueError("ceph/ceph.go declares no DefaultImage")
    return match[1]


def resolved_images(environment, root=ROOT):
    """The image of each role as harness.ResolveImageSet resolves it; a role
    the role layout leaves unset is absent."""
    if environment.get("CEPH_TEST_ESSENTIAL_IMAGE") or environment.get("SCENARIO_IMAGE_LAYOUT") == "roles":
        if not environment.get("CEPH_TEST_ESSENTIAL_IMAGE"):
            return {}
        return {role: environment[variable] for role, variable in ROLE_VARIABLES.items() if environment.get(variable)}
    aggregate = environment.get("CEPH_TEST_IMAGE") or default_image(root)
    return {role: environment.get(variable) or aggregate for role, variable in ROLE_VARIABLES.items()}


def expectations(catalog, environment, platform, network=None, root=ROOT):
    """The matching combination's ID and the verdict it expects of each unit
    that runs, or (None, {}). With a network, only the units of that mode
    run (a matrix entry of a non-default network mode)."""
    if catalog is None:
        return None, {}
    images = resolved_images(environment, root)
    wanted = tuple(images.get(role) for role in catalog["roles"])
    if None in wanted:
        return None, {}
    for combination in catalog["combinations"]:
        if combination["platform"] == platform and tuple(combination["images"][role] for role in catalog["roles"]) == wanted:
            verdicts = {unit: verdict for unit, verdict in combination["verdicts"].items()
                        if network is None or catalog["units"][unit]["network"] == network}
            return combination["id"], verdicts
    return None, {}


def verdict_lines(text):
    """Every REPRODUCTION_VERDICT summary line of a verbose test log."""
    lines = []
    for raw in text.splitlines():
        index = raw.find("REPRODUCTION_VERDICT ")
        match = LINE.match(raw[index:]) if index >= 0 else None
        if match:
            lines.append({"unit": match[1], "verdict": match[2], "expected": match[3], "combination": match[4],
                          "attempts": int(match[5]), "platform": match[6], "network": match[7]})
    return lines


def check(text, combination, verdicts):
    """Whether the log meets the expectations: each expected unit reports
    exactly one line, from the same combination, with the expected verdict."""
    lines = verdict_lines(text)
    result = {"combination": combination, "expected": verdicts, "verdicts": lines, "met": True, "reasons": []}
    for unit, verdict in sorted(verdicts.items()):
        own = [line for line in lines if line["unit"] == unit]
        if len(own) != 1:
            result["reasons"].append("%d REPRODUCTION_VERDICT lines of %s" % (len(own), unit))
        elif (own[0]["verdict"], own[0]["expected"], own[0]["combination"]) != (verdict, verdict, combination):
            result["reasons"].append("expected %s of %s in %s, the line reports %s, expected %s in %s"
                                     % (verdict, unit, combination, own[0]["verdict"], own[0]["expected"],
                                        own[0]["combination"]))
    result["met"] = not result["reasons"]
    return result


def batch_roles(catalog, package):
    """The image roles that the //ci lines of a case's test files declare."""
    return {role for file in package_files(catalog, package)
            for line in file.get("metadata", ()) for role in IMAGE_ROLES.findall(line)}


def verify(catalog, root=ROOT):
    """Every catalogue of the experiment cases: its units are the ones the
    tests start and its roles are the ones its batch declares. Its
    combinations stay valid when the CI release pins change, since each names
    immutable images (validate)."""
    catalogs = {}
    for directory in sorted(Path(root, "internal/integration/experiments").iterdir()):
        if not directory.is_dir():
            continue
        package = str(directory.relative_to(root))
        catalogue = load(catalog, package, root)
        if catalogue is None:
            continue
        declared = batch_roles(catalog, package)
        if declared != {",".join(catalogue["roles"])}:
            raise ValueError("%s: roles %s, but the batch declares image-roles %s"
                             % (package, ",".join(catalogue["roles"]), sorted(declared)))
        catalogs[package] = catalogue
    return catalogs
