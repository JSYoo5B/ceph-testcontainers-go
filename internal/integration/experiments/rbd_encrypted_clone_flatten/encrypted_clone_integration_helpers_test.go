//go:build all || (integration && features)

package rbd_encrypted_clone_flatten_test

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"

// rbdEncryptedCloneScript characterizes native librbd encrypted clones: an
// inherited clone, a clone formatted with its own secret, flatten with the
// whole chain loaded, and the restored flattened clone.
var rbdEncryptedCloneScript = harness.RBDClientFeaturePrelude + `if phase in ["encryption-clone-flatten", "encryption-clone-restore"]:
    # Secrets are bytes on purpose: encryption_load2 converts a str secret
    # into a temporary that it frees before the load, and an equal-length
    # later secret reuses that memory, so two str secrets fail with EPERM on
    # Squid, Tentacle and Umbrella while bytes load the same chain. Do not
    # read that EPERM as unsupported encrypted clones or flatten.
    fmt, raw_size = rbd.RBD_ENCRYPTION_FORMAT_LUKS2, 64 << 20
    parent_pass, child_pass = b"temporary-fixture-parent-key", b"temporary-fixture-child-key"
    expected = patch + payload[len(patch):]
    if phase == "encryption-clone-flatten":
        with session() as (_, io):
            parent_id = create(io, "encrypted-parent", data=None, size=raw_size)
            with rbd.Image(io, "encrypted-parent") as parent:
                parent.encryption_format(fmt, parent_pass, rbd.RBD_ENCRYPTION_ALGORITHM_AES256)
            with rbd.Image(io, "encrypted-parent") as parent:
                parent.encryption_load(fmt, parent_pass)
                assert parent.write(payload, 0) == len(payload)
                parent.flush()
                parent.create_snap("baseline")
                parent.protect_snap("baseline")
            # An unformatted clone inherits the parent's encryption: the parent
            # secret alone opens it and child writes stay copy-on-write.
            api.clone(io, "encrypted-parent", "baseline", io, "inherited-child", features=features, order=20)
            with rbd.Image(io, "inherited-child") as child:
                child.encryption_load(fmt, parent_pass)
                assert child.read(0, len(payload)) == payload, "inherited clone lost parent bytes"
                assert child.write(patch, 0) == len(patch)
                child.flush()
                assert child.read(0, len(expected)) == expected
            api.remove(io, "inherited-child")
            # A separately formatted clone needs every explicitly formatted
            # ancestor's secret until it is flattened.
            api.clone(io, "encrypted-parent", "baseline", io, "encrypted-child", features=features, order=20)
            with rbd.Image(io, "encrypted-child") as child:
                child_id = child.id()
                child.encryption_format(fmt, child_pass, rbd.RBD_ENCRYPTION_ALGORITHM_AES256)
            with rbd.Image(io, "encrypted-child") as child:
                require_passphrase_denied(child, fmt, child_pass, "luks2", "child-secret-only-before-flatten")
            with rbd.Image(io, "encrypted-child") as child:
                child.encryption_load2([(fmt, child_pass), (fmt, parent_pass)])
                assert child.read(0, len(payload)) == payload, "formatted clone lost parent bytes"
                assert child.write(patch, 0) == len(patch)
                child.flush()
                # Flatten only while the whole chain is loaded. A raw flatten
                # copies parent ciphertext the child secret cannot decrypt.
                child.flatten()
                try:
                    child.parent_id()
                except rbd.ImageNotFound:
                    pass
                else:
                    raise AssertionError("flatten retained parent dependency")
        with session() as (_, io):
            with rbd.Image(io, "encrypted-child") as child:
                require_passphrase_denied(child, fmt, parent_pass, "luks2", "parent-secret-after-flatten")
            with rbd.Image(io, "encrypted-child") as child:
                child.encryption_load(fmt, child_pass)
                assert child.read(0, len(expected)) == expected, "flattened clone bytes changed"
            with rbd.Image(io, "encrypted-parent") as parent:
                parent.encryption_load(fmt, parent_pass)
                assert parent.read(0, len(payload)) == payload, "clone writes reached the parent"
            with rbd.Image(io, "encrypted-parent") as parent:
                parent.unprotect_snap("baseline")
                parent.remove_snap("baseline")
            api.remove(io, "encrypted-parent")
        with session() as (_, io):
            absent(io, "encrypted-parent")
            with rbd.Image(io, "encrypted-child") as child:
                assert child.id() == child_id
                child.encryption_load(fmt, child_pass)
                assert child.read(0, len(expected)) == expected, "parent removal changed the flattened clone"
        # encrypted-child stays for the fixture's ExportBackup/RestoreBackup.
        retained = ["encrypted-child"]
        proof.update(parent_id=parent_id, child_id=child_id, inherited_clone=True, flattened_loaded=True, parent_removed=True)
    else:
        restored_id = verify_restored_encryption("encrypted-restored", child_pass, parent_pass, expected, raw_size)
        with session() as (_, io):
            api.remove(io, "encrypted-restored")
            api.remove(io, "encrypted-child")
        proof.update(restored_id=restored_id, bytes=len(expected), backup_ciphertext=True)
` + harness.RBDClientFeatureEpilogue
