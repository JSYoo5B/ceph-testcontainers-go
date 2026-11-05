//go:build all || (integration && features)

package rbd_test

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"

// rbdClientFeatureScript holds the native client recipes whose fixture needs
// the rbd README lists.
var rbdClientFeatureScript = harness.RBDClientFeaturePrelude + `if phase == "layering-flatten":
    with session() as (_, io):
        parent_id = create(io, "parent")
        with rbd.Image(io, "parent") as parent:
            parent.create_snap("baseline")
            parent.protect_snap("baseline")
        api.clone(io, "parent", "baseline", io, "child", features=features, order=20, clone_format=1)
        with rbd.Image(io, "child") as child:
            child_id = child.id()
            assert child.parent_id() == parent_id
            assert child.read(0, len(payload)) == payload
            assert child.write(patch, 0) == len(patch)
            child.flush()
        read(io, "parent", payload, parent_id)
    expected = patch + payload[len(patch):]
    with session() as (_, io):
        read(io, "child", expected, child_id)
        with rbd.Image(io, "child") as child:
            child.flatten()
            try:
                child.parent_id()
            except rbd.ImageNotFound:
                pass
            else:
                raise AssertionError("flatten retained parent dependency")
        with rbd.Image(io, "parent") as parent:
            parent.unprotect_snap("baseline")
            parent.remove_snap("baseline")
        api.remove(io, "parent")
    with session() as (_, io):
        absent(io, "parent")
        read(io, "child", expected, child_id)
        api.remove(io, "child")
    proof.update(parent_id=parent_id, child_id=child_id, bytes=len(expected), parent_removed=True)

elif phase == "trash-restore-purge":
    with session() as (_, io):
        original_id = create(io, "recoverable")
        api.trash_move(io, "recoverable", delay=3600)
        absent(io, "recoverable")
        entry = api.trash_get(io, original_id)
        assert entry["id"] == original_id and entry["name"] == "recoverable"
        api.trash_restore(io, original_id, "restored")
    with session() as (_, io):
        read(io, "restored", payload, original_id)
        api.trash_move(io, "restored", delay=3600)
        expired_id = create(io, "expired", patch)
        api.trash_move(io, "expired", delay=0)
        # Make expiry deterministic despite second-resolution native timestamps.
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=2)
        api.trash_purge(io, expire_ts=now)
        entries = list(api.trash_list(io))
        assert [entry["id"] for entry in entries] == [original_id], "purge deleted unexpired recoverable image"
        try:
            api.trash_get(io, expired_id)
        except rbd.ImageNotFound:
            pass
        else:
            raise AssertionError("expired trash survived purge")
        api.trash_restore(io, original_id, "restored")
    with session() as (_, io):
        read(io, "restored", payload, original_id)
        api.remove(io, "restored")
    proof.update(image_id=original_id, expired_id=expired_id, bytes=len(payload), deferment_preserved=True)

elif phase in ["migration-commit", "migration-abort"]:
    with session() as (_, io):
        original_id = create(io, "migration-source")
        api.migration_prepare(io, "migration-source", io, "migration-destination", features=features, order=20)
        status = api.migration_status(io, "migration-destination")
        assert status["state"] == rbd.RBD_IMAGE_MIGRATION_STATE_PREPARED
        assert status["source_image_id"] == original_id
        assert status["source_pool_namespace"] == namespace and status["dest_pool_namespace"] == namespace
        destination_id = status["dest_image_id"]
        assert destination_id and destination_id != original_id
        read(io, "migration-destination", payload, destination_id)
        if phase == "migration-abort":
            api.migration_abort(io, "migration-destination")
        else:
            # Clients may change destination data before materialization. The
            # resulting copy must combine migrated old bytes and the new write.
            with rbd.Image(io, "migration-destination") as destination:
                assert destination.write(patch, 0) == len(patch)
                destination.flush()
            api.migration_execute(io, "migration-destination")
            assert api.migration_status(io, "migration-destination")["state"] == rbd.RBD_IMAGE_MIGRATION_STATE_EXECUTED
            api.migration_commit(io, "migration-destination")
    with session() as (_, io):
        if phase == "migration-abort":
            absent(io, "migration-destination")
            read(io, "migration-source", payload, original_id)
            api.remove(io, "migration-source")
        else:
            absent(io, "migration-source")
            expected = patch + payload[len(patch):]
            read(io, "migration-destination", expected, destination_id)
            assert all(entry["id"] != original_id for entry in api.trash_list(io)), "committed migration retained source data in trash"
            api.remove(io, "migration-destination")
    proof.update(source_id=original_id, destination_id=destination_id, bytes=len(payload))

elif phase == "group-snapshot":
    with session() as (_, io):
        ids = {name: create(io, name, value) for name, value in [("group-a", payload), ("group-b", payload[::-1])]}
        api.group_create(io, "checkpoint")
        group = rbd.Group(io, "checkpoint")
        checkpoint_id = group_id(io, group, "checkpoint")
        for name in ids:
            group.add_image(io, name)
        # All writers have flushed and closed before the group checkpoint.
        group.create_snap("baseline")
        snap = group_snap_info(io, group, "checkpoint", "baseline", ids)
        assert snap["state"] == rbd.RBD_GROUP_SNAP_STATE_COMPLETE
        assert {entry["image_name"] for entry in snap["image_snaps"]} == set(ids)
        assert all(entry["pool_id"] == expected_pool_id and entry["snap_id"] > 0 for entry in snap["image_snaps"])
        for name in ids:
            with rbd.Image(io, name) as image:
                assert image.write(patch, 0) == len(patch)
                image.flush()
        group.rollback_to_snap("baseline")
        snapshot_id = snap["id"]
    with session() as (_, io):
        group = rbd.Group(io, "checkpoint")
        assert group_id(io, group, "checkpoint") == checkpoint_id
        read(io, "group-a", payload, ids["group-a"])
        read(io, "group-b", payload[::-1], ids["group-b"])
        group.remove_snap("baseline")
        for name in ids:
            group.remove_image(io, name)
            api.remove(io, name)
        api.group_remove(io, "checkpoint")
    proof.update(group_id=checkpoint_id, snapshot_id=snapshot_id, images=ids, bytes_per_image=len(payload))

elif phase == "exclusive-lock":
    with session() as (_, io):
        original_id = create(io, "locked")
    with session() as (first_conn, first_io), session() as (second_conn, second_io):
        assert first_conn.get_instance_id() != second_conn.get_instance_id(), "lock contenders share a session"
        with rbd.Image(first_io, "locked") as first, rbd.Image(second_io, "locked") as second:
            assert first.id() == second.id() == original_id
            first.lock_acquire(rbd.RBD_LOCK_MODE_EXCLUSIVE)
            assert first.is_exclusive_lock_owner()
            owners = list(second.lock_get_owners())
            assert len(owners) == 1 and owners[0]["mode"] == rbd.RBD_LOCK_MODE_EXCLUSIVE
            first_owner = owners[0]["owner"]
            try:
                second.lock_acquire(rbd.RBD_LOCK_MODE_EXCLUSIVE)
            except rbd.Error as error:
                # RBD_LOCK_MODE_EXCLUSIVE installs StandardPolicy, whose peer
                # release refusal is -EROFS in Ceph v20.2.4. A native busy lock
                # may also report -EBUSY. Reject every other class/errno pair.
                actual = getattr(error, 'errno', None)
                valid = ((isinstance(error, rbd.ReadOnlyImage) and actual == errno.EROFS) or
                         (isinstance(error, rbd.ImageBusy) and actual == errno.EBUSY))
                if not valid:
                    raise AssertionError('unexpected native lock contention: type=%s errno=%s' % (type(error).__name__, actual)) from None
                proof['contention'] = {'type': type(error).__name__, 'errno': actual}
            else:
                raise AssertionError("competing client acquired held exclusive lock")
            assert first.is_exclusive_lock_owner() and not second.is_exclusive_lock_owner()
            owners = list(second.lock_get_owners())
            assert len(owners) == 1 and owners[0]['mode'] == rbd.RBD_LOCK_MODE_EXCLUSIVE and owners[0]['owner'] == first_owner, 'contention changed the original owner'
            first.lock_release()
            assert not first.is_exclusive_lock_owner()
            second.lock_acquire(rbd.RBD_LOCK_MODE_EXCLUSIVE)
            assert second.is_exclusive_lock_owner()
            owners = list(second.lock_get_owners())
            assert len(owners) == 1 and owners[0]["owner"] != first_owner
            assert second.write(patch, 0) == len(patch)
            second.flush()
            second.lock_release()
            assert list(second.lock_get_owners()) == []
    with session() as (_, io):
        read(io, "locked", patch + payload[len(patch):], original_id)
        api.remove(io, "locked")
    proof.update(image_id=original_id, contenders=2, held_lock_denied=True, handoff_bytes=len(payload))

elif phase in ["encryption-format-load", "encryption-rekey"]:
    cryptsetup = shutil.which("cryptsetup")
    if phase == "encryption-rekey" and cryptsetup is None:
        raise RuntimeError("cryptsetup executable required by the client role contract: repair the selected image or optionally supply a compatible CEPH_TEST_RBD_CLIENT_IMAGE; rekey is an external LUKS passphrase change, not a native RBD API")
    encrypted_ids = {}
    for label, fmt in [("luks1", rbd.RBD_ENCRYPTION_FORMAT_LUKS1), ("luks2", rbd.RBD_ENCRYPTION_FORMAT_LUKS2)]:
        name = phase + "-" + label
        old_pass, new_pass = "temporary-fixture-old-key", "temporary-fixture-new-key"
        raw_size = 64 << 20
        with session() as (_, io):
            image_id = create(io, name, data=None, size=raw_size)
            with rbd.Image(io, name) as image:
                # Journaling is absent; format writes a LUKS header, not data.
                assert not image.features() & rbd.RBD_FEATURE_JOURNALING
                image.encryption_format(fmt, old_pass, rbd.RBD_ENCRYPTION_ALGORITHM_AES256)
            with rbd.Image(io, name) as image:
                image.encryption_load(fmt, old_pass)
                header_size = raw_size - image.size()
                assert 0 < header_size < raw_size - len(payload)
                assert image.write(payload, 0) == len(payload)
                image.flush()
        with session() as (_, io):
            with rbd.Image(io, name) as image:
                require_passphrase_denied(image, fmt, "wrong-fixture-key", label, "wrong-passphrase")
            with rbd.Image(io, name) as image:
                # Generic LUKS auto-detection must identify both header versions.
                image.encryption_load(rbd.RBD_ENCRYPTION_FORMAT_LUKS, old_pass)
                assert image.id() == image_id
                assert image.read(0, len(payload)) == payload
            with rbd.Image(io, name) as raw:
                assert raw.id() == image_id and raw.size() == raw_size
                assert raw.read(0, 6) == b"LUKS\xba\xbe"
                assert raw.read(header_size, 4096) != payload[:4096], "raw ciphertext contains plaintext"
        if phase == "encryption-rekey":
            # All native writers are closed. Work on a regular-file raw export,
            # then apply only changed header chunks to this same owned image.
            # No kernel RBD mapping or privileged container is required.
            with tempfile.TemporaryDirectory(prefix="tc-rbd-luks-") as temp:
                raw_path = pathlib.Path(temp) / "raw-rbd"
                old_path, new_path = pathlib.Path(temp) / "old-key", pathlib.Path(temp) / "new-key"
                old_path.write_bytes(old_pass.encode())
                new_path.write_bytes(new_pass.encode())
                os.chmod(old_path, 0o600)
                os.chmod(new_path, 0o600)
                chunks = []
                with session() as (_, io):
                    with rbd.Image(io, name) as raw, raw_path.open("wb") as output:
                        assert raw.id() == image_id
                        for offset in range(0, raw_size, 1 << 20):
                            block = raw.read(offset, 1 << 20)
                            output.write(block)
                            chunks.append(hashlib.sha256(block).digest())
                # Cheap test-only PBKDF cost avoids benchmarking/OOM in the
                # small Docker VM. It is not a production encryption policy.
                subprocess.run([cryptsetup, "--batch-mode", "luksChangeKey", "--key-file", str(old_path),
                                "--pbkdf", "pbkdf2", "--pbkdf-force-iterations", "1000", str(raw_path), str(new_path)],
                               check=True, timeout=45, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                assert raw_path.stat().st_size == raw_size, "rekey changed raw image size"
                changes = []
                with raw_path.open("rb") as updated:
                    for index, digest in enumerate(chunks):
                        block = updated.read(1 << 20)
                        if hashlib.sha256(block).digest() != digest:
                            offset = index << 20
                            assert offset + len(block) <= header_size, "rekey changed encrypted payload outside LUKS header"
                            changes.append((offset, block))
                assert changes, "rekey did not change the LUKS header"
                with session() as (_, io):
                    with rbd.Image(io, name) as raw:
                        assert raw.id() == image_id and raw.size() == raw_size
                        for offset, block in changes:
                            assert raw.write(block, offset) == len(block)
                        raw.flush()
            with session() as (_, io):
                with rbd.Image(io, name) as image:
                    assert image.id() == image_id
                    require_passphrase_denied(image, fmt, old_pass, label, "old-passphrase-after-change")
                with rbd.Image(io, name) as image:
                    image.encryption_load(rbd.RBD_ENCRYPTION_FORMAT_LUKS, new_pass)
                    assert image.id() == image_id and image.size() == raw_size - header_size
                    assert image.read(0, len(payload)) == payload, "rekey lost encrypted data"
        with session() as (_, io):
            api.remove(io, name)
        encrypted_ids[label] = image_id
    proof.update(images=encrypted_ids, bytes_per_image=len(payload), raw_plaintext_hidden=True, rekey=phase == "encryption-rekey")

elif phase == "encryption-backup-source":
    # The fixture archives this encrypted image with its own credentials; the
    # next phase reads the restored copy back as the SUT.
    with session() as (_, io):
        source_id = create(io, "encrypted-source", data=None, size=64 << 20)
        with rbd.Image(io, "encrypted-source") as image:
            image.encryption_format(rbd.RBD_ENCRYPTION_FORMAT_LUKS2, b"temporary-fixture-backup-key", rbd.RBD_ENCRYPTION_ALGORITHM_AES256)
        with rbd.Image(io, "encrypted-source") as image:
            image.encryption_load(rbd.RBD_ENCRYPTION_FORMAT_LUKS2, b"temporary-fixture-backup-key")
            assert image.write(payload, 0) == len(payload)
            image.flush()
    retained = ["encrypted-source"]
    proof.update(source_id=source_id)

elif phase == "encryption-backup-restore":
    restored_id = verify_restored_encryption("encrypted-backup-restored", b"temporary-fixture-backup-key", b"wrong-fixture-backup-key", payload, 64 << 20)
    with session() as (_, io):
        api.remove(io, "encrypted-backup-restored")
        api.remove(io, "encrypted-source")
    proof.update(restored_id=restored_id, bytes=len(payload), backup_ciphertext=True)
` + harness.RBDClientFeatureEpilogue
