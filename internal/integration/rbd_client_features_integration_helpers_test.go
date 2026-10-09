//go:build all || (integration && features)

package integration_test

const rbdClientFeatureSentinelScript = `import rados, rbd, sys
pool, phase = sys.argv[1:]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "10", "rados_osd_op_timeout": "10"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        api = rbd.RBD()
        for namespace in ["", "foreign"]:
            io.set_namespace(namespace)
            expected = ("untouched-namespace:" + namespace).encode() * 4096
            if phase == "seed":
                api.create(io, "sentinel", 8 << 20, old_format=False, features=rbd.RBD_FEATURE_LAYERING)
                with rbd.Image(io, "sentinel") as image:
                    assert image.write(expected, 0) == len(expected)
                    image.flush()
                    original_id = image.id()
                api.trash_move(io, "sentinel", delay=0)
                assert api.trash_get(io, original_id)["name"] == "sentinel"
            else:
                entries = list(api.trash_list(io))
                assert len(entries) == 1 and entries[0]["name"] == "sentinel", "purge crossed namespaces"
                original_id = entries[0]["id"]
                api.trash_restore(io, original_id, "sentinel")
                with rbd.Image(io, "sentinel") as image:
                    assert image.id() == original_id
                    assert image.read(0, len(expected)) == expected, "foreign sentinel bytes changed"
                api.remove(io, "sentinel")
print("foreign/default expired trash and exact bytes preserved")
`

const rbdClientFeatureScript = `import contextlib, datetime, errno, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, time
import rados, rbd
# The native binding converts UTC-labelled expiry through local time.mktime.
# Pin this consumer's local timezone before any deferred-trash operation.
os.environ["TZ"] = "UTC"
time.tzset()
phase, pool, namespace, entity, keyring, expected_fsid, expected_pool_id = sys.argv[1:]
expected_pool_id = int(expected_pool_id)
api = rbd.RBD()
features = rbd.RBD_FEATURE_LAYERING | rbd.RBD_FEATURE_EXCLUSIVE_LOCK
payload = bytes((i * 37 + 17) % 251 + 1 for i in range(2 << 20))
patch = b"changed-child-and-migration-data" * 4096
proof = {"phase": phase, "fsid": expected_fsid, "pool_id": expected_pool_id}

def require_passphrase_denied(image, fmt, passphrase, label, stage):
    # Ceph v20.2.4 test_mock_LoadRequest.cc::WrongPassphrase requires -EPERM.
    # LUKS Header passes crypt_volume_key_get's errno through unchanged; the
    # Python binding maps it to PermissionError with positive errno.EPERM.
    # Invalid format/header, timeout, I/O and unsupported algorithms are not
    # evidence that a passphrase was denied. Never print key/error text.
    try:
        image.encryption_load(fmt, passphrase)
    except rbd.Error as error:
        error_type, actual = type(error).__name__, getattr(error, "errno", None)
        if not isinstance(error, rbd.PermissionError) or actual != errno.EPERM:
            raise AssertionError("unexpected native key rejection: type=%s errno=%s" % (error_type, actual)) from None
        proof.setdefault("key_denials", []).append({"format": label, "stage": stage, "type": error_type, "errno": actual})
        return
    raise AssertionError("native passphrase unexpectedly loaded: " + stage)

@contextlib.contextmanager
def session():
    with rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity,
                     conf={"keyring": keyring, "rados_mon_op_timeout": "10", "rados_osd_op_timeout": "15", "rbd_cache": "false"}) as conn:
        assert conn.get_fsid() == expected_fsid, "connected to another cluster"
        with conn.open_ioctx(pool) as io:
            assert io.get_pool_id() == expected_pool_id, "native pool was replaced"
            io.set_namespace(namespace)
            yield conn, io

def create(io, name, data=payload, size=8 << 20):
    api.create(io, name, size, old_format=False, features=features, order=20)
    with rbd.Image(io, name) as image:
        if data:
            assert image.write(data, 0) == len(data)
            image.flush()
        return image.id()

def read(io, name, expected, image_id=None, offset=0):
    with rbd.Image(io, name) as image:
        if image_id is not None:
            assert image.id() == image_id, "same-named image was replaced"
        assert image.read(offset, len(expected)) == expected, "native image bytes changed"

def absent(io, name):
    try:
        with rbd.Image(io, name):
            pass
    except rbd.ImageNotFound:
        return
    raise AssertionError("image must be absent: " + name)

if phase == "layering-flatten":
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
        group_id = group.id()
        for name in ids:
            group.add_image(io, name)
        # All writers have flushed and closed before the group checkpoint.
        group.create_snap("baseline")
        snap = group.get_snap_info("baseline")
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
        assert group.id() == group_id
        read(io, "group-a", payload, ids["group-a"])
        read(io, "group-b", payload[::-1], ids["group-b"])
        group.remove_snap("baseline")
        for name in ids:
            group.remove_image(io, name)
            api.remove(io, name)
        api.group_remove(io, "checkpoint")
    proof.update(group_id=group_id, snapshot_id=snapshot_id, images=ids, bytes_per_image=len(payload))

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
        raise RuntimeError("cryptsetup executable required by the control/all image contract: repair the selected image or optionally supply a compatible CEPH_TEST_RBD_CLIENT_IMAGE; rekey is an external LUKS passphrase change, not a native RBD API")
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

else:
    raise AssertionError("unknown probe phase: " + phase)

with session() as (_, io):
    assert list(api.list(io)) == [] and list(api.trash_list(io)) == [], "client probe left images/trash behind"
print(json.dumps(proof, sort_keys=True))
`
