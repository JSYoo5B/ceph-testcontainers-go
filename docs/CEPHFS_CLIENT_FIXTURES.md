# CephFS client 테스트를 위한 서버 fixture

이 모듈은 filesystem/MDS, data pool, MGR volumes의 group·subvolume·snapshot·clone과 Cephx 조건을 준비합니다. POSIX 파일 읽기/쓰기, inode/xattr, directory traversal, client snapshot API는 소비하는 libcephfs/go-ceph 애플리케이션에서 검증합니다. Go 모듈은 go-ceph/cgo를 링크하지 않으며 native 검증 client는 Linux container 안에서 실행합니다.

## subvolume별 새 client 권한

이미 만든 `cluster`와 `fs := cluster.Filesystems()[0]`를 사용합니다. `WithCephFS` 또는 `StartCephFSWithConfig`로 filesystem을 먼저 준비하고, namespace가 분리된 subvolume을 생성합니다.

```go
group, err := fs.CreateSubvolumeGroup(ctx,
    ceph.CephFSSubvolumeGroupConfig{Name: "tenants"})
if err != nil { return err }
volume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{
    Name: "consumer", GroupName: group.Name,
    SizeBytes: 8 << 20, NamespaceIsolated: true,
})
if err != nil { return err }
grant, err := fs.AuthorizeSubvolume(ctx, volume,
    ceph.CephFSSubvolumeAuthorizationConfig{
        ClientID: "consumer-writer", Access: "rw", // "r" 또는 "rw"
    })
if err != nil {
    // non-nil grant는 partial 생성일 수 있습니다. 보존해서 상태를 조사하거나
    // fixture cluster를 종료합니다. 기존 이름으로 다시 authorize하지 않습니다.
    return err
}
client, err := testcontainers.Run(ctx, nativeClientImage,
    cluster.WithClientIdentity(grant.Client),
    ceph.WithIdleEntrypoint(),
)
if client != nil { testcontainers.CleanupContainer(t, client) }
if err != nil { return err }
// Linux native client가 grant.Path를 mount root로 선택합니다.
// auth ID: grant.Client.User(); keyring: grant.Client.KeyringPath().
authorized, err := fs.SubvolumeAuthorizedClients(ctx, volume.Name, volume.GroupName)
if err != nil { return err }
_ = authorized
```

`AuthorizeSubvolume`은 fresh key로 새 principal만 생성합니다. 이미 존재하는 principal을 채택하거나 그 권한을 덮어쓰지 않습니다. native `--allow_existing_id`는 이 호출이 방금 생성하고 key를 확인한 principal에만 사용합니다. 실제 native grant는 다음 범위입니다.

| service | 범위 |
| --- | --- |
| MON | `allow r fsname=<filesystem>` |
| MDS | `allow r/rw path=<실제 UUID mount path>` |
| OSD | `allow r/rw pool=<실제 data pool> namespace=<실제 namespace>` |

빈 namespace는 거부합니다. path 권한만으로 직접 RADOS를 통한 file data 접근까지 제한할 수 없기 때문입니다. `rw`는 layout 변경의 `p`, snapshot 관리의 `s`, root-squash 정책을 제공하지 않습니다. namespace 권한은 그 **전체 namespace**에 적용되며, native clone이 source layout을 상속하거나 같은 이름을 다시 만들면 namespace를 공유할 수 있습니다. 서로 다른 사용자 데이터 격리를 주장하려면 실제 layout과 namespace도 확인해야 합니다.

`SubvolumeAuthorizedClients`는 MGR volumes의 `authorized_list`입니다. 별도로 추가한 raw Cephx capability 전체를 감사하는 목록은 아닙니다. 조회 결과는 수정 소유권을 부여하지 않습니다.

## deauthorize와 기존 session eviction

```go
if err := fs.DeauthorizeSubvolume(ctx, grant); err != nil { return err }
// fresh connection으로 해당 mount/namespace 접근 거부를 확인합니다.
// 기존 session도 차단해야 할 경우 원래 mount root 범위에 eviction을 요청합니다.
if err := fs.EvictSubvolumeClients(ctx, grant); err != nil { return err }
// principal 자체를 제거하는 별도 선택입니다. 먼저 unrelated 권한 소유자와 조율합니다.
if err := cluster.DeleteClient(ctx, grant.Client); err != nil { return err }
if err := fs.RemoveSubvolume(ctx, volume); err != nil { return err }
```

`DeauthorizeSubvolume`은 이 handle의 정확한 MDS path/OSD pool·namespace clause와 native volumes grant만 제거합니다. 같은 key와 다른 path/pool/service 권한은 보존하며, 별도로 추가한 겹치는 권한은 회수하지 않습니다. 모든 권한이 없으면 native Ceph가 principal을 삭제할 수 있습니다. typed grant가 active 또는 결과 불확실 상태인 동안 `RemoveSubvolume`은 거부합니다.

권한 회수는 이미 발급된 ticket이나 기존 mount의 즉시 종료를 뜻하지 않습니다. `EvictSubvolumeClients`는 성공한 deauthorize 뒤에만 허용하며, native auth ID와 **원래 UUID mount root가 모두 일치하는** session을 대상으로 합니다. 다른 mount root나 별도 권한으로 허용되는 미래 연결을 포괄하지 않습니다. eviction은 client connection address를 blocklist할 수 있고, 여러 MDS rank 중 일부만 처리한 오류는 같은 범위로 재시도합니다. client container는 cluster보다 먼저 종료합니다.

권한을 전부 없앤 client는 native mount/RADOS service 인증 단계에서 timeout을 반환할 수 있으므로 timeout 하나를 회수 증거로 쓰지 않습니다. [실행 probe](../internal/integration/cephfs_authorization_integration_test.go)는 source-only RO/RW·namespace 격리를 먼저 검증한 뒤, 별도 neighbor의 RO 권한을 추가합니다. 원래 grant를 회수한 뒤 source mount의 `EPERM`/`EACCES`, 직접 source RADOS의 `PermissionError`, native cap/list 부재를 엄격히 요구하며 같은 key의 neighbor mount·namespace bytes 읽기와 fresh admin 연결도 성공해야 합니다. 이 recipe는 한 grant의 회수와 다른 권한의 보존을 검증하며, 모든 service 권한이 사라진 principal의 거부 errno를 규정하지 않습니다.

## clone 취소와 partial target 정리

`CloneSubvolumeSnapshot`으로 얻은 handle에 `SubvolumeCloneStatus`/`WaitForSubvolumeClone`을 사용합니다. `CancelSubvolumeClone`은 pending/in-progress 작업만 취소하고, canceled 상태와 source snapshot의 해당 pending reference 해제를 기다립니다. target data는 남깁니다. complete/failed 작업은 취소 성공으로 취급하지 않습니다.

`RemovePartialSubvolumeClone`은 원래 source snapshot이 그대로 존재하고 보호 reference가 해제된 **owned failed/canceled target만** native `--force`로 명시적으로 제거합니다. source data/snapshot은 제거하지 않습니다. 완료 target, 교체된 UUID, 미확인 source protection은 거부합니다. 성공은 논리적 이름 부재를 뜻하며 asynchronous trash의 물리적 purge 완료를 뜻하지 않습니다.

미완료 clone의 CLI에는 generation ID가 없으므로 제출 직후 version-checked native volumes v2 metadata를 읽어 UUID path·inode·birth time을 캡처합니다. 이 읽기에는 control image 내부 Python libcephfs가 필요하며 Go host의 native SDK는 필요하지 않습니다. identity 캡처가 실패한 handle을 나중에 조회해서 채택하지 않습니다. [실행 probe](../internal/integration/cephfs_clone_lifecycle_integration_test.go)는 native delay를 사용한 취소와 owned target의 leaf 충돌로 발생한 실제 failed 상태를 다룹니다. source snapshot을 삭제해서 실패를 만들지 않습니다.

## retained snapshot과 custom metadata의 raw 조합

`cluster.Ceph(ctx, args...)`로 native server 기능을 조합합니다. 아래는 기존 owned `volume`/`snapshot`, 같은 filesystem 안의 새 target `restored`를 사용하는 명령 순서입니다. raw 편집은 caller가 모든 외부 writer/session과 중복 fixture 실행을 fence한 뒤 수행합니다.

```go
commands := [][]string{
    {"fs", "subvolume", "metadata", "set", fs.FilesystemName, volume.Name,
        "CaseKey", "source-value", "--group_name", volume.GroupName},
    {"fs", "subvolume", "snapshot", "metadata", "set", fs.FilesystemName,
        volume.Name, snapshot.Name, "CheckpointLabel", "frozen-label",
        "--group_name", volume.GroupName},
    {"fs", "subvolume", "rm", fs.FilesystemName, volume.Name,
        "--group_name", volume.GroupName, "--retain-snapshots"},
    {"fs", "subvolume", "snapshot", "clone", fs.FilesystemName,
        volume.Name, snapshot.Name, "restored", "--group_name", volume.GroupName,
        "--target_group_name", volume.GroupName},
}
for _, args := range commands {
    if _, err := cluster.Ceph(ctx, args...); err != nil { return err }
}
// bounded polling: fs clone status <fs> restored --group_name <group> --format json
// complete 뒤 SubvolumeInfo의 path/quota/layout과 native client bytes를 확인합니다.
```

Ceph 20.2.4 volumes v2의 `snapshot-retained` 상태에는 live getpath가 없습니다. snapshot list/info/getpath와 raw clone-source 사용은 가능하지만, subvolume custom metadata와 snapshot custom metadata 호출은 이 상태에서 허용되지 않습니다. live source의 custom metadata는 retention 시 제거되고 snapshot metadata는 별도로 남습니다. snapshot은 source의 custom metadata를 자동 복사하지 않으며 clone에도 자동 전달하지 않습니다. key는 native metadata에서 소문자로 정규화됩니다. 이 metadata는 POSIX xattr/파일 metadata와 별개입니다.

같은 retained 이름에 raw `fs subvolume create`를 하면 새 UUID의 live incarnation을 만들 수 있습니다. 아직 native asynchronous purge가 진행 중이면 해당 EAGAIN만 deadline 안에서 재시도하고, 현재 상태가 retained인지 매번 확인합니다. force cleanup으로 진행시키거나 새 complete 상태를 채택하지 않습니다. 재생성 후 snapshot metadata를 다시 읽을 수 있지만 source custom metadata는 새로 시작합니다.

원래 typed `volume`/`snapshot` handle은 retained 상태나 새 UUID를 채택하지 않습니다. retained source의 typed clone/resize, raw 재생성 이후 원래 handle의 remove는 거부합니다. raw로 생성한 target/source의 정리도 caller의 raw 책임입니다. [실행 가능한 전체 recipe](../internal/integration/cephfs_retained_snapshot_integration_test.go)는 frozen bytes·복원 target의 quota/layout·새 UUID·metadata 독립성·명시적 정리를 확인합니다.

## 소유권과 오류 경계

typed handle의 exported field는 설명이며 수정해도 mutation 대상을 바꾸지 않습니다. 복사본은 내부 lifecycle 상태를 공유합니다. mutation은 creation-captured filesystem/pool ID, 원래 UUID/path/birth, grant key와 layout을 다시 확인합니다. group/subvolume/snapshot/clone/권한 생성의 non-nil 결과와 error는 partial 또는 불확실한 native 결과일 수 있으므로 handle을 보존합니다.

deauthorize와 partial-clone remove는 응답 유실 뒤 현재 native 상태를 확인해 같은 handle의 재시도를 수렴시킵니다. 외부 key/UUID/권한 교체나 불일치는 오류로 드러내며 복원·권한 확대·data 삭제로 자동 해결하지 않습니다. helper의 mutex는 같은 Go fixture 안의 작업만 직렬화합니다. native CLI에는 이러한 검사와 mutation을 하나로 묶는 CAS가 없으므로 외부 auth/layout/source 편집을 caller가 fence해야 합니다.

## 실행과 근거

아래 selector는 각각 bridge와 host subtest를 포함합니다. host mode에는 Linux Docker host networking 또는 Docker Desktop의 host networking 설정이 필요합니다. native client/control image에는 Python `cephfs`/`rados`가 있어야 합니다.

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:official-20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:official-20.2.4-osd \
CEPH_TEST_MDS_IMAGE=ceph-testcontainers:official-20.2.4-mds \
CGO_ENABLED=0 go test -mod=readonly -count=1 -tags=integration,features \
  ./internal/integration \
  -run '^(TestCephFSSubvolumeClientAuthorization|TestCephFSCloneCancellationAndPartialCleanup|TestCephFSRetainedSnapshotAndMetadataRecipe)$' \
  -timeout 25m -v
```

실행 결과와 완료 범위는 [coverage matrix](CLIENT_FIXTURE_COVERAGE.md)에서 관리합니다. 회사/새 release 이미지는 환경변수로 교체하고 native CLI·volumes metadata grammar를 다시 검증합니다.

Ceph 20.2.4와 Docker의 Linux native client로 다음 selector의 실제 실행을 확인했습니다.

| selector | bridge | host | 실제 확인한 범위 |
| --- | --- | --- | --- |
| `TestCephFSCloneCancellationAndPartialCleanup` | PASS · 204.58초 | PASS · 209.80초 | native canceled 및 실제 failed 상태, source protection 해제, 명시적 partial target 정리, source/snapshot bytes 보존, 완료·교체 target 보호 |
| `TestCephFSRetainedSnapshotAndMetadataRecipe` | PASS · 64.54초 | PASS · 72.35초 | retained snapshot의 frozen bytes와 clone quota/layout, 같은 이름의 새 UUID, 기존 typed handle의 mutation 거부, custom metadata의 독립성과 raw 정리 |
| `TestCephFSSubvolumeClientAuthorization` | PASS · 114.56초 | PASS · 112.73초 | source-only RO/RW·namespace 격리, 두 principal의 source revoke 후 strict permission 오류·neighbor bytes/동일 key 보존, held native session eviction |

failed clone은 owned target의 `data` 경로를 directory로 만들어 source의 regular file copy가 native `EISDIR`로 실패하도록 주입했습니다. 원래 source/snapshot을 바꾸지 않았고, failed 상태와 source reference 해제를 확인한 뒤 명시적으로 target을 제거했습니다. 이 증거는 해당 취소·실패 조건과 논리적 cleanup을 다루며 모든 I/O 장애 원인이나 trash의 물리적 purge 완료를 포괄하지 않습니다. retention 검증은 외부 writer를 fence한 단일 source/snapshot의 native v2 lifecycle과 custom metadata 계약을 확인합니다. 실제 애플리케이션의 POSIX 작업이나 여러 writer의 transaction 일관성은 consumer 테스트에서 별도로 검증합니다.

- [Ceph Tentacle fs-volumes](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/): authorization, snapshot/clone, retention, custom metadata.
- [CephFS client authentication](https://docs.ceph.com/en/tentacle/cephfs/client-auth/): path, pool/namespace, `p`/`s`, root-squash의 별도 권한.
- [Ceph 20.2.4 native access helper](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/access.py)와 [subvolume v1](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v1.py): 정확한 cap 제거와 native list/eviction 범위.
- [subvolume v2](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v2.py), [async cloner](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/async_cloner.py), [native tests](https://github.com/ceph/ceph/blob/v20.2.4/qa/tasks/cephfs/test_volumes.py): retained 상태 제약, clone terminal state와 metadata lifecycle.
