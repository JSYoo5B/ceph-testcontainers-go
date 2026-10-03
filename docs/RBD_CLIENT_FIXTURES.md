# RBD client 기능을 위한 서버 fixture

RBD image의 clone·flatten·trash·migration·group snapshot·암호화·lock은 소비하는 client의 API입니다. 이 모듈은 그 CRUD를 다시 감싸지 않고, replicated metadata pool·RBD 초기화·namespace·Cephx 권한·native client의 네트워크와 설정을 제공합니다. 공개 Go module에는 go-ceph나 cgo 의존성이 없습니다.

참고 범위는 [go-ceph v0.41.0의 RBD](https://github.com/ceph/go-ceph/tree/v0.41.0/rbd)와 Ceph Tentacle입니다. [실행 가능한 public API 조합과 native client probe](../internal/integration/rbd_client_features_integration_test.go)의 selector는 `TestRBDClientFeatures`입니다. bridge와 host networking에서 같은 시나리오를 실행합니다. 이 문서를 추가한 단계의 검증은 CGO=0 tag compile과 Python script syntax 검사입니다. 실제 Docker 통과 여부는 [전체 완료 기준](CLIENT_FIXTURE_COVERAGE.md)의 R02 상태로 확인합니다.

## 공개 API 조합

```go
cluster, err := ceph.Run(ctx, serverImage,
    ceph.WithOSDCount(1),
    ceph.WithOSDImage(osdImage),
)
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}
const pool, namespace = "rbd-fixture", "consumer"
if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{
    Name: pool, PGNum: 8, Replicas: 1, MinSize: 1,
}); err != nil {
    t.Fatal(err)
}
if err := cluster.InitRBDPool(ctx, pool); err != nil {
    t.Fatal(err)
}
if _, err := cluster.CreateRBDNamespace(ctx, pool, namespace); err != nil {
    t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
    t.Fatal(err)
}
identity, err := cluster.CreateClient(ctx, "rbd-consumer", ceph.ClientCaps{
    Mon: "profile rbd",
    MGR: "profile rbd pool=" + pool + " namespace=" + namespace,
    OSD: "profile rbd pool=" + pool + " namespace=" + namespace,
})
if err != nil {
    t.Fatal(err)
}
client, err := testcontainers.Run(ctx, nativeClientImage,
    cluster.WithClientIdentity(identity),
    testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
    testcontainers.WithWaitStrategy(wait.ForExec([]string{
        "python3", "-c", "import rados, rbd",
    })),
)
if client != nil {
    testcontainers.CleanupContainer(t, client)
}
if err != nil {
    t.Fatal(err)
}
// Run the application's native client in client. Select pool/namespace in its
// IO context; use identity.Name() and identity.KeyringPath() for authentication.
```

여기서 replica 1은 빠른 기능 fixture의 선택입니다. failure/recovery 시나리오는 replica·failure domain·OSD 수를 목적에 맞게 조합합니다. EC image data pool은 별도로 `CreatePool`의 overwrite-enabled EC pool을 만들고 client image create의 `data_pool`로 선택합니다. RBD metadata pool은 replicated로 유지합니다.

host mode에서는 `Run`에 `ceph.WithHostNetwork()`를 추가합니다. `WithClientIdentity`가 같은 network mode와 현재 MON 주소를 client에 전달합니다. macOS에서 위 native client는 Docker의 Linux container 안에서 실행하며, go-ceph를 사용하는 실제 애플리케이션 빌드·테스트는 Linux에서 수행합니다. 직접 MON/OSD 접속을 하는 client를 macOS host의 임의 mapped port로 옮기지는 않습니다.

## client probe와 서버 조건

각 probe는 연결한 FSID·native pool ID를 확인하고, 선택한 namespace에서 생성한 native image ID와 실제 bytes를 검증합니다. 성공 후 image·snapshot·group·trash를 정리하고, 다른 namespace의 expired trash와 데이터도 보존하는지 확인합니다. 모든 native 호출은 별도 subprocess deadline으로 제한합니다.

| probe | client API 및 필요한 조건 | 명령 성공 외의 검증 |
| --- | --- | --- |
| `layering-flatten` | go-ceph Clone/Open/Flatten에 해당하는 native librbd; format 2와 `layering`, protected parent snapshot의 clone format 1 | child의 native parent ID, parent/child 쓰기 격리, flatten 후 parent dependency 없음, parent snapshot/image 제거 후 같은 child ID와 정확한 2 MiB |
| `trash-restore-purge` | Trash/List/Restore/Remove 계열; dedicated namespace의 native purge | restore가 원래 native ID와 bytes 유지, 만료된 entry만 제거, 1시간 deferment entry 및 default/foreign namespace의 만료 entry 보존 |
| `migration-commit` | MigrationPrepare/Execute/Commit; closed source writer, replicated initialized pool, source/destination layering | prepared/executed native state와 양쪽 namespace/ID, materialization 전 destination write 포함, commit 후 source와 source trash 없음, 새 destination ID와 정확한 bytes |
| `migration-abort` | MigrationPrepare/Abort; 같은 fixture 조건 | destination 제거, 원래 source ID와 bytes 복구 |
| `group-snapshot` | GroupCreate/ImageAdd/SnapCreate/GetInfo/Rollback; 두 format 2 image, flushed/closed writers | native complete 상태와 두 image membership·pool/snapshot ID, 두 image 변경 뒤 rollback으로 각 원래 bytes 복구, group ID 보존 |
| `exclusive-lock` | LockAcquire/GetOwners/IsExclusiveOwner/Release; `exclusive-lock`, 독립된 두 RADOS session | held owner 한 명과 경쟁 client의 거부, release 뒤 다른 session이 실제 owner가 됨, write/flush와 같은 image ID·bytes |
| `encryption-format-load` | EncryptionFormat/Load/Load2 계열의 대표 단일 image; LUKS1/LUKS2, AES256, `librbd`·`libcryptsetup`, journaling 없음 | 틀린 key 거부, generic LUKS load의 양쪽 format 감지, raw LUKS header/ciphertext, fresh loaded client의 exact bytes와 native ID |
| `encryption-rekey` | RBD 자체에 rekey API 없음; 같은 native client에 외부 `cryptsetup` 실행 파일 추가 | 원래 image ID·size 유지, 변경이 raw header에만 한정됨, 새 fresh load 성공·old key 실패, 원래 encrypted payload의 exact bytes 보존 |

group snapshot probe의 writers는 checkpoint 전에 flush하고 닫습니다. 이 증거는 두 image의 native group checkpoint/rollback을 확인합니다. 임의의 distributed application transaction이나 파일시스템 freeze의 일관성까지 입증하지 않습니다. lock probe는 수동 acquire/release와 협력 client의 ownership을 확인합니다. 죽은 client의 강제 lock break·blocklist fencing은 [별도 fencing fixture](../internal/integration/fencing_integration_test.go)에서 검증합니다.

trash expiry의 native binding은 UTC-labelled datetime에도 local `time.mktime`을 사용하므로 probe process의 timezone을 UTC로 고정합니다. 암호화의 wrong/old passphrase는 native `PermissionError/EPERM`만 인정합니다. timeout·I/O·unsupported·invalid-header 오류는 key 거부의 증거가 되지 않으며 실패로 처리합니다.

migration은 이 테스트에서 같은 metadata pool·namespace 안의 image 이름을 바꿉니다. native/external source-spec import, 연결된 다른 cluster의 import 또는 HTTP/S3 source의 credential fixture는 이 probe의 증거 범위에 포함되지 않습니다. encrypted clone chain의 `EncryptionLoad2`, encryption payload 전체의 새 volume key로 재암호화도 이 단일 image passphrase 변경과 별개입니다. client 기능의 모든 parameter 조합을 검증했다는 뜻은 아닙니다.

## cryptsetup이 있는 native client 이미지

기본 control role에는 Python rados/rbd와 librbd encryption의 runtime dependency가 있지만 `cryptsetup` 실행 파일은 없습니다. [test-only client Dockerfile](../internal/integration/rbd-client/Dockerfile)은 같은 고정 Quay release의 RPM image에서 distro package로 그 실행 파일을 설치합니다. extracted slim role에는 package database·dnf가 없으므로 이 Dockerfile의 기반에는 원본 RPM image를 지정합니다. client에만 도구가 추가되며 OSD/MON/MGR 이미지 구성은 바뀌지 않습니다.

```sh
docker build --platform linux/arm64 \
  -f internal/integration/rbd-client/Dockerfile \
  -t ceph-testcontainers:20.2.4-rbd-client .

CEPH_TEST_RBD_CLIENT_IMAGE=ceph-testcontainers:20.2.4-rbd-client \
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd \
CGO_ENABLED=0 go test -mod=readonly -count=1 \
  -tags=integration,features ./internal/integration \
  -run '^TestRBDClientFeatures$' -timeout 35m -v
```

Linux x86-64에서는 platform을 `linux/amd64`로 선택합니다. 새 Ceph release나 회사 RPM 기반 native client는 Docker build의 `--build-arg CEPH_CLIENT_IMAGE=...`로 지정하고 서버와 ABI·release·architecture를 맞춥니다. Ubuntu/Debian 회사 client는 해당 배포판에서 `python3-rados`, `python3-rbd`, `librbd`와 `cryptsetup`을 설치한 별도 이미지를 `CEPH_TEST_RBD_CLIENT_IMAGE`로 주입합니다. 이 RPM 전용 Dockerfile에 다른 배포판 바이너리만 복사해서 섞지 않습니다. image 빌드의 package 설치에는 distro repository 접속이 필요합니다.

rekey probe는 native writers를 닫고 raw RBD를 container의 임시 regular file로 export합니다. `cryptsetup luksChangeKey`가 변경한 header chunk만 동일 RBD에 기록한 후 fresh client에서 다시 load합니다. kernel RBD/NBD mapping, `/dev/mapper`, privileged container 또는 host native SDK가 필요하지 않습니다. test는 PBKDF2 iteration 1000을 명시해 작은 Docker VM에서 오래 걸리는 benchmark를 피합니다. 이 값은 테스트용이며 production key 정책이 아닙니다. format을 다시 호출해서 원래 bytes를 잃는 방식을 rekey로 취급하지 않습니다. `cryptsetup`이 없으면 rekey subtest는 원인을 표시하고 실패하며 미검증 경로를 skip으로 숨기지 않습니다.

## 근거

- [go-ceph RBD tests](https://github.com/ceph/go-ceph/tree/v0.41.0/rbd): `rbd_test.go`, `migration_test.go`, `group_snap_test.go`, `locks_test.go`, `encryption_test.go`, `encryption_load2_test.go`가 요구하는 native client의 전제조건.
- [Ceph Tentacle snapshots](https://docs.ceph.com/en/tentacle/rbd/rbd-snapshot/) 및 [live migration](https://docs.ceph.com/en/tentacle/rbd/rbd-live-migration/): parent dependency와 migration lifecycle.
- [Ceph Tentacle image encryption](https://docs.ceph.com/en/tentacle/rbd/rbd-encryption/): journaling 제약, raw LUKS layout, 외부 cryptsetup을 통한 passphrase 변경의 구분.
- [Ceph v20.2.4 Python librbd binding](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/rbd/rbd.pyx): 실제 실행한 native method·state·JSON/dictionary grammar.
- [cryptsetup luksChangeKey manual](https://man7.org/linux/man-pages/man8/cryptsetup-lukschangekey.8.html): 기존 key file과 positional new key file을 사용한 regular-file header 변경.
