# Ceph 클러스터 구성과 복제 연결의 분리

`ceph` 패키지는 단일 일회성 클러스터, `federation` 패키지는 기존 클러스터 사이의 복제 연결을 관리합니다. Go module은 하나를 유지하며 의존 방향은 `federation → ceph`입니다. `ceph.Run`에는 federation 설정을 섞지 않습니다. 일반 RGW/RBD/CephFS 테스트에서는 복제용 컨테이너를 실행하지 않습니다.

## 책임과 수명

| 계층 | API | 소유 리소스 |
| --- | --- | --- |
| 단일 클러스터 | `ceph.Run`, `AddOSD`, `StartRGW`, `StartCephFS` | MON, MGR, OSD, 필요한 RGW/MDS, 자체 Ceph network |
| RGW multisite | `federation.RunRGWMultisite` | 기존 두 클러스터 위의 gateway 2개, 관리 client 2개, HTTP bridge |
| RBD snapshot mirror | `federation.RunRBDMirror` | 관리 client 2개, destination의 `rbd-mirror` daemon |
| CephFS snapshot mirror | `federation.RunCephFSMirror` | source의 `cephfs-mirror` daemon, source MGR에 추가한 remote network 연결 |
| 백업 | 호출자의 CLI와 archive 전달 | export/import와 보관처. 상시 federation daemon은 불필요 |

각 연결은 기존 `*ceph.Container`를 참조합니다. FSID, OSD, 키, 클러스터 네트워크를 공유하지 않습니다. RBD pool과 CephFS/MDS는 호출자가 먼저 만듭니다. RGW multisite는 realm/zone을 기동 전에 구성해야 하므로 standalone `StartRGW`를 실행하지 않은 새 클러스터를 사용합니다.

클러스터 cleanup을 먼저 등록하고 연결 cleanup을 나중에 등록합니다. LIFO로 연결 → 클러스터 순서로 종료합니다. 오류와 함께 non-nil 연결이 반환돼도 cleanup이 필요합니다. Mirror는 `testcontainers.Container`를 embed하여 `Stop`/`Start`로 장애를 주입할 수 있습니다. `Terminate`는 연결이 소유한 추가 컨테이너와 네트워크 연결을 제거하고 클러스터나 데이터를 삭제하지 않습니다. Ceph에 쓴 realm, peer, auth, directory policy는 일회성 클러스터에 남습니다. 설정 해제나 기존 클러스터의 원래 상태 복원은 현재 API의 계약에 포함하지 않습니다.

## 사용 예

```go
source, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if source != nil { testcontainers.CleanupContainer(t, source) }
if err != nil { t.Fatal(err) }

destination, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if destination != nil { testcontainers.CleanupContainer(t, destination) }
if err != nil { t.Fatal(err) }

multisite, err := federation.RunRGWMultisite(ctx, rgwImage,
    federation.RGWMultisiteConfig{
        Source: source, Destination: destination, ControlImage: controlImage,
    })
if multisite != nil {
    t.Cleanup(func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
        defer cancel()
        if err := multisite.Terminate(cleanupCtx); err != nil { t.Error(err) }
    })
}
if err != nil { t.Fatal(err) }
endpoint, err := multisite.Destination.S3Endpoint(ctx)
if err != nil { t.Fatal(err) }
// endpoint, AccessKey, SecretKey, Region을 path-style S3 client에 전달합니다.
```

RBD pool을 초기화한 뒤에는 다음과 같이 연결합니다. 대상 image마다 `rbd mirror image enable POOL/IMAGE snapshot`과 `rbd mirror image snapshot POOL/IMAGE`을 따로 실행합니다. 연결 API의 성공이 데이터 동기화 완료나 자동 failover를 의미하지는 않습니다.

```go
mirror, err := federation.RunRBDMirror(ctx, controlImage,
    federation.RBDMirrorConfig{Source: source, Destination: destination, Pool: "images"})
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

CephFS는 양쪽에서 `StartCephFS`를 호출한 뒤 연결합니다. 지정한 directory의 snapshot을 복제합니다.

```go
mirror, err := federation.RunCephFSMirror(ctx, controlImage,
    federation.CephFSMirrorConfig{
        Source: source, Destination: destination,
        SourceFilesystem: sourceFS.FilesystemName,
        DestinationFilesystem: destinationFS.FilesystemName,
        Directories: []string{"/application"},
    })
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

## Testcontainers와 이미지의 경계

단일 기술 module의 진입점은 기존 `ceph.Run(ctx, image, opts...)`입니다. 다중 클러스터 연결 API는 그 위의 구성 helper로 두며 image를 명시하고, `ContainerCustomizer`를 daemon에 마지막으로 적용하고, 부분 생성 결과를 반환하고, 소유한 리소스를 cleanup합니다. 별개의 공식 Testcontainers module로 등록한 것은 아닙니다. [Testcontainers Go module 지침](https://golang.testcontainers.org/modules/)의 Run·composition·customizer·SDK dependency 원칙을 기반으로 복수 리소스의 수명을 명시한 추가 API입니다.

이미지는 기존 다섯 역할을 유지합니다. `control`은 MON/MGR/CLI에 더해 `rbd-mirror`와 `cephfs-mirror`를 제공하고 `all`도 포함합니다. `osd`/`rgw`/`mds`에는 mirror daemon을 넣지 않습니다. 이미지에 포함해도 일반 클러스터에서 자동으로 실행하지 않으며 각각의 연결 API에서만 실행합니다.

`federation`은 프로젝트 내 구성 helper의 이름입니다. 공통 Ceph federation control plane을 뜻하지 않습니다. RGW는 realm/zone의 HTTP multisite, RBD는 destination이 source MON/OSD에서 읽는 rx-only snapshot mirror, CephFS는 source daemon이 destination에 쓰는 snapshot mirror입니다. CephFS bootstrap에서는 source MGR의 원격 filesystem 검사도 필요하므로 추가 네트워크 연결을 API가 관리합니다. [RGW](https://docs.ceph.com/en/tentacle/radosgw/multisite/), [RBD](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/)의 기능과 제약을 각각 적용합니다.
