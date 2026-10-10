# Ceph release 지원

기본 이미지와 필수 CI는 Tentacle(`ceph.DefaultImage`, Ceph 20.2.4)입니다. fixture는
`Run`에서 MON을 띄운 직후 control 이미지의 `ceph --version`을 읽어
`cluster.CephVersion()`에 기록하고, release마다 다른 명령을 이 값으로 고릅니다.

Squid(Ceph 19.2.5)는 두 단계를 거쳐 지원합니다. 먼저 images 프로젝트가 원본
Quay 이미지, 추출한 role 이미지, Debian·Ubuntu 패키지 이미지를 amd64·arm64에서
검사하고 GHCR에 `<variant>-19.2.5-<role>`로 배포합니다. 그다음 Go CI의 `Ceph squid`
workflow가 push마다 short 범주 전체를 GHCR의 `official-19.2.5` role 이미지로 다시
실행합니다. 이 workflow는 Tentacle용 `Ceph short`와 같은 batch를 쓰므로, short
범주에 새 테스트를 넣으면 두 release에서 함께 검증됩니다. topology, multicluster,
recovery 범주는 push마다 돌리면 CI 시간이 크게 늘어서 `Ceph squid extended`
workflow가 매주 한 번과 수동 실행 때 같은 이미지로 실행합니다.

```go
cluster, err := cephfs.Run(ctx, "quay.io/ceph/ceph:v19.2.5")
if err != nil {
    t.Fatal(err)
}
if strings.HasPrefix(cluster.CephVersion(), "19.") {
    t.Log("Squid: RBD namespace mapping is unavailable")
}
```

버전은 첫 MON과 fixture CLI가 쓰는 이미지 기준입니다. `WithOSDImage` 등으로
다른 release의 role 이미지를 섞으면 daemon마다 release가 다를 수 있습니다. 버전을
읽지 못한 경우는 기본 이미지의 명령 형식을 씁니다.

## Squid에서 달라지는 동작

| 영역 | Tentacle | Squid에서의 처리 |
| --- | --- | --- |
| CephFS 생성 | `fs new ... set max_mds N standby_count_wanted M`으로 생성과 동시에 rank 수 지정 | Squid는 inline 설정을 받지 않으므로 `fs new` 직후 `fs set`으로 지정합니다. 두 경우 모두 MDS를 띄우기 전에 끝납니다. |
| RBD mirror 기본 namespace 준비 | 이름 있는 namespace만 mirror할 때 기본 namespace를 `init-only`로 둡니다. | Squid에는 `init-only`가 없어서 기본 namespace를 `image` mode로 둡니다. 이미지를 명시적으로 켜지 않으면 아무것도 복제하지 않습니다. |
| RBD namespace mapping | `SourceNamespace`와 `DestinationNamespace`를 다르게 줄 수 있습니다. | Squid의 `rbd`에는 `--remote-namespace`가 없어서 같은 이름의 namespace끼리만 mirror합니다. 다른 이름을 주면 변경 전에 오류를 반환합니다. |
| RBD mirror UUID | `rbd mirror pool info`가 `mirror_uuid`와 `remote_namespace`를 출력합니다. | Squid는 둘 다 출력하지 않습니다. UUID는 pool의 `rbd_mirroring` object omap에서 읽고, remote namespace는 같은 이름으로 채웁니다. Tentacle 응답에서 이 값이 빠지면 지금처럼 오류로 처리합니다. |
| RGW sync pipe 기록 | system mode pipe의 `params`에 `user`가 없습니다. | Squid는 `"user": ""`를 함께 기록합니다. pipe 생성 후 readback을 비교할 때 Squid에서만 이 빈 값을 기대합니다. |
| RGW bucket sync 상태 | `bucket sync status --format json`이 JSON을 출력합니다. | Squid는 `--format json`을 무시하고 텍스트만 출력합니다. `WaitBucketSyncReady`와 `BucketSyncStatus`는 이 텍스트를 같은 구조로 바꿔 똑같이 판정하고, 모르는 줄이 있으면 오류로 처리합니다. |

## Squid에서 다르게 검증하는 테스트

| 테스트 | Squid에서의 차이 |
| --- | --- |
| `TestMultiClusterRBDMirrorScopeAndNamespaces`의 이름이 다른 mapping 4개, `TestMultiClusterRBDNamespaceBinding` | fixture가 변경 전에 "Ceph 20 이상 필요" 오류로 거부하는지 확인하고 끝냅니다. 같은 이름 namespace(`pool-same-named`)는 두 release 모두에서 실제 복제까지 검증합니다. |
| `TestRGWProtocolBackends/sts` | 없는 role을 조회할 때 Squid의 `radosgw-admin`은 종료 코드 2와 함께 ENOENT 기록 한 줄을 남깁니다. 그 한 줄만 부재로 인정합니다. |
| `TestRBDClientFeatures/group-snapshot` | Squid binding에는 `Group.id()`와 `get_snap_info()`가 없어서, group ID는 `rbd_group_directory`에서, member snapshot은 각 이미지의 group namespace에서 읽습니다. Tentacle에서는 이 값이 native API와 같은지 함께 확인합니다. |
| `TestMultiClusterRBDReceiverReadiness`의 이름이 다른 mapping 4개, `TestMultiClusterRBDNamespaceImageObservation` | 위 namespace mapping 테스트와 같이 fixture가 변경 전에 거부하는지 확인하고 끝냅니다. |
| `TestMultiClusterNoInitialMirrorDaemons/*/rbd-journal-partial-first` | Squid에서는 `ns-a`를 같은 이름의 `ns-a`로 mirror해서 journal pool scope 검증을 그대로 수행합니다. |
| RBD mirror topology를 직접 읽는 테스트 | 테스트도 `rbd mirror pool info`에서 빠진 `mirror_uuid`와 `remote_namespace`를 fixture와 같은 방법으로 채워서 비교합니다. Tentacle 출력은 그대로 검사합니다. |

## 이미지 호환 확인

`make image-compatibility`의 대표 테스트 9개는 원본 Quay Squid 이미지로도 실행할
수 있습니다.

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b \
make image-compatibility
```

| 이미지 | 결과 |
| --- | --- |
| Squid 19.2.5 | 9개 모두 통과했습니다. `TestClusterLifecycle`, `TestRBDLifecycle`, `TestCephFSFilesystem`, `TestRGWS3`, `TestManagerLifecycle`, `TestMultiClusterRBDBackup`, `TestMultiClusterRBDSnapshotMirror`, `TestMultiClusterCephFSSnapshotMirrorAndBackup`, `TestMultiClusterRGWMultisite`입니다. |
| Squid 19.2.6 | RGW multisite를 제외한 영역은 19.2.5와 같은 수정으로 동작합니다. RGW multisite는 아래 Ceph 회귀 때문에 시작하지 못합니다. |

위 결과는 macOS Docker Desktop(Linux ARM64 엔진)에서 수행한 수동 실행입니다. 필수
검증은 `Ceph squid` workflow가 맡습니다.

## Squid 19.2.6의 RGW multisite 회귀

19.2.6에서는 secondary zone의 `radosgw-admin realm pull`이 master gateway에서 403으로
거부됩니다. debug log의 거부 이유는 `'x-amz-content-sha256' supplied, but not in
CanonicalHeaders`입니다. 같은 release의 `radosgw-admin`이 이 header를 보내면서
서명 대상에는 넣지 않고, RGW 서버는 그런 요청을 거부합니다. 이 검사를 끄는 설정은
없습니다. 19.2.5에서는 같은 fixture로 multisite 테스트 전체가 통과하므로 Ceph
19.2.6의 회귀로 판단합니다. Squid로 multisite를 테스트하려면 19.2.5를 사용합니다.
