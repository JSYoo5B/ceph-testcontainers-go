# Ceph release 지원

기본 이미지와 필수 CI는 Tentacle(`ceph.DefaultImage`, Ceph 20.2.4)입니다. 이전
안정 release인 Squid(Ceph 19.2)도 `Run`에 이미지를 넘겨 사용할 수 있습니다.
fixture는 `Run`에서 MON을 띄운 직후 control 이미지의 `ceph --version`을 읽어
`cluster.CephVersion()`에 기록하고, release마다 다른 명령을 이 값으로 고릅니다.

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

## 검증

`make image-compatibility`의 대표 테스트 9개를 Squid 이미지로 실행했습니다.

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b \
make image-compatibility
```

| 이미지 | 결과 |
| --- | --- |
| Squid 19.2.5 | 9개 모두 통과했습니다. `TestClusterLifecycle`, `TestRBDLifecycle`, `TestCephFSFilesystem`, `TestRGWS3`, `TestManagerLifecycle`, `TestMultiClusterRBDBackup`, `TestMultiClusterRBDSnapshotMirror`, `TestMultiClusterCephFSSnapshotMirrorAndBackup`, `TestMultiClusterRGWMultisite`입니다. |
| Squid 19.2.6 | RGW multisite를 제외한 영역은 19.2.5와 같은 수정으로 동작합니다. RGW multisite는 아래 Ceph 회귀 때문에 시작하지 못합니다. |

검증은 macOS Docker Desktop(Linux ARM64 엔진)에서 수행했습니다. 필수 CI는 여전히
Tentacle만 실행하므로 Squid 동작은 위 수동 실행으로만 확인된 상태입니다.

## Squid 19.2.6의 RGW multisite 회귀

19.2.6에서는 secondary zone의 `radosgw-admin realm pull`이 master gateway에서 403으로
거부됩니다. debug log의 거부 이유는 `'x-amz-content-sha256' supplied, but not in
CanonicalHeaders`입니다. 같은 release의 `radosgw-admin`이 이 header를 보내면서
서명 대상에는 넣지 않고, RGW 서버는 그런 요청을 거부합니다. 이 검사를 끄는 설정은
없습니다. 19.2.5에서는 같은 fixture로 multisite 테스트 전체가 통과하므로 Ceph
19.2.6의 회귀로 판단합니다. Squid로 multisite를 테스트하려면 19.2.5를 사용합니다.
