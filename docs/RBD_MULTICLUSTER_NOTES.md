# RBD cluster 간 복사와 복구 PoC

[rbd_multicluster_integration_test.go](../internal/integration/rbd_multicluster_integration_test.go)는 FSID와 MON/MGR/OSD·네트워크·인증키가 서로 다른 두 Ceph cluster를 사용합니다. 각 cluster는 OSD 두 개를 갖고, mirror 연결이 소유한 관리 client와 mirror daemon만 양쪽 네트워크에 연결합니다. 데이터 검증 client는 자기 클러스터에만 접속합니다. Go 호스트에는 `go-ceph`, cgo, Linux block-device mapping이 필요하지 않습니다.

## 전체 및 증분 backup

`TestMultiClusterRBDBackup`은 모든 byte가 0이 아닌 8 MiB image를 만들고 `baseline` snapshot을 저장합니다. `rbd export --export-format 2`로 image와 snapshot·image metadata를 함께 보관한 뒤, 1 MiB object 하나를 바꾸고 `next` snapshot까지의 `rbd export-diff --from-snap baseline`을 만듭니다. `rbd.ExportBackup`·`rbd.ExportIncremental`로 archive를 보관하고, source 정지 후 `rbd.RestoreBackup`·`rbd.RestoreIncremental`로 destination에 전달하고 복원합니다. 임시 archive는 helper가 정리하며, 보관처와 클라이언트는 호출자가 소유합니다.

source MON/OSD를 멈춘 상태에서 destination에 전체 archive를 import하고, baseline을 바탕으로 증분 archive를 적용합니다. image head와 두 snapshot의 전체 8 MiB를 원본 fixture와 비교하고 metadata도 확인합니다. baseline이 없는 다른 image에는 증분 복구가 실패해야 합니다. 복구한 image를 다시 수정해도 과거 snapshot의 내용은 그대로 남아야 합니다.

이 테스트는 증분 archive가 전체 backup의 절반보다 작은지도 확인합니다. fixture의 write는 공개 [RBD diff v1 형식](https://docs.ceph.com/en/tentacle/dev/rbd-diff/)을 만들어 `rbd import-diff`로 실행하며, 실제 backup archive는 Ceph CLI가 생성합니다. 전체 format-2 backup과 증분 복구의 snapshot 의존성은 [rbd 공식 명령 문서](https://docs.ceph.com/en/tentacle/man/8/rbd/)에 정의돼 있습니다.

## Native snapshot mirroring

`TestMultiClusterRBDSnapshotMirror`은 양쪽 pool을 image mode로 설정하고 source bootstrap token을 destination에 `rx-only`로 import합니다. destination에서 실제 `rbd-mirror` daemon 하나를 실행합니다. daemon은 별도 `client.rbd-mirror.tc` 계정과 양쪽 MON/OSD에 접근 가능한 네트워크를 사용합니다.

source image에 snapshot mirroring을 활성화한 뒤 destination의 전체 image byte가 일치할 때까지 기다립니다. source의 1 MiB object를 바꾸고 명시적으로 mirror snapshot을 추가한 다음, 변경한 전체 8 MiB가 destination까지 도착하는지도 확인합니다. JSON info의 snapshot mode·primary 상태와 daemon status를 기록합니다.

source를 demote한 뒤 destination을 강제 옵션 없이 promote합니다. mirror daemon과 source MON/OS드를 멈춘 다음에도 destination에서 전체 내용을 읽고 새 데이터를 쓸 수 있어야 합니다. 이것은 계획된 failover 검증입니다. source MGR과 네트워크는 fixture cleanup까지 남아 있지만 source MON/OSD가 정지하므로 source 데이터는 읽거나 복제할 수 없습니다.

[Ceph RBD mirroring 문서](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/)가 설명하듯 snapshot mirroring은 mirror snapshot 시점의 비동기 복제입니다. 최신 checkpoint 이후 write의 복구, journal mirroring, 강제 promote에 따른 split-brain, failback, WAN 장애 정책, 애플리케이션의 multi-volume 일관성은 이 테스트 범위에 포함되지 않습니다.

## 전환·복구·peer 변경 확장

[rbd_multicluster_scenarios_integration_test.go](../internal/integration/rbd_multicluster_scenarios_integration_test.go)는 위의 기본 mirror와 별도로 세 경로를 검증합니다.

- `TestMultiClusterRBDFailback`: 고정 site 이름과 양쪽 receiver를 구성하고 A → B → A 순서로 강제 옵션 없이 demote/promote합니다. B에서 쓴 데이터와 user snapshot이 A에 도착한 뒤 A를 다시 승격하고, 이후 A의 변경이 B에 복제되는지 확인합니다.
- `TestMultiClusterRBDSplitBrainResync`: 일회성 image를 일부러 양쪽 primary로 만들고 서로 다른 데이터와 snapshot을 생성합니다. Ceph의 `up+error / split-brain`을 확인한 뒤 A를 기준으로 B를 demote·resync합니다. B의 상충된 변경과 snapshot은 폐기되고, 이후 새 checkpoint가 다시 복제돼야 합니다.
- `TestMultiClusterRBDPeerLifecycle`: destination peer를 제거하고 daemon을 실행해도 새 checkpoint가 도착하지 않는지 제한된 시간 동안 읽습니다. 새 UUID로 peer를 재등록하고 명시적으로 resync한 뒤 후속 checkpoint를 검증합니다.

Ceph 20.2.4의 bootstrap import는 기존 peer를 재사용할 때 direction과 client 이름을 모두 채우지는 않습니다. `rbd.RunMirror`/`Rebootstrap`은 token의 source FSID와 client ID 및 실제 pool mirror UUID로 해당 peer를 확인하고, 인증 client를 갱신한 뒤 기존 tx-only peer를 rx-tx로 확장합니다. Native bootstrap이 monitor 주소와 key를 설정하며 secret을 로그에 출력하지 않습니다. 이 구성은 자동 failover나 상충된 데이터의 merge를 제공하지 않습니다. 실제 결과는 [PoC 보고서](MULTICLUSTER_POC.md)를 확인합니다.

## 실행

고정 [이미지 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)에 따라 `control`과 `all`은 `rbd-mirror`를 포함해야 합니다. Native mirror test는 `rbd.RunMirror`로 bootstrap/auth/client/daemon을 구성하며 source 클러스터의 `ControlImage()`를 사용합니다. 모든 역할의 Ceph 버전·architecture를 맞춥니다. 복제 연결의 수명은 클러스터와 분리합니다. [API 계약](MULTICLUSTER_API.md)을 확인합니다.

```sh
CGO_ENABLED=0 \
CEPH_TEST_IMAGE=ceph-testcontainers:official-20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:official-20.2.4-osd \
go test -tags='integration multicluster' -run '^TestMultiClusterRBD' \
  -count=1 -v -timeout=35m ./internal/integration
```

추가 daemon과 두 cluster가 필요한 PoC이므로 일반 `integration` tag의 단일 cluster 회귀 테스트와 분리했습니다. 동시에 실행하면 작은 Docker VM의 memory budget을 넘을 수 있어 테스트는 순차적으로 실행합니다. 실제 실행 결과와 소요 시간은 상위 multi-cluster PoC 보고서에 기록합니다.
