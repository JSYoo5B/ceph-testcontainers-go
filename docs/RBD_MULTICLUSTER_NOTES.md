# RBD cluster 간 복사와 복구 PoC

[rbd_multicluster_integration_test.go](../rbd_multicluster_integration_test.go)는 FSID와 MON/MGR/OSD·네트워크·인증키가 서로 다른 두 Ceph cluster를 사용합니다. 각 cluster는 OSD 두 개를 갖고, 복사용 client와 mirror daemon만 양쪽 네트워크에 연결합니다. Go 호스트에는 `go-ceph`, cgo, Linux block-device mapping이 필요하지 않습니다.

## 전체 및 증분 backup

`TestMultiClusterRBDBackup`은 모든 byte가 0이 아닌 8 MiB image를 만들고 `baseline` snapshot을 저장합니다. `rbd export --export-format 2`로 image와 snapshot·image metadata를 함께 보관한 뒤, 1 MiB object 하나를 바꾸고 `next` snapshot까지의 `rbd export-diff --from-snap baseline`을 만듭니다. 생성한 두 archive를 Go를 통해 destination client로 복사하고 byte 단위로 확인합니다.

source MON/OSD를 멈춘 상태에서 destination에 전체 archive를 import하고, baseline을 바탕으로 증분 archive를 적용합니다. image head와 두 snapshot의 전체 8 MiB를 원본 fixture와 비교하고 metadata도 확인합니다. baseline이 없는 다른 image에는 증분 복구가 실패해야 합니다. 복구한 image를 다시 수정해도 과거 snapshot의 내용은 그대로 남아야 합니다.

이 테스트는 증분 archive가 전체 backup의 절반보다 작은지도 확인합니다. fixture의 write는 공개 [RBD diff v1 형식](https://docs.ceph.com/en/tentacle/dev/rbd-diff/)을 만들어 `rbd import-diff`로 실행하며, 실제 backup archive는 Ceph CLI가 생성합니다. 전체 format-2 backup과 증분 복구의 snapshot 의존성은 [rbd 공식 명령 문서](https://docs.ceph.com/en/tentacle/man/8/rbd/)에 정의돼 있습니다.

## Native snapshot mirroring

`TestMultiClusterRBDSnapshotMirror`은 양쪽 pool을 image mode로 설정하고 source bootstrap token을 destination에 `rx-only`로 import합니다. destination에서 실제 `rbd-mirror` daemon 하나를 실행합니다. daemon은 별도 `client.rbd-mirror.tc` 계정과 양쪽 MON/OSD에 접근 가능한 네트워크를 사용합니다.

source image에 snapshot mirroring을 활성화한 뒤 destination의 전체 image byte가 일치할 때까지 기다립니다. source의 1 MiB object를 바꾸고 명시적으로 mirror snapshot을 추가한 다음, 변경한 전체 8 MiB가 destination까지 도착하는지도 확인합니다. JSON info의 snapshot mode·primary 상태와 daemon status를 기록합니다.

source를 demote한 뒤 destination을 강제 옵션 없이 promote합니다. mirror daemon과 source MON/OS드를 멈춘 다음에도 destination에서 전체 내용을 읽고 새 데이터를 쓸 수 있어야 합니다. 이것은 계획된 failover 검증입니다. source MGR과 네트워크는 fixture cleanup까지 남아 있지만 source MON/OSD가 정지하므로 source 데이터는 읽거나 복제할 수 없습니다.

[Ceph RBD mirroring 문서](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/)가 설명하듯 snapshot mirroring은 mirror snapshot 시점의 비동기 복제입니다. 최신 checkpoint 이후 write의 복구, journal mirroring, 강제 promote에 따른 split-brain, failback, WAN 장애 정책, 애플리케이션의 multi-volume 일관성은 이 테스트 범위에 포함되지 않습니다.

## 실행

현재 다섯 slim 역할에는 선택 package인 `rbd-mirror` daemon이 포함되지 않습니다. Native mirror test는 기본적으로 고정된 원본 Quay image를 daemon에 사용하고, MON/MGR·client와 OSD에는 기존 이미지 환경변수를 적용합니다. 다른 Ceph 버전을 검증할 때는 그 버전의 daemon image를 `CEPH_TEST_MIRROR_IMAGE`에 지정합니다.

```sh
CGO_ENABLED=0 \
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd \
go test -tags='integration multicluster' -run '^TestMultiClusterRBD' \
  -count=1 -v -timeout=35m ./...
```

추가 daemon과 두 cluster가 필요한 PoC이므로 일반 `integration` tag의 단일 cluster 회귀 테스트와 분리했습니다. 동시에 실행하면 작은 Docker VM의 memory budget을 넘을 수 있어 테스트는 순차적으로 실행합니다. 실제 실행 결과와 소요 시간은 상위 multi-cluster PoC 보고서에 기록합니다.
