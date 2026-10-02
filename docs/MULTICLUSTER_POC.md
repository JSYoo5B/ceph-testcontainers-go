# 클러스터 간 복제와 백업 PoC

검증일: 2026-10-02, Asia/Seoul. Ceph 20.2.4의 **독립된 두 클러스터**에서 RGW multisite, RBD snapshot mirroring·전체/증분 백업 복원, CephFS snapshot mirroring·별도 archive 복원을 실제로 실행했습니다. 최종 케이스별 검증은 모두 통과했습니다. Native CephFS mirroring의 user xattr 불일치는 아래와 같이 별도 제한으로 기록합니다.

여기서 federation은 RGW realm/zone을 연결하는 multisite입니다. 두 클러스터의 MON quorum이나 RADOS pool을 하나로 합치는 기능을 검증한 것은 아닙니다. RBD와 CephFS는 각자의 snapshot 복제 기능을 사용합니다. [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/), [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS snapshot mirroring](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/).

## 구성과 독립성

- macOS ARM64 호스트, Docker Desktop Linux ARM64, 4 vCPU와 약 3916 MiB RAM에서 순차 실행했습니다.
- 클러스터마다 MON 1개, MGR 1개, OSD 2개와 서로 다른 FSID·네트워크·admin keyring을 사용합니다. Client가 자신에게 배정된 FSID에 접속하는지도 확인합니다.
- MON/MGR/client, OSD, RGW, MDS는 각각 기존 `20.2.4-control`, `20.2.4-osd`, `20.2.4-rgw`, `20.2.4-mds` slim 이미지를 사용했습니다.
- 최초 PoC에서는 두 mirror 데몬만 원본 Quay 이미지를 사용했습니다. 현재는 mirror를 포함하도록 다시 빌드한 slim `control`로 실행하며 `all`에도 두 데몬이 있습니다.
- 검증용 data client는 자기 클러스터에만 접속합니다. Federation API가 mirror daemon과 필요한 관리 client의 양쪽 클러스터 접속을 소유합니다. RGW끼리는 별도 HTTP bridge로 통신합니다. CephFS는 peer 등록 시 원격 filesystem을 검사하는 source MGR에도 destination 네트워크가 필요합니다.
- 호스트에는 `go-ceph`, cgo, RBD kernel mapping이나 CephFS kernel/FUSE mount를 추가하지 않았습니다. 모든 Ceph 제어와 native client I/O는 컨테이너 안에서 실행하며 Go는 `CGO_ENABLED=0`입니다.

원본은 다음 digest입니다.

```text
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
```

Source outage 단계에서는 source MON/OSD와 해당 케이스의 RGW/MDS를 정지합니다. MGR과 네트워크는 cleanup까지 남아 있지만 source data daemon은 데이터를 제공할 수 없습니다. Destination의 새 CLI/libcephfs session으로 전체 payload를 다시 읽어 검증합니다. 종료 시 mirror·client와 추가 네트워크 연결을 먼저 정리하고 각 클러스터를 제거합니다.

`ManagerContainer()`는 소유 MGR을 검사하거나 장애·네트워크 조건을 주입하기 위한 accessor입니다. 컨테이너의 수명은 여전히 클러스터가 관리합니다. `federation.RunCephFSMirror`가 Docker SDK로 source MGR의 remote network를 연결하고 cleanup 시 자신이 추가한 연결을 해제합니다.

## 구성 API

단일 클러스터는 `ceph.Run`, 클러스터 사이의 구성은 별도 `federation.RunRGWMultisite`·`RunRBDMirror`·`RunCephFSMirror`로 분리했습니다. 테스트에 있던 bootstrap/peer/auth/daemon 조립을 API로 옮겼으며 backup archive 전달과 데이터 검증은 테스트에 남겼습니다. 연결의 cleanup은 클러스터보다 먼저 수행합니다. Ceph 내부 설정은 일회성 클러스터에 유지하며 데이터 삭제나 설정 롤백은 하지 않습니다. [책임·수명과 사용 예](FEDERATION_API.md)를 확인합니다.

## 최초 PoC 결과

| 케이스 | 확인한 동작 | 최종 결과와 실행 시간 |
| --- | --- | --- |
| RGW multisite | 사용자·버킷·객체 복제, 양방향 PUT/DELETE, overwrite/delete 전파, primary gateway outage 중 secondary 읽기·쓰기, 재시작 catch-up, source MON/OSD/RGW 정지 후 secondary 읽기 | PASS, 326.72초 |
| RBD 전체/증분 백업 | format-2 export/import, snapshot·image metadata 보존, baseline 없는 증분 복원 거부, source 정지 후 destination restore·읽기·쓰기 | PASS, 62.36초 |
| RBD snapshot mirror | 실제 `rbd-mirror`, rx-only peer bootstrap, 최초·변경 checkpoint의 전체 8 MiB 일치, 강제 옵션 없는 demote/promote, source 정지 후 destination 읽기·쓰기 | PASS, 71.63초 |
| CephFS mirror + backup | 실제 `cephfs-mirror`, archive restore, mirror 중단/재시작 catch-up, snapshot 삭제 전파, destination OSD `2 → 3 → 2`, source 정지 후 새 session 읽기 | PASS, 140.69초; native user xattr 차이는 별도 기록 |

시간은 각 테스트의 생성·검증·cleanup을 포함한 한 번의 관측값이며 성능 기준이나 보장된 RPO가 아닙니다. 전체 초기 suite에서 CephFS의 빈 파일 write가 실패했고, 해당 fixture와 peer 네트워크를 수정한 뒤 CephFS를 별도로 재실행했습니다. 표의 네 케이스가 같은 단일 Go 실행에서 모두 PASS한 결과라고 해석하지 않습니다.

### RGW

두 클러스터가 하나의 realm을 공유하지만 zone ID는 서로 다릅니다. 일반 S3 user를 두 gateway 기동 후 primary에서 생성하고 secondary에서 같은 자격 증명으로 private 객체를 읽어 user·bucket metadata와 object data의 복제를 함께 확인했습니다. 호스트가 native replication 대상 객체를 destination에 복사하지 않습니다.

초기 객체 전파는 약 15초, overwrite/delete 및 반대 방향 변경은 약 20초였습니다. Primary gateway를 정지한 동안 secondary에서 생성한 객체는 primary 재시작 후 **약 2분 1.6초**에 도착했습니다. 재시작 시 mapped HTTP port를 다시 조회합니다. Metadata master 승격이나 master 부재 중 새 user/bucket 생성은 검증하지 않았습니다.

삭제도 복제됐습니다. 이 구성의 replica를 삭제 방지 백업으로 취급할 수는 없습니다.

### RBD

모든 byte가 0이 아닌 8 MiB image에서 전체 backup은 **8,389,068 bytes**, 한 1 MiB object를 바꾼 뒤의 증분 backup은 **1,048,637 bytes**였습니다. 실제 backup은 Ceph CLI의 `export --export-format 2`와 `export-diff --from-snap baseline`이 생성합니다. 범위 write용 diff stream은 테스트 입력을 만드는 도구이며 backup encoder를 대신하지 않습니다.

전체 archive와 증분 archive를 destination client로 전달한 뒤 source MON/OSD를 정지하고 복원했습니다. Head·baseline·next snapshot의 전체 byte와 image metadata를 확인했습니다. Destination-only write 후에도 복원한 과거 snapshot의 내용이 유지됐습니다.

Native 경로에서는 destination mirror daemon이 직접 source MON/OSD에서 데이터를 읽습니다. Source를 demote한 후 destination을 강제 옵션 없이 promote하고 source와 mirror를 정지한 상태에서 destination에 새 데이터를 썼습니다. Journal mirroring, 강제 승격에 따른 split-brain 복구와 failback은 범위 밖입니다. [세부 설계와 공식 자료](RBD_MULTICLUSTER_NOTES.md).

### CephFS

9개 tree entry와 5개 file로 구성한 fixture에 nested directory, UTF-8/공백 이름, binary/empty file, relative symlink, mode, numeric UID/GID와 user xattr를 넣었습니다.

Native mirror는 byte·SHA-256·tree 이름·symlink target·mode·UID/GID를 엄격히 비교했습니다. **Root와 `nested/deeper/자료.bin`의 user xattr는 원본과 일치하지 않았습니다.** 이 차이는 `user_xattr_difference_paths`로 기록했으며 native mirroring의 metadata 완전 보존을 주장하지 않습니다. 별도의 application archive restore는 같은 user xattr까지 엄격한 비교를 통과했습니다.

Mirror를 정지한 동안 파일 변경·삭제·rename·추가와 두 번째 snapshot을 만들고, 재시작 후 최신 snapshot의 byte와 이전 snapshot의 불변성을 확인했습니다. Source snapshot 삭제는 destination에도 전파됐지만, 별도로 복원한 `/restored` directory는 첫 백업의 원래 내용을 유지했습니다. Destination OSD 교체 후 source MON/MDS/OSD와 mirror를 정지하고 두 경로를 새 session에서 다시 읽었습니다.

Application archive는 JSON/base64의 작은 fixture format입니다. 일반 backup 제품이나 전체 filesystem backup을 구현한 것이 아니며 hardlink 관계, ACL, sparse extent, timestamps, layout/quota는 검증하지 않았습니다. [세부 구성](CEPHFS_MULTICLUSTER_NOTES.md).

## mirror 포함 slim과 분리 API 재검증

새 `control`/`all`에 mirror를 포함한 뒤 `federation` API를 사용하여 혼합 slim 역할을 다시 실행했습니다. 선택 daemon도 원본 Quay fallback 없이 `20.2.4-control`을 사용합니다. RGW 단독 실행과 나머지 세 케이스 실행, 총 두 Go 명령에서 네 케이스가 모두 PASS했습니다.

| 케이스 | 결과 | 관측 시간 |
| --- | --- | ---: |
| `RunRGWMultisite` | 양방향 객체 복제·삭제, outage/restart, source 중단 후 읽기 PASS | 349.35초 |
| RBD 전체/증분 backup | source 중단 후 archive 복원·읽기·쓰기 PASS | 62.41초 |
| `RunRBDMirror` | 최초·변경 snapshot 복제, demote/promote, source 중단 후 읽기·쓰기 PASS | 92.55초 |
| `RunCephFSMirror` + archive | mirror restart·삭제 전파, OSD 교체, source 중단 후 읽기 PASS | 137.16초 |

CephFS native user xattr 차이는 앞의 두 경로에서 다시 관측됐고 별도 archive restore는 xattr까지 일치했습니다. 시간이 기존 실행과 다르므로 성능 개선/저하나 RPO 보장으로 해석하지 않습니다. `CGO_ENABLED=0` unit test, 전체 tag compile, vet, 다섯 image smoke와 layer 공유 검증도 통과했습니다. 생성한 컨테이너와 네트워크가 남지 않은 것을 확인했습니다. Cleanup의 재시도·이미 삭제된 리소스·실제 오류가 섞인 joined error 경로는 별도 단위 테스트로 확인했습니다.

실행 로그와 이미지 ID·case 결과·resource audit는 `artifacts/federation-slim-20.2.4/{rgw.log,mirrors-backup.log,summary.json}`에 있습니다. Mirror 포함 빌드 기록은 `artifacts/slim-mirror-20.2.4/`입니다. 이 artifact 디렉터리들은 git에서 제외합니다.

## 재현

기존 고정 Quay 이미지로 모든 역할을 실행할 때는 다음 명령을 사용합니다.

```sh
make multicluster
```

현재 검증한 slim 역할 조합은 다음과 같습니다.

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd \
CEPH_TEST_RGW_IMAGE=ceph-testcontainers:20.2.4-rgw \
CEPH_TEST_MDS_IMAGE=ceph-testcontainers:20.2.4-mds \
make multicluster
```

별도 선택은 `go test -tags=integration,multicluster -run '^TestMultiClusterRBD' -count=1 -v -timeout=35m ./...`처럼 실행합니다. 다른 Ceph 버전에서는 모든 역할과 **`CEPH_TEST_MIRROR_IMAGE`**를 같은 검증 대상 버전으로 맞춥니다. Mirror image의 기본값은 `CEPH_TEST_IMAGE`로 선택한 control 이미지이며, 이 값도 없으면 고정 `DefaultImage`입니다.

`make slim-images-multicluster` 또는 빌더의 `--multicluster`는 새 이미지를 빌드하고 control을 mirror 이미지로 명시하여 이 suite를 실행합니다. 일반 `integration` 및 `slim-images-verify`는 추가 `multicluster` tag를 사용하지 않습니다. 두 클러스터와 선택 mirror image가 필요한 검증을 기존 단일 클러스터 회귀 테스트와 분리했습니다.

로컬 근거는 git에서 제외되는 `artifacts/multicluster-20.2.4/`에 있습니다. `suite.log`에는 RBD 두 경로와 RGW의 PASS 및 수정 전 CephFS 실패가, `cephfs-final.log`에는 수정 후 CephFS PASS와 실제 metadata 차이가 있습니다. 초기 RGW port 수정 전 실행은 `rgw.log`에 보존했습니다. 로그의 최종 케이스 결과를 정리한 `summary.json`도 남깁니다.

AMD64, WAN latency/partition, 버전 혼합, 자동 site failover, 다수 peer·mirror daemon의 HA, 전체 클러스터 설정/daemon directory 복원은 이번 검증 범위에 포함하지 않습니다.
