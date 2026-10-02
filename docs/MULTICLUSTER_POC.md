# 다중 클러스터 구성·전환·복제·백업 PoC

검증일: 2026-10-02, Asia/Seoul. Ceph 20.2.4의 **독립된 두 클러스터**에서 RGW multisite, RBD snapshot mirroring·전체/증분 백업 복원, CephFS snapshot mirroring·별도 archive 복원을 실행하고, 선택 정책과 연결 변경·site 전환·복구 시나리오를 추가했습니다. 이전 복제·백업 검증과 확장 검증의 결과를 구분하여 기록합니다. Native CephFS mirroring의 user xattr 불일치는 별도 제한입니다.

상위 API 이름을 `federation`에서 `multicluster`로 바꿨습니다. RGW realm/zone의 multisite·정책·metadata master 전환, RBD/CephFS mirroring·관계 변경, archive 백업·복원을 서비스별 책임으로 다룹니다. 두 클러스터의 MON quorum이나 RADOS pool을 합치는 공통 control plane은 구현하지 않습니다. RBD와 CephFS는 각자의 snapshot 복제 기능을 사용합니다. [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/), [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS snapshot mirroring](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/).

## 구성과 독립성

- macOS ARM64 호스트, Docker Desktop Linux ARM64, 4 vCPU와 약 3916 MiB RAM에서 순차 실행했습니다.
- 클러스터마다 MON 1개, MGR 1개, OSD 2개와 서로 다른 FSID·네트워크·admin keyring을 사용합니다. Client가 자신에게 배정된 FSID에 접속하는지도 확인합니다.
- MON/MGR/client, OSD, RGW, MDS는 각각 기존 `20.2.4-control`, `20.2.4-osd`, `20.2.4-rgw`, `20.2.4-mds` slim 이미지를 사용했습니다.
- 최초 PoC에서는 두 mirror 데몬만 원본 Quay 이미지를 사용했습니다. 현재는 mirror를 포함하도록 다시 빌드한 slim `control`로 실행하며 `all`에도 두 데몬이 있습니다.
- 검증용 data client는 자기 클러스터에만 접속합니다. Multicluster API가 mirror daemon과 필요한 관리 client의 양쪽 클러스터 접속을 소유합니다. RGW끼리는 별도 HTTP bridge로 통신합니다. CephFS는 peer 등록 시 원격 filesystem을 검사하는 source MGR에도 destination 네트워크가 필요합니다.
- 호스트에는 `go-ceph`, cgo, RBD kernel mapping이나 CephFS kernel/FUSE mount를 추가하지 않았습니다. 모든 Ceph 제어와 native client I/O는 컨테이너 안에서 실행하며 Go는 `CGO_ENABLED=0`입니다.

원본은 다음 digest입니다.

```text
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
```

Source outage 단계에서는 source MON/OSD와 해당 케이스의 RGW/MDS를 정지합니다. MGR과 네트워크는 cleanup까지 남아 있지만 source data daemon은 데이터를 제공할 수 없습니다. Destination의 새 CLI/libcephfs session으로 전체 payload를 다시 읽어 검증합니다. 종료 시 mirror·client와 추가 네트워크 연결을 먼저 정리하고 각 클러스터를 제거합니다.

`ManagerContainer()`는 초기 MGR의 호환 accessor이며 현재 후보와 active 상태는 `Managers()`·`ManagerStatus()`로 조회합니다. 컨테이너의 수명은 클러스터가 관리합니다. 현재 `multicluster.RunCephFSMirror`는 Docker SDK로 owned source MGR 후보들의 remote network를 연결하고 cleanup 시 자신이 추가한 연결만 해제합니다. 새 후보 추가 뒤에는 `AttachManagers()`로 재조정합니다. MGR 교체와 결합한 최신 검증은 [CLUSTER_SCENARIOS.md](CLUSTER_SCENARIOS.md)를 따릅니다.

## 구성 API

단일 클러스터는 `ceph.Run`, 클러스터 사이의 구성은 별도 `multicluster.RunRGWMultisite`·`RunRBDMirror`·`RunCephFSMirror`로 분리했습니다. Bootstrap/peer/auth/daemon 조립은 연결 API가, RBD full/incremental archive 전달은 백업·복원 helper가 담당합니다. 보관처와 데이터 검증, 전환·복구 순서는 호출자가 소유합니다. 연결의 cleanup은 클러스터보다 먼저 수행합니다. Ceph 내부 설정은 일회성 클러스터에 유지하며 데이터 삭제나 설정 롤백은 하지 않습니다. [책임·수명과 사용 예](MULTICLUSTER_API.md)를 확인합니다.

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

## Multicluster 시나리오 확장

상위 패키지 이름을 바꾸면서 복제 외의 정책·관계 변경·전환·복구를 추가했습니다. 같은 Ceph 20.2.4 slim 역할을 사용하고 추가 daemon은 `control`로 실행합니다. 기존 RGW 양방향 복제 케이스는 앞 절의 결과를 유지하며, 다음 확장 및 회귀 검증은 케이스별 Go 명령으로 순차 실행했습니다.

| 케이스 | 확인한 동작 | 결과·관측 시간 |
| --- | --- | --- |
| RGW 선택 정책 | 특정 bucket/prefix와 source → destination만 복제, 다른 bucket/prefix·반대 방향 제외, live prefix 변경, 선택된 삭제 전파·기존 객체 유지 | PASS, 338.12초 |
| RGW metadata master 전환·복귀 | metadata period/epoch·64 incremental shard와 catch-up 확인, 이전 gateway fencing, A → B → A 승격·realm 복귀, 각 master의 새 user/bucket/private object 복제, 기존 데이터 유지 | PASS, 406.88초 |
| RBD 백업 API | format-2 full·native incremental helper, source 정지 후 restore, head·두 snapshot·image metadata 일치, baseline 없는 restore 거부, 복원 후 쓰기 | PASS, 66.88초 |
| RBD 기본 snapshot mirror | 최초·변경 checkpoint의 전체 8 MiB, 강제 옵션 없는 demote/promote, source 정지 후 destination 읽기·쓰기 | PASS, 98.77초 |
| RBD planned failback | A → B → A, B-only 데이터·user snapshot의 A 복제 완료 후 강제 옵션 없는 A 재승격, 이후 A 변경의 B 복제, 양쪽 과거 snapshot의 전체 내용 유지 | PASS, 148.69초 |
| RBD split-brain 복구 | 양쪽 primary의 상충 이력, native `up+error / split-brain`, A 선택·B demote/resync, B branch 폐기·A snapshot 보존, 다음 checkpoint 복제 | PASS, 163.47초 |
| RBD peer lifecycle | peer 제거 후 기존 데이터 유지·새 checkpoint 중단, 새 UUID로 rebootstrap·명시적 resync, 후속 checkpoint 복제 | PASS, 151.40초 |
| CephFS directory·peer lifecycle + archive | directory 제거·재등록, peer 제거·새 UUID로 재등록, 기존 snapshot 보존·새 snapshot catch-up, 기존 restart·삭제·OSD 교체·source 정지·archive 복원 회귀 | PASS, 188.49초; native user xattr 차이 유지 |

선택되지 않은 RGW 객체는 positive 객체가 실제 도착한 뒤 35초 동안 반복 GET으로 `NoSuchKey`를 확인했습니다. CephFS는 daemon 정책 제거가 반영된 뒤 새 snapshot의 부재를 10초 동안 확인했습니다. RBD는 peer 목록이 빈 상태에서 기존 전체 image를 세 차례 비교하고 각 비교 뒤 2초씩 대기했습니다. 이 제한된 관측을 영구 부재나 WAN partition 보장으로 해석하지 않습니다.

RBD split-brain 복구는 상충된 데이터의 merge가 아닙니다. 테스트가 A를 authoritative로 선택하고 B의 변경과 `discarded-b` snapshot을 폐기하도록 요청합니다. 전체 8 MiB와 A snapshot을 비교한 뒤 새 checkpoint가 정상 복제되는 것까지 확인했습니다. Snapshot mirror는 애플리케이션의 지속적인 write를 자동으로 quiesce하거나 최신 checkpoint 이후 변경을 보호하지 않습니다.

RGW 전환은 애플리케이션 write가 없는 계획된 변경입니다. 승격 전 current period·realm epoch·incremental shard와 master의 metadata 상태를 비교하고 이전 gateway를 정지합니다. Ceph 20.2.4에서는 승격의 `period update`와 `period commit`을 분리해야 native guard가 이전 committed topology의 실제 sync 상태를 읽습니다. 복귀에는 `realm pull`로 활성 period 포인터와 local zonegroup을 함께 갱신합니다. `period pull`만으로는 이 갱신이 되지 않았습니다. 강제 승격 옵션은 사용하지 않았습니다. 두 번의 복귀 후 새 user의 private 객체가 도착하는 데 각각 약 2분 1.7초가 걸렸습니다. 이는 한 번의 관측값이며 자동 routing·자동 failover·RPO 보장이 아닙니다.

초기 RGW 승격은 combined period 명령의 빈 sync 상태 때문에 거부됐고, 다음 시도는 period 저장만으로 이전 site를 복귀시키지 못했습니다. RBD 초기 왕복 전환은 bootstrap이 재사용한 tx-only peer에 client 이름이 없어 실패했습니다. Native 명령의 실제 동작에 맞춰 수정하고 실패 케이스를 각각 재실행했습니다. API의 세부 절차는 [구성 문서](MULTICLUSTER_API.md)에 기록했습니다.

실행 로그와 최종 `summary.json`은 `artifacts/multicluster-scenarios-20.2.4/`에 보존합니다. 8개 케이스가 개별 최종 실행에서 PASS했고, 7개 Go 실행의 테스트 session label로 잔여 컨테이너·네트워크가 0개임을 확인했습니다. 실패한 시도도 별도 로그로 남기며, 최종 케이스 결과의 PASS는 같은 단일 전체 suite 실행에서 모든 케이스가 통과했다는 뜻이 아닙니다. `CGO_ENABLED=0` unit test·전체 integration tag vet, builder Python 문법·local Markdown link 검사도 통과했습니다. Native CephFS xattr 차이와 작은 archive fixture의 지원 범위는 앞 절과 같습니다.

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

별도 선택은 `go test -tags=integration,multicluster -run '^TestMultiClusterRBD' -count=1 -v -timeout=35m ./internal/integration`처럼 실행합니다. 다른 Ceph 버전에서는 모든 역할과 **`CEPH_TEST_MIRROR_IMAGE`**를 같은 검증 대상 버전으로 맞춥니다. Mirror image의 기본값은 `CEPH_TEST_IMAGE`로 선택한 control 이미지이며, 이 값도 없으면 고정 `DefaultImage`입니다.

`make slim-images-multicluster` 또는 빌더의 `--multicluster`는 새 이미지를 빌드하고 control을 mirror 이미지로 명시하여 이 suite를 실행합니다. 일반 `integration` 및 `slim-images-verify`는 추가 `multicluster` tag를 사용하지 않습니다. 두 클러스터와 선택 mirror image가 필요한 검증을 기존 단일 클러스터 회귀 테스트와 분리했습니다.

로컬 근거는 git에서 제외되는 `artifacts/multicluster-20.2.4/`에 있습니다. `suite.log`에는 RBD 두 경로와 RGW의 PASS 및 수정 전 CephFS 실패가, `cephfs-final.log`에는 수정 후 CephFS PASS와 실제 metadata 차이가 있습니다. 초기 RGW port 수정 전 실행은 `rgw.log`에 보존했습니다. 로그의 최종 케이스 결과를 정리한 `summary.json`도 남깁니다.

AMD64, WAN latency/partition, 버전 혼합, 자동 site failover, 다수 peer·mirror daemon의 HA, 전체 클러스터 설정/daemon directory 복원은 이번 검증 범위에 포함하지 않습니다.
