# CephFS 클러스터 간 snapshot 복제와 backup PoC

`TestMultiClusterCephFSSnapshotMirrorAndBackup`은 서로 다른 FSID, MON/MGR,
OSD, MDS와 CephX 자격 증명을 가진 두 클러스터를 만든다. 데이터 검증용 userspace
client는 각각 자기 클러스터에 접속하고 source MGR와 하나의 mirror daemon은 두 Docker network에 접속한다. 호스트의
kernel/FUSE mount, go-ceph, cgo는 사용하지 않는다.

## 서로 다른 두 가지 경로

**Application backup/restore:** source의 CephFS directory snapshot을
container 안의 Python `libcephfs`로 읽어 JSON/base64 fixture archive로
만들고, destination container로 전달해 별도 `/restored` directory에 쓴다.
파일마다 SHA-256을 검증하고 전체 tree를 다시 읽어 원본과 비교한다. 이 경로는
native mirroring과 별개이며 일반적인 backup 도구를 구현한 것은 아니다.

검증 fixture에는 nested directory, UTF-8 및 공백을 포함한 이름, binary와
empty file, relative symlink, permission mode, numeric UID/GID, `user.*`
xattr가 포함된다. hardlink 관계, ACL, sparse extent, timestamps, quota와
Ceph layout 등은 fixture의 검증 범위 밖이다.

**Native snapshot mirroring:** source의 MGR `mirroring` module을 켜고
destination에서 `rwps` 권한을 갖는 peer bootstrap token을 발급한다.
source에서 이를 import하고 `/federation`을 mirror directory로 등록한다.
별도 `cephfs-mirror` daemon이 destination에 실제 snapshot을 만든다.
source MGR도 peer 등록 시 destination filesystem에 연결하여 FSID와 filesystem
ID를 확인하고 root의 `ceph.mirror.info`를 기록하므로 원격 network 접근이 필요하다.
`multicluster.RunCephFSMirror`가 Docker SDK로 기존 MGR에 destination network를
추가하고, cluster network 삭제 전에 자신이 추가한 연결을 해제한다.
고정 [이미지 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)에 따라
`control`과 `all`은 `cephfs-mirror`를 포함해야 한다. 통합 테스트의 mirror 이미지는
source 클러스터의 `ControlImage()`를 사용한다.
단일 클러스터와 복제 연결의 수명은 [API 계약](MULTICLUSTER_API.md)처럼 구분한다.

native 검증은 파일 bytes와 SHA-256, tree의 이름/삭제, symlink target,
permission mode와 UID/GID를 엄격하게 비교한다. user xattr는 별도 비교하여
`user_xattr_difference_paths`로 로그에 남긴다. Application backup의 xattr
보존 결과를 native mirroring의 보장으로 취급하지 않는다. 실제 차이와 실행
결과는 최종 `MULTICLUSTER_POC` 보고서와 runtime log를 기준으로 판단한다.

## 장애 및 독립성 검증

1. 첫 snapshot을 복제하고 source와 destination에서 읽은 fixture를 비교한다.
2. mirror daemon을 정지한 동안 파일 수정/삭제/이름 변경/추가와 두 번째 snapshot을 만든다.
3. daemon을 재시작해 두 번째 snapshot의 catch-up과 첫 snapshot의 불변성을 검사한다.
4. source에서 첫 snapshot을 삭제하고 destination에서도 삭제되는지 기다린다.
5. mirror directory를 제거하고 daemon의 directory count 반영을 확인한다. 새 snapshot이
   10초 관측 동안 도착하지 않고 기존 snapshot이 유지되는지 확인한 뒤 directory를 다시 등록해 catch-up을 검증한다.
6. peer를 제거해 API와 daemon에서 UUID가 사라진 것을 확인한다. 새 snapshot이
   10초 관측 동안 도착하지 않고 기존 데이터가 유지되는지 확인한 뒤 새 UUID로 peer를 재등록해 catch-up을 검증한다.
7. destination OSD를 추가/제거한 뒤 source MON/MDS/OSDs와 mirror를 정지한다.
8. 새로운 destination `libcephfs` session으로 두 번째·네 번째 mirrored snapshot과
   첫 snapshot에서 별도로 복구한 `/restored`를 읽는다.

첫 번째 native snapshot `backup-1`은 source에서 삭제하면 destination에서도 삭제된다. 따라서 이
경로의 복제본과 별도 보존 정책을 가진 backup archive를 구분해야 한다.

`RebootstrapPeer`는 제거한 동일 destination을 다시 연결한다. Import의 결과가 불확실할 때는
이 연결에 pending import가 있는 경우에만 client/site/filesystem identity가 정확히 일치하는
하나의 peer UUID를 채택한다. 다른 연결이 이미 만든 peer를 임의로 소유하지 않는다.

## 실행

```sh
CGO_ENABLED=0 go test -tags='integration multicluster' \
  -run '^TestMultiClusterCephFS' -count=1 -v -timeout=20m ./internal/integration
```

PoC는 하나의 source/destination peer와 mirror daemon을 사용한다. active-active
쓰기, 자동 site failover, mirror daemon의 다중 인스턴스 HA를 검증하지 않는다.
Destination mirror directory에는 PoC 자체가 별도 파일이나 snapshot을 만들지 않는다.

구성 근거: [Ceph Tentacle의 CephFS Snapshot Mirroring 문서](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/).
이 문서는 native snapshot mirroring의 interface, 지원 파일 종류와 snapshot
동기화 동작을 설명한다. xattr 보존 여부는 실제 fixture 비교로 확인한다.
