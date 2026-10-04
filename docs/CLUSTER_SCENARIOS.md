# 테스트용 클러스터 구성 목표와 검증

이 문서는 기존 토폴로지 단계의 기준·증거와 현재 원본 Quay 검증 목표를 기록합니다. 클러스터 내부 리소스·정책 API의 제공 범위와 검증은 [CLUSTER_INTERNAL_FEATURES.md](CLUSTER_INTERNAL_FEATURES.md)에 정리합니다.

목표는 **클라이언트 테스트에 필요한 Ceph 토폴로지를 testcontainers로 생성하고, 구성 요소의 추가·교체·중단·복구와 클러스터 간 연결이 가능한지** 확인하는 것입니다. CRUSH rule·EC·pool 정책·권한과 개별 RADOS/RBD/CephFS/S3 기능은 후속 확장으로 둡니다. 이들 기능의 제공 여부는 완료 조건에 포함하지 않습니다. go-ceph와 다른 native client의 읽기·쓰기는 구성의 연결성을 확인하는 증거로 사용합니다.

공개 모듈은 CLI/파일로 제어하며 cgo에 의존하지 않습니다. go-ceph 소비자 테스트는 별도 Linux 전용 모듈에 둡니다. 기본 bridge에서는 클러스터별 전용 네트워크를 생성하고, 애플리케이션은 `WithClient`로 해당 네트워크에 연결합니다. host mode에서는 서로 다른 FSID·키와 자동 선택 MON/RGW 포트를 사용합니다. RADOS/RBD/CephFS 클라이언트는 MON뿐 아니라 광고된 OSD/MDS 주소에도 도달해야 합니다.

## 원본 pinned Quay 검증 목표

현재 필수 기준은 `ceph.DefaultImage`의 원본 Ceph 20.2.4입니다. Slim·회사 `.deb`·native 패치 빌드는 선택 도구이며 이 검증의 사전 조건이 아닙니다. 아래의 기존 성공 기록은 대부분 Quay-derived 역할별 slim 이미지와 Docker Desktop Linux ARM64의 관측입니다. 같은 source에서 추출했다는 사실만으로 원본 전체 이미지의 새 실행을 PASS 처리하지 않습니다.

새 검증은 다음 대표 범위를 유지합니다. `topology-smoke`의 3 MON/2 MGR 및 RGW 2 zone만으로 전체 목표를 닫지 않습니다. 아래 runtime 성공 표는 IPAM 수정 전 기본·r2 배치입니다. 후속 `43099aa`의 Linux AMD64 CI에서도 같은 기본 14개와 토폴로지 38개 전체를 통과했으며, 이후 RGW 공개망 우선순위 수정의 재검증은 별도 기록합니다.

| 필수 실행 경로 | 대표 구성·변경 기준 | 새 원본 Quay 실행 상태 |
|---|---|---|
| `make check` | unit·race·vet·전체 tag compile, Python 이미지 도구의 host guard. Ceph native build나 이미지 생성은 실행하지 않음 | MON·IPAM 수정 후 PASS. Python guard 63개. Runtime 증거와 별도 |
| `make quay-default` | 기본 MON/MGR/OSD와 OSD 추가·제거·데이터 유지, RGW/RBD/CephFS 연결, bootstrap 실패 cleanup | PASS: 14개 top-level test, 725.243초, Linux ARM64 |
| `make quay-topology` | 3 MON quorum 상실·복구·교체와 active MGR failover, 동적 MGR 증감, 여러 FS/multi-active MDS/standby/replay와 MDS scale, 초기 Run composition과 같은 zone의 RGW 증감·교체 | 수정 후 PASS: 8/8 named test, profile elapsed 909.566초, Linux ARM64. Owned container/network 0개 |
| `make quay-multicluster-topology` | 독립 host cluster 두 개·MON 포트 충돌 재시도·RGW endpoint 분리, RBD snapshot pair·journal 전환·3-cluster fanout·peer 제거/재등록·backup/restore, CephFS pair·MGR HA 연결, RGW 2/3 zone 및 metadata master 전환·복귀. 실제 FSID·key·peer/zone graph와 데이터 유지 | PASS: 18/18 named test, Go 4122.876초 / profile elapsed 4125.396초, Linux ARM64. Metadata master A→B→A·RBD peer 제거/재등록 포함. Owned container/network 0개 |
| `make quay-topology-extensions` | 5 MON quorum, public/backend 분리·endpoint 단절/복구, RBD/CephFS 복수 mirror daemon 증감·HA, RGW 초기/동적 여러 zonegroup·zone 탈퇴, 세 서비스의 peer 단절·catch-up | PASS: 12/12 named test, Go 2345.826초 / profile elapsed 2349.432초, Linux ARM64. Mirror daemon·RGW zonegroup bridge/host와 peer 단절·복구 포함. Owned container/network 0개 |

각 runtime profile은 daemon/mirror 이미지 환경 변수 다섯 개를 해제하여 원본 Quay를 직접 선택하며, 테스트와 cluster를 순차 실행합니다. 성공은 요청한 native identity·map·peer graph, 실제 client I/O 또는 복제 bytes, 변경 후 보존·복구, owned cleanup으로 확인합니다. 기존 artifact나 tag compile을 새 runtime PASS로 대체하지 않으며 실제 실행 결과·이미지·platform·로그를 이 절에 추가합니다. Linux AMD64 CI 등록 자체도 해당 환경의 관측 PASS가 아닙니다.

2026-10-04 새 원본 실행은 기존 `golang:1.27.1` 컨테이너의 host network에서 수행하며 서버·native client는 모두 고정 Quay 이미지를 사용합니다. Docker Desktop Linux ARM64, Engine 29.8.1/API 1.55, 4 CPU/3916 MiB 환경입니다. 새 서버 이미지나 Ceph source 빌드는 실행하지 않았습니다. Digest `6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`는 AMD64·ARM64 manifest를 포함한 upstream OCI index입니다. 이 실행의 platform은 ARM64이며 manifest 존재만으로 AMD64 실행 성공을 주장하지 않습니다.

기본 suite의 이전 실패와 수정 후 성공을 구분합니다.

| 로컬 로그 | 실제 결과와 조치 |
|---|---|
| `artifacts/quay-baseline-20261004/integration.log` | 702.902초 FAIL. Host-network RGW는 `127.0.0.1`에서 실제 200 응답했으나 container 안의 Go provider가 host를 `172.17.0.1`로 추론해 첫 signed PUT이 실패. Runner 시작부터 `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`을 지정 |
| `artifacts/quay-baseline-20261004/integration-final.log` | 905.002초 FAIL. Host RGW 접속은 통과했지만 pool 정책 bridge test가 CRUSH weight를 원래 값 대신 `1`로 복원하여 `active+clean+remapped`에서 대기. Native CRUSH 16.16 weight를 저장·정확히 복원하는 test 수정으로 해결하며 timeout·clean·데이터 판정은 유지 |
| `artifacts/quay-baseline-20261004/pool-policy-fix.log` | 144.46초 PASS, bridge/host 모두 원래 `65/65536` weight와 zero-weight fault·복원 readback, replica·quota·pool identity·실제 bytes 확인 |
| `artifacts/quay-baseline-20261004/quay-default.log` | 전체 14개 top-level test, 725.243초 PASS. RGW/RBD/CephFS 및 기본 topology의 최종 원본 실행 |
| `artifacts/quay-baseline-20261004/quay-topology.log` | 871.027초 FAIL. 8개 중 7개 PASS. `TestMonitorManagerTopology/bridge`는 quorum 상실·복구와 같은 native session의 I/O, 교체 MON 가입 및 기존 MON의 native membership 제거·컨테이너 정리까지 완료했으나 제거 직후 단발 CLI `quorum_status`의 5초 접속 제한에 걸림. 동일 host case는 PASS. API를 수정하여 기존 startup deadline 안에서 제거 대상의 monmap/quorum 부재와 surviving majority를 조회하고 설정 복사 후 소유권 제거를 확정. Membership mutation·timeout 수치·이미지는 유지 |
| `artifacts/quay-validation-20261004-r2/monitor-manager-fix.log` | 수정 후 377.730초 PASS. Bridge 173.33초, host 204.39초 모두 MON 3→4→3 교체·quorum 상실/복구, 같은 native 연결과 새 연결의 bytes, active MGR failover와 durable volumes 상태 확인. Owned container/network 0개 |
| `artifacts/quay-validation-20261004-r2/quay-topology.log` | 전체 핵심 8/8 named test PASS, profile elapsed 909.566초. 일반/replay MDS 증감·multi-active/standby 장애 복구·여러 filesystem 독립성, 초기 구성·동적 MGR 변경, RGW bridge/host 증감·교체, MON/MGR bridge/host native 연결·상태 보존 확인. Owned container/network 0개 |
| `artifacts/quay-validation-20261004-r2/quay-multicluster-topology.log` | 전체 18/18 named test PASS, Go 4122.876초 / profile elapsed 4125.396초. 독립 FSID·peer/zone ID·endpoint와 실제 복제 데이터, RBD fanout·journal 전환·peer 재등록·backup, CephFS MGR HA·mirror/backup, RGW 2/3-zone·metadata master 전환/복귀, host MON 포트 충돌 재시도 확인. Owned container/network 0개 |
| `artifacts/quay-validation-20261004-r2/quay-topology-extensions.log` | 전체 12/12 named test PASS, Go 2345.826초 / profile elapsed 2349.432초. Public/backend 분리와 5 MON quorum 격리·복구, RBD/CephFS mirror daemon 증감·HA, RGW 초기/동적 zonegroup·zone 제거, peer 단절·catch-up 확인. Owned container/network 0개 |

`artifacts/quay-baseline-20261004/profiles-summary.json`은 각 명령의 exit code·case 수·elapsed와 testcontainers session label별 잔여 리소스를 기록합니다. 위 수정 검증과 최종 기본 suite의 owned container·network는 각각 0개입니다. `runtime-source-manifest.json`, `images.json`, `docker-info.json`, `quay-manifest.txt`로 실행 소스·기존 이미지·platform을 확인하며 artifacts는 Git에서 제외합니다. CRUSH fixture 수정은 commit `04cf65e`, MON 제거 수정은 `7b2339b`에 포함되어 있습니다. MON 수정과 후속 suite는 `artifacts/quay-validation-20261004-r2/profiles-summary.json` 및 각 `*-source.json`에 별도 기록합니다. CI는 Linux AMD64에서 같은 Quay target들을 실행하도록 구성했으며 진행 중인 profile은 완료 증거로 합산하지 않습니다.

r2 MON 회귀·core 실행은 `04cf65e` worktree에 MON 수정을 적용한 상태에서 시작했으며, 그 수정이 이후 `7b2339b`에 커밋됐습니다. Git revision 문자열만으로 실행 소스를 추정하지 않고 manifest에 담긴 runtime input 185개의 이름·SHA를 `5009662` 시점의 파일과 대조하여 모두 일치함을 확인했습니다. Multicluster profile은 `7b2339b`, extensions는 `5009662` 이후 시작했으며 같은 185개 입력을 사용했습니다. 세 topology profile은 exit 0, selector·RUN·PASS·summary의 이름 집합 일치와 owned cleanup 0개까지 확인했습니다.

이전 기본 14개 배치는 MON 수정 전 실행입니다. 해당 source manifest와 `5009662` 시점의 입력은 `ceph/topology.go`, `ceph/monitor_lifecycle_test.go` 두 파일이 다르며, 기본 14개와 r2 topology 38개를 같은 revision의 단일 실행으로 표현하지 않습니다. 기본 14개에는 native/runtime 11개와 S3 signer helper 3개가 포함됩니다. `5009662` source의 기본 profile은 별도 원격 CI에서 PASS를 확인했습니다.

원격 CI는 실행별로 구분합니다. [`04cf65e` 실행](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37155507293)은 static·기본·core·multicluster job이 PASS였으나 extensions job이 FAIL하여 전체 run도 FAIL로 종료했습니다. 익명 API와 공개 UI에서는 상세 test 로그를 읽을 수 없고 annotation도 exit code 2만 제공하므로 이 실패의 testcase나 원인을 단정하지 않습니다. [`7b2339b` 실행](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37157646300)은 별도 실행이며 static·기본·core·multicluster job이 PASS였으나 extensions job이 FAIL하여 전체 run도 FAIL로 종료했습니다. 이 실행의 공개 annotation도 exit code 2만 제공하며 testcase와 원인은 확인되지 않았습니다. 각 run의 `ci-observation.json`과 이전 실패의 `ci-public-failure-111300323675.json` 및 `7b2339b` 실패의 `ci-public-failure-111307017841.json`을 보존하며, 이전 결과나 CI 등록만으로 현재 source의 전체 PASS를 선언하지 않습니다.

Commit `5009662`는 확장 CI job에서 완료된 Go 실패 testcase/subtest 이름만 공개 annotation으로 기록하는 진단 변경입니다. `shell: bash`의 pipefail로 `tee`가 test 실패를 가리지 않으며 reporter가 기존 실패 status를 성공으로 바꾸지 않습니다. 서버 이미지·Go API·Makefile selector와 runtime input 185개의 SHA는 유지했습니다. 해당 push의 [CI 실행](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37161683057)은 이전 실행과 별도로 확인합니다. 2026-10-04 00:57 UTC 최종 관측에서 static·기본 runtime·핵심 topology·multicluster job은 PASS였지만 extensions job이 FAIL하여 전체 run도 FAIL로 종료했습니다. 확인된 실패 testcase는 아래에 기록합니다. 최종 관측은 `artifacts/quay-ci-diagnostics-20261004/ci-observation.json`, 앞선 중간 관측은 `ci-live-verification-0023.json`에 보존합니다.

`5009662`의 공개 annotation으로 확인한 실패는 `TestMultiClusterCephFSMirrorDaemonRebalanceTopology`, `TestSeparateClusterNetworksAndInterruptions`, `TestFiveMonitorQuorumAndNetworkRecovery`, `TestMultiClusterRBDPeerNetworkInterruption`, `TestMultiClusterRGWPeerNetworkTopology`입니다. 다섯 케이스는 모두 bridge endpoint 단절·원래 IP로의 복구를 실행합니다. 상세 CI 오류 본문은 아직 접근할 수 없으므로 annotation만으로 원인을 확정하지 않습니다. 원본 필드와 head SHA는 `artifacts/quay-ci-diagnostics-20261004/ci-public-failure-111318654085.json`에 보존합니다.

별도 Docker Engine 28.0.4/API 1.48 Linux ARM64 재현에서 자동 IPAM bridge의 endpoint를 원래 IP로 다시 연결하면 `user configured subnets` 조건으로 거부됐고, subnet을 명시한 bridge에서는 같은 주소·alias로 복구했습니다. `artifacts/quay-network28-20261004/summary.json`과 `reconnect-probe.log`에 결과를 기록했습니다. 공식 DinD·BusyBox 이미지를 사용한 Docker API 검사이며 Ceph 이미지를 생성하거나 Ceph suite PASS로 합산하지 않습니다.

Commit `43099aa`는 Docker가 선택한 주소 풀을 사용하는 명시적 IPAM 생성으로 public·backend·RGW peer bridge를 수정합니다. 빈 probe의 subnet·gateway를 읽어 명시적으로 재생성하며 Docker pool overlap만 최대 4회 새 할당을 시도합니다. 정리 실패 때 반환되는 네트워크의 소유권을 bootstrap cleanup에 보존합니다. 고정 CIDR을 선택하거나 daemon 이미지를 교체하지 않습니다. 위 r2 성공은 이 후속 네트워크 수정 이전의 증거입니다.

수정한 Go API의 `TestRecoverableBridgeEndpointIdentity`는 Docker Desktop Engine 29.8.1과 격리된 Engine 28.0.4에서 모두 PASS했습니다. 두 bridge의 독립 subnet과 원래 endpoint의 IP·aliases·gateway priority·동일 PID, 다른 endpoint 보존 및 반복 복구를 검사합니다. 각 최종 owned container/network는 0개이며 Engine 28 daemon·socket volume도 정리했습니다. 로그와 source 188개 SHA, 초기·최종 cleanup 관측은 `artifacts/quay-network-recovery-20261004-r3/network28-*` 및 `network29-*`에 기록합니다. 두 SDK source manifest는 `5009662` worktree에 IPAM 수정을 적용한 상태를 기록하며, 188개 이름·SHA가 후속 커밋 `43099aa`와 일치함을 확인했습니다. `make check`의 최종 결과는 같은 디렉터리의 `check.log`입니다.

`43099aa`의 [Linux AMD64 CI 전체 실행](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37165945276)은 2026-10-04 02:13 UTC SUCCESS로 종료했습니다. 03:04 UTC 공개 REST API 재조회에서 exact head SHA와 static·기본 14개·핵심 8개·멀티클러스터 18개·확장 12개 필수 job의 SUCCESS, SDK endpoint 회귀 step의 SUCCESS를 다시 확인했습니다. 기본 14개에는 native/runtime 11개와 signer 3개가 포함됩니다. 필수 selector를 줄이거나 알려진 upstream 기능 회귀를 이 38개 topology에 포함·skip하지 않았습니다. 실제 CI 단계와 source SHA는 `artifacts/quay-network-recovery-ci-20261004-r3/terminal-revalidation.json`에 보존합니다.

같은 `43099aa`의 로컬 Linux ARM64 targeted 실행은 네트워크 관련 5개 중 4개가 PASS, `TestMultiClusterRGWPeerNetworkTopology`는 peer 단절 직후 기존 공개 URL의 GET에서 EOF로 FAIL했습니다. 이 오류는 endpoint Restore 이전이며, 로컬 endpoint IP·실행 PID·inspect상 공개 URL은 유지됐습니다. 전체 targeted 명령의 결과는 FAIL로 보존하며 `artifacts/quay-network-cases-20261004-r3/`에 5개 이름·결과·source 188개와 owned cleanup 0개를 기록합니다. 이전 CI 성공을 이 로컬 실패 대신 사용하지 않습니다.

### RGW 공개 포트와 peer gateway 분리

두 bridge의 gateway priority가 같으면 network 이름 순서에 따라 peer가 선택될 수 있습니다. [Moby endpoint 정렬](https://github.com/moby/moby/blob/v28.0.4/libnetwork/sandbox.go#L610-L661)과 [Docker gateway priority 계약](https://docs.docker.com/engine/network/#connecting-to-multiple-networks)에 따라 공개망 우선순위를 구분합니다. 격리된 Engine 28.0.4 실험에서 peer가 gateway인 경우 단절 후 동적 공개 포트가 32768→32769로 바뀌어 기존 URL 연결이 거부됐습니다. Public priority를 1, peer를 0으로 구분한 경우 원래 포트·public 경로·PID와 fresh GET 4회가 유지됐습니다. Ceph 없는 Docker API 실험의 원본 결과는 `artifacts/quay-rgw-port-probe-20261004/engine28-summary.json`이며 Ceph topology 성공 수에 합산하지 않습니다.

Commit `3f79a78`은 bridge RGW의 public endpoint를 `GwPriority=1`로 설정합니다. Host listener, 일반 `WithClient`, peer 기본값 0과 후속 사용자 customizer 합성은 유지합니다. SDK `TestRecoverableBridgePublishedPort`는 같은 published URL에서 전용 fresh HTTP 연결의 200·정확한 body를 단절 전·격리 중·복구 후·반복 복구에 확인하며 IP·aliases·priority·native PID도 비교합니다. CI는 기존 endpoint identity 검사와 이 HTTP 검사 2개를 별도 step으로 실행한 뒤 기존 확장 12개 전체를 실행합니다.

수정 후 `make check`는 unit·race·vet·전체 tag compile 및 Python guard 63개에서 PASS했습니다. 실제 SDK 2개는 격리된 Engine 28.0.4에서 Go 21.105초, Docker Desktop의 Linux host-network Go runner에서 Go 21.852초에 PASS했습니다. 각각 profile elapsed는 23.478초, 36.234초이며 owned container/network는 0개입니다. 원본 Quay RGW peer topology도 142.918초 PASS했습니다. 같은 공개 URL의 격리 중 기존 object 읽기·새 object 쓰기, 상대 zone에 새 update 부재, 복구 뒤 양방향 bytes와 원래 period/PID/IP/alias 보존을 확인했습니다. 로그·source 189개 SHA는 `artifacts/quay-published-endpoint-20261004-r4/` 및 `artifacts/quay-gateway-priority-20261004-r4/`에 보존합니다. Source manifest의 revision은 수정 전 `43099aa` worktree이며 실제 189개 이름·SHA로 후속 `3f79a78`의 runtime 입력을 대조합니다. 이 focused 결과를 후속 전체 CI 성공으로 표현하지 않습니다. `3f79a78`을 main에 push했으며 [후속 전체 CI](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37173593510)의 exact head SHA로 기본 14개·토폴로지 38개 및 SDK 회귀 2개를 재검증 중입니다. 관측은 `artifacts/quay-gateway-priority-ci-20261004-r4/`에 기록합니다.

Docker Desktop에는 별도 공개 포트 경로 문제가 관측됐습니다. Public priority 1을 유지한 BusyBox 서버에 peer를 **Start 전에** 연결하면 peer 단절 중 public-only observer의 직접 IP GET은 PASS하지만 VM host namespace와 macOS에서 기존 공개 포트 GET은 timeout이었습니다. 원래 peer를 복구하면 두 공개 경로 모두 정상 응답했습니다. Peer를 **Start 후** 연결한 별도 케이스는 격리 중에도 세 경로가 모두 PASS했습니다. `artifacts/quay-desktop-ingress-paths-20261004-r4/summary.json`에 실제 native identity·port·HTTP·정리를 보존합니다. 이는 Ceph가 아닌 Engine/Desktop published-port 경로의 결함 후보이며 현재 forwarder의 target 선택 원인까지 확정하지 않습니다. Priority 고정만으로 모든 Desktop/macOS 공개 연결을 보장하지 않습니다. 추가 proxy·서버 패치·이미지 생성 없이 지원되는 연결 순서의 차이를 확인했으며, 정식 fixture 적용 여부는 별도 검토 범위입니다.

현재 [Makefile](../Makefile)의 finite selector는 core topology 8개, multicluster 18개, extensions 12개 named test입니다. 내부 bridge/host subtest가 있는 함수도 있어 named selector 수를 실행 횟수로 해석하지 않습니다. Metadata master 전환은 일반 `TestMultiClusterRGWMultisite`와 별도인 `TestMultiClusterRGWMetadataMasterFailover`로, RBD peer 제거·재등록은 daemon HA와 별도인 `TestMultiClusterRBDPeerLifecycle`로 확인합니다. CephFS MGR HA와 mirror daemon 재분배도 별도 representative를 유지합니다.

서로 다른 배치·네트워크 모드를 한 번의 전체 suite PASS로 합치지 않습니다. 일반 MDS scale·standby/replay와 fanout 등 기존 bridge-only 대표는 그 범위를 유지하고, bridge/host wrapper가 있는 topology는 두 모드를 실행합니다. Docker bridge endpoint interruption은 host namespace에서 제공되는 동작이 아니므로 host-mode 단절을 새로 주장하지 않습니다.

CephFS mirror 증설은 기본 20.2.4의 자동 shuffle 오류와 검증 가능한 `RebalanceDirectories` 명시적 경로를 구분합니다. RGW numeric priority 및 ordinary-user source 권한 거부도 원본 서버의 한계로 별도 회귀를 유지합니다. 이 조건을 PASS로 만들기 위해 필수 경로에서 서버를 패치하거나 판정을 완화하지 않습니다. 개별 CRUD·pool/EC/권한 정책 전수 검증과 모든 split-brain/backup parameter 조합은 topology 완료 조건으로 확대하지 않습니다.

## 기존 구성별 제공 상태

2026-10-03 작업 중 기록입니다. “구현”과 실제 Docker PoC 통과를 구분합니다. 이 문서는 검증 결과에 맞춰 갱신합니다.

| 구성 | 생성·변경 API | 실제 검증 상태 |
|---|---|---|
| 빠른 단일 클러스터 | `Run`, `WithOSDCount`, `AddOSD`, `RemoveOSD` | 기존 PoC 통과. 기본 MON 1/MGR 1/OSD 2, sparse BlueStore. Linux go-ceph의 RADOS/RBD/CephFS 통신도 통과 |
| MON quorum / MGR standby | `WithMonitorCount(3)`, `WithManagerCount(2)`, `Monitors`, `Managers`, `AddMonitor`, `RemoveMonitor`, 독립 `ControlContainer` | bridge/host 통과. MON 중단·재시작·3→4→3 교체 동안 같은 native RADOS 연결 유지. quorum 상실 시 새 인증 실패, 복구 후 기존/새 연결 성공. active MGR 변경 및 volumes 명령 확인 |
| 실행 중 MGR 증감 | `AddManager`, `RemoveManager` | bridge 통과. 1→2→1→2→1에서 active 제거·standby 승격, 교체 standby 추가·제거 후 native MGR 명령과 실제 map 확인. 마지막 candidate 제거 거부 |
| CephFS active/standby/replay | `WithCephFS`, `StartCephFSWithConfig`, `CephFSConfig` | standalone standby-replay 장애·승격·재가입 통과. 요청한 rank와 standby의 실제 FSMap 확인 |
| 다중 active MDS / 여러 filesystem | `WithCephFS`의 이름·active/standby 수·MDS affinity | Run에서 두 filesystem, 2 active + 1 standby 구성 통과. rank1 중단·승격 후 통신과 다른 filesystem의 독립성 확인 |
| 실행 중 MDS 증감 | `CephFSContainer.ScaleMDS` | bridge 통과. 일반 standby의 1/0→2/1→1/1→1/0과 replay follower의 1/1→2/1→1/1→1/0 확인. rank handoff·남는 standby 제거, 실제 FSMap/컨테이너 수 확인. 같은 filesystem/pool ID와 기존 파일, 다른 filesystem MDS의 GID 유지 |
| RGW 단독 / 같은 zone의 여러 gateway | `WithRGW`, `StartRGWWithConfig`, `Gateways`, `RemoveRGW` | bridge/host 통과. 초기 Run의 gateway, 동적 추가, 2→1→2 제거·교체, 중단·재시작 뒤 같은 zone의 기존 데이터 확인 |
| 독립 클러스터 2개 동시 사용 | 독립 `Run`과 `WithClient`/`ConnectionConfig` | bridge/host 통과. 동일 pool/object 이름의 서로 다른 데이터, 양쪽 OSD 교체, Linux go-ceph의 실제 session 확인 |
| RGW multisite 2 zone | `multicluster.RunRGWMultisite` | 복제·gateway 중단/복구는 bridge/host 통과. host의 자동 선택 gateway endpoint를 양쪽 최종 period에서 확인. metadata master 전환/복귀는 bridge에서 검증 |
| RGW multisite 여러 zone | `RunRGWTopology`, `RGWTopologyConfig.Zones/MetadataMaster`, `AddZone`, `Zones`, `ZoneAdmin` | 3 zone bridge/host 통과. 초기 MON a 제거 후 남은 quorum으로 구성, 입력 순서와 다른 metadata master 지정, 실제 zone ID/endpoint, secondary 중단·재가입과 각 zone의 통신, master gateway 중단 후 보조 zone의 독립 읽기 확인 |
| RBD snapshot mirror / journal mirror | `multicluster.RunRBDMirror`, `RBDMirrorConfig.Mode` | snapshot은 bridge/host 통과. journal의 receiver 재시작·A→B→A 구성도 통과 |
| RBD 3 cluster fanout | 기존 pair API를 A→B, A→C로 조합 | bridge 통과. 서로 다른 FSID/네트워크/키, 실제 tx/rx peer graph, 한 receiver 중단 중 다른 receiver 유지, 재시작 후 catch-up. source MON/OSD와 두 receiver 중단 후 B/C의 독립 읽기 확인 |
| RBD backup/restore | 전체·증분 export/restore API | 실제 source 중단 후 복원·새 session 통과 |
| CephFS snapshot mirror / archive restore | `multicluster.RunCephFSMirror`, `AttachManagers`, directory/peer 변경 | bridge/host 통과. 초기 MGR a 제거 후 b active/c standby로 생성, d 추가·연결 준비, b 제거 후 c 승격·peer 재등록과 새 snapshot 도달 확인. 기존 snapshot 및 양쪽 클러스터 유지, fixture가 추가한 attachment만 cleanup. 기존 daemon 재시작·OSD 교체·source 중단 사례도 통과 |

기존 세부 결과와 로그 경로는 [MULTICLUSTER_POC.md](MULTICLUSTER_POC.md), [HOST_NETWORK_POC.md](HOST_NETWORK_POC.md), [SERVICES_POC.md](SERVICES_POC.md)에 있습니다. 위 “통과”는 각 케이스의 최종 실행 결과이며 전체 테스트가 한 번의 명령에서 모두 통과했다는 뜻은 아닙니다.

이번 토폴로지 실행은 Ceph 20.2.4 역할별 slim 이미지와 Docker Desktop의 Linux ARM64 엔진에서 수행했습니다. 주요 로그는 로컬 `artifacts/cluster-core-topologies-final.log`와 `artifacts/cluster-peer-gateway-topologies-final.log`입니다. 앞의 core 실행은 665.794초, peer/gateway 실행은 808.041초에 통과했습니다. host RGW multisite 514.54초, RBD fanout 144.49초, 같은 zone RGW lifecycle bridge/host 148.55초입니다. fanout의 source/receiver 중단은 통신 경로의 독립성 확인이며 물리 장애 내성이나 전체 네트워크 partition 검증으로 확대 해석하지 않습니다.

후속 실행의 증거는 다음과 같습니다. 시간은 해당 로그의 실제 결과이며 서로 다른 실행을 하나의 전체 suite 통과로 합치지 않습니다.

| 로그 | 결과 |
|---|---|
| `artifacts/cluster-manager-lifecycle-final2.log` | MGR lifecycle 18.95초, 명령 전체 19.409초 PASS |
| `artifacts/cluster-mds-scale-final.log` | 일반 standby MDS scale 명령 전체 93.742초 PASS |
| `artifacts/cluster-mds-replay-scale-final.log` | standby-replay MDS scale 명령 전체 99.372초 PASS |
| `artifacts/cluster-topology-review-regression.log` | MON/MGR 및 RGW의 bridge 회귀 검증 276.464초 PASS |
| `artifacts/go-ceph-linux-topology-regression/integration.log` | Linux go-ceph 소비자 테스트 bridge/host 233.04초 PASS |
| `artifacts/go-ceph-linux-topology-regression/summary.json` | `passed: true`, `remaining_owned_container_ids: []`. 이 runner가 추적한 컨테이너의 잔여 리소스 0개 |
| `artifacts/cluster-rgw-three-zone-final2.log` | host 3 zone PASS, bridge 3 zone FAIL. 명령 전체는 FAIL |
| `artifacts/cluster-rgw-three-zone-bridge-final3.log` | 불필요한 새 gateway 재시작 제거 후 bridge 3 zone 414.97초 PASS |
| `artifacts/cluster-topology-final-validation.log` | 초기 metadata shard 준비 조건을 추가한 최종 RGW host 597.39초 / bridge 313.82초 개별 PASS. 같은 명령의 CephFS MGR 준비 전환 검사 88.20초 FAIL로 명령 전체는 FAIL; 해당 대기는 수정 후 별도 재검증 |
| `artifacts/cluster-cephfs-manager-topology-final2.log` | MGR 준비 대기 수정 후 CephFS MGR topology bridge 123.05초 / host 117.96초, 명령 전체 241.241초 PASS |

### 마지막 구성 수정과 검증

- **RGW 3 zone**: bridge의 HTTP 403 실패 뒤 새 gateway의 불필요한 초기 재시작을 제거하고, 실행 중 secondary의 모든 metadata shard가 incremental 상태인지 확인합니다. 이 수정으로 bridge/host 검증이 완료됐습니다. 이전 실패의 Ceph 내부 원인을 확정한 것은 아닙니다.
- **CephFS mirror와 MGR HA의 결합**: 초기 MGR 포인터 의존을 제거하고 현재 owned 후보 전체의 peer network를 준비합니다. `AttachManagers`와 `RebootstrapPeer`가 새 후보를 재조정하며 MGR 모듈 활성화 중 실제 active 준비를 기다립니다. 초기 MGR 제거 후 mirror 생성과 standby 승격 뒤 peer 재등록이 bridge/host에서 통과했습니다.

최종 실행 뒤 Docker의 running/stopped container 목록은 비어 있었고 testcontainers가 만든 임시 network도 남지 않았습니다. 기존 `kind` network는 유지했습니다. CephFS 검증은 mirror 종료 후 현재 MGR의 owned attachment가 제거됐으며 양쪽 cluster control이 계속 동작하는지도 확인했습니다. 불확실한 network connect·disconnect, 삭제된 MGR, Docker client close 실패의 cleanup 재시도는 별도 단위 테스트에서 확인했습니다.

## 우선순위와 완료 판단

첫 단계는 **daemon 역할별 수와 active/standby, 노드 추가·제거·교체, 네트워크와 포트, 독립 cluster 및 peer/zone 연결 그래프**를 public 구성 API로 만드는 것입니다. native/HTTP 읽기·쓰기는 구성된 노드가 실제로 통신하는지 확인하는 최소 증거로 사용합니다. 클라이언트의 모든 기능과 데이터 정책을 검증하는 것은 이 작업의 목표가 아닙니다.

이번 단계는 다음 대표 구성의 생성과 변경을 완료 기준으로 삼습니다. 숫자와 정책 조합의 전수 검증 대신 실제 map과 연결을 확인합니다.

1. 기본 단일 클러스터와 3 MON quorum, active/standby MGR, 여러 OSD를 요청한 수와 identity로 생성합니다.
2. MON/MGR/OSD의 추가·제거·교체, filesystem별 multi-active MDS와 standby/replay 증감, 같은 zone의 여러 RGW 생명주기를 확인합니다.
3. bridge/host에서 독립 클러스터를 함께 사용하고, RGW 2/3 zone과 RBD pair/fanout, CephFS pair의 peer 그래프가 실제로 통신합니다.
4. 초기 MON/MGR을 제거한 뒤에도 남은 quorum과 active MGR로 구성·연결 API를 사용할 수 있으며 중단·승격·재가입 뒤 연결이 복구됩니다.
5. 성공과 부분 생성 실패에서 fixture가 소유한 컨테이너·네트워크 연결을 정리할 수 있습니다. cleanup 증거의 범위도 해당 실행의 owned resource로 명시합니다.

단순히 `HEALTH_OK`인지보다 요청한 토폴로지가 만들어졌는지가 중요합니다. bootstrap에 필요한 최소 pool/auth 설정은 생성 코드에 포함하지만 정책 조합의 전수 검증은 하지 않습니다.

위 첫 대표 토폴로지에 이어 RGW 여러 zonegroup·zone 탈퇴, 복수 mirror daemon, 별도의 cluster/public network와 제한된 endpoint 단절·복구의 구성 API와 대표 PoC를 완료했습니다. 5 MON quorum, 분리 네트워크, RBD/CephFS daemon 증감·HA, RGW group 추가·zone 탈퇴와 세 서비스의 peer endpoint 복구가 통과했습니다. CephFS 20.2.4의 자동 증설 재분배 오류와 검증된 명시적 재분배 경로는 [TOPOLOGY_EXTENSIONS.md](TOPOLOGY_EXTENSIONS.md)에 구분합니다. 숫자 조합 전체나 모든 네트워크 장애를 검증했다는 의미는 아닙니다. CRUSH rule, EC profile, pool replica 정책, namespace·권한·layout 등은 별도 후속 기능 과제입니다. 이미 작성한 정책 API와 PoC는 유지하되 토폴로지 작업의 완료 기준으로 삼지 않습니다.

CRUSH host/rack은 같은 Docker 엔진에서 만든 논리적 배치 도메인입니다. 실제 물리 노드나 디스크의 장애 내성을 입증하지 않습니다. CSI가 필요로 하는 pool·identity·filesystem·MDS 구성은 이 fixture의 범위에 들어가지만, krbd/NBD, kernel CephFS/FUSE, Kubernetes NodeStage/NodePublish는 별도 Linux/Kubernetes harness의 구성 과제로 둡니다.

## 후속 기능 작업의 기록

이번 조사 중 작성한 `CreatePool`/`WithPools`, `WithPoolDefaults`, CRUSH placement 및 default-root 옵션, EC 설정, `CreateClient`/`WithClientIdentity`는 후속 기능 확장의 출발점입니다. EC RADOS/RBD와 제한된 identity의 bridge/host PoC는 통과했습니다. 이들은 토폴로지 단계의 완료 조건과 별개였으며, 다음 내부 설정 단계에서 pool 정책·caps 변경·RBD namespace·CephFS subvolume·RGW 사용자 API를 확장합니다. [내부 설정 API](CLUSTER_INTERNAL_FEATURES.md)

CephFS는 native mirror module의 filesystem당 single peer 제한을 따릅니다. 같은 filesystem의 A→B/C fanout은 API 일반화만으로 제공할 수 있는 구성으로 분류하지 않습니다. 여러 독립 filesystem을 각각 다른 peer에 연결하는 방식은 별도 구성으로 검토합니다. [CephFS mirror peer 제한](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/#mirroring-module)

## 클라이언트 요구를 참고한 근거

- [go-ceph의 native library와 build tag 계약](https://github.com/ceph/go-ceph/blob/v0.41.0/README.md): 공개 모듈 의존성으로 넣지 않고 Linux 소비자 fixture에서 실제 통신을 확인합니다.
- [MON quorum](https://docs.ceph.com/en/tentacle/rados/configuration/mon-config-ref/#monitor-quorum), [MON 추가·제거](https://docs.ceph.com/en/tentacle/rados/operations/add-or-rm-mons/), [MGR HA](https://docs.ceph.com/en/tentacle/mgr/administrator/#high-availability): 공유 monmap과 다수결, standby promotion을 구성 기준으로 사용합니다.
- [MDS standby](https://docs.ceph.com/en/tentacle/cephfs/standby/), [multi-MDS](https://docs.ceph.com/en/tentacle/cephfs/multimds/): active rank, standby/replay, filesystem별 daemon affinity와 rank handoff를 구성 기준으로 사용합니다.
- [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/): 클러스터 자체와 peer/zone 연결은 별도 패키지에서 구성합니다.
- [AWS SDK endpoint 설정](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html), [MinIO client 옵션](https://github.com/minio/minio-go/blob/master/api.go): RGW 소비자에는 연결 가능한 HTTP endpoint와 테스트 자격 증명이 필요합니다. SDK의 모든 기능을 모듈 자체가 구현할 필요는 없습니다.
