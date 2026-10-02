# PoC 실행 결과

실행일: 2026-10-02, Asia/Seoul. OSD topology를 변경해도 실제 Ceph 클라이언트 통신과 기존 데이터 접근이 유지되는지 확인했습니다.

## 환경

| 항목 | 검증 환경 |
| --- | --- |
| 호스트 / Go | macOS ARM64 / Go 1.27.1 darwin/arm64 |
| Docker | Docker Desktop, 서버 29.8.1, Linux aarch64 |
| Docker VM | 4 vCPU, 약 3916 MiB RAM |
| testcontainers-go | v0.44.0 |
| 최종 Ceph | Tentacle 20.2.4, stable, 실제 `ceph --version` 확인 |
| Go 설정 | `CGO_ENABLED=0` |
| 기본 topology | MON 1개 + MGR 1개 + OSD 2개, 각 별도 컨테이너 |
| 데이터 경로 | OSD당 1 GiB sparse BlueStore 파일, 컨테이너 내부 |
| 테스트 클라이언트 | 같은 Docker 네트워크의 별도 Ceph CLI 컨테이너 |

기본 manifest: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

Ceph MON의 Docker inspect에서 `Privileged=false`, host devices와 bind mounts가 없음을 확인했습니다. 구현은 다른 Ceph 데몬에도 privileged/devices/binds 설정을 넣지 않습니다. Ryuk의 Docker 소켓 접근은 testcontainers 자원 정리를 위한 별도 메커니즘입니다.

## 통합 테스트

`ceph_integration_test.go`의 `TestClusterLifecycle`은 아래를 순서대로 실행합니다.

1. MON/MGR/OSD 2개를 부트스트랩하고 Cephx 인증으로 CLI 통신합니다.
2. 8 PG, 복제 수 2, min_size 1인 `tc-poc` 풀을 만들고 모든 PG가 active+clean이 될 때까지 기다립니다.
3. 별도 클라이언트 컨테이너에서 실제 `rados put/get`으로 객체 16개를 저장하고 바이트 단위로 비교합니다.
4. 기존 OSD 하나를 정지한 상태에서 객체를 읽고, 다시 시작해 clean 상태로 복구합니다.
5. OSD를 추가해 2 → 3으로 확장하고 재배치 완료를 확인합니다.
6. 처음부터 데이터를 보유하던 OSD를 drain/purge하여 3 → 2로 축소하고 모든 객체를 다시 비교합니다.
7. OSD를 재추가·재삭제하는 두 번째 cycle을 수행하고 모든 객체를 다시 비교합니다.
8. 클라이언트를 먼저 정리하고 OSD/MGR/MON 및 네트워크를 정리합니다.

최종 실행에서 각 안정 상태의 결과는 다음과 같습니다. PG 9개는 `tc-poc` 풀의 8개와 MGR가 만든 내부 풀의 1개입니다.

| 단계 | OSD total/up/in | PG 상태 | Health |
| --- | --- | --- | --- |
| 최초 구성 | 2 / 2 / 2 | 9 active+clean | HEALTH_OK |
| osd.2 추가 | 3 / 3 / 3 | 9 active+clean | HEALTH_OK |
| 기존 osd.0 제거 | 2 / 2 / 2 | 9 active+clean | HEALTH_OK |
| 두 번째 추가·삭제 | 2개로 복귀, WaitForClean 성공 | active+clean 확인 | 별도 health assertion 없음 |

이미지가 캐시된 최종 실행의 부트스트랩은 8.757초, 두 번의 topology cycle과 객체 비교까지 52.085초, cleanup을 포함한 lifecycle 테스트는 64.20초였습니다. 초기화 실패 cleanup 테스트까지 포함한 Go 테스트 출력은 `ok ... 64.847s`였습니다. 이미지 다운로드 시간이나 반복 측정에 따른 성능 보장은 포함하지 않습니다.

`TestBootstrapFailureCleanup`은 MON 시작 명령을 의도적으로 실패시킨 뒤 오류와 함께 반환된 부분 생성 클러스터를 정리하고, `Terminate`를 다시 호출하여 정상 종료를 확인합니다. 최종 테스트 session label로 컨테이너와 네트워크를 조회했을 때 잔여 자원이 없었습니다.

최초 Squid 19.2.3 실험도 같은 객체 I/O 및 두 번의 topology cycle을 통과했습니다. 당시 HEALTH_WARN은 insecure global ID reclaim 허용 설정 때문이었습니다. 이를 명시적으로 비활성화한 최종 Tentacle 구성에서는 HEALTH_OK를 확인했습니다. Squid의 최신 패치 버전에 대한 지원을 검증했다는 의미는 아닙니다.

이후 RGW/RBD/CephFS를 추가하고 전역 PG 설정을 조정한 뒤에도 `TestClusterLifecycle`(68.19초)과 `TestBootstrapFailureCleanup`(0.30초)을 다시 실행하여 통과했습니다. 해당 로그는 `artifacts/poc-services-regression.log`이며 서비스별 결과는 [SERVICES_POC.md](SERVICES_POC.md)에 정리했습니다.

## 기타 검증

- `CGO_ENABLED=0 go test ./...`: 잘못된 설정의 사전 거부, CLI stdout/stderr 분리 검증 통과
- `go vet -tags=integration ./...`: 통과
- 셸 스크립트 `sh -n`: 통과
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...`: 통과
- `CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...`: 통과
- `git diff --check`: 통과

Linux/Windows 항목은 교차 컴파일 검증입니다. Docker 런타임 통합 테스트를 해당 OS에서 실행한 것은 아닙니다. GitHub Actions workflow는 Ubuntu용으로 작성했으며 아직 원격 CI에서 실행하지 않았습니다. 모듈 최소 Go 버전은 1.25이고 로컬 실행은 위의 Go 1.27.1로 수행했습니다.

로컬 전체 로그는 `.gitignore`에 포함된 `artifacts/poc-final.log`에 남겨 두었습니다. 첫 실험 로그는 `artifacts/poc-squid-19.2.3.log`, Tentacle 전환 후 첫 실행은 `artifacts/poc-tentacle-20.2.4.log`입니다. 저장소를 복제하면 로그는 따라가지 않으며 테스트로 재생성합니다.

## 범위와 후속 판단

OSD 노드를 개별 컨테이너로 다루는 생성/추가/삭제는 충분히 실현 가능하다고 판단합니다. 실제 Cephx 인증과 MON discovery 이후의 OSD 직접 I/O까지 수행했으므로 단순 CLI status 성공 이상의 근거가 있습니다. 제어용 Go 라이브러리의 네이티브 링킹을 없애는 방향도 검증했습니다.

현재 node는 OSD daemon 1개에 대응합니다. RGW/S3, RBD, CephFS/MDS는 별도 통합 테스트로 확장했으며 실제 실행과 소비 경로는 [SERVICES_POC.md](SERVICES_POC.md)에 기록합니다. MON/MGR의 동적 수 변경과 quorum, 여러 OSD를 가진 호스트, 서비스 failover, 소비 애플리케이션 자체의 Ceph wire protocol 구현은 아직 검증하지 않았습니다.

macOS 호스트 프로세스의 직접 RADOS 연결, 다른 아키텍처의 실제 실행, 병렬 클러스터 대량 실행, 중간 단계 취소/네트워크 단절에 대한 완전한 복구도 별도 검증 대상입니다. `RemoveOSD`의 drain timeout은 자동 rollback하지 않습니다. 데이터 이동을 완료할 수 없는 복제/용량 구성에서는 제한 시간 내 제거 성공을 기대할 수 없습니다.
