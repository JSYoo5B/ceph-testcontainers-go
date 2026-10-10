# 컨테이너 일시정지 fixture

`Container.PauseContainer`는 owned daemon이나 `WithClient`로 연결한 caller
컨테이너를 Docker cgroup freezer로 멈추고, `ContainerPause.Resume`으로 되살린다.
`docker pause`와 같은 동작이다. 응답하지 않는 OSD나 멈춘 client를 재현해 client
timeout, 재시도, `SLOW_OPS`, peer의 down 판정과 재합류를 테스트하는 데 쓴다.

```go
hold, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
if hold != nil {
    defer hold.Restore(cleanupCtx)
}
if err != nil {
    return err
}
pause, err := cluster.PauseContainer(ctx, cluster.OSDs()[1].Container)
// Docker 응답이 유실돼도 컨테이너가 멈췄을 수 있다.
// 오류와 함께 받은 non-nil handle도 Resume해야 한다.
if pause != nil {
    defer pause.Resume(cleanupCtx)
}
if err != nil {
    return err
}
// client의 rados_osd_op_timeout 만료와 HealthDetails의 SLOW_OPS를 확인한다.
```

## Stop, 네트워크 단절과의 차이

`Stop`은 프로세스를 끝내므로 peer가 연결 종료를 바로 감지한다.
`InterruptNetwork`는 bridge endpoint를 떼어 내며 host network에서는 쓸 수 없다.
일시정지는 프로세스의 메모리, 소켓, Ceph identity를 그대로 둔 채 실행만 멈춘다.
Peer는 연결 종료 없이 응답만 받지 못하므로 heartbeat grace가 지난 뒤에야 down을
보고한다. Host network에서도 동작한다.

## 계약

실행 중이고 멈춰 있지 않은 컨테이너만 받는다. 이미 멈춘 컨테이너는 fixture가
멈춘 것인지 알 수 없으므로 소유하지 않고 거부한다. Cluster의 control CLI가 도는
컨테이너도 거부한다. 이 컨테이너가 멈추면 모든 fixture 명령이 함께 멈추기
때문이다. MON이 하나인 cluster에서는 첫 MON 컨테이너가 control CLI를 겸한다.

같은 컨테이너에 대한 일시정지는 하나만 허용한다. 해제되지 않은 handle이 있으면
그 handle과 오류를 반환한다. Cluster는 handle을 보관하고 `Terminate`에서 다른
컨테이너를 정리하기 전에 먼저 Resume한다. Caller가 소유한 컨테이너를 지우기
전에는 직접 Resume한다.

`Resume`은 재시도할 수 있고 멱등적이다. 컨테이너가 이미 지워졌거나 멈춰 있지
않으면 Docker에 아무것도 요청하지 않고 완료한다. 상태를 확인할 수 없으면 완료로
표시하지 않고 오류를 반환한다. Ceph 정책, flag, 설정은 바꾸지 않는다.

## Ceph에서 확인한 동작

OSD 두 개와 replica 2 pool에서는 모든 PG가 두 OSD를 포함한다. 그래서 한 OSD를
멈추면 그 OSD가 primary인 PG와 replica인 PG 모두에서 write가 끝나지 않는다.

`nodown`을 함께 켜면 멈춘 OSD가 map에서 up으로 남는다. Client는
`rados_osd_op_timeout`이 지나 errno 110(ETIMEDOUT)을 받고, 살아 있는 primary가
replica 응답을 기다리는 op를 `SLOW_OPS`로 보고한다. 기본 `osd_op_complaint_time`은
30초이므로 테스트에서는 `TemporaryConfig`로 낮춘다. 멈춘 OSD 자신은 slow op를
보고하지 못한다.

`nodown` 없이 멈추면 peer가 heartbeat grace(기본 20초) 뒤에 down을 보고하고, MON이
OSD를 down으로 표시하면 degraded I/O가 재개된다. MON은 기본적으로 서로 다른 host의
reporter 두 개를 요구한다(`mon_osd_min_down_reporters=2`). 따라서 OSD가 두 개뿐인
cluster에서는 `TemporaryConfig`로 이 값을 1로 낮춰야 한다. 그렇지 않으면
`mon_osd_report_timeout`(기본 900초)까지 down으로 표시되지 않는다. Resume한 OSD는
자신이 down으로 표시된 것을 알고 다시 up으로 합류한다.

## 멈춘 client

`WithClient`로 연결한 client 컨테이너를 멈추면 서버가 응답 없는 client를 정리하는
경로를 재현할 수 있다. 세 경우 모두 기준이 되는 시간은 OSD의 `osd_client_watch_timeout`
(기본 30초)이나 CephFS의 `session_timeout`이다.

| 대상 | 관측 API | Ceph 20.2.4·19.2.5에서 확인한 동작 | 계약 |
| --- | --- | --- | --- |
| RADOS object watch/notify | `Container.ObjectWatchers` | 멈춘 동안 다른 client의 notify가 자기 timeout 뒤 errno 110으로 실패한다. 28~30초 뒤 watch가 사라지면 notify가 즉시 성공한다. blocklist 항목은 생기지 않는다. | 이 문서 |
| RBD exclusive lock | `rbd.ImageClients` | watch가 만료되면 다른 client가 lock을 깨고 이전 owner를 blocklist한다. `TemporaryBlocklist`로 막으면 1초 안에 넘어간다. | [lock owner](RBD_LOCK_OWNER.md) |
| CephFS session | `cephfs.Filesystem.Sessions` | 경쟁이 없으면 `session_autoclose`에, 다른 client가 capability를 원하면 `session_timeout` 직후에 evict·blocklist한다. | [client session](CEPHFS_CLIENT_SESSIONS.md) |

`ObjectWatchers`는 `rados listwatchers`를 읽는다. 이 명령은 `--format json`을 무시하고
`watcher=IP:port/nonce client.<ID> cookie=<N>` 형식의 텍스트만 출력하므로 그 줄을
파싱한다. `Address`는 `BlocklistEntries`와 같은 표기다. namespace가 빈 문자열이면 기본
namespace다. 이름이 dash로 시작하거나 제어 문자를 포함하면 rados option이나 출력 줄로
잘못 읽힐 수 있어서 명령 전에 거부한다.

## 실행 검증

`TestPausedOSDFaults`는 bridge와 host network 각각 다음을 확인한다.

| 단계 | 조작 | 확인 |
| --- | --- | --- |
| guard | control CLI 컨테이너 일시정지 | 거부, handle 없음 |
| frozen_up | `nodown` 후 OSD 일시정지, 같은 OSD 재요청 | 중복 거부, write 4개 모두 errno 110, `SLOW_OPS` 보고, up OSD 2개 유지 |
| thawed | Resume 두 번, `nodown` 복원 | clean 뒤 새 write와 기존 object 8개 검증, `SLOW_OPS` 해제 |
| frozen_down | flag 없이 OSD 일시정지 | up OSD 1개로 감소, 120초 timeout client의 degraded write와 기존 object 검증 |
| rejoined | Resume | up OSD 2개, clean, 새 write와 기존 object 검증 |

준비 단계에서 `TemporaryConfig`로 `mon_osd_min_down_reporters=1`,
`osd_op_complaint_time=2`를 설정한다. CI에서는 `Ceph recovery`의
`container_pause` batch로 실행한다.

`TestPausedObjectWatcher`는 bridge와 host network 각각에서 살아 있는 watcher에 대한
notify 성공, 멈춘 watcher에 대한 notify의 errno 110, watch 만료(15초 이전이면 실패),
만료 뒤 notify 즉시 성공, blocklist 항목 없음을 확인한다. CI에서는 short 범주의
`rados_fixtures_watchers` batch로 Tentacle과 Squid에서 실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=40m -run '^TestPausedOSDFaults$' ./internal/integration
```

2026-10-09 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 bridge/host 각 2개 단계
subtest 모두 PASS, 전체 193.63초였다. 두 network 모두 5초 timeout write 4개가
20.0초 동안 errno 110으로 끝났다. `nodown` 없이 멈춘 OSD는 bridge 25초, host
27초 뒤 down으로 표시됐고, Resume 뒤 3초 안에 다시 up이 됐다. 공식 역할 이미지
조합과 CI 실행은 이 기록에 포함하지 않는다.
