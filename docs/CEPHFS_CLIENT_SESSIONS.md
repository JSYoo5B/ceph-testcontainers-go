# CephFS client session fixture

`cephfs.Filesystem.Sessions`는 MDS가 보고하는 client session을 읽고,
`TemporarySessionTimeouts`는 MDS가 응답 없는 client를 정리하기까지 걸리는 시간을
테스트 안에서 기다릴 수 있을 만큼 줄인다. `ceph.Container.PauseContainer`로 client
컨테이너를 멈추면 다음 경로를 재현할 수 있다.

- 멈춘 client가 쥔 capability 때문에 다른 client의 요청이 기다리는 상황
- MDS가 session을 evict하고 client 주소를 blocklist하는 시점
- evict된 뒤 fsync하지 않은 데이터가 사라지는 동작
- 되살아난 client가 기존 session으로는 오류를 받고 새 session을 열어야 하는 동작

```go
fs := cephfs.Filesystems(cluster)[0]
change, err := fs.TemporarySessionTimeouts(ctx, cephfs.SessionTimeouts{
    Timeout:   30 * time.Second,
    Autoclose: 30 * time.Second,
})
// 응답이 유실돼도 값이 바뀌었을 수 있으므로 오류와 함께 받은 handle도 복원한다.
if change != nil {
    defer change.Restore(cleanupCtx)
}
if err != nil {
    return err
}
pause, err := cluster.PauseContainer(ctx, client)
if pause != nil {
    defer pause.Resume(cleanupCtx)
}
if err != nil {
    return err
}
// fs.Sessions에서 client의 session이 사라지고, cluster.BlocklistEntries에
// 같은 Address가 생기는지 확인한다.
```

## Session 제한 값

Ceph는 client가 capability를 갱신하지 않은 채 `session_timeout`이 지나면 session을
stale로 표시하고, `session_autoclose`가 지나면 evict한다. 기본값
`mds_session_blocklist_on_timeout=true`에서는 evict와 함께 client 주소를 OSDMap
blocklist에 넣는다. 기본값은 각각 60초와 300초다.

두 값은 central config가 아니라 filesystem의 MDSMap 값이어서 `TemporaryConfig`로는
바꿀 수 없다. 그래서 `fs set <name> session_timeout`과 `session_autoclose`를 쓰는 별도
handle을 둔다. MDSMonitor는 두 값 모두 30초 미만을 거부한다. fixture도 명령을 보내기
전에 30초 미만, 초 단위가 아닌 값, int32 범위를 넘는 값을 거부한다.

## 계약

`TemporarySessionTimeouts`는 filesystem마다 handle 하나만 허용한다. 복원하지 않은
handle이 있으면 오류를 반환한다. 적용 전 값을 기록하고, 달라진 값만 한 번에 하나씩
바꾼 뒤 다시 읽어 확인한다. 명령이 하나씩 실행되므로 적용과 복원은 원자적이지 않다.
중간에 실패하면 handle과 오류를 함께 반환한다.

`Restore`는 두 값이 각각 적용 전 값이나 적용한 값일 때만 기록한 값으로 되돌린다.
그 밖의 값은 외부 변경으로 보고 덮어쓰지 않는다. Filesystem ID가 달라졌으면, 즉 같은
이름으로 다시 만든 filesystem이면 거부한다. 복원을 확인한 handle은 복사본을 포함해
멱등적이며, cluster를 종료한 뒤에도 오류 없이 끝난다.

`Sessions`는 모든 active rank에 `session ls`를 보내 결과를 합친다. 여러 rank에 session을
가진 client는 rank마다 한 번씩 나온다. `Address`는 `IP:port/nonce` 형식으로
`BlocklistEntries`와 같은 표기라서 evict된 session의 blocklist 항목을 바로 찾을 수 있다.
`EntityID`, `Hostname`, `Root`, `PID`는 client가 session을 열 때 보낸 metadata다.
fixture 자신이 control 컨테이너에서 잠깐 여는 libcephfs session도 목록에 나올 수 있다.
특정 client는 `ID`(libcephfs의 instance ID)나 `Hostname`으로 찾는다. host network에서는
모든 컨테이너의 hostname이 host 이름과 같으므로 `ID`로 찾아야 한다.

evict가 만든 blocklist 항목은 fixture가 만든 것이 아니므로 `Restore`가 지우지 않는다.
Ceph가 `mds_blocklist_interval`(기본 1시간) 뒤에 스스로 지운다. 같은 client
프로세스라도 새로 mount하면 nonce가 달라서 이 항목의 영향을 받지 않는다.

## Ceph에서 확인한 동작

두 값을 30초로 줄이고 libcephfs client가 파일에 `durable`을 쓰고 fsync한 뒤
`-volatile`을 cache에만 남긴 상태에서 client 컨테이너를 멈췄다.

| 단계 | 관측 |
| --- | --- |
| 멈추기 전 | session state `open`, capability 2개, `EntityID=admin`, `Root=/` |
| 멈춘 뒤 | 다른 client의 `stat`과 읽기가 멈춘 client의 capability 회수를 기다림 |
| 33~35초 뒤 | session이 목록에서 사라지고 같은 `Address`가 blocklist에 추가됨. `stale` 상태는 timeout과 autoclose가 같아서 따로 관측되지 않음 |
| 다른 client | 37~38초 기다린 뒤 `durable`만 읽음. 멈춘 client의 cache에 있던 데이터는 사라짐 |
| Resume 뒤 기존 session | 쓰기가 errno 108(ESHUTDOWN)로 실패하고 client log에 blocklist 기록이 남음 |
| Resume 뒤 새 mount | 새 instance ID로 mount되고 `durable`을 읽음 |

evict 뒤 health는 `HEALTH_OK`였다.

## 실행 검증

`TestCephFSPausedClientEviction`은 bridge와 host network 각각에서 위 단계를 모두
확인한다. 기본값 60·300초 읽기, 30초 미만 거부, 적용 값 readback, 복원 뒤 기본값
readback도 함께 확인한다. macOS Docker Desktop(Linux ARM64 엔진)에서 원본 Quay 20.2.4와
19.2.5 이미지로 실행해 두 release 모두 통과했고, release에 따른 차이는 없었다. CI에서는 short 범주의 `cephfs_fixtures_sessions` batch로
Tentacle과 Squid 모두에서 실행한다.
