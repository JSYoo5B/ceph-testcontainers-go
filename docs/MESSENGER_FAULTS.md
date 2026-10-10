# Messenger 지연·차단 recipe

Client의 timeout과 재시도를 테스트하려면 특정 daemon이 client 메시지에 늦게
답하거나 아예 답하지 않는 상황이 필요합니다. 공식 Quay 이미지에는 `tc`, `ip`,
`iptables`, `nft`가 없어서 netem이나 packet filter로는 이 상황을 만들 수 없습니다.
대신 Ceph messenger에 들어 있는 fault 옵션을 `TemporaryConfig`로 켜고 끕니다.
이 옵션들은 Tentacle 20.2.4와 Squid 19.2.5 공식 이미지에 모두 있고, 컨테이너
권한이나 Docker network 조작이 필요 없어서 host network에서도 같은 방식으로
동작합니다.

```go
// osd.1이 client 메시지를 버리게 한다. osd.1은 peer와 계속 통신하므로 up으로 남는다.
hold, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
    Section: fmt.Sprintf("osd.%d", cluster.OSDs()[1].ID),
    Name:    "ms_blackhole_client",
    Value:   "true",
})
if hold != nil {
    defer hold.Restore(cleanupCtx)
}
if err != nil {
    return err
}
// primary가 osd.1인 object의 op는 client의 rados_osd_op_timeout 뒤 errno 110으로 끝난다.
```

```go
// 모든 OSD가 client에게서 받은 메시지를 0~2초 사이에서 무작위로 늦게 처리한다.
for _, setting := range []ceph.ConfigSetting{
    {Section: "osd", Name: "ms_inject_delay_type", Value: "client"},
    {Section: "osd", Name: "ms_inject_delay_probability", Value: "1"},
    {Section: "osd", Name: "ms_inject_delay_max", Value: "2"},
} {
    hold, err := cluster.TemporaryConfig(ctx, setting)
    if hold != nil {
        defer hold.Restore(cleanupCtx)
    }
    if err != nil {
        return err
    }
}
```

## 옵션

| 옵션 | 효과 |
| --- | --- |
| `ms_blackhole_client`, `ms_blackhole_osd`, `ms_blackhole_mon`, `ms_blackhole_mds`, `ms_blackhole_mgr` | 설정한 daemon이 해당 종류의 상대에게서 받은 메시지를 버립니다. 연결은 끊지 않아서 상대는 자신의 timeout이 지나야 실패를 압니다. |
| `ms_inject_delay_type` | 지연을 넣을 상대 종류입니다. `client`, `osd`처럼 entity 종류 이름을 공백으로 구분해 적습니다. |
| `ms_inject_delay_probability` | 메시지마다 지연을 넣을 확률입니다. `1`이면 모든 메시지에 넣습니다. |
| `ms_inject_delay_max` | 최대 지연 초입니다. 실제 지연은 0부터 이 값 사이에서 고르게 고릅니다. |

## MDS

Active MDS에 `ms_blackhole_client`를 걸면 MDS는 MON에 beacon을 계속 보내므로
active로 남고 failover도 일어나지 않습니다. 새 client는 session을 열지 못해
`client_mount_timeout`이 지난 뒤 errno 110으로 mount에 실패합니다. MDS 컨테이너를
`PauseContainer`로 멈추면 beacon이 끊겨 MON이 standby로 교체하므로, "MDS는 살아
있지만 client에 답하지 않는" 상황은 이 옵션으로 만듭니다.

```go
status, err := fs.MDSStatus(ctx)
if err != nil {
    return err
}
hold, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
    Section: "mds." + status.Active[0].Name,
    Name:    "ms_blackhole_client",
    Value:   "true",
})
```

## Section 고르기

`Section`은 `osd`처럼 종류 전체나 `osd.1`처럼 daemon 하나를 고릅니다. 한 OSD만
차단하면 그 OSD가 acting primary인 object의 op만 멈추므로, `ceph osd map <pool>
<object>`의 `acting_primary`로 어떤 object가 영향을 받을지 미리 알 수 있습니다.

## 주의할 점

MON에는 `ms_blackhole_client`를 걸지 않습니다. Fixture의 control CLI도 client라서
MON이 client 메시지를 버리면 `TemporaryConfig`의 확인과 `Restore`까지 모두 멈추고,
설정을 되돌릴 방법이 없어집니다. 실제로 MON에 걸었을 때 `TemporaryConfig`가
timeout으로 끝났습니다. MGR에 걸어도 fixture가 MGR로 보내는 명령(subvolume,
MGR module 등)이 같은 이유로 멈추므로 피합니다. `client` section에도
`ms_blackhole_*`를 걸지 않습니다. Fixture CLI가 이 section의 설정을 읽기 때문입니다.
Client 쪽에서 지연이나 차단을 넣으려면 테스트 client의 자체 설정(librados의
`conf` 인자, `ceph.conf`)에 넣습니다.

`ms_inject_socket_failures`는 지정한 횟수마다 socket 작업을 실패시킵니다. 값을
`10`으로 두면 연결 자체가 성립하지 않아 client가 진행하지 못했고, `500`으로
두면 짧은 I/O에서는 차이가 보이지 않았습니다. 재현 결과가 실행마다 크게 달라서
이 recipe에는 포함하지 않습니다.

검증은 설정을 바꾼 뒤 새로 연결한 client로 했습니다. 이미 열려 있는 연결에도
같은 효과가 나는지는 확인하지 않았으므로, 장애 단계마다 새 client를 만듭니다.

## 검증

`TestMessengerFaultInjection`이 bridge와 host network에서 OSD 2개, replica 2
pool로 확인합니다.

1. `osd.1`에 `ms_blackhole_client`를 켜면 primary가 `osd.1`인 object의 write는
   5초 op timeout 뒤 errno 110으로 끝나고, 나머지 object는 바로 쓰고 읽힙니다.
   차단하는 동안 두 OSD는 모두 up/in입니다. 되돌린 뒤에는 모든 object를 다시
   쓰고 읽을 수 있습니다.
2. 모든 OSD에 client 메시지 지연(최대 2초)을 넣으면 write와 read가 모두 성공하고
   가장 느린 op가 0.3초를 넘습니다. 되돌린 뒤에는 모든 op가 1초 안에 끝납니다.
3. 같은 cluster에 CephFS를 만들고 active MDS에 `ms_blackhole_client`를 켜면, 새
   libcephfs mount가 5초 `client_mount_timeout` 뒤 errno 110으로 실패하고 active
   MDS의 이름과 GID는 그대로입니다. 되돌린 뒤에는 mount하고 앞서 쓴 파일을 그대로
   읽습니다.

로컬 macOS Docker Desktop(Linux ARM64)에서 Tentacle 20.2.4 공식 이미지와
Squid 19.2.5 공식 이미지로 두 network 모두 통과했습니다. CI에서는 recovery
범주의 `container_pause` batch로 실행합니다.
