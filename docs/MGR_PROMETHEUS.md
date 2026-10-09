# MGR Prometheus exporter recipe

모니터링 client나 exporter 연동을 테스트할 때 active MGR의 `prometheus` module이
metrics를 제공하게 한다. 기존 `TemporaryConfig`와 `TemporaryMGRModule`로 구성하고,
`Container.ManagerServices`로 active MGR이 광고하는 URL을 읽는다. 별도 module
전용 API는 두지 않는다.

```go
module, err := cluster.TemporaryMGRModule(ctx, "prometheus", true)
if module != nil {
    defer module.Restore(cleanupCtx)
}
if err != nil {
    return err
}
// listener가 뜨면 MGR map에 URL이 나타난다.
services, err := cluster.ManagerServices(ctx)
// services["prometheus"] == "http://<active MGR 주소>:9283/"
// WithClient 컨테이너에서 <URL>metrics를 scrape한다.
```

## ManagerServices

`ManagerServices`는 `ceph mgr services`가 보고한 module별 URL을 반환한다.
Listener를 띄운 module만 나타나며, MGR failover 뒤에는 새 active MGR의 URL로
바뀐다. URL은 module이 bind한 주소를 그대로 담는다. Docker host 포트로 매핑한
주소가 아니므로 bridge 모드에서는 `WithClient` 컨테이너처럼 cluster network에
연결된 곳에서 사용한다. 빈 map은 서비스를 광고하는 module이 없다는 뜻이다.

## Network별 구성

Bridge 모드에서는 추가 설정이 필요 없다. 각 MGR이 자기 컨테이너 주소의 9283
포트에서 듣고, standby MGR은 빈 200 응답을 돌려준다.

Host 모드에서는 기본 설정(bind 주소 `::`, 포트 9283)으로 module을 켜면 MGR 두 개
cluster의 `mgr services`가 비어 있었고 어느 MGR도 응답하지 않았다. Active와 standby
MGR이 같은 host network를 쓰므로 MGR마다 다른 포트가 필요하다. 그래서
`TemporaryConfig`로 `mgr` section의 `mgr/prometheus/server_addr`를
`PublicAddress()`로, `mgr/prometheus/<MGR 이름>/server_port`를 MGR마다 다른 값으로
지정한다. 이 두 설정을 함께 적용하면 동작하는 것을 확인했으며, 둘 중 어느 쪽이
기본 설정 실패의 원인인지는 분리해 확인하지 않았다. 다른 프로세스나 다른 host
모드 cluster와 겹치지 않는 포트를 고른다.

```go
settings := []ceph.ConfigSetting{
    {Section: "mgr", Name: "mgr/prometheus/server_addr", Value: cluster.PublicAddress()},
    {Section: "mgr", Name: "mgr/prometheus/a/server_port", Value: "19283"},
    {Section: "mgr", Name: "mgr/prometheus/b/server_port", Value: "19284"},
}
// 각 설정을 TemporaryConfig로 적용한 뒤 TemporaryMGRModule로 module을 켠다.
```

이 설정은 적용 시점의 MGR에만 포트를 준다. 나중에 `AddManager`로 추가한 MGR은
기본 포트를 쓰므로 host 모드에서는 그 MGR의 포트도 같은 방식으로 지정한다.

## 실행 검증

`TestManagerPrometheusExporter`는 MGR 두 개로 bridge와 host network 각각 다음을
확인한다.

| 단계 | 조작 | 확인 |
| --- | --- | --- |
| 활성화 | host 모드 설정, module 활성화 | 광고 URL이 active MGR의 기대 주소·포트와 일치, `WithClient` 컨테이너에서 `ceph_health_status`·`ceph_osd_up` scrape |
| failover | `ceph mgr fail` | 새 active MGR이 바뀌고 URL도 그 MGR의 주소·포트로 바뀜, 다시 scrape |
| 복원 | module과 설정 복원 | `ManagerServices`에서 `prometheus` 제거 |

Host 모드 포트는 client 컨테이너에서 빈 포트를 골라 사용한다. CI에서는
`Ceph short`의 `mgr_prometheus` batch로 실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=30m -run '^TestManagerPrometheusExporter$' ./internal/integration
```

2026-10-09 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 bridge/host 모두 PASS, 전체
68.59초였다. Bridge는 MGR `a`의 `172.19.0.3:9283`에서 failover 뒤 MGR `b`의
`172.19.0.4:9283`으로, host는 `127.0.0.1`의 MGR별 포트 사이에서 URL이 바뀌었다.
공식 역할 이미지 조합과 CI 실행은 이 기록에 포함하지 않는다.

[Prometheus module](https://docs.ceph.com/en/tentacle/mgr/prometheus/)을 참고한다.
