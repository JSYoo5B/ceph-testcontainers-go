# CephFS mirror의 원래 process 관측

Peer/directory 제거 receipt의 `ProcessQuiescence(ctx)`는 제거를 시작할 때 확보한 원래 container run과 Ceph filesystem watcher가 끝났는지 읽습니다. `OriginalQuiescent`는 원래 cohort에 대한 관측입니다. 현재 fixture 전체의 복제 중단이나 정상 peer shutdown·directory cycle 해제를 대신하지 않습니다.

## 선택적 observer

`cephfs.MirrorConfig.OriginalProcessClientFactory`로 새 raw Docker client를 제공해야 합니다. 각 nonnil client의 소유권은 fixture로 넘기며 동일 client를 여러 fixture에 공유하지 않습니다. Factory가 없으면 이 capability는 비활성입니다. 준비된 container 이미지를 그대로 사용하고 일반 Testcontainers Run·customizer·reuse·partial startup 계약을 유지합니다.

```go
config.OriginalProcessClientFactory = func(ctx context.Context) (*client.Client, error) {
    docker, err := testcontainers.NewDockerClientWithOpts(ctx)
    if err != nil {
        return nil, err
    }
    return docker.Client, nil
}
mirror, err := cephfs.RunMirror(ctx, image, config)
```

예제의 `client`는 `github.com/moby/moby/client`입니다. Testcontainers wrapper의 `Info`는 [v0.44.0에서 전역 캐시](https://github.com/testcontainers/testcontainers-go/blob/v0.44.0/docker_client.go#L26-L56)를 사용하므로 process observer는 반환된 raw client의 Info를 매번 호출합니다.

처음 성공한 daemon Run 뒤 실제 반환 handle과 raw client 양쪽에서 동일 full container ID·정상 실행 상태·StartedAt을 반복 조회해 연결을 확인합니다. Raw engine ID도 관측 전후 일치해야 합니다. 확인되지 않은 client는 원래 engine의 observer로 채택하지 않습니다. 선택적 연결 실패는 Run 결과를 유지하며 `daemon.ProcessObserverBindingStatus()`에서 finite state로 확인할 수 있습니다. Capability가 필요한 strict Begin은 모든 원래 구성원의 binding을 확인한 뒤 제거를 시작합니다. `ProcessObserverBindingStatus`는 `disabled`, `partial-startup`, `context-unavailable`, `factory-error`, `empty-client`, `positive-binding-failed`, `bound`, `closed`를 구분합니다. Available은 local client 소유 상태이며 지금 engine에 접근할 수 있다는 의미는 아닙니다. 이 context-free getter는 진행 중인 raw query의 잠금을 기다릴 수 있습니다.

Binding 준비에는 최대 30초의 child context를 사용하고 factory는 이를 따라야 합니다. Context를 무시하는 임의 callback을 강제로 중단하지는 않습니다. Nonnil client와 오류가 함께 반환되면 client를 닫고 원래 성공한 Run을 유지합니다. Run 자체가 partial container와 오류를 반환하면 원래 container를 cleanup에 등록하고 factory를 호출하지 않습니다.

## 원래 cohort의 판단

Receipt는 binding의 생성 시각과 별개로 Begin 당시의 원래 StartedAt·native watcher GID를 고정합니다. Begin 전 정상 Stop/Start 뒤에는 현재 run을 확인해 새 receipt를 만들 수 있습니다. Begin 뒤 새 run이나 watcher를 원래 witness로 바꾸지 않습니다. Raw client와 원래 반환 handle은 daemon inventory에서 제거해도 fixture cleanup까지 유지합니다.

`OriginalQuiescent=true`에는 다음 관측이 모두 필요합니다.

- 원래 cluster FSID·filesystem·metadata pool·peer 또는 path generation이 유지됩니다.
- Peer receipt에서는 원래 peer UUID가 MON/MGR 양쪽에서 없습니다. Directory receipt에서는 원래 peer tuple을 유지하고 exact native `mirror ls`에서 선택 경로가 관측 전후 없습니다.
- 같은 retained raw client의 uncached engine Info 사이에서 원래 full CID를 조회합니다. 정상 종료 기록, 원래 StartedAt보다 뒤의 정상 run 또는 확인된 원래 CID 부재가 필요합니다.
- 원래 metadata object의 전체 watcher set을 반복 읽고, 모든 주소에서 원래 numeric GID가 없습니다.
- 마지막 policy·owner·context 확인까지 모두 성공합니다.

원래 StartedAt의 running run 또는 paused/restarting/created/removing/dead 등의 불완전한 상태, 모순된 flags·PID·timestamp, malformed 조회, query 오류·취소는 종료 증거로 쓰지 않습니다. 다른 endpoint의 404, engine ID 변경·부재도 원래 container의 제거를 증명하지 않습니다. 새 GID는 현재 별도 process의 관측일 뿐이며 원래 GID를 대신하지 않습니다.

## Lifetime과 후속 범위

이 API는 최대 30초의 read-only 관측이며 caller의 더 짧은 deadline을 따릅니다. 부분 daemon 결과와 오류를 함께 반환하고 `OriginalQuiescent` 성공은 마지막 확인 뒤에만 기록합니다. Factory와 endpoint 오류의 원문은 공개하지 않으며 finite 문제 설명과 canonical context 원인을 사용합니다.

`ProcessQuiescence`는 기존 `Drained`·`Released`나 pending overlap gate를 변경하지 않습니다. 원래 daemon을 모두 명시적으로 제거한 뒤 새 증거를 확인하고 overlap gate를 여는 별도 `AcknowledgeProcessQuiescence`는 [복구 승인 계약](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다. 기존 `Drained`·`Released` 상태와 분리합니다. Fixture 전체 종료는 기존 `Terminate`로 수행합니다.

Docker engine ID는 opaque identity이며 engine process의 incarnation이나 연속된 실행을 증명하지 않습니다. 신뢰하는 일반 Docker API endpoint와 foreground run을 전제로 합니다. 같은 ID를 가장하는 proxy·engine clone, checkpoint restore/runtime migration, 경합하는 외부 policy·container 변경, irreversible death, 원격 lock 해제·snapshot 완료는 이 관측의 계약 밖입니다. 실제 복제 데이터는 별도의 source checkpoint 및 destination client I/O로 확인합니다.

## 검증

15개 새 unit parent는 optional binding 소유권·실패 상태, 실제 raw SDK 조회와 context 원인, 같은 CID의 handle 대체 거부, Begin 전 재시작, Stop/later-run/removed, numeric GID alias, 마지막 policy·watcher·context 변화와 held gate를 검증합니다. 전체 `make check`의 unit·race·vet·tag compile 및 독립 코드 검토가 통과했습니다.

`TestMultiClusterCephFSOriginalProcessQuiescence`는 원본 고정 Quay Ceph 20.2.4 digest `6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`, Linux ARM64에서 독립 cluster pair 네 개를 순서대로 실행했습니다.

| Network | Receipt | 결과 |
| --- | --- | --- |
| bridge | peer | PASS 264.33초 |
| bridge | directory | PASS 257.63초 |
| host | peer | PASS 263.70초 |
| host | directory | PASS 258.98초 |

각 케이스는 별도 raw Docker oracle로 원래 engine/full CID를 확인하고, Begin 전 Stop/Start로 creation과 receipt StartedAt·GID가 다른 것을 검증합니다. 동일 receipt에서 정상 Stop→새 Start→RemoveDaemon의 12개 관측을 public 결과와 독립 native watcher·MON/MGR 정책·filesystem/metadata pool 조회에 대조했습니다. Factory와 stateful customizer는 케이스당 1회이며 원래 client는 membership 제거 뒤에도 유지됐습니다. Drained·Released는 false로 유지되고, pending overlap은 추가 CLI 없이 거부됐습니다.

Source/destination의 32개 positive byte/hash 기록은 같은 초기 snapshot의 반복 검증입니다. 각 기록은 43,264 bytes와 SHA256 `5c05125cbff3905a10468fde94eda262d216abb2bf5733213955a07f63cd5fb9`를 확인합니다. Public checkpoint 관측 8회는 케이스당 동일 초기 source ID/name을 두 번 읽은 것이며 독립 초기 checkpoint는 4개입니다. 원래 task/watcher retirement를 확인한 뒤 만든 새 snapshot은 12개 관측 구간에서 각각 10초 이상·5–6회 미도착으로 확인했습니다. 이 시간 구간의 관측을 전체 fixture의 영구 중단이나 모든 metadata 보존으로 확대하지 않습니다.

`artifacts/cephfs-process-quiescence-20261007/`에 원본 로그·source manifest·독립 검토·selector inventory·strict 검증을 보존합니다. Package terminal은 PASS 1045.106초이며 source·Makefile 227개 SHA256은 실행 전후 동일합니다. 동일 Docker engine에서 신규 container/network/Ryuk는 0개이고 이미지 정책 SHA256 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`를 유지했습니다. 해당 source의 필수 selector 108개에 이 parent를 추가했습니다. 현재 이 parent는 별도 `scenario-cephfs-removal` profile에서 실행합니다. 이번 focused 실행은 새 전체 CI나 다른 이미지 계열의 전체 PASS를 뜻하지 않습니다.
