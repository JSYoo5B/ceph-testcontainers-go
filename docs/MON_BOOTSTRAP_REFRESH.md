# MON 교체 후 client와 multicluster 재연결

실행 중인 Ceph session은 새 monmap을 배우지만 프로세스를 다시 시작하면 파일의 bootstrap 주소를 사용합니다. Cluster가 소유한 daemon은 `AddMonitor`·`RemoveMonitor`·cluster `RefreshMonitorConfig`로 갱신합니다. `WithClient`로 만든 caller client와 multicluster 연결은 별도 snapshot을 소유하므로 명시적으로 갱신합니다.

| 대상 | 공개 API | 갱신 범위 |
| --- | --- | --- |
| 현재 quorum 주소 | `cluster.MonitorBootstrapAddresses(ctx)` | 원래 FSID와 현재 majority monmap을 확인해 versioned MON address vector 반환 |
| caller client | `cluster.RefreshClientMonitorConfig(ctx, client)` | 지정 container의 `/etc/ceph/ceph.conf` global `mon_host`만 갱신 |
| RBD link | `rbdLink.RefreshMonitorConfig(ctx)` | source/destination setup CLI와 현재 owned destination daemon의 local 설정 |
| RBD remote peer | `rbdLink.Rebootstrap(ctx)` | 기존 native bootstrap import로 receiving peer의 source MON 주소·key 갱신 |
| CephFS link | `fsLink.RefreshMonitorConfig(ctx)` | 현재 owned source daemon의 local 설정 |
| CephFS remote peer | `fsLink.RefreshPeerMonitorConfig(ctx)` | 기존 peer UUID의 config-key에서 destination `mon_host`만 갱신 |

파일 갱신 API는 Docker archive를 사용하므로 running/stopped container 모두 처리합니다. Keyring·개별 section·private 설정·container ID·process 상태와 cleanup 소유권을 유지합니다. 지정 client의 FSID가 original cluster template 및 현재 quorum과 같아야 하며 잘못된 schema·중복/모호한 설정·foreign identity를 거부합니다. 이미 최신인 파일은 쓰지 않습니다.

성공한 일부 복사는 다른 복사 실패나 caller deadline 뒤에도 유지됩니다. 새 context로 재시도하면 최신 파일은 건너뛰고 나머지를 갱신합니다. Common API는 최대 1분, link API는 최대 2분과 caller context를 따르며 mutex 대기도 해당 context에 포함됩니다. Cluster MON 변경을 먼저 완료한 뒤 refresh batch를 실행합니다. 파일·native policy·config-key의 외부 변경을 여러 조회와 원자적으로 묶지 않으므로 같은 시간에 변경하지 않습니다.

RBD는 양쪽 원래 pool ID와 default/selected namespace의 mirror UUID·scope·mapping·site를 갱신 전후 확인합니다. CephFS는 원래 양쪽 filesystem·metadata pool ID와 peer destination tuple을 확인합니다. CephFS peer 갱신은 원래 destination FSID·key와 확장 JSON field를 보존하고 현재 destination quorum 주소만 기록합니다. 불확실한 write 응답은 오류로 남고 이미 적용된 값의 재시도는 write를 반복하지 않습니다. Auth key를 포함한 config-key payload는 control의 0600 임시 파일로 전달하며 argv에 넣지 않습니다. 별도 bounded cleanup의 실패도 보고합니다. 그 경우 파일은 control 수명 안에 남을 수 있으며 config가 이미 최신인 재시도는 이전 임시 파일 cleanup을 재수행하지 않습니다.

CephFS native replayer는 remote peer 설정을 초기화할 때 읽습니다. 갱신 후 필요한 daemon을 명시적으로 stop/start하고 준비 상태와 실제 데이터를 확인합니다. 초기 peer 생성 및 `RebootstrapPeer`도 current destination monmap으로 token의 `mon_host`를 보정합니다. 실행 중인 MGR의 `conf_get("mon_host")`는 파일 갱신 이후에도 이전 값일 수 있기 때문입니다. Stale token 입력의 보정은 unit에서 별도로 검사하며 native PoC에서는 MGR를 명시적으로 재시작하지 않은 peer 재등록과 current 주소·데이터를 확인합니다. 기존 peer 주소 갱신과 제거 뒤 새 UUID 생성은 구분합니다.

```go
// 양쪽 cluster의 MON rolling replacement를 먼저 완료합니다.
if err := source.RefreshClientMonitorConfig(ctx, sourceClient); err != nil {
    return err
}
if err := destination.RefreshClientMonitorConfig(ctx, destinationClient); err != nil {
    return err
}
if err := rbdLink.RefreshMonitorConfig(ctx); err != nil {
    return err
}
if err := rbdLink.Rebootstrap(ctx); err != nil {
    return err
}
if err := fsLink.RefreshMonitorConfig(ctx); err != nil {
    return err
}
if err := fsLink.RefreshPeerMonitorConfig(ctx); err != nil {
    return err
}
// 명시적으로 daemon을 cold-start한 뒤 WaitReplayReady /
// WaitSnapshotSynced 및 독립 destination client의 bytes를 검증합니다.
```

새 peer·daemon·network를 만들거나 data/policy를 재구성해 재연결을 대신하지 않습니다. Custom image는 기존 control/all runtime 계약을 그대로 소비하며 image builder·mirror role·go-ceph/cgo dependency를 추가하지 않습니다.

Native 근거: [RBD bootstrap peer 재사용과 attribute 갱신](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/api/Mirror.cc#L203-L250), [CephFS bootstrap token과 peer config](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/mirroring/fs/snapshot_mirror.py), [CephFS replayer 초기 remote config](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L239-L274).

## 검증

`TestMultiClusterMonitorBootstrapRefresh/bridge`와 `/host`는 서로 다른 FSID의 두 cluster에서 a,b,c → d,e,f 전체 MON 교체, 중지된 caller client·RBD/CephFS daemon의 explicit local/remote 갱신, 같은 container cold restart, original native identity·keyring·개별 설정 보존을 검사합니다. RBD는 8 MiB 전체 원문과 새 ranged journal write, CephFS는 source에서 독립적으로 읽은 snapshot ID·이름과 destination의 파일·symlink·mode·owner를 비교합니다. Warm destination MGR에서 peer 제거·새 token import도 별도 phase로 검사합니다.

2026-10-07 원본 고정 Quay Ceph 20.2.4, Docker Desktop Linux ARM64에서 bridge는 329.563초 PASS입니다. Host도 362.045초 PASS입니다. RBD remote 주소는 native comma/vector/nonce 표현을 type/IP/port multiset으로 비교하며 원래 UUID·auth key도 확인합니다. 임의 IP/port 재사용이나 warm session이 오래된 파일/peer 주소 검증을 대신하지 못하게 합니다.

최종 `artifacts/bootstrap-refresh-20261007/bridge-runtime-final.log`·`host-runtime.log`와 각 `bridge-final-cleanup/after.json`·`host-cleanup/after.json`에 terminal PASS 및 같은 engine의 신규 container/network/Ryuk 잔여 0개를 보관합니다. `verification.json`은 4개씩의 source checkpoint(2/backup-1 → cold restart의 동일 2/backup-1 → 3/backup-2 → 새 peer의 4/backup-3), terminal·cleanup 원본을 대조합니다. `source-final-before.json`·`source-final-after.json`의 관련 source 205개 SHA256은 두 최종 실행 전후 동일하며 고정 이미지 정책 SHA256도 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`로 유지됐습니다. 초기 bridge는 검증 helper가 설정 전체 줄을 주소 값과 비교한 오류로 실패했으며 `bridge-runtime.log` 및 신규 잔여 0개인 `bridge-cleanup/after.json`으로 별도 보존합니다. 주소 비교를 수정하고 RBD remote 주소 직접 검사도 추가한 최종 source의 결과와 구분합니다.

`check.log`의 unit·race·vet·전체 tag compile과 최종 integration의 `tag-compile-final.log`가 PASS입니다. `selector-inventory.json`으로 현재 Make selector의 내부 scenario 102개·SDK 2개, 총 104개를 확인했습니다. 이 focused 실행을 전체 CI나 다른 image matrix의 새 검증으로 합산하지 않습니다.
