# Bootstrap 옵션 조합

`ceph.Run`과 서비스 패키지의 `Run`(`cephfs.Run`, `rgw.Run`, `rbd.Run`)이 받는 옵션 중
어떤 조합을 거부하고, 어떤 조합을 실제 cluster로 검증했는지 정리한다. 각 옵션의 의미는
README의 옵션 표와 옵션별 문서를 따른다.

## 거부하는 조합

아래 조합은 컨테이너나 network를 만들기 전에 오류를 반환한다. 옵션 순서와 관계없이
모든 옵션을 적용한 뒤 검사한다.

| 조합 | 오류 | 위치 |
| --- | --- | --- |
| `WithHostAddress` + bridge mode | `WithHostAddress requires WithHostNetwork` | [ceph.go](../internal/cluster/ceph.go) |
| `WithHostNetwork` + `WithSeparateClusterNetwork` | `WithSeparateClusterNetwork requires bridge mode` | [ceph.go](../internal/cluster/ceph.go) |
| `WithNoInitialOSDs` + `WithOSDCount` 또는 `WithInitialOSDs` | `WithNoInitialOSDs cannot be combined with explicit initial OSD count or layout` | [composition.go](../internal/cluster/composition.go) |
| `WithNoInitialOSDs` + 초기 pool, filesystem, gateway | `WithNoInitialOSDs requires no initial user pools, CephFS filesystems or RGW gateways; add OSDs before provisioning them` | [composition.go](../internal/cluster/composition.go) |
| `WithNoInitialManagers` + `WithManagerCount` | `WithNoInitialManagers cannot be combined with an explicit initial manager count` | [composition.go](../internal/cluster/composition.go) |
| `WithNoInitialManagers` + 초기 filesystem, gateway | `WithNoInitialManagers requires no initial CephFS filesystems or RGW gateways; add a manager before provisioning them` | [composition.go](../internal/cluster/composition.go) |
| 기본 CRUSH root의 OSD 수 < 자동 생성 pool의 replica 수 | `default CRUSH root %q needs %d initial OSDs ...` | [composition.go](../internal/cluster/composition.go) |
| pool이 요구하는 failure domain 수 > 초기 layout의 domain 수 | `initial pool %q needs %d distinct %s domains ...` | [composition.go](../internal/cluster/composition.go) |
| 이름이 겹치는 pool, filesystem, gateway (filesystem이 만드는 pool 이름 포함) | `initial pool %q is duplicated` 등 | [composition.go](../internal/cluster/composition.go) |
| `NoInitialMDS` filesystem + active/standby 수 지정 또는 standby replay | `NoInitialMDS requires omitted active/standby counts and no standby replay` | [cephfs.go](../internal/cluster/cephfs.go) |

서비스 패키지의 `Run`은 자기 기본 자원(filesystem, gateway, RBD pool)을 추가하므로
위 규칙에 따라 다음과 같이 동작한다.

| `Run` | `WithNoInitialOSDs` | `WithNoInitialManagers` |
| --- | --- | --- |
| `cephfs.Run` | 거부 | 거부 |
| `rgw.Run` | 거부 | 거부 |
| `rbd.Run` | 거부 | 허용. pool은 MGR 없이도 만들 수 있음 |

이 거부 규칙은 Docker 없이 도는 unit test가 보호한다.
`TestInvalidSettingsDoNotCreateResources`, `TestInvalidInitialCompositionDoesNotCreateResources`,
`TestNoInitialOSDsRejectsContradictionsBeforeRuntime`,
`TestNoInitialManagersRejectsContradictionsBeforeRuntime`,
`TestSeparateClusterNetworkRejectsHostBeforeCreation`,
`TestRunRequiresStorageForItsDefaultService`가 그 예다.

자원을 만든 뒤 실행 환경에서 거부하는 경우도 있다. host network를 쓸 수 없는 Docker
engine(Linux가 아닌 engine), Docker host에 없는 `WithHostAddress` 주소, test process에서
첫 MON에 닿지 않는 경우, public과 cluster subnet이 겹치는 경우다.

## 실제 cluster로 검증한 조합

통합 테스트가 `Run`에 함께 넘기는 옵션 기준이다. "host"는 host network 변형에서만,
"bridge"는 bridge 변형에서만 더하는 옵션이다.

| 조합 | 테스트 | CI 범주 / batch |
| --- | --- | --- |
| `WithNoInitialOSDs` + host 또는 bridge의 `WithSeparateClusterNetwork` | no_initial_osd | topology / empty_bootstrap |
| `WithNoInitialOSDs` + `WithDefaultCRUSHRoot` + `WithPoolDefaults(1,1)` + host | no_initial_osd | topology / empty_bootstrap |
| `WithNoInitialManagers` + `WithNoInitialOSDs` 또는 초기 pool + host | no_initial_manager | topology / empty_bootstrap |
| `WithNoInitialOSDs` + `WithOSDBlockSize`(64~512 MiB) + `WithOSDInMemoryStorage` (bridge만) | osd_block_size, osd_memory_storage | topology / empty_bootstrap |
| `WithMessengerMode(MessengerV2Secure)` + `WithNoInitialOSDs` + `WithOSDBlockSize` + host 또는 bridge의 `WithSeparateClusterNetwork` | messenger_secure | topology / messenger_secure |
| 두 cluster의 messenger mode 혼합 + `WithPoolDefaults(1,1)` | messenger_secure_multicluster | multicluster / messenger_secure_mix |
| `WithNoInitialOSDs` + `WithOSDBlockSize(512 MiB)` + host | container_pause, osd_full_ratios, pool_relocation, scrub | recovery·short |
| `WithInitialOSDs`(device class) + `WithConfigFile` + host | config | short / cluster_fixtures |
| `WithInitialOSDs`(rack, root) + `WithPoolDefaults(3,2)` + `WithDefaultCRUSHRoot` | placement | short / default |
| 초기 pool + filesystem + gateway + host 또는 bridge의 `WithSeparateClusterNetwork` | diagnostics | topology / diagnostics |
| `WithMonitorCount(3~5)` + `WithManagerCount(1~2)` + 서비스 + host | topology, monitor_rolling, network_topology | topology |
| `WithHostNetwork` + `WithHostAddress` | host_network, host_network_retry | multicluster / multicluster_topology_infra |
| 서비스 `Run` + `WithOSDCount`, 그리고 `ceph.Run` + 세 서비스 옵션 + host | service_packages, cephfs_session | short |

이 밖의 대부분 테스트는 `WithOSDCount(1~4)`와 host 변형의 `WithHostNetwork`만 쓴다.
테스트별 전체 batch는 각 파일 첫 줄의 `//go:build`에서 확인한다.

이전에 확인하지 않았던 아래 네 조합은 `TestBootstrapOptionCombinations`가 검증한다
(topology / bootstrap_combinations). 각 경우 OSD 기동, clean 대기, client의 object 쓰기와
읽기를 확인한다.

| 조합 | 확인 내용 |
| --- | --- |
| host + `WithNoInitialOSDs` + `WithOSDBlockSize(256 MiB)` + `WithOSDInMemoryStorage` | 작은 OSD 절차로 `AddOSD` 두 번 |
| host + `WithNoInitialOSDs` + `WithOSDBlockSize(128 MiB)` | 같음 |
| `WithMessengerMode(MessengerV2Secure)` + `WithConfigFile` | messenger mode와 config file 값이 osd.0에 적용됨 |
| `rbd.Run` + `WithNoInitialManagers` | MGR 없이 RBD pool 초기화, object 쓰기와 `rbd create` 성공. MGR이 없으면 PG 통계가 없어서 clean 대기는 건너뜀 |

## 지원하지 않는 조합

거부하지는 않지만 동작하지 않는 조합이다.

| 조합 | 결과 | 대신 쓸 방법 |
| --- | --- | --- |
| 초기 OSD(`WithNoInitialOSDs` 없음) + 작은 `WithOSDBlockSize`(128~256 MiB에서 확인) | OSD가 첫 기동 때 mClock 용량 측정을 하다가 BlueStore ENOSPC로 중단되고, `Run`이 OSD up/in을 기다리다 시간 초과로 실패한다. | [작은 OSD 절차](SMALL_OSD_STORAGE.md): `WithNoInitialOSDs`로 시작해 `osd_mclock_skip_benchmark`를 켠 뒤 `AddOSD` |

`WithOSDInMemoryStorage`의 최대 크기가 OSD 수와 block 크기의 곱보다 작은지는 검사하지
않는다. 부족하면 OSD가 tmpfs를 채우는 시점에 실패한다.

아직 실제 cluster로 확인하지 않은 조합은 127.0.0.1이 아닌 `WithHostAddress`를 다른
옵션과 함께 쓰는 경우다. host_network_retry 테스트만 다른 주소를 쓴다.
