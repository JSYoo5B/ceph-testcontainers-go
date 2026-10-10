# Ceph 설정 파일

`ceph.WithConfigFile(path)`는 테스트 호스트의 `ceph.conf` 형식 파일을 읽어서
fixture가 만드는 bootstrap 설정에 합친다. testcontainers-go module들이
`postgresql.conf`, `redis.conf`, `nginx.conf`를 `WithConfigFile`로 받는 방식과
같다. 파일은 서비스의 원래 설정 형식을 그대로 쓰고, 클러스터 구성(daemon 수,
pool, filesystem, gateway)은 지금처럼 Go option으로 고른다.

```go
cluster, err := cephfs.Run(ctx, ceph.DefaultImage,
    ceph.WithOSDCount(3),
    ceph.WithConfigFile(filepath.Join("testdata", "ceph.conf")),
)
```

```ini
# testdata/ceph.conf
[osd]
bluestore cache size = 134217728
osd memory target = 1073741824
[mon]
mon_max_pg_per_osd = 320
[client.admin]
rados_osd_op_timeout = 25
```

## 적용 범위

설정은 첫 MON이 store를 만들기 전에 `/etc/ceph/ceph.conf`에 들어간다. 이후
MON, MGR, OSD, MDS, RGW와 `WithClient` 컨테이너, `ConnectionConfig`가 모두 같은
파일을 받으므로 bootstrap 시점에만 읽히는 설정도 반영된다. 이미 실행 중인
클러스터에는 적용하지 않는다. 실행 중 변경은 `TemporaryConfig`를 쓴다.

같은 section과 key가 fixture 기본값에도 있으면 파일의 값으로 바꾸고 fixture
값은 지운다. Ceph는 같은 key가 두 번 나오면 첫 값을 쓰기 때문에, 뒤에 덧붙이는
방식으로는 기본값을 바꿀 수 없다. Key 이름의 공백, `-`, `_`는 Ceph와 같이 같은
이름으로 본다. 파일을 여러 개 주면 앞의 파일 위에 뒤의 파일을 key 단위로 덮는다.
공용 파일 하나에 테스트별 파일을 더하는 구성이 가능하다.

Ceph는 local 파일을 MON configuration database보다 우선한다. 그래서 이 파일에
넣은 key는 `TemporaryConfig`나 `ceph config set`으로 바꿔도 daemon에 반영되지
않는다. 테스트 중에 바꿀 설정은 파일에 넣지 않는다.

RGW는 `client.admin` identity로 실행된다. RGW 설정은 `[client]`나
`[client.admin]`에 둔다. `[client.rgw.*]` section은 RGW에 적용되지 않는다.

## 거부하는 입력

다음 key는 fixture가 직접 관리하거나 다른 option으로 고르므로 거부한다. 오류
메시지에 대신 쓸 option을 함께 적는다.

| Key | 대신 쓰는 방법 |
| --- | --- |
| `fsid`, `mon_host`, `mon_initial_members`, `mon_addr` | fixture가 생성, `WithMonitorCount` |
| `public_network`, `cluster_network`, `public_addr`, `cluster_addr`, `*_bind_addr` | `WithHostNetwork`, `WithSeparateClusterNetwork`, `WithHostAddress` |
| `auth_*`, `keyring`, `key`, `keyfile` | fixture의 Cephx 구성 |
| `ms_*_mode`, `ms_mon_*_mode`, `ms_bind_*` | `WithMessengerMode`, `WithHostNetwork` |
| `osd_pool_default_size`, `osd_pool_default_min_size` | `WithPoolDefaults` |
| `osd_pool_default_crush_rule` | `WithDefaultCRUSHRoot` |
| `bluestore_block_size` | `WithOSDBlockSize` |
| `osd_objectstore`, `bluestore_block_path`, `bluestore_block_create` | fixture의 OSD 저장소 구성 |
| `log_file`, `log_to_file`, `log_to_stderr`, `err_to_stderr`, `mon_cluster_log_to_*` | daemon log는 컨테이너 출력으로 고정 |
| `admin_socket`, `run_dir`, `mon_data`, `osd_data`, `mgr_data`, `mds_data` | fixture 경로 고정 |
| `mds_join_fs`, `mds_cache_memory_limit` | MDS 명령행에서 지정 |
| `rgw_frontends`, `rgw_thread_pool_size`, `rgw_exit_timeout_secs`, `rgw_realm`, `rgw_zonegroup`, `rgw_zone`, `rgw_sync_obj_etag_verify` | RGW 명령행에서 지정 |

명령행으로 넘기는 값은 파일보다 우선하므로, 받아들이면 조용히 무시된다. 그래서
이런 key도 오류로 처리한다.

형식 검사도 container를 만들기 전에 끝낸다. Section 밖의 key, `!include`와
`include`/`includedir`, 줄 끝 `\`로 이어지는 줄, 같은 파일 안에서 같은 section의
중복 key, NUL이나 UTF-8이 아닌 내용, 64 KiB를 넘는 파일(합친 결과 포함)을
거부한다. Ceph는 따옴표 안의 `#`, `;`도 주석으로 읽는다. `"a;b"`처럼 따옴표가
깨지는 값은 거부하고, 문자 그대로 쓰려면 `\#`, `\;`로 escape한다.

## 검증

단위 테스트는 파싱, 정규화, 파일 간 덮어쓰기, 거부 규칙을 확인한다. 실제
`mon.sh`를 stub 명령으로 실행해 합친 결과에 section이 한 번씩만 나오고 바꾼 key가
하나만 남는지도 본다. 이 결과는 MON 주소 갱신과 FSID 판독, 기본 CRUSH rule
지정 helper가 그대로 처리할 수 있어야 한다.

`TestConfigurationOverrides`의 bridge와 host 실행은 이 파일로 클러스터를 띄운다.
`ceph config show`로 모든 OSD의 `bluestore_cache_size`가 fixture 기본값 대신 파일
값인지, fixture에 없던 `osd_memory_target`도 적용됐는지 확인한다. `mon.a`의
`mon_max_pg_per_osd`가 bootstrap 때부터 파일 값인지도 본다. `ConnectionConfig`에
`[client.admin]` 설정이 들어 있고 fixture의 원래 `bluestore cache size` 줄이 남지
않았는지도 함께 확인한다.

`osd_memory_target`은 Ceph가 896 MiB보다 작은 값을 무시한다. 무시된 값은 오류
없이 Ceph 기본값 4 GiB로 남으므로, 파일에 넣은 값은 `ceph config show`로 실제
적용 여부를 확인한다.
