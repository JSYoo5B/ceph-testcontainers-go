# Structural metrics: sample on a merged change

Historical record for [Structural metrics](../contributing/structural-metrics.md).
It quotes type names that no longer exist in `ceph`, so it is listed as a
historical record for the retired-name check; the names below are not current
API.

The merged change moved the OSD flag override handle from `ceph` to `override`
(#146). Command, with the base and head commits named in the paired-change
sample of issue #226:

```sh
go run ./internal/tools/structmetrics delta -packages ceph,override \
  -base-ref ecb052c9af0f1d98e34886783293027a9a0e9a72 \
  -head-ref 6709439bb24df1c63be66ee134628c975902bde6
```

| Owner | Exported | Private | Fields | Status |
| --- | ---: | ---: | ---: | --- |
| `ceph.Container` | 43 to 42 | 189 to 189 | 35 to 35 | changed |
| `ceph.overrideHooks` | 22 to 24 | 0 to 0 | 0 to 0 | changed |
| `ceph.OSDFlagOverrideHandle` | 1 to - | 1 to - | 3 to - | removed |
| `ceph.osdFlagOverrideState` | 0 to - | 0 to - | 4 to - | removed |
| `override.OSDFlagHandle` | - to 1 | - to 2 | - to 3 | introduced |
| `override.osdFlagState` | - to 0 | - to 0 | - to 5 | introduced |

The two removed `ceph` rows and the two introduced `override` rows carry the
`146-override-osdflag-package` move record. The package totals of `ceph` and
`override` also changed and are reported as `package` rows. These are the numbers
of the paired-change sample in issue #226; both commits are resolved and
recorded in the report header.
