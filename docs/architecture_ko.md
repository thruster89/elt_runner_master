# ELT Runner — 아키텍처 및 코드 레퍼런스

## 개요

ELT Runner는 Oracle/Vertica에서 데이터를 추출(Export)하여 CSV로 변환하고,
로컬 DB(DuckDB/SQLite3/Oracle)에 로드(Load)한 뒤, 변환(Transform) 및 리포트(Report)를 생성하는
파이프라인 자동화 도구입니다. CLI(`runner.py`)와 GUI(`batch_runner_gui.py`) 모두 지원합니다.

```
[소스 DB] → EXPORT → CSV → LOAD → [타겟 DB] → TRANSFORM → REPORT → CSV/Excel
```

---

## 프로젝트 구조

```
elt_runner_master/
├── runner.py                    # CLI 진입점 (~500줄)
├── batch_runner_gui.py          # GUI 진입점 (~10줄)
│
├── engine/                      # 핵심 엔진 (공용 유틸리티)
│   ├── context.py               # RunContext 데이터클래스
│   ├── stage_registry.py        # 스테이지 이름 → 함수 매핑
│   ├── runtime_state.py         # stop_event (스레드 안전 중단 신호)
│   ├── connection.py            # connect_target() 연결 팩토리
│   ├── path_utils.py            # resolve_path() 상대→절대 경로 변환
│   └── sql_utils.py             # SQL 정렬, 테이블명, 파라미터 감지, 렌더링
│
├── stages/                      # 파이프라인 스테이지 (각각 run(ctx) 진입점)
│   ├── export_stage.py          # 소스 DB → CSV 추출 (662줄)
│   ├── load_stage.py            # CSV → 타겟 DB 로드 (251줄)
│   ├── transform_stage.py       # 타겟 DB에서 SQL 실행 (133줄)
│   └── report_stage.py          # CSV 생성 → Excel 변환 (367줄)
│
├── adapters/
│   ├── sources/                 # 소스 DB 커넥터
│   │   ├── oracle_client.py     # Oracle thin/thick 초기화, 연결
│   │   ├── oracle_source.py     # Oracle export_sql_to_csv()
│   │   ├── vertica_client.py    # Vertica 연결
│   │   └── vertica_source.py    # Vertica export_sql_to_csv()
│   └── targets/                 # 타겟 DB 로더
│       ├── duckdb_target.py     # DuckDB 로드 (Parquet 고속 경로)
│       ├── oracle_target.py     # Oracle 로드 (스키마 생성, 메타데이터)
│       └── sqlite_target.py     # SQLite3 로드 (타입 추론)
│
├── gui/                         # Tkinter GUI 패키지
│   ├── __init__.py              # BatchRunnerGUI re-export
│   ├── constants.py             # THEMES, C, FONTS, TOOLTIPS, STAGE_CONFIG
│   ├── utils.py                 # load_jobs, load_env_hosts, scan_sql_params
│   ├── widgets.py               # SqlSelectorDialog, CollapsibleSection, Tooltip
│   ├── app.py                   # BatchRunnerGUI (Mixin 조립 + __init__)
│   └── mixins/                  # Mixin 모듈 (UI, 상태, 실행, 다이얼로그, 로그, 검색)
│
├── config/env.sample.yml        # 환경 설정 템플릿
├── jobs/                        # Job 정의 파일 (두 가지 레이아웃 지원)
│   ├── *.yml                    # 글로벌 레이아웃 (레거시)
│   └── {name}/{name}.yml        # Job-centric 레이아웃 (권장)
├── sql/                         # 공유 SQL 템플릿 (선택사항)
└── VERSION                      # 버전 (단일 진실 공급원)
```

---

## 파이프라인 흐름

```
runner.py main()
  │
  ├─ CLI 인자 파싱 (--job, --env, --mode, --param, --set, --include, --stage)
  ├─ job.yml + env.yml 로드
  ├─ RunContext 생성
  │
  └─ run_pipeline(ctx)
       │
       ├─ [1] EXPORT  ─ export_stage.run(ctx)
       │     ThreadPoolExecutor → SQL 쿼리 실행 → CSV + .meta.json 저장
       │
       ├─ [2] LOAD    ─ load_stage.run(ctx)
       │     CSV 파일 → 타겟 DB (DuckDB/SQLite3/Oracle)
       │     _LOAD_HISTORY로 SHA256 기반 중복 방지
       │
       ├─ [3] TRANSFORM ─ transform_stage.run(ctx)
       │     타겟 DB에서 SQL 파일 실행 (스키마 주입)
       │
       └─ [4] REPORT  ─ report_stage.run(ctx)
             SQL → CSV 저장 후 CSV → Excel (.xlsx) 변환
```

---

## 1. RunContext (`engine/context.py`)

모든 스테이지가 공유하는 실행 컨텍스트 데이터클래스:

```python
@dataclass
class RunContext:
    job_name: str              # 예: "job_duckdb"
    run_id: str                # 예: "job_duckdb_01"
    job_config: dict           # 파싱된 job.yml
    env_config: dict           # 파싱된 env.yml (DB 접속 정보)
    params: dict               # 병합된 파라미터 (yml + CLI --param)
    work_dir: Path             # 작업 디렉토리
    mode: str                  # "plan" | "run" | "retry"
    logger: logging.Logger
    include_patterns: list     # --include SQL 필터 패턴
    stage_filter: list         # --stage 필터 목록
```

---

## 2. 스테이지 레지스트리 (`engine/stage_registry.py`)

```python
STAGE_REGISTRY = {
    "export":      export_stage.run,
    "load":        load_stage.run,
    "load_local":  load_stage.run,   # 하위 호환 별칭
    "transform":   transform_stage.run,
    "report":      report_stage.run,
}
```

---

## 3. Export 스테이지 (`stages/export_stage.py`)

가장 큰 스테이지 (~662줄). Oracle/Vertica에서 데이터를 CSV 파일로 추출합니다.

### 주요 함수

| 함수 | 역할 |
|------|------|
| `run(ctx)` | 메인 진입점 — Export 전체 조율 |
| `expand_params(params)` | 파라미터 조합 생성 (카테시안 곱) |
| `expand_range_value(value)` | 범위 문자열 파싱 |
| `build_csv_name(...)` | 출력 CSV 파일명 생성 |
| `get_thread_connection(...)` | 스레드별 DB 연결 풀링 |
| `sanitize_sql(sql)` | 후행 `;`, `/` 제거 |
| `backup_existing_file(...)` | 기존 파일 백업 로테이션 (N개 유지) |

### 파라미터 확장

```python
# 단일 값
{"clsYymm": "202303"}  →  [{"clsYymm": "202303"}]

# 범위 (YYYYMM 형식, 월 단위 인식)
{"clsYymm": "202301:202312"}  →  12개 조합 (202301..202312)

# 범위 + 필터
"202001:202412~Q"   →  분기만 (03, 06, 09, 12월)
"202001:202412~H"   →  반기만 (06, 12월)
"202001:202412~Y"   →  연말만 (12월)
"202001:202412~2"   →  특정 월만 (02 = 2월만)

# 리스트
{"region": "A,B,C"}  →  3개 조합

# 다중 파라미터 카테시안 곱
{"clsYymm": "202301:202303", "region": "A,B"}  →  6개 조합
```

### CSV 파일명 규칙

```
{SQL파일명}__{호스트}__{파라미터키}_{값}[__...].csv[.gz]

예시:
  full 모드:      01_contract__local__clsYymm_202303.csv.gz
  compact 모드:   01_contract__local__202303.csv.gz
  strip_prefix:   contract__local__clsYymm_202303.csv.gz   (01_contract → contract)
  strip_prefix:   qpv_005__local__clsYymm_202303.csv.gz   (3. qpv_005 → qpv_005)
  둘 다 적용:     contract__local__202303.csv.gz
```

### 메타데이터 (.meta.json)

각 CSV 파일마다 `.meta.json` 동반 파일이 생성됩니다:

```json
{
  "sql_file": "01_contract.sql",
  "host": "local",
  "params": {"clsYymm": "202303"},
  "columns": [
    {"name": "CONTRACT_ID", "type": "DB_TYPE_NUMBER", "precision": 10, "scale": 0},
    {"name": "CONTRACT_AMT", "type": "DB_TYPE_NUMBER", "precision": 15, "scale": 2}
  ],
  "row_count": 15420,
  "exported_at": "2026-03-01 10:30:45"
}
```

이 메타데이터는 Load 시 테이블 생성에 활용됩니다 (Oracle 타겟에서 원본 컬럼 타입 복원).

### 병렬 실행

```yaml
export:
  parallel_workers: 4   # ThreadPoolExecutor 스레드 수
```

각 스레드는 `threading.local()`로 독립 DB 연결을 유지합니다.
생성된 연결은 `_thread_connections` 리스트에서 추적하며, 종료 시 일괄 정리합니다.

---

## 4. Load 스테이지 (`stages/load_stage.py`)

Export된 CSV 파일을 타겟 데이터베이스에 로드합니다.

### 주요 함수

| 함수 | 역할 |
|------|------|
| `run(ctx)` | 메인 진입점 — 어댑터로 분배 |
| `_extract_params(csv_path)` | .meta.json에서 파라미터 읽기 (없으면 파일명 파싱) |
| `_sha256_file(path)` | 중복 방지용 파일 해시 |
| `_collect_csv_info(...)` | Plan 리포트용 파일 정보 수집 |
| `_run_load_loop(...)` | CSV 파일 순회하며 어댑터 호출 |
| `_run_load_plan(...)` | Plan 모드 — 로드 없이 파일 목록만 출력 |

### 로드 모드

| 모드 | 동작 |
|------|------|
| `replace` | 테이블 DROP → CREATE → INSERT (완전 재생성) |
| `truncate` | 전체 행 DELETE → INSERT (구조 유지) |
| `delete` | 파라미터 조건 WHERE DELETE → INSERT (부분 갱신) |
| `append` | INSERT만 (기존 데이터 유지) |

### _LOAD_HISTORY를 통한 중복 방지

각 타겟 어댑터는 `_LOAD_HISTORY` 테이블을 유지합니다:

```sql
CREATE TABLE _LOAD_HISTORY (
    job_name TEXT, table_name TEXT, file_name TEXT,
    file_hash TEXT, loaded_at TEXT
)
```

파일의 SHA256 해시가 이전 로드와 일치하면 건너뜁니다.

### Delete 모드 (엄격 모드)

`load.mode = delete`일 때, `.meta.json`에서 파라미터를 추출하여
`DELETE FROM 테이블 WHERE 파라미터_컬럼 = 값` 쿼리를 생성합니다.

컬럼 매칭: 정확한 이름 먼저 → 정규화(언더스코어 제거) 매칭 시도.

```
params: {clsYymm: "202303"}
→ DELETE FROM TB_CONTRACT WHERE CLS_YYMM = '202303'
  (clsYymm → CLS_YYMM 매칭: 언더스코어 제거 후 CLSYYMM으로 비교)
```

---

## 5. Transform 스테이지 (`stages/transform_stage.py`)

타겟 DB에서 SQL 파일을 실행하여 데이터를 변환/집계합니다.

### 주요 동작

1. `transform.sql_dir`에서 SQL 파일 읽기 (숫자 접두사 순 정렬)
2. `render_sql()`로 파라미터 치환
3. 각 SQL문을 순차 실행
4. `on_error: stop` (기본) — 첫 오류에서 중단 / `continue` — 무시하고 계속

### 스키마 주입

```yaml
transform:
  schema: MYDATA
```

**세션 스키마** (GUI schema 필드로 설정):
- DuckDB: `SET schema = 'MYDATA'` — 모든 테이블이 해당 스키마에서 조회
- Oracle: `ALTER SESSION SET CURRENT_SCHEMA = MYDATA` — 동일 효과
- `@{}` 파라미터 치환과는 별개 기능

**SQL에서 `@{}` 접두사 사용** (Params에서 설정):
- `@{src}TABLE_NAME` → `SRCSCHEMA.TABLE_NAME` (param `src=SRCSCHEMA`)
- `@{tgt}TABLE_NAME` → `TGTSCHEMA.TABLE_NAME` (param `tgt=TGTSCHEMA`)
- 값이 비어있으면 접두사 제거

### Transfer (DB→DB 전송)

Transfer 모드는 Transform 실행 시 대상 DB를 소스 DB에 ATTACH하여
SQL에서 다른 데이터베이스에 쓸 수 있게 합니다:

```yaml
transform:
  transfer:
    dest:
      type: duckdb           # duckdb | sqlite3
      db_path: other.duckdb
```

- SQL에서 `dest.schema.table`로 참조하여 ATTACH된 DB에 쓰기 가능
- DuckDB↔DuckDB, SQLite↔SQLite 조합만 지원 (ATTACH 메커니즘)
- GUI의 Transform 섹션에서 체크박스로 설정 가능

---

## 6. Report 스테이지 (`stages/report_stage.py`)

CSV 리포트를 생성하고 Excel 파일로 변환합니다.

### 두 가지 모드

**일반 모드** (`skip_sql: false`):
1. 타겟(또는 소스) DB에 연결
2. 리포트 SQL 실행 → CSV 저장
3. CSV → Excel (.xlsx) 변환

**Skip-SQL 모드** (`skip_sql: true`):
1. `csv_union_dir`에서 기존 CSV 파일 읽기
2. DB 연결 없이 바로 Excel로 변환

### Excel 생성

- 각 SQL/CSV가 워크북의 시트 하나로 생성
- `max_files`로 시트 수 제한 (초과 시 새 파일 생성)
- 데이터 기반 컬럼 너비 자동 계산

---

## 7. SQL 유틸리티 (`engine/sql_utils.py`)

### 파라미터 문법 (4종)

| 문법 | 동작 | 예시 |
|------|------|------|
| `:param` | 자동 싱글쿼트 감싸기, 리터럴 외부만 | `:clsYymm` → `'202303'` |
| `${param}` | 값 그대로 치환 (전체 대상) | `'${clsYymm}'` → `'202303'` |
| `{#param}` | `${}`과 동일 (별칭) | `{#clsYymm}` → `202303` |
| `@{param}` | 접두사 (값 있으면 `.` 자동 추가) | `@{src}TABLE` → `SRCSCHEMA.TABLE` |

**`:param` 안전 처리**: 싱글쿼트 문자열 리터럴(`'...'`) 내부의 `:word`는 치환하지 않습니다.
예: `TO_CHAR(dt, 'HH24:MI:SS')` → `:MI`, `:SS`를 파라미터로 오인하지 않음.

### 테이블명 결정

```sql
-- SQL 파일 첫 번째 비어있지 않은 줄에 힌트가 있으면:
--[MY_CUSTOM_TABLE]    ← 이 값을 테이블명으로 사용
SELECT * FROM ...

-- 힌트가 없으면 → 파일명 stem 사용:
-- 01_contract.sql → 테이블명 = "01_contract"
```

### SQL 파일 정렬

숫자 접두사 기준 정렬: `01_a.sql`, `02_b.sql`, `10_c.sql`.
접두사 없는 파일은 숫자 파일 뒤에 알파벳순 정렬.

---

## 8. 타겟 어댑터

### DuckDB (`adapters/targets/duckdb_target.py`)

- 임시 Parquet 변환을 통한 고속 CSV 임포트
- 스키마 지원 (`CREATE SCHEMA IF NOT EXISTS`)
- `_LOAD_HISTORY` 중복 방지 테이블
- `VACUUM`으로 파일 최적화

### Oracle (`adapters/targets/oracle_target.py`)

- 스키마(유저) 자동 생성: `CREATE USER` + `GRANT CONNECT, RESOURCE`
- 메타데이터 기반 테이블 생성 (소스 컬럼 타입 보존)
- NLS_DATE_FORMAT 세션 설정
- 배열 바인딩을 통한 대량 INSERT
- 4가지 로드 모드 모두 지원 (replace/truncate/delete/append)

### SQLite3 (`adapters/targets/sqlite_target.py`)

- 데이터 기반 타입 자동 추론 (INTEGER/REAL/TEXT)
- 메타데이터 미지원 (DBAPI 제약)
- replace/truncate/delete/append 모드

---

## 9. 연결 팩토리 (`engine/connection.py`)

```python
conn, conn_type, label = connect_target(ctx, target_cfg)
# conn_type: "duckdb" | "sqlite3" | "oracle"
# label: "duckdb (C:\data\result.duckdb)"
```

`target.type` 설정에 따라 적절한 어댑터의 `connect()` 함수로 분기합니다.

---

## 10. CLI 사용법 (`runner.py`)

```bash
# 기본 실행 (job-centric, 글로벌 경로 모두 가능)
python runner.py --job jobs/job_duckdb/job_duckdb.yml --env config/env.yml
python runner.py --job jobs/job_duckdb.yml --env config/env.yml  # 레거시도 지원

# Plan 모드 (사전 확인, 실제 실행 없음)
python runner.py --job jobs/job_duckdb.yml --mode plan

# Retry 모드 (실패한 작업 재실행)
python runner.py --job jobs/job_duckdb.yml --mode retry

# 파라미터 오버라이드
python runner.py --job jobs/job_duckdb.yml --param clsYymm=202301:202312

# 설정값 오버라이드
python runner.py --job jobs/job_duckdb.yml \
  --set export.compression=none \
  --set target.db_path=data/custom.duckdb

# 특정 SQL 파일만 필터링
python runner.py --job jobs/job_duckdb.yml --include contract --include payment

# 특정 스테이지만 실행
python runner.py --job jobs/job_duckdb.yml --stage export --stage load_local

# 디버그 모드 (상세 로깅)
python runner.py --job jobs/job_duckdb.yml --debug
```

---

## 11. Job YAML 구조

Job-centric 레이아웃 (`jobs/{name}/{name}.yml`). 경로 필드를 생략하면
`jobs/{job_name}/` 폴더 존재 여부에 따라 자동으로 기본값이 결정됩니다.

```yaml
job_name: my_job

pipeline:
  stages: [export, load_local, transform, report]

source:
  type: oracle              # oracle | vertica
  host: local               # env.yml의 호스트 키

export:
  # sql_dir / out_dir 생략 → job-centric 기본값:
  #   sql_dir: jobs/my_job/sql/export
  #   out_dir: jobs/my_job/data/export
  overwrite: true           # 기존 파일 덮어쓰기
  parallel_workers: 4       # 병렬 스레드 수
  compression: gzip         # gzip | none
  format: csv
  csv_name_style: full      # full | compact
  csv_strip_prefix: false   # 숫자 접두사 제거 여부 (구분자: _ . - 공백)

load:
  mode: replace             # replace | truncate | delete | append

target:
  type: duckdb              # duckdb | sqlite3 | oracle
  # db_path 생략 → jobs/my_job/data/my_job.duckdb
  schema: MY_SCHEMA         # 선택사항

transform:
  # sql_dir 생략 → jobs/my_job/sql/transform
  on_error: stop            # stop | continue
  schema: MY_SCHEMA         # 선택사항 (target.schema 오버라이드)
  # transfer:               # 선택사항: DB→DB 전송
  #   dest:
  #     type: duckdb
  #     db_path: other.duckdb

report:
  source: target            # target | oracle | vertica
  skip_sql: false           # true: DB 연결 없이 CSV만 사용
  csv_union_dir: data/export
  export_csv:
    enabled: true
    # sql_dir / out_dir 생략 → job-centric 기본값
  excel:
    enabled: true
    max_files: 10           # Excel 파일당 최대 시트 수

params:
  clsYymm: "202301:202312"  # 범위, 리스트, 단일값 모두 가능
```

### 경로 결정 우선순위

```
yml 명시 경로  >  job-centric convention  >  글로벌 기본값
```

---

## 12. 환경 설정 (`config/env.yml`)

```yaml
sources:
  oracle:
    thick:
      instant_client: "C:\\oracle\\instantclient_21_3"  # 선택사항
    run:
      hosts: [local]                # 실행 가능 호스트 목록
    export:
      fetch_size: 20000             # fetchmany 배치 크기
      timeout_seconds: 1800         # 쿼리 타임아웃 (초)
    hosts:
      local:
        dsn: 127.0.0.1:1521/ORCLPDB
        user: my_user
        password: "my_password"

  vertica:
    run:
      hosts: [pdwvdbs]
    export:
      fetch_size: 50000
    hosts:
      pdwvdbs:
        host: 10.0.0.200
        port: 5433
        database: MYDB
        user: my_user
        password: "my_password"
```
