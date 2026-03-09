# Job 폴더 구조 가이드

## 개요

ELT Runner는 두 가지 폴더 구조를 지원합니다:

1. **글로벌 구조** (기존) — SQL, 데이터, DB 파일이 기능별 폴더에 분산
2. **Job-centric 구조** (v1.98+) — 한 job의 모든 파일이 `jobs/{job_name}/` 아래에 집중

두 구조는 **동시에 공존** 가능하며, yml에 경로가 명시되어 있으면 항상 그 경로를 우선합니다.

---

## 1. 글로벌 구조 (기존 방식)

```
project/
├── jobs/
│   ├── job_duckdb.yml          ← job 설정
│   └── test_insurance.yml
├── sql/
│   ├── export/                 ← export SQL
│   │   ├── test/
│   │   └── A/
│   ├── transform/
│   │   ├── duckdb/             ← DB 엔진별 분리
│   │   ├── oracle/
│   │   └── sqlite3/
│   └── report/
├── data/
│   ├── export/{job_name}/      ← export CSV 출력
│   ├── local/result.duckdb     ← DB 파일
│   └── report/                 ← 리포트 출력
└── config/
    └── env.yml
```

**특징**: job 하나를 작업하려면 여러 폴더를 돌아다녀야 함.

---

## 2. Job-centric 구조 (권장)

```
project/
├── jobs/
│   ├── my_job.yml              ← job 설정 (yml은 기존 위치 유지)
│   └── my_job/                 ← yml과 동명 폴더 ← 핵심!
│       ├── sql/
│       │   ├── export/         ← export SQL
│       │   ├── transform/      ← transform SQL (DB 엔진 구분 불필요)
│       │   └── report/         ← report SQL
│       └── data/
│           ├── export/my_job/  ← CSV 출력 (stage가 자동 생성)
│           ├── report/         ← 리포트 출력
│           └── my_job.duckdb   ← DB 파일
├── config/
│   └── env.yml
└── sql/                        ← 공유 SQL (여러 job이 참조 가능)
    ├── ddl/
    └── setup/
```

**특징**: `jobs/my_job/` 한 폴더만 보면 해당 job의 모든 것을 확인 가능.

---

## 3. Convention 규칙

`jobs/{job_name}/` **폴더가 존재하면** 자동으로 job-centric 기본값이 적용됩니다:

| yml 필드 | Job-centric 기본값 | 글로벌 기본값 (폴더 없을 때) |
|---|---|---|
| `export.sql_dir` | `jobs/{name}/sql/export` | `sql/export` |
| `export.out_dir` | `jobs/{name}/data/export` | `data/export` |
| `transform.sql_dir` | `jobs/{name}/sql/transform` | `sql/transform/{db_type}` |
| `report.export_csv.sql_dir` | `jobs/{name}/sql/report` | `sql/report` |
| `report.export_csv.out_dir` | `jobs/{name}/data/report` | `data/report` |
| `target.db_path` | `jobs/{name}/data/{name}.duckdb` | `data/local/result.duckdb` |

### 우선순위

```
yml에 명시된 경로  >  job 폴더 convention  >  글로벌 기본값
```

- yml에 `sql_dir: sql/export/custom` 이라고 적으면 → 그 경로 사용 (convention 무시)
- yml에 `sql_dir`가 없고 `jobs/my_job/` 폴더가 있으면 → `jobs/my_job/sql/export`
- yml에 `sql_dir`가 없고 `jobs/my_job/` 폴더도 없으면 → `sql/export` (기존 동작)

---

## 4. 새 Job-centric job 만들기

### 방법 1: 폴더 먼저 생성

```bash
# 1. job 폴더 구조 생성
mkdir -p jobs/my_project/sql/{export,transform,report}
mkdir -p jobs/my_project/data

# 2. SQL 파일 배치
cp my_queries/*.sql jobs/my_project/sql/export/

# 3. yml 생성 (경로 생략 = convention 자동 적용)
cat > jobs/my_project.yml << 'EOF'
job_name: my_project
pipeline:
  stages: [export, load_local, transform, report]
source:
  type: oracle
  host: prod
target:
  type: duckdb
EOF
```

경로를 전혀 안 적어도 `jobs/my_project/` 폴더가 있으면 자동으로 찾습니다.

### 방법 2: GUI에서 자동 완성

1. GUI에서 새 job 생성
2. `jobs/{job_name}/` 폴더가 존재하면 경로 필드가 자동으로 채워짐
3. 필요 시 수동 override 가능

---

## 5. 기존 Job 마이그레이션

기존 글로벌 구조 → job-centric으로 전환:

```bash
# 예: test_insurance job 마이그레이션

# 1. job 폴더 생성
mkdir -p jobs/test_insurance/sql/{export,transform,report}
mkdir -p jobs/test_insurance/data

# 2. SQL 이동
mv sql/export/test/*         jobs/test_insurance/sql/export/
mv sql/transform/duckdb/test/* jobs/test_insurance/sql/transform/
mv sql/report/test/*          jobs/test_insurance/sql/report/

# 3. yml에서 경로 제거 (convention이 대신 처리)
# 변경 전:
#   export:
#     sql_dir: sql/export/test
#     out_dir: data/export/test
# 변경 후:
#   export: {}   ← 또는 경로 필드 자체를 삭제
```

**주의**: yml에 경로가 남아있으면 convention보다 yml이 우선합니다. 마이그레이션 시 기존 경로를 제거해야 convention이 동작합니다.

---

## 6. Export → Load 경로 연결

Export와 Load는 동일한 `export.out_dir` 기본값을 공유합니다:

```
Export 출력: {export.out_dir}/{job_name}/  ← CSV 저장
Load 입력:   {export.out_dir}/{job_name}/  ← CSV 읽기
```

- Job-centric: `jobs/my_job/data/export/my_job/`
- 글로벌: `data/export/my_job/`

**두 stage가 같은 기본값을 사용하므로**, export에서 만든 CSV를 load에서 자동으로 찾습니다.
별도 `load.csv_dir` 설정은 불필요합니다 (export→load 파이프라인 시).

---

## 7. SQL 공유

여러 job이 같은 SQL을 재사용해야 하면, yml에 경로를 명시합니다:

```yaml
# jobs/job_a.yml — job-centric 폴더의 SQL 대신 공유 SQL 사용
export:
  sql_dir: sql/shared/common_exports
```

공유 SQL은 `sql/` 글로벌 폴더에 두는 것을 권장합니다.

---

## 8. .gitignore 권장 설정

```gitignore
# data 출력물 (job-centric 포함)
jobs/*/data/
data/

# DB 파일
*.duckdb
*.duckdb.wal
*.sqlite

# 단, SQL은 반드시 추적
!jobs/*/sql/
```
