# ELT Runner GUI 사용자 매뉴얼

## 목차

1. [시작하기](#1-시작하기)
2. [화면 구성](#2-화면-구성)
3. [기본 워크플로우](#3-기본-워크플로우)
4. [좌측 패널 — 설정](#4-좌측-패널--설정)
5. [우측 패널 — 실행 로그](#5-우측-패널--실행-로그)
6. [하단 바 — 실행 제어](#6-하단-바--실행-제어)
7. [고급 기능](#7-고급-기능)
8. [단축키](#8-단축키)
9. [디렉토리 구조](#9-디렉토리-구조)
10. [FAQ / 문제 해결](#10-faq--문제-해결)

---

## 1. 시작하기

### 실행 방법

```bash
python batch_runner_gui.py
```

### 필수 준비물

| 항목 | 경로 | 설명 |
|------|------|------|
| 환경 설정 | `config/env.yml` | DB 접속 정보 (env.sample.yml 참고) |
| Job 파일 | `jobs/{name}/{name}.yml` 또는 `jobs/*.yml` | 실행 대상 SQL/설정 정의 |
| SQL 파일 | `jobs/{name}/sql/export/` 등 (Job-Centric) | 각 스테이지에서 실행할 SQL |

> **첫 시작**: Job 선택 → **Dir Setup** 버튼 클릭 → Job-Centric 폴더 구조 자동 생성 + yml 이동

---

## 2. 화면 구성

```
┌─────────────────────────────────────────────────────────┐
│  ELT Runner v1.xx.x                    [테마 선택] [New]│  ← 타이틀 바
├────────────────────────┬────────────────────────────────┤
│                        │  Run Log    [필터] [Time]      │
│  Job:  [▼ 선택]        │  [History] [Log] [Clear]       │
│  [save][save as][dup]  │  ──────────────────────────────│
│  [del][edit][…][📂]    │  ▓▓▓▓░░░░ EXPORT (3/10) 02:30 │
│  Work Dir: [...]       │  ──────────────────────────────│
│  Env Path: [...]       │  Command                       │
│                        │  python runner.py --job ...    │
│  ┌─ Source ──────────┐ │  ──────────────────────────────│
│  │ Type: [oracle  ▼] │ │                                │
│  │ Host: [local   ▼] │ │    (로그 출력 영역)              │
│  │         [Test]    │ │                                │
│  └───────────────────┘ │                                │
│  ┌─ Target ──────────┐ │                                │
│  │ Type: [duckdb  ▼] │ │                                │
│  │ DB Path: [...]    │ │                                │
│  └───────────────────┘ │                                │
│                        │                                │
│  Stages:               │                                │
│  [Export][Load]         │                                │
│  [Transform][Report]   │                                │
│                        │                                │
│  ┌─ Export Settings ─┐ │                                │
│  │  SQL Dir / Out Dir│ │                                │
│  │  Overwrite/Workers│ │                                │
│  └───────────────────┘ │                                │
│  ┌─ Params ──────────┐ │                                │
│  │ key = value       │ │                                │
│  └───────────────────┘ │                                │
├────────────────────────┴────────────────────────────────┤
│ [Dryrun] [▶ Run] [Retry] [Stop] [Queue]  [⏱ Schedule]  │  ← 하단 바
│ [F5] Run · [Ctrl+F5] Dryrun · ...             15:30    │
└─────────────────────────────────────────────────────────┘
```

---

## 3. 기본 워크플로우

### 3.1 가장 간단한 실행

1. **Job 선택** — 좌측 상단 Job 드롭다운에서 `.yml` 파일 선택
2. **Dir Setup** — 상단 바의 Dir Setup 버튼으로 Job-Centric 폴더 구조 자동 생성 (최초 1회)
3. **Stage 확인** — 실행할 스테이지 토글 (Export / Load / Transform / Report)
4. **▶ Run 클릭** (또는 `F5`)
5. 확인 다이얼로그에서 설정 검토 → **OK**

### 3.2 실행 모드

| 모드 | 설명 | 사용 시점 |
|------|------|-----------|
| **Run** | 전체 실행 | 일반적인 ELT 작업 |
| **Dryrun** | 실행 계획만 출력 (DB 변경 없음) | 실행 전 검증 |
| **Retry** | 이전 실행에서 실패한 Task만 재실행 | 부분 실패 복구 |

### 3.3 파이프라인 흐름

```
Export → Load → Transform → Report
(DB→CSV)  (CSV→DuckDB)  (DuckDB SQL)  (SQL→Excel/CSV)
```

각 스테이지는 독립적으로 ON/OFF 할 수 있으며, 필요한 스테이지만 선택 실행이 가능합니다.

---

## 4. 좌측 패널 — 설정

### 4.1 Job 관리

**Job 드롭다운**: `jobs/` 폴더의 `.yml` 파일 목록. Job-Centric 구조(`jobs/{name}/{name}.yml`)와 글로벌 구조(`jobs/*.yml`) 모두 인식합니다.

**Job 관리 버튼:**

| 버튼 | 기능 |
|------|------|
| **save** | 현재 GUI 설정을 Job yml에 저장 (변경된 필드 확인 팝업) |
| **save as** | 새 이름으로 Job yml 저장 |
| **dup** | 현재 Job 복제 (`{name}_copy.yml` 생성) |
| **del** | Job yml 파일 삭제 (확인 다이얼로그) |
| **edit** | OS 기본 편집기에서 yml 파일 열기 |
| **…** | 파일 선택기 — `jobs/` 폴더 외부에서도 yml 가져오기 가능 |
| **📂** | `jobs/` 폴더를 OS 탐색기로 열기 |

> Job을 변경하면 타이틀에 `*` 표시가 나타납니다 (미저장 변경).
> Job-Centric 폴더가 존재하면 저장 시 `jobs/{name}/{name}.yml`에 저장됩니다.

### 4.2 Work Dir / Env Path

- **Work Dir**: 작업 기준 디렉토리 (runner.py 실행 위치). 최근 사용 디렉토리 이력 지원.
- **Env Path**: DB 접속 정보 파일 경로 (기본: `config/env.yml`)
- **↺ Refresh**: 프로젝트 새로고침 (Job 목록, env 호스트 다시 로드)
- 경로가 존재하지 않으면 빨간색 테두리로 경고

### 4.3 Source (Export 데이터 원본)

| 항목 | 설명 |
|------|------|
| **Type** | `oracle` 또는 `vertica` |
| **Host** | env.yml에 정의된 호스트 (`local`, `host2` 등) |
| **Test** | DB 연결 테스트 (SELECT 1 실행) |

### 4.4 Target (Load 대상)

| 항목 | 설명 |
|------|------|
| **Type** | `duckdb` (기본), `sqlite3`, `oracle` |
| **DB Path** | DuckDB/SQLite 파일 경로 |
| **Schema** | DuckDB 스키마명 |

### 4.5 Stage 토글

4개 스테이지를 개별 ON/OFF:

| 스테이지 | 색상 | 역할 |
|----------|------|------|
| **Export** | 파란색 | Source DB → CSV 파일 추출 |
| **Load** | 청록색 | CSV → DuckDB 적재 |
| **Transform** | 보라색 | DuckDB 내부 SQL 변환 |
| **Report** | 주황색 | SQL 결과 → Excel/CSV 출력 |

### 4.6 Export 설정

| 항목 | 설명 | 기본값 |
|------|------|--------|
| **SQL Dir** | Export SQL 파일 경로 | `jobs/{name}/sql/export` |
| **Output Dir** | CSV 출력 경로 | `jobs/{name}/data/export` |
| **SQL 선택** | Select 버튼 → 개별 SQL 파일 선택/해제 | 전체 |
| **Overwrite** | ON: 기존 파일 덮어쓰기 | OFF |
| **Workers** | 병렬 프로세스 수 (1~4). 높을수록 빠르지만 DB 부하 증가 | 1 |
| **Compression** | 출력 압축 (`gzip` / `none`) | gzip |
| **Timeout** | SQL 실행 제한 시간 (초) | 1800 |

### 4.7 Load 설정

| 항목 | 설명 | 기본값 |
|------|------|--------|
| **Load Mode** | `replace` / `truncate` / `append` (+ Oracle Target: `delete`) | replace |
| **CSV Dir** | Load 대상 CSV 경로 (비워두면 Export Output 사용) | (빈 값) |

**Load Mode 상세:**
- `replace`: DROP 후 새로 CREATE
- `truncate`: TRUNCATE 후 INSERT
- `delete`: WHERE 조건으로 DELETE 후 INSERT (Oracle Target 전용)
- `append`: 기존 데이터 유지, INSERT만

> Target Type이 Oracle이면 `delete` 모드가 추가 표시되며 기본값도 `delete`로 변경됩니다.

### 4.8 Transform 설정

| 항목 | 설명 |
|------|------|
| **SQL Dir** | SQL 파일 경로 (`jobs/{name}/sql/transform`) |
| **Schema** | 실행 시 세션 스키마 (DuckDB: `SET schema` / Oracle: `ALTER SESSION`) |
| **On Error** | `stop` (즉시 중단) / `continue` (나머지 계속) |
| **SQL 선택** | Select 버튼 → 개별 SQL 선택 |

#### Transfer (DB→DB 전송)

Transform 섹션 내에 **Transfer** 옵션이 있습니다.

| 항목 | 설명 |
|------|------|
| **Transfer 체크박스** | DB→DB 전송 모드 활성화 |
| **Source** | 현재 Target DB (자동 표시) |
| **Dest Type** | 전송 대상 DB 타입 (`duckdb`, `sqlite3`) |
| **Dest DB Path** | 전송 대상 DB 파일 경로 |

**동작 원리:**
- Source DB에 Dest DB를 ATTACH하여 SQL에서 `dest.schema.table`로 참조 가능
- DuckDB↔DuckDB, SQLite↔SQLite 조합만 지원 (ATTACH 메커니즘)
- 예: DuckDB Transform SQL에서 `INSERT INTO dest.main.summary SELECT ...` 형태로 사용

### 4.9 Report 설정

| 항목 | 설명 |
|------|------|
| **SQL Dir** | SQL 파일 경로 (`jobs/{name}/sql/report`) |
| **Output Dir** | 리포트 출력 경로 (`jobs/{name}/data/report`) |
| **Excel / CSV** | 출력 형식 선택 (중복 선택 가능) |
| **Max Files** | Excel 파일당 최대 시트 수 (1~100). 초과 시 새 파일 생성 |
| **Skip SQL** | ON: SQL 실행 건너뛰고 CSV → Excel 변환만 |
| **Union Dir** | skip_sql 시 CSV 원본 폴더 (입력) |
| **On Error** | `stop` / `continue` |
| **Name Style** | `full` (key_value) / `compact` (value만) |
| **Strip Prefix** | ON: 숫자 접두어 제거 (01_contract → contract) |

### 4.10 파라미터 (Params)

각 스테이지별로 Key-Value 파라미터를 설정합니다.

- **+ 버튼**: 파라미터 행 추가
- **- 버튼**: 마지막 행 제거

**파라미터 값 문법:**
| 표현 | 의미 | 예시 |
|------|------|------|
| `단일값` | 1개 값 | `202403` |
| `값1\|값2\|값3` | 여러 값 (파이프 구분) | `202401\|202402\|202403` |
| `시작:끝` | 범위 자동 확장 | `202401:202412` → 12개월 |

**조합 모드 (Param Mode):**
- `product` (기본): 카르테시안 곱 — 모든 조합 생성
  - 예: `a=1|2`, `b=x|y` → 4 조합 (1-x, 1-y, 2-x, 2-y)
- `zip`: 위치별 1:1 매칭
  - 예: `a=1|2`, `b=x|y` → 2 조합 (1-x, 2-y)

> 파라미터 입력 시 `→ N combinations` 텍스트가 나타나며, 마우스를 올리면 확장된 값 미리보기가 표시됩니다.

---

## 5. 우측 패널 — 실행 로그

### 5.1 로그 필터

| 필터 | 표시 내용 |
|------|-----------|
| **ALL** | 전체 로그 |
| **SUM** | Job 정보 + Stage 헤더 + 요약 + 경고/에러 |
| **WARN+** | 경고 + 에러만 |
| **ERR** | 에러만 |

### 5.2 로그 헤더 버튼

| 버튼 | 기능 |
|------|------|
| **Time** | 타임스탬프 표시 ON/OFF |
| **History** | 과거 실행 이력 다이얼로그 |
| **Log** | `logs/` 폴더를 OS 탐색기로 열기 |
| **Clear** | 로그 화면 클리어 |
| **--debug** | 디버그 모드 토글 |

### 5.3 로그 관리

- **자동 클리어**: Job 실행 시 이전 로그 자동 클리어 (GUI 성능 보호)
- **GUI 라인 제한**: 20,000줄 초과 시 오래된 줄 자동 제거
- **파일 로그 정리**: 30일 초과된 `logs/*.log` 파일 자동 삭제

### 5.4 로그 우클릭 메뉴

| 메뉴 | 기능 |
|------|------|
| Copy | 선택 영역 복사 |
| Select All | 전체 선택 |
| Copy Errors | 에러 줄만 추출하여 복사 |
| Save Log... | 로그를 `.txt` 파일로 저장 |
| Clear | 로그 클리어 |

### 5.5 로그 검색 (Ctrl+F)

- `Ctrl+F` → 검색 바 표시
- 검색어 입력 → 매치 하이라이트
- `Enter` / `Shift+Enter` → 다음/이전 매치 이동
- `Esc` → 검색 바 닫기

### 5.6 프로그레스 표시

실행 중 다음 정보가 실시간 표시됩니다:

- **프로그레스바**: 전체 파이프라인 진행률
- **경과 시간**: `MM:SS` 형식
- **스테이지 세그먼트**: 각 스테이지별 진행 카운트
  - 실행 중: `Export 3/10`
  - 완료: `Export ✓ 10` (성공) 또는 `Export ✗ 2err` (실패)

### 5.7 Command Preview

하단에 실제 실행될 CLI 명령어가 실시간으로 표시됩니다. 설정을 변경하면 즉시 반영됩니다.

---

## 6. 하단 바 — 실행 제어

### 6.1 실행 버튼

| 버튼 | 단축키 | 기능 |
|------|--------|------|
| **Dryrun** | `Ctrl+F5` | 실행 계획만 출력 (DB 변경 없음) |
| **▶ Run** | `F5` | 실제 실행 |
| **Retry** | — | 이전 실패 Task만 재실행 |
| **Stop** | `Esc` | 실행 중지 |
| **Queue** | — | 다중 Job 큐 (순차 실행) |

### 6.2 예약 실행 (Schedule)

입력란에 시각을 입력하고 **⏱ Schedule** 버튼 클릭:

| 입력 형식 | 의미 | 예시 |
|-----------|------|------|
| `+Nm` | N분 후 | `+30m` |
| `+Nh` | N시간 후 | `+2h` |
| `HH:MM` | 지정 시각 (오늘/내일) | `18:00` |
| `MMDD HH:MM` | 특정 날짜 시각 | `0302 18:00` |

- 예약 중에는 카운트다운 표시
- **✕ Cancel** 클릭으로 예약 취소

### 6.3 다중 Job 큐

여러 Job을 순서대로 자동 실행하는 기능입니다.

1. **Queue** 버튼 클릭
2. Available 목록에서 실행할 Job을 `>>` 로 Queue에 추가
3. `Up` / `Dn` 으로 실행 순서 조정
4. **Run Queue** 클릭

**동작 규칙:**
- 각 Job이 성공하면 2초 후 다음 Job 자동 시작
- 중간에 실패하면 큐가 멈춤 (데이터 정합성 보호)
- Stop 버튼으로 수동 중지 시 큐 전체 중단

---

## 7. 고급 기능

### 7.1 Dir Setup (Job-Centric 구조 자동 생성)

상단 바의 **Dir Setup** 버튼으로 선택된 Job에 대해 Job-Centric 폴더 구조를 자동 생성합니다.

**Dir Setup이 수행하는 작업:**

1. **폴더 생성**: `jobs/{name}/sql/export`, `jobs/{name}/data/export` 등 전체 Job-Centric 디렉토리 트리 생성
2. **yml 이동**: `jobs/{name}.yml` → `jobs/{name}/{name}.yml` 자동 이동 (글로벌 → Job-Centric)
3. **경로 정리**: yml 내 하드코딩된 글로벌 경로(`sql_dir`, `out_dir`, `db_path`) 제거 → Job-Centric defaults에 위임
4. **GUI 업데이트**: 모든 경로 필드를 Job-Centric 경로로 자동 업데이트
5. **env.sample.yml**: `config/env.sample.yml` 자동 생성 (최초 1회)

**생성되는 디렉토리 구조:**
```
work_dir/
├── config/
├── jobs/
│   └── {job_name}/
│       ├── {job_name}.yml   ← Job 설정 파일
│       ├── sql/
│       │   ├── export/      ← Export SQL
│       │   ├── transform/   ← Transform SQL
│       │   └── report/      ← Report SQL
│       └── data/
│           ├── export/      ← Export CSV 출력
│           ├── {job_name}.duckdb
│           ├── transform/   ← Transform 트래킹
│           ├── report/      ← Report 결과물
│           └── report_tracking/
└── logs/
```

> Dir Setup 실행 전에 확인 다이얼로그가 표시되며, 새로 생성할 폴더와 이미 존재하는 폴더, yml 이동 여부가 미리 보여집니다.

### 7.2 실행 이력 (History)

로그 헤더의 **History** 버튼으로 과거 실행 기록을 조회합니다.

| 컬럼 | 내용 |
|------|------|
| Run ID | 실행 고유 ID |
| Stage | 실행 스테이지 (export/transform/report) |
| Start Time | 시작 시각 |
| Mode | 실행 모드 (run/plan/retry) |
| OK | 성공 Task 수 |
| Fail | 실패 Task 수 |
| Skip | 건너뜀 Task 수 |
| Elapsed | 소요 시간 |

- 행 클릭 시 실패한 Task 상세 정보 표시 (에러 메시지 포함)

### 7.3 Connection Test

Source 설정 옆의 **Test** 버튼으로 DB 접속을 테스트합니다.

- Oracle: `SELECT 1 FROM DUAL` 실행 (`oracledb` 패키지 필요)
- Vertica: `SELECT 1` 실행 (`vertica_python` 패키지 필요)
- 결과가 로그에 `[ConnTest] OK/FAIL` 형태로 표시
- Oracle Thick Mode (Instant Client) 지원

### 7.4 Pre-flight 검증

Run 확인 다이얼로그에서 자동으로 검증:

- **파라미터 누락 경고**: SQL 파일에서 `${param_name}` 패턴을 스캔하여 GUI에 미입력된 파라미터를 빨간색 경고로 표시
- **Retry 정보**: Retry 모드 시 이전 실패 Task 수 표시 (예: `Export: 3/10 retry`)

### 7.5 테마 변경

타이틀 바 우측의 테마 드롭다운에서 11종 테마 선택:

**다크 테마 (5종):**
Mocha · Nord · Dracula · Tokyo Night · One Dark

**라이트 테마 (6종):**
Latte · White · Paper · Solarized Light · Gruvbox Light · Rose Pine Dawn

- 테마는 앱 종료 시 자동 저장되며 다음 실행 시 복원됩니다
- `ELT_GUI_THEME` 환경변수로 시작 테마 지정 가능

### 7.6 SQL 선택 필터

Export/Transform/Report 각각의 **Select** 버튼을 클릭하면 SQL 파일 선택 다이얼로그가 열립니다.

- **검색**: 상단 검색란에 파일명 입력 → 실시간 필터링
- **전체 선택/해제**: All / None 버튼
- **개별 체크박스**: 실행할 SQL 파일만 선택

### 7.7 새 창 열기

타이틀 바의 **New** 버튼으로 새 GUI 인스턴스를 별도 프로세스로 실행합니다. 다른 테마가 랜덤 적용되어 여러 창을 시각적으로 구분할 수 있습니다.

---

## 8. 단축키

| 단축키 | 기능 |
|--------|------|
| `F5` | Run (실행) |
| `Ctrl+F5` | Dryrun |
| `Esc` | 검색 바 닫기 / 실행 중지 |
| `Ctrl+S` | Job 설정 저장 |
| `Ctrl+R` | 프로젝트 새로고침 |
| `Ctrl+F` | 로그 검색 |
| `Ctrl+L` | 로그 파일 저장 |

---

## 9. 디렉토리 구조

### 9.1 Job-Centric 구조 (권장)

**Dir Setup** 버튼으로 자동 생성되는 표준 구조입니다. 각 Job이 독립된 폴더를 가지며, SQL·데이터·DB가 모두 Job 단위로 격리됩니다.

```
work_dir/
├── config/
│   └── env.yml                  ← DB 접속 정보 (Source/Target)
├── jobs/
│   ├── _default.yml             ← 기본 Job 설정
│   ├── job_a/
│   │   ├── job_a.yml            ← Job 설정 파일
│   │   ├── sql/
│   │   │   ├── export/          ← Export SQL
│   │   │   ├── transform/       ← Transform SQL
│   │   │   └── report/          ← Report SQL
│   │   └── data/
│   │       ├── export/          ← Export CSV 출력
│   │       ├── job_a.duckdb     ← DuckDB 파일
│   │       ├── transform/       ← Transform 트래킹
│   │       ├── report/          ← Report 결과물
│   │       └── report_tracking/ ← Report 트래킹
│   └── job_b/
│       ├── job_b.yml
│       └── ...
└── logs/                        ← 실행 로그 파일
```

> `jobs/{name}/` 폴더가 존재하면 자동으로 Job-Centric 경로가 적용됩니다.
> yml에 경로를 명시하지 않으면 convention 기반 기본값이 사용됩니다.

### 9.2 글로벌 구조 (레거시)

Job-Centric 폴더가 없는 경우의 기본 구조입니다. 모든 Job이 공통 폴더를 공유합니다.

```
work_dir/
├── config/
│   └── env.yml
├── jobs/
│   └── my_job.yml
├── sql/
│   ├── export/
│   ├── transform/
│   └── report/
├── data/
│   ├── export/
│   ├── local/
│   │   └── result.duckdb
│   ├── transform/
│   ├── report/
│   └── report_tracking/
└── logs/
```

### 9.3 데이터 흐름

```
                    jobs/{name}/sql/export/*.sql
                        │
Source DB ─── Export ──→ jobs/{name}/data/export/*.csv
(oracle)                    │
                            │
                        ┌── Load ──→ jobs/{name}/data/{name}.duckdb
                        │                 │
                        │   jobs/{name}/sql/transform/*.sql
                        │                 │
                        │         Transform (in-place)
                        │                 │
                        │   jobs/{name}/sql/report/*.sql
                        │                 │
                        └── Report ──→ jobs/{name}/data/report/*.xlsx, *.csv
```

- **Export**: Source DB에서 SQL 실행 → `data/export/`에 CSV 저장
- **Load**: CSV를 DuckDB에 적재
- **Transform**: DuckDB 내부에서 SQL 실행 (in-place 변환). Transfer 활성화 시 다른 DB에 ATTACH하여 DB→DB 전송 가능
- **Report**: DuckDB에서 SQL 실행 → CSV·Excel 출력

> **report_out 경로**: Report 섹션의 출력 폴더는 CSV와 Excel 결과물이 모두 저장되는 공통 출력 폴더입니다.
> **csv_source (union_dir)**: skip_sql 모드에서 기존 CSV를 Excel로 변환할 때 원본 CSV를 읽을 입력 폴더입니다.

---

## 10. FAQ / 문제 해결

### Q. Job 드롭다운이 비어 있어요
`jobs/` 폴더에 `.yml` 파일이 없습니다. `jobs/*.yml` 또는 `jobs/{name}/{name}.yml` 형태가 필요합니다. GUI에서 설정 후 `Ctrl+S`로 저장하세요.

### Q. Dir Setup은 언제 사용하나요?
새 Job을 만들거나, 기존 글로벌 구조를 Job-Centric으로 전환할 때 사용합니다. Job 선택 후 Dir Setup 클릭하면 폴더 생성 + yml 이동 + 경로 정리가 한번에 수행됩니다.

### Q. Source Host가 비어 있어요
`config/env.yml` 파일에 해당 Source Type의 hosts 설정이 필요합니다. `env.sample.yml`을 참고하세요.

### Q. Export 중 Timeout 에러가 발생해요
Export Settings에서 **Timeout** 값을 늘리세요 (기본 1800초 = 30분). 큰 테이블은 3600(1시간) 이상 권장합니다.

### Q. Retry 모드가 전체 실행되어요
이전 실행 기록(`data/{stage}/runs/{job}/*/run_info.json`)이 없으면 전체 실행됩니다. 첫 실행 후부터 Retry가 동작합니다.

### Q. 로그가 너무 길어서 GUI가 느려요
- GUI 로그는 Job 실행 시 자동 클리어됩니다
- 20,000줄 초과 시 자동 트림
- 상세 로그는 **Log** 버튼으로 `logs/` 폴더에서 텍스트 파일로 확인

### Q. 파라미터 범위(`:`)가 동작하지 않아요
범위 확장은 Export 스테이지의 `expand_range_value` 함수에 의존합니다. 지원되는 형식:
- `YYYYMM:YYYYMM` — 월 단위 범위
- `값1|값2|값3` — 파이프 구분은 항상 동작

### Q. Transfer가 활성화되지 않아요
Transform 섹션에서 Transfer 체크박스를 ON으로 설정하세요. DuckDB↔DuckDB 또는 SQLite↔SQLite 조합만 지원됩니다.

### Q. 실행 후 알림이 안 와요
OS 알림은 플랫폼별로 다릅니다:
- Windows: PowerShell Toast 알림
- macOS: `osascript` 알림
- Linux: `notify-send` 필요 (`sudo apt install libnotify-bin`)

### Q. Connection Test에 필요한 패키지는?
- Oracle: `pip install oracledb`
- Vertica: `pip install vertica-python`
