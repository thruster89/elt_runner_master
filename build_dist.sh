#!/usr/bin/env bash
# ============================================================
# build_dist.sh — elt_runner_dist 공개 레포 빌드 스크립트
# 사용법: bash build_dist.sh [출력 디렉토리]
# ============================================================
set -euo pipefail

DIST_DIR="${1:-../elt_runner_dist}"

echo "=== elt_runner_dist 빌드 ==="
echo "출력 경로: $DIST_DIR"

# ── 초기화 ─────────────────────────────────────────────────
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# ── 포함할 파일/디렉토리 ──────────────────────────────────
# 진입점
cp runner.py "$DIST_DIR/"
cp batch_runner_gui.py "$DIST_DIR/"

# 엔진
cp -r engine "$DIST_DIR/"

# 스테이지
cp -r stages "$DIST_DIR/"

# 어댑터
cp -r adapters "$DIST_DIR/"

# GUI
cp -r gui "$DIST_DIR/"

# 폰트
cp -r fonts "$DIST_DIR/"

# 설정
mkdir -p "$DIST_DIR/config"
cp config/env.sample.yml "$DIST_DIR/config/"

# 문서
mkdir -p "$DIST_DIR/docs"
cp docs/USER_GUIDE.md "$DIST_DIR/docs/"

# Job: _default만
mkdir -p "$DIST_DIR/jobs"
cp jobs/_default.yml "$DIST_DIR/jobs/"

# SQL: 루트 파일만 복사 (하위 운영/테스트 폴더 제외)
mkdir -p "$DIST_DIR/sql/export"
cp sql/export/01_contract.sql "$DIST_DIR/sql/export/"
cp sql/export/02_payment.sql  "$DIST_DIR/sql/export/"

mkdir -p "$DIST_DIR/sql/postwork"
cp sql/postwork/01_summary.sql "$DIST_DIR/sql/postwork/"

mkdir -p "$DIST_DIR/sql/report"
cp sql/report/01_monthly_summary.sql   "$DIST_DIR/sql/report/"
cp sql/report/02_pay_type_breakdown.sql "$DIST_DIR/sql/report/"

# Transform (DB별 기본 SQL만)
mkdir -p "$DIST_DIR/sql/transform/duckdb"
mkdir -p "$DIST_DIR/sql/transform/sqlite3"
mkdir -p "$DIST_DIR/sql/transform/oracle"
cp sql/transform/duckdb/01_summary.sql  "$DIST_DIR/sql/transform/duckdb/"
cp sql/transform/sqlite3/01_summary.sql "$DIST_DIR/sql/transform/sqlite3/"
cp sql/transform/oracle/01_summary.sql  "$DIST_DIR/sql/transform/oracle/"

# ── 샘플 Job + SQL 세트 생성 ─────────────────────────────
# job_sample_sqlite3.yml + 전용 sql/sample/ 폴더 구조
cat > "$DIST_DIR/jobs/job_sample_sqlite3.yml" << 'SAMPLEJOB'
# ============================================================
# job_sample_sqlite3.yml  — 샘플 Job (Oracle → SQLite3)
# 이 파일과 sql/sample/ 폴더가 하나의 세트입니다.
# ============================================================

job_name: sample_sqlite3

params:
  clsYymm: "202401"

pipeline:
  stages:
    - export
    - load_local
    - transform
    - report

# ── 소스 DB ──────────────────────────────────────────────
source:
  type: oracle
  host: local

# ── Export ───────────────────────────────────────────────
export:
  sql_dir: sql/sample/export          # ← 샘플 전용 SQL
  out_dir: data/export/sample
  format: csv
  compression: gzip
  overwrite: true
  parallel_workers: 1

# ── Target ───────────────────────────────────────────────
target:
  type: sqlite3
  db_path: data/local/sample.sqlite

# ── Load ─────────────────────────────────────────────────
load:
  mode: replace

# ── Transform ────────────────────────────────────────────
transform:
  sql_dir: sql/sample/transform       # ← 샘플 전용 transform
  on_error: continue

# ── Report ───────────────────────────────────────────────
report:
  source: target
  export_csv:
    enabled: true
    sql_dir: sql/sample/report        # ← 샘플 전용 report
    out_dir: data/report/sample
    compression: none
  excel:
    enabled: true
    out_dir: data/report/sample
    max_files: 10
SAMPLEJOB

# 샘플 SQL 폴더 구조 생성
mkdir -p "$DIST_DIR/sql/sample/export"
mkdir -p "$DIST_DIR/sql/sample/transform"
mkdir -p "$DIST_DIR/sql/sample/report"

cat > "$DIST_DIR/sql/sample/export/01_contract.sql" << 'SQL'
--[TB_CONTRACT]
-- 계약 기본 정보 Export (샘플)
SELECT
    CONTRACT_ID,
    CLS_YYMM,
    PRODUCT_CD,
    CUSTOMER_ID,
    CONTRACT_AMT,
    STATUS,
    TO_CHAR(REG_DTM, 'YYYY-MM-DD HH24:MI:SS') AS REG_DTM
FROM TB_CONTRACT
WHERE CLS_YYMM = :clsYymm
ORDER BY CONTRACT_ID
SQL

cat > "$DIST_DIR/sql/sample/export/02_payment.sql" << 'SQL'
--[TB_PAYMENT]
-- 납입 내역 Export (샘플)
SELECT
    PAYMENT_ID,
    CONTRACT_ID,
    CLS_YYMM,
    PAY_AMT,
    TO_CHAR(PAY_DT, 'YYYY-MM-DD') AS PAY_DT,
    PAY_TYPE
FROM TB_PAYMENT
WHERE CLS_YYMM = :clsYymm
ORDER BY PAYMENT_ID
SQL

cat > "$DIST_DIR/sql/sample/transform/01_summary.sql" << 'SQL'
-- ============================================================
-- TRANSFORM: 계약·납입 집계 테이블 생성 (SQLite3 호환)
-- ============================================================

DROP TABLE IF EXISTS TB_CONTRACT_SUMMARY;

CREATE TABLE TB_CONTRACT_SUMMARY AS
SELECT
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS,
    COUNT(c.CONTRACT_ID)                    AS CONTRACT_CNT,
    SUM(c.CONTRACT_AMT)                     AS CONTRACT_AMT_SUM,
    ROUND(AVG(c.CONTRACT_AMT), 0)           AS CONTRACT_AMT_AVG,
    COALESCE(SUM(p.PAY_AMT), 0)            AS PAY_AMT_SUM,
    COUNT(p.PAYMENT_ID)                     AS PAY_CNT
FROM TB_CONTRACT c
LEFT JOIN TB_PAYMENT p
    ON c.CONTRACT_ID = p.CONTRACT_ID
   AND c.CLS_YYMM    = p.CLS_YYMM
GROUP BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
ORDER BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
SQL

cat > "$DIST_DIR/sql/sample/report/01_monthly_summary.sql" << 'SQL'
-- 월별 계약 집계 리포트 (샘플)
SELECT
    CLS_YYMM,
    PRODUCT_CD,
    STATUS,
    CONTRACT_CNT,
    CONTRACT_AMT_SUM,
    CONTRACT_AMT_AVG,
    PAY_AMT_SUM,
    PAY_CNT
FROM TB_CONTRACT_SUMMARY
ORDER BY CLS_YYMM, PRODUCT_CD, STATUS
SQL

# 빌드/실행
cp build.bat "$DIST_DIR/"
cp build.ps1 "$DIST_DIR/"
cp run.ps1 "$DIST_DIR/"
cp run_gui.bat "$DIST_DIR/"
cp elt_runner.spec "$DIST_DIR/"
cp rthook_workdir.py "$DIST_DIR/"

# 기타
cp requirements.txt "$DIST_DIR/"
cp VERSION "$DIST_DIR/"
cp CHANGELOG.md "$DIST_DIR/"

# ── __pycache__ 정리 ──────────────────────────────────────
find "$DIST_DIR" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── .gitignore 생성 ───────────────────────────────────────
cat > "$DIST_DIR/.gitignore" << 'GITIGNORE'
# Python
__pycache__/
*.pyc
*.pyo

# Runtime output
logs/
data/

# Data files
*.csv
*.gz
*.duckdb
*.sqlite
*.log

# Run info
run_info.json
plan_report.json

# Sensitive config
config/env.yml

# IDE / OS
*.swp
Thumbs.db
.DS_Store
GITIGNORE

# ── README 생성 ───────────────────────────────────────────
VERSION=$(cat "$DIST_DIR/VERSION" 2>/dev/null || echo "unknown")
cat > "$DIST_DIR/README.md" << EOF
# ELT Runner v${VERSION}

Oracle/Vertica 등 소스 DB에서 데이터를 추출(Export)하여
DuckDB/SQLite 등 로컬 DB로 적재(Load)하고,
변환(Transform) 및 리포트(Report)까지 수행하는 ELT 파이프라인 도구입니다.

## 빠른 시작

\`\`\`bash
pip install -r requirements.txt
cp config/env.sample.yml config/env.yml   # DB 접속 정보 설정
python runner.py --job jobs/job_sample_sqlite3.yml
\`\`\`

## GUI 실행

\`\`\`bash
python batch_runner_gui.py
\`\`\`

## 샘플 구조

\`\`\`
jobs/job_sample_sqlite3.yml     ← 샘플 Job 설정
sql/sample/
├── export/                     ← Export SQL (소스 → CSV)
│   ├── 01_contract.sql
│   └── 02_payment.sql
├── transform/                  ← Transform SQL (타겟 DB 내 가공)
│   └── 01_summary.sql
└── report/                     ← Report SQL (결과 리포트)
    └── 01_monthly_summary.sql
\`\`\`

자세한 사용법은 [docs/USER_GUIDE.md](docs/USER_GUIDE.md)를 참고하세요.
EOF

# ── 결과 출력 ─────────────────────────────────────────────
echo ""
echo "=== 빌드 완료 ==="
echo "포함된 파일:"
find "$DIST_DIR" -type f | sort | sed "s|$DIST_DIR/||"
echo ""
echo "다음 단계:"
echo "  cd $DIST_DIR"
echo "  git init && git add -A && git commit -m 'Initial release v${VERSION}'"
echo "  gh repo create elt_runner_dist --public --source=. --push"
