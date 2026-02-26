# examples.ps1 - 자주 쓰는 실행 예시
# 원하는 줄 주석 해제 후 실행하거나, run.ps1 로 실행하세요.

# ── 기본 실행 ──────────────────────────────────────────────
# python runner.py --job job_oracle.yml  --mode run --param clsYymm=202406
# python runner.py --job job_duckdb.yml  --mode run --param clsYymm=202406
# python runner.py --job job_sqlite3.yml --mode run --param clsYymm=202406

# ── 범위 실행 ──────────────────────────────────────────────
# python runner.py --job job_oracle.yml --param clsYymm=202401:202406

# ── 분기 실행 (매 분기 마지막 월: 03/06/09/12) ─────────────
# python runner.py --job job_oracle.yml --param clsYymm=202101:202412~Q

# ── Plan 모드 (SQL 검증, DB 연결 없음) ──────────────────────
# python runner.py --job job_oracle.yml --mode plan --param clsYymm=202406

# ── 특정 Stage만 실행 ──────────────────────────────────────
# python runner.py --job job_oracle.yml --stage export --stage load_local --param clsYymm=202406

# ── Override 예시 ───────────────────────────────────────────
# python runner.py --job job_duckdb.yml --param clsYymm=202406 `
#     --set export.overwrite=true `
#     --set export.parallel_workers=4 `
#     --set target.db_path=data/custom.duckdb `
#     --set report.excel.enabled=false

# ── run.ps1 사용 예시 ──────────────────────────────────────
# .\run.ps1 -GUI
# .\run.ps1 -Job job_oracle.yml -Param clsYymm=202406
# .\run.ps1 -Job job_oracle.yml -Param clsYymm=202401:202406
# .\run.ps1 -Job job_oracle.yml -Mode plan -Param clsYymm=202406
# .\run.ps1 -Job job_oracle.yml -Stage export,load_local -Param clsYymm=202406
# .\run.ps1 -Job job_duckdb.yml -Set @("export.parallel_workers=4") -Param clsYymm=202406 -Debug

Write-Host "원하는 줄 주석 해제 후 실행하세요." -ForegroundColor Yellow
