# file: stages/transform_stage.py
"""
TRANSFORM stage
load된 target DB에서 SQL을 실행하는 가공/집계 단계.

job.yml 설정:
  transform:
    sql_dir: sql/transform/duckdb    # target DB에서 실행할 SQL 디렉토리
    on_error: stop                   # stop(기본) / continue
"""

import re
import time

from engine.connection import connect_target
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, render_sql


def run(ctx: RunContext):
    logger = ctx.logger
    transform_cfg = ctx.job_config.get("transform")
    if not transform_cfg:
        logger.info("TRANSFORM stage skipped (no config)")
        return

    if ctx.mode == "plan":
        logger.info("TRANSFORM stage skipped (plan mode)")
        return

    sql_dir_str = transform_cfg.get("sql_dir")
    on_error    = (transform_cfg.get("on_error") or "stop").strip().lower()

    if not sql_dir_str:
        logger.info("TRANSFORM stage skipped (no sql_dir configured)")
        return

    sql_dir = resolve_path(ctx, sql_dir_str)
    if not sql_dir.exists():
        logger.warning("TRANSFORM sql_dir not found: %s", sql_dir)
        return

    sql_files = sort_sql_files(sql_dir)
    if not sql_files:
        logger.info("TRANSFORM no SQL files in %s", sql_dir)
        return

    # ── --include-transform 필터 적용 ──────────────────────
    include_patterns = getattr(ctx, "include_transform_patterns", []) or []
    if include_patterns:
        def _matches(f):
            rel  = f.relative_to(sql_dir).as_posix().lower()
            stem = f.stem.lower()
            return any(
                pat.lower() in rel or pat.lower() in stem
                for pat in include_patterns
            )
        before = len(sql_files)
        sql_files = [f for f in sql_files if _matches(f)]
        logger.info(
            "TRANSFORM --include filter applied: %d -> %d files (patterns: %s)",
            before, len(sql_files), include_patterns
        )
        if not sql_files:
            logger.warning("TRANSFORM --include filter resulted in no SQL files (patterns=%s)", include_patterns)
            return

    target_cfg = ctx.job_config.get("target", {})
    tgt_type   = (target_cfg.get("type") or "").strip().lower()

    if not tgt_type:
        logger.warning("TRANSFORM stage skipped (no target config)")
        return

    conn, conn_type, label = connect_target(ctx, target_cfg)

    # schema 결정: transform.schema 우선, 없으면 target.schema fallback
    schema = (transform_cfg.get("schema") or "").strip() \
             or (target_cfg.get("schema") or "").strip() \
             or ""

    # 세션 기본 스키마 지정 (schema 입력 = 해당 스키마에서 SQL 실행)
    if schema:
        if conn_type == "duckdb":
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            conn.execute(f"SET schema = '{schema}'")
        elif conn_type == "oracle":
            cur = conn.cursor()
            cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
            cur.close()
        logger.info("TRANSFORM session schema = '%s'", schema)

    # @{param} 스키마 접두사에 사용된 스키마 자동 생성 (DuckDB)
    if conn_type == "duckdb" and ctx.params:
        _ensure_param_schemas(conn, sql_files, ctx.params, logger)

    schema_display = schema if schema else "(default)"
    logger.info("TRANSFORM target=%s | schema=%s | sql_count=%d | on_error=%s",
                label, schema_display, len(sql_files), on_error)

    try:
        _run_sql_loop(ctx, conn, conn_type, sql_files, on_error)
    finally:
        conn.close()

    # logger.info("TRANSFORM stage end")


def _run_sql_loop(ctx, conn, conn_type, sql_files, on_error):
    logger = ctx.logger
    total = len(sql_files)
    success = failed = 0

    for i, sql_file in enumerate(sql_files, 1):
        sql_text = sql_file.read_text(encoding="utf-8")
        rendered = render_sql(sql_text, ctx.params)

        logger.info("TRANSFORM [%d/%d] %s", i, total, sql_file.name)
        start = time.time()
        try:
            _execute(conn, conn_type, rendered)
            logger.info("TRANSFORM [%d/%d] done (%.2fs)", i, total, time.time() - start)
            success += 1
        except Exception as e:
            logger.error("TRANSFORM [%d/%d] FAILED (%.2fs): %s", i, total, time.time() - start, e)
            failed += 1
            if on_error == "stop":
                logger.error("TRANSFORM aborted (on_error=stop)")
                break

    logger.info("TRANSFORM summary | success=%d failed=%d total=%d", success, failed, total)


def _ensure_param_schemas(conn, sql_files, params, logger):
    """SQL 파일에서 @{param} 패턴을 스캔, 대응하는 param 값이 있으면 스키마 자동 생성."""
    at_pattern = re.compile(r"@\{(\w+)\}")
    schemas = set()
    for sf in sql_files:
        text = sf.read_text(encoding="utf-8")
        for m in at_pattern.finditer(text):
            key = m.group(1)
            val = str(params.get(key, "")).strip()
            if val:
                schemas.add(val)
    for s in sorted(schemas):
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{s}"')
        logger.info("TRANSFORM ensure schema '%s'", s)


def _execute(conn, conn_type, sql_text):
    """세미콜론으로 분리해서 순차 실행. 주석·공백 statement 제거."""
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    if conn_type == "duckdb":
        for stmt in statements:
            conn.execute(stmt)

    elif conn_type == "sqlite3":
        cur = conn.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
            conn.commit()
        finally:
            cur.close()

    elif conn_type == "oracle":
        cur = conn.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
            conn.commit()
        finally:
            cur.close()
