# file: stages/transform_stage.py
"""
TRANSFORM stage
load된 target DB에서 SQL을 실행하는 가공/집계 단계.

job.yml 설정:
  transform:
    sql_dir: sql/transform/duckdb    # target DB에서 실행할 SQL 디렉토리
    on_error: stop                   # stop(기본) / continue
"""

import time

from engine.connection import connect_target
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, render_sql


def run(ctx: RunContext):
    logger = ctx.logger
    # logger.info("TRANSFORM stage start")

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

    # DuckDB: schema가 설정되어 있으면 세션 기본 스키마 지정
    if conn_type == "duckdb" and schema:
        conn.execute(f'SET schema = \'{schema}\'')
        logger.info("TRANSFORM SET schema = '%s'", schema)

    # schema를 params에 주입
    # SQL에서 ${schema}.tablename 또는 @{schema}tablename 사용 가능
    # @{schema} 사용 시: 값이 있으면 "schema." 으로, 없으면 "" 으로 치환
    ctx.params.setdefault("schema", schema)

    logger.info("TRANSFORM target=%s | sql_count=%d | on_error=%s", label, len(sql_files), on_error)

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
