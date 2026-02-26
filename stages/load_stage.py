# file: v2/stages/load_stage.py

import hashlib
import time
from pathlib import Path

from engine.connection import connect_target
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, resolve_table_name, extract_sqlname_from_csv, extract_params_from_csv


def _sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024
    return f"{nbytes:.1f}TB"


def _collect_csv_info(csv_files, sql_map):
    """CSV 파일 목록에 대해 테이블 매핑·크기 정보를 수집한다 (stat만 사용, 파일 내용 미읽음)."""
    items = []
    for csv_path in csv_files:
        sqlname = extract_sqlname_from_csv(csv_path)
        sql_file = sql_map.get(sqlname)
        table_name = resolve_table_name(sql_file) if sql_file else None
        size = csv_path.stat().st_size
        items.append({
            "csv_file": csv_path.name,
            "table": table_name,
            "sql_found": sql_file is not None,
            "size": size,
            "size_h": _human_size(size),
        })
    return items


def run(ctx: RunContext):
    logger = ctx.logger
    job_cfg = ctx.job_config

    # logger.info("LOAD stage start")

    export_cfg = job_cfg.get("export", {})
    if not export_cfg:
        logger.info("LOAD stage skipped (no export config)")
        return

    target_cfg = job_cfg.get("target", {})
    if not target_cfg:
        logger.info("LOAD stage skipped (no target config)")
        return

    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    export_dir = export_base / ctx.job_name
    if not export_dir.exists():
        if ctx.mode == "plan":
            logger.info("LOAD [PLAN] export dir not found: %s (export 실행 후 확인 가능)", export_dir)
            return
        export_dir = export_base

    csv_files = sorted([
        p for p in export_dir.iterdir()
        if p.is_file() and p.name.endswith((".csv", ".csv.gz"))
    ])
    if not csv_files:
        if ctx.mode == "plan":
            logger.info("LOAD [PLAN] CSV 파일 없음 — export 실행 후 확인 가능 (%s)", export_dir)
        else:
            logger.warning("No CSV/CSV.GZ files found in %s", export_dir)
        return

    sql_dir = resolve_path(ctx, export_cfg.get("sql_dir", "sql/export"))
    sql_files = sort_sql_files(sql_dir)
    sql_map = {p.stem: p for p in sql_files}

    # ── --include 필터: export와 동일하게 CSV도 필터링 ──
    include_patterns = getattr(ctx, "include_patterns", []) or []
    if include_patterns:
        before = len(csv_files)
        csv_files = [
            f for f in csv_files
            if any(pat.lower() in extract_sqlname_from_csv(f).lower()
                   for pat in include_patterns)
        ]
        logger.info("LOAD --include filter applied: %d -> %d files (patterns: %s)",
                     before, len(csv_files), include_patterns)
        if not csv_files:
            logger.warning("--include filter resulted in no CSV files to load (patterns=%s)",
                           include_patterns)
            return

    tgt_type = (target_cfg.get("type") or "").strip().lower()
    schema = (target_cfg.get("schema") or "").strip() or None  # None이면 스키마 없음

    # ── load.mode 결정 ──
    load_cfg = job_cfg.get("load", {})
    if tgt_type == "oracle":
        load_mode = load_cfg.get("mode", "delete")
        if load_mode in ("replace", "truncate"):
            logger.warning("Oracle: load.mode=%s not supported, falling back to delete", load_mode)
            load_mode = "delete"
    else:
        load_mode = load_cfg.get("mode", "replace")
    if load_mode not in ("replace", "truncate", "append", "delete"):
        logger.warning("Unknown load.mode=%s, using replace", load_mode)
        load_mode = "replace"

    # ── PLAN 모드: 사전 확인 리포트 ──
    if ctx.mode == "plan":
        _run_load_plan(ctx, logger, csv_files, sql_map, tgt_type, schema, load_mode)
        return

    if schema:
        logger.info("LOAD target type=%s | schema=%s | csv_count=%d | load.mode=%s",
                     tgt_type, schema, len(csv_files), load_mode)
    else:
        logger.info("LOAD target type=%s | csv_count=%d | load.mode=%s",
                     tgt_type, len(csv_files), load_mode)

    # ----------------------------------------
    # 연결 팩토리 사용 + Adapter별 초기화
    # ----------------------------------------
    conn, conn_type, label = connect_target(ctx, target_cfg)
    logger.info("LOAD target=%s", label)

    try:
        if conn_type == "duckdb":
            from adapters.targets.duckdb_target import load_csv, _ensure_schema, _ensure_history
            if schema:
                _ensure_schema(conn, schema)
            _ensure_history(conn, schema)
            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, schema, load_mode=load_mode))

        elif conn_type == "sqlite3":
            from adapters.targets.sqlite_target import load_csv, _ensure_history
            _ensure_history(conn)
            if schema:
                logger.info("SQLite: schema not supported, ignoring schema setting (schema=%s)", schema)
            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, load_mode=load_mode))

        elif conn_type == "oracle":
            from adapters.targets.oracle_target import load_csv
            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, schema, load_mode=load_mode,
                                        params=extract_params_from_csv(csv_path)))
    finally:
        conn.close()

    # logger.info("LOAD stage end")


def _run_load_plan(ctx, logger, csv_files, sql_map, tgt_type, schema, load_mode):
    """PLAN 모드: 로드 대상 파일 목록·테이블 매핑을 사전 확인한다."""
    items = _collect_csv_info(csv_files, sql_map)

    loadable = [it for it in items if it["sql_found"]]
    no_sql   = [it for it in items if not it["sql_found"]]

    logger.info("")
    logger.info("LOAD [PLAN] ── 사전 확인 리포트 ──")
    logger.info("  Target     : %s%s", tgt_type,
                f" (schema={schema})" if schema else "")
    logger.info("  Load Mode  : %s", load_mode)
    logger.info("  CSV Dir    : %s", csv_files[0].parent if csv_files else "?")
    total_size = sum(it["size"] for it in loadable)
    logger.info("  Total Files: %d  (loadable=%d, no_sql=%d)",
                len(items), len(loadable), len(no_sql))
    logger.info("  Total Size : %s", _human_size(total_size))
    logger.info("")

    for i, it in enumerate(loadable, 1):
        logger.info("  [%d/%d] %s → %s  (%s)",
                     i, len(loadable), it["csv_file"], it["table"], it["size_h"])

    if no_sql:
        logger.info("")
        logger.info("  ── SQL 매핑 없음 (스킵 예정) ──")
        for it in no_sql:
            logger.info("    %s  (%s)", it["csv_file"], it["size_h"])

    logger.info("")
    logger.info("LOAD [PLAN] 완료 — 실제 로드는 run 모드에서 실행하세요.")


def _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type, load_fn):
    total = len(csv_files)
    loaded = 0
    skipped = 0
    failed = 0

    for i, csv_path in enumerate(csv_files, 1):
        sqlname = extract_sqlname_from_csv(csv_path)
        sql_file = sql_map.get(sqlname)

        if not sql_file:
            logger.warning("CSV[%d/%d] skip (sql not found): %s", i, total, csv_path.name)
            skipped += 1
            continue

        table_name = resolve_table_name(sql_file)
        file_hash = _sha256_file(csv_path)

        logger.info("LOAD [%d/%d] | table=%s | file=%s", i, total, table_name, csv_path.name)

        try:
            result = load_fn(table_name, csv_path, file_hash)
            if result == -1:
                skipped += 1
            else:
                loaded += 1
        except Exception as e:
            logger.exception("LOAD failed | table=%s | file=%s | %s", table_name, csv_path.name, e)
            failed += 1

    logger.info("LOAD summary | loaded=%d skipped=%d failed=%d", loaded, skipped, failed)