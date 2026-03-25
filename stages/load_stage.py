# file: v2/stages/load_stage.py

import hashlib
import json
import time
from pathlib import Path

from engine.connection import connect_target
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, resolve_table_name, extract_sqlname_from_csv, extract_params_from_csv, strip_sql_prefix, _strip_data_ext


_LOAD_EXTENSIONS = (".csv", ".csv.gz", ".dat", ".dat.gz", ".tsv", ".tsv.gz")


def _extract_params(csv_path: Path) -> dict:
    """meta.json에서 params 읽기, 없으면 파일명 파싱 fallback."""
    name = csv_path.name
    stem = _strip_data_ext(name)
    meta_file = csv_path.parent / (stem + ".meta.json")
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        if isinstance(meta, dict) and "params" in meta:
            return meta["params"]
    # fallback: 파일명 파싱 (기존 export 데이터 호환)
    return extract_params_from_csv(csv_path)


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
        table_name = resolve_table_name(sql_file) if sql_file else strip_sql_prefix(sqlname)
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

    load_cfg   = job_cfg.get("load", {})
    export_cfg = job_cfg.get("export", {})

    target_cfg = job_cfg.get("target", {})
    if not target_cfg:
        logger.info("LOAD stage skipped (no target config)")
        return

    # csv_dir 결정: load.csv_dir 우선, 없으면 export.out_dir 폴백
    csv_dir_str = (load_cfg.get("csv_dir") or "").strip()
    if csv_dir_str:
        export_base = resolve_path(ctx, csv_dir_str)
    elif export_cfg:
        export_base = resolve_path(ctx, export_cfg.get("out_dir", ctx.get_default("export_out_dir")))
    else:
        logger.info("LOAD stage skipped (no csv_dir / export config)")
        return

    export_dir = export_base
    if not export_dir.exists() and not ctx.exported_files:
        if ctx.mode == "plan":
            logger.info("LOAD [PLAN] csv dir not found: %s (export 실행 후 확인 가능)", export_dir)
            return
        logger.warning("LOAD csv dir not found: %s — skipping (run export first)", export_dir)
        return

    # export → load 파이프라인: export가 생성한 파일만 로드 (잔여 CSV 혼입 방지)
    if ctx.exported_files:
        csv_files = sorted([p for p in ctx.exported_files if p.exists()])
        logger.info("LOAD using %d files from current export run", len(csv_files))
    else:
        csv_files = sorted([
            p for p in export_dir.iterdir()
            if p.is_file() and p.name.endswith(_LOAD_EXTENSIONS)
        ])
    if not csv_files:
        if ctx.mode == "plan":
            logger.info("LOAD [PLAN] CSV 파일 없음 — export 실행 후 확인 가능 (%s)", export_dir)
        else:
            logger.warning("No data files (csv/dat/tsv) found in %s", export_dir)
        return

    sql_dir = resolve_path(ctx, export_cfg.get("sql_dir", ctx.get_default("export_sql_dir")))
    sql_files = sort_sql_files(sql_dir)
    sql_map = {p.stem: p for p in sql_files}
    # prefix 제거된 CSV도 매핑되도록 stripped key 추가 (csv_strip_prefix 호환)
    for p in sql_files:
        stripped = strip_sql_prefix(p.stem)
        if stripped != p.stem:
            sql_map.setdefault(stripped, p)

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

    # ── load.mode / load.delimiter 결정 ──
    load_cfg = job_cfg.get("load", {})
    default_mode = "delete" if tgt_type == "oracle" else "replace"
    load_mode = load_cfg.get("mode", default_mode)
    if load_mode not in ("replace", "truncate", "append", "delete"):
        logger.warning("Unknown load.mode=%s, using replace", load_mode)
        load_mode = "replace"

    # delimiter: auto(None)이면 DuckDB read_csv_auto가 자동 판별
    delimiter = (load_cfg.get("delimiter") or "").strip() or None
    if delimiter:
        # 이스케이프 문자열 처리: \t → 탭
        delimiter = delimiter.replace("\\t", "\t")
        logger.info("LOAD delimiter = %r", delimiter)

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
            from adapters.targets.duckdb_target import load_csv, load_csv_batch, _ensure_schema, _ensure_history
            if schema:
                _ensure_schema(conn, schema)
            _ensure_history(conn, schema)

            # replace/truncate → 동일 테이블 다중 CSV를 batch로 한방 INSERT
            batch_fn = None
            if load_mode in ("replace", "truncate"):
                batch_fn = lambda table, csv_paths, file_hashes: \
                    load_csv_batch(conn, ctx.job_name, table, csv_paths, file_hashes,
                                   ctx.mode, schema, load_mode=load_mode,
                                   delimiter=delimiter)

            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash, lm=None:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, schema, load_mode=lm or load_mode,
                                        params=_extract_params(csv_path),
                                        delimiter=delimiter),
                           batch_fn=batch_fn)

        elif conn_type == "sqlite3":
            from adapters.targets.sqlite_target import load_csv, _ensure_history
            _ensure_history(conn)
            if schema:
                logger.info("SQLite: schema not supported, ignoring schema setting (schema=%s)", schema)
            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash, lm=None:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, load_mode=lm or load_mode,
                                        params=_extract_params(csv_path)))

        elif conn_type == "oracle":
            from adapters.targets.oracle_target import load_csv
            _run_load_loop(ctx, logger, csv_files, sql_map, conn_type,
                           load_fn=lambda table, csv_path, file_hash, lm=None:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash,
                                        ctx.mode, schema, load_mode=lm or load_mode,
                                        params=_extract_params(csv_path)))
    finally:
        conn.close()

    # logger.info("LOAD stage end")


def _run_load_plan(ctx, logger, csv_files, sql_map, tgt_type, schema, load_mode):
    """PLAN 모드: 로드 대상 파일 목록·테이블 매핑을 사전 확인한다."""
    items = _collect_csv_info(csv_files, sql_map)

    sql_mapped = [it for it in items if it["sql_found"]]
    direct     = [it for it in items if not it["sql_found"]]

    logger.info("")
    logger.info("LOAD [PLAN] ── 사전 확인 리포트 ──")
    logger.info("  Target     : %s%s", tgt_type,
                f" (schema={schema})" if schema else "")
    logger.info("  Load Mode  : %s", load_mode)
    logger.info("  CSV Dir    : %s", csv_files[0].parent if csv_files else "?")
    total_size = sum(it["size"] for it in items)
    logger.info("  Total Files: %d  (sql_mapped=%d, direct=%d)",
                len(items), len(sql_mapped), len(direct))
    logger.info("  Total Size : %s", _human_size(total_size))
    logger.info("")

    for i, it in enumerate(sql_mapped, 1):
        logger.info("  [%d/%d] %s → %s  (%s)",
                     i, len(sql_mapped), it["csv_file"], it["table"], it["size_h"])

    if direct:
        logger.info("")
        logger.info("  ── CSV 파일명 기반 매핑 (INSERT) ──")
        for i, it in enumerate(direct, 1):
            logger.info("  [%d/%d] %s → %s  (%s)",
                         i, len(direct), it["csv_file"], it["table"], it["size_h"])

    logger.info("")
    logger.info("LOAD [PLAN] 완료 — 실제 로드는 run 모드에서 실행하세요.")


def _group_by_table(csv_files, sql_map):
    """CSV 파일을 테이블명 기준으로 그룹핑 (순서 유지)."""
    from collections import OrderedDict
    groups = OrderedDict()
    for csv_path in csv_files:
        sqlname = extract_sqlname_from_csv(csv_path)
        sql_file = sql_map.get(sqlname)
        table_name = resolve_table_name(sql_file) if sql_file else strip_sql_prefix(sqlname)
        groups.setdefault(table_name, []).append((csv_path, sql_file))
    return groups


def _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type, load_fn, batch_fn=None):
    total = len(csv_files)
    loaded = 0
    skipped = 0
    failed = 0

    groups = _group_by_table(csv_files, sql_map)
    file_idx = 0

    for table_name, group in groups.items():
        has_sql = group[0][1] is not None

        # ── batch: 동일 테이블 다중 CSV → read_csv_auto([리스트]) 한방 INSERT ──
        if batch_fn and len(group) > 1:
            csv_paths = [g[0] for g in group]
            file_hashes = [_sha256_file(p) for p in csv_paths]
            start_idx = file_idx + 1
            file_idx += len(group)
            file_names = ", ".join(p.name for p in csv_paths)
            logger.info("LOAD [%d~%d/%d] | table=%s | %d files (batch)%s | %s",
                        start_idx, file_idx, total, table_name, len(group),
                        "" if has_sql else " (direct)", file_names)
            try:
                result = batch_fn(table_name, csv_paths, file_hashes)
                loaded += len(group)
            except Exception as e:
                logger.exception("LOAD batch failed | table=%s | %s", table_name, e)
                failed += len(group)
            continue

        # ── 단건: 기존 per-file 로직 ──
        seen_tables: set[str] = set()
        for csv_path, sql_file in group:
            file_idx += 1

            if table_name in seen_tables:
                override_mode = "append"
            else:
                override_mode = None
                seen_tables.add(table_name)

            file_hash = _sha256_file(csv_path)

            if has_sql:
                logger.info("LOAD [%d/%d] | table=%s | file=%s", file_idx, total, table_name, csv_path.name)
            else:
                logger.info("LOAD [%d/%d] | table=%s (direct) | file=%s", file_idx, total, table_name, csv_path.name)

            try:
                result = load_fn(table_name, csv_path, file_hash, override_mode)
                if result == -1:
                    skipped += 1
                else:
                    loaded += 1
            except Exception as e:
                logger.exception("LOAD failed | table=%s | file=%s | %s", table_name, csv_path.name, e)
                failed += 1

    logger.info("LOAD summary | loaded=%d skipped=%d failed=%d", loaded, skipped, failed)
    ctx.report_stage_result("load", success=loaded, failed=failed, skipped=skipped)