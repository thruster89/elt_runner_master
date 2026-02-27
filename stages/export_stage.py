# file: v2/stages/export_stage.py

import json
import time
import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from adapters.sources.oracle_client import init_oracle_client, get_oracle_conn
from adapters.sources.vertica_client import get_vertica_conn
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, render_sql, detect_used_params, _strip_sql_comments
from engine.runtime_state import stop_event


# ---------------------------
# Thread local storage
# ---------------------------
_thread_local = threading.local()
_thread_connections = []
_conn_list_lock = threading.Lock()


def get_thread_connection(source_type, env_cfg, host_name):
    """
    thread마다 connection 1개 재사용.
    생성된 connection은 _thread_connections에 추적하여 나중에 일괄 정리.
    """
    if hasattr(_thread_local, "conn") and _thread_local.conn:
        return _thread_local.conn

    if source_type == "oracle":
        oracle_cfg = env_cfg["sources"]["oracle"]
        host_cfg = oracle_cfg["hosts"].get(host_name)

        if not host_cfg:
            raise RuntimeError(f"Oracle host not found: {host_name}")

        init_oracle_client(oracle_cfg)
        conn = get_oracle_conn(host_cfg)

    elif source_type == "vertica":
        vertica_cfg = env_cfg["sources"]["vertica"]
        host_cfg = vertica_cfg["hosts"].get(host_name)
        conn = get_vertica_conn(host_cfg)

    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    _thread_local.conn = conn
    with _conn_list_lock:
        _thread_connections.append(conn)
    return conn


def _close_all_connections(logger):
    """export 종료 시 thread-local 커넥션 일괄 정리"""
    closed = 0
    for conn in _thread_connections:
        try:
            conn.close()
            closed += 1
        except Exception:
            pass
    _thread_connections.clear()
    _thread_local.__dict__.pop("conn", None)
    if closed:
        logger.debug("Thread connections closed: %d", closed)


# ---------------------------
# Param expand
# ---------------------------
def expand_range_value(value: str):
    raw = value.strip()

    if "~" in raw:
        range_part, opt = raw.split("~", 1)
        opt = opt.upper().strip()
    else:
        range_part = raw
        opt = None

    if ":" not in range_part:
        return [range_part]

    start, end = range_part.split(":", 1)

    def to_int_ym(s):
        return int(s[:4]) * 12 + int(s[4:6]) - 1

    def to_str_ym(n):
        y = n // 12
        m = n % 12 + 1
        return f"{y:04d}{m:02d}"

    s = to_int_ym(start)
    e = to_int_ym(end)

    result = []
    for i in range(s, e + 1):
        ym = to_str_ym(i)
        month = ym[4:6]

        if opt == "Q" and month not in ("03", "06", "09", "12"):
            continue
        if opt == "H" and month not in ("06", "12"):
            continue
        if opt == "Y" and month != "12":
            continue

        result.append(ym)

    return result


def expand_params(params: dict):
    from itertools import product
    import logging

    logger = logging.getLogger(__name__)

    multi_keys = []
    values = []

    for k, v in params.items():
        v_str = str(v).strip()
        multi_keys.append(k)

        if ":" in v_str:
            expanded = expand_range_value(v_str)
            # logger.info("Param expand | %s -> %d values", v_str, len(expanded))
            values.append(expanded)

        elif "," in v_str:
            split_vals = [x.strip() for x in v_str.split(",")]
            # logger.info("Param expand | %s -> %d values", v_str, len(split_vals))
            values.append(split_vals)

        else:
            # logger.info("Param expand | %s -> 1 value", v_str)
            values.append([v_str])

    expanded = []
    for combo in product(*values):
        expanded.append(dict(zip(multi_keys, combo)))

    return expanded


# ---------------------------
# Helpers
# ---------------------------
def sanitize_sql(sql: str) -> str:
    sql = sql.strip()
    while sql.endswith(";") or sql.endswith("/"):
        sql = sql[:-1].rstrip()
    return sql


def build_csv_name(sqlname: str, host: str, params: dict, ext: str,
                   name_style: str = "full",
                   strip_prefix: bool = False) -> str:
    """
    CSV 파일명 생성.
    name_style:
      "full"    — key_value 형태 (기본, 현행)  예: sql__host__clsYymm_202003.csv
      "compact" — value만 사용                  예: sql__host__202003.csv
    strip_prefix:
      True  — 숫자 접두사 제거  예: 01_contract → contract
      False — 유지 (기본)       예: 01_contract → 01_contract
    """
    from engine.sql_utils import strip_sql_prefix
    base = strip_sql_prefix(sqlname) if strip_prefix else sqlname
    parts = [base]

    if host:
        parts.append(host)

    for k in sorted(params.keys()):
        v = str(params[k]).replace(" ", "_")
        if name_style == "compact":
            parts.append(v)
        else:
            parts.append(f"{k}_{v}")

    return "__".join(parts) + f".{ext}"


def backup_existing_file(file_path: Path, backup_dir: Path, keep: int = 10):
    if not file_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = file_path.stem + f"__{ts}" + file_path.suffix
    target = backup_dir / backup_name

    shutil.move(str(file_path), str(target))

    prefix = file_path.stem + "__"
    backups = sorted(
        backup_dir.glob(prefix + "*.csv*"),
        key=lambda p: p.stat().st_mtime
    )

    while len(backups) > keep:
        backups[0].unlink()
        backups.pop(0)


def build_log_prefix(sql_file: Path, params: dict) -> str:
    if not params:
        return f"[{sql_file.stem}]"

    short = []
    for k in sorted(params.keys()):
        short.append(f"{k}={params[k]}")

    return f"[{sql_file.stem}|{' '.join(short)}]"


def _make_task_key(sql_file: Path, param_set: dict) -> str:
    """task를 고유하게 식별하는 키 생성"""
    param_part = "__".join(f"{k}={v}" for k, v in sorted(param_set.items()))
    return f"{sql_file.stem}__{param_part}" if param_part else sql_file.stem


# ---------------------------
# Plan mode: Dryrun report
# ---------------------------
def run_plan(ctx, sql_files, export_cfg, out_dir, ext, name_style="full",
             strip_prefix=False):
    logger = ctx.logger
    source_sel = ctx.job_config.get("source", {})
    host_name = source_sel.get("host", "")

    logger.info("EXPORT [PLAN] generating dryrun report...")

    tasks = []
    for sql_file in sql_files:
        sql_text_raw = sql_file.read_text(encoding="utf-8")

        # SQL별 사용 파라미터만 확장
        used_keys = detect_used_params(sql_text_raw, ctx.params)
        relevant_params = {k: v for k, v in ctx.params.items() if k in used_keys}
        sql_param_sets = expand_params(relevant_params) if relevant_params else [{}]

        for param_set in sql_param_sets:
            rendered = sanitize_sql(render_sql(sql_text_raw, param_set))
            csv_name = build_csv_name(
                sqlname=sql_file.stem,
                host=host_name,
                params=param_set,
                ext=ext,
                name_style=name_style,
                strip_prefix=strip_prefix,
            )
            out_file = out_dir / csv_name

            # SQL 기본 검증: SELECT 포함 여부, 파라미터 미치환 잔여 여부
            warnings = []
            upper = rendered.upper().strip()
            if not upper.startswith("SELECT") and "SELECT" not in upper[:200]:
                warnings.append("no SELECT found or first statement is not SELECT")

            # 치환 안 된 파라미터 패턴 감지 (${xxx}, :xxx, {#xxx})
            # 주석 행과 문자열 리터럴('...' 안) 내용을 제거한 뒤 검사 → 오탐 방지
            rendered_active = _strip_sql_comments(rendered)
            sql_no_strings = re.sub(r"'[^']*'", "''", rendered_active)
            leftover = re.findall(r'\$\{[^}]+\}|\{#[^}]+\}|(?<!\:)\:[a-zA-Z_]\w*', sql_no_strings)
            if leftover:
                warnings.append(f"suspected unresolved parameter: {leftover}")

            tasks.append({
                "sql_file": sql_file.name,
                "params": param_set,
                "task_key": _make_task_key(sql_file, param_set),
                "output_file": str(out_file),
                "rendered_sql_preview": rendered[:500] + ("..." if len(rendered) > 500 else ""),
                "warnings": warnings,
            })

    # JSON 리포트
    report = {
        "job_name": ctx.job_name,
        "run_id": ctx.run_id,
        "mode": "plan",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "params": ctx.params,
        "source": source_sel,
        "export_config": {
            "sql_dir": export_cfg.get("sql_dir"),
            "out_dir": export_cfg.get("out_dir"),
            "format": export_cfg.get("format", "csv"),
            "compression": export_cfg.get("compression", "none"),
            "overwrite": export_cfg.get("overwrite", False),
            "parallel_workers": export_cfg.get("parallel_workers", 1),
        },
        "total_tasks": len(tasks),
        "warning_count": sum(1 for t in tasks if t["warnings"]),
        "tasks": tasks,
    }

    # 리포트 저장 경로: data/export/{job_name}/{run_id}/plan_report.json
    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    report_dir = export_base / ctx.job_name / ctx.run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    json_path = report_dir / "plan_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # 사람이 읽기 쉬운 텍스트 리포트
    txt_path = report_dir / "plan_report.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write(f"  DRYRUN REPORT\n")
        f.write(f"  Job     : {ctx.job_name}\n")
        f.write(f"  Run ID  : {ctx.run_id}\n")
        f.write(f"  At      : {report['generated_at']}\n")
        f.write(f"  Source  : {source_sel.get('type')} / {source_sel.get('host')}\n")
        f.write(f"  Params  : {ctx.params}\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"total tasks    : {len(tasks)}\n")
        f.write(f"tasks with warnings : {report['warning_count']}\n\n")

        for i, t in enumerate(tasks, 1):
            status = "⚠ WARNING" if t["warnings"] else "OK"
            f.write(f"[{i:03d}] {status}\n")
            f.write(f"  SQL file  : {t['sql_file']}\n")
            f.write(f"  Params   : {t['params']}\n")
            f.write(f"  output file : {t['output_file']}\n")
            if t["warnings"]:
                for w in t["warnings"]:
                    f.write(f"  ⚠  {w}\n")
            f.write(f"  SQL preview:\n")
            for line in t["rendered_sql_preview"].splitlines():
                f.write(f"    {line}\n")
            f.write("\n")

    # 콘솔 요약 출력
    logger.info("")
    logger.info("=" * 60)
    logger.info(" DRYRUN REPORT summary")
    logger.info("-" * 60)
    logger.info(" total tasks       : %d", len(tasks))
    logger.info(" tasks with warnings : %d", report["warning_count"])
    logger.info(" report (JSON)    : %s", json_path)
    logger.info(" report (TEXT)    : %s", txt_path)
    logger.info("=" * 60)

    if report["warning_count"] > 0:
        logger.warning("some tasks have warnings. check plan_report.txt")

    for t in tasks:
        status = "⚠ WARN" if t["warnings"] else "OK  "
        warn_str = " | " + " / ".join(t["warnings"]) if t["warnings"] else ""
        logger.info("  [%s] %s  params=%s%s", status, t["sql_file"], t["params"], warn_str)

    logger.info("")
    logger.info("EXPORT [PLAN] done — no actual DB connection")


# ---------------------------
# Retry mode: 실패 task 로드
# ---------------------------
def load_failed_tasks(ctx, export_cfg) -> set:
    """
    이전 run 중 가장 최근 run_info.json에서 failed/pending task_key 목록 반환.
    없으면 None 반환 (전체 실행).
    """
    import logging
    logger = logging.getLogger(__name__)

    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    job_dir = export_base / ctx.job_name

    if not job_dir.exists():
        logger.warning("RETRY: no job directory found (%s) — running all tasks", job_dir)
        return None

    # run_info.json이 있는 디렉토리 중 tasks가 있는 것만, 최신순 정렬
    candidates = []
    for d in sorted(job_dir.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        run_info_path = d / "run_info.json"
        if not run_info_path.exists():
            continue
        try:
            with open(run_info_path, encoding="utf-8") as f:
                info = json.load(f)
            # 현재 run_id는 제외 (자기 자신)
            if info.get("run_id") == ctx.run_id:
                continue
            if "tasks" in info:
                candidates.append((d, info))
        except Exception:
            continue

    if not candidates:
        logger.warning("RETRY: no previous run history found — running all tasks")
        return None

    prev_dir, prev_info = candidates[0]
    prev_run_id = prev_info.get("run_id", prev_dir.name)
    tasks = prev_info.get("tasks", {})

    failed_keys = {k for k, v in tasks.items() if v.get("status") in ("failed", "pending")}

    logger.info("RETRY: based on previous run_id=%s", prev_run_id)
    logger.info("RETRY: total tasks=%d / failed+pending=%d", len(tasks), len(failed_keys))

    if not failed_keys:
        logger.info("RETRY: no failed tasks — running all tasks")
        return None

    for k in sorted(failed_keys):
        logger.info("  retry target: %s", k)

    return failed_keys


# ---------------------------
# Task 상태 기록
# ---------------------------
_status_lock = threading.Lock()


def _update_task_status(run_info_path: Path, task_key: str, status: str,
                        rows: int = None, elapsed: float = None, error: str = None):
    """run_info.json의 tasks 필드에 task 상태 업데이트 (thread-safe)"""
    with _status_lock:
        try:
            with open(run_info_path, encoding="utf-8") as f:
                info = json.load(f)

            if "tasks" not in info:
                info["tasks"] = {}

            entry = {"status": status, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            if rows is not None:
                entry["rows"] = rows
            if elapsed is not None:
                entry["elapsed"] = round(elapsed, 2)
            if error is not None:
                entry["error"] = str(error)[:500]

            info["tasks"][task_key] = entry

            with open(run_info_path, "w", encoding="utf-8") as f:
                json.dump(info, f, indent=2, ensure_ascii=False)
        except Exception:
            pass  # 상태 기록 실패는 무시


# ---------------------------
# Stage entry
# ---------------------------
def run(ctx: RunContext):
    logger = ctx.logger
    job_cfg = ctx.job_config
    env_cfg = ctx.env_config

    export_cfg = job_cfg.get("export")
    if not export_cfg:
        logger.info("EXPORT stage skipped (no config)")
        return

    sql_dir = resolve_path(ctx, export_cfg["sql_dir"])
    out_dir = resolve_path(ctx, export_cfg["out_dir"]) / ctx.job_name
    out_dir.mkdir(parents=True, exist_ok=True)

    source_sel = job_cfg.get("source", {})
    source_type = source_sel.get("type", "oracle")
    host_name = source_sel.get("host")

    sql_files = sort_sql_files(sql_dir)
    if not sql_files:
        logger.warning("No SQL files found in %s", sql_dir)
        return

    # ── --include 필터 적용 ──────────────────────────────
    include_patterns = getattr(ctx, "include_patterns", []) or []
    if include_patterns:
        def _matches(f: Path) -> bool:
            # sql_dir 기준 rel: "mart/01_bigt.sql"
            rel  = f.relative_to(sql_dir).as_posix().lower()
            stem = f.stem.lower()
            return any(
                pat.lower() in rel or pat.lower() in stem
                for pat in include_patterns
            )
        before = len(sql_files)
        sql_files = [f for f in sql_files if _matches(f)]
        logger.info(
            "EXPORT --include filter applied: %d -> %d files (patterns: %s)",
            before, len(sql_files), include_patterns
        )
        if not sql_files:
            logger.warning("--include filter resulted in no SQL files to run (patterns=%s)", include_patterns)
            return

    fmt = export_cfg.get("format", "csv")
    compression = export_cfg.get("compression", "none")
    overwrite = export_cfg.get("overwrite", False)
    backup_keep = export_cfg.get("backup_keep", 10)
    parallel_workers = export_cfg.get("parallel_workers", 1)
    name_style = export_cfg.get("csv_name_style", "full")
    strip_prefix = export_cfg.get("csv_strip_prefix", False)

    ext = "csv.gz" if compression == "gzip" else "csv"

    # ----------------------------------------
    # PLAN 모드: dryrun report만 생성하고 종료
    # ----------------------------------------
    if ctx.mode == "plan":
        run_plan(ctx, sql_files, export_cfg, out_dir, ext, name_style=name_style,
                 strip_prefix=strip_prefix)
        return

    # ----------------------------------------
    # RETRY 모드: 실패 task 목록 로드
    # ----------------------------------------
    failed_task_keys = None
    if ctx.mode == "retry":
        failed_task_keys = load_failed_tasks(ctx, export_cfg)

    # run_info.json 경로 (task 상태 기록용)
    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    run_info_path = export_base / ctx.job_name / ctx.run_id / "run_info.json"

    stall_seconds = export_cfg.get("timeout_seconds", 1800)

    def _export_one(sql_file, param_set, idx, total_sql, param_idx, total_param):

        if stop_event.is_set():
            logger.warning("Export interrupted before start")
            return

        task_key = _make_task_key(sql_file, param_set)
        prefix = build_log_prefix(sql_file, param_set)

        # retry 모드: failed_task_keys에 없으면 skip
        if failed_task_keys is not None and task_key not in failed_task_keys:
            logger.info("%s RETRY skip (succeeded in previous run)", prefix)
            return

        # task 시작 상태 기록
        _update_task_status(run_info_path, task_key, "running")

        try:
            conn = get_thread_connection(source_type, env_cfg, host_name)

            if source_type == "vertica":
                from adapters.sources.vertica_source import export_sql_to_csv as export_func
            else:
                from adapters.sources.oracle_source import export_sql_to_csv as export_func

            csv_name = build_csv_name(
                sqlname=sql_file.stem,
                host=host_name,
                params=param_set,
                ext=ext,
                name_style=name_style,
                strip_prefix=strip_prefix,
            )

            out_file = out_dir / csv_name

            if out_file.exists() and not overwrite and ctx.mode != "retry":
                logger.info("%s skip (already exists)", prefix)
                _update_task_status(run_info_path, task_key, "skipped")
                return

            if out_file.exists() and (overwrite or ctx.mode == "retry"):
                backup_existing_file(out_file, out_dir / "_backup", keep=backup_keep)

            logger.info(
                "%s EXPORT start [%d/%d] param[%d/%d]",
                prefix, idx, total_sql, param_idx, total_param
            )

            sql_text = sql_file.read_text(encoding="utf-8")
            rendered_sql = sanitize_sql(render_sql(sql_text, param_set))

            start_time = time.time()

            rows = export_func(
                conn=conn,
                sql_text=rendered_sql,
                out_file=out_file,
                logger=logger,
                compression=compression,
                fetch_size=10000,
                stall_seconds=stall_seconds,
                log_prefix=prefix,
            )

            elapsed = time.time() - start_time
            size_mb = out_file.stat().st_size / (1024 * 1024) if out_file.exists() else 0

            logger.info(
                "%s EXPORT done rows=%d size=%.2fMB elapsed=%.2fs",
                prefix,
                rows or 0,
                size_mb,
                elapsed
            )

            _update_task_status(run_info_path, task_key, "success",
                                rows=rows or 0, elapsed=elapsed)

        except Exception as e:
            # 커넥션 오류 시 thread-local에서 제거 → 다음 task에서 새 커넥션 생성
            _thread_local.__dict__.pop("conn", None)
            logger.exception("%s EXPORT failed: %s", prefix, e)
            _update_task_status(run_info_path, task_key, "failed", error=str(e))

    logger.info("Parallel workers=%d", parallel_workers)

    tasks = []
    for idx, sql_file in enumerate(sql_files, 1):
        sql_text_raw = sql_file.read_text(encoding="utf-8")
        used_keys = detect_used_params(sql_text_raw, ctx.params)
        relevant_params = {k: v for k, v in ctx.params.items() if k in used_keys}
        sql_param_sets = expand_params(relevant_params) if relevant_params else [{}]
        for param_idx, param_set in enumerate(sql_param_sets, 1):
            tasks.append((sql_file, param_set, idx, len(sql_files), param_idx, len(sql_param_sets)))

    # 전체 task를 pending으로 초기화 (retry 시 pending도 재실행 대상)
    for sql_file, param_set, *_ in tasks:
        task_key = _make_task_key(sql_file, param_set)
        if failed_task_keys is None or task_key in (failed_task_keys or set()):
            _update_task_status(run_info_path, task_key, "pending")

    try:
        if parallel_workers <= 1:
            for t in tasks:
                if stop_event.is_set():
                    logger.warning("EXPORT stopped by user")
                    break
                _export_one(*t)
        else:
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = [executor.submit(_export_one, *t) for t in tasks]
                for f in as_completed(futures):
                    if stop_event.is_set():
                        logger.warning("EXPORT cancelled")
                        break
                    f.result()
    finally:
        _close_all_connections(logger)