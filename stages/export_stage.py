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
from adapters.sources import oracle_client as _oc
from adapters.sources.vertica_client import get_vertica_conn
from engine.context import RunContext
from engine.path_utils import resolve_path
from engine.sql_utils import sort_sql_files, render_sql, detect_used_params, _strip_sql_comments
from engine.runtime_state import stop_event
from stages.task_tracking import (
    make_task_key, update_task_status,
    load_failed_tasks as _load_failed_tasks_shared,
)


# ---------------------------
# Thread local storage
# ---------------------------
_thread_local = threading.local()
_thread_connections = []
_conn_list_lock = threading.Lock()

# Thick 모드 OCI 메모리 누적 방지: 스레드당 N회 사용마다 연결 갱신
# set_recycle_interval()로 workers 수 반영하여 동적 설정
_conn_recycle_interval = 20


def set_recycle_interval(parallel_workers: int):
    """workers 수에 따라 스레드당 recycle 간격 설정.
    전체 동시 사용량(workers × interval)이 약 200을 넘지 않도록 조정."""
    global _conn_recycle_interval
    _conn_recycle_interval = max(10, 200 // max(parallel_workers, 1))


def _new_connection(source_type, env_cfg, host_name):
    """소스 타입에 맞는 새 연결 생성."""
    if source_type == "oracle":
        oracle_cfg = env_cfg["sources"]["oracle"]
        host_cfg = oracle_cfg["hosts"].get(host_name)
        if not host_cfg:
            raise RuntimeError(f"Oracle host not found: {host_name}")
        init_oracle_client(oracle_cfg)
        return get_oracle_conn(host_cfg)

    if source_type == "vertica":
        vertica_cfg = env_cfg["sources"]["vertica"]
        host_cfg = vertica_cfg["hosts"].get(host_name)
        return get_vertica_conn(host_cfg)

    raise ValueError(f"Unsupported source type: {source_type}")


def _need_recycle(source_type, use_count):
    """Oracle thick 모드에서만 OCI 메모리 누적 방지를 위해 recycle 필요."""
    if source_type != "oracle":
        return False
    if _oc._oracle_client_mode != "thick":
        return False
    return use_count >= _conn_recycle_interval


def get_thread_connection(source_type, env_cfg, host_name):
    """
    thread마다 connection 1개 재사용.
    Oracle thick 모드에서만 _conn_recycle_interval마다 연결 갱신 (OCI 메모리 누적 방지).
    """
    use_count = getattr(_thread_local, "use_count", 0)

    if hasattr(_thread_local, "conn") and _thread_local.conn:
        if not _need_recycle(source_type, use_count):
            _thread_local.use_count = use_count + 1
            return _thread_local.conn
        # 재활용 한도 도달 → 기존 연결 닫고 리스트에서도 제거
        old_conn = _thread_local.conn
        try:
            old_conn.close()
        except Exception:
            pass
        with _conn_list_lock:
            try:
                _thread_connections.remove(old_conn)
            except ValueError:
                pass
        _thread_local.conn = None

    conn = _new_connection(source_type, env_cfg, host_name)
    _thread_local.conn = conn
    _thread_local.use_count = 1
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
        if opt and opt.isdigit():
            if month != opt.zfill(2):
                continue

        result.append(ym)

    return result


MAX_PARAM_COMBINATIONS = 10_000


def expand_params(params: dict, mode: str = "product",
                  max_combinations: int | None = None):
    """파라미터 값을 확장하여 조합 리스트 반환.

    mode:
        "product" — 카르테시안 곱 (기본, 모든 조합)
        "zip"     — 위치별 1:1 매칭 (같은 인덱스끼리 쌍)

    구분자:
        "|" — 다중 값 확장 (예: critYm=201809|202001)
        ":" — 범위 확장   (예: critYm=201801:201812)
        ","  — 그대로 전달 (SQL IN 절 등에서 사용)

    max_combinations:
        조합수 상한. None이면 MAX_PARAM_COMBINATIONS 사용.
        초과 시 ValueError 발생.
    """
    from itertools import product as iproduct
    import logging

    logger = logging.getLogger(__name__)
    limit = max_combinations if max_combinations is not None else MAX_PARAM_COMBINATIONS

    multi_keys = []
    values = []

    for k, v in params.items():
        v_str = str(v).strip()
        multi_keys.append(k)

        if ":" in v_str:
            expanded = expand_range_value(v_str)
            values.append(expanded)

        elif "|" in v_str:
            split_vals = [x.strip() for x in v_str.split("|")]
            values.append(split_vals)

        else:
            values.append([v_str])

    # 조합수 사전 검증 (product 모드)
    if mode != "zip" and values:
        total = 1
        for v in values:
            total *= len(v)
            if total > limit:
                detail = ", ".join(f"{k}={len(v)}" for k, v in zip(multi_keys, values))
                raise ValueError(
                    f"파라미터 조합수가 상한({limit:,})을 초과합니다: "
                    f"총 {total:,}+ 조합 ({detail})"
                )

    expanded = []
    if mode == "zip":
        # 다중값(2개 이상) 파라미터들의 길이가 같은지 검증
        multi_lengths = [(k, len(v)) for k, v in zip(multi_keys, values) if len(v) > 1]
        if multi_lengths:
            lengths = set(ln for _, ln in multi_lengths)
            if len(lengths) > 1:
                detail = ", ".join(f"{k}={ln}" for k, ln in multi_lengths)
                raise ValueError(
                    f"zip 모드에서는 다중값 파라미터의 개수가 같아야 합니다: {detail}"
                )
            zip_len = multi_lengths[0][1]
            if zip_len > limit:
                raise ValueError(
                    f"파라미터 조합수가 상한({limit:,})을 초과합니다: "
                    f"{zip_len:,} 조합"
                )
            # 단일값 파라미터는 zip_len만큼 반복
            aligned = [v if len(v) > 1 else v * zip_len for v in values]
        else:
            aligned = values
        for combo in zip(*aligned):
            expanded.append(dict(zip(multi_keys, combo)))
    else:
        for combo in iproduct(*values):
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


def _cleanup_alt_ext(out_file: Path, backup_dir: Path, keep: int, logger):
    """compression 전환 시 반대 확장자의 orphan 파일(+meta.json) 정리.

    예: gzip→csv 전환 시, 같은 이름의 .csv.gz 가 남아있으면
    LOAD에서 중복 로드되므로 백업 후 제거한다.
    """
    name = out_file.name
    if name.endswith(".csv.gz"):
        alt = out_file.parent / (name[:-len(".gz")])       # .csv.gz → .csv
    elif name.endswith(".csv"):
        alt = out_file.parent / (name + ".gz")             # .csv → .csv.gz
    else:
        return

    if alt.exists():
        logger.info("Removing orphan (alt extension): %s", alt.name)
        backup_existing_file(alt, backup_dir, keep)
        # 대응하는 orphan meta.json도 정리
        alt_stem = alt.name[:-len(".csv.gz")] if alt.name.endswith(".csv.gz") else alt.name[:-len(".csv")]
        alt_meta = alt.parent / (alt_stem + ".meta.json")
        if alt_meta.exists():
            # meta.json은 현재 파일의 것과 같은 이름이므로 중복 삭제하지 않음
            # (out_file과 alt의 stem이 동일하면 같은 meta.json을 공유)
            out_stem = name[:-len(".csv.gz")] if name.endswith(".csv.gz") else name[:-len(".csv")]
            if alt_stem != out_stem:
                alt_meta.unlink()
                logger.debug("Removed orphan meta: %s", alt_meta.name)


def build_log_prefix(sql_file: Path, params: dict) -> str:
    if not params:
        return f"[{sql_file.stem}]"

    short = []
    for k in sorted(params.keys()):
        short.append(f"{k}={params[k]}")

    return f"[{sql_file.stem}|{' '.join(short)}]"


# ---------------------------
# Plan mode: Dryrun report
# ---------------------------
def run_plan(ctx, sql_files, export_cfg, out_dir, ext, name_style="full",
             strip_prefix=False, stage_params=None, stage_param_mode=None):
    logger = ctx.logger
    source_sel = ctx.job_config.get("source", {})
    host_name = source_sel.get("host", "")

    if stage_params is None:
        stage_params = ctx.get_stage_params("export")
    if stage_param_mode is None:
        stage_param_mode = ctx.get_stage_param_mode("export")

    logger.info("EXPORT [PLAN] generating dryrun report...")

    tasks = []
    for sql_file in sql_files:
        sql_text_raw = sql_file.read_text(encoding="utf-8")

        # SQL별 사용 파라미터만 확장
        used_keys = detect_used_params(sql_text_raw, stage_params)
        relevant_params = {k: v for k, v in stage_params.items() if k in used_keys}
        param_mode = stage_param_mode
        sql_param_sets = expand_params(relevant_params, mode=param_mode) if relevant_params else [{}]

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
                "task_key": make_task_key(sql_file, param_set),
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
        "params": stage_params,
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
        f.write(f"  Params  : {stage_params}\n")
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


def load_failed_tasks(ctx, export_cfg) -> set:
    """이전 run의 failed/pending task_key 반환. task_tracking 모듈 위임."""
    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    return _load_failed_tasks_shared(
        export_base, ctx.job_name, ctx.run_id, stage="export"
    )


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

    # 스테이지별 독립 params / param_mode
    stage_params = ctx.get_stage_params("export")
    stage_param_mode = ctx.get_stage_param_mode("export")

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
                 strip_prefix=strip_prefix,
                 stage_params=stage_params, stage_param_mode=stage_param_mode)
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

        task_key = make_task_key(sql_file, param_set)
        prefix = build_log_prefix(sql_file, param_set)

        # retry 모드: failed_task_keys에 없으면 skip
        if failed_task_keys is not None and task_key not in failed_task_keys:
            logger.info("%s RETRY skip (succeeded in previous run)", prefix)
            return

        # task 시작 상태 기록
        update_task_status(run_info_path, task_key, "running")

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
                update_task_status(run_info_path, task_key, "skipped")
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
                params=param_set,
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

            # compression 전환 시 이전 확장자 orphan 제거 (.csv ↔ .csv.gz)
            _cleanup_alt_ext(out_file, out_dir / "_backup", backup_keep, logger)

            update_task_status(run_info_path, task_key, "success",
                                rows=rows or 0, elapsed=elapsed)

        except Exception as e:
            # 커넥션 오류 시 즉시 close + thread-local에서 제거 → 다음 task에서 새 커넥션 생성
            bad_conn = _thread_local.__dict__.pop("conn", None)
            _thread_local.__dict__.pop("use_count", None)
            if bad_conn:
                try:
                    bad_conn.close()
                except Exception:
                    pass
                with _conn_list_lock:
                    try:
                        _thread_connections.remove(bad_conn)
                    except ValueError:
                        pass
            logger.exception("%s EXPORT failed: %s", prefix, e)
            update_task_status(run_info_path, task_key, "failed", error=str(e))

    set_recycle_interval(parallel_workers)
    if source_type == "oracle" and _oc._oracle_client_mode == "thick":
        logger.info("Parallel workers=%d  conn_recycle_interval=%d",
                    parallel_workers, _conn_recycle_interval)
    else:
        logger.info("Parallel workers=%d", parallel_workers)

    tasks = []
    for idx, sql_file in enumerate(sql_files, 1):
        sql_text_raw = sql_file.read_text(encoding="utf-8")
        used_keys = detect_used_params(sql_text_raw, stage_params)
        relevant_params = {k: v for k, v in stage_params.items() if k in used_keys}
        sql_param_sets = expand_params(relevant_params, mode=stage_param_mode) if relevant_params else [{}]
        for param_idx, param_set in enumerate(sql_param_sets, 1):
            tasks.append((sql_file, param_set, idx, len(sql_files), param_idx, len(sql_param_sets)))

    # 전체 task를 pending으로 초기화 (retry 시 pending도 재실행 대상)
    for sql_file, param_set, *_ in tasks:
        task_key = make_task_key(sql_file, param_set)
        if failed_task_keys is None or task_key in (failed_task_keys or set()):
            update_task_status(run_info_path, task_key, "pending")

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

    # ── 결과 요약 ────────────────────────────────────────
    success = failed = skipped = 0
    try:
        with open(run_info_path, encoding="utf-8") as f:
            info = json.load(f)
        for entry in info.get("tasks", {}).values():
            st = entry.get("status", "")
            if st == "success":
                success += 1
            elif st == "failed":
                failed += 1
            elif st == "skipped":
                skipped += 1
    except Exception:
        pass
    logger.info("EXPORT summary | success=%d failed=%d skipped=%d total=%d",
                success, failed, skipped, len(tasks))
    ctx.report_stage_result("export", success=success, failed=failed, skipped=skipped)