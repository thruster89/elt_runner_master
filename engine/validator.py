# file: engine/validator.py
"""
Pre-run Validation — 파이프라인 실행 전 설정/경로/연결 검증.

validate_pre_run(ctx) 호출 시:
  - errors: 실행 불가 문제 → 하나라도 있으면 파이프라인 중단
  - warnings: 잠재적 문제 → 로그에 경고만 출력

반환: (errors: list[str], warnings: list[str])
"""

from pathlib import Path

from engine.context import RunContext
from engine.path_utils import resolve_path


def validate_pre_run(ctx: RunContext):
    """파이프라인 실행 전 전체 검증. (errors, warnings) 튜플 반환."""
    errors = []
    warnings = []

    job_cfg = ctx.job_config
    env_cfg = ctx.env_config

    # 활성 스테이지 목록 결정
    pipeline_cfg = job_cfg.get("pipeline", {})
    stages = pipeline_cfg.get("stages", ["export", "load", "transform", "report"])
    if ctx.stage_filter:
        stages = [s for s in stages if s in ctx.stage_filter]

    # ── 1. Source 설정 검증 ──────────────────────────────
    if "export" in stages and job_cfg.get("export"):
        _validate_source(job_cfg, env_cfg, errors, warnings)

    # ── 2. Target 설정 검증 ──────────────────────────────
    needs_target = any(s in stages for s in ("load", "transform", "report"))
    if needs_target:
        _validate_target(job_cfg, errors, warnings)

    # ── 3. 경로 검증 ────────────────────────────────────
    _validate_paths(ctx, job_cfg, stages, errors, warnings)

    # ── 4. 파라미터 검증 ─────────────────────────────────
    _validate_params(ctx, stages, warnings)

    # ── 5. 스테이지별 설정값 범위 검증 ─────────────────
    _validate_stage_options(job_cfg, stages, errors, warnings)

    return errors, warnings


def _validate_source(job_cfg, env_cfg, errors, warnings):
    """소스 DB 설정 검증."""
    source_sel = job_cfg.get("source", {})
    source_type = (source_sel.get("type") or "oracle").strip().lower()
    host_name = source_sel.get("host")

    if not host_name:
        errors.append("source.host가 설정되지 않았습니다.")
        return

    sources_cfg = env_cfg.get("sources", {})

    if source_type == "oracle":
        oracle_cfg = sources_cfg.get("oracle")
        if not oracle_cfg:
            errors.append("env.yml에 sources.oracle 설정이 없습니다.")
            return
        hosts = oracle_cfg.get("hosts", {})
        host_cfg = hosts.get(host_name)
        if not host_cfg:
            errors.append(
                f"env.yml에 Oracle 호스트 '{host_name}'이 없습니다. "
                f"(사용 가능: {list(hosts.keys())})"
            )
            return
        for field in ("user", "password", "dsn"):
            if not host_cfg.get(field):
                errors.append(f"Oracle 호스트 '{host_name}'에 '{field}' 필드가 없습니다.")

    elif source_type == "vertica":
        vertica_cfg = sources_cfg.get("vertica")
        if not vertica_cfg:
            errors.append("env.yml에 sources.vertica 설정이 없습니다.")
            return
        hosts = vertica_cfg.get("hosts", {})
        if host_name not in hosts:
            errors.append(
                f"env.yml에 Vertica 호스트 '{host_name}'이 없습니다. "
                f"(사용 가능: {list(hosts.keys())})"
            )
            return
        host_cfg = hosts[host_name]
        for field in ("host", "user", "database"):
            if not host_cfg.get(field):
                errors.append(f"Vertica 호스트 '{host_name}'에 '{field}' 필드가 없습니다.")
    else:
        warnings.append(f"알 수 없는 소스 타입: '{source_type}'")


def _validate_target(job_cfg, errors, warnings):
    """타겟 DB 설정 검증."""
    target_cfg = job_cfg.get("target", {})
    tgt_type = (target_cfg.get("type") or "").strip().lower()

    if not tgt_type:
        # transform/report가 있어도 target 설정 없으면 스테이지에서 skip됨
        return

    if tgt_type not in ("duckdb", "sqlite3", "oracle"):
        errors.append(f"지원하지 않는 target 타입: '{tgt_type}' (duckdb/sqlite3/oracle)")


def _validate_paths(ctx, job_cfg, stages, errors, warnings):
    """경로 존재 여부 검증."""
    export_cfg = job_cfg.get("export", {})
    transform_cfg = job_cfg.get("transform", {})
    report_cfg = job_cfg.get("report", {})

    # Export sql_dir
    if "export" in stages and "export" in job_cfg:
        sql_dir_str = export_cfg.get("sql_dir")
        if sql_dir_str:
            sql_dir = resolve_path(ctx, sql_dir_str)
            if not sql_dir.exists():
                errors.append(f"Export sql_dir가 존재하지 않습니다: {sql_dir}")
            elif not list(sql_dir.glob("*.sql")):
                warnings.append(f"Export sql_dir에 SQL 파일이 없습니다: {sql_dir}")
        else:
            if "export" in stages:
                errors.append("export.sql_dir이 설정되지 않았습니다.")

    # Transform sql_dir
    if "transform" in stages and transform_cfg:
        sql_dir_str = transform_cfg.get("sql_dir")
        if sql_dir_str:
            sql_dir = resolve_path(ctx, sql_dir_str)
            if not sql_dir.exists():
                warnings.append(f"Transform sql_dir가 존재하지 않습니다: {sql_dir}")

    # Report sql_dir (skip_sql=false일 때)
    if "report" in stages and report_cfg:
        skip_sql = bool(report_cfg.get("skip_sql", False))
        if not skip_sql:
            export_csv_cfg = report_cfg.get("export_csv", {})
            if export_csv_cfg.get("enabled", False):
                sql_dir_str = export_csv_cfg.get("sql_dir")
                if sql_dir_str:
                    sql_dir = resolve_path(ctx, sql_dir_str)
                    if not sql_dir.exists():
                        warnings.append(f"Report sql_dir가 존재하지 않습니다: {sql_dir}")


def _validate_params(ctx, stages, warnings):
    """파라미터 설정 검증 — 범위/파이프 문법 유효성."""
    import re
    for stage_name in stages:
        params = ctx.get_stage_params(stage_name)
        if not params:
            continue
        for k, v in params.items():
            v_str = str(v).strip()
            if ":" in v_str:
                # 범위 문법 검증: YYYYMM:YYYYMM 또는 YYYYMM:YYYYMM~OPT
                raw = v_str.split("~", 1)[0] if "~" in v_str else v_str
                parts = raw.split(":", 1)
                if len(parts) == 2:
                    start, end = parts
                    if not (re.match(r"^\d{6}$", start) and re.match(r"^\d{6}$", end)):
                        warnings.append(
                            f"[{stage_name}] 파라미터 '{k}={v_str}': "
                            f"범위 문법은 YYYYMM:YYYYMM 형식이어야 합니다."
                        )
                    elif int(start) > int(end):
                        warnings.append(
                            f"[{stage_name}] 파라미터 '{k}={v_str}': "
                            f"시작값({start})이 끝값({end})보다 큽니다."
                        )


def _validate_stage_options(job_cfg, stages, errors, warnings):
    """스테이지별 설정값의 유효 범위를 검증한다."""

    # ── export 옵션 ────────────────────────────────────
    if "export" in stages:
        export_cfg = job_cfg.get("export", {})

        pw = export_cfg.get("parallel_workers")
        if pw is not None:
            try:
                pw_int = int(pw)
                if pw_int < 1:
                    errors.append(f"export.parallel_workers는 1 이상이어야 합니다 (현재: {pw})")
            except (TypeError, ValueError):
                errors.append(f"export.parallel_workers가 숫자가 아닙니다: {pw}")

        comp = (export_cfg.get("compression") or "").strip().lower()
        if comp and comp not in ("none", "gzip"):
            warnings.append(
                f"export.compression 값이 유효하지 않습니다: '{comp}' "
                f"(지원: none, gzip)"
            )

    # ── load 옵션 ──────────────────────────────────────
    if "load" in stages:
        load_cfg = job_cfg.get("load", {})
        mode = (load_cfg.get("mode") or "").strip().lower()
        valid_modes = ("replace", "truncate", "append", "delete", "")
        if mode and mode not in valid_modes:
            warnings.append(
                f"load.mode 값이 유효하지 않습니다: '{mode}' "
                f"(지원: replace, truncate, append, delete)"
            )

    # ── transform 옵션 ─────────────────────────────────
    if "transform" in stages:
        transform_cfg = job_cfg.get("transform", {})
        on_error = (transform_cfg.get("on_error") or "").strip().lower()
        if on_error and on_error not in ("stop", "continue"):
            warnings.append(
                f"transform.on_error 값이 유효하지 않습니다: '{on_error}' "
                f"(지원: stop, continue)"
            )
