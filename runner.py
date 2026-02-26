# file: runner.py
import os
import sys
import json
import argparse
import yaml
import logging
import time
from datetime import datetime
from pathlib import Path

if "NUMEXPR_MAX_THREADS" not in os.environ:
    import multiprocessing
    _cpu = multiprocessing.cpu_count()
    os.environ["NUMEXPR_MAX_THREADS"] = str(max(1, _cpu // 2))

from engine.stage_registry import STAGE_REGISTRY
from engine.runtime_state import stop_event
from engine.context import RunContext
import signal


# ────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────
def setup_logging(log_dir: Path, debug: bool):
    log_dir.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"run_{run_date}.log"
    level = logging.DEBUG if debug else logging.INFO

    if debug:
        file_fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        console_fmt = file_fmt
    else:
        file_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
        console_fmt = logging.Formatter("%(levelname)s | %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    if root.handlers:
        root.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(file_fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(console_fmt)
    root.addHandler(sh)

    logger = logging.getLogger("runner")
    logger.setLevel(level)
    logger.propagate = True
    return logger


# ────────────────────────────────────────────────────────────
# Run ID
# ────────────────────────────────────────────────────────────
def generate_run_id(base_dir: Path, job_name: str) -> str:
    job_dir = base_dir / job_name
    job_dir.mkdir(parents=True, exist_ok=True)
    existing = []
    for p in job_dir.iterdir():
        if p.is_dir() and p.name.startswith(f"{job_name}_"):
            try:
                existing.append(int(p.name.replace(f"{job_name}_", "")))
            except ValueError:
                pass
    return f"{job_name}_{max(existing) + 1 if existing else 1:02d}"


def resolve_retry_run_id(base_dir: Path, job_name: str, logger) -> str:
    job_dir = base_dir / job_name
    if not job_dir.exists():
        logger.warning("RETRY: no directory found → generating new run_id")
        return generate_run_id(base_dir, job_name)
    def _run_number(d: Path) -> int:
        try:
            return int(d.name.rsplit("_", 1)[-1])
        except (ValueError, IndexError):
            return -1

    for d in sorted(job_dir.iterdir(), key=_run_number, reverse=True):
        if not d.is_dir():
            continue
        run_info_path = d / "run_info.json"
        if not run_info_path.exists():
            continue
        try:
            with open(run_info_path, encoding="utf-8") as f:
                info = json.load(f)
            if any(v.get("status") in ("failed", "pending") for v in info.get("tasks", {}).values()):
                logger.info("RETRY: retry target run_id = %s", d.name)
                return d.name
        except Exception:
            continue
    logger.info("RETRY: no failed previous run found → generating new run_id")
    return generate_run_id(base_dir, job_name)


def write_run_info(run_dir: Path, ctx: RunContext, start_time: str):
    run_dir.mkdir(parents=True, exist_ok=True)
    run_info_path = run_dir / "run_info.json"
    existing_tasks = {}
    if run_info_path.exists():
        try:
            with open(run_info_path, encoding="utf-8") as f:
                existing_tasks = json.load(f).get("tasks", {})
        except Exception:
            pass
    info = {
        "job_name": ctx.job_name,
        "run_id": ctx.run_id,
        "start_time": start_time,
        "mode": ctx.mode,
        "params": ctx.params,
        "tasks": existing_tasks,
    }
    with open(run_info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)


# ────────────────────────────────────────────────────────────
# Mode / CLI helpers
# ────────────────────────────────────────────────────────────
def _parse_mode(v: str) -> str:
    alias = {
        "plan": "plan", "dryrun": "plan", "dry-run": "plan",
        "run": "run", "normal": "run", "all": "run", "execute": "run",
        "retry": "retry", "failed": "retry", "replay": "retry", "fail": "retry",
    }
    s = (v or "").strip().lower()
    if s not in alias:
        raise argparse.ArgumentTypeError(f"Invalid --mode: {v}  (plan / run / retry)")
    return alias[s]


def _mode_display(v: str) -> str:
    return {"plan": "Plan (Dryrun)", "run": "Run", "retry": "Retry"}.get(v, v)


def parse_cli_params(param_list) -> dict:
    result = {}
    for item in (param_list or []):
        if "=" not in item:
            raise ValueError(f"Invalid --param format: {item!r}  (use key=value)")
        k, v = item.split("=", 1)
        result[k.strip()] = v.strip()
    return result


# ────────────────────────────────────────────────────────────
# deep_merge: CLI override → job_config 딕셔너리 병합
# ────────────────────────────────────────────────────────────
def _deep_set(d: dict, key_path: str, value):
    """'export.compression' 같은 경로를 재귀적으로 설정"""
    keys = key_path.split(".", 1)
    if len(keys) == 1:
        d[keys[0]] = value
    else:
        d.setdefault(keys[0], {})
        _deep_set(d[keys[0]], keys[1], value)


def apply_overrides(job_config: dict, overrides: list) -> dict:
    """
    --set key.path=value 형태의 override를 job_config에 적용.
    예) --set export.compression=none
        --set target.db_path=data/custom.duckdb
        --set export.overwrite=false
    """
    for item in (overrides or []):
        if "=" not in item:
            raise ValueError(f"Invalid --set format: {item!r}  (use key.path=value)")
        path, val = item.split("=", 1)
        # 타입 변환
        if val.lower() == "true":
            val = True
        elif val.lower() == "false":
            val = False
        else:
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
        _deep_set(job_config, path.strip(), val)
    return job_config


# ────────────────────────────────────────────────────────────
# target_label: 로그용 target 식별 문자열
# ────────────────────────────────────────────────────────────
def _target_label(target_cfg: dict, work_dir: Path) -> str:
    tgt_type = (target_cfg.get("type") or "").lower()
    if tgt_type == "oracle":
        schema = target_cfg.get("schema", "")
        return f"oracle (schema={schema})" if schema else "oracle"
    elif tgt_type in ("duckdb", "sqlite3"):
        db_path = target_cfg.get("db_path", "")
        if db_path:
            resolved = (work_dir / db_path).resolve()
            return f"{tgt_type} ({resolved})"
        return tgt_type
    return tgt_type or "(none)"


# ────────────────────────────────────────────────────────────
# Pipeline
# ────────────────────────────────────────────────────────────
def run_pipeline(ctx: RunContext):
    stages = ctx.job_config.get("pipeline", {}).get("stages", [])
    if not stages:
        ctx.logger.warning("No stages defined in pipeline")
        return

    # --stage 필터 적용
    stage_filter = getattr(ctx, "stage_filter", []) or []
    if stage_filter:
        before = stages[:]
        stages = [s for s in stages if s in stage_filter]
        ctx.logger.info("Stage filter: %s → %s", before, stages)
        if not stages:
            ctx.logger.warning("--stage filter result: no stages to execute (filter=%s)", stage_filter)
            return

    ctx.logger.info("")
    ctx.logger.info("=" * 60)
    ctx.logger.info(" PIPELINE START")
    ctx.logger.info("-" * 60)
    ctx.logger.info("Stages total=%d | %s", len(stages), stages)
    ctx.logger.info("")

    for idx, stage_name in enumerate(stages, 1):
        if stop_event.is_set():
            ctx.logger.warning("Pipeline stopped before stage execution")
            break

        stage_func = STAGE_REGISTRY.get(stage_name)
        if not stage_func:
            ctx.logger.error("Unknown stage: %s  (available: %s)", stage_name, list(STAGE_REGISTRY))
            raise ValueError(f"Unknown stage: {stage_name}")

        ctx.logger.info("[%d/%d] %s", idx, len(stages), stage_name.upper())
        ctx.logger.info("-" * 60)

        start = time.time()
        stage_func(ctx)
        elapsed = time.time() - start

        ctx.logger.info("-" * 60)
        ctx.logger.info("[%d/%d] %s DONE (%.2fs)", idx, len(stages), stage_name.upper(), elapsed)
        ctx.logger.info("")

        if stop_event.is_set():
            ctx.logger.warning("Pipeline stopped by user")
            break

    ctx.logger.info("============== PIPELINE FINISHED ==============")


# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────
def main():
    stop_event.clear()
    job_start_time = time.time()
    parser = argparse.ArgumentParser(
        description="batch_runner — Oracle/Vertica → CSV → Local DB pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # ── 기본 인수 ──────────────────────────────────────────
    parser.add_argument(
        "--job",
        default=None,
        help="job yml 파일 경로 (기본: jobs/ 폴더에서 자동 검색)",
    )
    parser.add_argument("--env", default="config/env.yml", help="env yml 파일 경로 (default: config/env.yml)")
    parser.add_argument("--workdir", default=".", help="작업 디렉토리 (default: .)")
    parser.add_argument("--debug", action="store_true", help="DEBUG 레벨 로깅")
    parser.add_argument(
        "--mode", type=_parse_mode, default="run",
        help="실행 모드: plan / run(default) / retry",
    )

    # ── 파라미터 override ─────────────────────────────────
    parser.add_argument(
        "--param", action="append", metavar="KEY=VALUE",
        help="params override (예: --param clsYymm=202003)",
    )
    parser.add_argument(
        "--set", action="append", metavar="PATH=VALUE", dest="overrides",
        help="job yml 항목 override (예: --set export.compression=none\n"
             "                              --set target.db_path=data/my.duckdb\n"
             "                              --set export.overwrite=true\n"
             "                              --set export.parallel_workers=4\n"
             "                              --set target.schema=MYSCHEMA\n"
             "                              --set report.excel.enabled=true)",
    )
    parser.add_argument(
        "--include", action="append", metavar="PATTERN", dest="include_patterns",
        help="실행할 SQL 파일 필터 (파일명/경로 부분 일치, 여러 개 가능)\n"
             "예) --include contract\n"
             "    --include A/a1 --include 02_payment\n"
             "    미지정 시 전체 SQL 실행",
    )
    parser.add_argument(
        "--stage", action="append", metavar="STAGE", dest="stage_filter",
        help="실행할 stage 지정 (여러 개 가능, 미지정 시 전체 실행)\n"
             "예) --stage export --stage transform",
    )
    parser.add_argument(
        "--timeout", type=int, default=None, metavar="SECONDS",
        help="export SQL 실행 timeout 초 (기본: 1800=30분)\n"
             "예) --timeout 3600  (60분)",
    )

    args = parser.parse_args()

    work_dir = Path(args.workdir).resolve()

    # ── --job 미지정 시 jobs/ 폴더에서 yml 자동 검색 ───────
    if args.job is None:
        jobs_dir = work_dir / "jobs"
        yml_files = sorted(jobs_dir.glob("*.yml")) if jobs_dir.exists() else []
        if not yml_files:
            parser.error("specify --job or place a yml file in the jobs/ folder.")
        if len(yml_files) == 1:
            job_path = yml_files[0]
        else:
            print("Found the following yml files in jobs/ folder:")
            for i, f in enumerate(yml_files, 1):
                print(f"  [{i}] {f.name}")
            choice = input("Select number (Enter = 1): ").strip()
            idx = int(choice) - 1 if choice.isdigit() else 0
            job_path = yml_files[max(0, min(idx, len(yml_files) - 1))]
            print(f"  → {job_path.name} selected\n")
    else:
        job_path = Path(args.job)
        # jobs/ 상대경로 자동 보완
        if not job_path.exists() and not job_path.is_absolute():
            candidate = work_dir / "jobs" / job_path
            if candidate.exists():
                job_path = candidate
            else:
                candidate2 = work_dir / job_path
                if candidate2.exists():
                    job_path = candidate2

    env_path = Path(args.env)

    job_config = yaml.safe_load(job_path.read_text(encoding="utf-8"))
    if not job_config or not isinstance(job_config, dict):
        print(f"Error: job file is empty or invalid: {job_path}")
        sys.exit(1)

    env_config = yaml.safe_load(env_path.read_text(encoding="utf-8"))
    if not env_config or not isinstance(env_config, dict):
        print(f"Error: env file is empty or invalid: {env_path}")
        sys.exit(1)

    # --set override 적용
    if args.overrides:
        job_config = apply_overrides(job_config, args.overrides)

    # --timeout → export.timeout_seconds 주입
    if args.timeout is not None:
        job_config.setdefault("export", {})["timeout_seconds"] = args.timeout

    logger = setup_logging(work_dir / "logs", debug=args.debug)
    job_name = job_config.get("job_name", "unnamed_job")

    signal.signal(signal.SIGINT, lambda sig, frame: (logger.warning("STOP requested (Ctrl+C)"), stop_event.set()))

    export_base = Path(job_config.get("export", {}).get("out_dir", "data/export"))

    # params: yml 기본값 → CLI override
    params = dict(job_config.get("params", {}))
    params.update(parse_cli_params(args.param))

    if args.mode == "retry":
        run_id = resolve_retry_run_id(export_base, job_name, logger)
    else:
        run_id = generate_run_id(export_base, job_name)

    start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ctx = RunContext(
        job_name=job_name,
        run_id=run_id,
        job_config=job_config,
        env_config=env_config,
        params=params,
        work_dir=work_dir,
        mode=args.mode,
        logger=logger,
        include_patterns=args.include_patterns or [],
        stage_filter=args.stage_filter or [],
    )

    # ── JOB START 헤더 로그 ──────────────────────────────
    source_sel = job_config.get("source", {})
    target_cfg = job_config.get("target", {})
    export_cfg = job_config.get("export", {})

    logger.info("")
    logger.info("=" * 60)
    logger.info(" JOB START")
    logger.info("-" * 60)
    logger.info(" Job Name  : %s", job_name)
    logger.info(" Run ID    : %s", run_id)
    logger.info(" Job File  : %s", job_path.resolve())
    logger.info(" Start     : %s", start_time_str)
    logger.info(" Mode      : %s", _mode_display(ctx.mode))
    logger.info("-" * 60)
    logger.info(" [SOURCE]  type=%s  host=%s", source_sel.get("type", "oracle"), source_sel.get("host", ""))
    logger.info(" [TARGET]  %s", _target_label(target_cfg, work_dir))
    logger.info("-" * 60)
    logger.info(" SQL Dir   : %s", export_cfg.get("sql_dir", ""))
    logger.info(" Export Dir: %s", export_cfg.get("out_dir", ""))
    logger.info(" Overwrite : %s", export_cfg.get("overwrite", False))
    logger.info(" Workers   : %s", export_cfg.get("parallel_workers", 1))
    logger.info(" Timeout   : %ss", export_cfg.get("timeout_seconds", 1800))

    if params:
        logger.info(" Params    : %s", ", ".join(f"{k}={v}" for k, v in params.items()))

        try:
            from stages.export_stage import expand_params, expand_range_value
            from engine.sql_utils import detect_used_params

            for k, v in params.items():
                v_str = str(v).strip()

                if ":" in v_str:
                    vals = expand_range_value(v_str)
                    logger.info(" Expanded  : %s=%d values", k, len(vals))
                elif "," in v_str:
                    count = len(v_str.split(","))
                    logger.info(" Expanded  : %s=%d values", k, count)
                else:
                    logger.info(" Expanded  : %s=1 value (%s)", k, v_str)

            # Total tasks: SQL별 사용 파라미터만 확장하여 정확 계산
            sql_dir_str = export_cfg.get("sql_dir", "")
            if sql_dir_str:
                from engine.sql_utils import sort_sql_files
                sql_dir_path = work_dir / sql_dir_str
                sql_files = sort_sql_files(sql_dir_path)
                if ctx.include_patterns:
                    sql_files = [
                        f for f in sql_files
                        if any(pat.lower() in f.relative_to(sql_dir_path).as_posix().lower()
                               or pat.lower() in f.stem.lower()
                               for pat in ctx.include_patterns)
                    ]
                total_tasks = 0
                for sf in sql_files:
                    sql_text = sf.read_text(encoding="utf-8")
                    used = detect_used_params(sql_text, params)
                    rel = {k: v for k, v in params.items() if k in used}
                    total_tasks += len(expand_params(rel)) if rel else 1
                logger.info(" Total Tasks: %d (sql=%d)", total_tasks, len(sql_files))
        except Exception as e:
            logger.debug("Param expand preview skipped: %s", e)

    if ctx.include_patterns:
        logger.info(" Include   : %s", ctx.include_patterns)
    if ctx.stage_filter:
        logger.info(" Stages    : %s (filtered)", ctx.stage_filter)
    logger.info(" Work Dir  : %s", work_dir)

    log_file = next(
        (h.baseFilename for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)),
        None,
    )
    logger.info(" Log File  : %s", log_file)
    logger.info("=" * 60)
    logger.info("")

    run_dir = export_base / job_name / run_id
    write_run_info(run_dir, ctx, start_time_str)

    run_pipeline(ctx)
    elapsed = time.time() - job_start_time   
    logger.info("Job finished | elapsed=%.2fs", elapsed)  


if __name__ == "__main__":
    main()
