# file: stages/task_tracking.py
"""
스테이지 공통 task 상태 추적.

run_info.json에 각 task의 상태(success/failed/pending/running/skipped)를 기록하고,
retry 모드에서 이전 run의 실패 task만 재실행할 수 있도록 한다.
"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path

_lock = threading.Lock()
logger = logging.getLogger(__name__)


# ── task key ────────────────────────────────────────────────
def make_task_key(sql_file: Path, param_set: dict | None = None) -> str:
    """task를 고유하게 식별하는 키 생성."""
    if param_set:
        param_part = "__".join(f"{k}={v}" for k, v in sorted(param_set.items()))
        return f"{sql_file.stem}__{param_part}"
    return sql_file.stem


# ── 상태 기록 ───────────────────────────────────────────────
def init_run_info(run_info_path: Path, *, job_name: str, run_id: str,
                  stage: str, mode: str, params: dict | None = None):
    """run_info.json 초기화. 이미 존재하면 tasks 보존."""
    run_info_path.parent.mkdir(parents=True, exist_ok=True)
    existing_tasks = {}
    if run_info_path.exists():
        try:
            with open(run_info_path, encoding="utf-8") as f:
                existing_tasks = json.load(f).get("tasks", {})
        except Exception:
            logger.debug("기존 run_info 파싱 실패 (초기화 진행): %s", run_info_path, exc_info=True)
    info = {
        "job_name": job_name,
        "run_id": run_id,
        "stage": stage,
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "params": params or {},
        "tasks": existing_tasks,
    }
    with open(run_info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)


def update_task_status(run_info_path: Path, task_key: str, status: str,
                       rows: int | None = None, elapsed: float | None = None,
                       error: str | None = None):
    """run_info.json의 tasks 필드에 task 상태 업데이트 (thread-safe)."""
    with _lock:
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
            logger.warning("task status 기록 실패: %s/%s", task_key, status, exc_info=True)


# ── 실패 task 로드 ──────────────────────────────────────────
def load_failed_tasks(base_dir: Path, job_name: str, run_id: str,
                      stage: str) -> set | None:
    """
    이전 run 중 가장 최근 run_info.json에서 failed/pending task key 반환.
    실패 task 없거나 이전 run 없으면 None (전체 실행).
    """
    job_dir = base_dir / job_name
    if not job_dir.exists():
        logger.warning("[%s] RETRY: no directory found (%s) — running all tasks",
                       stage.upper(), job_dir)
        return None

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
            # 현재 run_id는 제외
            if info.get("run_id") == run_id:
                continue
            # 동일 stage만
            if info.get("stage") != stage:
                continue
            if "tasks" in info:
                candidates.append((d, info))
        except Exception:
            logger.debug("run_info 파싱 실패 (skip): %s", run_info_path, exc_info=True)
            continue

    if not candidates:
        logger.warning("[%s] RETRY: no previous run history — running all tasks",
                       stage.upper())
        return None

    _, prev_info = candidates[0]
    prev_run_id = prev_info.get("run_id", "?")
    tasks = prev_info.get("tasks", {})

    failed_keys = {k for k, v in tasks.items()
                   if v.get("status") in ("failed", "pending")}

    logger.info("[%s] RETRY: based on run_id=%s | total=%d failed=%d",
                stage.upper(), prev_run_id, len(tasks), len(failed_keys))

    if not failed_keys:
        logger.info("[%s] RETRY: no failed tasks — running all tasks", stage.upper())
        return None

    for k in sorted(failed_keys):
        logger.info("  retry target: %s", k)

    return failed_keys


def summarize_run_info(run_info_path: Path) -> tuple[int, int, int]:
    """run_info.json에서 (success, failed, skipped) 카운트 반환."""
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
        logger.warning("run_info 요약 파싱 실패: %s", run_info_path, exc_info=True)
    return success, failed, skipped
