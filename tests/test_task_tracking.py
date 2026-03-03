"""task_tracking 모듈 + 스테이지 결과 수집 + 파이프라인 요약 테스트."""
import json
import logging
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch, MagicMock

import pytest
from engine.context import RunContext
from stages.task_tracking import (
    make_task_key, init_run_info, update_task_status,
    load_failed_tasks, summarize_run_info,
)


def _import_pipeline_summary():
    """runner._log_pipeline_summary를 oracledb 등 외부 의존성 없이 import."""
    stubs = {}
    # export_stage → oracle/vertica adapter import chain을 전부 stub
    stub_mods = [
        "oracledb", "vertica_python",
        "adapters", "adapters.sources", "adapters.targets",
        "adapters.sources.oracle_client", "adapters.sources.vertica_client",
        "adapters.targets.duckdb_target", "adapters.targets.sqlite_target",
        "adapters.targets.oracle_target",
    ]
    for mod_name in stub_mods:
        if mod_name not in sys.modules:
            m = ModuleType(mod_name)
            # from X import Y가 실패하지 않도록 MagicMock 속성 부여
            m.__dict__.setdefault("__all__", [])
            stubs[mod_name] = m
            sys.modules[mod_name] = m
    # MagicMock 속성 처리: oracle_client 등에서 특정 이름 import
    for mod_name in stub_mods:
        sys.modules[mod_name] = MagicMock()
    try:
        # runner가 이미 캐시돼 있으면 reload
        if "runner" in sys.modules:
            import importlib
            import runner
            importlib.reload(runner)
        from runner import _log_pipeline_summary
        return _log_pipeline_summary
    finally:
        for mod_name in stubs:
            sys.modules.pop(mod_name, None)


def _ctx(job_config=None, params=None, mode="run"):
    return RunContext(
        job_name="test",
        run_id="test_01",
        job_config=job_config or {},
        env_config={},
        params=params or {},
        work_dir=Path("/tmp/test"),
        mode=mode,
        logger=logging.getLogger("test"),
    )


# =====================================================================
# make_task_key
# =====================================================================
class TestMakeTaskKey:

    def test_no_params(self, tmp_path):
        sql = tmp_path / "create_table.sql"
        sql.touch()
        assert make_task_key(sql) == "create_table"

    def test_with_params(self, tmp_path):
        sql = tmp_path / "report.sql"
        sql.touch()
        key = make_task_key(sql, {"month": "202401", "area": "KR"})
        assert key == "report__area=KR__month=202401"  # sorted

    def test_empty_params_ignored(self, tmp_path):
        sql = tmp_path / "abc.sql"
        sql.touch()
        assert make_task_key(sql, {}) == "abc"


# =====================================================================
# init_run_info / update_task_status / summarize_run_info
# =====================================================================
class TestRunInfoLifecycle:

    def test_init_creates_file(self, tmp_path):
        p = tmp_path / "run_info.json"
        init_run_info(p, job_name="j", run_id="r1", stage="transform", mode="run")
        assert p.exists()
        info = json.loads(p.read_text())
        assert info["stage"] == "transform"
        assert info["tasks"] == {}

    def test_init_preserves_existing_tasks(self, tmp_path):
        p = tmp_path / "run_info.json"
        p.write_text(json.dumps({"tasks": {"t1": {"status": "success"}}}))
        init_run_info(p, job_name="j", run_id="r2", stage="transform", mode="retry")
        info = json.loads(p.read_text())
        assert info["tasks"]["t1"]["status"] == "success"
        assert info["run_id"] == "r2"

    def test_update_and_summarize(self, tmp_path):
        p = tmp_path / "run_info.json"
        init_run_info(p, job_name="j", run_id="r1", stage="transform", mode="run")

        update_task_status(p, "sql_a", "success", elapsed=1.5)
        update_task_status(p, "sql_b", "failed", error="timeout")
        update_task_status(p, "sql_c", "skipped")
        update_task_status(p, "sql_d", "success", elapsed=2.0)

        s, f, sk = summarize_run_info(p)
        assert s == 2
        assert f == 1
        assert sk == 1

    def test_update_error_truncated(self, tmp_path):
        p = tmp_path / "run_info.json"
        init_run_info(p, job_name="j", run_id="r1", stage="t", mode="run")
        update_task_status(p, "t1", "failed", error="x" * 1000)
        info = json.loads(p.read_text())
        assert len(info["tasks"]["t1"]["error"]) == 500

    def test_init_creates_parent_dirs(self, tmp_path):
        p = tmp_path / "deep" / "nested" / "run_info.json"
        init_run_info(p, job_name="j", run_id="r", stage="s", mode="run")
        assert p.exists()


# =====================================================================
# load_failed_tasks
# =====================================================================
class TestLoadFailedTasks:

    def _setup_prev_run(self, base_dir, job_name, run_id, stage, tasks):
        """이전 run의 run_info.json 생성."""
        d = base_dir / job_name / run_id
        d.mkdir(parents=True, exist_ok=True)
        info = {
            "job_name": job_name, "run_id": run_id,
            "stage": stage, "tasks": tasks,
        }
        (d / "run_info.json").write_text(json.dumps(info))

    def test_loads_failed_and_pending(self, tmp_path):
        self._setup_prev_run(tmp_path, "j", "j_01", "transform", {
            "a": {"status": "success"},
            "b": {"status": "failed"},
            "c": {"status": "pending"},
        })
        result = load_failed_tasks(tmp_path, "j", "j_02", "transform")
        assert result == {"b", "c"}

    def test_skips_current_run(self, tmp_path):
        self._setup_prev_run(tmp_path, "j", "j_01", "transform", {
            "a": {"status": "failed"},
        })
        # 현재 run_id와 동일하면 무시
        result = load_failed_tasks(tmp_path, "j", "j_01", "transform")
        assert result is None  # 후보 없음 → 전체 실행

    def test_filters_by_stage(self, tmp_path):
        self._setup_prev_run(tmp_path, "j", "j_01", "transform", {
            "a": {"status": "failed"},
        })
        # report stage로 조회 → transform 기록은 무시
        result = load_failed_tasks(tmp_path, "j", "j_02", "report")
        assert result is None

    def test_no_failed_returns_none(self, tmp_path):
        self._setup_prev_run(tmp_path, "j", "j_01", "transform", {
            "a": {"status": "success"},
            "b": {"status": "success"},
        })
        result = load_failed_tasks(tmp_path, "j", "j_02", "transform")
        assert result is None

    def test_no_directory_returns_none(self, tmp_path):
        result = load_failed_tasks(tmp_path / "nonexist", "j", "j_01", "transform")
        assert result is None


# =====================================================================
# report_stage_result (RunContext)
# =====================================================================
class TestReportStageResult:

    def test_basic(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=10, failed=2, skipped=3)
        r = ctx.stage_results["export"]
        assert r == {"success": 10, "failed": 2, "skipped": 3, "detail": ""}

    def test_independent_instances(self):
        ctx1 = _ctx()
        ctx2 = _ctx()
        ctx1.report_stage_result("export", success=5)
        assert ctx2.stage_results == {}


# =====================================================================
# _log_pipeline_summary
# =====================================================================
class TestLogPipelineSummary:

    def test_all_success(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=10)
        ctx.report_stage_result("load", success=10)
        _log_pipeline_summary = _import_pipeline_summary()
        with patch.object(ctx.logger, "info") as mock:
            _log_pipeline_summary(ctx, ["export", "load"])
            msgs = " ".join(str(c) for c in mock.call_args_list)
            assert "SUCCESS" in msgs

    def test_with_failures(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=8, failed=2)
        ctx.report_stage_result("load", success=10)
        _log_pipeline_summary = _import_pipeline_summary()
        with patch.object(ctx.logger, "info") as mock:
            _log_pipeline_summary(ctx, ["export", "load"])
            msgs = " ".join(str(c) for c in mock.call_args_list)
            assert "FAIL" in msgs
            assert "FAILED" in msgs

    def test_missing_stage_result(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=5)
        _log_pipeline_summary = _import_pipeline_summary()
        with patch.object(ctx.logger, "info") as mock:
            _log_pipeline_summary(ctx, ["export", "transform"])
            msgs = " ".join(str(c) for c in mock.call_args_list)
            assert "결과 없음" in msgs
