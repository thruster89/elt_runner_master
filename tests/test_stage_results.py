"""스테이지 결과 수집 및 파이프라인 요약 테스트."""
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from engine.context import RunContext


def _ctx(job_config=None, params=None):
    """테스트용 RunContext."""
    return RunContext(
        job_name="test",
        run_id="001",
        job_config=job_config or {},
        env_config={},
        params=params or {},
        work_dir=Path("/tmp/test"),
        mode="run",
        logger=logging.getLogger("test"),
    )


# =====================================================================
# report_stage_result
# =====================================================================
class TestReportStageResult:

    def test_basic_result(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=10, failed=2, skipped=3)
        r = ctx.stage_results["export"]
        assert r["success"] == 10
        assert r["failed"] == 2
        assert r["skipped"] == 3

    def test_default_zeros(self):
        ctx = _ctx()
        ctx.report_stage_result("transform")
        r = ctx.stage_results["transform"]
        assert r["success"] == 0
        assert r["failed"] == 0
        assert r["skipped"] == 0

    def test_multiple_stages(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=5)
        ctx.report_stage_result("load", success=5, skipped=1)
        ctx.report_stage_result("transform", success=3, failed=1)
        assert len(ctx.stage_results) == 3
        assert ctx.stage_results["transform"]["failed"] == 1

    def test_overwrite_result(self):
        """같은 스테이지 결과를 다시 기록하면 덮어쓰기."""
        ctx = _ctx()
        ctx.report_stage_result("export", success=1)
        ctx.report_stage_result("export", success=10, failed=2)
        assert ctx.stage_results["export"]["success"] == 10
        assert ctx.stage_results["export"]["failed"] == 2

    def test_detail_field(self):
        ctx = _ctx()
        ctx.report_stage_result("export", success=1, detail="partial run")
        assert ctx.stage_results["export"]["detail"] == "partial run"

    def test_initial_empty(self):
        ctx = _ctx()
        assert ctx.stage_results == {}

    def test_independent_between_instances(self):
        """각 RunContext 인스턴스가 독립적인 stage_results를 가짐."""
        ctx1 = _ctx()
        ctx2 = _ctx()
        ctx1.report_stage_result("export", success=5)
        assert ctx2.stage_results == {}


# =====================================================================
# _log_pipeline_summary (runner.py)
# =====================================================================
class TestLogPipelineSummary:

    def test_all_success(self):
        """모든 스테이지 성공 시 SUCCESS 출력."""
        ctx = _ctx()
        ctx.report_stage_result("export", success=10)
        ctx.report_stage_result("load", success=10)

        from runner import _log_pipeline_summary
        # 로그 출력만 확인 (에러 없이 완료)
        _log_pipeline_summary(ctx, ["export", "load"])

    def test_with_failures(self):
        """실패가 있으면 FAILED 출력."""
        ctx = _ctx()
        ctx.report_stage_result("export", success=8, failed=2)
        ctx.report_stage_result("load", success=10)

        from runner import _log_pipeline_summary
        # 로그 메시지에 FAILED가 포함되는지 확인
        with patch.object(ctx.logger, "info") as mock_info:
            _log_pipeline_summary(ctx, ["export", "load"])
            log_messages = " ".join(str(call) for call in mock_info.call_args_list)
            assert "FAIL" in log_messages
            assert "FAILED" in log_messages

    def test_missing_stage_result(self):
        """결과 없는 스테이지 → '결과 없음' 표시."""
        ctx = _ctx()
        ctx.report_stage_result("export", success=5)

        from runner import _log_pipeline_summary
        with patch.object(ctx.logger, "info") as mock_info:
            _log_pipeline_summary(ctx, ["export", "transform"])
            log_messages = " ".join(str(call) for call in mock_info.call_args_list)
            assert "결과 없음" in log_messages

    def test_skipped_shown(self):
        """스킵 건수가 표시됨."""
        ctx = _ctx()
        ctx.report_stage_result("export", success=5, skipped=3)

        from runner import _log_pipeline_summary
        with patch.object(ctx.logger, "info") as mock_info:
            _log_pipeline_summary(ctx, ["export"])
            log_messages = " ".join(str(call) for call in mock_info.call_args_list)
            assert "skipped=3" in log_messages
