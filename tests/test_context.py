"""RunContext per-stage params 테스트."""
import logging
from pathlib import Path

import pytest
from engine.context import RunContext


def _make_ctx(job_config, params=None, param_mode="product"):
    """테스트용 RunContext 생성 헬퍼."""
    return RunContext(
        job_name="test_job",
        run_id="test_run_001",
        job_config=job_config,
        env_config={},
        params=params or {},
        work_dir=Path("/tmp/test"),
        mode="run",
        logger=logging.getLogger("test"),
        param_mode=param_mode,
    )


# =====================================================================
# get_stage_params
# =====================================================================
class TestGetStageParams:

    def test_global_params_fallback(self):
        """스테이지 section에 params 없으면 글로벌 params 반환."""
        ctx = _make_ctx(
            job_config={"export": {"sql_dir": "sql/export"}},
            params={"critYm": "202301"},
        )
        result = ctx.get_stage_params("export")
        assert result == {"critYm": "202301"}

    def test_stage_params_override(self):
        """스테이지 section에 params 있으면 그것만 사용 (글로벌 무시)."""
        ctx = _make_ctx(
            job_config={
                "export": {
                    "sql_dir": "sql/export",
                    "params": {"exportYm": "202306"},
                },
            },
            params={"critYm": "202301"},
        )
        result = ctx.get_stage_params("export")
        assert result == {"exportYm": "202306"}
        assert "critYm" not in result

    def test_stage_empty_params(self):
        """스테이지 params가 빈 dict이면 빈 dict 반환 (글로벌 사용 X)."""
        ctx = _make_ctx(
            job_config={"transform": {"params": {}}},
            params={"critYm": "202301"},
        )
        result = ctx.get_stage_params("transform")
        assert result == {}

    def test_nonexistent_stage(self):
        """존재하지 않는 스테이지 이름 → 글로벌 params fallback."""
        ctx = _make_ctx(
            job_config={},
            params={"global_key": "value"},
        )
        result = ctx.get_stage_params("nonexistent")
        assert result == {"global_key": "value"}

    def test_each_stage_independent(self):
        """각 스테이지가 서로 다른 params를 가질 수 있음."""
        ctx = _make_ctx(
            job_config={
                "export": {"params": {"ym": "202301|202302"}},
                "transform": {"params": {"critYm": "202306"}},
                "report": {},  # 글로벌 fallback
            },
            params={"globalKey": "gv"},
        )
        assert ctx.get_stage_params("export") == {"ym": "202301|202302"}
        assert ctx.get_stage_params("transform") == {"critYm": "202306"}
        assert ctx.get_stage_params("report") == {"globalKey": "gv"}

    def test_returns_copy(self):
        """반환값 수정이 원본에 영향 없음 (방어적 복사)."""
        ctx = _make_ctx(
            job_config={},
            params={"k": "v"},
        )
        result = ctx.get_stage_params("export")
        result["k"] = "modified"
        assert ctx.params["k"] == "v"  # 원본 불변


# =====================================================================
# get_stage_param_mode
# =====================================================================
class TestGetStageParamMode:

    def test_global_fallback(self):
        """스테이지에 param_mode 없으면 글로벌."""
        ctx = _make_ctx(
            job_config={"export": {}},
            param_mode="product",
        )
        assert ctx.get_stage_param_mode("export") == "product"

    def test_stage_override(self):
        """스테이지별 param_mode 우선."""
        ctx = _make_ctx(
            job_config={"export": {"param_mode": "zip"}},
            param_mode="product",
        )
        assert ctx.get_stage_param_mode("export") == "zip"

    def test_each_stage_independent_mode(self):
        """각 스테이지마다 다른 mode."""
        ctx = _make_ctx(
            job_config={
                "export": {"param_mode": "zip"},
                "transform": {"param_mode": "product"},
                "report": {},
            },
            param_mode="product",
        )
        assert ctx.get_stage_param_mode("export") == "zip"
        assert ctx.get_stage_param_mode("transform") == "product"
        assert ctx.get_stage_param_mode("report") == "product"  # 글로벌
