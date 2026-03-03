"""transform_stage / report_stage 단위 테스트 (DB mock)."""
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from engine.context import RunContext


def _ctx(tmp_path, job_config=None, params=None, mode="run"):
    return RunContext(
        job_name="test_job",
        run_id="run_01",
        job_config=job_config or {},
        env_config={},
        params=params or {},
        work_dir=tmp_path,
        mode=mode,
        logger=logging.getLogger("test"),
    )


# =====================================================================
# transform_stage
# =====================================================================
class TestTransformStage:

    def test_no_config_skips(self, tmp_path):
        """transform 설정 없으면 조기 리턴."""
        ctx = _ctx(tmp_path)
        from stages import transform_stage
        transform_stage.run(ctx)  # 예외 없이 리턴

    def test_plan_mode_skips(self, tmp_path):
        """plan 모드면 건너뜀."""
        ctx = _ctx(tmp_path, job_config={"transform": {"sql_dir": "sql"}}, mode="plan")
        from stages import transform_stage
        transform_stage.run(ctx)

    def test_no_sql_dir_skips(self, tmp_path):
        """sql_dir 미설정 → 건너뜀."""
        ctx = _ctx(tmp_path, job_config={"transform": {}})
        from stages import transform_stage
        transform_stage.run(ctx)

    def test_sql_dir_not_found_skips(self, tmp_path):
        """sql_dir가 존재하지 않는 경로 → 건너뜀."""
        ctx = _ctx(tmp_path, job_config={
            "transform": {"sql_dir": str(tmp_path / "no_dir")}
        })
        from stages import transform_stage
        transform_stage.run(ctx)

    def test_no_target_skips(self, tmp_path):
        """target 설정 없으면 건너뜀."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_t.sql").write_text("SELECT 1")
        ctx = _ctx(tmp_path, job_config={
            "transform": {"sql_dir": str(sql_dir)},
        })
        from stages import transform_stage
        transform_stage.run(ctx)

    def test_duckdb_run(self, tmp_path):
        """DuckDB 타겟 정상 transform 흐름."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_create.sql").write_text("CREATE TABLE t AS SELECT 1 AS col")

        ctx = _ctx(tmp_path, job_config={
            "transform": {"sql_dir": str(sql_dir)},
            "target": {"type": "duckdb"},
        })

        mock_conn = MagicMock()
        with patch("stages.transform_stage.connect_target",
                   return_value=(mock_conn, "duckdb", "duckdb (test)")):
            from stages import transform_stage
            transform_stage.run(ctx)

        mock_conn.execute.assert_called()
        mock_conn.close.assert_called_once()
        assert ctx.stage_results["transform"]["success"] == 1

    def test_on_error_continue(self, tmp_path):
        """on_error=continue: 첫 SQL 실패해도 나머지 실행."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_fail.sql").write_text("INVALID SQL")
        (sql_dir / "02_ok.sql").write_text("SELECT 1")

        ctx = _ctx(tmp_path, job_config={
            "transform": {"sql_dir": str(sql_dir), "on_error": "continue"},
            "target": {"type": "duckdb"},
        })

        call_count = {"n": 0}
        def _mock_execute(stmt):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("bad sql")

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = _mock_execute

        with patch("stages.transform_stage.connect_target",
                   return_value=(mock_conn, "duckdb", "duckdb (test)")):
            from stages import transform_stage
            transform_stage.run(ctx)

        assert ctx.stage_results["transform"]["failed"] == 1
        assert ctx.stage_results["transform"]["success"] == 1

    def test_set_session_schema_called(self, tmp_path):
        """schema 설정 시 set_session_schema가 호출되는지."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_t.sql").write_text("SELECT 1")

        ctx = _ctx(tmp_path, job_config={
            "transform": {"sql_dir": str(sql_dir), "schema": "MY_SCHEMA"},
            "target": {"type": "duckdb"},
        })

        mock_conn = MagicMock()
        with patch("stages.transform_stage.connect_target",
                   return_value=(mock_conn, "duckdb", "duckdb (test)")), \
             patch("stages.transform_stage.set_session_schema") as mock_set:
            from stages import transform_stage
            transform_stage.run(ctx)

        mock_set.assert_called_once_with(mock_conn, "duckdb", "MY_SCHEMA", ctx.logger)


# =====================================================================
# report_stage
# =====================================================================
class TestReportStage:

    def test_no_config_skips(self, tmp_path):
        """report 설정 없으면 조기 리턴."""
        ctx = _ctx(tmp_path)
        from stages import report_stage
        report_stage.run(ctx)

    def test_plan_mode_skips(self, tmp_path):
        """plan 모드면 건너뜀."""
        ctx = _ctx(tmp_path, job_config={"report": {"source": "target"}}, mode="plan")
        from stages import report_stage
        report_stage.run(ctx)

    def test_skip_sql_mode(self, tmp_path):
        """skip_sql=true: CSV만 수집, DB 연결 없음."""
        csv_dir = tmp_path / "data" / "export"
        csv_dir.mkdir(parents=True)
        (csv_dir / "report.csv").write_text("col\n1\n")

        ctx = _ctx(tmp_path, job_config={
            "report": {
                "skip_sql": True,
                "csv_union_dir": str(csv_dir),
            },
        })

        from stages import report_stage
        report_stage.run(ctx)  # DB 연결 없이 완료
