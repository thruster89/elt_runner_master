"""BUG-1~4 재현 + 수정 검증 테스트."""
import json
import logging
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from engine.context import RunContext
from stages.task_tracking import (
    make_task_key, init_run_info, update_task_status, load_failed_tasks,
)


def _ctx(job_config=None, params=None, mode="run"):
    return RunContext(
        job_name="test",
        run_id="test_01",
        job_config=job_config or {},
        env_config={},
        params=params or {},
        work_dir=Path("/tmp/test"),
        mode=mode,
        logger=logging.getLogger("test_bug"),
    )


# =====================================================================
# BUG-1: transform_stage _run_sql_loop — sql_file vs remaining 오타
#   on_error=stop 시 나머지 파일의 SQL 대신 실패한 파일의 SQL을 읽음
# =====================================================================
class TestBug1_TransformPendingWrongFile:
    """on_error=stop 후 pending 기록 시 remaining 파일을 읽어야 하는데
    실패한 sql_file을 읽는 버그 재현."""

    def test_pending_uses_correct_file_params(self, tmp_path):
        """각 SQL 파일이 서로 다른 파라미터를 사용할 때,
        on_error=stop 후 pending 기록에 올바른 파라미터 조합이 사용되는지 검증."""
        from stages.transform_stage import _run_sql_loop

        # SQL 파일 생성: 각각 다른 파라미터 사용
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        sql_a = sql_dir / "01_a.sql"
        sql_b = sql_dir / "02_b.sql"
        sql_c = sql_dir / "03_c.sql"

        # a.sql은 ${month}만 사용, b.sql은 ${area}만 사용, c.sql은 ${year}만 사용
        sql_a.write_text("SELECT * FROM t WHERE month = '${month}'")
        sql_b.write_text("SELECT * FROM t WHERE area = '${area}'")
        sql_c.write_text("SELECT * FROM t WHERE year = '${year}'")

        # run_info 초기화
        run_info = tmp_path / "run_info.json"
        init_run_info(run_info, job_name="test", run_id="r1",
                      stage="transform", mode="run")

        ctx = _ctx(params={"month": "202301", "area": "KR|US", "year": "2023|2024"})

        # conn mock: 첫 번째 SQL(a)에서 실패
        mock_conn = MagicMock()
        call_count = [0]

        def fail_first(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("DB error")

        mock_conn.execute.side_effect = fail_first

        _run_sql_loop(
            ctx, mock_conn, "duckdb", [sql_a, sql_b, sql_c], "stop",
            stage_params={"month": "202301", "area": "KR|US", "year": "2023|2024"},
            stage_param_mode="product",
            run_info_path=run_info,
        )

        # run_info.json 확인
        info = json.loads(run_info.read_text())
        tasks = info["tasks"]

        # sql_a (month=202301 파라미터 사용, 1개 조합) → failed
        assert tasks["01_a__month=202301"]["status"] == "failed"

        # sql_b는 area 파라미터 사용 → KR, US 2개 조합이 pending이어야 함
        assert tasks.get("02_b__area=KR", {}).get("status") == "pending", \
            f"02_b__area=KR should be pending, got: {tasks.get('02_b__area=KR')}"
        assert tasks.get("02_b__area=US", {}).get("status") == "pending", \
            f"02_b__area=US should be pending, got: {tasks.get('02_b__area=US')}"

        # sql_c는 year 파라미터 사용 → 2023, 2024 2개 조합이 pending이어야 함
        assert tasks.get("03_c__year=2023", {}).get("status") == "pending", \
            f"03_c__year=2023 should be pending, got: {tasks.get('03_c__year=2023')}"
        assert tasks.get("03_c__year=2024", {}).get("status") == "pending", \
            f"03_c__year=2024 should be pending, got: {tasks.get('03_c__year=2024')}"


# =====================================================================
# BUG-2: on_error=stop 시 현재 파일의 남은 param_set pending 누락
# =====================================================================
class TestBug2_TransformCurrentFileRemainingParams:
    """한 파일에 여러 param_set이 있을 때, 중간에서 실패+stop되면
    남은 param_set도 pending으로 기록되어야 함."""

    def test_remaining_param_sets_marked_pending(self, tmp_path):
        from stages.transform_stage import _run_sql_loop

        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "report.sql"
        sql_file.write_text("SELECT * FROM t WHERE month = '${month}'")

        run_info = tmp_path / "run_info.json"
        init_run_info(run_info, job_name="test", run_id="r1",
                      stage="transform", mode="run")

        ctx = _ctx(params={"month": "202301|202302|202303|202304|202305"})

        # conn mock: 2번째 실행에서 실패 (202302)
        mock_conn = MagicMock()
        exec_count = [0]

        def fail_on_second(*args, **kwargs):
            exec_count[0] += 1
            if exec_count[0] == 2:
                raise RuntimeError("timeout")

        mock_conn.execute.side_effect = fail_on_second

        _run_sql_loop(
            ctx, mock_conn, "duckdb", [sql_file], "stop",
            stage_params={"month": "202301|202302|202303|202304|202305"},
            stage_param_mode="product",
            run_info_path=run_info,
        )

        info = json.loads(run_info.read_text())
        tasks = info["tasks"]

        # 202301: success
        assert tasks["report__month=202301"]["status"] == "success"
        # 202302: failed
        assert tasks["report__month=202302"]["status"] == "failed"
        # 202303~202305: 실행되지 않았으므로 pending이어야 함
        assert tasks.get("report__month=202303", {}).get("status") == "pending", \
            f"202303 should be pending, got: {tasks.get('report__month=202303')}"
        assert tasks.get("report__month=202304", {}).get("status") == "pending", \
            f"202304 should be pending, got: {tasks.get('report__month=202304')}"
        assert tasks.get("report__month=202305", {}).get("status") == "pending", \
            f"202305 should be pending, got: {tasks.get('report__month=202305')}"


# =====================================================================
# BUG-3: report_stage _run_excel_export 예외 삼킴
#   내부에서 exception catch 후 re-raise하지 않아 호출자가 성공으로 간주
# =====================================================================
class TestBug3_ExcelExceptionSwallowed:
    """_run_excel_export가 실패해도 run()에서 report_success로 카운트되는 버그."""

    def test_excel_failure_counted_as_failed(self, tmp_path):
        """Excel 생성 실패 시 report_failed가 증가해야 함."""
        # report_stage의 run()을 직접 호출하기엔 의존성이 많으므로
        # _run_excel_export의 동작을 검증
        import stages.report_stage as rs

        ctx = _ctx(job_config={
            "report": {
                "skip_sql": True,
                "csv_union_dir": str(tmp_path),
                "excel": {"enabled": True, "out_dir": str(tmp_path)},
            }
        })

        # CSV 파일 하나 생성 (skip_sql=true 경로)
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2\n1,2\n")

        # _run_excel_export가 내부에서 exception을 잡는지 확인
        # pandas import 실패를 시뮬레이션
        with patch.dict("sys.modules", {"pandas": None}):
            # _run_excel_export 직접 호출 — 예외를 re-raise하지 않으면
            # 이 함수는 정상 반환됨 (버그!)
            try:
                rs._run_excel_export(ctx, ctx.job_config["report"],
                                     ctx.job_config["report"]["excel"],
                                     [csv_file])
                returned_normally = True
            except Exception:
                returned_normally = False

        # BUG: 현재는 True (예외 삼킴). 수정 후 False여야 함.
        # 수정 검증: 예외가 re-raise 되어야 함
        assert not returned_normally, \
            "_run_excel_export should re-raise exceptions so caller can count as failed"


# =====================================================================
# BUG-4: report_stage _run_csv_export skipped 카운터 누락
# =====================================================================
class TestBug4_ReportCsvSkippedCounter:
    """retry 모드에서 skip된 task의 skipped 카운트가 반환되지 않는 버그."""

    def test_skipped_count_returned(self, tmp_path):
        """_run_csv_export가 (generated, failed, skipped) 튜플을 반환해야 함.
        현재는 (generated, failed)만 반환."""
        # _run_csv_export의 반환값 구조를 검증
        import inspect
        from stages.report_stage import _run_csv_export

        sig = inspect.signature(_run_csv_export)
        # 반환 타입 annotation 확인 (tuple)

        # 실제로 함수를 실행하지 않고, 반환값의 길이를 확인하기 위해
        # mock을 사용하여 간이 실행
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "test.sql"
        sql_file.write_text("SELECT 1")

        run_info = tmp_path / "run_info.json"
        init_run_info(run_info, job_name="test", run_id="r1",
                      stage="report", mode="retry")

        ctx = _ctx(
            job_config={"report": {"source": "target"}, "target": {"type": "duckdb"}},
            mode="retry"
        )

        # mock connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value = MagicMock(
            description=[("col1",)],
            fetchmany=MagicMock(return_value=[])
        )

        report_cfg = {"source": "target"}
        cfg = {"sql_dir": str(sql_dir), "out_dir": str(tmp_path / "out")}

        # task_key = "test" (파라미터 없으므로)
        # failed_task_keys가 빈 set이면 모든 task가 skip됨
        with patch("stages.report_stage._open_connection", return_value=(mock_conn, "duckdb", "test")):
            result = _run_csv_export(
                ctx, report_cfg, cfg,
                stage_params={},
                stage_param_mode="product",
                run_info_path=run_info,
                failed_task_keys={"nonexistent_task"},  # test는 여기 없으므로 skip
            )

        # 수정 후: (generated, failed, skipped) 3-tuple이어야 함
        assert len(result) == 3, \
            f"_run_csv_export should return (generated, failed, skipped), got {len(result)}-tuple"
        generated, failed, skipped = result
        assert skipped == 1, f"Expected 1 skipped, got {skipped}"
