"""export_stage / load_stage 유닛 테스트 (DB 연결 불필요 영역)."""
import json
import logging
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest

from engine.context import RunContext
from stages.export_stage import (
    build_csv_name,
    build_log_prefix,
    backup_existing_file,
    sanitize_sql,
    load_failed_tasks,
    run_plan,
    _cleanup_alt_ext,
)
from stages.load_stage import (
    _extract_params,
    _sha256_file,
    _human_size,
    _collect_csv_info,
    _run_load_loop,
)


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
# export_stage — build_csv_name
# =====================================================================
class TestBuildCsvName:

    def test_basic(self):
        result = build_csv_name("report", "host1", {"ym": "202301"}, "csv")
        assert result == "report__host1__ym_202301.csv"

    def test_no_host(self):
        result = build_csv_name("report", "", {}, "csv")
        assert result == "report.csv"

    def test_compact_style(self):
        result = build_csv_name("report", "h", {"ym": "202301"}, "csv",
                                name_style="compact")
        assert result == "report__h__202301.csv"

    def test_strip_prefix(self):
        result = build_csv_name("01_contract", "h", {}, "csv", strip_prefix=True)
        assert result == "contract__h.csv"

    def test_no_strip_prefix(self):
        result = build_csv_name("01_contract", "h", {}, "csv", strip_prefix=False)
        assert result == "01_contract__h.csv"

    def test_gzip_ext(self):
        result = build_csv_name("tbl", "h", {}, "csv.gz")
        assert result == "tbl__h.csv.gz"

    def test_params_sorted(self):
        result = build_csv_name("tbl", "", {"b": "2", "a": "1"}, "csv")
        assert result == "tbl__a_1__b_2.csv"

    def test_space_in_param_replaced(self):
        result = build_csv_name("tbl", "", {"k": "hello world"}, "csv")
        assert "hello_world" in result


# =====================================================================
# export_stage — build_log_prefix
# =====================================================================
class TestBuildLogPrefix:

    def test_no_params(self, tmp_path):
        sql = tmp_path / "report.sql"
        sql.touch()
        assert build_log_prefix(sql, {}) == "[report]"

    def test_with_params(self, tmp_path):
        sql = tmp_path / "report.sql"
        sql.touch()
        result = build_log_prefix(sql, {"ym": "202301", "code": "A"})
        assert result == "[report|code=A ym=202301]"


# =====================================================================
# export_stage — sanitize_sql
# =====================================================================
class TestSanitizeSql:

    def test_strip_semicolon(self):
        assert sanitize_sql("SELECT 1;") == "SELECT 1"

    def test_strip_slash(self):
        assert sanitize_sql("SELECT 1/") == "SELECT 1"

    def test_strip_multiple(self):
        assert sanitize_sql("SELECT 1; /; ") == "SELECT 1"

    def test_strip_whitespace(self):
        assert sanitize_sql("  SELECT 1  ") == "SELECT 1"

    def test_no_trailing(self):
        assert sanitize_sql("SELECT 1") == "SELECT 1"


# =====================================================================
# export_stage — backup_existing_file
# =====================================================================
class TestBackupExistingFile:

    def test_creates_backup(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("hello")
        backup_dir = tmp_path / "_backup"

        backup_existing_file(f, backup_dir, keep=5)

        assert not f.exists()
        assert backup_dir.exists()
        backups = list(backup_dir.glob("data__*.csv"))
        assert len(backups) == 1

    def test_no_file_noop(self, tmp_path):
        f = tmp_path / "nonexistent.csv"
        backup_dir = tmp_path / "_backup"
        backup_existing_file(f, backup_dir)
        assert not backup_dir.exists()

    def test_keep_limit(self, tmp_path):
        backup_dir = tmp_path / "_backup"
        backup_dir.mkdir()
        # 3개의 기존 백업 생성
        for i in range(3):
            (backup_dir / f"data__2024010{i}_120000.csv").write_text(f"v{i}")
            time.sleep(0.01)

        f = tmp_path / "data.csv"
        f.write_text("new")

        backup_existing_file(f, backup_dir, keep=2)
        backups = list(backup_dir.glob("data__*.csv"))
        assert len(backups) == 2


# =====================================================================
# export_stage — _cleanup_alt_ext
# =====================================================================
class TestCleanupAltExt:

    def test_csv_removes_gz_orphan(self, tmp_path):
        csv = tmp_path / "report__host.csv"
        csv.write_text("data")
        gz_orphan = tmp_path / "report__host.csv.gz"
        gz_orphan.write_text("old")
        backup_dir = tmp_path / "_backup"

        logger = logging.getLogger("test")
        _cleanup_alt_ext(csv, backup_dir, 5, logger)

        assert not gz_orphan.exists()
        assert len(list(backup_dir.glob("*"))) == 1

    def test_gz_removes_csv_orphan(self, tmp_path):
        gz = tmp_path / "report__host.csv.gz"
        gz.write_text("data")
        csv_orphan = tmp_path / "report__host.csv"
        csv_orphan.write_text("old")
        backup_dir = tmp_path / "_backup"

        logger = logging.getLogger("test")
        _cleanup_alt_ext(gz, backup_dir, 5, logger)

        assert not csv_orphan.exists()

    def test_no_orphan_noop(self, tmp_path):
        csv = tmp_path / "report__host.csv"
        csv.write_text("data")
        backup_dir = tmp_path / "_backup"

        logger = logging.getLogger("test")
        _cleanup_alt_ext(csv, backup_dir, 5, logger)

        assert not backup_dir.exists()


# =====================================================================
# export_stage — load_failed_tasks (위임 확인)
# =====================================================================
class TestExportLoadFailedTasks:

    def test_delegates_to_task_tracking(self, tmp_path):
        """export_stage.load_failed_tasks가 task_tracking에 올바르게 위임하는지."""
        base = tmp_path / "data" / "export"
        job_dir = base / "test_job" / "prev_run"
        job_dir.mkdir(parents=True)
        (job_dir / "run_info.json").write_text(json.dumps({
            "run_id": "prev_run",
            "stage": "export",
            "tasks": {
                "a": {"status": "failed"},
                "b": {"status": "success"},
                "c": {"status": "pending"},
            },
        }))

        ctx = _ctx(tmp_path, mode="retry")
        ctx.run_id = "run_01"
        export_cfg = {"out_dir": str(base)}

        with patch("stages.export_stage.resolve_path", return_value=base):
            result = load_failed_tasks(ctx, export_cfg)

        assert result == {"a", "c"}

    def test_no_previous_run_returns_none(self, tmp_path):
        base = tmp_path / "data" / "export"
        base.mkdir(parents=True)

        ctx = _ctx(tmp_path, mode="retry")
        export_cfg = {"out_dir": str(base)}

        with patch("stages.export_stage.resolve_path", return_value=base):
            result = load_failed_tasks(ctx, export_cfg)

        assert result is None


# =====================================================================
# export_stage — run_plan (dryrun)
# =====================================================================
class TestRunPlan:

    def test_generates_plan_files(self, tmp_path):
        """run_plan이 JSON + TXT plan 파일을 생성하는지."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "01_report.sql"
        sql_file.write_text("SELECT :ym AS ym FROM dual")

        export_base = tmp_path / "export"
        export_base.mkdir()
        out_dir = export_base / "test_job"
        out_dir.mkdir()

        ctx = _ctx(tmp_path, job_config={
            "source": {"type": "oracle", "host": "myhost"},
            "export": {"sql_dir": str(sql_dir), "out_dir": str(export_base)},
        }, mode="plan")

        export_cfg = ctx.job_config["export"]

        # resolve_path가 export_base를 반환하도록 mock
        with patch("stages.export_stage.resolve_path", return_value=export_base):
            run_plan(
                ctx, [sql_file], export_cfg, out_dir, "csv",
                name_style="full", strip_prefix=False,
                stage_params={"ym": "202301"},
                stage_param_mode="product",
            )

        report_dir = export_base / "test_job" / "run_01"
        assert (report_dir / "plan_report.json").exists()
        assert (report_dir / "plan_report.txt").exists()

        report = json.loads((report_dir / "plan_report.json").read_text())
        assert report["total_tasks"] == 1
        assert report["tasks"][0]["params"] == {"ym": "202301"}


# =====================================================================
# load_stage — _extract_params
# =====================================================================
class TestExtractParams:

    def test_from_meta_json(self, tmp_path):
        csv = tmp_path / "report__host__ym_202301.csv"
        csv.write_text("a,b\n1,2\n")
        meta = tmp_path / "report__host__ym_202301.meta.json"
        meta.write_text(json.dumps({"params": {"ym": "202301"}}))

        result = _extract_params(csv)
        assert result == {"ym": "202301"}

    def test_fallback_to_filename(self, tmp_path):
        csv = tmp_path / "report__host__ym_202301.csv"
        csv.write_text("a,b\n1,2\n")
        # no meta.json
        result = _extract_params(csv)
        assert result.get("ym") == "202301"

    def test_csv_gz(self, tmp_path):
        csv = tmp_path / "tbl__host__code_A.csv.gz"
        csv.write_text("dummy")
        meta = tmp_path / "tbl__host__code_A.meta.json"
        meta.write_text(json.dumps({"params": {"code": "A"}}))

        result = _extract_params(csv)
        assert result == {"code": "A"}


# =====================================================================
# load_stage — _sha256_file
# =====================================================================
class TestSha256File:

    def test_consistent_hash(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("hello world")
        h1 = _sha256_file(f)
        h2 = _sha256_file(f)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest

    def test_different_content_different_hash(self, tmp_path):
        f1 = tmp_path / "a.csv"
        f1.write_text("aaa")
        f2 = tmp_path / "b.csv"
        f2.write_text("bbb")
        assert _sha256_file(f1) != _sha256_file(f2)


# =====================================================================
# load_stage — _human_size
# =====================================================================
class TestHumanSize:

    def test_bytes(self):
        assert _human_size(500) == "500.0B"

    def test_kilobytes(self):
        assert _human_size(2048) == "2.0KB"

    def test_megabytes(self):
        assert _human_size(5 * 1024 * 1024) == "5.0MB"

    def test_gigabytes(self):
        assert _human_size(3 * 1024 ** 3) == "3.0GB"


# =====================================================================
# load_stage — _collect_csv_info
# =====================================================================
class TestCollectCsvInfo:

    def test_basic(self, tmp_path):
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "report.sql"
        sql_file.write_text("--[MY_TABLE]\nSELECT 1")

        csv = tmp_path / "report.csv"
        csv.write_text("col1\nval1\n")

        sql_map = {"report": sql_file}
        items = _collect_csv_info([csv], sql_map)

        assert len(items) == 1
        assert items[0]["table"] == "MY_TABLE"
        assert items[0]["sql_found"] is True
        assert items[0]["size"] > 0

    def test_no_sql_mapping(self, tmp_path):
        csv = tmp_path / "unknown.csv"
        csv.write_text("col1\nval1\n")

        items = _collect_csv_info([csv], {})
        assert items[0]["sql_found"] is False
        assert items[0]["table"] == "unknown"

    def test_multiple_files(self, tmp_path):
        files = []
        for name in ("a.csv", "b.csv", "c.csv"):
            f = tmp_path / name
            f.write_text("data")
            files.append(f)

        items = _collect_csv_info(files, {})
        assert len(items) == 3


# =====================================================================
# export_stage.run() — 통합 테스트
# =====================================================================
def _setup_export_env(tmp_path, params=None, mode="run",
                      extra_export_cfg=None, sql_contents=None):
    """export run() 테스트용 공통 환경 구성."""
    sql_dir = tmp_path / "sql" / "export"
    sql_dir.mkdir(parents=True)

    if sql_contents is None:
        sql_contents = {"01_report.sql": "SELECT :ym AS ym FROM dual"}
    for name, content in sql_contents.items():
        (sql_dir / name).write_text(content)

    export_cfg = {
        "sql_dir": str(sql_dir),
        "out_dir": str(tmp_path / "data" / "export"),
        "parallel_workers": 1,
    }
    if extra_export_cfg:
        export_cfg.update(extra_export_cfg)

    job_config = {
        "source": {"type": "oracle", "host": "testhost"},
        "export": export_cfg,
    }

    ctx = RunContext(
        job_name="test_job",
        run_id="run_01",
        job_config=job_config,
        env_config={"sources": {"oracle": {"hosts": {"testhost": {}}}}},
        params=params or {"ym": "202301"},
        work_dir=tmp_path,
        mode=mode,
        logger=logging.getLogger("test_export"),
    )

    # run_info.json 위치를 미리 만들어 task_tracking이 기록할 수 있도록
    run_info_dir = tmp_path / "data" / "export" / "test_job" / "run_01"
    run_info_dir.mkdir(parents=True, exist_ok=True)
    run_info_path = run_info_dir / "run_info.json"
    run_info_path.write_text(json.dumps({"tasks": {}}))

    return ctx, sql_dir, run_info_path


def _mock_export_func(conn, sql_text, out_file, logger, **kwargs):
    """mock export_sql_to_csv: 파일 생성 + 행 수 반환."""
    Path(out_file).write_text("col1\nval1\n")
    return 1


class TestExportStageRun:
    """export_stage.run() 통합 테스트 (DB mock)."""

    @patch("stages.export_stage._close_all_connections")
    @patch("stages.export_stage.get_thread_connection")
    def test_normal_run_success(self, mock_conn, mock_close, tmp_path):
        """정상 run: SQL 1개, 파라미터 1세트 → success 1."""
        ctx, sql_dir, run_info_path = _setup_export_env(tmp_path)
        mock_conn.return_value = MagicMock()

        with patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_mock_export_func, create=True):
            import stages.export_stage as es
            es.run(ctx)

        info = json.loads(run_info_path.read_text())
        statuses = {v["status"] for v in info["tasks"].values()}
        assert "success" in statuses
        assert ctx.stage_results["export"]["success"] >= 1

    @patch("stages.export_stage._close_all_connections")
    @patch("stages.export_stage.get_thread_connection")
    def test_multiple_params_product(self, mock_conn, mock_close, tmp_path):
        """product 모드로 2개 파라미터 조합 → 2개 task."""
        ctx, sql_dir, run_info_path = _setup_export_env(
            tmp_path, params={"ym": "202301|202302"}
        )
        mock_conn.return_value = MagicMock()

        with patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_mock_export_func, create=True):
            import stages.export_stage as es
            es.run(ctx)

        info = json.loads(run_info_path.read_text())
        assert len(info["tasks"]) == 2
        assert ctx.stage_results["export"]["success"] == 2

    @patch("stages.export_stage._close_all_connections")
    @patch("stages.export_stage.get_thread_connection")
    def test_skip_existing_no_overwrite(self, mock_conn, mock_close, tmp_path):
        """overwrite=False일 때 기존 파일 → skipped."""
        ctx, sql_dir, run_info_path = _setup_export_env(tmp_path)
        mock_conn.return_value = MagicMock()

        # 기존 CSV 미리 생성
        out_dir = tmp_path / "data" / "export" / "test_job"
        out_dir.mkdir(parents=True, exist_ok=True)
        existing_csv = out_dir / "01_report__testhost__ym_202301.csv"
        existing_csv.write_text("old data")

        with patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_mock_export_func, create=True):
            import stages.export_stage as es
            es.run(ctx)

        info = json.loads(run_info_path.read_text())
        statuses = [v["status"] for v in info["tasks"].values()]
        assert "skipped" in statuses

    @patch("stages.export_stage._close_all_connections")
    @patch("stages.export_stage.get_thread_connection")
    def test_export_failure_records_failed(self, mock_conn, mock_close, tmp_path):
        """export 실패 시 failed 상태 기록."""
        ctx, sql_dir, run_info_path = _setup_export_env(tmp_path)
        mock_conn.return_value = MagicMock()

        def _failing_export(*args, **kwargs):
            raise RuntimeError("DB connection lost")

        with patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_failing_export, create=True):
            import stages.export_stage as es
            es.run(ctx)

        info = json.loads(run_info_path.read_text())
        failed_tasks = [k for k, v in info["tasks"].items() if v["status"] == "failed"]
        assert len(failed_tasks) == 1
        assert ctx.stage_results["export"]["failed"] == 1

    @patch("stages.export_stage._close_all_connections")
    @patch("stages.export_stage.get_thread_connection")
    def test_retry_skips_succeeded(self, mock_conn, mock_close, tmp_path):
        """retry 모드: 이전 run에서 success인 task는 건너뜀."""
        # 이전 run 기록 생성
        prev_dir = tmp_path / "data" / "export" / "test_job" / "prev_run"
        prev_dir.mkdir(parents=True)
        (prev_dir / "run_info.json").write_text(json.dumps({
            "run_id": "prev_run",
            "stage": "export",
            "tasks": {
                "01_report__ym=202301": {"status": "success"},
                "01_report__ym=202302": {"status": "failed"},
            },
        }))

        ctx, sql_dir, run_info_path = _setup_export_env(
            tmp_path, params={"ym": "202301|202302"}, mode="retry",
        )
        mock_conn.return_value = MagicMock()

        export_call_count = 0

        def _counting_export(*args, **kwargs):
            nonlocal export_call_count
            export_call_count += 1
            return _mock_export_func(*args, **kwargs)

        with patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_counting_export, create=True):
            import stages.export_stage as es
            es.run(ctx)

        # 202301은 skip, 202302만 재실행 → export 1회
        assert export_call_count == 1

    def test_no_export_config_skips(self, tmp_path):
        """export 설정 없으면 조기 리턴."""
        ctx = RunContext(
            job_name="test_job", run_id="run_01",
            job_config={},  # no export key
            env_config={}, params={}, work_dir=tmp_path,
            mode="run", logger=logging.getLogger("test"),
        )
        import stages.export_stage as es
        es.run(ctx)  # 예외 없이 리턴

    def test_no_sql_files_skips(self, tmp_path):
        """SQL 디렉토리에 .sql 없으면 조기 리턴."""
        sql_dir = tmp_path / "empty_sql"
        sql_dir.mkdir()
        ctx = RunContext(
            job_name="test_job", run_id="run_01",
            job_config={
                "source": {"type": "oracle", "host": "h"},
                "export": {"sql_dir": str(sql_dir), "out_dir": str(tmp_path / "out")},
            },
            env_config={}, params={}, work_dir=tmp_path,
            mode="run", logger=logging.getLogger("test"),
        )
        import stages.export_stage as es
        es.run(ctx)  # 예외 없이 리턴

    def test_plan_mode_delegates(self, tmp_path):
        """plan 모드면 run_plan으로 위임되고 DB 접근 없이 종료."""
        ctx, sql_dir, run_info_path = _setup_export_env(tmp_path, mode="plan")

        import stages.export_stage as es
        es.run(ctx)

        # plan_report.json이 생성되었는지 확인
        report_dir = tmp_path / "data" / "export" / "test_job" / "run_01"
        assert (report_dir / "plan_report.json").exists()

    def test_include_filter(self, tmp_path):
        """--include 필터가 SQL 파일을 걸러내는지."""
        ctx, sql_dir, run_info_path = _setup_export_env(
            tmp_path,
            sql_contents={
                "01_report.sql": "SELECT :ym FROM dual",
                "02_summary.sql": "SELECT :ym FROM dual",
            },
        )
        ctx.include_patterns = ["summary"]
        mock_conn = MagicMock()

        with patch("stages.export_stage._close_all_connections"), \
             patch("stages.export_stage.get_thread_connection", return_value=mock_conn), \
             patch("stages.export_stage.init_oracle_client"), \
             patch("adapters.sources.oracle_source.export_sql_to_csv",
                   side_effect=_mock_export_func, create=True):
            import stages.export_stage as es
            es.run(ctx)

        info = json.loads(run_info_path.read_text())
        # summary만 실행, report는 제외
        task_keys = list(info["tasks"].keys())
        assert all("summary" in k or "02_summary" in k for k in task_keys)
        assert len(task_keys) == 1


# =====================================================================
# export_stage — run_plan 추가 케이스
# =====================================================================
class TestRunPlanExtended:

    def test_multiple_sql_files(self, tmp_path):
        """복수 SQL 파일에 대한 plan report."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_a.sql").write_text("SELECT :ym FROM dual")
        (sql_dir / "02_b.sql").write_text("SELECT :ym FROM dual")

        export_base = tmp_path / "export"
        export_base.mkdir()
        out_dir = export_base / "test_job"
        out_dir.mkdir()

        ctx = _ctx(tmp_path, job_config={
            "source": {"type": "oracle", "host": "h"},
            "export": {"sql_dir": str(sql_dir), "out_dir": str(export_base)},
        }, mode="plan")

        sql_files = sorted(sql_dir.glob("*.sql"))

        with patch("stages.export_stage.resolve_path", return_value=export_base):
            run_plan(ctx, sql_files, ctx.job_config["export"], out_dir, "csv",
                     stage_params={"ym": "202301|202302"},
                     stage_param_mode="product")

        report_dir = export_base / "test_job" / "run_01"
        report = json.loads((report_dir / "plan_report.json").read_text())
        # 2 SQL × 2 params = 4 tasks
        assert report["total_tasks"] == 4

    def test_warning_on_unresolved_param(self, tmp_path):
        """치환 안 된 파라미터가 있으면 warning 포함."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_a.sql").write_text("SELECT :ym, :missing_param FROM dual")

        export_base = tmp_path / "export"
        export_base.mkdir()
        out_dir = export_base / "test_job"
        out_dir.mkdir()

        ctx = _ctx(tmp_path, job_config={
            "source": {"type": "oracle", "host": "h"},
            "export": {"sql_dir": str(sql_dir), "out_dir": str(export_base)},
        }, mode="plan")

        with patch("stages.export_stage.resolve_path", return_value=export_base):
            run_plan(ctx, [sql_dir / "01_a.sql"], ctx.job_config["export"],
                     out_dir, "csv",
                     stage_params={"ym": "202301"},
                     stage_param_mode="product")

        report_dir = export_base / "test_job" / "run_01"
        report = json.loads((report_dir / "plan_report.json").read_text())
        assert report["warning_count"] >= 1
        assert any("unresolved" in w for w in report["tasks"][0]["warnings"])

    def test_no_params_single_task(self, tmp_path):
        """파라미터 없는 SQL → task 1개."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_simple.sql").write_text("SELECT 1 FROM dual")

        export_base = tmp_path / "export"
        export_base.mkdir()
        out_dir = export_base / "test_job"
        out_dir.mkdir()

        ctx = _ctx(tmp_path, job_config={
            "source": {"type": "oracle", "host": "h"},
            "export": {"sql_dir": str(sql_dir), "out_dir": str(export_base)},
        }, mode="plan")

        with patch("stages.export_stage.resolve_path", return_value=export_base):
            run_plan(ctx, [sql_dir / "01_simple.sql"], ctx.job_config["export"],
                     out_dir, "csv", stage_params={}, stage_param_mode="product")

        report_dir = export_base / "test_job" / "run_01"
        report = json.loads((report_dir / "plan_report.json").read_text())
        assert report["total_tasks"] == 1
        assert report["tasks"][0]["params"] == {}


# =====================================================================
# load_stage — _run_load_loop 통합 테스트
# =====================================================================
class TestRunLoadLoop:
    """_run_load_loop 단위 테스트 (DB adapter mock)."""

    def test_normal_load(self, tmp_path):
        """CSV 2개 정상 로드."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "tbl_a.sql").write_text("--[TABLE_A]\nSELECT 1")
        (sql_dir / "tbl_b.sql").write_text("--[TABLE_B]\nSELECT 1")
        sql_map = {
            "tbl_a": sql_dir / "tbl_a.sql",
            "tbl_b": sql_dir / "tbl_b.sql",
        }

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        (csv_dir / "tbl_a.csv").write_text("col\n1\n")
        (csv_dir / "tbl_b.csv").write_text("col\n2\n")
        csv_files = sorted(csv_dir.glob("*.csv"))

        ctx = _ctx(tmp_path)
        logger = ctx.logger
        load_fn = MagicMock(return_value=1)

        _run_load_loop(ctx, logger, csv_files, sql_map, "duckdb", load_fn=load_fn)

        assert load_fn.call_count == 2
        assert ctx.stage_results["load"]["success"] == 2
        assert ctx.stage_results["load"]["failed"] == 0

    def test_load_skip(self, tmp_path):
        """load_fn이 -1 반환 시 skipped 처리."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        (csv_dir / "tbl.csv").write_text("col\n1\n")

        ctx = _ctx(tmp_path)
        load_fn = MagicMock(return_value=-1)

        _run_load_loop(ctx, ctx.logger, [csv_dir / "tbl.csv"], {}, "duckdb",
                       load_fn=load_fn)

        assert ctx.stage_results["load"]["skipped"] == 1

    def test_load_failure(self, tmp_path):
        """load_fn 예외 시 failed 처리."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        (csv_dir / "tbl.csv").write_text("col\n1\n")

        ctx = _ctx(tmp_path)
        load_fn = MagicMock(side_effect=RuntimeError("insert error"))

        _run_load_loop(ctx, ctx.logger, [csv_dir / "tbl.csv"], {}, "duckdb",
                       load_fn=load_fn)

        assert ctx.stage_results["load"]["failed"] == 1
        assert ctx.stage_results["load"]["success"] == 0

    def test_mixed_results(self, tmp_path):
        """성공/실패/스킵 혼합."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "ok.sql").write_text("--[OK]\nSELECT 1")

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        for name in ("ok.csv", "skip.csv", "fail.csv"):
            (csv_dir / name).write_text("col\n1\n")
        csv_files = sorted(csv_dir.glob("*.csv"))

        call_idx = {"n": 0}
        responses = [RuntimeError("boom"), 1, -1]  # fail, ok, skip

        def _side_effect(*args, **kwargs):
            r = responses[call_idx["n"]]
            call_idx["n"] += 1
            if isinstance(r, Exception):
                raise r
            return r

        ctx = _ctx(tmp_path)
        _run_load_loop(ctx, ctx.logger, csv_files,
                       {"ok": sql_dir / "ok.sql"}, "duckdb",
                       load_fn=MagicMock(side_effect=_side_effect))

        assert ctx.stage_results["load"]["success"] == 1
        assert ctx.stage_results["load"]["failed"] == 1
        assert ctx.stage_results["load"]["skipped"] == 1


# =====================================================================
# load_stage.run() — 통합 테스트
# =====================================================================
class TestLoadStageRun:
    """load_stage.run() 통합 테스트."""

    def test_no_target_config_skips(self, tmp_path):
        """target 설정 없으면 조기 리턴."""
        ctx = _ctx(tmp_path, job_config={})
        from stages import load_stage
        load_stage.run(ctx)  # 예외 없이 리턴

    def test_no_csv_dir_skips(self, tmp_path):
        """csv_dir/export 설정 모두 없으면 조기 리턴."""
        ctx = _ctx(tmp_path, job_config={
            "target": {"type": "duckdb"},
        })
        from stages import load_stage
        load_stage.run(ctx)  # 예외 없이 리턴

    def test_plan_mode_no_csv(self, tmp_path):
        """plan 모드에서 CSV 없으면 안내 로그만 출력."""
        csv_dir = tmp_path / "data" / "export" / "test_job"
        csv_dir.mkdir(parents=True)
        sql_dir = tmp_path / "sql" / "export"
        sql_dir.mkdir(parents=True)

        ctx = _ctx(tmp_path, job_config={
            "target": {"type": "duckdb"},
            "export": {"out_dir": "data/export", "sql_dir": "sql/export"},
        }, mode="plan")

        from stages import load_stage
        load_stage.run(ctx)  # 예외 없이 리턴

    def test_plan_mode_with_csv(self, tmp_path):
        """plan 모드에서 CSV 있으면 리포트 출력 (DB 접근 없음)."""
        csv_dir = tmp_path / "data" / "export" / "test_job"
        csv_dir.mkdir(parents=True)
        (csv_dir / "report.csv").write_text("col\nval\n")
        sql_dir = tmp_path / "sql" / "export"
        sql_dir.mkdir(parents=True)
        (sql_dir / "report.sql").write_text("--[RPT_TABLE]\nSELECT 1")

        ctx = _ctx(tmp_path, job_config={
            "target": {"type": "duckdb"},
            "export": {"out_dir": "data/export", "sql_dir": "sql/export"},
        }, mode="plan")

        from stages import load_stage
        load_stage.run(ctx)  # DB 접근 없이 완료

    def test_duckdb_run(self, tmp_path):
        """DuckDB 타겟 정상 run 흐름."""
        csv_dir = tmp_path / "data" / "export" / "test_job"
        csv_dir.mkdir(parents=True)
        (csv_dir / "tbl.csv").write_text("col1\nval1\n")
        sql_dir = tmp_path / "sql" / "export"
        sql_dir.mkdir(parents=True)

        ctx = _ctx(tmp_path, job_config={
            "target": {"type": "duckdb", "db_path": "data/result.duckdb"},
            "export": {"out_dir": "data/export", "sql_dir": "sql/export"},
        })

        mock_conn = MagicMock()
        mock_load_csv = MagicMock(return_value=1)

        with patch("stages.load_stage.connect_target",
                   return_value=(mock_conn, "duckdb", "duckdb (test)")), \
             patch("adapters.targets.duckdb_target.load_csv",
                   mock_load_csv, create=True), \
             patch("adapters.targets.duckdb_target._ensure_schema", create=True), \
             patch("adapters.targets.duckdb_target._ensure_history", create=True):
            from stages import load_stage
            load_stage.run(ctx)

        mock_conn.close.assert_called_once()
        assert ctx.stage_results["load"]["success"] == 1

    def test_include_filter(self, tmp_path):
        """--include 필터가 CSV를 걸러내는지."""
        csv_dir = tmp_path / "data" / "export" / "test_job"
        csv_dir.mkdir(parents=True)
        (csv_dir / "report.csv").write_text("col\n1\n")
        (csv_dir / "summary.csv").write_text("col\n1\n")
        sql_dir = tmp_path / "sql" / "export"
        sql_dir.mkdir(parents=True)

        ctx = _ctx(tmp_path, job_config={
            "target": {"type": "duckdb"},
            "export": {"out_dir": "data/export", "sql_dir": "sql/export"},
        })
        ctx.include_patterns = ["summary"]

        mock_conn = MagicMock()
        mock_load_csv = MagicMock(return_value=1)

        with patch("stages.load_stage.connect_target",
                   return_value=(mock_conn, "duckdb", "duckdb (test)")), \
             patch("adapters.targets.duckdb_target.load_csv",
                   mock_load_csv, create=True), \
             patch("adapters.targets.duckdb_target._ensure_schema", create=True), \
             patch("adapters.targets.duckdb_target._ensure_history", create=True):
            from stages import load_stage
            load_stage.run(ctx)

        # summary만 로드, report는 제외
        assert ctx.stage_results["load"]["success"] == 1
        assert mock_load_csv.call_count == 1
