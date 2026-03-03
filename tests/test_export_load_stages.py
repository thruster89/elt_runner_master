"""export_stage / load_stage 유닛 테스트 (DB 연결 불필요 영역)."""
import json
import logging
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

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
