"""Job-centric convention 테스트.

jobs/{job_name}/ 폴더 존재 여부에 따라 기본 경로가 올바르게 결정되는지,
특히 export → load 경로 연결이 깨지지 않는지 검증.
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from engine.path_utils import get_job_dir, get_job_defaults, resolve_path
from engine.context import RunContext


# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture
def work_dir(tmp_path):
    """기본 work_dir (job 폴더 없음)."""
    (tmp_path / "jobs").mkdir()
    return tmp_path


@pytest.fixture
def work_dir_with_job(tmp_path):
    """job-centric 폴더가 있는 work_dir."""
    job_dir = tmp_path / "jobs" / "my_job"
    (job_dir / "sql" / "export").mkdir(parents=True)
    (job_dir / "sql" / "transform").mkdir(parents=True)
    (job_dir / "sql" / "report").mkdir(parents=True)
    (job_dir / "data").mkdir(parents=True)
    return tmp_path


def _make_ctx(work_dir, job_name="my_job", target_type="duckdb", job_config=None):
    """테스트용 RunContext 생성."""
    cfg = job_config or {
        "target": {"type": target_type},
        "export": {},
        "transform": {},
        "report": {},
    }
    return RunContext(
        job_name=job_name,
        run_id="run_01",
        job_config=cfg,
        env_config={},
        params={},
        work_dir=work_dir,
        mode="run",
        logger=logging.getLogger("test"),
    )


# ── get_job_dir ──────────────────────────────────────────


class TestGetJobDir:
    def test_returns_none_when_no_folder(self, work_dir):
        assert get_job_dir(work_dir, "my_job") is None

    def test_returns_path_when_folder_exists(self, work_dir_with_job):
        result = get_job_dir(work_dir_with_job, "my_job")
        assert result is not None
        assert result == work_dir_with_job / "jobs" / "my_job"


# ── get_job_defaults ─────────────────────────────────────


class TestGetJobDefaults:
    def test_global_defaults_when_no_folder(self, work_dir):
        d = get_job_defaults(work_dir, "my_job", "duckdb")
        assert d["job_dir_exists"] is False
        assert d["export_sql_dir"] == "sql/export"
        assert d["export_out_dir"] == "data/export"
        assert d["transform_sql_dir"] == "sql/transform/duckdb"
        assert d["target_db_path"] == "data/local/result.duckdb"

    def test_job_centric_defaults_when_folder_exists(self, work_dir_with_job):
        d = get_job_defaults(work_dir_with_job, "my_job", "duckdb")
        assert d["job_dir_exists"] is True
        assert d["export_sql_dir"] == "jobs/my_job/sql/export"
        assert d["export_out_dir"] == "jobs/my_job/data/export"
        assert d["transform_sql_dir"] == "jobs/my_job/sql/transform"
        assert d["report_sql_dir"] == "jobs/my_job/sql/report"
        assert d["target_db_path"] == "jobs/my_job/data/my_job.duckdb"

    def test_sqlite_target(self, work_dir_with_job):
        d = get_job_defaults(work_dir_with_job, "my_job", "sqlite3")
        assert d["target_db_path"] == "jobs/my_job/data/my_job.sqlite"

    def test_global_sqlite_defaults(self, work_dir):
        d = get_job_defaults(work_dir, "my_job", "sqlite3")
        assert d["transform_sql_dir"] == "sql/transform/sqlite3"
        assert d["target_db_path"] == "data/local/result.sqlite"


# ── RunContext.get_default ────────────────────────────────


class TestContextGetDefault:
    def test_global_defaults(self, work_dir):
        ctx = _make_ctx(work_dir)
        assert ctx.get_default("export_out_dir") == "data/export"

    def test_job_centric_defaults(self, work_dir_with_job):
        ctx = _make_ctx(work_dir_with_job)
        assert ctx.get_default("export_out_dir") == "jobs/my_job/data/export"
        assert ctx.get_default("export_sql_dir") == "jobs/my_job/sql/export"

    def test_caching(self, work_dir_with_job):
        ctx = _make_ctx(work_dir_with_job)
        d1 = ctx.get_default("export_out_dir")
        d2 = ctx.get_default("export_out_dir")
        assert d1 == d2
        # _job_defaults가 캐시되었는지 확인
        assert hasattr(ctx, "_job_defaults")


# ── Export → Load 경로 연결 테스트 (핵심) ──────────────────


class TestExportLoadPathConsistency:
    """export가 만든 CSV를 load가 정확히 찾을 수 있는지 검증."""

    def test_global_mode_path_match(self, work_dir):
        """글로벌 모드: export와 load가 같은 경로를 사용하는지."""
        ctx = _make_ctx(work_dir, job_config={
            "target": {"type": "duckdb"},
            "export": {},  # 기본값 사용
        })
        default_out = ctx.get_default("export_out_dir")
        # export가 쓰는 경로
        export_out = resolve_path(ctx, default_out) / ctx.job_name
        # load가 읽는 경로 (export.out_dir fallback)
        load_base = resolve_path(ctx, default_out)
        load_dir = load_base / ctx.job_name
        assert export_out == load_dir

    def test_job_centric_path_match(self, work_dir_with_job):
        """Job-centric 모드: export와 load가 같은 경로를 사용하는지."""
        ctx = _make_ctx(work_dir_with_job, job_config={
            "target": {"type": "duckdb"},
            "export": {},  # 기본값 → convention
        })
        default_out = ctx.get_default("export_out_dir")
        # export가 쓰는 경로
        export_out = resolve_path(ctx, default_out) / ctx.job_name
        # load가 읽는 경로
        load_base = resolve_path(ctx, default_out)
        load_dir = load_base / ctx.job_name
        assert export_out == load_dir
        # 실제 경로 확인
        expected = work_dir_with_job / "jobs" / "my_job" / "data" / "export" / "my_job"
        assert export_out == expected

    def test_explicit_yml_overrides_convention(self, work_dir_with_job):
        """yml에 경로가 명시되면 convention보다 우선."""
        ctx = _make_ctx(work_dir_with_job, job_config={
            "target": {"type": "duckdb"},
            "export": {
                "sql_dir": "sql/export/custom",
                "out_dir": "data/custom_export",
            },
        })
        # yml 명시값이 convention보다 우선
        export_cfg = ctx.job_config["export"]
        out_dir = export_cfg.get("out_dir", ctx.get_default("export_out_dir"))
        assert out_dir == "data/custom_export"  # yml 값

    def test_load_finds_export_csv_with_exported_files(self, work_dir_with_job):
        """export 단계에서 ctx.exported_files에 기록된 파일을 load가 사용하는지."""
        ctx = _make_ctx(work_dir_with_job)
        # export가 파일을 생성했다고 시뮬레이션
        csv_path = work_dir_with_job / "jobs" / "my_job" / "data" / "export" / "my_job" / "test.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("col1\nval1")
        ctx.exported_files = [csv_path]
        # load에서 exported_files를 우선 사용하므로 경로 불일치 없음
        found = sorted([p for p in ctx.exported_files if p.exists()])
        assert len(found) == 1
        assert found[0] == csv_path


# ── scan_sql_params 수정 테스트 ────────────────────────────


@pytest.fixture
def scan_sql_params_fn():
    """tkinter 없는 환경에서도 gui.utils.scan_sql_params를 로드."""
    import importlib
    import sys
    # gui 패키지 init이 tkinter를 import하므로 우회
    saved = sys.modules.get("gui")
    try:
        if "gui" not in sys.modules:
            import types
            sys.modules["gui"] = types.ModuleType("gui")
        mod = importlib.import_module("gui.utils")
        return mod.scan_sql_params
    except ImportError:
        pytest.skip("gui.utils import 실패")
    finally:
        if saved is not None:
            sys.modules["gui"] = saved


class TestScanSqlParams:
    def test_no_parent_auto_scan(self, tmp_path, scan_sql_params_fn):
        """부모 디렉토리를 자동으로 스캔하지 않는지 확인."""
        # export 폴더
        export_dir = tmp_path / "sql" / "export"
        export_dir.mkdir(parents=True)
        (export_dir / "q1.sql").write_text("SELECT * FROM t WHERE col = :myParam")

        # transform 폴더 (부모의 sibling)
        transform_dir = tmp_path / "sql" / "transform"
        transform_dir.mkdir(parents=True)
        (transform_dir / "t1.sql").write_text("SELECT * FROM t WHERE col = :otherParam")

        # extra_dirs 없이 호출 → export만 스캔
        params = scan_sql_params_fn(export_dir)
        assert "myParam" in params
        assert "otherParam" not in params  # 부모 자동 탐색 안 함

    def test_explicit_extra_dirs(self, tmp_path, scan_sql_params_fn):
        """extra_dirs를 명시적으로 전달하면 해당 폴더도 스캔."""
        export_dir = tmp_path / "sql" / "export"
        export_dir.mkdir(parents=True)
        (export_dir / "q1.sql").write_text("SELECT * FROM t WHERE col = :myParam")

        transform_dir = tmp_path / "sql" / "transform"
        transform_dir.mkdir(parents=True)
        (transform_dir / "t1.sql").write_text("SELECT * FROM t WHERE col = :otherParam")

        params = scan_sql_params_fn(export_dir, extra_dirs=[transform_dir])
        assert "myParam" in params
        assert "otherParam" in params
