"""
DuckDB batch load 테스트: 동일 테이블 다중 CSV → read_csv_auto([리스트]) 한방 INSERT.
"""

import csv
import json
import logging
from pathlib import Path

import pytest

from engine.context import RunContext

# duckdb optional
duckdb = pytest.importorskip("duckdb")


# ── Helpers ──────────────────────────────────────────────


@pytest.fixture
def work_dir(tmp_path):
    (tmp_path / "data" / "local").mkdir(parents=True)
    (tmp_path / "data" / "export" / "test_job").mkdir(parents=True)
    (tmp_path / "sql" / "export").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def logger():
    log = logging.getLogger("test_duckdb_batch")
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        log.addHandler(logging.StreamHandler())
    return log


def _make_ctx(work_dir, logger, job_config, **kwargs):
    defaults = dict(
        job_name="test_job",
        run_id="test_job_01",
        job_config=job_config,
        env_config={},
        params={},
        work_dir=work_dir,
        mode="run",
        logger=logger,
    )
    defaults.update(kwargs)
    return RunContext(**defaults)


def _write_csv(path: Path, rows: list[list]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


# ── Tests ────────────────────────────────────────────────


class TestDuckDBBatchLoad:
    """DuckDB batch load (replace/truncate 모드) 검증."""

    def test_batch_replace_multiple_csv(self, work_dir, logger):
        """replace 모드: 동일 테이블 2개 CSV가 batch로 한방 적재되는지 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "sales__local__month_202301.csv", [
            ["item", "qty"],
            ["A", "10"],
        ])
        _write_csv(csv_dir / "sales__local__month_202302.csv", [
            ["item", "qty"],
            ["B", "20"],
        ])
        (work_dir / "sql" / "export" / "sales.sql").write_text(
            "SELECT item, qty FROM sales", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.duckdb"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "duckdb", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 2

    def test_batch_truncate_multiple_csv(self, work_dir, logger):
        """truncate 모드: 기존 데이터 삭제 후 다중 CSV batch 적재."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        db_path = work_dir / "data" / "local" / "test.duckdb"

        # 기존 데이터 삽입
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE products (id VARCHAR, name VARCHAR)")
        conn.execute("INSERT INTO products VALUES ('old', 'OldProduct')")
        conn.close()

        _write_csv(csv_dir / "products__local__cat_A.csv", [
            ["id", "name"],
            ["1", "Product1"],
        ])
        _write_csv(csv_dir / "products__local__cat_B.csv", [
            ["id", "name"],
            ["2", "Product2"],
        ])
        (work_dir / "sql" / "export" / "products.sql").write_text(
            "SELECT id, name FROM products", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "duckdb", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "load": {"mode": "truncate"},
        })

        load_run(ctx)

        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        conn.close()
        # old 행 삭제 후 2행만 남아야 함
        assert count == 2

    def test_single_csv_no_batch(self, work_dir, logger):
        """단일 CSV는 batch 미사용, 기존 로직대로 동작."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "orders.csv", [
            ["id", "amount"],
            ["1", "100"],
            ["2", "200"],
        ])

        db_path = work_dir / "data" / "local" / "test.duckdb"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "duckdb", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        conn.close()
        assert count == 2

    def test_batch_with_meta_json(self, work_dir, logger):
        """meta.json이 있을 때 batch load가 올바른 타입으로 테이블 생성."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "metrics__local__period_Q1.csv", [
            ["name", "value"],
            ["cpu", "85"],
        ])
        _write_csv(csv_dir / "metrics__local__period_Q2.csv", [
            ["name", "value"],
            ["mem", "70"],
        ])

        meta = {"columns": [
            {"name": "name", "type": "VARCHAR", "size": 100},
            {"name": "value", "type": "NUMBER", "precision": 10, "scale": 0},
        ]}
        (csv_dir / "metrics__local__period_Q1.meta.json").write_text(
            json.dumps(meta), encoding="utf-8"
        )

        (work_dir / "sql" / "export" / "metrics.sql").write_text(
            "SELECT name, value FROM metrics", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.duckdb"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "duckdb", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        assert count == 2
        # value 컬럼이 BIGINT인지 확인
        col_type = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'metrics' AND column_name = 'value'"
        ).fetchone()[0]
        conn.close()
        assert col_type == "BIGINT"

    def test_batch_history_per_file(self, work_dir, logger):
        """batch 후 _LOAD_HISTORY에 파일별로 기록되는지 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "logs__local__day_01.csv", [
            ["msg"], ["hello"],
        ])
        _write_csv(csv_dir / "logs__local__day_02.csv", [
            ["msg"], ["world"],
        ])
        (work_dir / "sql" / "export" / "logs.sql").write_text(
            "SELECT msg FROM logs", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.duckdb"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "duckdb", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = duckdb.connect(str(db_path))
        hist_count = conn.execute("SELECT COUNT(*) FROM _LOAD_HISTORY").fetchone()[0]
        conn.close()
        assert hist_count == 2
