"""
통합 테스트: SQLite3 기반 Load → Transform → Report 파이프라인 E2E.

외부 DB(Oracle/Vertica) 없이 파일 기반 SQLite3로 전체 파이프라인을 검증한다.
Export 스테이지는 소스 DB가 필요하므로 CSV를 직접 생성하여 Load부터 시작.
"""

import csv
import gzip
import json
import logging
import sqlite3
from pathlib import Path

import pytest

from engine.context import RunContext


# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture
def work_dir(tmp_path):
    """작업 디렉토리 구조 생성."""
    (tmp_path / "data" / "local").mkdir(parents=True)
    (tmp_path / "data" / "export" / "test_job").mkdir(parents=True)
    (tmp_path / "data" / "report").mkdir(parents=True)
    (tmp_path / "sql" / "export").mkdir(parents=True)
    (tmp_path / "sql" / "transform").mkdir(parents=True)
    (tmp_path / "sql" / "report").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def logger():
    log = logging.getLogger("test_integration")
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        log.addHandler(logging.StreamHandler())
    return log


def _make_ctx(work_dir, logger, job_config, **kwargs):
    """RunContext 헬퍼."""
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
    """CSV 파일 작성 헬퍼. rows[0]은 헤더."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def _write_csv_gz(path: Path, rows: list[list]):
    """gzip CSV 파일 작성 헬퍼."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


# ── Load 스테이지 통합 테스트 ──────────────────────────────


class TestLoadStage:
    """CSV → SQLite 적재 통합 테스트."""

    def test_load_basic_csv(self, work_dir, logger):
        """기본 CSV 파일이 SQLite 테이블로 적재되는지 검증."""
        from stages.load_stage import run as load_run

        # CSV 생성
        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "users.csv", [
            ["id", "name", "age"],
            ["1", "Alice", "30"],
            ["2", "Bob", "25"],
            ["3", "Charlie", "35"],
        ])

        # SQL 파일 (테이블명 매핑용)
        (work_dir / "sql" / "export" / "users.sql").write_text(
            "SELECT id, name, age FROM users", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        # 검증
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT name FROM users WHERE id='1'")
        assert cur.fetchone()[0] == "Alice"
        conn.close()

        assert ctx.stage_results["load"]["success"] == 1
        assert ctx.stage_results["load"]["failed"] == 0

    def test_load_gzip_csv(self, work_dir, logger):
        """gzip 압축 CSV 파일 적재 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv_gz(csv_dir / "orders.csv.gz", [
            ["order_id", "amount"],
            ["100", "500"],
            ["101", "750"],
        ])

        (work_dir / "sql" / "export" / "orders.sql").write_text(
            "SELECT order_id, amount FROM orders", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 2
        conn.close()

    def test_load_multiple_csv_same_table(self, work_dir, logger):
        """같은 테이블에 여러 CSV → 첫 번째는 replace, 이후 append 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        # 파일명 규칙: {sqlname}__{host}__{param}_{value}.csv
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

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })

        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sales")
        # 첫 번째 replace로 1행, 두 번째 append로 1행 추가 = 2행
        assert cur.fetchone()[0] == 2
        conn.close()

    def test_load_replace_mode(self, work_dir, logger):
        """replace 모드: 기존 데이터 삭제 후 재적재."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        db_path = work_dir / "data" / "local" / "test.sqlite"

        # 기존 데이터 삽입
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE products (id TEXT, name TEXT)")
        conn.execute("INSERT INTO products VALUES ('old', 'OldProduct')")
        conn.commit()
        conn.close()

        _write_csv(csv_dir / "products.csv", [
            ["id", "name"],
            ["1", "NewProduct"],
        ])
        (work_dir / "sql" / "export" / "products.sql").write_text(
            "SELECT id, name FROM products", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "load": {"mode": "replace"},
        })

        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM products")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT name FROM products")
        assert cur.fetchone()[0] == "NewProduct"
        conn.close()

    def test_load_append_mode_skip_duplicate(self, work_dir, logger):
        """append 모드: 동일 해시 파일 재적재 시 스킵 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        db_path = work_dir / "data" / "local" / "test.sqlite"

        _write_csv(csv_dir / "items.csv", [
            ["id", "val"],
            ["1", "x"],
        ])
        (work_dir / "sql" / "export" / "items.sql").write_text(
            "SELECT id, val FROM items", encoding="utf-8"
        )

        config = {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "load": {"mode": "append"},
        }

        # 첫 번째 로드
        ctx1 = _make_ctx(work_dir, logger, config)
        load_run(ctx1)

        # 두 번째 로드 (동일 파일 → 스킵되어야 함)
        ctx2 = _make_ctx(work_dir, logger, config)
        load_run(ctx2)

        assert ctx2.stage_results["load"]["skipped"] == 1

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM items")
        assert cur.fetchone()[0] == 1  # 중복 적재 없음
        conn.close()

    def test_load_null_handling(self, work_dir, logger):
        """빈 문자열 → NULL 변환 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "nulltest.csv", [
            ["id", "value"],
            ["1", "data"],
            ["2", ""],       # 빈 값 → NULL
            ["3", "  "],     # 공백만 → NULL
        ])
        (work_dir / "sql" / "export" / "nulltest.sql").write_text(
            "SELECT id, value FROM nulltest", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })
        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT value FROM nulltest WHERE id='2'")
        assert cur.fetchone()[0] is None
        conn.close()

    def test_load_with_meta_json(self, work_dir, logger):
        """meta.json에서 params 읽기 → delete 모드에서 조건부 삭제 검증."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        db_path = work_dir / "data" / "local" / "test.sqlite"

        # meta.json 생성 (columns에 실제 컬럼 메타 포함)
        _write_csv(csv_dir / "fact__local__month_202301.csv", [
            ["month", "value"],
            ["202301", "100"],
        ])
        meta = {
            "columns": [
                {"name": "month", "type": "DB_TYPE_VARCHAR", "size": 10},
                {"name": "value", "type": "DB_TYPE_VARCHAR", "size": 20},
            ],
            "params": {"month": "202301"},
        }
        (csv_dir / "fact__local__month_202301.meta.json").write_text(
            json.dumps(meta), encoding="utf-8"
        )

        (work_dir / "sql" / "export" / "fact.sql").write_text(
            "SELECT month, value FROM fact", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "load": {"mode": "delete"},
        })
        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact")
        assert cur.fetchone()[0] == 1
        conn.close()

    def test_load_include_filter(self, work_dir, logger):
        """--include 필터: 일부 CSV만 적재."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "alpha.csv", [["id"], ["1"]])
        _write_csv(csv_dir / "beta.csv", [["id"], ["2"]])

        (work_dir / "sql" / "export" / "alpha.sql").write_text("SELECT id FROM alpha", encoding="utf-8")
        (work_dir / "sql" / "export" / "beta.sql").write_text("SELECT id FROM beta", encoding="utf-8")

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        }, include_patterns=["alpha"])

        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != '_LOAD_HISTORY'"
        ).fetchall()]
        conn.close()

        assert "alpha" in tables
        assert "beta" not in tables

    def test_load_plan_mode(self, work_dir, logger):
        """plan 모드: 실제 DB 적재 없이 리포트만 출력."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "plan_test.csv", [["id"], ["1"]])
        (work_dir / "sql" / "export" / "plan_test.sql").write_text(
            "SELECT id FROM plan_test", encoding="utf-8"
        )

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        }, mode="plan")

        load_run(ctx)

        # DB 파일이 생성되지 않아야 함
        assert not db_path.exists()


# ── Transform 스테이지 통합 테스트 ─────────────────────────


class TestTransformStage:
    """SQLite에서 SQL 실행 검증."""

    def _setup_db(self, work_dir):
        """테스트용 SQLite DB + 테이블 생성."""
        db_path = work_dir / "data" / "local" / "test.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE raw_data (id INTEGER, value TEXT, category TEXT)")
        conn.executemany("INSERT INTO raw_data VALUES (?, ?, ?)", [
            (1, "100", "A"),
            (2, "200", "A"),
            (3, "300", "B"),
            (4, "400", "B"),
        ])
        conn.commit()
        conn.close()
        return db_path

    def test_transform_create_table(self, work_dir, logger):
        """SQL로 새 테이블 생성 검증."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_summary.sql").write_text(
            "CREATE TABLE summary AS SELECT category, COUNT(*) as cnt FROM raw_data GROUP BY category",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {"sql_dir": "sql/transform", "on_error": "stop"},
        })

        transform_run(ctx)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT cnt FROM summary WHERE category='A'")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT cnt FROM summary WHERE category='B'")
        assert cur.fetchone()[0] == 2
        conn.close()

        assert ctx.stage_results["transform"]["success"] == 1

    def test_transform_multiple_statements(self, work_dir, logger):
        """세미콜론으로 분리된 여러 SQL 문 실행."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_multi.sql").write_text(
            "CREATE TABLE t1 AS SELECT * FROM raw_data WHERE category='A';\n"
            "CREATE TABLE t2 AS SELECT * FROM raw_data WHERE category='B';",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {"sql_dir": "sql/transform"},
        })
        transform_run(ctx)

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM t1").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM t2").fetchone()[0] == 2
        conn.close()

    def test_transform_with_params(self, work_dir, logger):
        """파라미터 치환 검증 (${param} 문법)."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_param.sql").write_text(
            "CREATE TABLE filtered AS SELECT * FROM raw_data WHERE category = '${cat}'",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {
                "sql_dir": "sql/transform",
                "params": {"cat": "A"},
            },
        })
        transform_run(ctx)

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM filtered").fetchone()[0] == 2
        conn.close()

    def test_transform_param_expansion(self, work_dir, logger):
        """파라미터 다중값 확장 (product 모드)."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        # 파라미터 확장 → 2개 조합: cat=A, cat=B
        (sql_dir / "01_expand.sql").write_text(
            "INSERT INTO result_tbl SELECT * FROM raw_data WHERE category = '${cat}'",
            encoding="utf-8",
        )

        # 결과 테이블 미리 생성
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE result_tbl (id INTEGER, value TEXT, category TEXT)")
        conn.commit()
        conn.close()

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {
                "sql_dir": "sql/transform",
                "params": {"cat": "A|B"},  # 파이프로 다중값
            },
        })
        transform_run(ctx)

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM result_tbl").fetchone()[0] == 4
        conn.close()

        assert ctx.stage_results["transform"]["success"] == 2

    def test_transform_on_error_stop(self, work_dir, logger):
        """on_error=stop: 첫 에러에서 중단."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_good.sql").write_text(
            "CREATE TABLE good AS SELECT * FROM raw_data", encoding="utf-8"
        )
        (sql_dir / "02_bad.sql").write_text(
            "SELECT * FROM nonexistent_table", encoding="utf-8"
        )
        (sql_dir / "03_never.sql").write_text(
            "CREATE TABLE never AS SELECT 1", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {"sql_dir": "sql/transform", "on_error": "stop"},
        })
        transform_run(ctx)

        assert ctx.stage_results["transform"]["success"] == 1
        assert ctx.stage_results["transform"]["failed"] == 1

        conn = sqlite3.connect(str(db_path))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "good" in tables
        assert "never" not in tables  # 03_never는 실행 안 됨
        conn.close()

    def test_transform_on_error_continue(self, work_dir, logger):
        """on_error=continue: 에러 후에도 계속 실행."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_bad.sql").write_text(
            "SELECT * FROM nonexistent", encoding="utf-8"
        )
        (sql_dir / "02_good.sql").write_text(
            "CREATE TABLE survived AS SELECT 1 as v", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {"sql_dir": "sql/transform", "on_error": "continue"},
        })
        transform_run(ctx)

        assert ctx.stage_results["transform"]["failed"] == 1
        assert ctx.stage_results["transform"]["success"] == 1

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT v FROM survived").fetchone()[0] == 1
        conn.close()

    def test_transform_include_filter(self, work_dir, logger):
        """--include-transform 필터."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_run.sql").write_text(
            "CREATE TABLE run_result AS SELECT 1 as v", encoding="utf-8"
        )
        (sql_dir / "02_skip.sql").write_text(
            "CREATE TABLE skip_result AS SELECT 1 as v", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {"sql_dir": "sql/transform"},
        }, include_transform_patterns=["01_run"])

        transform_run(ctx)

        conn = sqlite3.connect(str(db_path))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "run_result" in tables
        assert "skip_result" not in tables
        conn.close()

    def test_transform_transfer_dest(self, work_dir, logger):
        """Transfer: source DB → dest DB ATTACH."""
        from stages.transform_stage import run as transform_run

        db_path = self._setup_db(work_dir)
        dest_path = work_dir / "data" / "local" / "dest.sqlite"

        sql_dir = work_dir / "sql" / "transform"
        (sql_dir / "01_transfer.sql").write_text(
            "CREATE TABLE dest.transferred AS SELECT * FROM raw_data WHERE category='A'",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "transform": {
                "sql_dir": "sql/transform",
                "transfer": {
                    "dest": {
                        "type": "sqlite3",
                        "db_path": str(dest_path),
                    }
                },
            },
        })
        transform_run(ctx)

        conn = sqlite3.connect(str(dest_path))
        assert conn.execute("SELECT COUNT(*) FROM transferred").fetchone()[0] == 2
        conn.close()


# ── Report 스테이지 통합 테스트 ────────────────────────────


class TestReportStage:
    """SQLite에서 쿼리 → CSV 출력 검증."""

    def _setup_db(self, work_dir):
        """테스트 DB 생성."""
        db_path = work_dir / "data" / "local" / "test.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE report_src (category TEXT, total INTEGER)")
        conn.executemany("INSERT INTO report_src VALUES (?, ?)", [
            ("A", 100), ("B", 200), ("C", 300),
        ])
        conn.commit()
        conn.close()
        return db_path

    def test_report_csv_export(self, work_dir, logger):
        """SQL 실행 → CSV 파일 생성."""
        from stages.report_stage import run as report_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "report"
        (sql_dir / "summary.sql").write_text(
            "SELECT category, total FROM report_src ORDER BY category",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                    "compression": "none",
                },
                "excel": {"enabled": False},
            },
        })
        report_run(ctx)

        out_file = work_dir / "data" / "report" / "summary.csv"
        assert out_file.exists()

        with open(out_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0] == ["category", "total"]
        assert len(rows) == 4  # 헤더 + 3행

    def test_report_csv_gzip(self, work_dir, logger):
        """gzip 압축 CSV 출력."""
        from stages.report_stage import run as report_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "report"
        (sql_dir / "compressed.sql").write_text(
            "SELECT * FROM report_src", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                    "compression": "gzip",
                },
                "excel": {"enabled": False},
            },
        })
        report_run(ctx)

        gz_file = work_dir / "data" / "report" / "compressed.csv.gz"
        assert gz_file.exists()

        with gzip.open(gz_file, "rt", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 4

    def test_report_with_params(self, work_dir, logger):
        """파라미터 다중값 → 파일명에 파라미터값 포함."""
        from stages.report_stage import run as report_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "report"
        (sql_dir / "by_cat.sql").write_text(
            "SELECT * FROM report_src WHERE category = '${cat}'",
            encoding="utf-8",
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "report": {
                "source": "target",
                "params": {"cat": "A|B"},
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                },
                "excel": {"enabled": False},
            },
        })
        report_run(ctx)

        out_dir = work_dir / "data" / "report"
        csv_files = sorted(out_dir.glob("by_cat*.csv"))
        assert len(csv_files) == 2  # A, B 각각 1파일

    def test_report_skip_sql(self, work_dir, logger):
        """skip_sql=True: DB 연결 없이 기존 CSV 사용."""
        from stages.report_stage import run as report_run

        # CSV 직접 생성 (DB 미사용)
        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "existing.csv", [
            ["col1", "col2"],
            ["x", "y"],
        ])

        ctx = _make_ctx(work_dir, logger, {
            "report": {
                "skip_sql": True,
                "csv_union_dir": str(csv_dir),
                "excel": {"enabled": False},
            },
        })
        report_run(ctx)

        # skip_sql이면 DB 연결 없이 정상 실행됨 (에러 없음)
        assert "report" in ctx.stage_results

    def test_report_include_filter(self, work_dir, logger):
        """--include-report 필터."""
        from stages.report_stage import run as report_run

        db_path = self._setup_db(work_dir)

        sql_dir = work_dir / "sql" / "report"
        (sql_dir / "include_me.sql").write_text(
            "SELECT * FROM report_src", encoding="utf-8"
        )
        (sql_dir / "exclude_me.sql").write_text(
            "SELECT * FROM report_src", encoding="utf-8"
        )

        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                },
                "excel": {"enabled": False},
            },
        }, include_report_patterns=["include_me"])

        report_run(ctx)

        out_dir = work_dir / "data" / "report"
        assert (out_dir / "include_me.csv").exists()
        assert not (out_dir / "exclude_me.csv").exists()


# ── E2E 파이프라인 통합 테스트 ─────────────────────────────


class TestPipelineE2E:
    """Load → Transform → Report 전체 파이프라인."""

    def test_full_pipeline(self, work_dir, logger):
        """CSV → Load → Transform → Report CSV 전체 흐름."""
        from runner import run_pipeline

        db_path = work_dir / "data" / "local" / "test.sqlite"

        # 1. Export 결과 CSV 생성 (실제 export 대신 수동 생성)
        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "sales.csv", [
            ["region", "product", "amount"],
            ["Seoul", "Widget", "1000"],
            ["Seoul", "Gadget", "2000"],
            ["Busan", "Widget", "1500"],
            ["Busan", "Gadget", "500"],
        ])

        (work_dir / "sql" / "export" / "sales.sql").write_text(
            "SELECT region, product, amount FROM sales", encoding="utf-8"
        )

        # 2. Transform SQL
        (work_dir / "sql" / "transform" / "01_agg.sql").write_text(
            "CREATE TABLE region_summary AS\n"
            "SELECT region, SUM(CAST(amount AS INTEGER)) as total\n"
            "FROM sales GROUP BY region",
            encoding="utf-8",
        )
        (work_dir / "sql" / "transform" / "02_rank.sql").write_text(
            "CREATE TABLE region_rank AS\n"
            "SELECT region, total,\n"
            "       RANK() OVER (ORDER BY total DESC) as rnk\n"
            "FROM region_summary",
            encoding="utf-8",
        )

        # 3. Report SQL
        (work_dir / "sql" / "report" / "final_report.sql").write_text(
            "SELECT region, total, rnk FROM region_rank ORDER BY rnk",
            encoding="utf-8",
        )

        # 파이프라인 실행
        job_config = {
            "pipeline": {
                "stages": ["load", "transform", "report"],
            },
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "transform": {"sql_dir": "sql/transform", "on_error": "stop"},
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                },
                "excel": {"enabled": False},
            },
        }

        ctx = _make_ctx(work_dir, logger, job_config)
        run_pipeline(ctx)

        # Load 검증
        assert ctx.stage_results["load"]["success"] == 1

        # Transform 검증
        assert ctx.stage_results["transform"]["success"] == 2

        # Report 검증
        report_csv = work_dir / "data" / "report" / "final_report.csv"
        assert report_csv.exists()

        with open(report_csv, "r", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        assert rows[0] == ["region", "total", "rnk"]
        # Seoul=3000(1위), Busan=2000(2위)
        assert rows[1][0] == "Seoul"
        assert rows[1][1] == "3000"
        assert rows[2][0] == "Busan"

        # DB 최종 상태 검증
        conn = sqlite3.connect(str(db_path))
        tables = sorted(r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != '_LOAD_HISTORY'"
        ).fetchall())
        assert "sales" in tables
        assert "region_summary" in tables
        assert "region_rank" in tables
        conn.close()

    def test_pipeline_with_params(self, work_dir, logger):
        """파라미터가 있는 파이프라인 E2E."""
        from runner import run_pipeline

        db_path = work_dir / "data" / "local" / "test.sqlite"

        # CSV (월별 데이터)
        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "monthly__local__ym_202301.csv", [
            ["ym", "value"],
            ["202301", "100"],
        ])
        _write_csv(csv_dir / "monthly__local__ym_202302.csv", [
            ["ym", "value"],
            ["202302", "200"],
        ])

        (work_dir / "sql" / "export" / "monthly.sql").write_text(
            "SELECT ym, value FROM monthly", encoding="utf-8"
        )

        # Transform: 파라미터별 집계
        (work_dir / "sql" / "transform" / "01_total.sql").write_text(
            "CREATE TABLE IF NOT EXISTS ym_total (ym TEXT, total INTEGER);\n"
            "INSERT INTO ym_total SELECT ym, SUM(CAST(value AS INTEGER)) FROM monthly WHERE ym = '${ym}' GROUP BY ym",
            encoding="utf-8",
        )

        # Report
        (work_dir / "sql" / "report" / "ym_report.sql").write_text(
            "SELECT * FROM ym_total ORDER BY ym", encoding="utf-8"
        )

        job_config = {
            "pipeline": {"stages": ["load", "transform", "report"]},
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "transform": {
                "sql_dir": "sql/transform",
                "params": {"ym": "202301|202302"},
            },
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": True,
                    "sql_dir": "sql/report",
                    "out_dir": "data/report",
                },
                "excel": {"enabled": False},
            },
        }

        ctx = _make_ctx(work_dir, logger, job_config)
        run_pipeline(ctx)

        # Transform: 2개 파라미터 세트 실행
        assert ctx.stage_results["transform"]["success"] == 2

        # 최종 CSV 검증
        report_csv = work_dir / "data" / "report" / "ym_report.csv"
        assert report_csv.exists()
        with open(report_csv, "r", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3  # 헤더 + 2행

    def test_pipeline_stage_filter(self, work_dir, logger):
        """--stage 필터: 특정 스테이지만 실행."""
        from runner import run_pipeline

        db_path = work_dir / "data" / "local" / "test.sqlite"

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "data.csv", [["id"], ["1"]])
        (work_dir / "sql" / "export" / "data.sql").write_text(
            "SELECT id FROM data", encoding="utf-8"
        )
        (work_dir / "sql" / "transform" / "01_t.sql").write_text(
            "CREATE TABLE t_result AS SELECT * FROM data", encoding="utf-8"
        )

        job_config = {
            "pipeline": {"stages": ["load", "transform"]},
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
            "transform": {"sql_dir": "sql/transform"},
        }

        # load만 실행
        ctx = _make_ctx(work_dir, logger, job_config, stage_filter=["load"])
        run_pipeline(ctx)

        assert "load" in ctx.stage_results
        assert "transform" not in ctx.stage_results

    def test_pipeline_no_stages(self, work_dir, logger):
        """stages가 비어있으면 경고만 출력."""
        from runner import run_pipeline

        ctx = _make_ctx(work_dir, logger, {"pipeline": {"stages": []}})
        run_pipeline(ctx)
        # 에러 없이 종료되어야 함

    def test_load_direct_csv_no_sql(self, work_dir, logger):
        """SQL 파일 없이 CSV 파일명으로 직접 테이블 매핑."""
        from stages.load_stage import run as load_run

        csv_dir = work_dir / "data" / "export" / "test_job"
        _write_csv(csv_dir / "direct_table.csv", [
            ["col_a", "col_b"],
            ["x", "1"],
            ["y", "2"],
        ])

        db_path = work_dir / "data" / "local" / "test.sqlite"
        ctx = _make_ctx(work_dir, logger, {
            "target": {"type": "sqlite3", "db_path": str(db_path)},
            "export": {"sql_dir": "sql/export", "out_dir": "data/export"},
        })
        load_run(ctx)

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM direct_table").fetchone()[0] == 2
        conn.close()
