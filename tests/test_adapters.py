"""adapters mock 단위 테스트."""
import csv
import gzip
import io
import logging
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# =====================================================================
# _normalize_date_value 단위 테스트
# =====================================================================
class TestNormalizeDateValue:
    """YYYYMMDD → YYYY-MM-DD 자동 변환 테스트."""

    def test_yyyymmdd_string(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("20260201") == "2026-02-01"

    def test_yyyymmdd_with_time(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("20260201 00:00:00") == "2026-02-01 00:00:00"

    def test_yyyymmdd_with_time_t_separator(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("20260201T12:30:45") == "2026-02-01T12:30:45"

    def test_already_formatted(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("2026-02-01 00:00:00") == "2026-02-01 00:00:00"

    def test_python_datetime(self):
        from adapters.sources.vertica_source import _normalize_date_value
        dt = datetime(2026, 2, 1, 13, 30, 0)
        assert _normalize_date_value(dt) == "2026-02-01 13:30:00"

    def test_python_date(self):
        from adapters.sources.vertica_source import _normalize_date_value
        d = date(2026, 2, 1)
        assert _normalize_date_value(d) == "2026-02-01"

    def test_none_passthrough(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value(None) is None

    def test_empty_passthrough(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("") == ""

    def test_non_date_string_passthrough(self):
        from adapters.sources.vertica_source import _normalize_date_value
        assert _normalize_date_value("hello") == "hello"

    def test_oracle_normalize_same_behavior(self):
        """oracle_source의 _normalize_date_value도 동일 동작."""
        from adapters.sources.oracle_source import _normalize_date_value
        assert _normalize_date_value("20260201 00:00:00") == "2026-02-01 00:00:00"
        assert _normalize_date_value(datetime(2026, 1, 15, 9, 0, 0)) == "2026-01-15 09:00:00"


# =====================================================================
# vertica_source — 날짜 컬럼 자동 포맷팅 통합 테스트
# =====================================================================
class TestVerticaDateAutoFormat:
    """컬럼명 패턴(DT, _ST, _CLSTR, _DTHMS)으로 날짜 자동 변환."""

    def test_dt_suffix_auto_formatted(self, tmp_path):
        """PPDT 같은 *DT 접미사 컬럼이 자동 변환되는지."""
        from adapters.sources.vertica_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("PY_BJNO", None, None, None, None, None, None),
            ("PPDT", None, None, None, None, None, None),
            ("CC_STDT", None, None, None, None, None, None),
            ("RTAMT", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [
                ("26A23F2C99", "20260201 00:00:00", "20260202 00:00:00", "3950"),
                ("26A23F2C9D", "20260201", "20260202", "916450"),
            ],
            [],
        ]

        out_file = tmp_path / "result.csv"
        rows = export_sql_to_csv(mock_conn, "SELECT 1", out_file,
                                  logging.getLogger("test"))

        assert rows == 2
        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        # PPDT, CC_STDT는 변환되고 PY_BJNO, RTAMT는 그대로
        assert lines[1] == "26A23F2C99,2026-02-01 00:00:00,2026-02-02 00:00:00,3950"
        assert lines[2] == "26A23F2C9D,2026-02-01,2026-02-02,916450"

    def test_dthms_and_st_suffix(self, tmp_path):
        """_DTHMS, _ST 접미사도 감지되는지."""
        from adapters.sources.vertica_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("ID", None, None, None, None, None, None),
            ("REG_DTHMS", None, None, None, None, None, None),
            ("START_ST", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [("1", "20260315 14:30:00", "20260101 00:00:00")],
            [],
        ]

        out_file = tmp_path / "result.csv"
        rows = export_sql_to_csv(mock_conn, "SELECT 1", out_file,
                                  logging.getLogger("test"))

        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert lines[1] == "1,2026-03-15 14:30:00,2026-01-01 00:00:00"

    def test_non_date_columns_untouched(self, tmp_path):
        """날짜 패턴에 안 맞는 컬럼은 변환하지 않음."""
        from adapters.sources.vertica_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("USER_ID", None, None, None, None, None, None),
            ("STATUS", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [("20260201", "20260201")],
            [],
        ]

        out_file = tmp_path / "result.csv"
        export_sql_to_csv(mock_conn, "SELECT 1", out_file,
                          logging.getLogger("test"))

        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        # 날짜 컬럼이 아니므로 YYYYMMDD 그대로
        assert lines[1] == "20260201,20260201"

    def test_null_date_column_stays_empty(self, tmp_path):
        """날짜 컬럼이 NULL이면 빈 문자열 유지."""
        from adapters.sources.vertica_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("ID", None, None, None, None, None, None),
            ("PPDT", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [("1", None)],
            [],
        ]

        out_file = tmp_path / "result.csv"
        export_sql_to_csv(mock_conn, "SELECT 1", out_file,
                          logging.getLogger("test"))

        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert lines[1] == "1,"


# =====================================================================
# oracle_source — NULL 처리
# =====================================================================
class TestOracleSourceNullHandling:
    """oracle_source.export_sql_to_csv에서 None → 빈 문자열 처리 확인."""

    def test_none_becomes_empty_string(self, tmp_path):
        """None 값이 CSV에 빈 문자열로 기록되는지."""
        from adapters.sources.oracle_source import export_sql_to_csv

        # mock cursor 설정
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("ID", None, None, None, None, None, None),
            ("NAME", None, None, None, None, None, None),
        ]
        # fetchmany: 첫 호출 → 데이터, 둘째 호출 → 빈 리스트
        mock_cursor.fetchmany.side_effect = [
            [(1, None), (2, "hello"), (None, None)],
            [],
        ]

        out_file = tmp_path / "result.csv"
        logger = logging.getLogger("test")

        rows = export_sql_to_csv(mock_conn, "SELECT 1", out_file, logger)

        assert rows == 3
        assert out_file.exists()

        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert lines[0] == "ID,NAME"   # header
        assert lines[1] == "1,"        # None → ""
        assert lines[2] == "2,hello"
        assert lines[3] == ","         # 두 컬럼 모두 None

    def test_gzip_none_handling(self, tmp_path):
        """gzip 모드에서도 None → 빈 문자열."""
        from adapters.sources.oracle_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("COL", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(None,), ("val",)],
            [],
        ]

        out_file = tmp_path / "result.csv.gz"
        logger = logging.getLogger("test")

        rows = export_sql_to_csv(mock_conn, "SELECT 1", out_file, logger,
                                  compression="gzip")

        assert rows == 2
        with gzip.open(out_file, "rt") as f:
            reader = list(csv.reader(f))
        assert reader[1] == [""]     # None → ""
        assert reader[2] == ["val"]


# =====================================================================
# vertica_source — NULL 처리
# =====================================================================
class TestVerticaSourceNullHandling:
    """vertica_source.export_sql_to_csv에서 None → 빈 문자열 처리 확인."""

    def test_none_becomes_empty_string(self, tmp_path):
        """None 값이 CSV에 빈 문자열로 기록되는지."""
        from adapters.sources.vertica_source import export_sql_to_csv

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("ID", None, None, None, None, None, None),
            ("VAL", None, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(1, None), (None, "text")],
            [],
        ]

        out_file = tmp_path / "result.csv"
        logger = logging.getLogger("test")

        rows = export_sql_to_csv(mock_conn, "SELECT 1", out_file, logger)

        assert rows == 2
        content = out_file.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert lines[1] == "1,"        # None → ""
        assert lines[2] == ",text"     # None → ""


# =====================================================================
# duckdb_target — _delete_by_params (build_delete_condition 통합)
# =====================================================================
class TestDuckdbDeleteByParams:

    def test_calls_build_delete_condition(self, tmp_path):
        """build_delete_condition을 호출하고 DELETE를 실행하는지."""
        from adapters.targets.duckdb_target import _delete_by_params

        mock_conn = MagicMock()
        # _get_table_columns → information_schema 쿼리 mock
        mock_conn.execute.return_value.fetchall.return_value = [("ym",)]
        # SELECT COUNT(*) mock
        mock_conn.execute.return_value.fetchone.return_value = (5,)

        with patch("adapters.targets.duckdb_target._get_table_columns",
                   return_value={"ym", "code"}):
            _delete_by_params(mock_conn, "schema", "table", {"ym": "202301"})

        # DELETE가 호출되었는지 확인
        calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("DELETE" in c for c in calls)

    def test_raises_on_empty_params(self):
        """빈 params → ValueError."""
        from adapters.targets.duckdb_target import _delete_by_params
        with pytest.raises(ValueError, match="params가 필수"):
            _delete_by_params(MagicMock(), "schema", "table", {})


# =====================================================================
# sqlite_target — _delete_by_params
# =====================================================================
class TestSqliteDeleteByParams:

    def test_raises_on_empty_params(self):
        """빈 params → ValueError."""
        from adapters.targets.sqlite_target import _delete_by_params
        with pytest.raises(ValueError, match="params가 필수"):
            _delete_by_params(MagicMock(), "table", {})


# =====================================================================
# oracle_target — 비밀번호 폴백 경고
# =====================================================================
class TestOracleTargetPasswordWarning:

    def test_password_fallback_logs_warning(self):
        """schema_password 미설정 시 경고 로그가 출력되는지."""
        from adapters.targets.oracle_target import _ensure_schema

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        # _schema_exists → True (이미 존재)
        mock_cur.execute.return_value = None
        mock_cur.fetchone.return_value = (1,)  # schema exists

        with patch("adapters.targets.oracle_target.logger") as mock_logger:
            _ensure_schema(mock_cur, mock_conn, "test_schema", password=None)
            # 스키마 이미 존재하면 경고 불필요 (CREATE 시에만)

        # 스키마 미존재 → password 폴백 경고
        mock_cur.fetchone.return_value = (0,)  # schema not exists
        with patch("adapters.targets.oracle_target.logger") as mock_logger:
            _ensure_schema(mock_cur, mock_conn, "test_schema", password=None)
            assert any(
                "schema_password 미설정" in str(c)
                for c in mock_logger.warning.call_args_list
            )
