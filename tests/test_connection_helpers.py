"""engine/connection.py 헬퍼 함수 단위 테스트."""
import pytest
from unittest.mock import MagicMock

from engine.connection import set_session_schema


class TestSetSessionSchema:

    def test_duckdb_creates_and_sets(self):
        """DuckDB: CREATE SCHEMA + SET schema 호출."""
        conn = MagicMock()
        set_session_schema(conn, "duckdb", "my_schema")
        calls = [c.args[0] for c in conn.execute.call_args_list]
        assert any("CREATE SCHEMA" in c and "my_schema" in c for c in calls)
        assert any("SET schema" in c and "my_schema" in c for c in calls)

    def test_oracle_alter_session(self):
        """Oracle: ALTER SESSION SET CURRENT_SCHEMA 호출."""
        conn = MagicMock()
        mock_cur = MagicMock()
        conn.cursor.return_value = mock_cur
        set_session_schema(conn, "oracle", "my_schema")
        sql = mock_cur.execute.call_args[0][0]
        assert "ALTER SESSION" in sql
        assert "MY_SCHEMA" in sql
        mock_cur.close.assert_called_once()

    def test_rejects_injection(self):
        """SQL 인젝션 시도 → ValueError."""
        conn = MagicMock()
        with pytest.raises(ValueError, match="유효하지 않은 schema"):
            set_session_schema(conn, "oracle", "'; DROP TABLE x; --")

    def test_rejects_empty(self):
        """빈 문자열 → ValueError."""
        conn = MagicMock()
        with pytest.raises(ValueError, match="유효하지 않은 schema"):
            set_session_schema(conn, "duckdb", "")

    def test_rejects_space(self):
        """공백 포함 → ValueError."""
        conn = MagicMock()
        with pytest.raises(ValueError, match="유효하지 않은 schema"):
            set_session_schema(conn, "duckdb", "my schema")

    def test_allows_valid_identifiers(self):
        """유효한 식별자 허용: 영문, 숫자, _, $, #."""
        conn = MagicMock()
        for name in ("SCHEMA_1", "my$schema", "test#01", "_private"):
            set_session_schema(conn, "duckdb", name)  # 예외 없이 통과

    def test_custom_logger(self):
        """사용자 정의 logger 전달 시 그 logger 사용."""
        conn = MagicMock()
        logger = MagicMock()
        set_session_schema(conn, "duckdb", "test_schema", logger=logger)
        logger.info.assert_called_once()
        assert "test_schema" in logger.info.call_args[0][1]
