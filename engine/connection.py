# file: engine/connection.py
"""
Target DB 연결 팩토리 및 공통 유틸.

connect_target(ctx, target_cfg) → (conn, conn_type, label)
  - DuckDB / SQLite3 / Oracle 분기를 한 곳에서 처리
"""

import re
import logging
from datetime import datetime

from engine.path_utils import resolve_path

_log = logging.getLogger(__name__)

# Oracle/DuckDB 식별자로 허용할 문자: 영문, 숫자, _, $, #  (최대 128자)
_SAFE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_$#]{0,127}$')


def now_str() -> str:
    """공통 타임스탬프 문자열 (adapter에서 공유)"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def set_session_schema(conn, conn_type: str, schema: str, logger=None):
    """세션 기본 스키마를 설정한다. schema 값을 검증하여 SQL 인젝션을 방지."""
    if not _SAFE_IDENTIFIER.match(schema):
        raise ValueError(f"유효하지 않은 schema 이름: {schema!r}")

    log = logger or _log
    if conn_type == "duckdb":
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.execute(f"SET schema = '{schema}'")
    elif conn_type == "oracle":
        cur = conn.cursor()
        cur.execute(f'ALTER SESSION SET CURRENT_SCHEMA = "{schema.upper()}"')
        cur.close()
    log.info("Session schema = '%s'", schema)


def connect_target(ctx, target_cfg: dict) -> tuple:
    """
    target_cfg 설정에 따라 DB 연결을 생성한다.

    Returns:
        (conn, conn_type, label) 튜플
        - conn: DB 연결 객체
        - conn_type: "duckdb" | "sqlite3" | "oracle"
        - label: 로그용 식별 문자열
    """
    tgt_type = (target_cfg.get("type") or "").strip().lower()

    if tgt_type == "duckdb":
        from adapters.targets.duckdb_target import connect
        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.duckdb"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        label = f"duckdb ({db_path.resolve()})"
        return connect(db_path), "duckdb", label

    elif tgt_type == "sqlite3":
        from adapters.targets.sqlite_target import connect
        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.sqlite"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        label = f"sqlite3 ({db_path.resolve()})"
        return connect(db_path), "sqlite3", label

    elif tgt_type == "oracle":
        from adapters.targets.oracle_target import connect
        schema = (target_cfg.get("schema") or "").strip() or None
        schema_pw = (target_cfg.get("schema_password") or "").strip() or None
        label = f"oracle (schema={schema})"
        return connect(ctx.env_config, schema=schema, schema_password=schema_pw), "oracle", label

    else:
        raise ValueError(f"Unsupported target type: {tgt_type}")
