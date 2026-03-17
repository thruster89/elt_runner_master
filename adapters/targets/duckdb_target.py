# file: v2/adapters/targets/duckdb_target.py

import json
import time
import logging
from datetime import datetime
from pathlib import Path

from engine.connection import now_str
from engine.delete_utils import build_delete_condition

logger = logging.getLogger(__name__)


def _ensure_schema(conn, schema: str):
    """스키마가 없으면 생성"""
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def _ensure_history(conn, schema: str = None):
    prefix = f'"{schema}".' if schema else ""
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {prefix}_LOAD_HISTORY (
            job_name      VARCHAR,
            table_name    VARCHAR,
            csv_file      VARCHAR,
            file_hash     VARCHAR,
            file_size     BIGINT,
            mtime         VARCHAR,
            loaded_at     VARCHAR
        )
        """
    )


def _history_exists(conn, schema: str, job_name: str, table_name: str, file_hash: str) -> bool:
    prefix = f'"{schema}".' if schema else ""
    rows = conn.execute(
        f"""
        SELECT 1 FROM {prefix}_LOAD_HISTORY
         WHERE job_name   = ?
           AND table_name = ?
           AND file_hash  = ?
         LIMIT 1
        """,
        [job_name, table_name, file_hash],
    ).fetchall()
    return bool(rows)


def _insert_history(conn, schema: str, job_name: str, table_name: str, csv_file: str,
                    file_hash: str, file_size: int, mtime: str):
    prefix = f'"{schema}".' if schema else ""
    conn.execute(
        f"""
        INSERT INTO {prefix}_LOAD_HISTORY
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [job_name, table_name, csv_file, file_hash, file_size, mtime, now_str()],
    )


def _table_exists(conn, schema: str, table_name: str) -> bool:
    sch = schema or conn.execute("SELECT current_schema()").fetchone()[0]
    rows = conn.execute(
        """
        SELECT 1 FROM information_schema.tables
         WHERE table_schema = ? AND table_name = ?
         LIMIT 1
        """,
        [sch, table_name],
    ).fetchall()
    return bool(rows)


def _get_table_columns(conn, schema: str, table_name: str) -> set:
    """테이블 컬럼명 집합 반환"""
    sch = schema or conn.execute("SELECT current_schema()").fetchone()[0]
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [sch, table_name],
    ).fetchall()
    return {row[0] for row in rows}


def _delete_by_params(conn, schema: str, table_name: str, params: dict):
    """params 기반 WHERE 조건으로 DELETE 실행 (엄격 모드: params 필수, 컬럼 매칭 필수)"""
    tbl = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    table_cols = _get_table_columns(conn, schema, table_name)
    matched, _ = build_delete_condition(params, table_cols, tbl)

    where = " AND ".join(f'"{col}" = ?' for col, _ in matched)
    values = [val for _, val in matched]
    del_count = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {where}", values).fetchone()[0]
    conn.execute(f"DELETE FROM {tbl} WHERE {where}", values)
    logger.info("DELETE %s | %d rows | WHERE %s", tbl, del_count, where)


def _find_meta_file(csv_path: Path) -> Path | None:
    """CSV와 같은 디렉토리에서 대응하는 .meta.json 탐색."""
    name = csv_path.name
    stem = name[:-len(".csv.gz")] if name.endswith(".csv.gz") else name[:-len(".csv")]
    meta = csv_path.parent / (stem + ".meta.json")
    return meta if meta.exists() else None


def _meta_type_to_duckdb(col: dict) -> str:
    """meta.json 컬럼 정보 → DuckDB DDL 타입 문자열 변환."""
    t = col.get("type", "").upper()
    size = col.get("size")
    precision = col.get("precision")
    scale = col.get("scale")

    if "NUMBER" in t or "BINARY_DOUBLE" in t or "BINARY_FLOAT" in t:
        if precision and scale and scale > 0:
            return f"DECIMAL({precision},{scale})"
        elif precision:
            return f"BIGINT"
        return "DOUBLE"
    if "FLOAT" in t:
        return "DOUBLE"
    if "DATE" in t and "TIMESTAMP" not in t:
        return "DATE"
    if "TIMESTAMP" in t:
        if "TIME_ZONE" in t or "TZ" in t:
            return "TIMESTAMPTZ"
        return "TIMESTAMP"
    if "CLOB" in t or "LONG" in t:
        return "VARCHAR"
    if "BLOB" in t or "RAW" in t:
        return "BLOB"
    if "NVARCHAR" in t or "NCHAR" in t:
        return f"VARCHAR({size})" if size else "VARCHAR"
    if "CHAR" in t:
        return f"VARCHAR({size})" if size else "VARCHAR"
    # VARCHAR / default
    if size and size > 0:
        return f"VARCHAR({size})"
    return "VARCHAR"


def _create_table_from_meta(conn, schema: str, table_name: str, meta: list[dict]):
    """소스 메타데이터 기반으로 정확한 타입의 테이블 생성."""
    tbl = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    col_defs = [f'  "{col["name"]}" {_meta_type_to_duckdb(col)}' for col in meta]
    ddl = f"CREATE TABLE {tbl} (\n" + ",\n".join(col_defs) + "\n)"

    logger.info("CREATE TABLE %s (from source metadata)", tbl)
    logger.debug("DDL:\n%s", ddl)
    conn.execute(ddl)


def load_csv(conn, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str, schema: str = None,
             load_mode: str = "replace", params: dict = None) -> int:
    """
    CSV를 DuckDB 테이블에 적재.
    schema 지정 시 해당 스키마에 생성/INSERT.
    load_mode: replace(DROP+CREATE) | truncate(DELETE ALL) | delete(params WHERE) | append(INSERT)
    반환값: 적재된 row 수 (-1이면 skip)
    """
    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    full_table = f"{schema}.{table_name}" if schema else table_name

    # replace/truncate 시 히스토리 체크 스킵 (어차피 덮어쓰므로)
    if load_mode == "append":
        if mode != "retry" and _history_exists(conn, schema, job_name, full_table, file_hash):
            logger.info("LOAD skip (already loaded) | %s | %s", full_table, csv_path.name)
            return -1

    start = time.time()
    tbl = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

    if load_mode == "replace" and _table_exists(conn, schema, table_name):
        logger.info("LOAD mode=replace → DROP TABLE %s", tbl)
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        prefix = f'"{schema}".' if schema else ""
        conn.execute(
            f"DELETE FROM {prefix}_LOAD_HISTORY WHERE job_name = ? AND table_name = ?",
            [job_name, full_table],
        )

    if load_mode == "truncate" and _table_exists(conn, schema, table_name):
        logger.info("LOAD mode=truncate → DELETE FROM %s", tbl)
        conn.execute(f"DELETE FROM {tbl}")

    if load_mode == "delete" and _table_exists(conn, schema, table_name):
        _delete_by_params(conn, schema, table_name, params or {})

    if not _table_exists(conn, schema, table_name):
        logger.info("Table not found, creating: %s", tbl)
        meta_file = _find_meta_file(csv_path)
        if meta_file:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            columns = meta["columns"] if isinstance(meta, dict) and "columns" in meta else meta
            _create_table_from_meta(conn, schema, table_name, columns)
            conn.execute(
                f"INSERT INTO {tbl} SELECT * FROM read_csv_auto(?, header=True, all_varchar=true)",
                [str(csv_path)],
            )
        else:
            conn.execute(
                f"CREATE TABLE {tbl} AS SELECT * FROM read_csv_auto(?, header=True)",
                [str(csv_path)],
            )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    else:
        logger.debug("Table exists: %s", tbl)
        before = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        conn.execute(
            f"INSERT INTO {tbl} SELECT * FROM read_csv_auto(?, header=True, all_varchar=true)",
            [str(csv_path)],
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0] - before
    _insert_history(conn, schema, job_name, full_table, str(csv_path), file_hash, file_size, mtime)

    elapsed = time.time() - start
    if row_count == 0:
        logger.info("LOAD done | table=%s rows=0 (empty) elapsed=%.2fs | mode=%s",
                     full_table, elapsed, load_mode)
    else:
        logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs | mode=%s",
                     full_table, row_count, elapsed, load_mode)
    return row_count


def load_csv_batch(conn, job_name: str, table_name: str, csv_paths: list[Path],
                   file_hashes: list[str], mode: str, schema: str = None,
                   load_mode: str = "replace") -> int:
    """
    동일 테이블에 여러 CSV를 한번에 적재 (replace/truncate 전용).
    read_csv_auto([파일목록]) 으로 단일 INSERT 수행.
    반환값: 적재된 총 row 수.
    """
    tbl = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    full_table = f"{schema}.{table_name}" if schema else table_name
    path_strs = [str(p) for p in csv_paths]

    start = time.time()

    # 1) DROP or TRUNCATE
    if load_mode == "replace" and _table_exists(conn, schema, table_name):
        logger.info("LOAD mode=replace → DROP TABLE %s", tbl)
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        prefix = f'"{schema}".' if schema else ""
        conn.execute(
            f"DELETE FROM {prefix}_LOAD_HISTORY WHERE job_name = ? AND table_name = ?",
            [job_name, full_table],
        )
    elif load_mode == "truncate" and _table_exists(conn, schema, table_name):
        logger.info("LOAD mode=truncate → DELETE FROM %s", tbl)
        conn.execute(f"DELETE FROM {tbl}")

    # 2) CREATE or INSERT (read_csv_auto에 리스트 전달)
    if not _table_exists(conn, schema, table_name):
        meta_file = _find_meta_file(csv_paths[0])
        if meta_file:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            columns = meta["columns"] if isinstance(meta, dict) and "columns" in meta else meta
            _create_table_from_meta(conn, schema, table_name, columns)
            conn.execute(
                f"INSERT INTO {tbl} SELECT * FROM read_csv_auto(?, header=True, all_varchar=true)",
                [path_strs],
            )
        else:
            conn.execute(
                f"CREATE TABLE {tbl} AS SELECT * FROM read_csv_auto(?, header=True)",
                [path_strs],
            )
    else:
        conn.execute(
            f"INSERT INTO {tbl} SELECT * FROM read_csv_auto(?, header=True, all_varchar=true)",
            [path_strs],
        )

    row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

    # 3) 파일별 히스토리 기록
    for csv_path, file_hash in zip(csv_paths, file_hashes):
        file_size = csv_path.stat().st_size
        mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        _insert_history(conn, schema, job_name, full_table, str(csv_path), file_hash, file_size, mtime)

    elapsed = time.time() - start
    logger.info("LOAD done (batch) | table=%s | %d files | rows=%d | elapsed=%.2fs | mode=%s",
                full_table, len(csv_paths), row_count, elapsed, load_mode)
    return row_count


def connect(db_path: Path):
    import duckdb
    return duckdb.connect(str(db_path))