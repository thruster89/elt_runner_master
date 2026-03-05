# file: v2/adapters/targets/sqlite_target.py

import csv
import gzip
import json
import time
import logging
from datetime import datetime
from pathlib import Path

from engine.connection import now_str
from engine.delete_utils import build_delete_condition

logger = logging.getLogger(__name__)


def _ensure_history(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _LOAD_HISTORY (
            job_name      TEXT,
            table_name    TEXT,
            csv_file      TEXT,
            file_hash     TEXT,
            file_size     INTEGER,
            mtime         TEXT,
            loaded_at     TEXT
        )
        """
    )
    conn.commit()


def _history_exists(conn, job_name: str, table_name: str, file_hash: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM _LOAD_HISTORY
         WHERE job_name   = ?
           AND table_name = ?
           AND file_hash  = ?
         LIMIT 1
        """,
        (job_name, table_name, file_hash),
    )
    return cur.fetchone() is not None


def _insert_history(conn, job_name: str, table_name: str, csv_file: str,
                    file_hash: str, file_size: int, mtime: str):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO _LOAD_HISTORY
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_name, table_name, csv_file, file_hash, file_size, mtime, now_str()),
    )
    conn.commit()


def _table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _infer_sqlite_type(values: list) -> str:
    """샘플 값으로 SQLite 컬럼 타입 추론"""
    non_empty = [v for v in values if v.strip() != ""]
    if not non_empty:
        return "TEXT"

    try:
        [int(v) for v in non_empty]
        return "INTEGER"
    except ValueError:
        pass

    try:
        [float(v) for v in non_empty]
        return "REAL"
    except ValueError:
        pass

    return "TEXT"


def _get_table_columns(conn, table_name: str) -> set:
    """테이블 컬럼명 집합 반환 (대소문자 원본 유지)"""
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table_name}")')
    return {row[1] for row in cur.fetchall()}


def _delete_by_params(conn, table_name: str, params: dict):
    """params 기반 WHERE 조건으로 DELETE 실행 (엄격 모드: params 필수, 컬럼 매칭 필수)"""
    table_cols = _get_table_columns(conn, table_name)
    matched, _ = build_delete_condition(params, table_cols, table_name)

    where = " AND ".join(f'"{col}" = ?' for col, _ in matched)
    values = [val for _, val in matched]
    cur = conn.cursor()
    cur.execute(f'DELETE FROM "{table_name}" WHERE {where}', values)
    conn.commit()
    logger.info("DELETE %s | %d rows | WHERE %s", table_name, cur.rowcount, where)


def _find_meta_file(csv_path: Path) -> Path | None:
    """CSV와 같은 디렉토리에서 대응하는 .meta.json 탐색."""
    name = csv_path.name
    stem = name[:-len(".csv.gz")] if name.endswith(".csv.gz") else name[:-len(".csv")]
    meta = csv_path.parent / (stem + ".meta.json")
    return meta if meta.exists() else None


def _meta_type_to_sqlite(col: dict) -> str:
    """meta.json 컬럼 정보 → SQLite DDL 타입 문자열 변환."""
    t = col.get("type", "").upper()
    precision = col.get("precision")
    scale = col.get("scale")

    if "NUMBER" in t or "BINARY_DOUBLE" in t or "BINARY_FLOAT" in t:
        if precision and scale and scale > 0:
            return "REAL"
        return "INTEGER"
    if "FLOAT" in t:
        return "REAL"
    if "DATE" in t or "TIMESTAMP" in t:
        return "TEXT"
    if "BLOB" in t or "RAW" in t:
        return "BLOB"
    return "TEXT"


def _create_table_from_csv(conn, table_name: str, csv_path: Path):
    """CSV 헤더 + 메타 JSON(우선) 또는 샘플 100행으로 SQLite 테이블 자동 생성"""
    # 메타 파일 우선 탐색 → 소스 타입 그대로 생성
    meta_file = _find_meta_file(csv_path)
    if meta_file:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        columns = meta["columns"] if isinstance(meta, dict) and "columns" in meta else meta
        col_defs = [f'  "{col["name"]}" {_meta_type_to_sqlite(col)}' for col in columns]
        ddl = f'CREATE TABLE "{table_name}" (\n' + ",\n".join(col_defs) + "\n)"
        logger.info("CREATE TABLE %s (from source metadata)", table_name)
        logger.debug("DDL:\n%s", ddl)
        conn.execute(ddl)
        conn.commit()
        return

    # fallback: CSV 샘플 기반 타입 추론
    logger.debug("No .meta.json found, inferring types from CSV samples")
    open_fn = gzip.open if str(csv_path).endswith(".gz") else open

    with open_fn(csv_path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        samples = [[] for _ in headers]
        for i, row in enumerate(reader):
            if i >= 100:
                break
            for j, val in enumerate(row):
                if j < len(samples):
                    samples[j].append(val)

    col_defs = []
    for col, vals in zip(headers, samples):
        sqlite_type = _infer_sqlite_type(vals)
        col_defs.append(f'  "{col}" {sqlite_type}')

    ddl = f'CREATE TABLE "{table_name}" (\n' + ",\n".join(col_defs) + "\n)"

    logger.info("CREATE TABLE %s (from CSV inference)", table_name)
    logger.debug("DDL:\n%s", ddl)

    conn.execute(ddl)
    conn.commit()


def load_csv(conn, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str,
             load_mode: str = "replace", params: dict = None) -> int:
    """
    CSV를 SQLite 테이블에 적재. (pandas 미사용 → numexpr 로그 없음)
    테이블이 없으면 CSV 헤더 기반으로 자동 생성.
    load_mode: replace(DROP+CREATE) | truncate(DELETE ALL) | delete(params WHERE) | append(INSERT)
    반환값: 적재된 row 수 (-1이면 skip)
    """
    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    # replace/truncate 시 히스토리 체크 스킵
    if load_mode == "append":
        if mode != "retry" and _history_exists(conn, job_name, table_name, file_hash):
            logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
            return -1

    if load_mode == "replace" and _table_exists(conn, table_name):
        logger.info("LOAD mode=replace → DROP TABLE %s", table_name)
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.commit()

    if load_mode == "truncate" and _table_exists(conn, table_name):
        logger.info("LOAD mode=truncate → DELETE FROM %s", table_name)
        conn.execute(f'DELETE FROM "{table_name}"')
        conn.commit()

    if load_mode == "delete" and _table_exists(conn, table_name):
        _delete_by_params(conn, table_name, params or {})

    # 테이블 없으면 자동 생성
    if not _table_exists(conn, table_name):
        logger.info("Table not found, creating: %s", table_name)
        _create_table_from_csv(conn, table_name, csv_path)
    else:
        logger.debug("Table exists: %s", table_name)

    start = time.time()
    total_rows = 0

    open_fn = gzip.open if str(csv_path).endswith(".gz") else open
    with open_fn(csv_path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        col_list = ", ".join(f'"{h}"' for h in headers)
        placeholders = ", ".join(["?" for _ in headers])
        insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'

        batch = []
        batch_size = 1000
        cur = conn.cursor()

        for row in reader:
            # 빈 문자열 → None (SQLite NULL)
            batch.append([v if v.strip() != "" else None for v in row])
            total_rows += 1
            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                batch.clear()

        if batch:
            cur.executemany(insert_sql, batch)

    conn.commit()

    _insert_history(conn, job_name, table_name, str(csv_path), file_hash, file_size, mtime)

    elapsed = time.time() - start
    if total_rows == 0:
        logger.info("LOAD done | table=%s rows=0 (empty) elapsed=%.2fs | mode=%s",
                     table_name, elapsed, load_mode)
    else:
        logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs | mode=%s",
                     table_name, total_rows, elapsed, load_mode)

    return total_rows


def connect(db_path: Path):
    import sqlite3
    return sqlite3.connect(str(db_path))