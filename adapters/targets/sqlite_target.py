# file: v2/adapters/targets/sqlite_target.py

import csv
import gzip
import time
import logging
from datetime import datetime
from pathlib import Path

from engine.connection import now_str

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


def _create_table_from_csv(conn, table_name: str, csv_path: Path):
    """CSV 헤더 + 샘플 100행으로 SQLite 테이블 자동 생성"""
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

    logger.info("CREATE TABLE %s", table_name)
    logger.debug("DDL:\n%s", ddl)

    conn.execute(ddl)
    conn.commit()


def load_csv(conn, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str,
             load_mode: str = "replace") -> int:
    """
    CSV를 SQLite 테이블에 적재. (pandas 미사용 → numexpr 로그 없음)
    테이블이 없으면 CSV 헤더 기반으로 자동 생성.
    load_mode: replace(DROP+CREATE) | truncate(DELETE+INSERT) | append(INSERT)
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
    logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs | mode=%s",
                table_name, total_rows, elapsed, load_mode)

    return total_rows


def connect(db_path: Path):
    import sqlite3
    return sqlite3.connect(str(db_path))