# file: v2/adapters/targets/oracle_target.py

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


def _qualified(schema: str, name: str) -> str:
    """schema가 있으면 "SCHEMA"."NAME", 없으면 "NAME" """
    if schema:
        return f'"{schema.upper()}"."{name.upper()}"'
    return f'"{name.upper()}"'


# --------------------------------------------------
# 스키마(유저) 자동 생성
# --------------------------------------------------

def _schema_exists(cur, schema: str) -> bool:
    cur.execute(
        "SELECT COUNT(1) FROM dba_users WHERE username = :1",
        (schema.upper(),),
    )
    return cur.fetchone()[0] > 0


def _ensure_schema(cur, conn, schema: str, password: str):
    """
    스키마(유저)가 없으면 CREATE USER + 기본 권한 부여.
    DBA 권한이 있는 접속 유저에서만 동작.
    password: 새 유저 비밀번호 (없으면 schema 이름과 동일하게 설정)
    """
    if _schema_exists(cur, schema):
        logger.debug("Schema exists: %s", schema.upper())
        return

    if not password:
        logger.warning(
            "schema_password 미설정 — 스키마명(%s)을 비밀번호로 사용합니다. "
            "운영 환경에서는 job.yml에 schema_password를 명시하세요.",
            schema.upper(),
        )
    pwd = password or schema
    s = schema.upper()

    logger.info("Schema not found, creating: %s", s)

    cur.execute(f'CREATE USER "{s}" IDENTIFIED BY "{pwd}"')
    cur.execute(f'GRANT CONNECT, RESOURCE TO "{s}"')
    cur.execute(f'GRANT UNLIMITED TABLESPACE TO "{s}"')
    conn.commit()

    logger.info("CREATE USER %s + GRANT done", s)


# --------------------------------------------------
# _LOAD_HISTORY 관리
# --------------------------------------------------

def _ensure_history(cur, conn, schema: str = None):
    owner = schema.upper() if schema else None
    if owner:
        cur.execute(
            "SELECT COUNT(1) FROM all_tables WHERE owner = :1 AND table_name = '_LOAD_HISTORY'",
            (owner,),
        )
    else:
        cur.execute("SELECT COUNT(1) FROM user_tables WHERE table_name = '_LOAD_HISTORY'")

    if cur.fetchone()[0] == 0:
        tbl = _qualified(schema, "_LOAD_HISTORY")
        cur.execute(f"""
            CREATE TABLE {tbl} (
                job_name   VARCHAR2(100),
                table_name VARCHAR2(200),
                csv_file   VARCHAR2(500),
                file_hash  VARCHAR2(64),
                file_size  NUMBER,
                mtime      VARCHAR2(30),
                loaded_at  VARCHAR2(30)
            )
        """)
        conn.commit()
        logger.info("CREATE TABLE %s", tbl)


def _history_exists(cur, schema: str, job_name: str, table_name: str, file_hash: str) -> bool:
    tbl = _qualified(schema, "_LOAD_HISTORY")
    cur.execute(
        f"SELECT COUNT(1) FROM {tbl} WHERE job_name=:1 AND table_name=:2 AND file_hash=:3",
        (job_name, table_name, file_hash),
    )
    return cur.fetchone()[0] > 0


def _insert_history(cur, conn, schema: str, job_name: str, table_name: str,
                    csv_file: str, file_hash: str, file_size: int, mtime: str):
    tbl = _qualified(schema, "_LOAD_HISTORY")
    cur.execute(
        f"""
        INSERT INTO {tbl}
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (:1, :2, :3, :4, :5, :6, :7)
        """,
        (job_name, table_name, csv_file, file_hash, file_size, mtime, now_str()),
    )
    conn.commit()


# --------------------------------------------------
# 테이블 자동 생성
# --------------------------------------------------

def _table_exists(cur, schema: str, table_name: str) -> bool:
    if schema:
        cur.execute(
            "SELECT COUNT(1) FROM all_tables WHERE owner = :1 AND table_name = :2",
            (schema.upper(), table_name.upper()),
        )
    else:
        cur.execute(
            "SELECT COUNT(1) FROM user_tables WHERE table_name = :1",
            (table_name.upper(),),
        )
    return cur.fetchone()[0] > 0


def _infer_oracle_type(values: list) -> str:
    non_empty = [v for v in values if v.strip() != ""]
    if not non_empty:
        return "VARCHAR2(4000)"
    try:
        [int(v) for v in non_empty]
        return "NUMBER"
    except ValueError:
        pass
    try:
        [float(v) for v in non_empty]
        return "NUMBER"
    except ValueError:
        pass
    max_len = max(len(v) for v in non_empty)
    if max_len <= 100:
        return "VARCHAR2(100)"
    elif max_len <= 500:
        return "VARCHAR2(500)"
    elif max_len <= 2000:
        return "VARCHAR2(2000)"
    else:
        return "VARCHAR2(4000)"


def _meta_type_to_oracle(col: dict) -> str:
    """meta.json 컬럼 정보 → Oracle DDL 타입 문자열 변환."""
    t = col.get("type", "").upper()
    size = col.get("size")
    precision = col.get("precision")
    scale = col.get("scale")

    # DB_TYPE_NUMBER, DB_TYPE_BINARY_DOUBLE 등
    if "NUMBER" in t or "BINARY_DOUBLE" in t or "BINARY_FLOAT" in t:
        if precision and scale:
            return f"NUMBER({precision},{scale})"
        elif precision:
            return f"NUMBER({precision})"
        return "NUMBER"
    if "FLOAT" in t:
        return f"FLOAT({precision})" if precision else "FLOAT"
    if "DATE" in t and "TIMESTAMP" not in t:
        return "DATE"
    if "TIMESTAMP" in t:
        if "TIME_ZONE" in t or "TZ" in t:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"
    if "CLOB" in t:
        return "CLOB"
    if "BLOB" in t:
        return "BLOB"
    if "NVARCHAR" in t or "NCHAR_VAR" in t:
        return f"NVARCHAR2({size})" if size else "NVARCHAR2(2000)"
    if "NCHAR" in t:
        return f"NCHAR({size})" if size else "NCHAR(1)"
    if "RAW" in t and "LONG" not in t:
        return f"RAW({size})" if size else "RAW(2000)"
    if "LONG_RAW" in t:
        return "LONG RAW"
    if "LONG" in t:
        return "LONG"
    if "CHAR" in t and "VAR" not in t:
        return f"CHAR({size})" if size else "CHAR(1)"
    # VARCHAR2 / default
    if size and size > 0:
        return f"VARCHAR2({size})"
    return "VARCHAR2(4000)"


def _find_meta_file(csv_path: Path) -> Path | None:
    """CSV와 같은 디렉토리에서 대응하는 .meta.json 탐색."""
    name = csv_path.name
    stem = name[:-len(".csv.gz")] if name.endswith(".csv.gz") else name[:-len(".csv")]
    meta = csv_path.parent / (stem + ".meta.json")
    return meta if meta.exists() else None


def _create_table_from_meta(cur, conn, schema: str, table_name: str, meta: list[dict]):
    """소스 메타데이터 기반으로 정확한 타입의 테이블 생성."""
    col_defs = [f'  "{col["name"].upper()}" {_meta_type_to_oracle(col)}' for col in meta]
    tbl = _qualified(schema, table_name)
    ddl = f"CREATE TABLE {tbl} (\n" + ",\n".join(col_defs) + "\n)"

    logger.info("CREATE TABLE %s (from source metadata)", tbl)
    logger.debug("DDL:\n%s", ddl)
    cur.execute(ddl)
    conn.commit()


def _create_table_from_csv(cur, conn, schema: str, table_name: str, csv_path: Path):
    # 메타 파일 우선 탐색 → 소스 타입 그대로 생성
    meta_file = _find_meta_file(csv_path)
    if meta_file:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        # 새 dict 구조 {"columns": [...], "params": {...}} 또는 기존 list 구조 호환
        columns = meta["columns"] if isinstance(meta, dict) and "columns" in meta else meta
        _create_table_from_meta(cur, conn, schema, table_name, columns)
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

    col_defs = [f'  "{col.upper()}" {_infer_oracle_type(vals)}'
                for col, vals in zip(headers, samples)]
    tbl = _qualified(schema, table_name)
    ddl = f"CREATE TABLE {tbl} (\n" + ",\n".join(col_defs) + "\n)"

    logger.info("CREATE TABLE %s (from CSV inference)", tbl)
    logger.debug("DDL:\n%s", ddl)
    cur.execute(ddl)
    conn.commit()


# --------------------------------------------------
# 메인 load 함수
# --------------------------------------------------

def _get_table_columns(cur, schema: str, table_name: str) -> list:
    """테이블의 컬럼명 목록 반환 (UPPER)"""
    if schema:
        cur.execute(
            "SELECT column_name FROM all_tab_columns WHERE owner = :1 AND table_name = :2 ORDER BY column_id",
            (schema.upper(), table_name.upper()),
        )
    else:
        cur.execute(
            "SELECT column_name FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
            (table_name.upper(),),
        )
    return [row[0] for row in cur.fetchall()]


def _delete_by_params(cur, conn, schema: str, table_name: str, params: dict):
    """params 기반 WHERE 조건으로 DELETE 실행 (엄격 모드: params 필수, 컬럼 매칭 필수)"""
    tbl = _qualified(schema, table_name)
    table_cols = set(_get_table_columns(cur, schema, table_name))
    matched, _ = build_delete_condition(params, table_cols, tbl)

    where = " AND ".join(f'"{col}" = :{i}' for i, (col, _) in enumerate(matched, 1))
    values = [val for _, val in matched]
    cur.execute(f"DELETE FROM {tbl} WHERE {where}", values)
    logger.info("DELETE %s | %d rows | WHERE %s", tbl, cur.rowcount, where)


def load_csv(conn, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str, schema: str = None,
             load_mode: str = "delete", params: dict = None) -> int:
    """
    CSV를 Oracle 테이블에 적재.
    schema 지정 시 해당 스키마에 테이블 생성/INSERT.
    테이블 없으면 CSV 헤더로 자동 생성.
    load_mode: replace(DROP+CREATE) | truncate(DELETE ALL) | delete(params WHERE) | append
    반환값: row 수 (-1이면 skip)
    """
    cur = conn.cursor()
    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    full_table = f"{schema.upper()}.{table_name.upper()}" if schema else table_name.upper()

    try:
        _ensure_history(cur, conn, schema)

        # append 모드에서만 히스토리 체크
        if load_mode == "append":
            if mode != "retry" and _history_exists(cur, schema, job_name, full_table, file_hash):
                logger.info("LOAD skip (already loaded) | %s | %s", full_table, csv_path.name)
                return -1

        # replace 모드: DROP TABLE → CREATE TABLE
        if load_mode == "replace" and _table_exists(cur, schema, table_name):
            tbl = _qualified(schema, table_name)
            logger.info("LOAD mode=replace → DROP TABLE %s", tbl)
            cur.execute(f"DROP TABLE {tbl} PURGE")
            hist_tbl = _qualified(schema, "_LOAD_HISTORY")
            cur.execute(
                f"DELETE FROM {hist_tbl} WHERE job_name = :1 AND table_name = :2",
                [job_name, full_table],
            )
            conn.commit()

        if not _table_exists(cur, schema, table_name):
            logger.info("Table not found, creating: %s", _qualified(schema, table_name))
            _create_table_from_csv(cur, conn, schema, table_name, csv_path)
        else:
            logger.debug("Table exists: %s", _qualified(schema, table_name))
            # truncate 모드: 테이블 구조 유지, 데이터 전체 삭제
            if load_mode == "truncate":
                tbl = _qualified(schema, table_name)
                logger.info("LOAD mode=truncate → TRUNCATE TABLE %s", tbl)
                cur.execute(f"TRUNCATE TABLE {tbl}")
            # delete 모드: INSERT 전 기존 데이터 삭제
            elif load_mode == "delete":
                _delete_by_params(cur, conn, schema, table_name, params or {})

        start = time.time()
        total_rows = 0
        tbl = _qualified(schema, table_name)

        open_fn = gzip.open if str(csv_path).endswith(".gz") else open
        with open_fn(csv_path, "rt", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            col_list = ", ".join(f'"{h.upper()}"' for h in headers)
            placeholders = ", ".join([f":{j + 1}" for j in range(len(headers))])
            insert_sql = f"INSERT INTO {tbl} ({col_list}) VALUES ({placeholders})"

            batch = []
            for row in reader:
                batch.append([v if v.strip() != "" else None for v in row])
                total_rows += 1
                if len(batch) >= 1000:
                    cur.executemany(insert_sql, batch)
                    batch.clear()
            if batch:
                cur.executemany(insert_sql, batch)

        conn.commit()
        _insert_history(cur, conn, schema, job_name, full_table, str(csv_path),
                        file_hash, file_size, mtime)

        elapsed = time.time() - start
        if total_rows == 0:
            logger.info("LOAD done | table=%s rows=0 (empty) elapsed=%.2fs | mode=%s",
                         full_table, elapsed, load_mode)
        else:
            logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs | mode=%s",
                         full_table, total_rows, elapsed, load_mode)
        return total_rows

    finally:
        cur.close()


# --------------------------------------------------
# 연결 (target은 항상 thin 모드)
# --------------------------------------------------

def connect(env_config: dict, schema: str = None, schema_password: str = None):
    """
    target 연결은 항상 thin 모드 (Instant Client 불필요).
    schema 지정 시 해당 스키마 유저가 없으면 자동 생성 (DBA 권한 필요).
    """
    import oracledb

    oracle_cfg = env_config.get("sources", {}).get("oracle", {})
    if not oracle_cfg:
        raise RuntimeError("oracle config not found in env_config['sources']['oracle']")

    host_cfg = oracle_cfg.get("hosts", {}).get("local")
    if not host_cfg:
        raise RuntimeError("Oracle target requires hosts.local in env.yml")

    # target은 항상 thin 모드로 직접 연결
    conn = oracledb.connect(
        user=host_cfg["user"],
        password=host_cfg["password"],
        dsn=host_cfg["dsn"],
        expire_time=10,  # 10분마다 TCP keepalive → 방화벽 idle timeout 방지
    )
    logger.info("Oracle target connected (thin) | dsn=%s | user=%s", host_cfg["dsn"], host_cfg["user"])

    # CSV 날짜 문자열(Python datetime.__str__)과 Oracle DATE/TIMESTAMP 호환 설정
    _cur = conn.cursor()
    try:
        _cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        _cur.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'")
        _cur.execute("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'")
        logger.debug("NLS date/timestamp formats set for CSV compatibility")
    finally:
        _cur.close()

    # 스키마 자동 생성 (schema 지정된 경우)
    if schema:
        cur = conn.cursor()
        try:
            _ensure_schema(cur, conn, schema, schema_password)
        finally:
            cur.close()

    return conn