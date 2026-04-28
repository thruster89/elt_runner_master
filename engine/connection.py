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


def _default_memory_limit() -> str:
    """현재 가용 메모리의 60%를 GB 단위로 반환한다 (Windows/Linux 모두 지원).

    기존에는 total RAM의 75%를 사용했으나, 다른 프로세스(Oracle client 등)가
    메모리를 점유하고 있을 때 OOM이 발생할 수 있어 available 기준으로 변경.
    """
    try:
        import psutil
        available = psutil.virtual_memory().available
    except ImportError:
        # psutil 없으면 OS별 분기
        import sys, os
        if sys.platform == "win32":
            import ctypes
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            stat = MEMORYSTATUSEX(dwLength=ctypes.sizeof(MEMORYSTATUSEX))
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            available = stat.ullAvailPhys
        else:
            try:
                # /proc/meminfo 에서 MemAvailable 읽기
                with open("/proc/meminfo") as f:
                    for line in f:
                        if line.startswith("MemAvailable:"):
                            available = int(line.split()[1]) * 1024  # kB → bytes
                            break
                    else:
                        return "4GB"
            except (OSError, ValueError):
                return "4GB"
    except Exception:
        return "4GB"
    gb = max(1, int(available * 0.60 / (1024 ** 3)))
    return f"{gb}GB"


def _default_threads() -> int:
    """논리 CPU 수의 절반 (최소 1)."""
    import os
    return max(1, (os.cpu_count() or 2) // 2)


def _apply_duckdb_settings(conn, target_cfg: dict, db_path=None):
    """DuckDB 연결에 memory_limit, threads, temp_directory 등 SET 옵션을 적용한다."""
    memory_limit = (target_cfg.get("memory_limit") or "").strip()
    threads = (target_cfg.get("threads") or "")
    if isinstance(threads, int):
        threads = str(threads)
    else:
        threads = threads.strip()

    if not memory_limit:
        memory_limit = _default_memory_limit()
    if not threads:
        threads = str(_default_threads())

    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET threads = {int(threads)}")

    # temp_directory 설정: YAML 지정값 > db파일.tmp > 시스템 임시 폴더
    temp_dir = (target_cfg.get("temp_directory") or "").strip()
    if not temp_dir and db_path is not None:
        from pathlib import Path
        default_tmp = Path(str(db_path) + ".tmp")
        try:
            default_tmp.mkdir(parents=True, exist_ok=True)
            temp_dir = str(default_tmp.resolve())
        except OSError:
            # 기본 경로 생성 실패 시 시스템 임시 폴더의 고정 위치 재사용 (누적 방지)
            import tempfile
            stable_tmp = Path(tempfile.gettempdir()) / "duckdb_runner_tmp"
            stable_tmp.mkdir(parents=True, exist_ok=True)
            temp_dir = str(stable_tmp)
            _log.warning("기본 temp 경로 생성 실패, 시스템 임시 폴더 사용: %s", temp_dir)
    if temp_dir:
        from pathlib import Path
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        conn.execute(f"SET temp_directory = '{temp_dir}'")

    _log.info("DuckDB SET memory_limit = '%s', threads = %s, temp_directory = '%s'",
              memory_limit, threads, temp_dir or "(default)")


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
        db_path = resolve_path(ctx, target_cfg.get("db_path", ctx.get_default("target_db_path")))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect(db_path)
        # DuckDB SET 옵션 적용 (memory_limit, threads, temp_directory 등)
        _apply_duckdb_settings(conn, target_cfg, db_path=db_path)
        label = f"duckdb ({db_path.resolve()})"
        return conn, "duckdb", label

    elif tgt_type == "sqlite3":
        from adapters.targets.sqlite_target import connect
        db_path = resolve_path(ctx, target_cfg.get("db_path", ctx.get_default("target_db_path")))
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
