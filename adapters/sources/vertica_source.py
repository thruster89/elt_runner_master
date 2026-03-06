# file: v2/adapters/sources/vertica_source.py

import csv
import gzip
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from engine.runtime_state import stop_event

# 날짜 컬럼 접미사 패턴 (대소문자 무시)
# DT, _ST, _CLSTR, _DTHMS 로 끝나는 컬럼을 자동 포맷팅
_DATE_COL_RE = re.compile(r"(?:DT|_ST|_CLSTR|_DTHMS)$", re.IGNORECASE)

# YYYYMMDD (8자리) 또는 YYYYMMDD HH:MM:SS / YYYYMMDD HH24:MI:SS 패턴
_YYYYMMDD_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})([ T]\d{2}:\d{2}:\d{2}.*)?$")


def _normalize_date_value(val):
    """날짜 컬럼 값을 YYYY-MM-DD 형식으로 정규화.

    - Python datetime/date 객체 → 문자열 변환
    - 'YYYYMMDD...' 문자열 → 'YYYY-MM-DD...' 로 변환
    - 이미 'YYYY-MM-DD' 형식이면 그대로 반환
    """
    if val is None or val == "":
        return val
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    m = _YYYYMMDD_RE.match(s)
    if m:
        formatted = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if m.group(4):
            formatted += m.group(4)
        return formatted
    return s


def export_sql_to_csv(
    conn,
    sql_text,
    out_file,
    logger,
    compression="none",
    fetch_size=10000,
    stall_seconds=1800,
    log_prefix="",
    params=None,
):
    cursor = conn.cursor()

    try:
        cursor.execute(sql_text)

        if cursor.description is None:
            logger.warning("No result set returned, skipping CSV export")
            return 0

        columns = [col[0] for col in cursor.description]

        # 날짜 컬럼 인덱스 감지 (DT, _ST, _CLSTR, _DTHMS 접미사)
        date_col_indices = [i for i, c in enumerate(columns) if _DATE_COL_RE.search(c)]
        if date_col_indices:
            logger.debug(
                "Auto date-format columns detected: %s",
                [columns[i] for i in date_col_indices],
            )

        out_file = Path(out_file)
        tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
        out_file.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        last_log_ts = time.time()

        try:
            if compression == "gzip":
                f = gzip.open(tmp_file, "wt", newline="", encoding="utf-8")
            else:
                f = open(tmp_file, "w", newline="", encoding="utf-8")

            interrupted = False
            with f:
                writer = csv.writer(f)
                writer.writerow(columns)

                while True:
                    if stop_event.is_set():
                        logger.warning("%s Export interrupted", log_prefix)
                        interrupted = True
                        break

                    fetch_start = time.time()
                    rows = cursor.fetchmany(fetch_size)
                    fetch_elapsed = time.time() - fetch_start

                    if not rows:
                        break

                    if fetch_elapsed > stall_seconds:
                        raise RuntimeError(
                            f"Fetch stalled > {stall_seconds}s (took {fetch_elapsed:.0f}s)"
                        )

                    for row in rows:
                        out = ["" if v is None else v for v in row]
                        if date_col_indices:
                            for i in date_col_indices:
                                if out[i] != "":
                                    out[i] = _normalize_date_value(out[i])
                        writer.writerow(out)
                    total_rows += len(rows)

                    if total_rows % (fetch_size * 5) == 0:
                        logger.info("%s CSV progress: %d rows", log_prefix, total_rows)
                        last_log_ts = time.time()
                    else:
                        now = time.time()
                        if now - last_log_ts >= 120:
                            logger.info("%s CSV progress: %d rows (heartbeat)", log_prefix, total_rows)
                            last_log_ts = now

            if interrupted:
                if tmp_file.exists():
                    tmp_file.unlink()
                logger.warning("Incomplete file removed: %s", out_file.name)
                return total_rows

            tmp_file.replace(out_file)
            logger.debug("File committed: %s", out_file)

            # 파라미터 메타데이터 사이드카 저장 (.meta.json)
            if params:
                csv_name = out_file.name
                csv_stem = csv_name[:-len(".csv.gz")] if csv_name.endswith(".csv.gz") else csv_name[:-len(".csv")]
                meta_file = out_file.parent / (csv_stem + ".meta.json")
                meta_data = {"params": params}
                meta_file.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2),
                                     encoding="utf-8")
                logger.debug("Params metadata saved: %s", meta_file.name)

            logger.info(
                "%s CSV export completed | rows=%d file=%s",
                log_prefix,
                total_rows,
                out_file,
            )

        except Exception:
            if tmp_file.exists():
                tmp_file.unlink()
            raise

        return total_rows

    finally:
        try:
            cursor.close()
        except Exception:
            logger.debug("cursor close 실패", exc_info=True)
