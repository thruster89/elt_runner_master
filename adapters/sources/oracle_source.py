import csv
import gzip
import json
import time
from pathlib import Path
from engine.runtime_state import stop_event


def _describe_to_meta(description) -> list[dict]:
    """cursor.description → 직렬화 가능한 컬럼 메타 리스트 변환.

    oracledb cursor.description 튜플:
      (name, type, display_size, internal_size, precision, scale, null_ok)
    type 객체의 name 속성: 'DB_TYPE_VARCHAR', 'DB_TYPE_NUMBER' 등
    """
    meta = []
    for col in description:
        name, dbtype, display_size, internal_size, precision, scale, null_ok = col
        type_name = getattr(dbtype, "name", str(dbtype))  # 'DB_TYPE_VARCHAR' 등
        entry = {"name": name, "type": type_name}
        if internal_size is not None:
            entry["size"] = internal_size
        if precision is not None:
            entry["precision"] = precision
        if scale is not None:
            entry["scale"] = scale
        meta.append(entry)
    return meta

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
    """
    fetchmany 기반 고속 CSV export

    stall_seconds:
      - fetch/execute가 예외 없이 멈추는(hang) 케이스 대응용
      - 가능한 경우 Oracle driver의 call_timeout을 설정해서 stall을 예외로 전환
    """

    cursor = conn.cursor()

    try:
        # fetch 성능
        cursor.arraysize = fetch_size

        # stall 대응: call_timeout (가능한 경우만)
        # - python-oracledb에서 ms 단위
        call_timeout_ms = int(stall_seconds * 1000)

        # connection 레벨
        if hasattr(conn, "call_timeout"):
            try:
                conn.call_timeout = call_timeout_ms
            except Exception:
                pass

        # cursor 레벨(지원되는 경우)
        if hasattr(cursor, "call_timeout"):
            try:
                cursor.call_timeout = call_timeout_ms
            except Exception:
                pass

        cursor.execute(sql_text)

        if cursor.description is None:
            logger.warning("No result set returned, skipping CSV export")
            return 0

        columns = [col[0] for col in cursor.description]
        col_meta = _describe_to_meta(cursor.description)

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
                    # fetchmany block 구간
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break

                    writer.writerows(rows)
                    total_rows += len(rows)

                    # 진행 로그
                    if total_rows % (fetch_size * 5) == 0:
                        logger.info("%s CSV progress: %d rows", log_prefix, total_rows)
                        last_log_ts = time.time()
                    else:
                        # heartbeat 로그 (2분 간격)
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

            # 컬럼 + 파라미터 메타데이터 사이드카 저장 (.meta.json)
            csv_name = out_file.name
            csv_stem = csv_name[:-len(".csv.gz")] if csv_name.endswith(".csv.gz") else csv_name[:-len(".csv")]
            meta_file = out_file.parent / (csv_stem + ".meta.json")
            meta_data = {"columns": col_meta}
            if params:
                meta_data["params"] = params
            meta_file.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2),
                                 encoding="utf-8")
            logger.debug("Column metadata saved: %s", meta_file.name)

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
        # cursor 누수 방지 (핵심 보강 포인트)
        try:
            cursor.close()
        except Exception:
            pass
