# file: v2/adapters/sources/vertica_source.py

import csv
import gzip
import time
from pathlib import Path
from engine.runtime_state import stop_event


def export_sql_to_csv(
    conn,
    sql_text,
    out_file,
    logger,
    compression="none",
    fetch_size=10000,
    stall_seconds=1800,
    log_prefix="",
):
    cursor = conn.cursor()

    try:
        cursor.execute(sql_text)

        if cursor.description is None:
            logger.warning("No result set returned, skipping CSV export")
            return 0

        columns = [col[0] for col in cursor.description]

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

                    writer.writerows(rows)
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
            pass
