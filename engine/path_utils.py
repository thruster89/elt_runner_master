# file: v2/engine/path_utils.py

from pathlib import Path


def resolve_path(ctx, path_str: str) -> Path:
    """
    상대경로 → work_dir 기준 변환
    절대경로 → 그대로 반환
    """
    p = Path(path_str)

    if p.is_absolute():
        return p

    return ctx.work_dir / p


def get_job_dir(work_dir: Path, job_name: str) -> Path | None:
    """jobs/{job_name}/ 폴더가 존재하면 그 경로를 반환, 없으면 None."""
    job_dir = work_dir / "jobs" / job_name
    return job_dir if job_dir.is_dir() else None


def get_job_defaults(work_dir: Path, job_name: str, target_type: str = "duckdb") -> dict:
    """Job convention 기반 기본 경로 반환.

    jobs/{job_name}/ 폴더가 있으면 그 하위 경로를 기본값으로 사용 (job-centric).
    없으면 기존 글로벌 경로를 기본값으로 반환.

    Returns:
        {
            "job_dir_exists": bool,
            "export_sql_dir": str,
            "export_out_dir": str,
            "transform_sql_dir": str,
            "report_sql_dir": str,
            "report_out_dir": str,
            "target_db_path": str,
            "tracking_dir_transform": str,
            "tracking_dir_report": str,
        }
    """
    job_dir = get_job_dir(work_dir, job_name)

    if job_dir:
        base = f"jobs/{job_name}"
        db_ext = "duckdb" if target_type == "duckdb" else "sqlite"
        return {
            "job_dir_exists": True,
            "export_sql_dir":          f"{base}/sql/export",
            "export_out_dir":          f"{base}/data/export",
            "transform_sql_dir":       f"{base}/sql/transform",
            "report_sql_dir":          f"{base}/sql/report",
            "report_out_dir":          f"{base}/data/report",
            "target_db_path":          f"{base}/data/{job_name}.{db_ext}",
            "tracking_dir_transform":  f"{base}/data/transform",
            "tracking_dir_report":     f"{base}/data/report_tracking",
        }

    # 기존 글로벌 기본값 (하위 호환)
    return {
        "job_dir_exists": False,
        "export_sql_dir":          "sql/export",
        "export_out_dir":          "data/export",
        "transform_sql_dir":       f"sql/transform/{target_type}",
        "report_sql_dir":          "sql/report",
        "report_out_dir":          "data/report",
        "target_db_path":          f"data/local/result.{('duckdb' if target_type == 'duckdb' else 'sqlite')}",
        "tracking_dir_transform":  "data/transform",
        "tracking_dir_report":     "data/report_tracking",
    }
