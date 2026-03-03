# file: engine/context.py

import logging
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RunContext:
    job_name: str
    run_id: str
    job_config: dict
    env_config: dict
    params: dict
    work_dir: Path
    mode: str
    logger: logging.Logger = field(repr=False)
    include_patterns: list = field(default_factory=list)  # --include 패턴 목록 (export/load)
    include_transform_patterns: list = field(default_factory=list)  # --include-transform 패턴 목록
    include_report_patterns: list = field(default_factory=list)     # --include-report 패턴 목록
    stage_filter: list = field(default_factory=list)      # --stage 필터 목록
    param_mode: str = "product"   # "product" (카르테시안 곱) | "zip" (위치별 1:1 매칭)
