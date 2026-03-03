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
    stage_results: dict = field(default_factory=dict, repr=False)  # 스테이지별 실행 결과

    def report_stage_result(self, stage_name: str, *,
                            success: int = 0, failed: int = 0,
                            skipped: int = 0, detail: str = ""):
        """스테이지 실행 결과 기록."""
        self.stage_results[stage_name] = {
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "detail": detail,
        }

    def get_stage_params(self, stage_name: str) -> dict:
        """스테이지별 params 반환. 스테이지 section에 params가 있으면 그것만 사용, 없으면 글로벌 params."""
        stage_cfg = self.job_config.get(stage_name, {})
        stage_params = stage_cfg.get("params")
        if stage_params is not None:
            return dict(stage_params)
        return dict(self.params)

    def get_stage_param_mode(self, stage_name: str) -> str:
        """스테이지별 param_mode 반환. 스테이지 section 우선, 없으면 글로벌."""
        stage_cfg = self.job_config.get(stage_name, {})
        return stage_cfg.get("param_mode") or self.param_mode
