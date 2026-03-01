"""
gui/app.py  ─  BatchRunnerGUI 클래스 (Mixin 조립 + __init__)
"""

import os
import subprocess
import tkinter as tk
from pathlib import Path

from gui.constants import C, THEMES, APP_VERSION
from gui.mixins.ui_build import UiBuildMixin
from gui.mixins.run_control import RunControlMixin
from gui.mixins.state_job import StateJobMixin
from gui.mixins.log_panel import LogPanelMixin
from gui.mixins.search import SearchMixin
from gui.mixins.dialogs import DialogsMixin


class BatchRunnerGUI(
    UiBuildMixin,
    RunControlMixin,
    StateJobMixin,
    LogPanelMixin,
    SearchMixin,
    DialogsMixin,
    tk.Tk,
):
    def __init__(self):
        super().__init__()
        self.title(f"ELT Runner  v{APP_VERSION}")
        self.geometry("1340x800")
        self.minsize(1000, 620)
        self.configure(bg=C["base"])

        self._process: subprocess.Popen | None = None
        self._work_dir = tk.StringVar(value=str(Path(".").resolve()))
        self._selected_sqls: set[str] = set()

        self._jobs: dict = {}
        self._env_hosts: dict = {}
        _init_theme = os.environ.pop("ELT_GUI_THEME", None)
        self._is_new_window = bool(os.environ.pop("ELT_GUI_NEW_WINDOW", None))
        if _init_theme and _init_theme in THEMES:
            self._theme_var = tk.StringVar(value=_init_theme)
            self._theme_from_env = True
            C.update(THEMES[_init_theme])
        else:
            self._theme_var = tk.StringVar(value="Mocha")
            self._theme_from_env = False

        # Job / Run Mode
        self.job_var = tk.StringVar()
        self.mode_var = tk.StringVar(value="run")
        self._env_path_var = tk.StringVar(value="config/env.yml")
        self._debug_var = tk.BooleanVar(value=False)

        # ── 1급 설정 변수 (Settings-First) ──────────────────────
        # Source
        self._source_type_var = tk.StringVar(value="oracle")
        self._source_host_var = tk.StringVar(value="")
        # Target
        self._target_type_var = tk.StringVar(value="duckdb")
        self._target_db_path  = tk.StringVar(value="data/local/result.duckdb")
        self._target_schema   = tk.StringVar(value="")
        # Export paths
        self._export_sql_dir  = tk.StringVar(value="sql/export")
        self._export_out_dir  = tk.StringVar(value="data/export")
        # Transform / Report paths
        self._transform_schema  = tk.StringVar(value="")
        self._transform_sql_dir = tk.StringVar(value="sql/transform/duckdb")
        self._report_sql_dir    = tk.StringVar(value="sql/report")
        self._report_out_dir    = tk.StringVar(value="data/report")
        self._report_schema     = tk.StringVar(value="")
        # Stages — 4개 고정 BooleanVar
        self._stage_export     = tk.BooleanVar(value=True)
        self._stage_load_local = tk.BooleanVar(value=True)
        self._stage_transform  = tk.BooleanVar(value=True)
        self._stage_report     = tk.BooleanVar(value=True)
        # Stage 버튼 dict (토글 버튼 참조용)
        self._stage_buttons: dict = {}

        # Advanced overrides
        self._ov_overwrite    = tk.BooleanVar(value=False)
        self._ov_workers      = tk.IntVar(value=1)
        self._ov_compression  = tk.StringVar(value="gzip")
        self._ov_load_mode    = tk.StringVar(value="replace")
        self._ov_on_error     = tk.StringVar(value="stop")
        self._ov_excel        = tk.BooleanVar(value=True)
        self._ov_csv          = tk.BooleanVar(value=True)
        self._ov_max_files    = tk.IntVar(value=10)
        self._ov_skip_sql     = tk.BooleanVar(value=False)
        self._ov_union_dir    = tk.StringVar(value="")
        self._ov_timeout      = tk.StringVar(value="1800")
        self._ov_name_style   = tk.StringVar(value="full")
        self._ov_strip_prefix = tk.BooleanVar(value=False)

        # Dirty flag (변경 감지)
        self._job_loaded_snapshot = None
        self._restoring_job = False

        # 최근 Work Dir 히스토리
        self._recent_dirs: list = []

        # 로그 필터
        self._log_filter = tk.StringVar(value="ALL")
        self._log_raw_lines: list[tuple[str, str, str]] = []  # (text, tag, timestamp)
        self._log_filter_btns: dict = {}
        self._show_time = tk.BooleanVar(value=False)

        # 예약 실행
        self._schedule_time = tk.StringVar(value="+30m / 18:00")
        self._schedule_id: str | None = None

        # 검색 상태
        self._search_var = tk.StringVar()
        self._search_matches = []
        self._search_match_idx = 0
        # 애니메이션 상태
        self._anim_id = None
        self._anim_dots = 0

        self._build_style()
        self._build_ui()
        self._reload_project()
        self._bind_shortcuts()
        self._load_geometry()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
