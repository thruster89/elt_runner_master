"""
gui/mixins/ui_build.py  ─  _build_* 메서드 전체 (UI 빌드)
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog
from pathlib import Path
from typing import TYPE_CHECKING

from gui.constants import C, FONTS, FONT_FAMILY, FONT_MONO, THEMES, TOOLTIPS, STAGE_CONFIG
from gui.widgets import CollapsibleSection, Tooltip

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI


class UiBuildMixin:

    def _build_style(self: "BatchRunnerGUI"):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TCombobox",
                        fieldbackground=C["surface0"],
                        background=C["surface1"],
                        foreground=C["text"],
                        selectbackground=C["surface1"],
                        selectforeground=C["text"],
                        insertcolor=C["text"],
                        arrowcolor=C["text"],
                        bordercolor=C["surface1"],
                        lightcolor=C["surface0"],
                        darkcolor=C["surface0"])
        # 드롭다운 팝업 Listbox 색상 — option_add 로 전역 설정
        self.option_add("*TCombobox*Listbox.background",  C["surface0"])
        self.option_add("*TCombobox*Listbox.foreground",  C["text"])
        self.option_add("*TCombobox*Listbox.selectBackground", C["blue"])
        self.option_add("*TCombobox*Listbox.selectForeground", C["crust"])
        self.option_add("*TCombobox*Listbox.font", f"{FONT_MONO} 10")
        style.configure("TSeparator", background=C["surface0"])
        style.configure("TScrollbar",
                        background=C["surface0"],
                        troughcolor=C["crust"],
                        arrowcolor=C["text"])
        style.configure("green.Horizontal.TProgressbar",
                        troughcolor=C["surface0"],
                        background=C["green"],
                        lightcolor=C["green"],
                        darkcolor=C["green"],
                        bordercolor=C["surface0"])
        # 선택된 텍스트 색상 명시 (Mocha/Nord에서 흰 bg + 흰 fg 방지)
        style.map("TCombobox",
                  fieldbackground=[("readonly", C["surface0"]),
                                   ("disabled", C["mantle"])],
                  foreground=[("readonly", C["text"]),
                               ("disabled", C["overlay0"])],
                  selectbackground=[("readonly", C["surface0"])],
                  selectforeground=[("readonly", C["text"])])

    # ── 마우스휠 ─────────────────────────────────────────────
    def _setup_mousewheel(self: "BatchRunnerGUI"):
        """콤보박스/스핀박스 마우스휠 차단 + 위치 기반 캔버스 스크롤"""
        self.bind_class("TCombobox", "<MouseWheel>", lambda e: "break")
        self.bind_class("Spinbox", "<MouseWheel>", lambda e: "break")

        def _on_mousewheel(e):
            try:
                mx, my = self.winfo_pointerxy()
                w = self.winfo_containing(mx, my)
                if w is None:
                    return
                # 위젯 → 부모 체인을 따라가며 Canvas 탐색
                widget = w
                while widget:
                    if isinstance(widget, (ttk.Combobox, tk.Spinbox, ttk.Spinbox)):
                        return "break"
                    if isinstance(widget, tk.Canvas):
                        widget.yview_scroll(-1 * (e.delta // 120), "units")
                        return "break"
                    try:
                        widget = widget.master
                    except Exception:
                        break
            except Exception:
                pass

        self.bind_all("<MouseWheel>", _on_mousewheel)

    # ── UI 조립 ──────────────────────────────────────────────
    def _build_ui(self: "BatchRunnerGUI"):
        # 상단 타이틀 바
        self._build_title_bar()

        # 메인 영역 (좌 + 우)
        main = tk.Frame(self, bg=C["base"])
        main.pack(fill="both", expand=True, padx=10, pady=(4, 0))

        left = tk.Frame(main, bg=C["mantle"], width=430)
        left.pack(side="left", fill="y", padx=(0, 8))
        left.pack_propagate(False)
        self._left_frame = left
        self._build_left(left)

        right = tk.Frame(main, bg=C["mantle"])
        right.pack(side="left", fill="both", expand=True)
        self._build_right(right)

        # 하단 버튼 바
        self._build_button_bar()

        # 마우스휠 바인딩 (위치 기반)
        self._setup_mousewheel()

    def _build_title_bar(self: "BatchRunnerGUI"):
        self._title_bar = tk.Frame(self, bg=C["crust"], pady=7)
        self._title_bar.pack(fill="x")
        bar = self._title_bar

        # Work Dir
        tk.Label(bar, text="Work Dir:", font=FONTS["body"],
                 bg=C["crust"], fg=C["subtext"]).pack(side="left", padx=(14, 4))
        self._wd_entry = ttk.Combobox(bar, textvariable=self._work_dir,
                            font=FONTS["mono"], width=60,
                            values=self._recent_dirs)
        self._wd_entry.pack(side="left", ipady=2)
        self._wd_entry.bind("<<ComboboxSelected>>", lambda _: self._reload_project())
        self._wd_entry.bind("<Return>", lambda _: self._reload_project())
        self._wd_btn = tk.Button(bar, text="…", font=FONTS["mono"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._browse_workdir)
        self._wd_btn.pack(side="left", padx=2)
        tk.Button(bar, text="\U0001f4c2", font=FONTS["mono"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=lambda: self._open_in_explorer(self._work_dir.get())
                  ).pack(side="left", padx=(0, 2))
        self._reload_btn = tk.Button(bar, text="↺ Reload", font=FONTS["button_sm"],
                  bg=C["blue"], fg=C["crust"], relief="flat", padx=8,
                  activebackground=C["sky"],
                  command=self._reload_project)
        self._reload_btn.pack(side="left", padx=6)

        # env yml
        tk.Label(bar, text="env:", font=FONTS["body"],
                 bg=C["crust"], fg=C["subtext"]).pack(side="left", padx=(10, 4))
        tk.Entry(bar, textvariable=self._env_path_var,
                 bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat",
                 font=FONTS["mono"], width=20).pack(side="left", ipady=2)

        # 테마 선택 (우측) — combo 먼저 pack해야 우측 끝에 배치
        self._theme_combo = ttk.Combobox(bar, textvariable=self._theme_var,
                                         values=list(THEMES.keys()),
                                         state="readonly", font=FONTS["mono_small"], width=16)
        self._theme_combo.pack(side="right", padx=(0, 10))
        self._theme_combo.bind("<<ComboboxSelected>>", lambda _: self._apply_theme())
        tk.Label(bar, text="Theme:", font=FONTS["small"],
                 bg=C["crust"], fg=C["subtext"]).pack(side="right", padx=(0, 4))

    # ── 좌측 옵션 패널 ───────────────────────────────────────
    def _build_left(self: "BatchRunnerGUI", parent):
        # 스크롤 가능하게
        canvas = tk.Canvas(parent, bg=C["mantle"], highlightthickness=0)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        inner = tk.Frame(canvas, bg=C["mantle"])
        win = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: (
            canvas.configure(scrollregion=canvas.bbox("all")),
            canvas.itemconfig(win, width=canvas.winfo_width())
        ))
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(win, width=e.width))

        self._left_canvas = canvas
        self._left_inner = inner
        self._build_option_sections(inner)

    def _build_option_sections(self: "BatchRunnerGUI", parent):
        tk.Label(parent, text="Settings", font=FONTS["h1"],
                 bg=C["mantle"], fg=C["text"]).pack(pady=(14, 4), padx=12, anchor="w")
        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        self._build_source_section(parent)      # 1. Source Type + Host       [펼침, teal]
        self._build_target_section(parent)      # 2. Target Type + DB/Schema  [펼침, mauve]
        self._build_paths_section(parent)       # 3. export.sql_dir, out_dir  [펼침, blue]
        self._build_stages_section(parent)      # 4. 4개 토글 버튼            [펼침, green]
        self._build_params_section(parent)      # 5. Params key=value         [펼침, green]
        self._build_advanced_section(parent)    # 6. SQL Filter / 세부 옵션   [접힘, sky]
        self._build_job_preset_section(parent)  # 7. Job + Presets + Run Mode [접힘, peach]

        # 변경 감지 → preview 갱신
        for ov_var in (self._ov_compression, self._ov_on_error,
                       self._ov_load_mode, self._ov_union_dir, self._ov_timeout,
                       self._export_sql_dir, self._export_out_dir,
                       self._target_db_path, self._target_schema,
                       self._transform_sql_dir, self._report_sql_dir,
                       self._report_out_dir, self._source_host_var):
            ov_var.trace_add("write", lambda *_: self._refresh_preview())
        for var in (self.job_var, self.mode_var, self._env_path_var, self._debug_var):
            var.trace_add("write", lambda *_: self._refresh_preview())
        # auto-suggest 트리거 (export / transform / report sql_dir 변경 시 파라미터 재스캔)
        self._export_sql_dir.trace_add("write", lambda *_: self.after(300, self._on_export_sql_dir_change))
        self._transform_sql_dir.trace_add("write", lambda *_: self.after(300, self._scan_and_suggest_params))
        self._report_sql_dir.trace_add("write", lambda *_: self.after(300, self._scan_and_suggest_params))
        self._target_type_var.trace_add("write", lambda *_: self._refresh_preview())

    # ── 헬퍼 ─────────────────────────────────────────────────
    def _entry_row(self: "BatchRunnerGUI", parent_frame, label, var, **kw):
        row = tk.Frame(parent_frame, bg=C["mantle"])
        row.pack(fill="x", padx=12, pady=2)
        tk.Label(row, text=label, font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        e = tk.Entry(row, textvariable=var, bg=C["surface0"], fg=C["text"],
                     insertbackground=C["text"], relief="flat",
                     font=FONTS["mono"], **kw)
        e.pack(side="left", fill="x", expand=True, ipady=2)
        return e

    def _ov_row(self: "BatchRunnerGUI", parent_frame, label, widget_fn, note="", tooltip=""):
        r = tk.Frame(parent_frame, bg=C["mantle"])
        r.pack(fill="x", padx=12, pady=2)
        lbl = tk.Label(r, text=label, font=FONTS["mono_small"], width=18, anchor="w",
                       bg=C["mantle"], fg=C["subtext"])
        lbl.pack(side="left")
        if tooltip:
            Tooltip(lbl, tooltip)
        widget_fn(r)
        if note:
            tk.Label(r, text=note, font=FONTS["shortcut"],
                     bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

    def _path_row(self: "BatchRunnerGUI", parent_frame, label, var, browse_title="Select folder"):
        """경로 입력 + ... 버튼 행 (경로+버튼 우측 정렬)"""
        row = tk.Frame(parent_frame, bg=C["mantle"])
        row.pack(fill="x", padx=12, pady=2)
        tk.Label(row, text=label, font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=18, anchor="w").pack(side="left")
        def _browse():
            wd = self._work_dir.get()
            d = filedialog.askdirectory(initialdir=var.get() or wd, title=browse_title)
            if d:
                try:
                    rel = Path(d).relative_to(Path(wd))
                    var.set(rel.as_posix())
                except ValueError:
                    var.set(d)
        tk.Button(row, text="\U0001f4c2", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=lambda: self._open_in_explorer(var.get())).pack(side="right", padx=(2, 0))
        tk.Button(row, text="...", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=_browse).pack(side="right", padx=(2, 0))
        tk.Entry(row, textvariable=var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat",
                 font=FONTS["mono"], width=16).pack(side="right", fill="x", expand=True, ipady=2)

    # ── 1) Source ─────────────────────────────────────────────
    def _build_source_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Source", color_key="teal", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        # Source Type
        row1 = tk.Frame(body, bg=C["mantle"])
        row1.pack(fill="x", padx=12, pady=(8, 2))
        tk.Label(row1, text="Source Type", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        self._source_type_combo = ttk.Combobox(
            row1, textvariable=self._source_type_var,
            state="readonly", font=FONTS["mono"], width=10)
        self._source_type_combo.pack(side="left")
        self._source_type_combo.bind("<<ComboboxSelected>>", self._on_source_type_change)
        tk.Label(row1, text="overwrite", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left", padx=(10, 0))
        tk.Checkbutton(row1, variable=self._ov_overwrite, text="",
                       bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                       activebackground=C["mantle"],
                       command=self._refresh_preview).pack(side="left")

        # Host + timeout
        row2 = tk.Frame(body, bg=C["mantle"])
        row2.pack(fill="x", padx=12, pady=(2, 6))
        tk.Label(row2, text="Host", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        self._host_combo = ttk.Combobox(
            row2, textvariable=self._source_host_var,
            state="readonly", font=FONTS["mono"], width=10)
        self._host_combo.pack(side="left")
        self._host_combo.bind("<<ComboboxSelected>>", lambda _: self._refresh_preview())
        tk.Label(row2, text="timeout", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left", padx=(10, 4))
        tk.Entry(row2, textvariable=self._ov_timeout,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=FONTS["mono_small"], width=6).pack(side="left", ipady=2)
        tk.Label(row2, text="sec", font=FONTS["shortcut"],
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

    # ── 2) Target ─────────────────────────────────────────────
    def _build_target_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Target", color_key="mauve", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        # Target Type
        row1 = tk.Frame(body, bg=C["mantle"])
        row1.pack(fill="x", padx=12, pady=(8, 2))
        tk.Label(row1, text="Target Type", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        self._target_type_combo = ttk.Combobox(
            row1, textvariable=self._target_type_var,
            values=["duckdb", "sqlite3", "oracle"],
            state="readonly", font=FONTS["mono"], width=14)
        self._target_type_combo.pack(side="left", fill="x", expand=True)
        self._target_type_combo.bind("<<ComboboxSelected>>", self._on_target_type_change)

        # DB Path (duckdb/sqlite3)
        self._db_path_row = tk.Frame(body, bg=C["mantle"])
        self._db_path_row.pack(fill="x", padx=12, pady=2)
        tk.Label(self._db_path_row, text="DB Path", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        tk.Entry(self._db_path_row, textvariable=self._target_db_path,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=FONTS["mono"], width=16).pack(side="left", fill="x", expand=True, ipady=2)
        def _browse_db():
            d = filedialog.asksaveasfilename(
                initialdir=self._work_dir.get(),
                defaultextension=".duckdb",
                filetypes=[("DuckDB", "*.duckdb"), ("SQLite", "*.db *.sqlite3"), ("All", "*.*")],
                title="Select DB file")
            if d:
                try:
                    rel = Path(d).relative_to(Path(self._work_dir.get()))
                    self._target_db_path.set(rel.as_posix())
                except ValueError:
                    self._target_db_path.set(d)
        tk.Button(self._db_path_row, text="...", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=_browse_db).pack(side="left", padx=(2, 0))

        # Schema (oracle)
        self._schema_row = tk.Frame(body, bg=C["mantle"])
        tk.Label(self._schema_row, text="Schema", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        tk.Entry(self._schema_row, textvariable=self._target_schema,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=FONTS["mono"], width=16).pack(side="left", fill="x", expand=True, ipady=2)

        # Oracle target 힌트 (host=local 연결 안내)
        self._oracle_hint_row = tk.Frame(body, bg=C["mantle"])
        tk.Label(self._oracle_hint_row, text="⚠  Target Oracle → source.host=local 로 연결됩니다",
                 font=FONTS["small"], bg=C["mantle"], fg=C["yellow"]).pack(anchor="w", padx=14, pady=(2, 4))

        # Load Mode
        self._load_mode_row = tk.Frame(body, bg=C["mantle"])
        tk.Label(self._load_mode_row, text="Load Mode", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        self._load_mode_combo = ttk.Combobox(
            self._load_mode_row, textvariable=self._ov_load_mode,
            values=["replace", "truncate", "append"],
            state="readonly", font=FONTS["mono"], width=14)
        self._load_mode_combo.pack(side="left", fill="x", expand=True)

        self._load_note_oracle = tk.Label(body,
            text="oracle: replace (DROP+CREATE) / truncate / delete (param WHERE) / append",
            font=FONTS["small"], bg=C["mantle"], fg=C["overlay0"])
        self._load_note_other = tk.Label(body,
            text="duckdb/sqlite: replace (DROP+CREATE) / truncate / append",
            font=FONTS["small"], bg=C["mantle"], fg=C["overlay0"])

        self._update_target_visibility()

    # ── 3) Paths — Export ─────────────────────────────────────
    def _build_paths_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Paths \u2014 Export", color_key="blue", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        self._path_row(body, "export.sql_dir", self._export_sql_dir, "Select SQL dir")
        self._path_row(body, "export.out_dir", self._export_out_dir, "Select output dir")

    # ── 4) Stages — 토글 버튼 ────────────────────────────────
    def _build_stages_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Stages", color_key="yellow", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        btn_frame = tk.Frame(body, bg=C["mantle"])
        btn_frame.pack(fill="x", padx=12, pady=(8, 2))

        self._stage_buttons = {}
        for stage_key, label, color_key in STAGE_CONFIG:
            btn = tk.Button(btn_frame, text=label, font=(FONT_FAMILY, 9, "bold"),
                            relief="flat", width=9, pady=4, bd=0,
                            command=lambda sk=stage_key: self._toggle_stage(sk))
            btn.pack(side="left", padx=(0, 4), fill="x", expand=True)
            self._stage_buttons[stage_key] = (btn, color_key)

        self._refresh_stage_buttons()

        # all / none
        ctrl = tk.Frame(body, bg=C["mantle"])
        ctrl.pack(fill="x", padx=12, pady=(2, 6))
        for txt, cmd in [("all", self._stages_all), ("none", self._stages_none)]:
            tk.Button(ctrl, text=txt, font=FONTS["shortcut"],
                      bg=C["surface0"], fg=C["subtext"], relief="flat",
                      padx=5, pady=0, activebackground=C["surface1"],
                      command=cmd).pack(side="left", padx=(0, 6))

    # ── 5) Params ─────────────────────────────────────────────
    def _build_params_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Params  (--param)", color_key="green", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        self._params_frame = tk.Frame(body, bg=C["mantle"])
        self._params_frame.pack(fill="x", padx=12)
        self._param_entries: list[tuple[tk.StringVar, tk.StringVar]] = []
        self._refresh_param_rows([])
        tk.Button(body, text="+ add param", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6, pady=2,
                  activebackground=C["surface1"],
                  command=self._add_param_row).pack(anchor="w", padx=12, pady=(2, 6))

    # ── 6) Advanced ───────────────────────────────────────────
    def _build_advanced_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Advanced", color_key="sky", expanded=False)
        sec.pack(fill="x")
        body = sec.body

        # ─── SQL Filter ───
        tk.Label(body, text="SQL Filter", font=FONTS["body_bold"],
                 bg=C["mantle"], fg=C["sky"]).pack(anchor="w", padx=12, pady=(8, 2))
        sql_row = tk.Frame(body, bg=C["mantle"])
        sql_row.pack(fill="x", padx=12, pady=(2, 4))
        self._sql_btn = tk.Button(
            sql_row, text="SQL filter...", font=FONTS["mono_small"],
            bg=C["surface0"], fg=C["text"], relief="flat", padx=10, pady=4,
            activebackground=C["surface1"],
            command=self._open_sql_selector)
        self._sql_btn.pack(side="left")
        self._sql_count_label = tk.Label(sql_row, text="(all)", font=FONTS["mono_small"],
                                         bg=C["mantle"], fg=C["overlay0"])
        self._sql_count_label.pack(side="left", padx=8)
        self._sql_preview = tk.Text(body, bg=C["crust"], fg=C["subtext"],
                                    font=FONTS["mono_small"], height=3, relief="flat",
                                    bd=4, state="disabled", wrap="none")
        self._sql_preview.pack(fill="x", padx=12, pady=(0, 6))

        ttk.Separator(body, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # ─── Export 옵션 ───
        tk.Label(body, text="Export", font=FONTS["body_bold"],
                 bg=C["mantle"], fg=C["sky"]).pack(anchor="w", padx=12, pady=(4, 2))

        def _w_workers(r):
            tk.Spinbox(r, from_=1, to=16, width=4, textvariable=self._ov_workers,
                       bg=C["surface0"], fg=C["text"], buttonbackground=C["surface1"],
                       relief="flat", font=FONTS["mono_small"],
                       command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "export.workers", _w_workers, "1~16", tooltip=TOOLTIPS.get("export.workers", ""))

        def _w_compression(r):
            ttk.Combobox(r, textvariable=self._ov_compression,
                         values=["gzip", "none"], state="readonly",
                         font=FONTS["mono_small"], width=8).pack(side="left")
        self._ov_row(body, "export.compression", _w_compression, tooltip=TOOLTIPS.get("export.compression", ""))

        ttk.Separator(body, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # ─── Transform 옵션 ───
        tk.Label(body, text="Transform", font=FONTS["body_bold"],
                 bg=C["mantle"], fg=C["sky"]).pack(anchor="w", padx=12, pady=(4, 2))
        self._path_row(body, "transform.sql_dir", self._transform_sql_dir, "Select transform SQL dir")

        def _w_tfm_schema(r):
            tk.Entry(r, textvariable=self._transform_schema,
                     bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                     relief="flat", font=FONTS["mono_small"], width=16).pack(side="left", fill="x", expand=True, ipady=2)
        self._ov_row(body, "transform.schema", _w_tfm_schema, note="@{schema} 접두사용", tooltip=TOOLTIPS.get("transform.schema", ""))

        def _w_on_error(r):
            ttk.Combobox(r, textvariable=self._ov_on_error,
                         values=["stop", "continue"], state="readonly",
                         font=FONTS["mono_small"], width=8).pack(side="left")
        self._ov_row(body, "transform.on_error", _w_on_error, tooltip=TOOLTIPS.get("transform.on_error", ""))

        ttk.Separator(body, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # ─── Report 옵션 ───
        tk.Label(body, text="Report", font=FONTS["body_bold"],
                 bg=C["mantle"], fg=C["sky"]).pack(anchor="w", padx=12, pady=(4, 2))
        self._path_row(body, "report.sql_dir", self._report_sql_dir, "Select report SQL dir")
        self._path_row(body, "report.out_dir", self._report_out_dir, "Select report output dir")

        def _w_excel(r):
            tk.Checkbutton(r, variable=self._ov_excel, text="",
                           bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.excel", _w_excel, tooltip=TOOLTIPS.get("report.excel", ""))

        def _w_csv(r):
            tk.Checkbutton(r, variable=self._ov_csv, text="",
                           bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.csv", _w_csv, tooltip=TOOLTIPS.get("report.csv", ""))

        def _w_max_files(r):
            tk.Spinbox(r, from_=1, to=100, width=4, textvariable=self._ov_max_files,
                       bg=C["surface0"], fg=C["text"], buttonbackground=C["surface1"],
                       relief="flat", font=FONTS["mono_small"],
                       command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.max_files", _w_max_files, tooltip=TOOLTIPS.get("report.max_files", ""))

        def _w_skip_sql(r):
            tk.Checkbutton(r, variable=self._ov_skip_sql, text="",
                           bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.skip_sql", _w_skip_sql, "skip DB -> CSV union only", tooltip=TOOLTIPS.get("report.skip_sql", ""))

        # report.csv_union_dir
        union_row = tk.Frame(body, bg=C["mantle"])
        union_row.pack(fill="x", padx=12, pady=2)
        tk.Label(union_row, text="report.union_dir", font=FONTS["mono_small"],
                 width=18, anchor="w", bg=C["mantle"], fg=C["subtext"]).pack(side="left")
        tk.Entry(union_row, textvariable=self._ov_union_dir,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=FONTS["mono_small"], width=12).pack(side="left", fill="x", expand=True, ipady=2)
        def _browse_union():
            d = filedialog.askdirectory(
                initialdir=self._ov_union_dir.get() or self._work_dir.get(),
                title="CSV union source folder")
            if d:
                try:
                    rel = Path(d).relative_to(Path(self._work_dir.get()))
                    self._ov_union_dir.set(rel.as_posix())
                except ValueError:
                    self._ov_union_dir.set(d)
        tk.Button(union_row, text="...", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=_browse_union).pack(side="left", padx=(2, 0))

    # ── 7) Job / Presets ──────────────────────────────────────
    def _build_job_preset_section(self: "BatchRunnerGUI", parent):
        sec = CollapsibleSection(parent, "Job / Presets", color_key="peach", expanded=False)
        sec.pack(fill="x")
        body = sec.body

        # Row 1: combo + del
        job_row = tk.Frame(body, bg=C["mantle"])
        job_row.pack(fill="x", padx=12, pady=(8, 2))
        self._job_combo = ttk.Combobox(job_row, textvariable=self.job_var,
                                       state="readonly", font=FONTS["mono"], width=18)
        self._job_combo.pack(side="left", fill="x", expand=True)
        self._job_combo.bind("<<ComboboxSelected>>", self._on_job_change)
        tk.Button(job_row, text="dup", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["blue"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._on_job_duplicate).pack(side="left", padx=(4, 0))
        tk.Button(job_row, text="del", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["red"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._on_job_delete).pack(side="left", padx=(4, 0))

        # Row 2: save + save as
        btn_row = tk.Frame(body, bg=C["mantle"])
        btn_row.pack(fill="x", padx=12, pady=(2, 6))
        tk.Button(btn_row, text="save", font=FONTS["mono_small"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=6, pady=2,
                  activebackground=C["teal"],
                  command=self._on_save_yml).pack(side="left", padx=(0, 6))
        tk.Button(btn_row, text="save as", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6, pady=2,
                  activebackground=C["surface1"],
                  command=self._on_save_yml_as).pack(side="left")

    # ── 우측 로그 패널 ───────────────────────────────────────
    def _build_right(self: "BatchRunnerGUI", parent):
        header = tk.Frame(parent, bg=C["mantle"])
        header.pack(fill="x")
        tk.Label(header, text="Run Log", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14, pady=10)
        tk.Checkbutton(header, text="--debug", variable=self._debug_var,
                       bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                       activebackground=C["mantle"], font=FONTS["mono_small"]
                       ).pack(side="left", padx=(0, 6))

        # 로그 필터 버튼
        self._log_filter_btns = {}
        for level in ("ALL", "WARN+", "ERR"):
            btn = tk.Button(header, text=f"[{level}]", font=FONTS["shortcut"],
                            relief="flat", padx=4, pady=0, bd=0,
                            command=lambda lv=level: self._set_log_filter(lv))
            btn.pack(side="left", padx=(0, 2))
            self._log_filter_btns[level] = btn
        self._refresh_log_filter_btns()

        # 타임스탬프 토글 버튼
        self._time_btn = tk.Button(header, text="[Time]", font=FONTS["shortcut"],
                                   relief="flat", padx=4, pady=0, bd=0,
                                   command=self._toggle_show_time)
        self._time_btn.pack(side="left", padx=(4, 0))
        self._refresh_time_btn()

        self._status_label = tk.Label(header, text="● idle", font=FONTS["mono_small"],
                                      bg=C["mantle"], fg=C["overlay0"])
        self._status_label.pack(side="right", padx=10)

        tk.Button(header, text="Clear", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._clear_log).pack(side="right", padx=4)
        tk.Button(header, text="Save Log", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._export_log).pack(side="right", padx=(0, 2))
        tk.Label(header, text="Ctrl+L", font=FONTS["shortcut"],
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="right")

        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        # Progress bar + 경과시간
        prog_frame = tk.Frame(parent, bg=C["mantle"])
        prog_frame.pack(fill="x", padx=8, pady=(4, 0))
        self._progress_bar = ttk.Progressbar(prog_frame, mode="determinate",
                                              maximum=100, value=0,
                                              style="green.Horizontal.TProgressbar")
        self._progress_bar.pack(side="left", fill="x", expand=True, padx=(4, 6))
        self._progress_label = tk.Label(prog_frame, text="", font=FONTS["mono_small"],
                                        bg=C["mantle"], fg=C["overlay0"], width=18, anchor="w")
        self._progress_label.pack(side="left")
        self._elapsed_start = None
        self._elapsed_job_id = None
        self._stage_total = 0

        # CLI Preview
        preview_frame = tk.Frame(parent, bg=C["mantle"])
        preview_frame.pack(fill="x", padx=8, pady=(6, 0))
        tk.Label(preview_frame, text="Command", font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["overlay0"]).pack(anchor="w", padx=4)
        self._cmd_preview = tk.Text(preview_frame, bg=C["crust"], fg=C["green"],
                                    font=FONTS["cmd"], height=3, relief="flat",
                                    bd=4, wrap="word", state="disabled")
        self._cmd_preview.pack(fill="x", padx=4, pady=(2, 6))
        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        # 검색 바 (Ctrl+F — 초기 숨김)
        self._search_frame = tk.Frame(parent, bg=C["mantle"])
        search_inner = tk.Frame(self._search_frame, bg=C["mantle"])
        search_inner.pack(fill="x", padx=4, pady=4)
        tk.Label(search_inner, text="Find:", font=FONTS["small"],
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left", padx=4)
        self._search_entry = tk.Entry(search_inner, textvariable=self._search_var,
                                      bg=C["surface0"], fg=C["text"],
                                      insertbackground=C["text"], relief="flat",
                                      font=FONTS["mono"], width=20)
        self._search_entry.pack(side="left", padx=2, ipady=2)
        self._search_var.trace_add("write", self._on_search_change)
        self._search_entry.bind("<Return>", lambda e: self._search_next())
        self._search_entry.bind("<Shift-Return>", lambda e: self._search_prev())
        self._search_count_label = tk.Label(search_inner, text="", font=FONTS["small"],
                                             bg=C["mantle"], fg=C["overlay0"])
        self._search_count_label.pack(side="left", padx=4)
        tk.Button(search_inner, text="Prev", font=FONTS["shortcut"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  command=self._search_prev).pack(side="left", padx=1)
        tk.Button(search_inner, text="Next", font=FONTS["shortcut"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  command=self._search_next).pack(side="left", padx=1)
        tk.Button(search_inner, text="X", font=FONTS["shortcut"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  command=self._toggle_search).pack(side="left", padx=1)

        self._log = scrolledtext.ScrolledText(
            parent, bg=C["crust"], fg=C["text"],
            font=FONTS["log"], relief="flat", bd=8, wrap="word",
            spacing1=2, spacing3=2
        )
        self._log.pack(fill="both", expand=True, padx=8, pady=8)

        for tag, fg in [("INFO", C["text"]), ("SUCCESS", C["green"]),
                        ("WARN",  C["yellow"]), ("ERROR", C["red"]),
                        ("SYS",   C["blue"]),   ("TIME",  C["overlay0"]),
                        ("DIM",   C["subtext"])]:
            self._log.tag_config(tag, foreground=fg)
        self._log.tag_config("STAGE_HEADER", foreground=C["mauve"],
                             font=(FONT_MONO, 11, "bold"),
                             background=C["surface0"],
                             spacing1=12, spacing3=4)
        self._log.tag_config("STAGE_DONE", foreground=C["teal"],
                             font=(FONT_MONO, 10, "bold"),
                             spacing3=8)
        self._log.tag_config("HIGHLIGHT", background=C["yellow"], foreground=C["crust"])

        # 로그 우클릭 컨텍스트 메뉴
        self._build_log_context_menu()
        self._log.bind("<Button-3>", self._show_log_context_menu)

    # ── 하단 버튼 바 ─────────────────────────────────────────
    def _build_button_bar(self: "BatchRunnerGUI"):
        bar = tk.Frame(self, bg=C["crust"], pady=8)
        bar.pack(fill="x", padx=10, pady=(4, 8))

        def _make_run_cmd(mode):
            def _cmd():
                self.mode_var.set(mode)
                self._on_run()
            return _cmd

        self._dryrun_btn = tk.Button(
            bar, text="Dryrun", font=FONTS["button"],
            bg=C["yellow"], fg=C["crust"], relief="flat", padx=14, pady=6,
            activebackground=C["peach"], activeforeground=C["crust"],
            command=_make_run_cmd("plan")
        )
        self._dryrun_btn.pack(side="left", padx=(0, 4))

        self._run_btn = tk.Button(
            bar, text="▶  Run", font=FONTS["button"],
            bg=C["blue"], fg=C["crust"], relief="flat", padx=14, pady=6,
            activebackground=C["sky"], activeforeground=C["crust"],
            command=_make_run_cmd("run")
        )
        self._run_btn.pack(side="left", padx=(0, 4))

        self._retry_btn = tk.Button(
            bar, text="Retry", font=FONTS["button"],
            bg=C["peach"], fg=C["crust"], relief="flat", padx=14, pady=6,
            activebackground=C["yellow"], activeforeground=C["crust"],
            command=_make_run_cmd("retry")
        )
        self._retry_btn.pack(side="left", padx=(0, 4))

        self._stop_btn = tk.Button(
            bar, text="■  Stop", font=FONTS["button"],
            bg=C["surface0"], fg=C["overlay0"], relief="flat", padx=14, pady=6,
            state="disabled", command=self._on_stop
        )
        self._stop_btn.pack(side="left")

        self._stage_status = tk.Label(bar, text="", font=FONTS["small"],
                                      bg=C["crust"], fg=C["overlay0"])
        self._stage_status.pack(side="left", padx=10)

        # 예약 실행
        sep = tk.Frame(bar, bg=C["overlay0"], width=1)
        sep.pack(side="left", fill="y", padx=8, pady=4)
        tk.Label(bar, text="예약시각", font=FONTS["mono_small"],
                 bg=C["crust"], fg=C["overlay0"]).pack(side="left", padx=(0, 2))
        self._schedule_entry = tk.Entry(
            bar, textvariable=self._schedule_time, width=5,
            font=FONTS["mono_small"], bg=C["surface0"], fg=C["overlay0"],
            insertbackground=C["text"], relief="flat", justify="center")
        self._schedule_entry.pack(side="left", padx=(0, 4), ipady=2)
        self._schedule_entry.bind("<FocusIn>", self._on_schedule_focus_in)
        self._schedule_entry.bind("<FocusOut>", self._on_schedule_focus_out)
        Tooltip(self._schedule_entry, "24시간제 실행 시각 (예: 09:30, 14:00)")
        self._schedule_btn = tk.Button(
            bar, text="Schedule", font=FONTS["mono_small"],
            bg=C["surface0"], fg=C["subtext"], relief="flat", padx=8,
            activebackground=C["surface1"], command=self._on_schedule)
        self._schedule_btn.pack(side="left", padx=(0, 4))
        self._schedule_label = tk.Label(bar, text="", font=FONTS["mono_small"],
                                        bg=C["crust"], fg=C["overlay0"])
        self._schedule_label.pack(side="left")

        # 단축키 힌트
        for hint in [("Ctrl+F5", "Dryrun"), ("F5", "Run"), ("Esc", "Stop"),
                      ("Ctrl+S", "Save"), ("Ctrl+R", "Reload"), ("Ctrl+F", "Search")]:
            tk.Label(bar, text=f"{hint[0]} {hint[1]}", font=FONTS["shortcut"],
                     bg=C["crust"], fg=C["overlay0"]).pack(side="left", padx=6)

        self._clock_label = tk.Label(bar, text="", bg=C["crust"],
                                     fg=C["overlay0"], font=FONTS["mono_small"])
        self._clock_label.pack(side="right", padx=10)
        self._tick_clock()

    def _bind_shortcuts(self: "BatchRunnerGUI"):
        """전역 단축키 바인딩"""
        self.bind_all("<F5>",         lambda e: self._run_btn.invoke() if self._run_btn["state"] != "disabled" else None)
        self.bind_all("<Control-F5>", lambda e: self._dryrun_btn.invoke() if self._dryrun_btn["state"] != "disabled" else None)
        self.bind_all("<Escape>",     lambda e: self._on_stop() if self._stop_btn["state"] != "disabled" else None)
        self.bind_all("<Control-s>",  lambda e: self._on_save_yml())
        self.bind_all("<Control-r>",  lambda e: self._reload_project())
        self.bind_all("<Control-l>",  lambda e: self._export_log())
        self.bind_all("<Control-f>",  lambda e: self._toggle_search())
