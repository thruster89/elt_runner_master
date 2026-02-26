"""
batch_runner_gui.py  ─  Tkinter GUI for ELT Runner v1.91
실행: python batch_runner_gui.py   (batch_runner 프로젝트 루트에서)
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import subprocess
import threading
import sys
import os
import signal
import yaml
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# 색상 팔레트 5종
# ─────────────────────────────────────────────────────────────
THEMES = {
    "Mocha": {           # Dark 1 — Catppuccin Mocha
        "base":    "#1e1e2e", "mantle":  "#181825", "crust":   "#11111b",
        "surface0":"#313244", "surface1":"#45475a", "surface2":"#585b70",
        "overlay0":"#6c7086", "overlay1":"#7f849c",
        "text":    "#cdd6f4", "subtext": "#a6adc8",
        "blue":    "#89b4fa", "green":   "#a6e3a1", "yellow":  "#f9e2af",
        "red":     "#f38ba8", "peach":   "#fab387", "mauve":   "#cba6f7",
        "teal":    "#94e2d5", "sky":     "#89dceb",
    },
    "Nord": {            # Dark 2 — Nord
        "base":    "#2e3440", "mantle":  "#272c36", "crust":   "#1e2430",
        "surface0":"#3b4252", "surface1":"#434c5e", "surface2":"#4c566a",
        "overlay0":"#616e88", "overlay1":"#7b88a1",
        "text":    "#eceff4", "subtext": "#d8dee9",
        "blue":    "#81a1c1", "green":   "#a3be8c", "yellow":  "#ebcb8b",
        "red":     "#bf616a", "peach":   "#d08770", "mauve":   "#b48ead",
        "teal":    "#88c0d0", "sky":     "#8fbcbb",
    },
    "Latte": {           # Light 1 — Catppuccin Latte
        "base":    "#eff1f5", "mantle":  "#e6e9ef", "crust":   "#dce0e8",
        "surface0":"#ccd0da", "surface1":"#bcc0cc", "surface2":"#acb0be",
        "overlay0":"#9ca0b0", "overlay1":"#8c8fa1",
        "text":    "#4c4f69", "subtext": "#6c6f85",
        "blue":    "#1e66f5", "green":   "#40a02b", "yellow":  "#df8e1d",
        "red":     "#d20f39", "peach":   "#fe640b", "mauve":   "#8839ef",
        "teal":    "#179299", "sky":     "#04a5e5",
    },
    "White": {           # Light 2 — Clean White
        "base":    "#ffffff", "mantle":  "#f5f5f5", "crust":   "#ebebeb",
        "surface0":"#e0e0e0", "surface1":"#cccccc", "surface2":"#b8b8b8",
        "overlay0":"#999999", "overlay1":"#808080",
        "text":    "#1a1a1a", "subtext": "#444444",
        "blue":    "#0066cc", "green":   "#2d8a2d", "yellow":  "#b38600",
        "red":     "#cc0000", "peach":   "#e06000", "mauve":   "#7700cc",
        "teal":    "#007a7a", "sky":     "#0099bb",
    },
    "Paper": {           # Light 3 — Warm Paper
        "base":    "#f8f4e8", "mantle":  "#f0ead6", "crust":   "#e8e0c8",
        "surface0":"#ddd5bb", "surface1":"#cec5a8", "surface2":"#b8ad92",
        "overlay0":"#9a9070", "overlay1":"#807558",
        "text":    "#2c2416", "subtext": "#5a4e36",
        "blue":    "#3d6b8e", "green":   "#4a7c3f", "yellow":  "#8a6500",
        "red":     "#a02020", "peach":   "#a04818", "mauve":   "#6b3fa0",
        "teal":    "#2a7a6a", "sky":     "#2068a0",
    },
}

# 현재 테마 (전역 — 위젯 생성 시 참조)
_CURRENT_THEME = "Mocha"
C = dict(THEMES[_CURRENT_THEME])


# ─────────────────────────────────────────────────────────────
# 유틸 ─ 프로젝트에서 동적 데이터 읽기
# ─────────────────────────────────────────────────────────────
def load_jobs(work_dir: Path) -> dict:
    """jobs/ 폴더의 *.yml 파싱 → {filename: parsed_dict}"""
    jobs = {}
    jobs_dir = work_dir / "jobs"
    if jobs_dir.exists():
        for f in sorted(jobs_dir.glob("*.yml")):
            try:
                data = yaml.safe_load(f.read_text(encoding="utf-8"))
                jobs[f.name] = data
            except Exception:
                pass
    return jobs


def load_env_hosts(work_dir: Path, env_path: str = "config/env.yml") -> dict:
    """env.yml 에서 source type → host 목록 반환 {type: [host, ...]}"""
    result = {}
    p = Path(env_path) if Path(env_path).is_absolute() else work_dir / env_path
    if not p.exists():
        return result
    try:
        env = yaml.safe_load(p.read_text(encoding="utf-8"))
        sources = env.get("sources", {})
        for src_type, cfg in sources.items():
            hosts = list((cfg.get("hosts") or {}).keys())
            if hosts:
                result[src_type] = hosts
    except Exception:
        pass
    return result


def scan_sql_params(sql_dir: Path) -> list[str]:
    """
    sql_dir 하위 .sql 파일 전체 스캔,
    :param  {#param}  ${param} 세 가지 패턴으로 파라미터 이름 추출 → 정렬된 리스트 반환.
    싱글쿼트 문자열 리터럴 내부의 :word 는 파라미터로 인식하지 않음.
    sql_dir 의 부모(workdir/sql/)에서 transform/, report/ 도 함께 스캔.
    """
    import re
    PAT_DOLLAR = re.compile(r'\$\{(\w+)\}')
    PAT_HASH   = re.compile(r'\{#(\w+)\}')
    PAT_COLON  = re.compile(r'(?<![:\w]):(\w+)\b')
    # SQL 키워드 + Oracle/일반 날짜 포맷 토큰 제외
    EXCLUDE = {
        "null","true","false","and","or","not","in","is","as","by","on",
        "MI","SS","HH","HH12","HH24","DD","MM","MON","MONTH","YY","YYYY",
        "RR","DY","DAY","WW","IW","Q","J","FF","TZH","TZM","TZR","TZD",
    }

    def _non_literal_chunks(text):
        """싱글쿼트 리터럴 밖 텍스트 청크 yield"""
        i, n, buf_start = 0, len(text), 0
        while i < n:
            if text[i] == "'":
                yield text[buf_start:i]
                i += 1
                while i < n:
                    if text[i] == "'" :
                        if i + 1 < n and text[i+1] == "'" :
                            i += 2; continue
                        else:
                            i += 1; break
                    i += 1
                buf_start = i
            else:
                i += 1
        yield text[buf_start:]

    found: set[str] = set()
    if not sql_dir.exists():
        return []

    # sql_dir 외에 transform/, report/ sql 폴더도 자동 포함
    scan_dirs = [sql_dir]
    sql_root = sql_dir.parent  # 보통 workdir/sql/
    for extra in ("transform", "report"):
        extra_dir = sql_root / extra
        if extra_dir.exists() and extra_dir not in scan_dirs:
            scan_dirs.append(extra_dir)

    for scan_dir in scan_dirs:
        for sql_file in scan_dir.rglob("*.sql"):
            try:
                text = sql_file.read_text(encoding="utf-8", errors="ignore")
                for m in PAT_DOLLAR.finditer(text):
                    if m.group(1) not in EXCLUDE: found.add(m.group(1))
                for m in PAT_HASH.finditer(text):
                    if m.group(1) not in EXCLUDE: found.add(m.group(1))
                for chunk in _non_literal_chunks(text):
                    for m in PAT_COLON.finditer(chunk):
                        if m.group(1) not in EXCLUDE: found.add(m.group(1))
            except Exception:
                pass
    return sorted(found)


def _scan_params_from_files(files: list) -> list[str]:
    """지정 파일 목록만 스캔해서 파라미터 추출 (sql filter 선택 시 사용)"""
    import re
    PAT_DOLLAR = re.compile(r'\$\{(\w+)\}')
    PAT_HASH   = re.compile(r'\{#(\w+)\}')
    PAT_COLON  = re.compile(r'(?<![:\w]):(\w+)\b')
    EXCLUDE = {
        "null","true","false","and","or","not","in","is","as","by","on",
        "MI","SS","HH","HH12","HH24","DD","MM","MON","MONTH","YY","YYYY",
        "RR","DY","DAY","WW","IW","Q","J","FF","TZH","TZM","TZR","TZD",
    }
    def _non_literal_chunks(text):
        i, n, buf_start = 0, len(text), 0
        while i < n:
            if text[i] == "'":
                yield text[buf_start:i]
                i += 1
                while i < n:
                    if text[i] == "'":
                        if i + 1 < n and text[i+1] == "'": i += 2; continue
                        else: i += 1; break
                    i += 1
                buf_start = i
            else:
                i += 1
        yield text[buf_start:]

    found: set[str] = set()
    for sql_file in files:
        try:
            text = Path(sql_file).read_text(encoding="utf-8", errors="ignore")
            for m in PAT_DOLLAR.finditer(text):
                if m.group(1) not in EXCLUDE: found.add(m.group(1))
            for m in PAT_HASH.finditer(text):
                if m.group(1) not in EXCLUDE: found.add(m.group(1))
            for chunk in _non_literal_chunks(text):
                for m in PAT_COLON.finditer(chunk):
                    if m.group(1) not in EXCLUDE: found.add(m.group(1))
        except Exception:
            pass
    return sorted(found)


def collect_sql_tree(sql_dir: Path) -> dict:
    """
    sql_dir 하위 폴더/파일 트리 반환
    {
      "export": {
          "__files__": ["01_contract.sql", "02_payment.sql"],
          "A": {"__files__": ["a1.sql", "a2.sql"]},
          "B": {"__files__": ["rate.sql"]},
      },
      ...
    }
    """
    def _walk(path: Path) -> dict:
        node = {"__files__": []}
        for item in sorted(path.iterdir()):
            if item.is_file() and item.suffix.lower() == ".sql":
                node["__files__"].append(item.name)
            elif item.is_dir():
                node[item.name] = _walk(item)
        return node

    if not sql_dir.exists():
        return {}
    tree = {}
    for item in sorted(sql_dir.iterdir()):
        if item.is_dir():
            tree[item.name] = _walk(item)
        elif item.is_file() and item.suffix.lower() == ".sql":
            tree.setdefault("__root__", {"__files__": []})["__files__"].append(item.name)
    return tree


# ─────────────────────────────────────────────────────────────
# SQL 파일 선택 다이얼로그
# ─────────────────────────────────────────────────────────────
class SqlSelectorDialog(tk.Toplevel):
    """SQL 폴더 트리 + 파일 체크박스 선택 다이얼로그"""

    def __init__(self, parent, sql_dir: Path, pre_selected: set = None):
        super().__init__(parent)
        self.title("SQL 파일 선택")
        self.configure(bg=C["base"])
        self.resizable(True, True)
        self.geometry("500x560")
        self.transient(parent)
        self.grab_set()

        self.sql_dir = sql_dir
        self.selected: set[str] = set(pre_selected or [])  # relative paths from sql_dir
        self._check_vars: dict[str, tk.BooleanVar] = {}

        self._build()
        self._center(parent)

    def _center(self, parent):
        self.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{px + pw//2 - w//2}+{py + ph//2 - h//2}")

    def _build(self):
        # 헤더
        hdr = tk.Frame(self, bg=C["mantle"], pady=8)
        hdr.pack(fill="x")
        tk.Label(hdr, text="📂  SQL File Select", font=("Consolas", 11, "bold"),
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14)
        tk.Label(hdr, text=str(self.sql_dir), font=("Consolas", 8),
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=6)

        # 전체선택 / 전체해제
        ctrl = tk.Frame(self, bg=C["base"], pady=4)
        ctrl.pack(fill="x", padx=10)
        tk.Button(ctrl, text="Select All", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._select_all).pack(side="left", padx=(0, 4))
        tk.Button(ctrl, text="Deselect All", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._deselect_all).pack(side="left")

        # 스크롤 영역
        outer = tk.Frame(self, bg=C["base"])
        outer.pack(fill="both", expand=True, padx=10, pady=6)

        canvas = tk.Canvas(outer, bg=C["crust"], highlightthickness=0)
        vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        self._scroll_frame = tk.Frame(canvas, bg=C["crust"])
        canvas_win = canvas.create_window((0, 0), window=self._scroll_frame, anchor="nw")

        def _on_frame_resize(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfig(canvas_win, width=canvas.winfo_width())
        self._scroll_frame.bind("<Configure>", _on_frame_resize)
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(canvas_win, width=e.width))
        
        def _scroll_if_canvas(e):
            w = e.widget
            # 위젯 자신 또는 부모 체인에 Combobox/Spinbox/Entry가 있으면 무시
            def _has_interactive_ancestor(widget):
                try:
                    while widget:
                        if isinstance(widget, (ttk.Combobox, tk.Spinbox, ttk.Spinbox, tk.Entry, tk.Listbox)):
                            return True
                        widget = widget.master
                except Exception:
                    pass
                return False
            if not _has_interactive_ancestor(w):
                canvas.yview_scroll(-1*(e.delta//120), "units")
        canvas.bind_all("<MouseWheel>", _scroll_if_canvas)

        # 트리 렌더링
        tree = collect_sql_tree(self.sql_dir)
        self._render_tree(self._scroll_frame, tree, prefix="", indent=0)

        # 하단 버튼
        btn_bar = tk.Frame(self, bg=C["mantle"], pady=8)
        btn_bar.pack(fill="x")
        self._count_label = tk.Label(btn_bar, text="", font=("Consolas", 9),
                                     bg=C["mantle"], fg=C["subtext"])
        self._count_label.pack(side="left", padx=14)
        self._update_count()

        tk.Button(btn_bar, text="Cancel", font=("Consolas", 10),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=14, pady=4,
                  activebackground=C["surface1"],
                  command=self.destroy).pack(side="right", padx=8)
        tk.Button(btn_bar, text="✔  OK", font=("Consolas", 10, "bold"),
                  bg=C["green"], fg=C["crust"], relief="flat", padx=14, pady=4,
                  activebackground=C["teal"],
                  command=self._confirm).pack(side="right", padx=(0, 4))

    def _render_tree(self, parent_frame, node: dict, prefix: str, indent: int):
        pad = indent * 20

        # 파일 먼저
        for fname in node.get("__files__", []):
            rel = (prefix + "/" + fname).lstrip("/")
            var = tk.BooleanVar(value=(rel in self.selected))
            self._check_vars[rel] = var
            var.trace_add("write", lambda *_, r=rel: self._on_check(r))

            row = tk.Frame(parent_frame, bg=C["crust"])
            row.pack(fill="x")
            tk.Label(row, width=pad // 8 or 1, bg=C["crust"]).pack(side="left")
            cb = tk.Checkbutton(
                row, text=f"  {fname}", variable=var,
                bg=C["crust"], fg=C["text"], selectcolor=C["surface0"],
                activebackground=C["crust"], activeforeground=C["text"],
                font=("Consolas", 9), anchor="w"
            )
            cb.pack(fill="x", side="left", padx=(pad, 0))

        # 하위 폴더
        for key, sub in node.items():
            if key == "__files__" or key == "__root__":
                continue
            folder_prefix = (prefix + "/" + key).lstrip("/")

            folder_row = tk.Frame(parent_frame, bg=C["crust"], pady=2)
            folder_row.pack(fill="x")
            tk.Label(folder_row, width=pad // 8 or 1, bg=C["crust"]).pack(side="left")

            # 폴더 토글 버튼
            toggle_var = tk.BooleanVar(value=True)
            child_frame = tk.Frame(parent_frame, bg=C["crust"])
            child_frame.pack(fill="x")

            def make_toggle(cf, tv, btn_ref):
                def toggle():
                    if tv.get():
                        cf.pack_forget()
                        btn_ref.config(text=f"  ▶  {key}")
                    else:
                        cf.pack(fill="x")
                        btn_ref.config(text=f"  ▼  {key}")
                    tv.set(not tv.get())
                return toggle

            folder_btn = tk.Button(
                folder_row,
                text=f"  ▼  {key}",
                font=("Consolas", 9, "bold"),
                bg=C["crust"], fg=C["blue"], relief="flat",
                anchor="w", padx=pad
            )
            folder_btn.config(command=make_toggle(child_frame, toggle_var, folder_btn))
            folder_btn.pack(fill="x", side="left", expand=True)

            # 폴더 전체 선택 버튼
            tk.Button(
                folder_row, text="All", font=("Consolas", 8),
                bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6,
                activebackground=C["surface1"],
                command=lambda fp=folder_prefix, nd=sub: self._select_folder(fp, nd, True)
            ).pack(side="right", padx=2)
            tk.Button(
                folder_row, text="None", font=("Consolas", 8),
                bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6,
                activebackground=C["surface1"],
                command=lambda fp=folder_prefix, nd=sub: self._select_folder(fp, nd, False)
            ).pack(side="right", padx=2)

            self._render_tree(child_frame, sub, prefix=folder_prefix, indent=indent + 1)

    def _on_check(self, rel: str):
        var = self._check_vars.get(rel)
        if var:
            if var.get():
                self.selected.add(rel)
            else:
                self.selected.discard(rel)
        self._update_count()

    def _update_count(self):
        count = sum(1 for v in self._check_vars.values() if v.get())
        total = len(self._check_vars)
        self._count_label.config(text=f"{count} / {total} selected")

    def _select_all(self):
        for v in self._check_vars.values():
            v.set(True)
        self.selected = set(self._check_vars.keys())
        self._update_count()

    def _deselect_all(self):
        for v in self._check_vars.values():
            v.set(False)
        self.selected = set()
        self._update_count()

    def _select_folder(self, folder_prefix: str, node: dict, value: bool):
        def _recurse(nd, pfx):
            for fname in nd.get("__files__", []):
                rel = (pfx + "/" + fname).lstrip("/")
                if rel in self._check_vars:
                    self._check_vars[rel].set(value)
            for key, sub in nd.items():
                if key == "__files__":
                    continue
                _recurse(sub, (pfx + "/" + key).lstrip("/"))
        _recurse(node, folder_prefix)
        self._update_count()

    def _confirm(self):
        self.selected = {rel for rel, v in self._check_vars.items() if v.get()}
        self.destroy()


# ─────────────────────────────────────────────────────────────
# 메인 GUI
# ─────────────────────────────────────────────────────────────
class BatchRunnerGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ELT Runner  v1.91")
        self.geometry("1200x740")
        self.minsize(900, 560)
        self.configure(bg=C["base"])

        self._process: subprocess.Popen | None = None
        self._work_dir = tk.StringVar(value=str(Path(".").resolve()))
        self._selected_sqls: set[str] = set()  # relative paths (from sql_dir in job)

        self._jobs: dict = {}          # {filename: parsed yml}
        self._env_hosts: dict = {}     # {type: [hosts]}
        self._stage_vars: dict[str, tk.BooleanVar] = {}  # stage 선택 vars
        self._pipeline_widgets: list = []  # pipeline 버튼 참조
        self._presets: dict = {}   # {name: snapshot}
        self._theme_var = tk.StringVar(value="Mocha")
        self._preset_var = tk.StringVar()
        # Job Override 위젯 변수들
        self._ov_overwrite    = tk.BooleanVar(value=False)
        self._ov_workers      = tk.IntVar(value=1)
        self._ov_compression  = tk.StringVar(value="gzip")
        self._ov_out_dir      = tk.StringVar(value="")
        self._ov_db_path      = tk.StringVar(value="")
        self._ov_schema       = tk.StringVar(value="")
        self._ov_on_error     = tk.StringVar(value="stop")
        self._ov_excel        = tk.BooleanVar(value=True)
        self._ov_csv          = tk.BooleanVar(value=True)
        self._ov_max_files    = tk.IntVar(value=10)
        self._ov_skip_sql     = tk.BooleanVar(value=False)
        self._ov_union_dir    = tk.StringVar(value="")
        self._ov_sql_dir      = tk.StringVar(value="")

        self._build_style()
        self._build_ui()
        self._reload_project()
        self._load_presets()
        self._bind_shortcuts()

    def _bind_shortcuts(self):
        """전역 단축키 바인딩"""
        self.bind_all("<F5>",         lambda e: self._on_run()  if self._run_btn["state"] != "disabled" else None)
        self.bind_all("<Escape>",     lambda e: self._on_stop() if self._stop_btn["state"] != "disabled" else None)
        self.bind_all("<Control-s>",  lambda e: self._on_preset_save())
        self.bind_all("<Control-r>",  lambda e: self._reload_project())
        self.bind_all("<Control-l>",  lambda e: self._export_log())

    def _export_log(self):
        """로그 내용을 .txt 파일로 저장"""
        from tkinter import filedialog
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = self.job_var.get().replace(".yml","") or "elt_runner"
        init_file = f"{fname}_log_{ts}.txt"
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=init_file,
            title="Save Log"
        )
        if not path:
            return
        content = self._log.get("1.0", "end")
        Path(path).write_text(content, encoding="utf-8")
        self._log_sys(f"Log saved: {path}")

    def _apply_theme(self):
        """테마 전환: C 딕셔너리 업데이트 후 앱 전체 재빌드"""
        global C
        theme_name = self._theme_var.get()
        C.update(THEMES.get(theme_name, THEMES["Mocha"]))
        # 현재 상태 스냅샷
        snap = self._snapshot()
        wd = self._work_dir.get()
        # 모든 자식 위젯 제거
        for w in self.winfo_children():
            w.destroy()
        # ttk 스타일 재적용
        self._build_style()
        # UI 재빌드
        self._build_ui()
        # 상태 복원
        self._work_dir.set(wd)
        self._reload_project()
        self._load_presets()
        # 테마 드롭다운을 새 theme_var에 연결
        self._theme_var.set(theme_name)
        self._restore_snapshot(snap)

    def _build_style(self):
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
        self.option_add("*TCombobox*Listbox.font", "Consolas 9")
        style.configure("TSeparator", background=C["surface0"])
        style.configure("TScrollbar",
                        background=C["surface0"],
                        troughcolor=C["crust"],
                        arrowcolor=C["text"])
        # 선택된 텍스트 색상 명시 (Mocha/Nord에서 흰 bg + 흰 fg 방지)
        style.map("TCombobox",
                  fieldbackground=[("readonly", C["surface0"]),
                                   ("disabled", C["mantle"])],
                  foreground=[("readonly", C["text"]),
                               ("disabled", C["overlay0"])],
                  selectbackground=[("readonly", C["surface0"])],
                  selectforeground=[("readonly", C["text"])])

    # ── UI 조립 ──────────────────────────────────────────────
    def _build_ui(self):
        # 상단 타이틀 바
        self._build_title_bar()

        # 메인 영역 (좌 + 우)
        main = tk.Frame(self, bg=C["base"])
        main.pack(fill="both", expand=True, padx=10, pady=(4, 0))

        left = tk.Frame(main, bg=C["mantle"], width=380)
        left.pack(side="left", fill="y", padx=(0, 8))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg=C["mantle"])
        right.pack(side="left", fill="both", expand=True)
        self._build_right(right)

        # 하단 버튼 바
        self._build_button_bar()

    def _build_title_bar(self):
        self._title_bar = tk.Frame(self, bg=C["crust"], pady=7)
        self._title_bar.pack(fill="x")
        bar = self._title_bar

        # Work Dir
        tk.Label(bar, text="Work Dir:", font=("Consolas", 10),
                 bg=C["crust"], fg=C["subtext"]).pack(side="left", padx=(14, 4))
        self._wd_entry = tk.Entry(bar, textvariable=self._work_dir,
                            bg=C["surface0"], fg=C["text"],
                            insertbackground=C["text"], relief="flat",
                            font=("Consolas", 10), width=38)
        self._wd_entry.pack(side="left")
        self._wd_btn = tk.Button(bar, text="…", font=("Consolas", 10),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._browse_workdir)
        self._wd_btn.pack(side="left", padx=2)
        self._reload_btn = tk.Button(bar, text="↺ Reload", font=("Consolas", 10),
                  bg=C["blue"], fg=C["crust"], relief="flat", padx=8,
                  activebackground=C["sky"],
                  command=self._reload_project)
        self._reload_btn.pack(side="left", padx=6)

        # 테마 선택 (우측)
        tk.Label(bar, text="Theme:", font=("Consolas", 9),
                 bg=C["crust"], fg=C["subtext"]).pack(side="right", padx=(0, 4))
        self._theme_combo = ttk.Combobox(bar, textvariable=self._theme_var,
                                         values=list(THEMES.keys()),
                                         state="readonly", font=("Consolas", 9), width=8)
        self._theme_combo.pack(side="right", padx=(0, 10))
        self._theme_combo.bind("<<ComboboxSelected>>", lambda _: self._apply_theme())

    # ── 좌측 옵션 패널 ───────────────────────────────────────
    def _build_left(self, parent):
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
        def _scroll_if_canvas(e):
            w = e.widget
            def _has_interactive_ancestor(widget):
                try:
                    while widget:
                        if isinstance(widget, (ttk.Combobox, tk.Spinbox, ttk.Spinbox, tk.Entry, tk.Listbox)):
                            return True
                        widget = widget.master
                except Exception:
                    pass
                return False
            if not _has_interactive_ancestor(w):
                canvas.yview_scroll(-1*(e.delta//120), "units")
        canvas.bind_all("<MouseWheel>", _scroll_if_canvas)

        self._left_inner = inner
        self._build_option_sections(inner)

    def _build_option_sections(self, parent):
        def sec(text):
            f = tk.Frame(parent, bg=C["mantle"])
            f.pack(fill="x", padx=12, pady=(10, 2))
            tk.Label(f, text=text, font=("맑은 고딕", 10, "bold"),
                     bg=C["mantle"], fg=C["blue"]).pack(side="left")
            return f

        def sep():
            ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8, pady=3)

        def entry_row(parent_frame, label, var, **kw):
            row = tk.Frame(parent_frame, bg=C["mantle"])
            row.pack(fill="x", padx=12, pady=2)
            tk.Label(row, text=label, font=("Consolas", 9),
                     bg=C["mantle"], fg=C["subtext"], width=12, anchor="w").pack(side="left")
            e = tk.Entry(row, textvariable=var, bg=C["surface0"], fg=C["text"],
                         insertbackground=C["text"], relief="flat",
                         font=("Consolas", 9), **kw)
            e.pack(side="left", fill="x", expand=True)
            return e

        tk.Label(parent, text="Options", font=("Consolas", 13, "bold"),
                 bg=C["mantle"], fg=C["text"]).pack(pady=(14, 4), padx=12, anchor="w")
        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        # ── Job / Run Mode / Stages 3-zone 레이아웃 ──
        # 바깥 grid: col0=Job+Stages, col1=RunMode
        jm_outer = tk.Frame(parent, bg=C["mantle"])
        jm_outer.pack(fill="x", padx=12, pady=(8, 0))
        jm_outer.columnconfigure(0, weight=1)
        jm_outer.columnconfigure(1, weight=0)

        # ── 헤더 행 (row=0): Job 레이블 | Run Mode 레이블 ──
        tk.Label(jm_outer, text="Job  (--job)", font=("Consolas", 9, "bold"),
                 bg=C["mantle"], fg=C["blue"]).grid(
                     row=0, column=0, sticky="w", pady=(0, 2))
        tk.Label(jm_outer, text="Run Mode", font=("Consolas", 9, "bold"),
                 bg=C["mantle"], fg=C["blue"]).grid(
                     row=0, column=1, sticky="nw", padx=(16, 0), pady=(0, 2))

        # ── row=1: Job 콤보 | Plan 라디오 ──
        self.job_var = tk.StringVar()
        self._job_combo = ttk.Combobox(jm_outer, textvariable=self.job_var,
                                       state="readonly", font=("Consolas", 10), width=18)
        self._job_combo.grid(row=1, column=0, sticky="ew")
        self._job_combo.bind("<<ComboboxSelected>>", self._on_job_change)

        self.mode_var = tk.StringVar(value="run")
        for r_idx, (m, label, color) in enumerate([
                ("plan",  "Plan (Dryrun)", C["yellow"]),
                ("run",   "Run",           C["green"]),
                ("retry", "Retry",         C["peach"])]):
            tk.Radiobutton(jm_outer, text=label, variable=self.mode_var, value=m,
                           bg=C["mantle"], fg=color, selectcolor=C["surface0"],
                           activebackground=C["mantle"], activeforeground=color,
                           font=("Consolas", 9)).grid(
                               row=1 + r_idx, column=1, sticky="w", padx=(16, 0))

        # ── row=2~3: Stages 2×2 그리드 (col=0, Job 콤보 아래) ──
        self._pipeline_frame = tk.Frame(jm_outer, bg=C["mantle"])
        self._pipeline_frame.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self._pipeline_widgets = []

        sep()

        # ── Env 파일 ──
        sec("Env file  (--env)")
        self._env_path_var = tk.StringVar(value="config/env.yml")
        entry_row(parent, "env yml:", self._env_path_var)

        sep()

        # ── Source host ──
        sec("Source Host  (--set source.host=)")
        host_outer = tk.Frame(parent, bg=C["mantle"])
        host_outer.pack(fill="x", padx=12, pady=(2, 4))
        host_list_frame = tk.Frame(host_outer, bg=C["surface0"])
        host_list_frame.pack(side="left", fill="x", expand=True)
        self._host_listbox = tk.Listbox(
            host_list_frame, font=("Consolas", 9), height=4,
            bg=C["surface0"], fg=C["text"],
            selectbackground=C["blue"], selectforeground=C["crust"],
            relief="flat", bd=0, activestyle="none",
            exportselection=False  # ← 다른 위젯 선택해도 해제 안됨 (핵심!)
        )
        host_scroll = ttk.Scrollbar(host_list_frame, orient="vertical",
                                    command=self._host_listbox.yview)
        self._host_listbox.config(yscrollcommand=host_scroll.set)
        self._host_listbox.pack(side="left", fill="both", expand=True)
        host_scroll.pack(side="right", fill="y")
        self._host_listbox.bind("<<ListboxSelect>>", lambda _: self._refresh_preview())
        tk.Label(host_outer, text=" → --set source.host", font=("Consolas", 7),
                 bg=C["mantle"], fg=C["overlay0"], justify="left").pack(side="left", padx=4)

        sep()

        # ── Params ──
        sec("Params  (--param)")
        self._params_frame = tk.Frame(parent, bg=C["mantle"])
        self._params_frame.pack(fill="x", padx=12)
        self._param_entries: list[tuple[tk.StringVar, tk.StringVar]] = []
        self._refresh_param_rows([])
        tk.Button(parent, text="+ add param", font=("Consolas", 8),
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6, pady=2,
                  activebackground=C["surface1"],
                  command=self._add_param_row).pack(anchor="w", padx=12, pady=(2, 4))

        sep()

        # ── SQL 선택 ──
        sec("SQL 파일 선택 (--sql-filter)")
        sql_row = tk.Frame(parent, bg=C["mantle"])
        sql_row.pack(fill="x", padx=12, pady=(2, 4))
        self._sql_btn = tk.Button(
            sql_row, text="📂  SQL filter...", font=("Consolas", 9),
            bg=C["surface0"], fg=C["text"], relief="flat", padx=10, pady=4,
            activebackground=C["surface1"],
            command=self._open_sql_selector
        )
        self._sql_btn.pack(side="left")
        self._sql_count_label = tk.Label(sql_row, text="(all)", font=("Consolas", 8),
                                         bg=C["mantle"], fg=C["overlay0"])
        self._sql_count_label.pack(side="left", padx=8)

        # 선택된 SQL 목록 미니 프리뷰
        self._sql_preview = tk.Text(parent, bg=C["crust"], fg=C["subtext"],
                                    font=("Consolas", 8), height=4, relief="flat",
                                    bd=4, state="disabled", wrap="none")
        self._sql_preview.pack(fill="x", padx=12, pady=(0, 4))

        sep()

        # ── Job Override ──
        sec("Job Override  (--set)")

        def ov_row(label, widget_fn, note=""):
            r = tk.Frame(parent, bg=C["mantle"])
            r.pack(fill="x", padx=12, pady=2)
            tk.Label(r, text=label, font=("Consolas", 8), width=18, anchor="w",
                     bg=C["mantle"], fg=C["subtext"]).pack(side="left")
            widget_fn(r)
            if note:
                tk.Label(r, text=note, font=("Consolas", 7),
                         bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

        # export.overwrite
        def _w_overwrite(r):
            tk.Checkbutton(r, variable=self._ov_overwrite, text="",
                           bg=C["mantle"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        ov_row("export.overwrite", _w_overwrite)

        # export.parallel_workers
        def _w_workers(r):
            tk.Spinbox(r, from_=1, to=16, width=4, textvariable=self._ov_workers,
                       bg=C["surface0"], fg=C["text"], buttonbackground=C["surface1"],
                       relief="flat", font=("Consolas", 9),
                       command=self._refresh_preview).pack(side="left")
        ov_row("export.workers", _w_workers, "1~16")

        # export.compression
        def _w_compression(r):
            ttk.Combobox(r, textvariable=self._ov_compression,
                         values=["gzip", "none"], state="readonly",
                         font=("Consolas", 9), width=8).pack(side="left")
        ov_row("export.compression", _w_compression)

        # export.out_dir
        def _w_out_dir(r):
            tk.Entry(r, textvariable=self._ov_out_dir,
                     bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                     relief="flat", font=("Consolas", 8), width=12).pack(side="left", fill="x", expand=True)
            def browse_out_dir():
                d = filedialog.askdirectory(
                    initialdir=self._ov_out_dir.get() or self._work_dir.get(),
                    title="Select output folder"
                )
                if d:
                    # workdir 기준 상대경로로 변환
                    try:
                        rel = Path(d).relative_to(Path(self._work_dir.get()))
                        self._ov_out_dir.set(rel.as_posix())
                    except ValueError:
                        self._ov_out_dir.set(d)
            tk.Button(r, text="📂", font=("Consolas", 9),
                      bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                      activebackground=C["surface1"],
                      command=browse_out_dir).pack(side="left", padx=(2, 0))
        ov_row("export.out_dir", _w_out_dir, "empty = yml default")

        # export.sql_dir
        def _w_sql_dir(r):
            tk.Entry(r, textvariable=self._ov_sql_dir,
                     bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                     relief="flat", font=("Consolas", 8), width=12).pack(side="left", fill="x", expand=True)
            self._ov_sql_dir.trace_add("write", lambda *_: self.after(300, self._scan_and_suggest_params))

            def browse_sql_dir():
                wd = self._work_dir.get()
                init = self._ov_sql_dir.get() or str(Path(wd) / "sql" / "export")
                d = filedialog.askdirectory(initialdir=init, title="Select SQL dir")
                if d:
                    try:
                        rel = Path(d).relative_to(Path(wd))
                        self._ov_sql_dir.set(rel.as_posix())
                    except ValueError:
                        self._ov_sql_dir.set(d)
                    self._scan_and_suggest_params()
            tk.Button(r, text="📂", font=("Consolas", 9),
                      bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                      activebackground=C["surface1"],
                      command=browse_sql_dir).pack(side="left", padx=(2, 0))
        ov_row("export.sql_dir", _w_sql_dir, "empty = yml default")

        # target: db_path / schema (동적으로 show/hide)
        self._ov_db_path_row = tk.Frame(parent, bg=C["mantle"])
        self._ov_db_path_row.pack(fill="x", padx=12, pady=2)
        tk.Label(self._ov_db_path_row, text="target.db_path", font=("Consolas", 8),
                 width=18, anchor="w", bg=C["mantle"], fg=C["subtext"]).pack(side="left")
        tk.Entry(self._ov_db_path_row, textvariable=self._ov_db_path,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=("Consolas", 8), width=16).pack(side="left", fill="x", expand=True)
        tk.Label(self._ov_db_path_row, text="empty = yml default", font=("Consolas", 7),
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

        self._ov_schema_row = tk.Frame(parent, bg=C["mantle"])
        self._ov_schema_row.pack(fill="x", padx=12, pady=2)
        tk.Label(self._ov_schema_row, text="target.schema", font=("Consolas", 8),
                 width=18, anchor="w", bg=C["mantle"], fg=C["subtext"]).pack(side="left")
        tk.Entry(self._ov_schema_row, textvariable=self._ov_schema,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=("Consolas", 8), width=16).pack(side="left", fill="x", expand=True)
        tk.Label(self._ov_schema_row, text="Oracle only", font=("Consolas", 7),
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

        # transform.on_error
        def _w_on_error(r):
            ttk.Combobox(r, textvariable=self._ov_on_error,
                         values=["stop", "continue"], state="readonly",
                         font=("Consolas", 9), width=8).pack(side="left")
        ov_row("transform.on_error", _w_on_error)

        # report.excel.enabled / report.export_csv.enabled
        def _w_excel(r):
            tk.Checkbutton(r, variable=self._ov_excel, text="",
                           bg=C["mantle"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        ov_row("report.excel", _w_excel)

        def _w_csv(r):
            tk.Checkbutton(r, variable=self._ov_csv, text="",
                           bg=C["mantle"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        ov_row("report.csv", _w_csv)

        # report.excel.max_files
        def _w_max_files(r):
            tk.Spinbox(r, from_=1, to=100, width=4, textvariable=self._ov_max_files,
                       bg=C["surface0"], fg=C["text"], buttonbackground=C["surface1"],
                       relief="flat", font=("Consolas", 9),
                       command=self._refresh_preview).pack(side="left")
        ov_row("report.max_files", _w_max_files)

        # report.skip_sql
        def _w_skip_sql(r):
            tk.Checkbutton(r, variable=self._ov_skip_sql, text="",
                           bg=C["mantle"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        ov_row("report.skip_sql", _w_skip_sql, "skip DB → CSV union only")

        # report.csv_union_dir
        self._ov_union_dir_row = tk.Frame(parent, bg=C["mantle"])
        self._ov_union_dir_row.pack(fill="x", padx=12, pady=2)
        tk.Label(self._ov_union_dir_row, text="report.union_dir", font=("Consolas", 8),
                 width=18, anchor="w", bg=C["mantle"], fg=C["subtext"]).pack(side="left")
        tk.Entry(self._ov_union_dir_row, textvariable=self._ov_union_dir,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=("Consolas", 8), width=12).pack(side="left", fill="x", expand=True)
        def browse_union_dir():
            d = filedialog.askdirectory(
                initialdir=self._ov_union_dir.get() or self._work_dir.get(),
                title="CSV union source folder"
            )
            if d:
                try:
                    rel = Path(d).relative_to(Path(self._work_dir.get()))
                    self._ov_union_dir.set(rel.as_posix())
                except ValueError:
                    self._ov_union_dir.set(d)
        tk.Button(self._ov_union_dir_row, text="📂", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=browse_union_dir).pack(side="left", padx=(2, 0))
        tk.Label(self._ov_union_dir_row, text="empty = yml default", font=("Consolas", 7),
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

        # override 변경 감지
        for ov_var in (self._ov_compression, self._ov_on_error,
                       self._ov_out_dir, self._ov_db_path, self._ov_schema,
                       self._ov_union_dir, self._ov_sql_dir):
            ov_var.trace_add("write", lambda *_: self._refresh_preview())

        tk.Button(parent, text="💾  save as yml", font=("Consolas", 8),
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6, pady=2,
                  activebackground=C["surface1"],
                  command=self._on_save_yml).pack(anchor="w", padx=12, pady=(4, 2))

        sep()

        # ── Presets ──
        sec("Presets")
        preset_row = tk.Frame(parent, bg=C["mantle"])
        preset_row.pack(fill="x", padx=12, pady=(2, 2))
        self._preset_combo = ttk.Combobox(preset_row, textvariable=self._preset_var,
                                          state="readonly", font=("Consolas", 9), width=16)
        self._preset_combo.pack(side="left", fill="x", expand=True)
        self._preset_combo.bind("<<ComboboxSelected>>", self._on_preset_load)
        tk.Button(preset_row, text="load", font=("Consolas", 8),
                  bg=C["blue"], fg=C["crust"], relief="flat", padx=6,
                  activebackground=C["sky"],
                  command=self._on_preset_load).pack(side="left", padx=(4, 2))
        tk.Button(preset_row, text="🗑", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["red"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._on_preset_delete).pack(side="left")
        tk.Button(parent, text="+ save as preset", font=("Consolas", 8),
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6, pady=2,
                  activebackground=C["surface1"],
                  command=self._on_preset_save).pack(anchor="w", padx=12, pady=(2, 4))

        sep()

        # ── 기타 플래그 ──
        sec("Options")
        self._debug_var = tk.BooleanVar(value=False)
        tk.Checkbutton(parent, text="--debug  (verbose)", variable=self._debug_var,
                       bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                       activebackground=C["mantle"], font=("Consolas", 9)
                       ).pack(anchor="w", padx=12)

        sep()

        # 변경 감지
        for var in (self.job_var, self.mode_var, self._env_path_var, self._debug_var):
            var.trace_add("write", lambda *_: self._refresh_preview())
        # host는 Listbox 사용 (콤보박스 포커스 충돌 버그 근본 해결)

    # ── 우측 로그 패널 ───────────────────────────────────────
    def _build_right(self, parent):
        header = tk.Frame(parent, bg=C["mantle"])
        header.pack(fill="x")
        tk.Label(header, text="Run Log", font=("Consolas", 12, "bold"),
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14, pady=10)

        self._status_label = tk.Label(header, text="● idle", font=("Consolas", 9),
                                      bg=C["mantle"], fg=C["overlay0"])
        self._status_label.pack(side="right", padx=10)

        tk.Button(header, text="Clear", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._clear_log).pack(side="right", padx=4)
        tk.Button(header, text="💾 Log", font=("Consolas", 9),
                  bg=C["surface0"], fg=C["subtext"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._export_log).pack(side="right", padx=(0, 2))
        tk.Label(header, text="Ctrl+L", font=("Consolas", 7),
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="right")

        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        # Progress bar + 경과시간
        prog_frame = tk.Frame(parent, bg=C["mantle"])
        prog_frame.pack(fill="x", padx=8, pady=(4, 0))
        self._progress_bar = ttk.Progressbar(prog_frame, mode="determinate",
                                              maximum=100, value=0)
        self._progress_bar.pack(side="left", fill="x", expand=True, padx=(4, 6))
        self._progress_label = tk.Label(prog_frame, text="", font=("Consolas", 9),
                                        bg=C["mantle"], fg=C["overlay0"], width=18, anchor="w")
        self._progress_label.pack(side="left")
        self._elapsed_start = None
        self._elapsed_job_id = None
        self._stage_total = 0

        # CLI Preview (우측 상단 고정)
        preview_frame = tk.Frame(parent, bg=C["mantle"])
        preview_frame.pack(fill="x", padx=8, pady=(6, 0))
        tk.Label(preview_frame, text="Command", font=("Consolas", 8),
                 bg=C["mantle"], fg=C["overlay0"]).pack(anchor="w", padx=4)
        self._cmd_preview = tk.Text(preview_frame, bg=C["crust"], fg=C["green"],
                                    font=("Consolas", 8), height=3, relief="flat",
                                    bd=4, wrap="word", state="disabled")
        self._cmd_preview.pack(fill="x", padx=4, pady=(2, 6))
        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=8)

        self._log = scrolledtext.ScrolledText(
            parent, bg=C["crust"], fg=C["text"],
            font=("Consolas", 11), relief="flat", bd=8, wrap="word"
        )
        self._log.pack(fill="both", expand=True, padx=8, pady=8)

        for tag, fg in [("INFO", C["text"]), ("SUCCESS", C["green"]),
                        ("WARN",  C["yellow"]), ("ERROR", C["red"]),
                        ("SYS",   C["blue"]),   ("TIME",  C["overlay0"]),
                        ("DIM",   C["subtext"])]:
            self._log.tag_config(tag, foreground=fg)

    # ── 하단 버튼 바 ─────────────────────────────────────────
    def _build_button_bar(self):
        bar = tk.Frame(self, bg=C["crust"], pady=8)
        bar.pack(fill="x", padx=10, pady=(4, 8))

        self._run_btn = tk.Button(
            bar, text="▶  Run", font=("맑은 고딕", 11, "bold"),
            bg=C["green"], fg=C["crust"], relief="flat", padx=20, pady=6,
            activebackground=C["teal"], activeforeground=C["crust"],
            command=self._on_run
        )
        self._run_btn.pack(side="left", padx=(0, 8))

        self._stop_btn = tk.Button(
            bar, text="■  Stop", font=("맑은 고딕", 11, "bold"),
            bg=C["surface0"], fg=C["overlay0"], relief="flat", padx=20, pady=6,
            state="disabled", command=self._on_stop
        )
        self._stop_btn.pack(side="left")

        # 단축키 힌트
        for hint in [("F5", "Run"), ("Esc", "Stop"), ("Ctrl+S", "Save"), ("Ctrl+R", "Reload")]:
            tk.Label(bar, text=f"{hint[0]} {hint[1]}", font=("Consolas", 7),
                     bg=C["crust"], fg=C["overlay0"]).pack(side="left", padx=6)

        tk.Label(bar, text=f"Python {sys.version.split()[0]}",
                 bg=C["crust"], fg=C["overlay0"], font=("Consolas", 9)
                 ).pack(side="right", padx=10)

    # ── 현재 설정 스냅샷 ────────────────────────────────────────
    def _snapshot(self) -> dict:
        """현재 GUI 설정 전체를 dict로 반환"""
        return {
            "job":         self.job_var.get(),
            "mode":        self.mode_var.get(),
            "env_path":    self._env_path_var.get(),
            "host":        self._host_listbox.get(self._host_listbox.curselection()[0]) if self._host_listbox.curselection() else "",
            "params":      [(k.get(), v.get()) for k, v in self._param_entries],
            "stages":      [s for s, v in self._stage_vars.items() if v.get()],
            "overrides": {
                "overwrite":    self._ov_overwrite.get(),
                "workers":      self._ov_workers.get(),
                "compression":  self._ov_compression.get(),
                "out_dir":      self._ov_out_dir.get(),
                "db_path":      self._ov_db_path.get(),
                "schema":       self._ov_schema.get(),
                "on_error":     self._ov_on_error.get(),
                "excel":        self._ov_excel.get(),
                "csv":          self._ov_csv.get(),
                "max_files":    self._ov_max_files.get(),
                "skip_sql":     self._ov_skip_sql.get(),
                "union_dir":    self._ov_union_dir.get(),
                "sql_dir":      self._ov_sql_dir.get(),
            },
        }

    def _restore_snapshot(self, snap: dict):
        """스냅샷으로 GUI 설정 복원"""
        job = snap.get("job", "")
        if job in self._jobs:
            self.job_var.set(job)
            self._on_job_change()

        self.mode_var.set(snap.get("mode", "run"))
        self._env_path_var.set(snap.get("env_path", "config/env.yml"))

        host = snap.get("host", "")
        if host:
            hosts = list(self._host_listbox.get(0, "end"))
            if host in hosts:
                idx = hosts.index(host)
                self._host_listbox.selection_clear(0, "end")
                self._host_listbox.selection_set(idx)
                self._host_listbox.see(idx)

        self._refresh_param_rows(snap.get("params", []))

        for stage, var in self._stage_vars.items():
            saved = snap.get("stages", [])
            var.set(stage in saved if saved else True)

        ov = snap.get("overrides", {})
        self._ov_overwrite.set(ov.get("overwrite", False))
        self._ov_workers.set(ov.get("workers", 1))
        self._ov_compression.set(ov.get("compression", "gzip"))
        self._ov_out_dir.set(ov.get("out_dir", ""))
        self._ov_db_path.set(ov.get("db_path", ""))
        self._ov_schema.set(ov.get("schema", ""))
        self._ov_on_error.set(ov.get("on_error", "stop"))
        self._ov_excel.set(ov.get("excel", True))
        self._ov_csv.set(ov.get("csv", True))
        self._ov_max_files.set(ov.get("max_files", 10))
        self._ov_skip_sql.set(ov.get("skip_sql", False))
        self._ov_union_dir.set(ov.get("union_dir", ""))
        self._ov_sql_dir.set(ov.get("sql_dir", ""))

        self._refresh_preview()

    # ── Presets (jobs/*.yml 기반) ────────────────────────────────
    def _presets_dir(self) -> Path:
        return Path(self._work_dir.get()) / "jobs"

    def _load_presets(self):
        """jobs/ 폴더의 모든 .yml 파일을 preset 목록으로 로드"""
        jobs_dir = self._presets_dir()
        self._presets = {}  # {stem: Path}
        if jobs_dir.exists():
            for p in sorted(jobs_dir.glob("*.yml")):
                self._presets[p.stem] = p
        self._refresh_preset_combo()

    def _refresh_preset_combo(self):
        names = list(self._presets.keys())
        self._preset_combo["values"] = names
        if self._preset_var.get() not in names:
            self._preset_var.set(names[0] if names else "")

    def _build_new_cfg(self):
        """현재 UI 상태를 base_cfg에 override 적용한 새 dict 반환"""
        import copy
        fname = self.job_var.get()
        base_cfg = self._jobs.get(fname, {})
        new_cfg = copy.deepcopy(base_cfg)
        exp = new_cfg.setdefault("export", {})
        tgt = new_cfg.setdefault("target", {})
        tfm = new_cfg.setdefault("transform", {})
        rep = new_cfg.setdefault("report", {})
        exp["overwrite"]        = self._ov_overwrite.get()
        if self._ov_sql_dir.get().strip():
            exp["sql_dir"]      = self._ov_sql_dir.get().strip()
        exp["parallel_workers"] = self._ov_workers.get()
        exp["compression"]      = self._ov_compression.get()
        if self._ov_out_dir.get().strip():
            exp["out_dir"]      = self._ov_out_dir.get().strip()
        if self._ov_db_path.get().strip():
            tgt["db_path"]      = self._ov_db_path.get().strip()
        if self._ov_schema.get().strip():
            tgt["schema"]       = self._ov_schema.get().strip()
        tfm["on_error"]         = self._ov_on_error.get()
        rep.setdefault("excel", {})["enabled"]      = self._ov_excel.get()
        rep.setdefault("excel", {})["max_files"]    = self._ov_max_files.get()
        rep.setdefault("export_csv", {})["enabled"] = self._ov_csv.get()
        rep["skip_sql"] = self._ov_skip_sql.get()
        if self._ov_union_dir.get().strip():
            rep["csv_union_dir"] = self._ov_union_dir.get().strip()
        sel = self._host_listbox.curselection()
        selected_host = self._host_listbox.get(sel[0]).strip() if sel else ""
        yml_host = (base_cfg.get("source", {}).get("host", "") or "")
        if selected_host and selected_host != yml_host:
            new_cfg.setdefault("source", {})["host"] = selected_host
        params = {k.get().strip(): v.get().strip()
                  for k, v in self._param_entries if k.get().strip()}
        if params:
            new_cfg["params"] = params
        return new_cfg

    def _save_yml_dialog(self, suggest: str, title: str = "Save as yml"):
        """jobs/<name>.yml 저장 공통 다이얼로그. 저장 후 reload + 선택."""
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.configure(bg=C["base"])
        dlg.geometry("380x130")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)
        tk.Label(dlg, text="Job filename (.yml)", font=("Consolas", 10),
                 bg=C["base"], fg=C["text"]).pack(pady=(18, 6))
        name_var = tk.StringVar(value=suggest)
        entry = tk.Entry(dlg, textvariable=name_var, font=("Consolas", 10),
                         bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                         relief="flat", width=30)
        entry.pack()
        entry.select_range(0, "end")
        entry.focus_set()

        def do_save(*_):
            raw = name_var.get().strip()
            if not raw:
                return
            if not raw.endswith(".yml"):
                raw += ".yml"
            jobs_dir = self._presets_dir()
            jobs_dir.mkdir(parents=True, exist_ok=True)
            out_path = jobs_dir / raw
            if out_path.exists():
                if not messagebox.askyesno("Overwrite", f"{raw} already exists. Overwrite?",
                                           parent=dlg):
                    return
            new_cfg = self._build_new_cfg()
            out_path.write_text(
                yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                          sort_keys=False),
                encoding="utf-8"
            )
            self._log_sys(f"Saved: {out_path.name}")
            self._reload_project()
            self.job_var.set(raw)
            self._on_job_change()
            dlg.destroy()

        entry.bind("<Return>", do_save)
        tk.Button(dlg, text="💾  Save", font=("Consolas", 10, "bold"),
                  bg=C["green"], fg=C["crust"], relief="flat", padx=16, pady=4,
                  activebackground=C["teal"],
                  command=do_save).pack(pady=10)

    def _on_preset_save(self):
        fname = self.job_var.get()
        suggest = fname.replace(".yml", "") + "_preset" if fname else "new_job"
        self._save_yml_dialog(suggest, "Save as yml (Preset)")

    def _on_preset_load(self, *_):
        name = self._preset_var.get()
        if not name or name not in self._presets:
            return
        yml_name = self._presets[name].name
        self.job_var.set(yml_name)
        self._on_job_change()
        self._log_sys(f"Loaded: {yml_name}")

    def _on_preset_delete(self):
        name = self._preset_var.get()
        if not name or name not in self._presets:
            return
        yml_path = self._presets[name]
        if not messagebox.askyesno("Delete", f"Delete '{yml_path.name}'?\nThis will permanently delete the file."):
            return
        try:
            yml_path.unlink()
            self._log_sys(f"Deleted: {yml_path.name}")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return
        self._load_presets()
        self._reload_project()

    # ── yml로 저장 ───────────────────────────────────────────────
    def _on_save_yml(self):
        fname = self.job_var.get()
        if not fname or fname not in self._jobs:
            messagebox.showwarning("Notice", "Please select a job first.")
            return
        suggest = fname.replace(".yml", "") + "_custom"
        self._save_yml_dialog(suggest, "Save as yml")

    # ── 프로젝트 로드 ────────────────────────────────────────
    def _reload_project(self):
        wd = Path(self._work_dir.get())
        self._jobs = load_jobs(wd)
        self._env_hosts = load_env_hosts(wd, self._env_path_var.get()
                                         if hasattr(self, "_env_path_var") else "config/env.yml")

        job_names = list(self._jobs.keys())
        if hasattr(self, "_job_combo"):
            self._job_combo["values"] = job_names
            # 이미 선택된 job이 있으면 유지, 없으면 빈 상태 (자동 선택 안 함)
            if self.job_var.get() in job_names:
                self._on_job_change()

        self._log_sys(f"Project loaded: {wd}  (jobs={len(self._jobs)}, "
                      f"env hosts={sum(len(v) for v in self._env_hosts.values())})")

    def _browse_workdir(self):
        d = filedialog.askdirectory(initialdir=self._work_dir.get())
        if d:
            self._work_dir.set(d)
            self._reload_project()

    # ── Job 선택 시 ──────────────────────────────────────────
    def _on_job_change(self, *_):
        fname = self.job_var.get()
        cfg = self._jobs.get(fname, {})
        if not cfg:
            return

        # 요약 표시
        stages = cfg.get("pipeline", {}).get("stages", [])
        src_type = cfg.get("source", {}).get("type", "?")
        src_host = cfg.get("source", {}).get("host", "")
        tgt_type = cfg.get("target", {}).get("type", "?")
        # host listbox 갱신
        hosts = self._env_hosts.get(src_type, [])
        if src_host and src_host not in hosts:
            hosts = [src_host] + hosts
        self._host_listbox.delete(0, "end")
        for h in hosts:
            self._host_listbox.insert("end", h)
        # 기본 host 선택
        if hosts:
            default = src_host or hosts[0]
            idx = hosts.index(default) if default in hosts else 0
            self._host_listbox.selection_set(idx)
            self._host_listbox.see(idx)

        # params 갱신 + SQL 파라미터 자동 감지
        params = cfg.get("params", {})
        self._refresh_param_rows(list(params.items()))
        # sql_dir 스캔 (after() 로 약간 지연 — UI 빌드 완료 후 실행)
        self.after(50, self._scan_and_suggest_params)

        # Pipeline 시각화 갱신
        self._refresh_pipeline_ui(stages)

        # Job Override 위젯 → yml 기본값으로 초기화
        exp = cfg.get("export", {})
        tgt = cfg.get("target", {})
        tfm = cfg.get("transform", {})
        rep = cfg.get("report", {})
        self._ov_overwrite.set(bool(exp.get("overwrite", False)))
        self._ov_workers.set(int(exp.get("parallel_workers", 1)))
        self._ov_compression.set(str(exp.get("compression", "gzip")))
        self._ov_out_dir.set("")
        self._ov_db_path.set("")
        self._ov_schema.set("")
        self._ov_on_error.set(str(tfm.get("on_error", "stop")))
        self._ov_excel.set(bool(rep.get("excel", {}).get("enabled", True)))
        self._ov_csv.set(bool(rep.get("export_csv", {}).get("enabled", True)))
        self._ov_max_files.set(int(rep.get("excel", {}).get("max_files", 10)))
        self._ov_skip_sql.set(bool(rep.get("skip_sql", False)))
        self._ov_union_dir.set("")
        self._ov_sql_dir.set("")

        # target 종류에 따라 행 show/hide
        if tgt_type in ("duckdb", "sqlite3"):
            self._ov_db_path_row.pack(fill="x", padx=12, pady=2)
            self._ov_schema_row.pack_forget()
        elif tgt_type == "oracle":
            self._ov_schema_row.pack(fill="x", padx=12, pady=2)
            self._ov_db_path_row.pack_forget()
        else:
            self._ov_db_path_row.pack_forget()
            self._ov_schema_row.pack_forget()

        # SQL 선택 초기화
        self._selected_sqls = set()
        self._update_sql_preview()

        self._refresh_preview()

    # ── SQL 파라미터 자동 감지 ─────────────────────────────────
    def _scan_and_suggest_params(self):
        """
        현재 sql_dir (override 또는 yml 기본값) 을 스캔해서
        발견된 파라미터를 Params 섹션에 자동 제시.
        - sql_dir override 있으면: 해당 dir 파라미터만 사용 (yml 기본 params는 값만 참조)
        - sql_dir override 없으면: yml params + 스캔 결과 병합
        - 이미 사용자가 입력한 값은 항상 유지
        """
        fname = self.job_var.get()
        if not fname:
            return
        cfg = self._jobs.get(fname, {})
        wd = Path(self._work_dir.get())

        # sql_dir 결정: override > yml > 기본
        sql_dir_override = self._ov_sql_dir.get().strip()
        sql_dir_yml = cfg.get("export", {}).get("sql_dir", "sql/export")
        sql_dir_rel = sql_dir_override or sql_dir_yml
        sql_dir = Path(sql_dir_rel) if Path(sql_dir_rel).is_absolute() else wd / sql_dir_rel

        # sql filter 선택 있으면 해당 파일만 스캔
        if self._selected_sqls:
            sel_files = [wd / p for p in self._selected_sqls if (wd / p).exists()]
            detected = _scan_params_from_files(sel_files)
        else:
            detected = scan_sql_params(sql_dir)

        # 현재 params 딕셔너리 (사용자 입력값)
        current = {k.get(): v.get() for k, v in self._param_entries if k.get()}

        # yml 기본값
        yml_params = cfg.get("params", {})

        if sql_dir_override and sql_dir_override != sql_dir_yml:
            # sql_dir override 케이스:
            # detected 파라미터만 표시 (미사용 yml 파라미터는 추가 안 함)
            # yml에 기본값이 있으면 값 참조, 사용자 입력값 우선
            merged = {}
            for p in detected:
                if p in current:
                    merged[p] = current[p]
                elif p in yml_params:
                    merged[p] = str(yml_params[p])
                else:
                    merged[p] = ""
            # # 사용자가 직접 추가한 값 (detected에 없어도 유지)
            # for k, v in current.items():
            #     if k not in merged and v:  # 값이 있는 것만 유지
            #         merged[k] = v
        else:
            # 기본 케이스: yml params + detected 병합
            merged = {}
            for p in detected:
                if p in current:
                    merged[p] = current[p]
                elif p in yml_params:
                    merged[p] = str(yml_params[p])
                else:
                    merged[p] = ""
            # 사용자가 직접 추가한 값은 항상 유지
            for k, v in current.items():
                if k not in merged:
                    merged[k] = v

        self._refresh_param_rows(list(merged.items()))
        if detected:
            self._log_sys(f"SQL params detected: {', '.join(detected)}")

    # ── Pipeline 시각화 UI ──────────────────────────────────
    def _refresh_pipeline_ui(self, stages):
        for w in self._pipeline_frame.winfo_children():
            w.destroy()
        self._stage_vars = {}
        self._pipeline_widgets = []

        STAGE_DISPLAY = {
            "export":     ("Export",    C["blue"]),
            "load_local": ("Load",      C["teal"]),
            "transform":  ("Transform", C["mauve"]),
            "report":     ("Report",    C["peach"]),
        }

        # 헤더: "Stages" + 전체선택/해제 버튼
        hdr = tk.Frame(self._pipeline_frame, bg=C["mantle"])
        hdr.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 2))
        tk.Label(hdr, text="Stages", font=("Consolas", 8, "bold"),
                 bg=C["mantle"], fg=C["blue"]).pack(side="left")

        def _select_all():
            for _, v, _ in self._pipeline_widgets:
                v.set(True)
            self._refresh_preview()

        def _deselect_all():
            for _, v, _ in self._pipeline_widgets:
                v.set(False)
            self._refresh_preview()

        for txt, cmd in [("all", _select_all), ("none", _deselect_all)]:
            tk.Button(hdr, text=txt, font=("Consolas", 7),
                      bg=C["surface0"], fg=C["subtext"], relief="flat",
                      padx=5, pady=0, activebackground=C["surface1"],
                      command=cmd).pack(side="left", padx=(6, 0))

        # 2×2 그리드 — yml stages 순서대로, 초기값 모두 True (yml에 있으면 켜짐)
        positions = [(0, 1), (1, 1), (0, 2), (1, 2)]
        for idx, stage in enumerate(stages[:4]):
            display, color = STAGE_DISPLAY.get(stage, (stage, C["text"]))
            var = tk.BooleanVar(value=True)   # yml에 있는 stage는 기본 ON
            self._stage_vars[stage] = var
            col, row = positions[idx]
            cb = tk.Checkbutton(
                self._pipeline_frame, text=display, variable=var,
                font=("Consolas", 9),
                bg=C["mantle"], fg=color,
                selectcolor=C["surface0"],
                activebackground=C["mantle"], activeforeground=color,
                anchor="w",
                command=lambda: self._refresh_preview()
            )
            cb.grid(row=row, column=col, sticky="w",
                    padx=(4 if col == 0 else 14, 4), pady=1)
            self._pipeline_widgets.append((cb, var, color))

    def _refresh_pipeline_buttons(self):
        pass

    # ── Param 행 관리 ────────────────────────────────────────
    def _refresh_param_rows(self, pairs: list):
        for w in self._params_frame.winfo_children():
            w.destroy()
        self._param_entries = []
        for k, v in pairs:
            self._add_param_row(k, str(v))

    def _add_param_row(self, key="", value=""):
        k_var = tk.StringVar(value=key)
        v_var = tk.StringVar(value=value)
        self._param_entries.append((k_var, v_var))
        k_var.trace_add("write", lambda *_: self._refresh_preview())
        v_var.trace_add("write", lambda *_: self._refresh_preview())

        row = tk.Frame(self._params_frame, bg=C["mantle"])
        row.pack(fill="x", pady=1)

        tk.Entry(row, textvariable=k_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=("Consolas", 9),
                 width=10).pack(side="left", padx=(0, 2))
        tk.Label(row, text="=", bg=C["mantle"], fg=C["overlay0"],
                 font=("Consolas", 9)).pack(side="left")
        tk.Entry(row, textvariable=v_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=("Consolas", 9),
                 width=14).pack(side="left", padx=(2, 0), fill="x", expand=True)

        def remove(r=row, pair=(k_var, v_var)):
            r.destroy()
            if pair in self._param_entries:
                self._param_entries.remove(pair)
            self._refresh_preview()
        tk.Button(row, text="✕", font=("Consolas", 8), bg=C["mantle"],
                  fg=C["overlay0"], relief="flat", padx=4,
                  command=remove).pack(side="right")

    # ── SQL 선택 ─────────────────────────────────────────────
    def _open_sql_selector(self):
        fname = self.job_var.get()
        cfg = self._jobs.get(fname, {})
        # 초기 디렉토리: job의 sql_dir 기준, 없으면 workdir
        sql_dir_rel = cfg.get("export", {}).get("sql_dir", "sql/export")
        init_dir = Path(self._work_dir.get()) / sql_dir_rel
        if not init_dir.exists():
            init_dir = Path(self._work_dir.get())

        files = filedialog.askopenfilenames(
            title="Select SQL files (multi-select)",
            initialdir=str(init_dir),
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")],
            parent=self,
        )
        if not files:
            return

        wd = Path(self._work_dir.get())
        new_selected = set()
        for f in files:
            p = Path(f)
            try:
                # workdir 기준 상대경로로 변환
                rel = p.relative_to(wd).as_posix()
            except ValueError:
                # workdir 밖이면 절대경로 사용
                rel = str(p)
            new_selected.add(rel)

        self._selected_sqls = new_selected
        self._update_sql_preview()
        self._refresh_preview()

    def _update_sql_preview(self):
        count = len(self._selected_sqls)
        if count == 0:
            self._sql_count_label.config(text="(all)", fg=C["overlay0"])
        else:
            self._sql_count_label.config(text=f"{count} selected", fg=C["green"])

        self._sql_preview.config(state="normal")
        self._sql_preview.delete("1.0", "end")
        if self._selected_sqls:
            for s in sorted(self._selected_sqls):
                self._sql_preview.insert("end", f"  {s}\n")
        else:
            self._sql_preview.insert("end", "  (none selected = run all)\n")
        self._sql_preview.config(state="disabled")

    # ── Command 빌드 & 미리보기 ──────────────────────────────
    def _build_command(self) -> list[str]:
        # 실행은 cwd=workdir 로 하므로 상대경로만 사용
        cmd = ["python", "runner.py"]

        fname = self.job_var.get()
        if fname:
            cmd += ["--job", fname]

        env_path = self._env_path_var.get().strip()
        if env_path and env_path != "config/env.yml":
            cmd += ["--env", env_path]

        mode = self.mode_var.get()
        if mode != "run":
            cmd += ["--mode", mode]

        if self._debug_var.get():
            cmd.append("--debug")

        # for k_var, v_var in self._param_entries:
        #     k, v = k_var.get().strip(), v_var.get().strip()
        #     if k and v:
        #         cmd += ["--param", f"{k}={v}"]
        
        # 수정
        # sql_dir override 시: 실제 해당 폴더 SQL에 등장하는 파라미터만 전달
        _sql_dir_override = self._ov_sql_dir.get().strip()
        _yml_sql_dir = (self._jobs.get(self.job_var.get(), {})
                        .get("export", {}).get("sql_dir", "sql/export"))

        if _sql_dir_override and _sql_dir_override != _yml_sql_dir:
            # 해당 sql_dir에서 실제 사용되는 파라미터 목록
            _wd = Path(self._work_dir.get())
            _sd = Path(_sql_dir_override) if Path(_sql_dir_override).is_absolute() else _wd / _sql_dir_override
            if self._selected_sqls:
                _active_params = set(_scan_params_from_files([_wd / p for p in self._selected_sqls if (_wd / p).exists()]))
            else:
                _active_params = set(scan_sql_params(_sd))
            for k_var, v_var in self._param_entries:
                k, v = k_var.get().strip(), v_var.get().strip()
                if k and v and k in _active_params:   # ← 실제 사용 파라미터만
                    cmd += ["--param", f"{k}={v}"]
        else:
            for k_var, v_var in self._param_entries:
                k, v = k_var.get().strip(), v_var.get().strip()
                if k and v:
                    cmd += ["--param", f"{k}={v}"]
        

        # ── Job Override → --set 자동 생성 ──────────────────────
        fname2 = self.job_var.get()
        cfg = self._jobs.get(fname2, {})
        exp_cfg = cfg.get("export", {})
        tgt_cfg = cfg.get("target", {})
        tfm_cfg = cfg.get("transform", {})
        rep_cfg = cfg.get("report", {})

        # export.overwrite
        ov_val = self._ov_overwrite.get()
        if ov_val != bool(exp_cfg.get("overwrite", False)):
            cmd += ["--set", f"export.overwrite={str(ov_val).lower()}"]

        # export.parallel_workers
        wk_val = self._ov_workers.get()
        if wk_val != int(exp_cfg.get("parallel_workers", 1)):
            cmd += ["--set", f"export.parallel_workers={wk_val}"]

        # export.compression
        cmp_val = self._ov_compression.get()
        if cmp_val != str(exp_cfg.get("compression", "gzip")):
            cmd += ["--set", f"export.compression={cmp_val}"]

        # export.out_dir
        out_val = self._ov_out_dir.get().strip()
        if out_val:
            cmd += ["--set", f"export.out_dir={out_val}"]

        # target.db_path
        db_val = self._ov_db_path.get().strip()
        if db_val:
            cmd += ["--set", f"target.db_path={db_val}"]

        # target.schema
        sc_val = self._ov_schema.get().strip()
        if sc_val:
            cmd += ["--set", f"target.schema={sc_val}"]

        # transform.on_error
        oe_val = self._ov_on_error.get()
        if oe_val != str(tfm_cfg.get("on_error", "stop")):
            cmd += ["--set", f"transform.on_error={oe_val}"]

        # report.excel.enabled
        ex_val = self._ov_excel.get()
        if ex_val != bool(rep_cfg.get("excel", {}).get("enabled", True)):
            cmd += ["--set", f"report.excel.enabled={str(ex_val).lower()}"]

        # report.export_csv.enabled
        cv_val = self._ov_csv.get()
        if cv_val != bool(rep_cfg.get("export_csv", {}).get("enabled", True)):
            cmd += ["--set", f"report.export_csv.enabled={str(cv_val).lower()}"]

        # report.excel.max_files
        mf_val = self._ov_max_files.get()
        if mf_val != int(rep_cfg.get("excel", {}).get("max_files", 10)):
            cmd += ["--set", f"report.excel.max_files={mf_val}"]

        # export.sql_dir
        sd_val = self._ov_sql_dir.get().strip()
        fname_sd = self.job_var.get()
        yml_sql_dir = (self._jobs.get(fname_sd, {}).get("export", {}).get("sql_dir", "") or "")
        if sd_val and sd_val != yml_sql_dir:
            cmd += ["--set", f"export.sql_dir={sd_val}"]

        # report.skip_sql
        ss_val = self._ov_skip_sql.get()
        if ss_val != bool(rep_cfg.get("skip_sql", False)):
            cmd += ["--set", f"report.skip_sql={str(ss_val).lower()}"]

        # report.csv_union_dir
        ud_val = self._ov_union_dir.get().strip()
        if ud_val and ud_val != rep_cfg.get("csv_union_dir", ""):
            cmd += ["--set", f"report.csv_union_dir={ud_val}"]

        # Host override: listbox 선택값이 yml과 다를 때만 --set으로 전달
        sel = self._host_listbox.curselection()
        selected_host = self._host_listbox.get(sel[0]) if sel else ""
        fname3 = self.job_var.get()
        yml_host = (self._jobs.get(fname3, {}).get("source", {}).get("host", "") or "")
        if selected_host and selected_host != yml_host:
            cmd += ["--set", f"source.host={selected_host}"]

        # pass selected stages via --stage (none selected = run all)
        selected_stages = [s for s, v in self._stage_vars.items() if v.get()]
        all_stages = list(self._stage_vars.keys())
        # 전부 선택이거나 아무것도 없으면 --stage 생략 (전체 실행)
        if selected_stages and selected_stages != all_stages:
            for stage in selected_stages:
                cmd += ["--stage", stage]

        # SQL 파일 선택 → --include 패턴
        # workdir 기준 rel_path: "sql/export/mart/01_bigt.sql"
        # sql_dir 기준으로 변환:  "mart/01_bigt"  (export_stage와 일치)
        _sql_dir_rel = (self._ov_sql_dir.get().strip()
                        or (self._jobs.get(self.job_var.get(), {})
                            .get("export", {}).get("sql_dir", "sql/export")))
        for rel_path in sorted(self._selected_sqls):
            p = Path(rel_path)
            try:
                # sql_dir prefix 제거 후 확장자 제거
                pattern = p.relative_to(Path(_sql_dir_rel)).with_suffix("").as_posix()
            except ValueError:
                # sql_dir 밖이면 stem만
                pattern = p.stem
            cmd += ["--include", str(pattern)]  # str 명시 (WindowsPath 방지)

        return [str(x) for x in cmd]  # 전체 str 보장

    def _refresh_preview(self):
        try:
            cmd = self._build_command()
            text = " ".join(cmd)
        except Exception as e:
            text = f"(오류: {e})"

        self._cmd_preview.config(state="normal")
        self._cmd_preview.delete("1.0", "end")
        self._cmd_preview.insert("end", text)
        self._cmd_preview.config(state="disabled")

    # ── 로그 헬퍼 ────────────────────────────────────────────
    def _log_write(self, text: str, tag="INFO"):
        self._log.insert("end", text + "\n", tag)
        self._log.see("end")

    def _log_sys(self, msg):
        self._log_write(msg, "SYS")

    def _clear_log(self):
        self._log.delete("1.0", "end")

    def _set_status(self, text, color):
        self._status_label.config(text=text, fg=color)

    # ── 실행 / 멈춤 ──────────────────────────────────────────
    def _on_run(self):
        cmd = self._build_command()
        self._log_sys(f"Run: {chr(32).join(cmd)}")
        self._set_status("● running", C["green"])
        import time
        self._elapsed_start = time.time()
        self._progress_bar["value"] = 0
        self._progress_label.config(text="Starting...")
        self._elapsed_job_id = self.after(1000, self._tick_elapsed)

        self._run_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="disabled")
        self._stop_btn.config(state="normal", bg=C["red"], fg=C["crust"],
                              activebackground=C["peach"])

        env = os.environ.copy()
        # Windows에서 한글 깨짐 방지: PYTHONIOENCODING, PYTHONUTF8 강제 설정
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding="utf-8",
                errors="replace",
                cwd=self._work_dir.get(),  # workdir 기준 실행
                env=env,
                creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP
                               if sys.platform == "win32" else 0),
            )
        except FileNotFoundError as e:
            self._log_write(f"[Error] Failed to start: {e}", "ERROR")
            self._reset_buttons()
            return

        threading.Thread(target=self._stream_output, daemon=True).start()

    def _stream_output(self):
        import re
        stage_pat = re.compile(r"\[(\d+)/(\d+)\]")
        for line in self._process.stdout:
            line = line.rstrip("\n")
            tag = self._guess_tag(line)
            self.after(0, self._log_write, line, tag)
            # [N/M] 패턴 파싱 → progress 업데이트
            m = stage_pat.search(line)
            if m:
                cur, total = int(m.group(1)), int(m.group(2))
                pct = int((cur - 1) / total * 100)
                label = f"Stage {cur}/{total}"
                self.after(0, self._update_progress, pct, label)
        ret = self._process.wait()
        self.after(0, self._on_done, ret)

    def _guess_tag(self, line: str) -> str:
        low = line.lower()
        if any(k in low for k in ("error", "exception", "traceback", "failed", "오류")):
            return "ERROR"
        if any(k in low for k in ("warn", "warning", "경고")):
            return "WARN"
        if any(k in low for k in ("done", "success", "finish", "completed", "완료")):
            return "SUCCESS"
        if any(k in low for k in ("===", "---", "pipeline", "stage", "job start", "job finish")):
            return "SYS"
        return "INFO"

    def _update_progress(self, pct: int, label: str):
        self._progress_bar["value"] = pct
        elapsed = ""
        if self._elapsed_start:
            import time
            secs = int(time.time() - self._elapsed_start)
            elapsed = f"  {secs//60:02d}:{secs%60:02d}"
        self._progress_label.config(text=f"{label}{elapsed}")

    def _tick_elapsed(self):
        import time
        if self._elapsed_start is None:
            return
        secs = int(time.time() - self._elapsed_start)
        cur_label = self._progress_label.cget("text").split("  ")[0]
        self._progress_label.config(text=f"{cur_label}  {secs//60:02d}:{secs%60:02d}")
        self._elapsed_job_id = self.after(1000, self._tick_elapsed)

    def _on_done(self, ret: int):
        import time
        # elapsed 타이머 정지
        if self._elapsed_job_id:
            self.after_cancel(self._elapsed_job_id)
            self._elapsed_job_id = None
        if self._elapsed_start:
            secs = int(time.time() - self._elapsed_start)
            self._elapsed_start = None
        else:
            secs = 0
        elapsed_str = f"{secs//60:02d}:{secs%60:02d}"

        if ret == 0:
            self._log_write(f"✔ Done  ({elapsed_str})", "SUCCESS")
            self._set_status("● done", C["green"])
            self._progress_bar["value"] = 100
            self._progress_label.config(text=f"Done  {elapsed_str}")
        elif ret < 0:
            self._log_write(f"✖ Stopped  ({elapsed_str})", "WARN")
            self._set_status("● stopped", C["yellow"])
            self._progress_label.config(text=f"Stopped  {elapsed_str}")
        else:
            self._log_write(f"✖ Error (code={ret})  ({elapsed_str})", "ERROR")
            self._set_status(f"● error (code={ret})", C["red"])
            self._progress_label.config(text=f"Error  {elapsed_str}")
        self._reset_buttons()

    def _on_stop(self):
        if self._process and self._process.poll() is None:
            self._log_write("Stopping process...", "WARN")
            if sys.platform == "win32":
                self._process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self._process.terminate()
        self._reset_buttons()
        self._set_status("● stopped", C["yellow"])

    def _reset_buttons(self):
        self._run_btn.config(state="normal", bg=C["green"], fg=C["crust"])
        self._stop_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="readonly")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = BatchRunnerGUI()
    app.mainloop()
