"""
batch_runner_gui.py  ─  Tkinter GUI for ELT Runner
실행: python batch_runner_gui.py   (batch_runner 프로젝트 루트에서)
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import subprocess
import threading
import sys
import os
import signal
import json
import yaml
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# 버전 (VERSION 파일에서 읽기)
# ─────────────────────────────────────────────────────────────
def _read_version() -> str:
    vf = Path(__file__).resolve().parent / "VERSION"
    if vf.exists():
        return vf.read_text(encoding="utf-8").strip()
    return "0.0"

APP_VERSION = _read_version()

# ─────────────────────────────────────────────────────────────
# 색상 팔레트 11종
# ─────────────────────────────────────────────────────────────
THEMES = {
    "Mocha": {           # Dark — Catppuccin Mocha
        "base":    "#1e1e2e", "mantle":  "#181825", "crust":   "#11111b",
        "surface0":"#313244", "surface1":"#45475a", "surface2":"#585b70",
        "overlay0":"#6c7086", "overlay1":"#7f849c",
        "text":    "#cdd6f4", "subtext": "#a6adc8",
        "blue":    "#89b4fa", "green":   "#a6e3a1", "yellow":  "#f9e2af",
        "red":     "#f38ba8", "peach":   "#fab387", "mauve":   "#cba6f7",
        "teal":    "#94e2d5", "sky":     "#89dceb",
    },
    "Nord": {            # Dark — Nord
        "base":    "#2e3440", "mantle":  "#272c36", "crust":   "#1e2430",
        "surface0":"#3b4252", "surface1":"#434c5e", "surface2":"#4c566a",
        "overlay0":"#616e88", "overlay1":"#7b88a1",
        "text":    "#eceff4", "subtext": "#d8dee9",
        "blue":    "#81a1c1", "green":   "#a3be8c", "yellow":  "#ebcb8b",
        "red":     "#bf616a", "peach":   "#d08770", "mauve":   "#b48ead",
        "teal":    "#88c0d0", "sky":     "#8fbcbb",
    },
    "Dracula": {         # Dark — Dracula
        "base":    "#282a36", "mantle":  "#21222c", "crust":   "#191a21",
        "surface0":"#343746", "surface1":"#424450", "surface2":"#515360",
        "overlay0":"#6272a4", "overlay1":"#7384b0",
        "text":    "#f8f8f2", "subtext": "#bfbfb2",
        "blue":    "#8be9fd", "green":   "#50fa7b", "yellow":  "#f1fa8c",
        "red":     "#ff5555", "peach":   "#ffb86c", "mauve":   "#bd93f9",
        "teal":    "#8be9fd", "sky":     "#69c3ff",
    },
    "Tokyo Night": {     # Dark — Tokyo Night
        "base":    "#1a1b26", "mantle":  "#16161e", "crust":   "#101014",
        "surface0":"#232433", "surface1":"#2f3043", "surface2":"#3b3d57",
        "overlay0":"#565f89", "overlay1":"#6b7394",
        "text":    "#c0caf5", "subtext": "#9aa5ce",
        "blue":    "#7aa2f7", "green":   "#9ece6a", "yellow":  "#e0af68",
        "red":     "#f7768e", "peach":   "#ff9e64", "mauve":   "#bb9af7",
        "teal":    "#73daca", "sky":     "#7dcfff",
    },
    "One Dark": {        # Dark — Atom One Dark
        "base":    "#282c34", "mantle":  "#21252b", "crust":   "#181a1f",
        "surface0":"#2c313a", "surface1":"#383e4a", "surface2":"#454b56",
        "overlay0":"#5c6370", "overlay1":"#737984",
        "text":    "#abb2bf", "subtext": "#8b929e",
        "blue":    "#61afef", "green":   "#98c379", "yellow":  "#e5c07b",
        "red":     "#e06c75", "peach":   "#d19a66", "mauve":   "#c678dd",
        "teal":    "#56b6c2", "sky":     "#67cddb",
    },
    "Latte": {           # Light — Catppuccin Latte
        "base":    "#eff1f5", "mantle":  "#e6e9ef", "crust":   "#dce0e8",
        "surface0":"#ccd0da", "surface1":"#bcc0cc", "surface2":"#acb0be",
        "overlay0":"#9ca0b0", "overlay1":"#8c8fa1",
        "text":    "#4c4f69", "subtext": "#6c6f85",
        "blue":    "#1e66f5", "green":   "#40a02b", "yellow":  "#df8e1d",
        "red":     "#d20f39", "peach":   "#fe640b", "mauve":   "#8839ef",
        "teal":    "#179299", "sky":     "#04a5e5",
    },
    "White": {           # Light — Clean White
        "base":    "#ffffff", "mantle":  "#f5f5f5", "crust":   "#ebebeb",
        "surface0":"#e0e0e0", "surface1":"#cccccc", "surface2":"#b8b8b8",
        "overlay0":"#999999", "overlay1":"#808080",
        "text":    "#1a1a1a", "subtext": "#444444",
        "blue":    "#0066cc", "green":   "#2d8a2d", "yellow":  "#b38600",
        "red":     "#cc0000", "peach":   "#e06000", "mauve":   "#7700cc",
        "teal":    "#007a7a", "sky":     "#0099bb",
    },
    "Paper": {           # Light — Warm Paper
        "base":    "#f8f4e8", "mantle":  "#f0ead6", "crust":   "#e8e0c8",
        "surface0":"#ddd5bb", "surface1":"#cec5a8", "surface2":"#b8ad92",
        "overlay0":"#9a9070", "overlay1":"#807558",
        "text":    "#2c2416", "subtext": "#5a4e36",
        "blue":    "#3d6b8e", "green":   "#4a7c3f", "yellow":  "#8a6500",
        "red":     "#a02020", "peach":   "#a04818", "mauve":   "#6b3fa0",
        "teal":    "#2a7a6a", "sky":     "#2068a0",
    },
    "Solarized Light": { # Light — Solarized Light
        "base":    "#fdf6e3", "mantle":  "#eee8d5", "crust":   "#e4ddc8",
        "surface0":"#d5cdb6", "surface1":"#c5bda6", "surface2":"#b0a890",
        "overlay0":"#93a1a1", "overlay1":"#839496",
        "text":    "#657b83", "subtext": "#586e75",
        "blue":    "#268bd2", "green":   "#859900", "yellow":  "#b58900",
        "red":     "#dc322f", "peach":   "#cb4b16", "mauve":   "#6c71c4",
        "teal":    "#2aa198", "sky":     "#2aa6c4",
    },
    "Gruvbox Light": {   # Light — Gruvbox Light
        "base":    "#fbf1c7", "mantle":  "#f2e5bc", "crust":   "#e8d8a8",
        "surface0":"#d5c4a1", "surface1":"#c9b995", "surface2":"#bdae93",
        "overlay0":"#a89984", "overlay1":"#928374",
        "text":    "#3c3836", "subtext": "#504945",
        "blue":    "#458588", "green":   "#79740e", "yellow":  "#b57614",
        "red":     "#cc241d", "peach":   "#d65d0e", "mauve":   "#8f3f71",
        "teal":    "#427b58", "sky":     "#4596a8",
    },
    "Rose Pine Dawn": {  # Light — Rose Pine Dawn
        "base":    "#faf4ed", "mantle":  "#f2e9e1", "crust":   "#e4d7c8",
        "surface0":"#dfdad0", "surface1":"#d0c8be", "surface2":"#c2b9af",
        "overlay0":"#9893a5", "overlay1":"#807d8e",
        "text":    "#575279", "subtext": "#6e6a86",
        "blue":    "#286983", "green":   "#56949f", "yellow":  "#ea9d34",
        "red":     "#b4637a", "peach":   "#d7827e", "mauve":   "#907aa9",
        "teal":    "#56949f", "sky":     "#569fb5",
    },
}

# 현재 테마 (전역 — 위젯 생성 시 참조)
_CURRENT_THEME = "Mocha"
C = dict(THEMES[_CURRENT_THEME])

# ─────────────────────────────────────────────────────────────
# 폰트 시스템
# ─────────────────────────────────────────────────────────────
# FONT_FAMILY = "IBM Plex Sans KR"
FONT_FAMILY = "Malgun Gothic"
FONT_MONO   = "Consolas"

def _load_bundled_fonts():
    """fonts/ 폴더의 ttf/otf를 프로세스 전용으로 등록 (시스템 설치 불필요)"""
    if sys.platform != "win32":
        return
    fonts_dir = Path(__file__).parent / "fonts"
    if not fonts_dir.is_dir():
        return
    import ctypes
    FR_PRIVATE = 0x10
    gdi32 = ctypes.windll.gdi32
    for f in fonts_dir.iterdir():
        if f.suffix.lower() in (".ttf", ".otf"):
            gdi32.AddFontResourceExW(str(f.resolve()), FR_PRIVATE, 0)

def _resolve_font():
    """번들 폰트 로드 후, 없으면 Malgun Gothic fallback"""
    global FONT_FAMILY
    _load_bundled_fonts()
    try:
        import tkinter as _tk
        _r = _tk.Tk()
        _r.withdraw()
        available = _r.tk.call("font", "families")
        _r.destroy()
        if FONT_FAMILY not in available:
            FONT_FAMILY = "Malgun Gothic"
    except Exception:
        FONT_FAMILY = "Malgun Gothic"
_resolve_font()
FONTS = {
    "h1":         (FONT_FAMILY, 14, "bold"),   # 최상위 헤더
    "h2":         (FONT_FAMILY, 11, "bold"),   # 섹션 헤더
    "body":       (FONT_FAMILY, 10),           # 기본 텍스트
    "body_bold":  (FONT_FAMILY, 10, "bold"),
    "small":      (FONT_FAMILY, 9),            # 보조 힌트
    "mono":       (FONT_MONO,   10),           # 입력 필드
    "mono_small": (FONT_MONO,   9),            # Override 라벨
    "log":        (FONT_MONO,   10),           # 로그 본문
    "cmd":        (FONT_MONO,   10),           # 커맨드 프리뷰
    "button":     (FONT_FAMILY, 11, "bold"),   # Run/Stop
    "button_sm":  (FONT_FAMILY, 10),           # 보조 버튼
    "shortcut":   (FONT_MONO,   8),            # 단축키 힌트
}

# ─────────────────────────────────────────────────────────────
# 설정 파일 경로 (geometry, theme 저장)
# ─────────────────────────────────────────────────────────────
_CONF_PATH = Path.home() / ".elt_runner_gui.conf"


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
    tree = {"__files__": []}
    for item in sorted(sql_dir.iterdir()):
        if item.is_dir():
            tree[item.name] = _walk(item)
        elif item.is_file() and item.suffix.lower() == ".sql":
            tree["__files__"].append(item.name)
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
        tk.Label(hdr, text="📂  SQL File Select", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14)
        tk.Label(hdr, text=str(self.sql_dir), font=FONTS["shortcut"],
                 bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=6)

        # 전체선택 / 전체해제
        ctrl = tk.Frame(self, bg=C["base"], pady=4)
        ctrl.pack(fill="x", padx=10)
        tk.Button(ctrl, text="Select All", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=8,
                  activebackground=C["surface1"],
                  command=self._select_all).pack(side="left", padx=(0, 4))
        tk.Button(ctrl, text="Deselect All", font=FONTS["mono_small"],
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
        
        # 트리 렌더링
        tree = collect_sql_tree(self.sql_dir)
        self._render_tree(self._scroll_frame, tree, prefix="", indent=0)

        # 하단 버튼
        btn_bar = tk.Frame(self, bg=C["mantle"], pady=8)
        btn_bar.pack(fill="x")
        self._count_label = tk.Label(btn_bar, text="", font=FONTS["mono_small"],
                                     bg=C["mantle"], fg=C["subtext"])
        self._count_label.pack(side="left", padx=14)
        self._update_count()

        tk.Button(btn_bar, text="Cancel", font=FONTS["mono"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=14, pady=4,
                  activebackground=C["surface1"],
                  command=self.destroy).pack(side="right", padx=8)
        tk.Button(btn_bar, text="✔  OK", font=FONTS["body_bold"],
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
                font=FONTS["mono_small"], anchor="w"
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
                font=(FONT_MONO, 9, "bold"),
                bg=C["crust"], fg=C["blue"], relief="flat",
                anchor="w", padx=pad
            )
            folder_btn.config(command=make_toggle(child_frame, toggle_var, folder_btn))
            folder_btn.pack(fill="x", side="left", expand=True)

            # 폴더 전체 선택 버튼
            tk.Button(
                folder_row, text="All", font=FONTS["shortcut"],
                bg=C["surface0"], fg=C["subtext"], relief="flat", padx=6,
                activebackground=C["surface1"],
                command=lambda fp=folder_prefix, nd=sub: self._select_folder(fp, nd, True)
            ).pack(side="right", padx=2)
            tk.Button(
                folder_row, text="None", font=FONTS["shortcut"],
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
# 접이식 섹션 위젯
# ─────────────────────────────────────────────────────────────
class CollapsibleSection(tk.Frame):
    """클릭으로 접기/펼치기 가능한 섹션 위젯"""

    def __init__(self, parent, title, color_key="blue", expanded=True, **kw):
        super().__init__(parent, bg=C["mantle"], **kw)
        self._expanded = expanded
        self._color_key = color_key

        # 헤더
        self._header = tk.Frame(self, bg=C["surface0"], cursor="hand2")
        self._header.pack(fill="x", padx=4, pady=(6, 0))

        self._color_bar = tk.Frame(self._header, bg=C[color_key], width=3)
        self._color_bar.pack(side="left", fill="y")
        self._color_bar.pack_propagate(False)

        self._toggle_label = tk.Label(
            self._header, text=" ▼ " if expanded else " ▶ ",
            font=FONTS["h2"], bg=C["surface0"], fg=C[color_key]
        )
        self._toggle_label.pack(side="left", padx=(4, 0))

        self._title_label = tk.Label(
            self._header, text=title, font=FONTS["h2"],
            bg=C["surface0"], fg=C["text"]
        )
        self._title_label.pack(side="left", padx=(2, 8), pady=5)

        # 본문
        self._body = tk.Frame(self, bg=C["mantle"])
        if expanded:
            self._body.pack(fill="x")

        # 클릭 바인딩
        for w in (self._header, self._toggle_label, self._title_label, self._color_bar):
            w.bind("<Button-1>", lambda e: self.toggle())

    @property
    def body(self):
        return self._body

    def toggle(self):
        self._expanded = not self._expanded
        if self._expanded:
            self._body.pack(fill="x")
            self._toggle_label.config(text=" ▼ ")
        else:
            self._body.pack_forget()
            self._toggle_label.config(text=" ▶ ")


# ─────────────────────────────────────────────────────────────
# Stage 토글 버튼 설정
# ─────────────────────────────────────────────────────────────
STAGE_CONFIG = [
    ("export",     "Export",    "blue"),
    ("load_local", "Load",      "teal"),
    ("transform",  "Transform", "mauve"),
    ("report",     "Report",    "peach"),
]


# ─────────────────────────────────────────────────────────────
# 메인 GUI
# ─────────────────────────────────────────────────────────────
class BatchRunnerGUI(tk.Tk):
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
        self._theme_var = tk.StringVar(value="Mocha")

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

    def _bind_shortcuts(self):
        """전역 단축키 바인딩"""
        self.bind_all("<F5>",         lambda e: self._run_btn.invoke() if self._run_btn["state"] != "disabled" else None)
        self.bind_all("<Control-F5>", lambda e: self._dryrun_btn.invoke() if self._dryrun_btn["state"] != "disabled" else None)
        self.bind_all("<Escape>",     lambda e: self._on_stop() if self._stop_btn["state"] != "disabled" else None)
        self.bind_all("<Control-s>",  lambda e: self._on_save_yml())
        self.bind_all("<Control-r>",  lambda e: self._reload_project())
        self.bind_all("<Control-l>",  lambda e: self._export_log())
        self.bind_all("<Control-f>",  lambda e: self._toggle_search())

    # ── 설정 저장/복원 ─────────────────────────────────────────
    def _load_geometry(self):
        try:
            if _CONF_PATH.exists():
                conf = json.loads(_CONF_PATH.read_text(encoding="utf-8"))
                if "geometry" in conf:
                    self.geometry(conf["geometry"])
                if "theme" in conf and conf["theme"] in THEMES:
                    if conf["theme"] != self._theme_var.get():
                        self._theme_var.set(conf["theme"])
                        self._apply_theme()
                # 마지막 설정 복원
                if "snapshot" in conf:
                    self._restore_snapshot(conf["snapshot"])
        except Exception:
            pass

    def _save_geometry(self):
        try:
            conf = {
                "geometry": self.geometry(),
                "theme": self._theme_var.get(),
                "snapshot": self._snapshot(),
            }
            _CONF_PATH.write_text(
                json.dumps(conf, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _on_close(self):
        if self._process and self._process.poll() is None:
            if not messagebox.askyesno("종료", "실행 중인 작업이 있습니다. 종료하시겠습니까?"):
                return
            self._on_stop()
        self._save_geometry()
        self.destroy()

    def _export_log(self):
        """로그 내용을 .txt 파일로 저장"""
        from tkinter import filedialog
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = (self.job_var.get().replace(".yml","") if hasattr(self, "job_var") and self.job_var.get() else "") or "elt_runner"
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
    def _setup_mousewheel(self):
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
    def _build_ui(self):
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

    def _build_title_bar(self):
        self._title_bar = tk.Frame(self, bg=C["crust"], pady=7)
        self._title_bar.pack(fill="x")
        bar = self._title_bar

        # Work Dir
        tk.Label(bar, text="Work Dir:", font=FONTS["body"],
                 bg=C["crust"], fg=C["subtext"]).pack(side="left", padx=(14, 4))
        self._wd_entry = tk.Entry(bar, textvariable=self._work_dir,
                            bg=C["surface0"], fg=C["text"],
                            insertbackground=C["text"], relief="flat",
                            font=FONTS["mono"], width=60)
        self._wd_entry.pack(side="left", ipady=2)
        self._wd_btn = tk.Button(bar, text="…", font=FONTS["mono"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  activebackground=C["surface1"],
                  command=self._browse_workdir)
        self._wd_btn.pack(side="left", padx=2)
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

        self._left_canvas = canvas
        self._left_inner = inner
        self._build_option_sections(inner)

    def _build_option_sections(self, parent):
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
    def _entry_row(self, parent_frame, label, var, **kw):
        row = tk.Frame(parent_frame, bg=C["mantle"])
        row.pack(fill="x", padx=12, pady=2)
        tk.Label(row, text=label, font=FONTS["mono_small"],
                 bg=C["mantle"], fg=C["subtext"], width=14, anchor="w").pack(side="left")
        e = tk.Entry(row, textvariable=var, bg=C["surface0"], fg=C["text"],
                     insertbackground=C["text"], relief="flat",
                     font=FONTS["mono"], **kw)
        e.pack(side="left", fill="x", expand=True, ipady=2)
        return e

    def _ov_row(self, parent_frame, label, widget_fn, note=""):
        r = tk.Frame(parent_frame, bg=C["mantle"])
        r.pack(fill="x", padx=12, pady=2)
        tk.Label(r, text=label, font=FONTS["mono_small"], width=18, anchor="w",
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left")
        widget_fn(r)
        if note:
            tk.Label(r, text=note, font=FONTS["shortcut"],
                     bg=C["mantle"], fg=C["overlay0"]).pack(side="left", padx=4)

    def _path_row(self, parent_frame, label, var, browse_title="Select folder"):
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
        tk.Button(row, text="...", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=4,
                  activebackground=C["surface1"],
                  command=_browse).pack(side="right", padx=(2, 0))
        tk.Entry(row, textvariable=var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat",
                 font=FONTS["mono"], width=16).pack(side="right", fill="x", expand=True, ipady=2)

    # ── 1) Source ─────────────────────────────────────────────
    def _build_source_section(self, parent):
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
    def _build_target_section(self, parent):
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
            text="oracle: delete (param WHERE) / append",
            font=FONTS["small"], bg=C["mantle"], fg=C["overlay0"])
        self._load_note_other = tk.Label(body,
            text="duckdb/sqlite: replace (DROP+CREATE) / truncate / append",
            font=FONTS["small"], bg=C["mantle"], fg=C["overlay0"])

        self._update_target_visibility()

    # ── 3) Paths — Export ─────────────────────────────────────
    def _build_paths_section(self, parent):
        sec = CollapsibleSection(parent, "Paths \u2014 Export", color_key="blue", expanded=True)
        sec.pack(fill="x")
        body = sec.body

        self._path_row(body, "export.sql_dir", self._export_sql_dir, "Select SQL dir")
        self._path_row(body, "export.out_dir", self._export_out_dir, "Select output dir")

    # ── 4) Stages — 토글 버튼 ────────────────────────────────
    def _build_stages_section(self, parent):
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
    def _build_params_section(self, parent):
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
    def _build_advanced_section(self, parent):
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
        self._ov_row(body, "export.workers", _w_workers, "1~16")

        def _w_compression(r):
            ttk.Combobox(r, textvariable=self._ov_compression,
                         values=["gzip", "none"], state="readonly",
                         font=FONTS["mono_small"], width=8).pack(side="left")
        self._ov_row(body, "export.compression", _w_compression)

        ttk.Separator(body, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # ─── Transform 옵션 ───
        tk.Label(body, text="Transform", font=FONTS["body_bold"],
                 bg=C["mantle"], fg=C["sky"]).pack(anchor="w", padx=12, pady=(4, 2))
        self._path_row(body, "transform.sql_dir", self._transform_sql_dir, "Select transform SQL dir")

        def _w_tfm_schema(r):
            tk.Entry(r, textvariable=self._transform_schema,
                     bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                     relief="flat", font=FONTS["mono_small"], width=16).pack(side="left", fill="x", expand=True, ipady=2)
        self._ov_row(body, "transform.schema", _w_tfm_schema, note="@{schema} 접두사용")

        def _w_on_error(r):
            ttk.Combobox(r, textvariable=self._ov_on_error,
                         values=["stop", "continue"], state="readonly",
                         font=FONTS["mono_small"], width=8).pack(side="left")
        self._ov_row(body, "transform.on_error", _w_on_error)

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
        self._ov_row(body, "report.excel", _w_excel)

        def _w_csv(r):
            tk.Checkbutton(r, variable=self._ov_csv, text="",
                           bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.csv", _w_csv)

        def _w_max_files(r):
            tk.Spinbox(r, from_=1, to=100, width=4, textvariable=self._ov_max_files,
                       bg=C["surface0"], fg=C["text"], buttonbackground=C["surface1"],
                       relief="flat", font=FONTS["mono_small"],
                       command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.max_files", _w_max_files)

        def _w_skip_sql(r):
            tk.Checkbutton(r, variable=self._ov_skip_sql, text="",
                           bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                           activebackground=C["mantle"],
                           command=self._refresh_preview).pack(side="left")
        self._ov_row(body, "report.skip_sql", _w_skip_sql, "skip DB -> CSV union only")

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
    def _build_job_preset_section(self, parent):
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
    def _build_right(self, parent):
        header = tk.Frame(parent, bg=C["mantle"])
        header.pack(fill="x")
        tk.Label(header, text="Run Log", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14, pady=10)
        tk.Checkbutton(header, text="--debug", variable=self._debug_var,
                       bg=C["mantle"], fg=C["text"], selectcolor=C["surface0"],
                       activebackground=C["mantle"], font=FONTS["mono_small"]
                       ).pack(side="left", padx=(0, 6))

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

    # ── 하단 버튼 바 ─────────────────────────────────────────
    def _build_button_bar(self):
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

        # 단축키 힌트
        for hint in [("Ctrl+F5", "Dryrun"), ("F5", "Run"), ("Esc", "Stop"),
                      ("Ctrl+S", "Save"), ("Ctrl+R", "Reload"), ("Ctrl+F", "Search")]:
            tk.Label(bar, text=f"{hint[0]} {hint[1]}", font=FONTS["shortcut"],
                     bg=C["crust"], fg=C["overlay0"]).pack(side="left", padx=6)

        self._clock_label = tk.Label(bar, text="", bg=C["crust"],
                                     fg=C["overlay0"], font=FONTS["mono_small"])
        self._clock_label.pack(side="right", padx=10)
        self._tick_clock()

    # ── 현재 설정 스냅샷 ────────────────────────────────────────
    def _snapshot(self) -> dict:
        """현재 GUI 설정 전체를 dict로 반환"""
        return {
            "job":         self.job_var.get(),
            "mode":        self.mode_var.get(),
            "env_path":    self._env_path_var.get(),
            "source_type": self._source_type_var.get(),
            "source_host": self._source_host_var.get(),
            "target_type": self._target_type_var.get(),
            "target_db_path": self._target_db_path.get(),
            "target_schema":  self._target_schema.get(),
            "export_sql_dir": self._export_sql_dir.get(),
            "export_out_dir": self._export_out_dir.get(),
            "transform_schema":  self._transform_schema.get(),
            "transform_sql_dir": self._transform_sql_dir.get(),
            "report_sql_dir":    self._report_sql_dir.get(),
            "report_out_dir":    self._report_out_dir.get(),
            "stage_export":     self._stage_export.get(),
            "stage_load_local": self._stage_load_local.get(),
            "stage_transform":  self._stage_transform.get(),
            "stage_report":     self._stage_report.get(),
            "params":      [(k.get(), v.get()) for k, v in self._param_entries],
            "overrides": {
                "overwrite":    self._ov_overwrite.get(),
                "workers":      self._ov_workers.get(),
                "compression":  self._ov_compression.get(),
                "load_mode":    self._ov_load_mode.get(),
                "on_error":     self._ov_on_error.get(),
                "excel":        self._ov_excel.get(),
                "csv":          self._ov_csv.get(),
                "max_files":    self._ov_max_files.get(),
                "skip_sql":     self._ov_skip_sql.get(),
                "union_dir":    self._ov_union_dir.get(),
                "timeout":      self._ov_timeout.get(),
            },
        }

    def _restore_snapshot(self, snap: dict):
        """스냅샷으로 GUI 설정 복원"""
        self._source_type_var.set(snap.get("source_type", "oracle"))
        if hasattr(self, "_source_type_combo"):
            self._on_source_type_change()
        self._source_host_var.set(snap.get("source_host", ""))

        self._target_type_var.set(snap.get("target_type", "duckdb"))
        self._target_db_path.set(snap.get("target_db_path", "data/local/result.duckdb"))
        self._target_schema.set(snap.get("target_schema", ""))
        if hasattr(self, "_db_path_row"):
            self._update_target_visibility()
        self._update_load_mode_options()

        self._export_sql_dir.set(snap.get("export_sql_dir", "sql/export"))
        self._export_out_dir.set(snap.get("export_out_dir", "data/export"))
        self._transform_schema.set(snap.get("transform_schema", ""))
        self._transform_sql_dir.set(snap.get("transform_sql_dir", "sql/transform/duckdb"))
        self._report_sql_dir.set(snap.get("report_sql_dir", "sql/report"))
        self._report_out_dir.set(snap.get("report_out_dir", "data/report"))

        self._stage_export.set(snap.get("stage_export", True))
        self._stage_load_local.set(snap.get("stage_load_local", True))
        self._stage_transform.set(snap.get("stage_transform", True))
        self._stage_report.set(snap.get("stage_report", True))
        if self._stage_buttons:
            self._refresh_stage_buttons()

        self._refresh_param_rows(snap.get("params", []))

        self.mode_var.set(snap.get("mode", "run"))
        self._env_path_var.set(snap.get("env_path", "config/env.yml"))

        job = snap.get("job", "")
        if job and job in self._jobs:
            self.job_var.set(job)

        ov = snap.get("overrides", {})
        self._ov_overwrite.set(ov.get("overwrite", False))
        self._ov_workers.set(ov.get("workers", 1))
        self._ov_compression.set(ov.get("compression", "gzip"))
        self._ov_load_mode.set(ov.get("load_mode", "replace"))
        self._ov_on_error.set(ov.get("on_error", "stop"))
        self._ov_excel.set(ov.get("excel", True))
        self._ov_csv.set(ov.get("csv", True))
        self._ov_max_files.set(ov.get("max_files", 10))
        self._ov_skip_sql.set(ov.get("skip_sql", False))
        self._ov_union_dir.set(ov.get("union_dir", ""))
        self._ov_timeout.set(ov.get("timeout", "1800"))

        self._refresh_preview()

    # ── Presets (jobs/*.yml 기반) ────────────────────────────────
    def _jobs_dir(self) -> Path:
        return Path(self._work_dir.get()) / "jobs"

    def _build_gui_config(self) -> dict:
        """GUI 전체 상태를 job yml dict로 조립"""
        stages = [s for s in ("export", "load_local", "transform", "report")
                  if getattr(self, f"_stage_{s}").get()]
        params = {k.get().strip(): v.get().strip()
                  for k, v in self._param_entries if k.get().strip() and v.get().strip()}

        cfg = {
            "job_name": self._jobs.get(self.job_var.get(), {}).get("job_name", "gui_run"),
            "pipeline": {"stages": stages},
            "source": {
                "type": self._source_type_var.get(),
                "host": self._source_host_var.get(),
            },
            "export": {
                "sql_dir": self._export_sql_dir.get(),
                "out_dir": self._export_out_dir.get(),
                "overwrite": self._ov_overwrite.get(),
                "parallel_workers": self._ov_workers.get(),
                "compression": self._ov_compression.get(),
                "format": "csv",
            },
            "load": {
                "mode": self._ov_load_mode.get(),
            },
            "target": {
                "type": self._target_type_var.get(),
            },
            "transform": {
                "sql_dir": self._transform_sql_dir.get(),
                "on_error": self._ov_on_error.get(),
                **({"schema": self._transform_schema.get().strip()}
                   if self._transform_schema.get().strip() else {}),
            },
            "report": {
                "source": "target",
                "export_csv": {
                    "enabled": self._ov_csv.get(),
                    "sql_dir": self._report_sql_dir.get(),
                    "out_dir": self._report_out_dir.get(),
                },
                "excel": {
                    "enabled": self._ov_excel.get(),
                    "out_dir": self._report_out_dir.get(),
                    "max_files": self._ov_max_files.get(),
                },
            },
        }
        if params:
            cfg["params"] = params
        # target specifics
        tgt_type = self._target_type_var.get()
        if tgt_type in ("duckdb", "sqlite3") and self._target_db_path.get().strip():
            cfg["target"]["db_path"] = self._target_db_path.get().strip()
        if self._target_schema.get().strip():
            cfg["target"]["schema"] = self._target_schema.get().strip()
        if self._ov_skip_sql.get():
            cfg["report"]["skip_sql"] = True
        if self._ov_union_dir.get().strip():
            cfg["report"]["csv_union_dir"] = self._ov_union_dir.get().strip()
        return cfg

    def _save_yml_dialog(self, suggest: str, title: str = "Save as yml"):
        """jobs/<name>.yml 저장 공통 다이얼로그. 저장 후 reload + 선택."""
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.configure(bg=C["base"])
        dlg.geometry("380x130")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)
        tk.Label(dlg, text="Job filename (.yml)", font=FONTS["mono"],
                 bg=C["base"], fg=C["text"]).pack(pady=(18, 6))
        name_var = tk.StringVar(value=suggest)
        entry = tk.Entry(dlg, textvariable=name_var, font=FONTS["mono"],
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
            jobs_dir = self._jobs_dir()
            jobs_dir.mkdir(parents=True, exist_ok=True)
            out_path = jobs_dir / raw
            if out_path.exists():
                if not messagebox.askyesno("Overwrite", f"{raw} already exists. Overwrite?",
                                           parent=dlg):
                    return
            new_cfg = self._build_gui_config()
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
        tk.Button(dlg, text="💾  Save", font=FONTS["body_bold"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=16, pady=4,
                  activebackground=C["teal"],
                  command=do_save).pack(pady=10)

    def _on_save_yml(self):
        """현재 선택된 job 파일을 덮어쓰기. 선택된 job 없으면 save as fallback."""
        fname = self.job_var.get()
        if not fname:
            self._on_save_yml_as()
            return
        jobs_dir = self._jobs_dir()
        out_path = jobs_dir / fname
        new_cfg = self._build_gui_config()
        jobs_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                      sort_keys=False),
            encoding="utf-8"
        )
        self._jobs[fname] = new_cfg
        self._log_sys(f"Saved: {out_path.name}")

    def _on_save_yml_as(self):
        """새 이름으로 저장 (다이얼로그)"""
        fname = self.job_var.get()
        suggest = fname.replace(".yml", "") if fname else "new_job"
        self._save_yml_dialog(suggest, "Save as yml")

    def _on_job_delete(self):
        """현재 선택된 job 파일 삭제"""
        fname = self.job_var.get()
        if not fname:
            return
        jobs_dir = self._jobs_dir()
        yml_path = jobs_dir / fname
        if not yml_path.exists():
            return
        if not messagebox.askyesno("Delete", f"Delete '{fname}'?\nThis will permanently delete the file."):
            return
        try:
            yml_path.unlink()
            self._log_sys(f"Deleted: {fname}")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return
        self.job_var.set("")
        self._reload_project()

    # ── 프로젝트 로드 ────────────────────────────────────────
    def _reload_project(self):
        wd = Path(self._work_dir.get())
        self._jobs = load_jobs(wd)
        self._env_hosts = load_env_hosts(wd, self._env_path_var.get()
                                         if hasattr(self, "_env_path_var") else "config/env.yml")

        job_names = list(self._jobs.keys())
        if hasattr(self, "_job_combo"):
            self._job_combo["values"] = job_names
            if self.job_var.get() in job_names:
                self._on_job_change()

        # source type combo 갱신
        if hasattr(self, "_source_type_combo"):
            src_types = list(self._env_hosts.keys())
            if src_types:
                self._source_type_combo["values"] = src_types
                if self._source_type_var.get() not in src_types:
                    self._source_type_var.set(src_types[0])
                self._on_source_type_change()

        self._log_sys(f"Project loaded: {wd}  (jobs={len(self._jobs)}, "
                      f"env hosts={sum(len(v) for v in self._env_hosts.values())})")

    def _browse_workdir(self):
        d = filedialog.askdirectory(initialdir=self._work_dir.get())
        if d:
            self._work_dir.set(d)
            self._reload_project()

    # ── Job 선택 시 → 모든 1급 필드 자동 채움 ────────────────
    def _on_job_change(self, *_):
        fname = self.job_var.get()
        cfg = self._jobs.get(fname, {})
        if not cfg:
            return

        # Source
        src = cfg.get("source", {})
        src_type = src.get("type", "oracle")
        self._source_type_var.set(src_type)
        self._on_source_type_change()
        src_host = src.get("host", "")
        if src_host:
            self._source_host_var.set(src_host)

        # Target
        tgt = cfg.get("target", {})
        self._target_type_var.set(tgt.get("type", "duckdb"))
        self._target_db_path.set(tgt.get("db_path", "data/local/result.duckdb"))
        self._target_schema.set(tgt.get("schema", ""))
        self._update_target_visibility()

        # Export paths
        exp = cfg.get("export", {})
        self._export_sql_dir.set(exp.get("sql_dir", "sql/export"))
        self._export_out_dir.set(exp.get("out_dir", "data/export"))

        # Transform / Report paths
        tfm = cfg.get("transform", {})
        self._transform_schema.set(tfm.get("schema", ""))
        self._transform_sql_dir.set(tfm.get("sql_dir", f"sql/transform/{tgt.get('type', 'duckdb')}"))
        rep = cfg.get("report", {})
        rep_csv = rep.get("export_csv", {})
        self._report_sql_dir.set(rep_csv.get("sql_dir", "sql/report"))
        self._report_out_dir.set(rep_csv.get("out_dir", rep.get("excel", {}).get("out_dir", "data/report")))

        # Stages
        stages = cfg.get("pipeline", {}).get("stages", [])
        self._stage_export.set("export" in stages)
        self._stage_load_local.set("load_local" in stages)
        self._stage_transform.set("transform" in stages)
        self._stage_report.set("report" in stages)
        self._refresh_stage_buttons()

        # Advanced overrides
        self._ov_overwrite.set(bool(exp.get("overwrite", False)))
        self._ov_workers.set(int(exp.get("parallel_workers", 1)))
        self._ov_compression.set(str(exp.get("compression", "gzip")))
        self._ov_load_mode.set(str(cfg.get("load", {}).get("mode", "replace")))
        self._ov_on_error.set(str(tfm.get("on_error", "stop")))
        self._ov_excel.set(bool(rep.get("excel", {}).get("enabled", True)))
        self._ov_csv.set(bool(rep_csv.get("enabled", True)))
        self._ov_max_files.set(int(rep.get("excel", {}).get("max_files", 10)))
        self._ov_skip_sql.set(bool(rep.get("skip_sql", False)))
        self._ov_union_dir.set(str(rep.get("csv_union_dir", "")))

        # Params
        params = cfg.get("params", {})
        self._refresh_param_rows(list(params.items()))
        self.after(50, self._scan_and_suggest_params)

        # SQL 선택 초기화
        self._selected_sqls = set()
        self._update_sql_preview()

        self._refresh_preview()

    # ── SQL 파라미터 자동 감지 ─────────────────────────────────
    def _scan_and_suggest_params(self):
        """
        export / transform / report SQL 디렉토리를 스테이지별로 스캔해서
        발견된 파라미터를 Params 섹션에 그룹별 자동 제시.
        이미 사용자가 입력한 값은 항상 유지.
        """
        wd = Path(self._work_dir.get())

        def _resolve(rel):
            if not rel:
                return None
            p = Path(rel) if Path(rel).is_absolute() else wd / rel
            return p if p.exists() else None

        # ── 스테이지별 SQL 파일 수집 + 파라미터 감지 ──
        stage_params: dict[str, set[str]] = {}

        # export
        export_files = []
        export_dir = _resolve(self._export_sql_dir.get().strip())
        if self._selected_sqls and export_dir:
            export_files = [export_dir / p for p in self._selected_sqls
                            if (export_dir / p).exists()]
        elif export_dir:
            export_files = list(export_dir.rglob("*.sql"))
        if export_files:
            stage_params["export"] = set(_scan_params_from_files(export_files))

        # transform
        tfm_dir = _resolve(self._transform_sql_dir.get().strip())
        if tfm_dir:
            tfm_files = list(tfm_dir.rglob("*.sql"))
            if tfm_files:
                stage_params["transform"] = set(_scan_params_from_files(tfm_files))

        # report
        rpt_dir = _resolve(self._report_sql_dir.get().strip())
        if rpt_dir:
            rpt_files = list(rpt_dir.rglob("*.sql"))
            if rpt_files:
                stage_params["report"] = set(_scan_params_from_files(rpt_files))

        all_detected = set()
        for s in stage_params.values():
            all_detected |= s

        if not all_detected:
            return

        # 현재 params (사용자 입력값)
        current = {k.get(): v.get() for k, v in self._param_entries if k.get()}

        # yml 기본값 (job 선택 시 참조)
        fname = self.job_var.get()
        yml_params = self._jobs.get(fname, {}).get("params", {}) if fname else {}

        def _val(p):
            if p in current:
                return current[p]
            if p in yml_params:
                return str(yml_params[p])
            return ""

        # 스테이지별 (key, value) 리스트 구성
        grouped: list[tuple[str, list[tuple[str, str]]]] = []
        shown = set()
        for stage in ("export", "transform", "report"):
            params = stage_params.get(stage, set())
            new_params = sorted(params - shown)
            if new_params:
                grouped.append((stage, [(p, _val(p)) for p in new_params]))
                shown |= params

        # 사용자가 직접 추가한 값 (자동감지 아닌 것) 보존
        prev_detected = getattr(self, "_last_detected_params", set())
        custom_pairs = [(k, v) for k, v in current.items()
                        if k not in shown and k not in prev_detected]
        if custom_pairs:
            grouped.append(("custom", custom_pairs))

        self._refresh_param_rows_grouped(grouped)
        if all_detected != getattr(self, "_last_detected_params", set()):
            self._last_detected_params = set(all_detected)
            self._log_sys(f"SQL params detected: {', '.join(sorted(all_detected))}")

    # ── Source Type 변경 핸들러 ──────────────────────────────
    def _on_source_type_change(self, *_):
        src_type = self._source_type_var.get()
        hosts = self._env_hosts.get(src_type, [])
        if hasattr(self, "_host_combo"):
            self._host_combo["values"] = hosts
        if hosts:
            self._source_host_var.set(hosts[0])
        else:
            self._source_host_var.set("")
        self._refresh_preview()

    # ── Target Type 변경 핸들러 ──────────────────────────────
    def _on_target_type_change(self, *_):
        tgt = self._target_type_var.get()
        self._transform_sql_dir.set(f"sql/transform/{tgt}")
        self._update_target_visibility()
        self._update_load_mode_options()
        self._refresh_preview()

    def _update_load_mode_options(self):
        """target type에 따라 load.mode 선택지 자동 전환"""
        if not hasattr(self, "_load_mode_combo"):
            return
        tgt = self._target_type_var.get()
        if tgt == "oracle":
            self._load_mode_combo["values"] = ["delete", "append"]
            if self._ov_load_mode.get() not in ("delete", "append"):
                self._ov_load_mode.set("delete")
        else:
            self._load_mode_combo["values"] = ["replace", "truncate", "append"]
            if self._ov_load_mode.get() not in ("replace", "truncate", "append"):
                self._ov_load_mode.set("replace")

    def _update_target_visibility(self):
        tgt = self._target_type_var.get()
        if not hasattr(self, "_db_path_row"):
            return
        # forget all dynamic rows to ensure correct pack order
        self._db_path_row.pack_forget()
        self._schema_row.pack_forget()
        self._oracle_hint_row.pack_forget()
        self._load_mode_row.pack_forget()
        self._load_note_oracle.pack_forget()
        self._load_note_other.pack_forget()

        if tgt in ("duckdb", "sqlite3"):
            self._db_path_row.pack(fill="x", padx=12, pady=2)
        elif tgt == "oracle":
            self._schema_row.pack(fill="x", padx=12, pady=2)
            self._oracle_hint_row.pack(fill="x", padx=12)

        # load mode always visible after target-specific rows
        self._load_mode_row.pack(fill="x", padx=12, pady=2)
        self._load_note_oracle.pack(anchor="w", padx=26, pady=(4, 0))
        self._load_note_other.pack(anchor="w", padx=26, pady=0)

    # ── Export sql_dir 변경 → auto-suggest ────────────────────
    def _on_export_sql_dir_change(self):
        sql_dir = self._export_sql_dir.get()
        if sql_dir:
            suggested = sql_dir.replace("sql/", "data/", 1)
            if suggested != sql_dir:
                self._export_out_dir.set(suggested)
        self._scan_and_suggest_params()

    # ── Stage 토글 버튼 ──────────────────────────────────────
    def _toggle_stage(self, stage_key):
        var = getattr(self, f"_stage_{stage_key}")
        var.set(not var.get())
        self._refresh_stage_buttons()
        self._refresh_preview()

    def _refresh_stage_buttons(self):
        for stage_key, (btn, color_key) in self._stage_buttons.items():
            var = getattr(self, f"_stage_{stage_key}")
            if var.get():
                btn.config(bg=C[color_key], fg=C["crust"],
                           activebackground=C[color_key], activeforeground=C["crust"])
            else:
                btn.config(bg=C["surface0"], fg=C["overlay0"],
                           activebackground=C["surface1"], activeforeground=C["overlay0"])

    def _stages_all(self):
        for s, _, _ in STAGE_CONFIG:
            getattr(self, f"_stage_{s}").set(True)
        self._refresh_stage_buttons()
        self._refresh_preview()

    def _stages_none(self):
        for s, _, _ in STAGE_CONFIG:
            getattr(self, f"_stage_{s}").set(False)
        self._refresh_stage_buttons()
        self._refresh_preview()

    # ── Param 행 관리 ────────────────────────────────────────
    _STAGE_LABELS = {
        "export": ("Export", "blue"),
        "transform": ("Transform", "mauve"),
        "report": ("Report", "teal"),
        "custom": ("Custom", "overlay0"),
    }

    def _refresh_param_rows(self, pairs: list):
        """그룹 없이 flat 리스트로 표시 (스냅샷 복원 / job 선택 시 사용)"""
        for w in self._params_frame.winfo_children():
            w.destroy()
        self._param_entries = []
        for k, v in pairs:
            self._add_param_row(k, str(v))

    def _refresh_param_rows_grouped(self, grouped: list):
        """스테이지별 그룹 헤더 + param 행 표시"""
        for w in self._params_frame.winfo_children():
            w.destroy()
        self._param_entries = []

        for stage, pairs in grouped:
            label_text, color_key = self._STAGE_LABELS.get(stage, (stage, "overlay0"))
            # 그룹 헤더
            hdr = tk.Frame(self._params_frame, bg=C["mantle"])
            hdr.pack(fill="x", pady=(4, 1))
            tk.Frame(hdr, bg=C[color_key], width=3, height=12).pack(side="left", padx=(0, 4))
            tk.Label(hdr, text=label_text, font=FONTS["mono_small"],
                     bg=C["mantle"], fg=C[color_key]).pack(side="left")
            # 파라미터 행
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
                 insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                 width=10).pack(side="left", padx=(0, 2), ipady=2)
        tk.Label(row, text="=", bg=C["mantle"], fg=C["overlay0"],
                 font=FONTS["mono"]).pack(side="left")
        tk.Entry(row, textvariable=v_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                 width=14).pack(side="left", padx=(2, 0), fill="x", expand=True, ipady=2)

        def remove(r=row, pair=(k_var, v_var)):
            r.destroy()
            if pair in self._param_entries:
                self._param_entries.remove(pair)
            self._refresh_preview()
        tk.Button(row, text="X", font=FONTS["shortcut"], bg=C["mantle"],
                  fg=C["overlay0"], relief="flat", padx=4,
                  command=remove).pack(side="right")

    # ── SQL 선택 ─────────────────────────────────────────────
    def _open_sql_selector(self):
        sql_dir_rel = self._export_sql_dir.get() or "sql/export"
        wd = Path(self._work_dir.get())
        sql_dir = wd / sql_dir_rel
        if not sql_dir.exists():
            messagebox.showwarning("SQL Filter",
                                   f"export.sql_dir 경로가 존재하지 않습니다:\n{sql_dir}",
                                   parent=self)
            return

        # 현재 선택 상태를 sql_dir 기준 상대경로로 변환하여 전달
        pre = set()
        for rel in self._selected_sqls:
            # _selected_sqls는 sql_dir 기준 상대경로로 저장됨
            pre.add(rel)

        dlg = SqlSelectorDialog(self, sql_dir, pre_selected=pre)
        self.wait_window(dlg)

        # 결과 반영 (sql_dir 기준 상대경로)
        self._selected_sqls = set(dlg.selected)
        self._update_sql_preview()
        self._scan_and_suggest_params()
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
    def _build_command_args(self) -> list[str]:
        """yml 쓰기 없이 CLI 인자만 조립 (preview용)"""
        job_name = self.job_var.get() or "_gui_temp.yml"
        cmd = ["python", "runner.py", "--job", job_name]

        # env
        env_path = self._env_path_var.get().strip()
        if env_path and env_path != "config/env.yml":
            cmd += ["--env", env_path]

        # mode
        mode = self.mode_var.get()
        if mode != "run":
            cmd += ["--mode", mode]

        # debug
        if self._debug_var.get():
            cmd.append("--debug")

        # params (yml에도 있지만 CLI 전달도 유지)
        for k_var, v_var in self._param_entries:
            k, v = k_var.get().strip(), v_var.get().strip()
            if k and v:
                cmd += ["--param", f"{k}={v}"]

        # stages (전부 선택이거나 아무것도 없으면 생략)
        selected_stages = [s for s in ("export", "load_local", "transform", "report")
                           if getattr(self, f"_stage_{s}").get()]
        all_stages = ["export", "load_local", "transform", "report"]
        if selected_stages and selected_stages != all_stages:
            for stage in selected_stages:
                cmd += ["--stage", stage]

        # SQL 파일 선택 → --include 패턴 (sql_dir 기준 상대경로)
        for rel_path in sorted(self._selected_sqls):
            pattern = Path(rel_path).with_suffix("").as_posix()
            cmd += ["--include", str(pattern)]

        # --timeout
        timeout_val = self._ov_timeout.get().strip()
        if timeout_val:
            cmd += ["--timeout", timeout_val]

        return [str(x) for x in cmd]

    def _build_command(self) -> list[str]:
        """실행용: yml 쓰기 + CLI 인자"""
        cfg = self._build_gui_config()
        wd = Path(self._work_dir.get())
        jobs_dir = wd / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        job_name = self.job_var.get() or "_gui_temp.yml"
        temp_path = jobs_dir / job_name
        temp_path.write_text(
            yaml.dump(cfg, allow_unicode=True, default_flow_style=False, sort_keys=False),
            encoding="utf-8"
        )
        return self._build_command_args()

    def _refresh_preview(self):
        try:
            cmd = self._build_command_args()
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

    # ── 로그 검색 (Ctrl+F) ──────────────────────────────────
    def _toggle_search(self):
        if self._search_frame.winfo_viewable():
            self._search_frame.pack_forget()
            self._clear_search_highlights()
        else:
            self._search_frame.pack(fill="x", padx=8, pady=(0, 4), before=self._log)
            self._search_entry.focus_set()

    def _on_search_change(self, *_):
        self._clear_search_highlights()
        query = self._search_var.get()
        if not query:
            self._search_count_label.config(text="")
            return
        self._search_matches = []
        start = "1.0"
        while True:
            pos = self._log.search(query, start, stopindex="end", nocase=True)
            if not pos:
                break
            end = f"{pos}+{len(query)}c"
            self._log.tag_add("HIGHLIGHT", pos, end)
            self._search_matches.append(pos)
            start = end
        count = len(self._search_matches)
        self._search_match_idx = 0
        self._search_count_label.config(text=f"{count} found" if count else "not found")
        if self._search_matches:
            self._log.see(self._search_matches[0])

    def _search_next(self):
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx + 1) % len(self._search_matches)
        self._log.see(self._search_matches[self._search_match_idx])

    def _search_prev(self):
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx - 1) % len(self._search_matches)
        self._log.see(self._search_matches[self._search_match_idx])

    def _clear_search_highlights(self):
        self._log.tag_remove("HIGHLIGHT", "1.0", "end")
        self._search_matches = []
        self._search_match_idx = 0

    # ── 좌측 패널 활성화/비활성화 ─────────────────────────────
    def _set_left_panel_state(self, enabled: bool):
        def _recurse(widget):
            for child in widget.winfo_children():
                try:
                    if isinstance(child, ttk.Combobox):
                        child.config(state="readonly" if enabled else "disabled")
                    elif isinstance(child, (tk.Entry, tk.Spinbox, tk.Checkbutton,
                                           tk.Radiobutton, tk.Button, tk.Listbox)):
                        child.config(state="normal" if enabled else "disabled")
                except Exception:
                    pass
                _recurse(child)
        if hasattr(self, '_left_inner'):
            _recurse(self._left_inner)

    # ── Run 버튼 애니메이션 ───────────────────────────────────
    def _animate_run_btn(self):
        if self._process is None or self._process.poll() is not None:
            return
        self._anim_dots = (self._anim_dots % 3) + 1
        mode = self.mode_var.get()
        btn = {"plan": self._dryrun_btn, "retry": self._retry_btn}.get(mode, self._run_btn)
        btn.config(text="Running" + "." * self._anim_dots)
        self._anim_id = self.after(500, self._animate_run_btn)

    # ── 타이틀 깜빡임 ────────────────────────────────────────
    def _flash_title(self, count=6):
        if count <= 0:
            self.title(f"ELT Runner  v{APP_VERSION}")
            return
        if count % 2 == 0:
            self.title(f">> Done -- ELT Runner  v{APP_VERSION}")
        else:
            self.title(f"ELT Runner  v{APP_VERSION}")
        self.after(500, self._flash_title, count - 1)

    # ── 커스텀 확인 다이얼로그 공통 ──────────────────────────
    def _themed_confirm(self, title, body_builder, ok_text="OK",
                        ok_color="green", ok_active="teal") -> bool:
        """테마 통일된 확인 다이얼로그. body_builder(frame)로 본문 구성."""
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.resizable(False, False)
        dlg.configure(bg=C["base"])
        dlg.grab_set()
        dlg.transient(self)

        result = [False]

        body = tk.Frame(dlg, bg=C["base"])
        body.pack(padx=4, pady=(12, 6))
        body_builder(body)

        tk.Frame(dlg, bg=C["surface1"], height=1).pack(fill="x", padx=12, pady=(4, 8))

        btn_frame = tk.Frame(dlg, bg=C["base"])
        btn_frame.pack(pady=(0, 12))

        def on_ok():
            result[0] = True
            dlg.destroy()

        def on_cancel():
            dlg.destroy()

        ok_btn = tk.Button(btn_frame, text=ok_text, width=10, command=on_ok,
                           bg=C[ok_color], fg=C["crust"], font=FONTS["body_bold"],
                           activebackground=C[ok_active], cursor="hand2")
        ok_btn.pack(side="left", padx=8)
        tk.Button(btn_frame, text="Cancel", width=10, command=on_cancel,
                  bg=C["surface0"], fg=C["text"], font=FONTS["body"],
                  activebackground=C["surface1"], cursor="hand2").pack(side="left", padx=8)

        dlg.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() - dlg.winfo_width()) // 2
        y = self.winfo_y() + (self.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")

        dlg.protocol("WM_DELETE_WINDOW", on_cancel)
        dlg.bind("<Return>", lambda e: on_ok())
        dlg.bind("<Escape>", lambda e: on_cancel())
        ok_btn.focus_set()

        dlg.wait_window()
        return result[0]

    # ── overwrite 확인 다이얼로그 ─────────────────────────────
    def _show_overwrite_confirm(self) -> bool:
        kw_key = {"bg": C["base"], "fg": C["overlay0"], "font": FONTS["body"]}

        def build(body):
            # 경고 아이콘 + 메시지
            tk.Label(body, text="\u26a0  export.overwrite = ON", bg=C["base"],
                     fg=C["red"], font=FONTS["h2"]).pack(pady=(0, 8))
            tk.Label(body, text="\uae30\uc874 \ub370\uc774\ud130\ub97c \ub36e\uc5b4\uc4f8 \uc218 \uc788\uc2b5\ub2c8\ub2e4.\n\uacc4\uc18d\ud558\uc2dc\uaca0\uc2b5\ub2c8\uae4c?",
                     **kw_key, justify="center").pack()

        return self._themed_confirm("\u2501 Overwrite \ud655\uc778", build,
                                    ok_text="Continue", ok_color="red", ok_active="peach")

    # ── 실행 확인 다이얼로그 ─────────────────────────────────
    def _show_run_confirm(self) -> bool:
        mode = self.mode_var.get()
        mode_label = {"run": "Run", "retry": "Retry"}.get(mode, mode)
        selected_stages = [s for s in ("export", "load_local", "transform", "report")
                           if getattr(self, f"_stage_{s}").get()]
        stages_str = " \u2192 ".join(selected_stages) if selected_stages else "(all)"
        params = {k.get().strip(): v.get().strip()
                  for k, v in self._param_entries
                  if k.get().strip() and v.get().strip()}
        params_str = ", ".join(f"{k}={v}" for k, v in params.items())
        timeout_val = self._ov_timeout.get().strip() or "1800"
        ov_on = self._ov_overwrite.get()

        kw_key = {"bg": C["base"], "fg": C["overlay0"], "font": FONTS["body"]}
        kw_val = {"bg": C["base"], "fg": C["text"], "font": FONTS["body_bold"]}
        pad_k = {"padx": (16, 4), "pady": 3}
        pad_v = {"padx": (0, 16), "pady": 3}

        def build(body):
            row = 0
            tk.Label(body, text="Mode", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            mode_fg = C["blue"] if mode == "run" else C["peach"]
            tk.Label(body, text=mode_label, bg=C["base"], fg=mode_fg,
                     font=FONTS["body_bold"]).grid(row=row, column=1, sticky="w", **pad_v)
            row += 1

            tk.Label(body, text="Source", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=self._source_type_var.get(), **kw_val).grid(
                row=row, column=1, sticky="w", **pad_v)
            tk.Label(body, text="Overwrite", **kw_key).grid(row=row, column=2, sticky="e", **pad_k)
            tk.Label(body, text="ON" if ov_on else "OFF", bg=C["base"],
                     fg=C["red"] if ov_on else C["overlay0"],
                     font=FONTS["body_bold"]).grid(row=row, column=3, sticky="w", **pad_v)
            row += 1

            tk.Label(body, text="Target", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=self._target_type_var.get(), **kw_val).grid(
                row=row, column=1, sticky="w", **pad_v)
            tk.Label(body, text="Timeout", **kw_key).grid(row=row, column=2, sticky="e", **pad_k)
            tk.Label(body, text=timeout_val, **kw_val).grid(row=row, column=3, sticky="w", **pad_v)
            row += 1

            tk.Label(body, text="Stages", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=stages_str, **kw_val).grid(
                row=row, column=1, columnspan=3, sticky="w", **pad_v)
            row += 1

            if params_str:
                tk.Label(body, text="Params", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
                tk.Label(body, text=params_str, **kw_val).grid(
                    row=row, column=1, columnspan=3, sticky="w", **pad_v)

        return self._themed_confirm("\u2501 \uc2e4\ud589 \ud655\uc778", build)

    # ── 실행 / 멈춤 ──────────────────────────────────────────
    def _on_run(self):
        mode = self.mode_var.get()

        # overwrite=true 확인 (Dryrun은 실제 덮어쓰기 없으므로 스킵)
        if self._ov_overwrite.get() and mode != "plan":
            if not self._show_overwrite_confirm():
                return

        # 실행 전 확인 다이얼로그 (Dryrun은 확인 없이 바로 실행)
        if mode != "plan":
            if not self._show_run_confirm():
                return

        cmd = self._build_command()
        self._log_sys(f"Run: {chr(32).join(cmd)}")
        self._set_status("● running", C["green"])
        import time
        self._elapsed_start = time.time()
        self._progress_bar["value"] = 0
        self._progress_label.config(text="Starting...")
        self._elapsed_job_id = self.after(1000, self._tick_elapsed)

        self._dryrun_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._run_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._retry_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="disabled")
        self._stop_btn.config(state="normal", bg=C["red"], fg=C["crust"],
                              activebackground=C["peach"])
        self._set_left_panel_state(False)
        self._anim_dots = 0
        self._anim_id = self.after(500, self._animate_run_btn)

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
        import re
        low = line.lower()
        # STAGE 시작/끝 패턴 (최우선)
        if any(k in low for k in ("=== stage", "--- stage", "stage start", "[stage")):
            return "STAGE_HEADER"
        if any(k in low for k in ("stage done", "stage complete", "stage finish")):
            return "STAGE_DONE"
        # 구분선 / 배너
        if any(k in low for k in ("===", "---", "pipeline", "job start", "job finish")):
            return "SYS"
        # summary 줄: failed=N 값으로 판단 (failed=0이면 SUCCESS)
        if "summary" in low:
            m = re.search(r"failed=(\d+)", low)
            if m and int(m.group(1)) > 0:
                return "ERROR"
            return "SUCCESS"
        # 에러 (명확한 에러 패턴)
        if any(k in low for k in ("exception", "traceback")):
            return "ERROR"
        if re.search(r"\bfailed\b", low) and "failed=" not in low:
            return "ERROR"
        if re.search(r"\berror\b", low):
            return "ERROR"
        # 경고
        if any(k in low for k in ("warn", "warning")):
            return "WARN"
        # 성공
        if any(k in low for k in ("done", "success", "completed")):
            return "SUCCESS"
        return "INFO"

    def _update_progress(self, pct: int, label: str):
        self._progress_bar["value"] = pct
        elapsed = ""
        if self._elapsed_start:
            import time
            secs = int(time.time() - self._elapsed_start)
            elapsed = f"  {secs//60:02d}:{secs%60:02d}"
        self._progress_label.config(text=f"{label}{elapsed}")
        if hasattr(self, '_stage_status'):
            self._stage_status.config(text=label)

    def _tick_clock(self):
        import datetime, locale
        days_kr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        now = datetime.datetime.now()
        day = days_kr[now.weekday()]
        self._clock_label.config(text=now.strftime(f"%Y-%m-%d ({day}) %H:%M"))
        self.after(30000, self._tick_clock)

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
        # 애니메이션 취소
        if self._anim_id:
            self.after_cancel(self._anim_id)
            self._anim_id = None
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
            self._log_write(f"Done  ({elapsed_str})", "SUCCESS")
            self._set_status("● done", C["green"])
            self._progress_bar["value"] = 100
            self._progress_label.config(text=f"Done  {elapsed_str}")
        elif ret < 0:
            self._log_write(f"Stopped  ({elapsed_str})", "WARN")
            self._set_status("● stopped", C["yellow"])
            self._progress_label.config(text=f"Stopped  {elapsed_str}")
        else:
            self._log_write(f"Error (code={ret})  ({elapsed_str})", "ERROR")
            self._set_status(f"● error (code={ret})", C["red"])
            self._progress_label.config(text=f"Error  {elapsed_str}")
        # 완료 알림
        self.bell()
        self._flash_title()
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
        self._dryrun_btn.config(state="normal", bg=C["yellow"], fg=C["crust"],
                                text="Dryrun")
        self._run_btn.config(state="normal", bg=C["blue"], fg=C["crust"],
                             text="▶  Run")
        self._retry_btn.config(state="normal", bg=C["peach"], fg=C["crust"],
                               text="Retry")
        self._stop_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="readonly")
        self._set_left_panel_state(True)
        if hasattr(self, '_stage_status'):
            self._stage_status.config(text="")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = BatchRunnerGUI()
    app.mainloop()
