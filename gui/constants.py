"""
gui/constants.py  ─  전역 상수, 테마, 폰트, 설정 경로
"""

import sys
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 버전 (VERSION 파일에서 읽기)
# ─────────────────────────────────────────────────────────────
def _read_version() -> str:
    vf = Path(__file__).resolve().parent.parent / "VERSION"
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
        "teal":    "#45e0b0", "sky":     "#69c3ff",
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
C = dict(THEMES["Mocha"])

# ─────────────────────────────────────────────────────────────
# 폰트 시스템
# ─────────────────────────────────────────────────────────────
FONT_FAMILY = "Malgun Gothic"
FONT_MONO   = "Consolas"

def _load_bundled_fonts():
    """fonts/ 폴더의 ttf/otf를 프로세스 전용으로 등록 (시스템 설치 불필요)"""
    if sys.platform != "win32":
        return
    fonts_dir = Path(__file__).parent.parent / "fonts"
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
    "h1":         (FONT_FAMILY, 14, "bold"),
    "h2":         (FONT_FAMILY, 11, "bold"),
    "body":       (FONT_FAMILY, 10),
    "body_bold":  (FONT_FAMILY, 10, "bold"),
    "small":      (FONT_FAMILY, 9),
    "mono":       (FONT_MONO,   10),
    "mono_small": (FONT_MONO,   9),
    "log":        (FONT_MONO,   10),
    "cmd":        (FONT_MONO,   10),
    "button":     (FONT_FAMILY, 11, "bold"),
    "button_sm":  (FONT_FAMILY, 10),
    "shortcut":   (FONT_MONO,   8),
}

# ─────────────────────────────────────────────────────────────
# 설정 파일 경로 (geometry, theme 저장)
# ─────────────────────────────────────────────────────────────
_CONF_PATH = Path.home() / ".elt_runner_gui.conf"

# 필드별 툴팁 텍스트
TOOLTIPS = {
    # Export
    "overwrite":    "ON: overwrite existing output files\nOFF: skip if already exists",
    "timeout":      "Max export wait time (seconds)",
    "workers":      "Concurrent export processes (1~16)\nHigher = faster but more DB load",
    "compression":  "CSV compression\ngzip: smaller files / none: no compression",
    # Load
    "load_mode":    "replace: DROP + CREATE\ntruncate: TRUNCATE + INSERT\n"
                    "delete: DELETE with param WHERE + INSERT\nappend: INSERT only (skip if exists)",
    # Transform
    "schema":       "Session schema for SQL execution\nDuckDB: SET schema / Oracle: ALTER SESSION",
    "on_error":     "Behavior on error\nstop: abort immediately / continue: run remaining",
    # Report
    "excel":        "Generate Excel(.xlsx) report",
    "csv":          "Generate CSV report",
    "max_files":    "Max sheets per Excel file (1~100)\nCreates new file when exceeded",
    "skip_sql":     "Skip SQL execution\nConvert union_dir CSVs directly to Excel",
    "union_dir":    "CSV source folder\nUsed when skip_sql=ON to merge CSVs into Excel",
    "name_style":   "CSV filename param format\nfull: key_value (clsYymm_202003)\ncompact: value only (202003)",
    "strip_prefix": "Remove numeric prefix from SQL filename\nON: 01_contract → contract\nOFF: 01_contract (keep as-is)",
}

# ─────────────────────────────────────────────────────────────
# Stage 토글 버튼 설정
# ─────────────────────────────────────────────────────────────
STAGE_CONFIG = [
    ("export",     "Export",    "blue"),
    ("load_local", "Load",      "teal"),
    ("transform",  "Transform", "mauve"),
    ("report",     "Report",    "peach"),
]
