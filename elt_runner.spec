# -*- mode: python ; coding: utf-8 -*-
# ELT Runner — PyInstaller spec (version read from VERSION file)
# 빌드: pyinstaller elt_runner.spec  (또는 build.bat / build.ps1)

from pathlib import Path as _P
APP_VERSION = (_P(__file__).resolve().parent / "VERSION").read_text().strip()

block_cipher = None

datas = [
    ("jobs",    "jobs"),
    ("sql",     "sql"),
    ("config",  "config"),
]

hiddenimports = [
    "oracledb", "vertica_python", "duckdb",
    "yaml", "openpyxl", "openpyxl.styles", "openpyxl.utils", "openpyxl.writer.excel",
    "tkinter", "tkinter.ttk", "tkinter.filedialog", "tkinter.messagebox", "tkinter.scrolledtext",
    "engine", "engine.sql_utils", "engine.context", "engine.path_utils",
    "engine.runtime_state", "engine.stage_registry",
    "stages", "stages.export_stage", "stages.load_stage",
    "stages.transform_stage", "stages.report_stage",
    "adapters",
    "adapters.sources.oracle_source", "adapters.sources.vertica_source",
    "adapters.targets.oracle_target", "adapters.targets.sqlite_target",
    "adapters.targets.duckdb_target",
]

a = Analysis(
    ["batch_runner_gui.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=["rthook_workdir.py"],
    excludes=["matplotlib", "numpy", "scipy", "PIL", "IPython", "pytest", "setuptools", "pip"],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz, a.scripts, a.binaries, a.zipfiles, a.datas, [],
    name="ELT_Runner",
    debug=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,          # GUI 전용 — 콘솔창 없음
    target_arch=None,
    # icon="assets/icon.ico",   # 아이콘 있으면 주석 해제
)
