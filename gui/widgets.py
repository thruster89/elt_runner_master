"""
gui/widgets.py  ─  독립 위젯 클래스 (SqlSelectorDialog, CollapsibleSection, Tooltip, RunHistoryDialog)
"""

import json
import tkinter as tk
from tkinter import ttk

from gui.constants import C, FONTS, FONT_MONO
from gui.utils import collect_sql_tree
from pathlib import Path


class SqlSelectorDialog(tk.Toplevel):
    """SQL 폴더 트리 + 파일 체크박스 선택 다이얼로그"""

    def __init__(self, parent, sql_dir: Path, pre_selected: set = None):
        super().__init__(parent)
        self.title("SQL File Select")
        self.configure(bg=C["base"])
        self.resizable(True, True)
        self.geometry("500x560")
        self.transient(parent)
        self.grab_set()

        self.sql_dir = sql_dir
        self.selected: set[str] = set(pre_selected or [])  # relative paths from sql_dir
        self._check_vars: dict[str, tk.BooleanVar] = {}
        self._all_rows: list[tuple[tk.Frame, str]] = []  # (row_widget, rel_path) for filtering

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
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left", padx=6)

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

        # 검색 필터
        search_frame = tk.Frame(self, bg=C["base"])
        search_frame.pack(fill="x", padx=10, pady=(2, 0))
        tk.Label(search_frame, text="🔍", font=FONTS["mono_small"],
                 bg=C["base"], fg=C["subtext"]).pack(side="left")
        self._search_var = tk.StringVar()
        self._search_entry = tk.Entry(
            search_frame, textvariable=self._search_var,
            bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
            relief="flat", font=FONTS["mono"], width=30)
        self._search_entry.pack(side="left", fill="x", expand=True, padx=4, ipady=2)
        self._search_var.trace_add("write", lambda *_: self._apply_search_filter())
        self._search_match_label = tk.Label(
            search_frame, text="", font=FONTS["mono_small"],
            bg=C["base"], fg=C["subtext"])
        self._search_match_label.pack(side="left", padx=4)

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
            self._all_rows.append((row, rel))

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

            def make_toggle(cf, tv, btn_ref, k=key):
                def toggle():
                    if tv.get():
                        cf.pack_forget()
                        btn_ref.config(text=f"  ▶  {k}")
                    else:
                        cf.pack(fill="x")
                        btn_ref.config(text=f"  ▼  {k}")
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
        if getattr(self, "_batch_update", False):
            return  # 일괄 업데이트 중 trace 콜백 스킵
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
        self._batch_update = True
        for v in self._check_vars.values():
            v.set(True)
        self._batch_update = False
        self.selected = set(self._check_vars.keys())
        self._update_count()

    def _deselect_all(self):
        self._batch_update = True
        for v in self._check_vars.values():
            v.set(False)
        self._batch_update = False
        self.selected = set()
        self._update_count()

    def _select_folder(self, folder_prefix: str, node: dict, value: bool):
        self._batch_update = True
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
        self._batch_update = False
        # 일괄 업데이트 후 selected set 동기화
        self.selected = {rel for rel, v in self._check_vars.items() if v.get()}
        self._update_count()

    def _apply_search_filter(self):
        """검색어에 따라 파일 행 표시/숨김"""
        query = self._search_var.get().strip().lower()
        visible = 0
        for row, rel in self._all_rows:
            if not query or query in rel.lower():
                row.pack(fill="x")
                visible += 1
            else:
                row.pack_forget()
        if query:
            self._search_match_label.config(text=f"{visible} match")
        else:
            self._search_match_label.config(text="")

    def _confirm(self):
        self.selected = {rel for rel, v in self._check_vars.items() if v.get()}
        self.destroy()


class CsvSelectorDialog(tk.Toplevel):
    """CSV 파일 체크박스 선택 다이얼로그 (Report CSV→Excel 필터용)"""

    def __init__(self, parent, csv_dir: Path, pre_selected: set = None):
        super().__init__(parent)
        self.title("CSV File Select")
        self.configure(bg=C["base"])
        self.resizable(True, True)
        self.geometry("520x480")
        self.transient(parent)
        self.grab_set()

        self.csv_dir = csv_dir
        self.selected: set[str] = set(pre_selected or [])
        self._check_vars: dict[str, tk.BooleanVar] = {}
        self._all_rows: list[tuple[tk.Frame, str]] = []

        self._build()
        self._center(parent)

    def _center(self, parent):
        self.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{px + pw // 2 - w // 2}+{py + ph // 2 - h // 2}")

    def _build(self):
        hdr = tk.Frame(self, bg=C["mantle"], pady=8)
        hdr.pack(fill="x")
        tk.Label(hdr, text="CSV File Select", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14)
        tk.Label(hdr, text=str(self.csv_dir), font=FONTS["shortcut"],
                 bg=C["mantle"], fg=C["subtext"]).pack(side="left", padx=6)

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

        search_frame = tk.Frame(self, bg=C["base"])
        search_frame.pack(fill="x", padx=10, pady=(2, 0))
        self._search_var = tk.StringVar()
        tk.Entry(search_frame, textvariable=self._search_var,
                 bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                 relief="flat", font=FONTS["mono"], width=30
                 ).pack(side="left", fill="x", expand=True, padx=4, ipady=2)
        self._search_var.trace_add("write", lambda *_: self._apply_filter())

        outer = tk.Frame(self, bg=C["base"])
        outer.pack(fill="both", expand=True, padx=10, pady=6)
        canvas = tk.Canvas(outer, bg=C["crust"], highlightthickness=0)
        vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        self._scroll_frame = tk.Frame(canvas, bg=C["crust"])
        canvas_win = canvas.create_window((0, 0), window=self._scroll_frame, anchor="nw")

        def _on_resize(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfig(canvas_win, width=canvas.winfo_width())
        self._scroll_frame.bind("<Configure>", _on_resize)
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(canvas_win, width=e.width))

        # CSV 파일 수집 (*.csv, *.csv.gz)
        csv_files = sorted(
            f for f in self.csv_dir.iterdir()
            if f.is_file() and (f.suffix == ".csv" or f.name.endswith(".csv.gz"))
        ) if self.csv_dir.exists() else []

        for f in csv_files:
            name = f.name
            var = tk.BooleanVar(value=(name in self.selected))
            self._check_vars[name] = var
            var.trace_add("write", lambda *_, n=name: self._on_check(n))
            row = tk.Frame(self._scroll_frame, bg=C["crust"])
            row.pack(fill="x")
            tk.Checkbutton(
                row, text=f"  {name}", variable=var,
                bg=C["crust"], fg=C["text"], selectcolor=C["surface0"],
                activebackground=C["crust"], activeforeground=C["text"],
                font=FONTS["mono_small"], anchor="w"
            ).pack(fill="x", side="left")
            self._all_rows.append((row, name))

        if not csv_files:
            tk.Label(self._scroll_frame, text="(no CSV files found)",
                     font=FONTS["mono_small"], bg=C["crust"], fg=C["subtext"]
                     ).pack(pady=20)

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
        tk.Button(btn_bar, text="OK", font=FONTS["body_bold"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=14, pady=4,
                  activebackground=C["teal"],
                  command=self._confirm).pack(side="right", padx=(0, 4))

    def _on_check(self, name):
        if getattr(self, "_batch_update", False):
            return
        var = self._check_vars.get(name)
        if var:
            if var.get():
                self.selected.add(name)
            else:
                self.selected.discard(name)
        self._update_count()

    def _update_count(self):
        count = sum(1 for v in self._check_vars.values() if v.get())
        total = len(self._check_vars)
        self._count_label.config(text=f"{count} / {total} selected")

    def _select_all(self):
        self._batch_update = True
        for v in self._check_vars.values():
            v.set(True)
        self._batch_update = False
        self.selected = set(self._check_vars.keys())
        self._update_count()

    def _deselect_all(self):
        self._batch_update = True
        for v in self._check_vars.values():
            v.set(False)
        self._batch_update = False
        self.selected = set()
        self._update_count()

    def _apply_filter(self):
        query = self._search_var.get().strip().lower()
        for row, name in self._all_rows:
            if not query or query in name.lower():
                row.pack(fill="x")
            else:
                row.pack_forget()

    def _confirm(self):
        self.selected = {n for n, v in self._check_vars.items() if v.get()}
        self.destroy()


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

        # 클릭 + 키보드 바인딩
        for w in (self._header, self._toggle_label, self._title_label, self._color_bar):
            w.bind("<Button-1>", lambda e: self.toggle())
        # Tab 포커스 지원: 헤더에 포커스 가능 + Enter/Space로 토글
        self._header.configure(takefocus=1)
        self._header.bind("<Return>", lambda e: self.toggle())
        self._header.bind("<space>", lambda e: self.toggle())
        self._header.bind("<FocusIn>", lambda e: self._header.configure(
            highlightbackground=C[color_key], highlightthickness=2))
        self._header.bind("<FocusOut>", lambda e: self._header.configure(
            highlightthickness=0))

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


class Tooltip:
    """위젯에 마우스를 올리면 표시되는 간단한 툴팁"""

    def __init__(self, widget, text, delay=500):
        self._widget = widget
        self._text = text
        self._delay = delay
        self._tip_win = None
        self._after_id = None
        widget.bind("<Enter>", self._on_enter, add="+")
        widget.bind("<Leave>", self._on_leave, add="+")
        widget.bind("<ButtonPress>", self._on_leave, add="+")

    def _on_enter(self, event=None):
        self._after_id = self._widget.after(self._delay, self._show)

    def _on_leave(self, event=None):
        if self._after_id:
            self._widget.after_cancel(self._after_id)
            self._after_id = None
        self._hide()

    def _show(self):
        if self._tip_win or not self._text:
            return
        x = self._widget.winfo_rootx() + 20
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
        self._tip_win = tw = tk.Toplevel(self._widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        lbl = tk.Label(tw, text=self._text, justify="left",
                       bg=C["surface1"], fg=C["text"], relief="solid", bd=1,
                       font=FONTS["mono"], padx=8, pady=4, wraplength=360)
        lbl.pack()

    def _hide(self):
        if self._tip_win:
            self._tip_win.destroy()
            self._tip_win = None


class RunHistoryDialog(tk.Toplevel):
    """과거 실행 이력(run_info.json) 조회 다이얼로그"""

    def __init__(self, parent, work_dir: Path, job_name: str):
        super().__init__(parent)
        self.title(f"Run History — {job_name}")
        self.configure(bg=C["base"])
        self.resizable(True, True)
        self.geometry("720x480")
        self.transient(parent)
        self.grab_set()

        self._work_dir = work_dir
        self._job_name = job_name
        self._runs: list[dict] = []

        self._build()
        self._load_history()
        self._center(parent)

    def _center(self, parent):
        self.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{px + pw//2 - w//2}+{py + ph//2 - h//2}")

    def _build(self):
        hdr = tk.Frame(self, bg=C["mantle"], pady=8)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Run History", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14)

        # Treeview
        cols = ("run_id", "stage", "start", "mode", "success", "failed", "skipped", "elapsed")
        style = ttk.Style(self)
        style.configure("History.Treeview",
                         background=C["crust"], foreground=C["text"],
                         fieldbackground=C["crust"], font=FONTS["mono_small"],
                         rowheight=24)
        style.configure("History.Treeview.Heading",
                         background=C["surface0"], foreground=C["text"],
                         font=FONTS["body_bold"])
        style.map("History.Treeview",
                   background=[("selected", C["surface1"])],
                   foreground=[("selected", C["text"])])

        tree_frame = tk.Frame(self, bg=C["crust"])
        tree_frame.pack(fill="both", expand=True, padx=10, pady=6)

        self._tree = ttk.Treeview(tree_frame, columns=cols, show="headings",
                                   style="History.Treeview")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._tree.pack(fill="both", expand=True)

        widths = {"run_id": 130, "stage": 80, "start": 130, "mode": 50,
                  "success": 60, "failed": 60, "skipped": 60, "elapsed": 70}
        labels = {"run_id": "Run ID", "stage": "Stage", "start": "Start Time",
                  "mode": "Mode", "success": "OK", "failed": "Fail",
                  "skipped": "Skip", "elapsed": "Elapsed"}
        for col in cols:
            self._tree.heading(col, text=labels[col])
            self._tree.column(col, width=widths.get(col, 80), anchor="center")
        self._tree.column("run_id", anchor="w")
        self._tree.column("start", anchor="w")

        # 하단 상세
        self._detail_frame = tk.Frame(self, bg=C["mantle"])
        self._detail_frame.pack(fill="x", padx=10, pady=(0, 6))
        self._detail_label = tk.Label(self._detail_frame, text="", font=FONTS["mono_small"],
                                      bg=C["mantle"], fg=C["subtext"], anchor="w",
                                      wraplength=680, justify="left")
        self._detail_label.pack(fill="x", padx=8, pady=4)

        self._tree.bind("<<TreeviewSelect>>", self._on_select)

        # 닫기 버튼
        btn_bar = tk.Frame(self, bg=C["mantle"], pady=6)
        btn_bar.pack(fill="x")
        tk.Button(btn_bar, text="Close", font=FONTS["body"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=14, pady=3,
                  command=self.destroy).pack(side="right", padx=10)

    _MAX_RUNS_PER_STAGE = 50  # 스테이지당 최대 로딩 수

    def _load_history(self):
        """data/ 하위에서 run_info.json을 수집 (스테이지당 최대 50건)"""
        stage_dirs = {
            "export": self._work_dir / "data" / "export",
            "transform": self._work_dir / "data" / "transform",
            "report": self._work_dir / "data" / "report_tracking",
        }
        for stage, base in stage_dirs.items():
            job_dir = base / self._job_name
            if not job_dir.is_dir():
                continue
            loaded = 0
            for d in sorted(job_dir.iterdir(), reverse=True):
                if loaded >= self._MAX_RUNS_PER_STAGE:
                    break
                ri = d / "run_info.json"
                if not ri.exists():
                    continue
                try:
                    info = json.loads(ri.read_text(encoding="utf-8"))
                    tasks = info.get("tasks", {})
                    s = sum(1 for v in tasks.values() if v.get("status") == "success")
                    f = sum(1 for v in tasks.values() if v.get("status") == "failed")
                    sk = sum(1 for v in tasks.values() if v.get("status") == "skipped")
                    # 최대 elapsed 계산
                    elapsed_vals = [v.get("elapsed", 0) for v in tasks.values()
                                    if isinstance(v.get("elapsed"), (int, float))]
                    total_elapsed = sum(elapsed_vals)
                    if total_elapsed < 60:
                        el_str = f"{total_elapsed:.0f}s"
                    else:
                        m, sec = divmod(int(total_elapsed), 60)
                        el_str = f"{m}m{sec:02d}s"

                    run = {
                        "run_id": info.get("run_id", d.name),
                        "stage": info.get("stage", stage),
                        "start": info.get("start_time", ""),
                        "mode": info.get("mode", ""),
                        "success": s, "failed": f, "skipped": sk,
                        "elapsed": el_str,
                        "tasks": tasks,
                        "path": str(ri),
                    }
                    self._runs.append(run)
                    loaded += 1
                except Exception:
                    continue

        # 시작 시간 역순 정렬
        self._runs.sort(key=lambda r: r.get("start", ""), reverse=True)

        for run in self._runs:
            tags = ()
            if run["failed"] > 0:
                tags = ("fail",)
            self._tree.insert("", "end", values=(
                run["run_id"], run["stage"], run["start"], run["mode"],
                run["success"], run["failed"], run["skipped"], run["elapsed"]
            ), tags=tags)

        self._tree.tag_configure("fail", foreground=C["red"])

        if not self._runs:
            self._detail_label.config(text="실행 이력이 없습니다.")

    def _on_select(self, _event=None):
        sel = self._tree.selection()
        if not sel:
            return
        idx = self._tree.index(sel[0])
        if idx >= len(self._runs):
            return
        run = self._runs[idx]
        tasks = run.get("tasks", {})
        # 실패 task 상세
        failed_tasks = [(k, v.get("error", "")) for k, v in tasks.items()
                        if v.get("status") == "failed"]
        if failed_tasks:
            lines = [f"Failed tasks ({len(failed_tasks)}):"]
            for k, err in failed_tasks[:10]:
                lines.append(f"  {k}: {err[:120]}" if err else f"  {k}")
            if len(failed_tasks) > 10:
                lines.append(f"  ... +{len(failed_tasks) - 10} more")
            self._detail_label.config(text="\n".join(lines), fg=C["red"])
        else:
            self._detail_label.config(
                text=f"All {run['success']} tasks succeeded.  ({run['elapsed']})",
                fg=C["green"])


class JobQueueDialog(tk.Toplevel):
    """다중 Job 큐 선택 다이얼로그 — 순차 실행할 Job 목록 구성"""

    def __init__(self, parent, job_list: list[str]):
        super().__init__(parent)
        self.title("Job Queue")
        self.configure(bg=C["base"])
        self.resizable(True, True)
        self.geometry("440x420")
        self.transient(parent)
        self.grab_set()

        self._all_jobs = sorted(job_list)
        self.queue: list[str] = []  # 확정된 큐

        self._build()
        self._center(parent)

    def _center(self, parent):
        self.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{px + pw//2 - w//2}+{py + ph//2 - h//2}")

    def _build(self):
        hdr = tk.Frame(self, bg=C["mantle"], pady=8)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Job Queue — 순차 실행", font=FONTS["h2"],
                 bg=C["mantle"], fg=C["text"]).pack(side="left", padx=14)

        # 좌: 전체 Job 목록 / 우: 큐 목록
        mid = tk.Frame(self, bg=C["base"])
        mid.pack(fill="both", expand=True, padx=10, pady=6)

        # 좌측 — Available Jobs
        left = tk.Frame(mid, bg=C["base"])
        left.pack(side="left", fill="both", expand=True)
        tk.Label(left, text="Available", font=FONTS["body_bold"],
                 bg=C["base"], fg=C["subtext"]).pack(anchor="w")
        self._avail_lb = tk.Listbox(left, bg=C["crust"], fg=C["text"],
                                     selectbackground=C["surface1"],
                                     selectforeground=C["text"],
                                     font=FONTS["mono_small"], relief="flat")
        self._avail_lb.pack(fill="both", expand=True, pady=(4, 0))
        for j in self._all_jobs:
            self._avail_lb.insert("end", j)

        # 중간 버튼
        btns = tk.Frame(mid, bg=C["base"])
        btns.pack(side="left", padx=8)
        tk.Button(btns, text=">>", font=FONTS["body_bold"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  command=self._add_job).pack(pady=4)
        tk.Button(btns, text="<<", font=FONTS["body_bold"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  command=self._remove_job).pack(pady=4)
        tk.Button(btns, text="Up", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  command=self._move_up).pack(pady=4)
        tk.Button(btns, text="Dn", font=FONTS["mono_small"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=6,
                  command=self._move_down).pack(pady=4)

        # 우측 — Queue
        right = tk.Frame(mid, bg=C["base"])
        right.pack(side="left", fill="both", expand=True)
        tk.Label(right, text="Queue (순서대로 실행)", font=FONTS["body_bold"],
                 bg=C["base"], fg=C["subtext"]).pack(anchor="w")
        self._queue_lb = tk.Listbox(right, bg=C["crust"], fg=C["text"],
                                     selectbackground=C["surface1"],
                                     selectforeground=C["text"],
                                     font=FONTS["mono_small"], relief="flat")
        self._queue_lb.pack(fill="both", expand=True, pady=(4, 0))

        # 하단
        btn_bar = tk.Frame(self, bg=C["mantle"], pady=8)
        btn_bar.pack(fill="x")
        self._count_label = tk.Label(btn_bar, text="0 jobs", font=FONTS["mono_small"],
                                     bg=C["mantle"], fg=C["subtext"])
        self._count_label.pack(side="left", padx=14)
        tk.Button(btn_bar, text="Cancel", font=FONTS["body"],
                  bg=C["surface0"], fg=C["text"], relief="flat", padx=14, pady=3,
                  command=self.destroy).pack(side="right", padx=8)
        tk.Button(btn_bar, text="Run Queue", font=FONTS["body_bold"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=14, pady=3,
                  command=self._confirm).pack(side="right", padx=4)

    def _add_job(self):
        sel = self._avail_lb.curselection()
        if not sel:
            return
        job = self._avail_lb.get(sel[0])
        self._queue_lb.insert("end", job)
        self._update_count()

    def _remove_job(self):
        sel = self._queue_lb.curselection()
        if not sel:
            return
        self._queue_lb.delete(sel[0])
        self._update_count()

    def _move_up(self):
        sel = self._queue_lb.curselection()
        if not sel or sel[0] == 0:
            return
        idx = sel[0]
        item = self._queue_lb.get(idx)
        self._queue_lb.delete(idx)
        self._queue_lb.insert(idx - 1, item)
        self._queue_lb.selection_set(idx - 1)

    def _move_down(self):
        sel = self._queue_lb.curselection()
        if not sel or sel[0] >= self._queue_lb.size() - 1:
            return
        idx = sel[0]
        item = self._queue_lb.get(idx)
        self._queue_lb.delete(idx)
        self._queue_lb.insert(idx + 1, item)
        self._queue_lb.selection_set(idx + 1)

    def _update_count(self):
        n = self._queue_lb.size()
        self._count_label.config(text=f"{n} jobs")

    def _confirm(self):
        self.queue = [self._queue_lb.get(i) for i in range(self._queue_lb.size())]
        self.destroy()
