"""
gui/widgets.py  ─  독립 위젯 클래스 (SqlSelectorDialog, CollapsibleSection, Tooltip)
"""

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
